import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gru.silver.transformation")

# Configurações de Caminho
BASE_DIR = Path(os.environ.get("GRU_BASE_DIR", Path(__file__).parent.parent))
WAREHOUSE_DIR = BASE_DIR / "warehouse"
DATE_FORMAT = "dd/MM/yyyy HH:mm"

def build_spark() -> SparkSession:
    """Cria a SparkSession com suporte ao Iceberg."""
    return (
        SparkSession.builder.appName("GRU-Connect-Silver-Transformation")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(WAREHOUSE_DIR))
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        )
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

def run_silver_transformation() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Reading Bronze: local.bronze.vra_gru_raw")
    df_bronze = spark.table("local.bronze.vra_gru_raw")

    # Mapeamos as colunas por posição para evitar erros de encoding (acentuação)
    # Baseado no log: 0: Sigla ICAO, 2: Número Voo, 15: Situação Voo
    cols = df_bronze.columns

    count_bronze = df_bronze.count()
    log.info("Records in Bronze: %d", count_bronze)
    if count_bronze == 0:
        raise ValueError("Bronze table is empty. Run the Bronze ingestion first.")

    # Transformação Silver
    df_silver = (
        df_bronze
        .select(
            F.trim(F.col(cols[0])).alias("cd_icao_empresa"),
            F.col(cols[2]).alias("nr_voo"),
            F.col("cd_icao_origem"),
            F.col("cd_icao_destino"),
            F.col("dt_partida_prevista"),
            F.col("dt_partida_real"),
            F.col("dt_chegada_prevista"),
            F.col("dt_chegada_real"),
            F.trim(F.col(cols[15])).alias("nm_situacao_voo"),
            F.col("nr_ano"),
            F.col("nr_mes"),
        )
        # Conversão de tipos: String para Timestamp
        .withColumn("dt_partida_prevista", F.to_timestamp("dt_partida_prevista", DATE_FORMAT))
        .withColumn("dt_partida_real", F.to_timestamp("dt_partida_real", DATE_FORMAT))
        .withColumn("dt_chegada_prevista", F.to_timestamp("dt_chegada_prevista", DATE_FORMAT))
        .withColumn("dt_chegada_real", F.to_timestamp("dt_chegada_real", DATE_FORMAT))
        # Cálculo de métrica: Atraso em minutos
        .withColumn(
            "vl_atraso_chegada_min",
            (F.unix_timestamp("dt_chegada_real") - F.unix_timestamp("dt_chegada_prevista")) / 60,
        )
    )

    # Verificação de qualidade (Nulls)
    for col_name in ["cd_icao_empresa", "nr_voo", "dt_chegada_real"]:
        null_count = df_silver.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            log.warning("Nulls in '%s': %d", col_name, null_count)

    # Criação do Namespace e Persistência na camada Silver (Iceberg)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    table_name = "local.silver.stg_anac_vra"

    log.info("Saving to Silver: %s", table_name)
    if spark.catalog.tableExists(table_name):
        df_silver.writeTo(table_name).overwritePartitions()
    else:
        df_silver.writeTo(table_name).partitionedBy("nr_ano", "nr_mes").createOrReplace()

    log.info("Silver Iceberg table updated successfully.")
    df_silver.show(5, truncate=False)

if __name__ == "__main__":
    log.info("Starting Silver transformation job")
    run_silver_transformation()
    log.info("Transformation completed successfully")
