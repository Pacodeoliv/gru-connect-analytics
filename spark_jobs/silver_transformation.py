import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gru.silver.transformation")

BASE_DIR = Path(os.environ.get("GRU_BASE_DIR", Path(__file__).parent.parent))
WAREHOUSE_DIR = BASE_DIR / "warehouse"
DATE_FORMAT = "dd/MM/yyyy HH:mm"


def build_spark() -> SparkSession:
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

    count_bronze = df_bronze.count()
    log.info("Records in Bronze: %d", count_bronze)
    if count_bronze == 0:
        raise ValueError("Bronze table is empty. Run the Bronze ingestion first.")

    df_silver = (
        df_bronze
        .select(
            F.trim(F.col("cd_icao_empresa")).alias("cd_icao_empresa"),
            F.col("nr_voo"),
            F.col("cd_icao_origem"),
            F.col("cd_icao_destino"),
            F.col("dt_partida_prevista"),
            F.col("dt_partida_real"),
            F.col("dt_chegada_prevista"),
            F.col("dt_chegada_real"),
            F.trim(F.col("nm_situacao_voo")).alias("nm_situacao_voo"),
            F.col("nr_ano"),
            F.col("nr_mes"),
        )
        .withColumn("dt_partida_prevista", F.to_timestamp("dt_partida_prevista", DATE_FORMAT))
        .withColumn("dt_partida_real", F.to_timestamp("dt_partida_real", DATE_FORMAT))
        .withColumn("dt_chegada_prevista", F.to_timestamp("dt_chegada_prevista", DATE_FORMAT))
        .withColumn("dt_chegada_real", F.to_timestamp("dt_chegada_real", DATE_FORMAT))
        .withColumn(
            "vl_atraso_chegada_min",
            (F.unix_timestamp("dt_chegada_real") - F.unix_timestamp("dt_chegada_prevista")) / 60,
        )
    )

    for col_name in ["cd_icao_empresa", "nr_voo", "dt_chegada_real"]:
        null_count = df_silver.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            log.warning("Nulls in '%s': %d", col_name, null_count)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    table_name = "local.silver.stg_anac_vra"

    if spark.catalog.tableExists(table_name):
        df_silver.writeTo(table_name).overwritePartitions()
    else:
        df_silver.writeTo(table_name).partitionedBy("nr_ano", "nr_mes").createOrReplace()

    log.info("Silver Iceberg table updated: %s", table_name)
    df_silver.show(5, truncate=False)


if __name__ == "__main__":
    log.info("Silver transformation")
    run_silver_transformation()
    log.info("Done")
