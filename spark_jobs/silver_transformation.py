"""
Silver Layer Transformation — ANAC VRA
=======================================
Lê os dados Parquet da camada Bronze, aplica tipagem, limpeza e cálculo
de atraso, e salva na camada Silver.

Uso:
    python spark_jobs/silver_transformation.py
    GRU_BASE_DIR=/meu/projeto python spark_jobs/silver_transformation.py
"""
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gru.silver.transformation")

# ─── Resolução de Caminhos via Env Var ───────────────────────────────────────
BASE_DIR = Path(os.environ.get("GRU_BASE_DIR", Path(__file__).parent.parent))
BRONZE_PATH = BASE_DIR / "data" / "bronze" / "vra_gru_raw"
SILVER_PATH = BASE_DIR / "data" / "silver" / "stg_anac_vra"

# Formato de data usado nos CSVs da ANAC
DATE_FORMAT = "dd/MM/yyyy HH:mm"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("GRU-Connect-Silver-Transformation")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def run_silver_transformation() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Lendo dados da Bronze em: %s", BRONZE_PATH)
    df_bronze = spark.read.parquet(str(BRONZE_PATH))

    # Validação básica de schema — falha rápida se a Bronze estiver vazia
    count_bronze = df_bronze.count()
    log.info("Registros lidos da Bronze: %d", count_bronze)
    if count_bronze == 0:
        raise ValueError("Bronze está vazia. Execute a ingestão Bronze primeiro.")

    # ── Seleção e tipagem ────────────────────────────────────────────────────
    # Após o fix de encoding no ingestion_vra.py, os nomes já estão em snake_case.
    # Aqui apenas selecionamos as colunas de interesse e convertemos os tipos.
    df_silver = (
        df_bronze
        # As colunas já estão renomeadas pelo ingestion_vra.py
        .select(
            F.col("cd_icao_empresa"),
            F.col("nr_voo"),
            F.col("cd_icao_origem"),
            F.col("cd_icao_destino"),
            F.col("dt_partida_prevista"),
            F.col("dt_partida_real"),
            F.col("dt_chegada_prevista"),
            F.col("dt_chegada_real"),
            F.col("nm_situacao_voo"),
        )
        # Conversão de strings para timestamps
        .withColumn("dt_partida_prevista", F.to_timestamp("dt_partida_prevista", DATE_FORMAT))
        .withColumn("dt_partida_real", F.to_timestamp("dt_partida_real", DATE_FORMAT))
        .withColumn("dt_chegada_prevista", F.to_timestamp("dt_chegada_prevista", DATE_FORMAT))
        .withColumn("dt_chegada_real", F.to_timestamp("dt_chegada_real", DATE_FORMAT))
        # Limpeza: remover espaços extras em campos de texto
        .withColumn("cd_icao_empresa", F.trim(F.col("cd_icao_empresa")))
        .withColumn("nm_situacao_voo", F.trim(F.col("nm_situacao_voo")))
    )

    # ── Métricas de Negócio ──────────────────────────────────────────────────
    df_silver = df_silver.withColumn(
        "vl_atraso_chegada_min",
        (F.unix_timestamp("dt_chegada_real") - F.unix_timestamp("dt_chegada_prevista")) / 60,
    )

    # ── QA: log de nulos em campos críticos ──────────────────────────────────
    for col_name in ["cd_icao_empresa", "nr_voo", "dt_chegada_real"]:
        null_count = df_silver.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            log.warning("Coluna '%s': %d registros nulos", col_name, null_count)

    # ── Salvar na Silver ─────────────────────────────────────────────────────
    df_silver.write.mode("overwrite").parquet(str(SILVER_PATH))
    log.info("Silver salvo com sucesso em: %s", SILVER_PATH)
    log.info("Amostra (5 registros):")
    df_silver.show(5, truncate=False)


if __name__ == "__main__":
    log.info("=== Iniciando transformação Silver ===")
    run_silver_transformation()
    log.info("=== Transformação Silver concluída ===")
