"""
Inspect Gold Layer — fato_conexoes
===================================
Utilitário para inspecionar os dados da camada Gold após execução do dbt.

Uso:
    python spark_jobs/inspect_gold.py
    GRU_BASE_DIR=/meu/projeto python spark_jobs/inspect_gold.py
"""
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gru.gold.inspect")

BASE_DIR = Path(os.environ.get("GRU_BASE_DIR", Path(__file__).parent.parent))
GOLD_PATH = BASE_DIR / "data" / "gold" / "fato_conexoes"


def inspect_data() -> None:
    spark = (
        SparkSession.builder.appName("GRU-Inspect-Gold")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    if not GOLD_PATH.exists():
        log.error("Gold não encontrada em: %s — Execute a DAG Gold primeiro.", GOLD_PATH)
        return

    log.info("Lendo Gold em: %s", GOLD_PATH)
    df = spark.read.parquet(str(GOLD_PATH))

    total = df.count()
    log.info("Total de conexões mapeadas: %d", total)

    print("\n── Amostra de Conexões e Riscos (ordenado por janela menor) ──")
    df.select(
        "cd_icao_empresa",
        "nr_voo_chegada",
        "nr_voo_partida",
        "janela_conexao_min",
        "desc_status_risco",
    ).orderBy("janela_conexao_min").show(10, truncate=False)

    print("\n── Resumo por Status de Risco ──")
    df.groupBy("desc_status_risco").count().orderBy("count", ascending=False).show()

    print("\n── Top 5 Empresas com mais conexões críticas ──")
    df.filter(df.desc_status_risco == "Risco Crítico") \
      .groupBy("cd_icao_empresa") \
      .count() \
      .orderBy("count", ascending=False) \
      .show(5)


if __name__ == "__main__":
    inspect_data()