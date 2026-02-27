import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
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
        log.error("Gold table not found at: %s", GOLD_PATH)
        return

    log.info("Reading Gold: %s", GOLD_PATH)
    df = spark.read.parquet(str(GOLD_PATH))

    log.info("Total connections mapped: %d", df.count())

    print("\nConnections sample (sorted by shortest window):")
    df.select(
        "cd_icao_empresa",
        "nr_voo_chegada",
        "nr_voo_partida",
        "janela_conexao_min",
        "desc_status_risco",
    ).orderBy("janela_conexao_min").show(10, truncate=False)

    print("\nRisk summary:")
    df.groupBy("desc_status_risco").count().orderBy("count", ascending=False).show()

    print("\nTop 5 airlines by critical connections:")
    df.filter(df.desc_status_risco == "Risco Crítico") \
      .groupBy("cd_icao_empresa") \
      .count() \
      .orderBy("count", ascending=False) \
      .show(5)


if __name__ == "__main__":
    inspect_data()
