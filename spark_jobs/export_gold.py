"""Export Gold Iceberg tables to simple Parquet files for the Streamlit dashboard.

This script reads the Gold layer Iceberg tables (created by dbt) and writes
them as single Parquet files into data/gold/. The Streamlit container reads
these files with pandas — no Spark dependency needed in the dashboard.

Usage:
    python spark_jobs/export_gold.py
"""
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gru.gold.export")

BASE_DIR = Path(os.environ.get("GRU_BASE_DIR", Path(__file__).parent.parent))
WAREHOUSE_DIR = BASE_DIR / "warehouse"
EXPORT_DIR = BASE_DIR / "data" / "gold"

GOLD_TABLES = [
    "local.gold.fato_conexoes",
    "local.gold.dim_aeroportos",
    "local.gold.dim_empresas",
    "local.gold.dim_calendario",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("GRU-Gold-Export")
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


def export_tables() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    for full_name in GOLD_TABLES:
        short_name = full_name.split(".")[-1]
        out_path = EXPORT_DIR / f"{short_name}.parquet"

        if not spark.catalog.tableExists(full_name):
            log.warning("Table %s does not exist — skipping", full_name)
            continue

        df = spark.table(full_name)
        count = df.count()
        log.info("Exporting %s (%d rows) -> %s", full_name, count, out_path)

        # Write as a single Parquet file (coalesce to 1 partition)
        df.coalesce(1).toPandas().to_parquet(str(out_path), index=False)

    log.info("Export complete — files in %s", EXPORT_DIR)


if __name__ == "__main__":
    log.info("Exporting Gold Iceberg tables to Parquet")
    export_tables()
    log.info("Done")
