import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gru.iceberg.time_travel")

BASE_DIR = Path(os.environ.get("GRU_BASE_DIR", Path(__file__).parent.parent))
WAREHOUSE_DIR = BASE_DIR / "warehouse"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("GRU-IcebergTimeTravelDemo")
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


def run_demo() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    table = "local.gold.fato_conexoes"

    print("\nIceberg Time Travel — fato_conexoes\n")

    df_current = spark.table(table)
    count_current = df_current.count()
    print(f"Current state: {count_current:,} connections\n")
    df_current.groupBy("desc_status_risco").count().orderBy("count", ascending=False).show()

    print("\nSnapshot history:")
    spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {table}.snapshots").show(
        truncate=False
    )

    print("\nTime travel - reading previous snapshot...")
    snapshots = (
        spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at")
        .collect()
    )

    if len(snapshots) >= 2:
        prev_snapshot_id = snapshots[-2]["snapshot_id"]
        df_prev = spark.read.option("snapshot-id", prev_snapshot_id).table(table)
        count_prev = df_prev.count()
        print(f"Snapshot {prev_snapshot_id}: {count_prev:,} connections")
        print(f"Delta: {count_current - count_prev:+,} connections\n")
        df_prev.groupBy("desc_status_risco").count().orderBy("count", ascending=False).show()
    else:
        print("Run the pipeline more than once to compare snapshots.")

    print("\nData files (Iceberg manifest):")
    spark.sql(f"SELECT file_path, record_count, file_size_in_bytes FROM {table}.files").show(
        5, truncate=True
    )


if __name__ == "__main__":
    run_demo()
