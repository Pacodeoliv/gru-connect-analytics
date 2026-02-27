"""
Time Travel Demo — Apache Iceberg
===================================
Demonstra o recurso de time travel do Apache Iceberg: consultar versões
anteriores da tabela fato_conexoes como se fosse um "git history" dos dados.

Casos de uso reais:
  - "Os dados mudaram entre ontem e hoje? Qual era o risco médio antes do reprocessamento?"
  - Auditoria regulatória (ANAC pode exigir rastreabilidade)
  - Debug: comparar execuções diferentes do pipeline

Uso:
    python spark_jobs/time_travel_demo.py
    GRU_BASE_DIR=/meu/projeto python spark_jobs/time_travel_demo.py
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

    # ── 1. Estado atual da tabela ─────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  DEMO: Apache Iceberg Time Travel — fato_conexoes")
    print("═" * 60)

    df_atual = spark.table(table)
    count_atual = df_atual.count()
    print(f"\n📊 Estado ATUAL da tabela: {count_atual:,} conexões\n")

    df_atual.groupBy("desc_status_risco").count().orderBy("count", ascending=False).show()

    # ── 2. Histórico de snapshots (cada run do pipeline = 1 snapshot) ─────────
    print("\n📜 Histórico de Snapshots (cada execução do pipeline gera um snapshot):")
    spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {table}.snapshots").show(
        truncate=False
    )

    # ── 3. Time Travel por número de snapshot ────────────────────────────────
    print("\n⏮  Time Travel: consultando a versão ANTERIOR (penúltimo snapshot)...")
    snapshots = (
        spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at")
        .collect()
    )

    if len(snapshots) >= 2:
        snapshot_anterior = snapshots[-2]["snapshot_id"]
        df_anterior = spark.read.option("snapshot-id", snapshot_anterior).table(table)
        count_anterior = df_anterior.count()
        print(f"   Snapshot anterior ({snapshot_anterior}): {count_anterior:,} conexões")
        print(f"   Diferença: {count_atual - count_anterior:+,} conexões desde então\n")
        df_anterior.groupBy("desc_status_risco").count().orderBy("count", ascending=False).show()
    else:
        print("   (Execute o pipeline mais de uma vez para ver a comparação entre snapshots)")

    # ── 4. Listar arquivos do manifesto Iceberg ──────────────────────────────
    print("\n📁 Arquivos de Dados (Iceberg Manifest):")
    spark.sql(f"SELECT file_path, record_count, file_size_in_bytes FROM {table}.files").show(
        5, truncate=True
    )

    print("\n✅ Demo concluída! O Iceberg rastreia TUDO automaticamente.")


if __name__ == "__main__":
    run_demo()
