import argparse
import logging
import os
import shutil
from pathlib import Path

import requests
import urllib3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gru.bronze.ingestion")

BASE_DIR = Path(os.environ.get("GRU_BASE_DIR", Path(__file__).parent.parent))
WAREHOUSE_DIR = BASE_DIR / "warehouse"
CSV_TEMP_DIR = BASE_DIR / "data" / "temp_csv"

# ANAC CSV headers use ISO-8859-1 encoding; mapping to snake_case before writing Parquet/Iceberg
ANAC_COLUMN_MAP = {
    "Sigla ICAO Empresa Aérea": "cd_icao_empresa",
    "Número Voo": "nr_voo",
    "Código Di": "cd_di",
    "Código Tipo Linha": "cd_tipo_linha",
    "Sigla ICAO Aeroporto Origem": "cd_icao_origem",
    "Sigla ICAO Aeroporto Destino": "cd_icao_destino",
    "Partida Prevista": "dt_partida_prevista",
    "Partida Real": "dt_partida_real",
    "Chegada Prevista": "dt_chegada_prevista",
    "Chegada Real": "dt_chegada_real",
    "Situação Voo": "nm_situacao_voo",
    "Código Justificativa": "cd_justificativa",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest ANAC VRA into Bronze Iceberg table.")
    parser.add_argument("--ano", default=os.environ.get("ANAC_ANO", "2025"))
    parser.add_argument("--mes", default=os.environ.get("ANAC_MES", "01"))
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("GRU-Connect-Bronze-Ingestion")
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


def download_vra_anac(ano: str, mes: str) -> Path:
    CSV_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    mes_pad = mes.zfill(2)
    url = f"https://siros.anac.gov.br/siros/registros/diversos/vra/{ano}/VRA_{ano}_{mes_pad}.csv"
    file_path = CSV_TEMP_DIR / f"vra_{ano}_{mes_pad}.csv"

    log.info("Downloading: %s", url)
    response = requests.get(url, timeout=60, verify=False)  # noqa: S501

    if response.status_code == 200:
        file_path.write_bytes(response.content)
        log.info("Downloaded: %d bytes", len(response.content))
        return file_path

    raise RuntimeError(f"HTTP {response.status_code} fetching VRA {ano}/{mes_pad}: {url}")


def run_ingestion(ano: str, mes: str) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    csv_path = None
    try:
        csv_path = download_vra_anac(ano, mes)

        df_raw = (
            spark.read.format("csv")
            .option("header", "true")
            .option("sep", ";")
            .option("encoding", "ISO-8859-1")
            .option("inferSchema", "false")
            .load(str(csv_path))
        )
        log.info("Records read from CSV: %d", df_raw.count())

        df_renamed = df_raw
        for col_original, col_alias in ANAC_COLUMN_MAP.items():
            if col_original in df_raw.columns:
                df_renamed = df_renamed.withColumnRenamed(col_original, col_alias)

        df_gru = df_renamed.filter(
            (F.col("cd_icao_origem") == "SBGR") | (F.col("cd_icao_destino") == "SBGR")
        )
        df_gru = df_gru.withColumn("nr_ano", F.lit(ano)).withColumn("nr_mes", F.lit(mes.zfill(2)))

        total = df_gru.count()
        log.info("GRU records after filter: %d", total)
        if total == 0:
            raise ValueError("No GRU flights found. Check the VRA file.")

        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

        table_name = "local.bronze.vra_gru_raw"
        if spark.catalog.tableExists(table_name):
            spark.sql(f"""
                DELETE FROM {table_name}
                WHERE nr_ano = '{ano}' AND nr_mes = '{mes.zfill(2)}'
            """)
            df_gru.writeTo(table_name).append()
        else:
            df_gru.writeTo(table_name).partitionedBy("nr_ano", "nr_mes").createOrReplace()

        log.info("Bronze Iceberg table updated: %s (ano=%s, mes=%s)", table_name, ano, mes)

    finally:
        if csv_path and CSV_TEMP_DIR.exists():
            shutil.rmtree(CSV_TEMP_DIR)
            log.info("Temp directory removed")


if __name__ == "__main__":
    args = parse_args()
    log.info("Bronze ingestion - VRA %s/%s", args.ano, args.mes)
    run_ingestion(ano=args.ano, mes=args.mes)
    log.info("Done")
