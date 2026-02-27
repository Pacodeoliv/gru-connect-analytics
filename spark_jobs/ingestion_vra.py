"""
Bronze Layer Ingestion — ANAC VRA Data (Apache Iceberg)
========================================================
Baixa o CSV mensal do VRA (Voos Realizados pela ANAC) e salva como
tabela Apache Iceberg na camada Bronze do Data Lakehouse.

Vantagens do Iceberg sobre Parquet puro:
  - ACID: se o job cair, nenhum dado parcial fica gravado
  - Time travel: consultar versões anteriores dos dados
  - Upsert idempotente: re-rodar o mesmo mês não duplica

Uso:
    python spark_jobs/ingestion_vra.py --ano 2025 --mes 01
    GRU_BASE_DIR=/meu/projeto python spark_jobs/ingestion_vra.py --ano 2025 --mes 01
"""
import argparse
import logging
import os
import shutil
import warnings
from pathlib import Path

import requests
import urllib3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gru.bronze.ingestion")

BASE_DIR = Path(os.environ.get("GRU_BASE_DIR", Path(__file__).parent.parent))
WAREHOUSE_DIR = BASE_DIR / "warehouse"
CSV_TEMP_DIR = BASE_DIR / "data" / "temp_csv"

# Mapeamento de colunas ANAC (ISO-8859-1) → snake_case
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
    parser = argparse.ArgumentParser(description="Ingere VRA da ANAC na camada Bronze (Iceberg).")
    parser.add_argument("--ano", default=os.environ.get("ANAC_ANO", "2025"))
    parser.add_argument("--mes", default=os.environ.get("ANAC_MES", "01"))
    return parser.parse_args()


def build_spark() -> SparkSession:
    """Cria SparkSession com suporte a Apache Iceberg."""
    return (
        SparkSession.builder.appName("GRU-Connect-Bronze-Ingestion")
        # ── Iceberg: extensões SQL e catalog local ──────────────────────────
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(WAREHOUSE_DIR))
        # ── Pacote Iceberg (baixado automaticamente pelo Spark) ─────────────
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        )
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def download_vra_anac(ano: str, mes: str) -> Path:
    """Baixa o CSV mensal do VRA do servidor SIROS/ANAC."""
    CSV_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    mes_pad = mes.zfill(2)
    url = f"https://siros.anac.gov.br/siros/registros/diversos/vra/{ano}/VRA_{ano}_{mes_pad}.csv"
    file_path = CSV_TEMP_DIR / f"vra_{ano}_{mes_pad}.csv"

    log.info("Baixando: %s", url)
    response = requests.get(url, timeout=60, verify=False)  # noqa: S501

    if response.status_code == 200:
        file_path.write_bytes(response.content)
        log.info("Download concluído: %d bytes", len(response.content))
        return file_path

    raise RuntimeError(f"HTTP {response.status_code} ao baixar VRA {ano}/{mes_pad}: {url}")


def run_ingestion(ano: str, mes: str) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    csv_path = None
    try:
        csv_path = download_vra_anac(ano, mes)

        # ── Leitura CSV com encoding correto ─────────────────────────────────
        df_raw = (
            spark.read.format("csv")
            .option("header", "true")
            .option("sep", ";")
            .option("encoding", "ISO-8859-1")
            .option("inferSchema", "false")
            .load(str(csv_path))
        )
        log.info("Registros lidos: %d", df_raw.count())

        # ── Renomear colunas (fix de encoding nos nomes) ─────────────────────
        df_renamed = df_raw
        for col_original, col_alias in ANAC_COLUMN_MAP.items():
            if col_original in df_raw.columns:
                df_renamed = df_renamed.withColumnRenamed(col_original, col_alias)

        # ── Filtro: apenas voos de/para GRU (SBGR) ───────────────────────────
        df_gru = df_renamed.filter(
            (F.col("cd_icao_origem") == "SBGR") | (F.col("cd_icao_destino") == "SBGR")
        )

        # ── Adicionar coluna de partição para organizar por mês ──────────────
        df_gru = df_gru.withColumn("nr_ano", F.lit(ano)).withColumn("nr_mes", F.lit(mes.zfill(2)))

        total = df_gru.count()
        log.info("Registros GRU filtrados: %d", total)
        if total == 0:
            raise ValueError("Nenhum voo GRU encontrado. Verifique o arquivo VRA.")

        # ── Criar namespace Iceberg se não existir ────────────────────────────
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

        # ── Salvar como tabela Iceberg (upsert por mês — idempotente) ─────────
        # A lógica: deletar registros do mesmo mês/ano e reinserir.
        # Com Iceberg isso é ACID — se falhar, os dados anteriores são preservados.
        table_name = "local.bronze.vra_gru_raw"

        if spark.catalog.tableExists(table_name):
            spark.sql(f"""
                DELETE FROM {table_name}
                WHERE nr_ano = '{ano}' AND nr_mes = '{mes.zfill(2)}'
            """)
            df_gru.writeTo(table_name).append()
        else:
            df_gru.writeTo(table_name).partitionedBy("nr_ano", "nr_mes").createOrReplace()

        log.info("Tabela Iceberg Bronze atualizada: %s (ano=%s, mes=%s)", table_name, ano, mes)

    finally:
        if csv_path and CSV_TEMP_DIR.exists():
            shutil.rmtree(CSV_TEMP_DIR)
            log.info("Pasta temporária limpa")


if __name__ == "__main__":
    args = parse_args()
    log.info("=== Bronze Ingestion — VRA %s/%s ===", args.ano, args.mes)
    run_ingestion(ano=args.ano, mes=args.mes)
    log.info("=== Concluído ===")
