"""
Bronze Layer Ingestion — ANAC VRA Data
======================================
Baixa o CSV mensal do VRA (Voos Realizados pela ANAC) e salva em formato
Parquet na camada Bronze do Data Lakehouse.

Uso:
    python spark_jobs/ingestion_vra.py --ano 2025 --mes 01
    # ou via variáveis de ambiente:
    ANAC_ANO=2025 ANAC_MES=01 python spark_jobs/ingestion_vra.py
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
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

# Suprime avisos de SSL do servidor governamental (certificado desatualizado)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gru.bronze.ingestion")

# ─── Resolução de Caminhos via Env Var ───────────────────────────────────────
BASE_DIR = Path(os.environ.get("GRU_BASE_DIR", Path(__file__).parent.parent))
RAW_DIR = BASE_DIR / "data" / "bronze"
CSV_TEMP_DIR = BASE_DIR / "data" / "temp_csv"

# Nomes das colunas do CSV da ANAC (encoding ISO-8859-1 → mapeamento explícito)
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
    parser = argparse.ArgumentParser(description="Ingere VRA da ANAC na camada Bronze.")
    parser.add_argument(
        "--ano",
        default=os.environ.get("ANAC_ANO", "2025"),
        help="Ano de referência ANAC (ex: 2025)",
    )
    parser.add_argument(
        "--mes",
        default=os.environ.get("ANAC_MES", "01"),
        help="Mês de referência ANAC com zero-padding (ex: 01)",
    )
    return parser.parse_args()


def download_vra_anac(ano: str, mes: str) -> Path:
    """Baixa o CSV mensal do VRA do servidor SIROS/ANAC."""
    CSV_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    mes_pad = mes.zfill(2)

    url = f"https://siros.anac.gov.br/siros/registros/diversos/vra/{ano}/VRA_{ano}_{mes_pad}.csv"
    file_path = CSV_TEMP_DIR / f"vra_{ano}_{mes_pad}.csv"

    log.info("Iniciando download: %s", url)

    # verify=False necessário: servidor da ANAC usa certificado SSL auto-assinado
    response = requests.get(url, timeout=60, verify=False)  # noqa: S501

    if response.status_code == 200:
        file_path.write_bytes(response.content)
        log.info("Download concluído: %s (%d bytes)", file_path, len(response.content))
        return file_path

    raise RuntimeError(
        f"HTTP {response.status_code} ao baixar VRA {ano}/{mes_pad}. "
        f"Verifique se o arquivo existe em: {url}"
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("GRU-Connect-Bronze-Ingestion")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def run_ingestion(ano: str, mes: str) -> None:
    """Pipeline completo de ingestão Bronze para um mês específico."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    csv_path = None
    try:
        csv_path = download_vra_anac(ano, mes)

        # ── Leitura com encoding correto (ISO-8859-1 = padrão ANAC) ─────────
        # NÃO usamos inferSchema=true para evitar colunas com nomes corrompidos.
        # O dicionário ANAC_COLUMN_MAP garante o mapeamento correto antes do save.
        df_raw = (
            spark.read.format("csv")
            .option("header", "true")
            .option("sep", ";")
            .option("encoding", "ISO-8859-1")
            .option("inferSchema", "false")  # Tudo como string — limpeza na Silver
            .load(str(csv_path))
        )

        log.info("Total de registros lidos do CSV: %d", df_raw.count())

        # ── Renomear colunas para snake_case antes de salvar no Parquet ──────
        # Isso resolve o bug de encoding nos nomes de colunas no Parquet/Silver.
        df_renamed = df_raw
        for col_original, col_alias in ANAC_COLUMN_MAP.items():
            if col_original in df_raw.columns:
                df_renamed = df_renamed.withColumnRenamed(col_original, col_alias)

        # ── Filtro de Negócio: foco em GRU (SBGR) ────────────────────────────
        df_gru = df_renamed.filter(
            (F.col("cd_icao_origem") == "SBGR")
            | (F.col("cd_icao_destino") == "SBGR")
        )

        total_gru = df_gru.count()
        log.info("Total de voos GRU filtrados: %d", total_gru)

        if total_gru == 0:
            raise ValueError(
                "Nenhum voo de/para GRU encontrado. Verifique o arquivo VRA."
            )

        # ── Salvar na Bronze ────────────────────────────────────────────────
        output_path = RAW_DIR / "vra_gru_raw"
        df_gru.write.mode("overwrite").parquet(str(output_path))
        log.info("Bronze salvo com sucesso em: %s", output_path)

    finally:
        # Limpeza do CSV temporário independente de sucesso/falha
        if csv_path and CSV_TEMP_DIR.exists():
            shutil.rmtree(CSV_TEMP_DIR)
            log.info("Pasta temporária limpa: %s", CSV_TEMP_DIR)


if __name__ == "__main__":
    args = parse_args()
    log.info("=== Iniciando ingestão Bronze — VRA %s/%s ===", args.ano, args.mes)
    run_ingestion(ano=args.ano, mes=args.mes)
    log.info("=== Ingestão Bronze concluída ===")
