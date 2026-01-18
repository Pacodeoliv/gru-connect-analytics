import os
import requests
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuração de caminhos baseada na estrutura que criamos
BASE_DIR = os.getcwd()
RAW_DIR = os.path.join(BASE_DIR, "data", "bronze")
CSV_TEMP_DIR = os.path.join(BASE_DIR, "data", "temp_csv")


def download_vra_anac(ano: str, mes: str):
    """Baixa o CSV diretamente do diretório anual do servidor SIROS da ANAC."""
    os.makedirs(CSV_TEMP_DIR, exist_ok=True)
    mes_pad = mes.zfill(2)

    # URL ajustada conforme o diretório de 2025 que você validou
    url = f"https://siros.anac.gov.br/siros/registros/diversos/vra/{ano}/VRA_{ano}_{mes_pad}.csv"
    file_path = os.path.join(CSV_TEMP_DIR, f"vra_{ano}_{mes_pad}.csv")

    print(f"--- Tentando baixar de: {url} ---")

    # verify=False ajuda se o servidor do governo estiver com problemas de SSL
    response = requests.get(url, timeout=60, verify=False)

    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"--- Download concluído com sucesso: {file_path} ---")
        return file_path
    else:
        raise Exception(
            f"Erro {response.status_code}. O arquivo não foi encontrado. Verifique o link."
        )


def run_ingestion():
    # Inicializa a Sessão Spark com foco em performance local
    spark = (
        SparkSession.builder.appName("GRU-Connect-Bronze-Ingestion")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    # Reduz logs para facilitar o debug do que importa
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Vamos testar com os dados de Janeiro de 2025 que você mapeou
        csv_path = download_vra_anac("2025", "01")

        # Leitura técnica: ANAC usa ';' e encoding ISO-8859-1 (Latin1)
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("sep", ";")
            .option("encoding", "ISO-8859-1")
            .option("inferSchema", "true")
            .load(csv_path)
        )

        print(f"Total de registros lidos: {df.count()}")

        # Filtro de Negócio: Foco em Guarulhos (SBGR)
        # Usando os nomes das colunas conforme o log do AnalysisException anterior
        df_gru = df.filter(
            (F.col("Sigla ICAO Aeroporto Origem") == "SBGR")
            | (F.col("Sigla ICAO Aeroporto Destino") == "SBGR")
        )

        # Salvando em Camada Bronze (Parquet)
        output_path = os.path.join(RAW_DIR, "vra_gru_raw")
        df_gru.write.mode("overwrite").parquet(output_path)

        print(f"--- Sucesso! Dados filtrados de GRU salvos em: {output_path} ---")
        print(f"Total de registros de GRU: {df_gru.count()}")

    except Exception as e:
        print(f"⚠️ Erro durante o processamento: {str(e)}")
        raise

    finally:
        # Limpeza da pasta temporária de CSVs
        if os.path.exists(CSV_TEMP_DIR):
            shutil.rmtree(CSV_TEMP_DIR)
            print("--- Pasta temporária limpa ---")


if __name__ == "__main__":
    run_ingestion()
