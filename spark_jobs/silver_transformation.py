from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# Configuração de caminhos
BASE_DIR = os.getcwd()
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze", "vra_gru_raw")
SILVER_PATH = os.path.join(BASE_DIR, "data", "silver", "stg_anac_vra")


def run_silver_transformation():
    spark = SparkSession.builder.appName("GRU-Connect-Silver-Transformation").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("--- Lendo dados da Camada Bronze ---")
    df_bronze = spark.read.parquet(BRONZE_PATH)

    # 1. Mapeamento de De/Para (Renomeação)
    # Aqui eliminamos os caracteres especiais e espaços
    df_silver = df_bronze.select(
        F.col("Sigla ICAO Empresa AÃ©rea").alias("cd_icao_empresa"),
        F.col("NÃºmero Voo").alias("nr_voo"),
        F.col("Sigla ICAO Aeroporto Origem").alias("cd_icao_origem"),
        F.col("Sigla ICAO Aeroporto Destino").alias("cd_icao_destino"),
        F.col("Partida Prevista").alias("dt_partida_prevista"),
        F.col("Partida Real").alias("dt_partida_real"),
        F.col("Chegada Prevista").alias("dt_chegada_prevista"),
        F.col("Chegada Real").alias("dt_chegada_real"),
        F.col("SituaÃ§Ã£o Voo").alias("nm_situacao_voo"),
    )

    # 2. Tipagem e Limpeza
    # Convertendo as strings de data para Timestamp real
    # O formato no CSV original costuma ser dd/MM/yyyy HH:mm
    date_format = "dd/MM/yyyy HH:mm"

    df_silver = (
        df_silver.withColumn(
            "dt_partida_prevista", F.to_timestamp("dt_partida_prevista", date_format)
        )
        .withColumn("dt_partida_real", F.to_timestamp("dt_partida_real", date_format))
        .withColumn("dt_chegada_prevista", F.to_timestamp("dt_chegada_prevista", date_format))
        .withColumn("dt_chegada_real", F.to_timestamp("dt_chegada_real", date_format))
    )

    # 3. Cálculo de Atraso (Métrica de Negócio)
    # Atraso em minutos: (Real - Previsto) / 60 segundos
    df_silver = df_silver.withColumn(
        "vl_atraso_chegada_min",
        (F.unix_timestamp("dt_chegada_real") - F.unix_timestamp("dt_chegada_prevista")) / 60,
    )

    # 4. Salvando na Silver (Parquet)
    # Usamos o modo overwrite para facilitar o desenvolvimento
    df_silver.write.mode("overwrite").parquet(SILVER_PATH)

    print(f"--- Sucesso! Camada Silver gerada em: {SILVER_PATH} ---")
    df_silver.show(5)


if __name__ == "__main__":
    run_silver_transformation()
