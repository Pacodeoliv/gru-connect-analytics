from pyspark.sql import SparkSession
import os

# Caminho para a sua tabela fato na Gold
GOLD_PATH = "/home/pacod/github/gru-connect-analytics/data/gold/fato_conexoes"

def inspect_data():
    spark = SparkSession.builder \
        .appName("Inspect-Gold-Data") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    if os.path.exists(GOLD_PATH):
        print(f"--- Lendo dados de: {GOLD_PATH} ---")
        df = spark.read.parquet(GOLD_PATH)
        
        # Mostra as colunas e os top 10 registros
        print(f"Total de conexões mapeadas: {df.count()}")
        
        print("\nAmostra de Conexões e Riscos:")
        df.select(
            "cd_icao_empresa", 
            "nr_voo_chegada", 
            "nr_voo_partida", 
            "janela_conexao_min", 
            "desc_status_risco"
        ).orderBy("janela_conexao_min").show(10)
        
        # Agrupamento para ver a saúde da operação em GRU
        print("\nResumo por Status de Risco:")
        df.groupBy("desc_status_risco").count().show()
        
    else:
        print("Erro: Pasta Gold não encontrada. Verifique o caminho.")

if __name__ == "__main__":
    inspect_data()