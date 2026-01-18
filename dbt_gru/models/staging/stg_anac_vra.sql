{{ config(materialized='view') }}

WITH source_data AS (
    -- Em Spark local, lemos o caminho físico entre aspas simples e prefixo parquet
    SELECT * FROM parquet.`/home/pacod/github/gru-connect-analytics/data/silver/stg_anac_vra`
)

SELECT
    cd_icao_empresa,
    nr_voo,
    cd_icao_origem,
    cd_icao_destino,
    dt_partida_prevista,
    dt_partida_real,
    dt_chegada_prevista,
    dt_chegada_real,
    nm_situacao_voo,
    vl_atraso_chegada_min
FROM source_data