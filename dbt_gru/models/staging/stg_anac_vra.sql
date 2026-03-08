{{ config(materialized='view') }}

WITH source_data AS (
    -- Lê da tabela Iceberg criada pelo job silver_transformation.py
    SELECT * FROM local.silver.stg_anac_vra
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