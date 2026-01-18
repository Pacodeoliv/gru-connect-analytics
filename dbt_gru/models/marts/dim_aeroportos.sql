{{ config(
    materialized='table',
    file_format='parquet',
    options={'path': '/home/pacod/github/gru-connect-analytics/data/gold/dim_aeroportos'},
    partition_by=['dt_referencia'] if 'dt_referencia' in column_names else none
) }}

WITH aeroportos AS (
    SELECT DISTINCT cd_icao_origem as cd_icao FROM {{ ref('stg_anac_vra') }}
    UNION
    SELECT DISTINCT cd_icao_destino as cd_icao FROM {{ ref('stg_anac_vra') }}
)

SELECT
    cd_icao,
    CASE 
        WHEN cd_icao = 'SBGR' THEN 'Guarulhos'
        WHEN cd_icao = 'SBRJ' THEN 'Santos Dumont'
        WHEN cd_icao = 'SBGL' THEN 'Galeão'
        WHEN cd_icao = 'SBSP' THEN 'Congonhas'
        ELSE 'Outros' 
    END as nm_aeroporto,
    CASE 
        WHEN cd_icao = 'SBGR' THEN 'São Paulo'
        WHEN cd_icao = 'SBRJ' THEN 'Rio de Janeiro'
        ELSE 'Interior/Outros'
    END as nm_cidade
FROM aeroportos