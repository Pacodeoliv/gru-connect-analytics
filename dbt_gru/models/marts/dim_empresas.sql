{{ config(
    materialized='table',
    file_format='parquet',
    options={'path': '/home/pacod/github/gru-connect-analytics/data/gold/dim_empresas'},
    partition_by=['dt_referencia'] if 'dt_referencia' in column_names else none
) }}

WITH empresas AS (
    SELECT DISTINCT 
        cd_icao_empresa 
    FROM {{ ref('stg_anac_vra') }}
)

SELECT
    cd_icao_empresa,
    CASE 
        WHEN cd_icao_empresa = 'GLO' THEN 'Gol Linhas Aéreas'
        WHEN cd_icao_empresa = 'TAM' THEN 'LATAM Brasil'
        WHEN cd_icao_empresa = 'AZU' THEN 'Azul Linhas Aéreas'
        WHEN cd_icao_empresa = 'PTB' THEN 'VoePass'
        ELSE concat('Empresa - ', cd_icao_empresa)
    END as nm_empresa
FROM empresas