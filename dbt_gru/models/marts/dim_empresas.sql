{{ config(
    materialized='table',
    file_format='parquet',
    options={'path': env_var('GRU_BASE_DIR') ~ '/data/gold/dim_empresas'}
) }}

WITH empresas AS (
    SELECT DISTINCT cd_icao_empresa
    FROM {{ ref('stg_anac_vra') }}
    WHERE cd_icao_empresa IS NOT NULL
)

SELECT
    cd_icao_empresa,
    CASE cd_icao_empresa
        WHEN 'GLO' THEN 'Gol Linhas Aéreas'
        WHEN 'TAM' THEN 'LATAM Brasil'
        WHEN 'AZU' THEN 'Azul Linhas Aéreas'
        WHEN 'PTB' THEN 'VoePass'
        WHEN 'ONE' THEN 'ITA Transportes Aéreos'
        WHEN 'MAP' THEN 'MAP Linhas Aéreas'
        ELSE concat('Empresa - ', cd_icao_empresa)
    END AS nm_empresa,
    CASE cd_icao_empresa
        WHEN 'GLO' THEN 'Nacional'
        WHEN 'TAM' THEN 'Internacional'
        WHEN 'AZU' THEN 'Nacional'
        WHEN 'PTB' THEN 'Regional'
        ELSE 'Outro'
    END AS cd_tipo_empresa
FROM empresas