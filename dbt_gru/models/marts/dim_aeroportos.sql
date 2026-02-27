{{ config(
    materialized='table',
    file_format='parquet',
    options={'path': env_var('GRU_BASE_DIR') ~ '/data/gold/dim_aeroportos'}
) }}

WITH aeroportos AS (
    SELECT DISTINCT cd_icao_origem AS cd_icao FROM {{ ref('stg_anac_vra') }}
    UNION
    SELECT DISTINCT cd_icao_destino AS cd_icao FROM {{ ref('stg_anac_vra') }}
),

enriquecido AS (
    SELECT
        cd_icao,
        CASE cd_icao
            WHEN 'SBGR' THEN 'Guarulhos'
            WHEN 'SBRJ' THEN 'Santos Dumont'
            WHEN 'SBGL' THEN 'Galeão'
            WHEN 'SBSP' THEN 'Congonhas'
            WHEN 'SBPA' THEN 'Salgado Filho'
            WHEN 'SBKP' THEN 'Viracopos'
            WHEN 'SBSV' THEN 'Dep. Luís Eduardo Magalhães'
            WHEN 'SBFZ' THEN 'Pinto Martins'
            WHEN 'SBRF' THEN 'Guararapes'
            WHEN 'SBBR' THEN 'Brasília'
            ELSE concat('Aeroporto - ', cd_icao)
        END AS nm_aeroporto,
        CASE cd_icao
            WHEN 'SBGR' THEN 'São Paulo'
            WHEN 'SBRJ' THEN 'Rio de Janeiro'
            WHEN 'SBGL' THEN 'Rio de Janeiro'
            WHEN 'SBSP' THEN 'São Paulo'
            WHEN 'SBKP' THEN 'Campinas'
            WHEN 'SBPA' THEN 'Porto Alegre'
            WHEN 'SBSV' THEN 'Salvador'
            WHEN 'SBFZ' THEN 'Fortaleza'
            WHEN 'SBRF' THEN 'Recife'
            WHEN 'SBBR' THEN 'Brasília'
            ELSE 'Outros'
        END AS nm_cidade,
        CASE
            WHEN cd_icao = 'SBGR' THEN true
            ELSE false
        END AS fl_hub_gru
    FROM aeroportos
)

SELECT * FROM enriquecido