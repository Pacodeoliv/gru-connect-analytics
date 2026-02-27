{{ config(
    materialized='table',
    file_format='parquet',
    options={'path': env_var('GRU_BASE_DIR') ~ '/data/gold/dim_calendario'}
) }}

WITH datas AS (
    SELECT DISTINCT
        to_date(dt_partida_prevista) AS data_referencia
    FROM {{ ref('stg_anac_vra') }}
    WHERE dt_partida_prevista IS NOT NULL
)

SELECT
    data_referencia                                             AS sk_data,
    year(data_referencia)                                       AS nr_ano,
    month(data_referencia)                                      AS nr_mes,
    day(data_referencia)                                        AS nr_dia,
    quarter(data_referencia)                                    AS nr_trimestre,
    date_format(data_referencia, 'EEEE')                        AS nm_dia_semana,
    date_format(data_referencia, 'MMMM')                        AS nm_mes,
    -- Flag de fim de semana (útil para análise de conexões operacionais)
    CASE WHEN dayofweek(data_referencia) IN (1, 7) THEN true ELSE false END AS fl_fim_de_semana,
    -- Número da semana no ano (para análise de sazonalidade)
    weekofyear(data_referencia)                                 AS nr_semana_ano
FROM datas
ORDER BY data_referencia