{{ config(
    materialized='table',
    file_format='parquet',
    options={'path': '/home/pacod/github/gru-connect-analytics/data/gold/dim_calendario'},
    partition_by=['dt_referencia'] if 'dt_referencia' in column_names else none
) }}

WITH datas AS (
    -- Geramos uma sequência de datas baseada nos voos que temos
    SELECT DISTINCT 
        to_date(dt_partida_prevista) as data_referencia
    FROM {{ ref('stg_anac_vra') }}
)

SELECT
    data_referencia as sk_data,
    year(data_referencia) as nr_ano,
    month(data_referencia) as nr_mes,
    day(data_referencia) as nr_dia,
    date_format(data_referencia, 'EEEE') as nm_dia_semana,
    quarter(data_referencia) as nr_trimestre
FROM datas