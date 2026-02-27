{{ config(
    materialized='table',
    file_format='parquet',
    options={'path': env_var('GRU_BASE_DIR') ~ '/data/gold/fato_conexoes'},
    partition_by=['nm_situacao_voo']
) }}

WITH voos AS (
    SELECT * FROM {{ ref('stg_anac_vra') }}
    WHERE nm_situacao_voo = 'REALIZADO'
),

conexoes AS (
    SELECT
        v1.cd_icao_empresa,
        v1.nr_voo                                          AS nr_voo_chegada,
        v2.nr_voo                                          AS nr_voo_partida,
        v1.cd_icao_origem                                  AS cd_icao_origem_chegada,
        v2.cd_icao_destino                                 AS cd_icao_destino_partida,
        v1.dt_chegada_real,
        v2.dt_partida_prevista,
        (unix_timestamp(v2.dt_partida_prevista)
         - unix_timestamp(v1.dt_chegada_real)) / 60        AS janela_conexao_min
    FROM voos v1
    INNER JOIN voos v2
        ON  v1.cd_icao_empresa   = v2.cd_icao_empresa
        AND v1.cd_icao_destino   = 'SBGR'
        AND v2.cd_icao_origem    = 'SBGR'
        AND v2.dt_partida_prevista > v1.dt_chegada_real
    -- Janela entre 30 e 240 min — filtra ruídos (conexões impossíveis ou sobra de tempo excessiva)
    WHERE (unix_timestamp(v2.dt_partida_prevista)
           - unix_timestamp(v1.dt_chegada_real)) / 60 BETWEEN 30 AND 240
)

SELECT
    *,
    CASE
        WHEN janela_conexao_min < 60  THEN 'Risco Crítico'
        WHEN janela_conexao_min < 90  THEN 'Risco Médio'
        ELSE                               'Seguro'
    END AS desc_status_risco,
    current_timestamp() AS dt_processamento
FROM conexoes