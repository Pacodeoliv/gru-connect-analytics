"""
DAG Bronze — ANAC VRA Ingestion
================================
Orquestra o download do CSV da ANAC e a geração da camada Bronze.
Executada mensalmente (os dados VRA são publicados mensalmente pela ANAC).

Schedule: mensal, no dia 15 de cada mês (quando a ANAC costuma liberar os dados).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PROJECT_DIR = os.environ.get("GRU_BASE_DIR", str(Path(__file__).parent.parent))

DEFAULT_ARGS = {
    "owner": "paco",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="dag_bronze_ingestion",
    default_args=DEFAULT_ARGS,
    description="Bronze Layer: download do VRA da ANAC e ingestão Parquet via PySpark.",
    schedule="0 8 15 * *",   # Todo dia 15 às 08h (UTC-3 → ~11h UTC)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["gru", "bronze", "anac", "spark"],
    params={
        "ano": Param(default="{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}", type="string",
                     description="Ano de referência VRA (ex: 2025)"),
        "mes": Param(default="{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}", type="string",
                     description="Mês de referência com zero-padding (ex: 01)"),
    },
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=(
            "cd {{ params.get('project_dir', '" + PROJECT_DIR + "') }} && "
            "poetry run python spark_jobs/ingestion_vra.py "
            "--ano {{ params.ano }} "
            "--mes {{ params.mes }}"
        ),
        env={
            "GRU_BASE_DIR": PROJECT_DIR,
            "ANAC_ANO": "{{ params.ano }}",
            "ANAC_MES": "{{ params.mes }}",
        },
        do_xcom_push=False,
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> ingest_bronze >> fim
