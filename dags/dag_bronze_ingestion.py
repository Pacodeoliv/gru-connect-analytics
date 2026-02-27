from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow import DAG

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
    description="Bronze Layer: ANAC VRA download and Iceberg ingestion via PySpark.",
    schedule="0 8 15 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["gru", "bronze", "anac", "spark"],
    params={
        "ano": Param(default="{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}", type="string",
                     description="Reference year (e.g. 2025)"),
        "mes": Param(default="{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}", type="string",
                     description="Reference month with zero-padding (e.g. 01)"),
    },
) as dag:

    start = EmptyOperator(task_id="start")

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

    end = EmptyOperator(task_id="end")

    start >> ingest_bronze >> end
