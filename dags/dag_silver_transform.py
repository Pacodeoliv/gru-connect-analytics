from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow import DAG

PROJECT_DIR = os.environ.get("GRU_BASE_DIR", str(Path(__file__).parent.parent))

DEFAULT_ARGS = {
    "owner": "paco",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_silver_transform",
    default_args=DEFAULT_ARGS,
    description="Silver Layer: typing and cleaning of GRU flight data via PySpark.",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["gru", "silver", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python spark_jobs/silver_transformation.py"
        ),
        env={"GRU_BASE_DIR": PROJECT_DIR},
        do_xcom_push=False,
    )

    end = EmptyOperator(task_id="end")

    start >> transform_silver >> end
