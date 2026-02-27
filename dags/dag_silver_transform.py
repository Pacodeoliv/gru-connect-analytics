from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
    schedule="0 10 15 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["gru", "silver", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")

    wait_bronze = ExternalTaskSensor(
        task_id="wait_bronze",
        external_dag_id="dag_bronze_ingestion",
        external_task_id="end",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "poetry run python spark_jobs/silver_transformation.py"
        ),
        env={"GRU_BASE_DIR": PROJECT_DIR},
        do_xcom_push=False,
    )

    end = EmptyOperator(task_id="end")

    start >> wait_bronze >> transform_silver >> end
