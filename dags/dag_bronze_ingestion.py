from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from dateutil.relativedelta import relativedelta

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

# Generate the last 12 months from today
today = datetime.today()
MONTHS = []
for i in range(1, 13):
    dt = today - relativedelta(months=i)
    MONTHS.append((str(dt.year), f"{dt.month:02d}"))

with DAG(
    dag_id="dag_bronze_ingestion",
    default_args=DEFAULT_ARGS,
    description="Bronze Layer: downloads the last 12 months of ANAC VRA data and ingests into Iceberg.",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["gru", "bronze", "anac", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # One task per month, chained sequentially (most recent first)
    previous_task = start
    for ano, mes in MONTHS:
        ingest_task = BashOperator(
            task_id=f"ingest_{ano}_{mes}",
            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"python spark_jobs/ingestion_vra.py --ano {ano} --mes {mes}"
            ),
            env={"GRU_BASE_DIR": PROJECT_DIR},
            do_xcom_push=False,
        )
        previous_task >> ingest_task
        previous_task = ingest_task

    previous_task >> end
