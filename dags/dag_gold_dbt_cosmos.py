from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow import DAG

try:
    from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
    from cosmos.profiles import SparkThriftProfileMapping
    COSMOS_AVAILABLE = True
except ImportError:
    COSMOS_AVAILABLE = False

PROJECT_DIR = Path(os.environ.get("GRU_BASE_DIR", Path(__file__).parent.parent))
DBT_PROJECT_DIR = PROJECT_DIR / "dbt_gru"

DEFAULT_ARGS = {
    "owner": "paco",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_gold_dbt_cosmos",
    default_args=DEFAULT_ARGS,
    description="Gold Layer: dbt models via Astronomer Cosmos (each model = Airflow task).",
    schedule="0 12 15 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["gru", "gold", "dbt", "cosmos"],
) as dag:

    start = EmptyOperator(task_id="start")

    wait_silver = ExternalTaskSensor(
        task_id="wait_silver",
        external_dag_id="dag_silver_transform",
        external_task_id="end",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    end = EmptyOperator(task_id="end")

    if COSMOS_AVAILABLE:
        dbt_gold = DbtTaskGroup(
            group_id="dbt_gold",
            project_config=ProjectConfig(
                dbt_project_path=str(DBT_PROJECT_DIR),
            ),
            profile_config=ProfileConfig(
                profile_name="gru_connect",
                target_name="dev",
                profile_mapping=SparkThriftProfileMapping(
                    conn_id="spark_default",
                    profile_args={
                        "schema": "gold",
                        "threads": 4,
                    },
                ),
            ),
            render_config=RenderConfig(
                select=["path:models/staging", "path:models/marts"],
                dbt_executable_path="dbt",
            ),
            operator_args={
                "env": {"GRU_BASE_DIR": str(PROJECT_DIR)},
                "install_deps": True,
            },
        )

        start >> wait_silver >> dbt_gold >> end

    else:
        from airflow.operators.bash import BashOperator

        dbt_run_fallback = BashOperator(
            task_id="dbt_run_gold_fallback",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                "dbt run --profiles-dir . --select models/staging+ --full-refresh"
            ),
            env={"GRU_BASE_DIR": str(PROJECT_DIR)},
        )

        start >> wait_silver >> dbt_run_fallback >> end
