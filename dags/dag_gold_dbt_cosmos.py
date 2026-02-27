"""
DAG Gold — dbt via Astronomer Cosmos
======================================
Orquestra a camada Gold usando o Astronomer Cosmos, que transforma cada
model dbt em uma task individual do Airflow — com linhagem, retries e logs
separados por model.

Dependências:
    pip install astronomer-cosmos apache-airflow-providers-common-sql

Referência: https://astronomer.github.io/astronomer-cosmos/
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

try:
    from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
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
    description="Gold Layer: modelagem dbt via Astronomer Cosmos (cada model = task Airflow).",
    schedule="0 12 15 * *",  # 2h após a Silver
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["gru", "gold", "dbt", "cosmos"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    # Aguarda a Silver do mesmo execution_date completar
    aguardar_silver = ExternalTaskSensor(
        task_id="aguardar_silver",
        external_dag_id="dag_silver_transform",
        external_task_id="fim",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    fim = EmptyOperator(task_id="fim")

    if COSMOS_AVAILABLE:
        # ── Cosmos: cada model dbt vira uma task com linhagem automática ─────
        # A ordem de execução reflete o grafo de dependências do dbt:
        # stg_anac_vra → dim_aeroportos + dim_empresas + dim_calendario → fato_conexoes
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
                # Roda apenas staging + marts (exclui análises e snapshots)
                select=["path:models/staging", "path:models/marts"],
                dbt_executable_path="poetry",
            ),
            operator_args={
                "env": {"GRU_BASE_DIR": str(PROJECT_DIR)},
                "install_deps": True,
            },
        )

        inicio >> aguardar_silver >> dbt_gold >> fim

    else:
        # ── Fallback: BashOperator enquanto o Cosmos não está instalado ──────
        # Para ativar o Cosmos: pip install astronomer-cosmos
        from airflow.operators.bash import BashOperator

        dbt_run_fallback = BashOperator(
            task_id="dbt_run_gold_fallback",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                "poetry run dbt run --profiles-dir . --select models/staging+ --full-refresh"
            ),
            env={"GRU_BASE_DIR": str(PROJECT_DIR)},
        )

        inicio >> aguardar_silver >> dbt_run_fallback >> fim
