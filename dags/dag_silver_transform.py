"""
DAG Silver — Spark Transformation
===================================
Lê a camada Bronze e aplica transformações (tipagem, limpeza, atraso) na Silver.
Disparada automaticamente após a conclusão bem-sucedida da DAG Bronze.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
    description="Silver Layer: limpeza e tipagem dos dados de voos GRU via PySpark.",
    schedule="0 10 15 * *",  # 2h após a Bronze (tempo suficiente para completar)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["gru", "silver", "spark"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    # Aguarda a DAG Bronze do mesmo execution_date completar com sucesso
    aguardar_bronze = ExternalTaskSensor(
        task_id="aguardar_bronze",
        external_dag_id="dag_bronze_ingestion",
        external_task_id="fim",
        timeout=3600,          # Máximo de 1h aguardando a Bronze
        poke_interval=60,
        mode="reschedule",     # Não bloqueia um worker slot enquanto espera
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

    fim = EmptyOperator(task_id="fim")

    inicio >> aguardar_bronze >> transform_silver >> fim
