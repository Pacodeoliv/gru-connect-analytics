from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'paco',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_gru_connect_analytics',
    default_args=default_args,
    description='Pipeline End-to-End: ANAC -> Spark -> dbt',
    schedule=None, # Roda uma vez por mês (quando sai o VRA novo)
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    # 1. Task Bronze
    ingestion_bronze = BashOperator(
        task_id='ingestion_bronze',
        bash_command='cd /home/pacod/github/gru-connect-analytics && poetry run python spark_jobs/ingestion_vra.py'
    )

    # 2. Task Silver
    transformation_silver = BashOperator(
        task_id='transformation_silver',
        bash_command='cd /home/pacod/github/gru-connect-analytics && poetry run python spark_jobs/silver_transformation.py'
    )

    # 3. Task Gold (dbt) 
    modeling_dbt_gold = BashOperator(
        task_id='modeling_dbt_gold',
        bash_command=(
        'export SPARK_CONF_DIR=/home/pacod/github/gru-connect-analytics/airflow && ' 
        'cd /home/pacod/github/gru-connect-analytics/dbt_gru && '
        'poetry run dbt run --profiles-dir . --full-refresh'
    )
    )

    # Definindo a Ordem de Execução
    ingestion_bronze >> transformation_silver >> modeling_dbt_gold