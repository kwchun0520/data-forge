import sys
import os
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/src')

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

try:
    from data_forge.scripts.load_weather_data import load_weather_data
except ImportError:
    def load_weather_data(layer, table):
        print(f"Loading {layer}.{table}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
    "catchup": False,
}

dag = DAG(
    dag_id="weather_data_pipeline_2",
    default_args=default_args,
    description="Weather data pipeline with medallion architecture",
    schedule=timedelta(hours=1),
    max_active_runs=1,
)

with dag:
    # Extract: Load raw data
    extract_weather = PythonOperator(
        task_id="extract_weather_data",
        python_callable=load_weather_data,
        op_kwargs={
            "layer": "source",
            "table": "weather_data"
        },
        execution_timeout=timedelta(minutes=15),
    )

    # dbt Debug - Test connection
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command="docker exec dbt_container dbt debug",
    )

    # dbt Parse - Parse project
    dbt_parse = BashOperator(
        task_id="dbt_parse",
        bash_command="docker exec dbt_container dbt parse",
    )

    # dbt Run Staging
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="docker exec dbt_container dbt run --select staging",
        execution_timeout=timedelta(minutes=15),
    )

    # dbt Run Mart
    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command="docker exec dbt_container dbt run --select mart",
        execution_timeout=timedelta(minutes=15),
    )

    # dbt Test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="docker exec dbt_container dbt test",
        execution_timeout=timedelta(minutes=15),
    )

    # dbt Docs Generate
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="docker exec dbt_container dbt docs generate",
        execution_timeout=timedelta(minutes=15),
    )

    # Set task dependencies
    extract_weather >> dbt_debug >> dbt_parse >> dbt_run_staging >> dbt_run_mart >> dbt_test >> dbt_docs_generate