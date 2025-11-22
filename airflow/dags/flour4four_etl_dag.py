# ~/airflow-local/airflow/dags/flour4four_etl_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# --- CONFIG
VENV_PYTHON = "/Users/daniel/Downloads/Flour4Four/.venv/bin/python"
PROJECT_DIR = "/Users/daniel/Downloads/Flour4Four"

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flour4four_etl",
    default_args=default_args,
    start_date=datetime(2025, 11, 22),
    schedule="@daily",
    catchup=False,
    tags=["etl", "flour4four"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f"{VENV_PYTHON} {PROJECT_DIR}/extract.py ",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=f"{VENV_PYTHON} {PROJECT_DIR}/transform.py ",
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"{VENV_PYTHON} {PROJECT_DIR}/load.py ",
    )

    extract >> transform >> load