# File: dags/count_rows.py

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def count_rows_in_table():
    hook = PostgresHook(postgres_conn_id="cnpg_cluster")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM faker_users;")
    result = cursor.fetchone()[0]
    print(f"Row count in faker_users: {result}")


with DAG(
    dag_id="example_row_count",
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    catchup=False,
) as dag:

    count_task = PythonOperator(
        task_id="count_rows",
        python_callable=count_rows_in_table,
    )
