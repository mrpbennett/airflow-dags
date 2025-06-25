from airflow import DAG
from airflow.operators.python import PythonOperator

from src.utils.cnpg_contextmgr import query_db


def cleanup_faker_tables():
    query = """
        DELETE from fact.users
        WHERE created_at::timestamp < CURRENT_DATE - INTERVAL '7 days';
            
        DELETE from aggr.users_address
        WHERE created_at::timestamp < CURRENT_DATE - INTERVAL '45 days';
            
        DELETE from aggr.users_contact
        WHERE created_at::timestamp < CURRENT_DATE - INTERVAL '45 days';
            
        DELETE from aggr.users_device
        WHERE created_at::timestamp < CURRENT_DATE - INTERVAL '45 days';
            
        DELETE from aggr.users_job
        WHERE created_at::timestamp < CURRENT_DATE - INTERVAL '45 days';
    """
    try:
        with query_db(query) as curr:
            print("Cleanup aggregation job completed successfully.")
    except Exception as err:
        raise err


# == DAG Definition ===

default_args = {
    "owner": "airflow",
    "start_date": "2025-01-01",
    "retries": 1,
    "retry_delay": 300,  # 5 minutes
}

with DAG(
    dag_id="faker_table_cleanup",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # daily at 03:00
    catchup=False,
    max_active_runs=1,
    tags=["cleanup"],
) as dag:

    cleanup_task = PythonOperator(
        task_id="cleanup_faker_tables",
        python_callable=cleanup_faker_tables,
    )
