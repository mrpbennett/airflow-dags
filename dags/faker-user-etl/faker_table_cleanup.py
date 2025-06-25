from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def cleanup_faker_tables():
    hook = PostgresHook(postgres_conn_id="cnpg_cluster")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
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
    )
    result = cursor.fetchone()[0]
    print(f"Address aggregation job completed successfully. Rows affected: {result}")


# == DAG Definition ===


with DAG(
    dag_id="faker_table_cleanup",
    description="Removing old data from faker tables",
    default_args={
        "owner": "airflow",
        "start_date": "2025-01-01",
        "retries": 1,
        "retry_delay": 300,  # 5 minutes
    },
    schedule_interval="0 3 * * *",  # daily at 03:00
    catchup=False,
    max_active_runs=1,
    tags=["cleanup", "daily"],
) as dag:

    cleanup_task = PythonOperator(
        task_id="cleanup_faker_tables",
        python_callable=cleanup_faker_tables,
    )
