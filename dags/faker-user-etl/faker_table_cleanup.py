from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task


@dag(
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
)
def cleanup_faker_tables():
    @task
    def cleanup_fact_users_table():
        query = """
            DELETE FROM fact.users
            WHERE created_at::timestamp < CURRENT_DATE - INTERVAL '7 days';
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="cnpg_cluster")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            return 0
        except Exception as e:
            print(f"Error during cleanup: {e}")
            return 1

    @task
    def cleanup_aggr_users_address_table():
        query = """
            DELETE FROM aggr.users_address
            WHERE created_at::timestamp < CURRENT_DATE - INTERVAL '45 days';
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="cnpg_cluster")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            return 0
        except Exception as e:
            print(f"Error during cleanup: {e}")
            return 1

    @task
    def cleanup_aggr_users_contact_table():
        query = """
            DELETE FROM aggr.users_contact
            WHERE created_at::timestamp < CURRENT_DATE - INTERVAL '45 days';
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="cnpg_cluster")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            return 0
        except Exception as e:
            print(f"Error during cleanup: {e}")
            return 1

    @task
    def cleanup_aggr_users_device_table():
        query = """
            DELETE FROM aggr.users_device
            WHERE created_at::timestamp < CURRENT_DATE - INTERVAL '45 days';
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="cnpg_cluster")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            return 0
        except Exception as e:
            print(f"Error during cleanup: {e}")
            return 1

    @task
    def cleanup_aggr_users_job_table():
        query = """
            DELETE FROM aggr.users_job
            WHERE created_at::timestamp < CURRENT_DATE - INTERVAL '45 days';
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="cnpg_cluster")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            return 0
        except Exception as e:
            print(f"Error during cleanup: {e}")
            return 1

    (
        cleanup_fact_users_table()
        >> cleanup_aggr_users_address_table()
        >> cleanup_aggr_users_contact_table()
        >> cleanup_aggr_users_device_table()
        >> cleanup_aggr_users_job_table()
    )  # type: ignore


dag = cleanup_faker_tables()
