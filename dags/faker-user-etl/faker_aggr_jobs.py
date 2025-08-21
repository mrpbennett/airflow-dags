from datetime import datetime, timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

# === DAG definition ===
local_tz = pendulum.timezone("Europe/London")


@dag(
    dag_id="faker_user_aggregation",
    description="Breaking down fact user data into aggregated tables",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
    },
    schedule="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=["aggregation", "daily"],
)
def faker_aggr_jobs():
    @task
    def aggr_user_address_job():
        query = """
          INSERT INTO aggr.user_location (
            uuid, address, state 
          )
          SELECT
            uuid, address, state 
          FROM fact.users
          WHERE ts >= CURRENT_DATE - INTERVAL '1 day'
      """
        try:
            hook = PostgresHook(postgres_conn_id="cnpg")
            conn = hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            return 0
        except Exception as e:
            print(f"Error during aggr_user_address_job aggregation: {e}")
            return 1

    @task
    def aggr_user_device_job():
        query = """
          INSERT INTO aggr.users_device (
            uuid, ip, user_agent
          )
          SELECT
            uuid, ip, user_agent
          FROM fact.users
          WHERE ts >= CURRENT_DATE - INTERVAL '1 day'
      """
        try:
            hook = PostgresHook(postgres_conn_id="cnpg")
            conn = hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            return 0
        except Exception as e:
            print(f"Error during aggr_user_device_job aggregation: {e}")
            return 1

    @task
    def aggr_user_contact_job():
        query = """
          INSERT INTO aggr.users_info (
            uuid, first_name, last_name, email, address
          )
          SELECT
            uuid, first_name, last_name, email, address
          FROM fact.users
          WHERE ts >= CURRENT_DATE - INTERVAL '1 day'
      """
        try:
            hook = PostgresHook(postgres_conn_id="cnpg")
            conn = hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            return 0
        except Exception as e:
            print(f"Error during aggr_user_contact_job aggregation: {e}")
            return 1

    aggr_user_address_job()
    aggr_user_device_job()
    aggr_user_contact_job()


dag = faker_aggr_jobs()
