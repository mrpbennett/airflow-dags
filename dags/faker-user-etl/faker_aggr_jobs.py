from datetime import datetime, timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

# === DAG definition ===
local_tz = pendulum.timezone("Europe/London")


@dag(
    dag_id="user_data_aggregation",
    description="Breaking down fact user data into aggregated tables",
    default_args={
        "owner": "data-eng",
        "depends_on_past": False,
        "start_date": local_tz.convert(datetime(2025, 6, 26, 2, 0)),
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    schedule_interval="0 2 * * *",  # daily at 02:00
    catchup=False,
    max_active_runs=1,
    tags=["aggregation", "daily"],
)
def faker_aggr_jobs():
    @task
    def aggr_user_address_job():
        query = """
          INSERT INTO aggr.users_address (
            uuid, street, city, state, zip_code, country, created_at
          )
          SELECT
            uuid, street, city, state, zip_code, country, created_at
          FROM fact.users
          WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
      """
        try:
            hook = PostgresHook(postgres_conn_id="cnpg_cluster")
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
            uuid, ip, user_agent, mac_address, token, created_at
          )
          SELECT
            uuid, ip, user_agent, mac_address, token, created_at
          FROM fact.users
          WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
      """
        try:
            hook = PostgresHook(postgres_conn_id="cnpg_cluster")
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
          INSERT INTO aggr.users_contact (
            uuid, first_name, last_name, email, phone_number, created_at
          )
          SELECT
            uuid, first_name, last_name, email, phone_number, created_at
          FROM fact.users
          WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
      """
        try:
            hook = PostgresHook(postgres_conn_id="cnpg_cluster")
            conn = hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            return 0
        except Exception as e:
            print(f"Error during aggr_user_contact_job aggregation: {e}")
            return 1

    @task
    def aggr_user_jobdetail_job():
        query = """
          INSERT INTO aggr.users_job (
            uuid, job_title, job_area, job_type, created_at
          )
          SELECT
            uuid, job_title, job_area, job_type, created_at
          FROM fact.users
          WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
      """
        try:
            hook = PostgresHook(postgres_conn_id="cnpg_cluster")
            conn = hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            return 0
        except Exception as e:
            print(f"Error during aggr_user_jobdetail_job aggregation: {e}")
            return 1

    (
        aggr_user_address_job()
        >> aggr_user_device_job()
        >> aggr_user_contact_job()
        >> aggr_user_jobdetail_job()
    )  # type: ignore


dag = faker_aggr_jobs()
