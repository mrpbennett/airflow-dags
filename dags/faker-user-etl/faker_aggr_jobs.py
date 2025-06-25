from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
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

    @task()
    def aggr_address_job():
        hook = PostgresHook(postgres_conn_id="cnpg_cluster")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
          INSERT INTO aggr.users_address (
            uuid, street, city, state, zip_code, country, created_at
          )
          SELECT
            uuid, street, city, state, zip_code, country, created_at
          FROM fact.users
          WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
      """
        )
        result = cursor.fetchone()[0]
        print(
            f"Address aggregation job completed successfully. Rows affected: {result}"
        )

    @task()
    def aggr_device_job():
        hook = PostgresHook(postgres_conn_id="cnpg_cluster")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
          INSERT INTO aggr.users_device (
            uuid, ip, user_agent, mac_address, token, created_at
          )
          SELECT
            uuid, ip, user_agent, mac_address, token, created_at
          FROM fact.users
          WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
      """
        )
        result = cursor.fetchone()[0]
        print(f"Device aggregation job completed successfully. Rows affected: {result}")

    @task()
    def aggr_contact_job():
        hook = PostgresHook(postgres_conn_id="cnpg_cluster")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
          INSERT INTO aggr.users_contact (
            uuid, first_name, last_name, email, phone_number, created_at
          )
          SELECT
            uuid, first_name, last_name, email, phone_number, created_at
          FROM fact.users
          WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
      """
        )
        result = cursor.fetchone()[0]
        print(
            f"Contact aggregation job completed successfully. Rows affected: {result}"
        )

    @task()
    def aggr_jobdetail_job():
        hook = PostgresHook(postgres_conn_id="cnpg_cluster")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
          INSERT INTO aggr.users_job (
            uuid, job_title, job_area, job_type, created_at
          )
          SELECT
            uuid, job_title, job_area, job_type, created_at
          FROM fact.users
          WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
      """
        )
        result = cursor.fetchone()[0]
        print(
            f"Job-detail aggregation job completed successfully. Rows affected: {result}"
        )

        aggr_address_job()
        aggr_device_job()
        aggr_contact_job()
        aggr_jobdetail_job()


faker_aggr_jobs()
