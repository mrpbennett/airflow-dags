import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.airflow_utils.cnpg_contextmgr import query_db


def aggr_address_job():
    query = """
        INSERT INTO aggr.users_address (
          uuid, street, city, state, zip_code, country, created_at
        )
        SELECT
          uuid, street, city, state, zip_code, country, created_at
        FROM fact.users
        WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
    """
    with query_db(query) as curr:
        logging.info("Address aggregation job completed successfully.")


def aggr_device_job():
    query = """
        INSERT INTO aggr.users_device (
          uuid, ip, user_agent, mac_address, token, created_at
        )
        SELECT
          uuid, ip, user_agent, mac_address, token, created_at
        FROM fact.users
        WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
    """
    with query_db(query) as curr:
        logging.info("Device aggregation job completed successfully.")


def aggr_contact_job():
    query = """
        INSERT INTO aggr.users_contact (
          uuid, first_name, last_name, email, phone_number, created_at
        )
        SELECT
          uuid, first_name, last_name, email, phone_number, created_at
        FROM fact.users
        WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
    """
    with query_db(query) as curr:
        logging.info("Contact aggregation job completed successfully.")


def aggr_jobdetail_job():
    query = """
        INSERT INTO aggr.users_job (
          uuid, job_title, job_area, job_type, created_at
        )
        SELECT
          uuid, job_title, job_area, job_type, created_at
        FROM fact.users
        WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
    """
    with query_db(query) as curr:
        logging.info("Job-detail aggregation job completed successfully.")


# === DAG definition ===
local_tz = pendulum.timezone("Europe/London")

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "start_date": local_tz.convert(datetime(2025, 6, 26, 2, 0)),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="user_data_aggregation",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # daily at 02:00
    catchup=False,
    max_active_runs=1,
    tags=["aggregation", "daily"],
    timezone="Europe/London",
) as dag:

    aggregate_address = PythonOperator(
        task_id="aggregate_address",
        python_callable=aggr_address_job,
    )

    aggregate_device = PythonOperator(
        task_id="aggregate_device",
        python_callable=aggr_device_job,
    )

    aggregate_contact = PythonOperator(
        task_id="aggregate_contact",
        python_callable=aggr_contact_job,
    )

    aggregate_jobdetail = PythonOperator(
        task_id="aggregate_job_detail",
        python_callable=aggr_jobdetail_job,
    )

    # If these can run in parallel, leave them unchained:
    [aggregate_address, aggregate_device, aggregate_contact, aggregate_jobdetail]
