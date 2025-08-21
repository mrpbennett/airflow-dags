from json import load
from requests.sessions import extract_cookies_to_jar
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
import pendulum
import random
import requests
import pandas as pd
import numpy as np
import hmac
import hashlib
from sqlalchemy import create_engine

local_tz = pendulum.timezone("Europe/London")
key = Variable.get("email_hash_key").encode("utf-8")

""" Helper functions """


def return_faker_data() -> list:
    random_num: int = random.randint(1, 1000)
    res = requests.get(f"http://192.168.7.52/api/users?count={random_num}")
    res.raise_for_status()

    if res.status_code != 200:
        raise Exception(f"Failed to fetch faker data: {res.status_code} - {res.text}")

    data = res.json()
    return data["data"]


def hash_email(email: str, key: bytes) -> str:
    """Hash the email using HMAC with SHA256."""
    return hmac.new(key, email.lower().encode("utf-8"), hashlib.sha256).hexdigest()


"""
Example response from the faker API:
{
  "status": "success",
  "message": "Generated 1 users successfully",
  "data": [
    {
      "uuid": "0cb72d86-e80e-4d25-a69b-b46973592856",
      "firstName": "Odie",
      "lastName": "Terry",
      "sex": "female",
      "email": "Cruz25@yahoo.com",
      "phone": "432.875.9428 x574",
      "address": "45170 Aurelia Lodge",
      "state": "New Jersey",
      "ip": "185.171.204.167",
      "userAgent": "Mozilla/5.0 (Windows NT 5.2; Win64; x64) AppleWebKit/554.40 (KHTML, like Gecko) Chrome/121.8.0.5 Safari/554.51 Edg/124.5.1.19",
      "ts": "2025-08-21"
    }
  ]
}
"""


@dag(
    dag_id="faker_data_ingestion",
    description="Ingesting fake user data into the fact table",
    schedule="0 1 * * *",  # Daily at midnight
    tags=["data_ingestion"],
)
def faker_data_ingestion():
    """DAG to ingest fake user data into a PostgreSQL database."""

    @task()
    def extract() -> pd.DataFrame:
        """Extract data from the Faker API and return it as a DataFrame."""
        data = return_faker_data()

        try:
            df = pd.read_json(data)
        except ValueError:
            # if data is already a dict/list normalize it
            df = pd.json_normalize(data)

        df = df.drop(columns=["sex", "phone"], errors="ignore")
        return df

    @task()
    def hashed_email(df: pd.DataFrame) -> pd.DataFrame:
        """Hash the email column using the provided key."""
        df = df.copy()
        df["email"] = df["email"].apply(lambda x: hash_email(email=x, key=key))
        return df

    @task()
    def load_into_table(df: pd.DataFrame):
        """Load the DataFrame into the PostgreSQL table."""

        pg_hook = PostgresHook(postgres_conn_id="cnpg")

        try:
            engine = pg_hook.get_sqlalchmey_engine()
        except AttributeError:
            engine = create_engine(pg_hook.get_uri())

        df.to_sql(
            name="fakerjs",
            con=engine,
            schema="fact",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

    extracted = extract()
    transformed = hashed_email(extracted)
    load_into_table(transformed)


dag = faker_data_ingestion()
