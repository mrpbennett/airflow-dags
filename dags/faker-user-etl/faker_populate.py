import json
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
import logging


# Common utility functions
def get_pg_engine(pg_hook: PostgresHook):
    """Get a SQLAlchemy engine from the PostgresHook."""
    try:
        engine = pg_hook.get_sqlalchmey_engine()
        logger.info("SQLAlchemy engine successfully obtained from PostgresHook.")
    except AttributeError:
        logger.warning(
            "PostgresHook does not support get_sqlalchmey_engine. Falling back to create_engine."
        )
        engine = create_engine(pg_hook.get_uri())
    return engine


def check_table_exists(engine, schema: str, table_name: str):
    """Check if a table exists in the specified schema."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table_name}';"
            )
            if result.rowcount == 0:
                logger.error(
                    f"Table '{table_name}' does not exist in schema '{schema}'. Aborting operation."
                )
                raise ValueError(
                    f"Table '{table_name}' does not exist in schema '{schema}'."
                )
            logger.info(
                f"Verified that the table '{table_name}' exists in schema '{schema}'."
            )
    except Exception as e:
        logger.error(f"Error while verifying table existence: {e}")
        raise


local_tz = pendulum.timezone("Europe/London")
key = Variable.get("email_hash_key").encode("utf-8")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

""" Helper functions """


def return_faker_data() -> list:
    random_num: int = random.randint(1, 2)
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
        logger.info("Starting data extraction from the Faker API.")
        data = json.dumps(return_faker_data())
        print(json.dumps(data, indent=4))

        df = df.drop(columns=["sex", "phone"], errors="ignore")
        print(df.head(10))

        logger.info("DataFrame columns filtered. Returning DataFrame.")
        return df

    @task()
    def hashed_email(df: pd.DataFrame) -> pd.DataFrame:
        """Hash the email column using the provided key."""
        logger.info("Starting email hashing transformation.")
        df = df.copy()
        df["email"] = df["email"].apply(lambda x: hash_email(email=x, key=key))
        logger.info("Email hashing completed.")
        return df

    @task()
    def load_into_table(df: pd.DataFrame):
        """Load the DataFrame into the PostgreSQL table."""

        logger.info("Starting data load into PostgreSQL table.")

        if df.empty:
            logger.error("DataFrame is empty. Aborting load.")
            raise ValueError("DataFrame is empty. Cannot load into database.")

        required_columns = [
            "uuid",
            "firstName",
            "lastName",
            "email",
            "address",
            "state",
            "ip",
            "userAgent",
            "ts",
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}. Aborting load.")
            raise ValueError(
                f"DataFrame is missing required columns: {missing_columns}"
            )

        pg_hook = PostgresHook(postgres_conn_id="cnpg")

        try:
            engine = pg_hook.get_sqlalchmey_engine()
            logger.info("SQLAlchemy engine successfully obtained from PostgresHook.")
        except AttributeError:
            logger.warning(
                "PostgresHook does not support get_sqlalchmey_engine. Falling back to create_engine."
            )
            engine = create_engine(pg_hook.get_uri())

        # Check if the schema and table exist
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = 'fact' AND table_name = 'users';"
                )
                if result.rowcount == 0:
                    logger.error(
                        "Table 'users' does not exist in schema 'fact'. Aborting load."
                    )
                    raise ValueError("Table 'users' does not exist in schema 'fact'.")
                logger.info("Verified that the table 'users' exists in schema 'fact'.")
        except Exception as e:
            logger.error(f"Error while verifying table existence: {e}")
            raise

        try:
            df.to_sql(
                name="fakerjs",
                con=engine,
                schema="fact",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )
            logger.info("Data successfully loaded into the PostgreSQL table: fakerjs.")
        except Exception as e:
            logger.error(f"Failed to load data into PostgreSQL: {e}")
            raise

    extracted = extract()
    transformed = hashed_email(extracted)
    load_into_table(transformed)


dag = faker_data_ingestion()
