from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
import pendulum
import random
import requests

local_tz = pendulum.timezone("Europe/London")


def return_faker_data() -> list:
    random_num: int = random.randint(1, 1000)

    res = requests.get(f"http://faker-api.home.local/api/users?count={random_num}")
    res.raise_for_status()

    if res.status_code != 200:
        raise Exception(f"Failed to fetch faker data: {res.status_code} - {res.text}")

    data = res.json()
    return data["users"]


@dag(
    dag_id="faker_data_ingestion",
    description="Ingesting fake user data into the fact table",
    schedule="0 1 * * *",  # Daily at midnight
    tags=["data_ingestion"],
)
def faker_data_ingestion():
    pass
