"""
Daily currency exchange-rate ETL: Frankfurter API -> transform -> MongoDB.
Tasks pass data using NAMED XComs (explicit keys via xcom_push / xcom_pull).
"""
import os
from datetime import datetime, timedelta

import requests
from pymongo import MongoClient, UpdateOne

try:  # Airflow 3 / Astro Runtime 3.x
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGETS = "USD,GBP,JPY"

MONGO_URI = os.getenv(
    "MONGO_URI", "mongodb://admin:admin123@mongo:27017/?authSource=admin"
)
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")

# Named XCom keys
RAW_KEY = "raw_exchange_rates"
TRANSFORMED_KEY = "transformed_exchange_rates"


@dag(
    dag_id="currency_exchange_etl",
    description="Extract -> Transform -> Load daily FX rates into MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"owner": "hannan", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "mongodb", "frankfurter"],
)
def currency_exchange_etl():

    @task(task_id="extract_exchange_rates")
    def extract(ti=None):
        resp = requests.get(
            API_URL, params={"base": BASE_CURRENCY, "quotes": TARGETS}, timeout=30
        )
        resp.raise_for_status()
        payload = {
            "rates": resp.json(),  # list of {date, base, quote, rate}
            "retrieved_at": datetime.utcnow().replace(microsecond=0).isoformat(),
        }
        ti.xcom_push(key=RAW_KEY, value=payload)
        print(f"Extracted {len(payload['rates'])} rates")

    @task(task_id="transform_exchange_rates")
    def transform(ti=None):
        raw = ti.xcom_pull(task_ids="extract_exchange_rates", key=RAW_KEY)
        if not raw or not raw.get("rates"):
            raise ValueError("No raw data received from extract task")

        docs = [
            {
                "base_currency": item["base"],
                "target_currency": item["quote"],
                "exchange_rate": float(item["rate"]),
                "rate_date": item["date"],
                "source": "Frankfurter API",
                "retrieved_at": raw["retrieved_at"],
            }
            for item in raw["rates"]
        ]
        ti.xcom_push(key=TRANSFORMED_KEY, value=docs)
        print(f"Transformed {len(docs)} documents")

    @task(task_id="load_to_mongodb")
    def load(ti=None):
        docs = ti.xcom_pull(task_ids="transform_exchange_rates", key=TRANSFORMED_KEY)
        if not docs:
            raise ValueError("No transformed data received")

        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        try:
            col = client[MONGO_DB][MONGO_COLLECTION]
            col.create_index(
                [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
                unique=True,
            )
            # Upsert => re-running the DAG never creates duplicates
            ops = [
                UpdateOne(
                    {
                        "base_currency": d["base_currency"],
                        "target_currency": d["target_currency"],
                        "rate_date": d["rate_date"],
                    },
                    {"$set": d},
                    upsert=True,
                )
                for d in docs
            ]
            result = col.bulk_write(ops)
            print(f"Upserted={result.upserted_count} Modified={result.modified_count}")
        finally:
            client.close()

    extract() >> transform() >> load()


currency_exchange_etl()
