from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "raw_events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "job2_cleaner_group")

SQLITE_PATH = os.getenv("SQLITE_PATH", "data/app.db")
EVENTS_TABLE = os.getenv("EVENTS_TABLE", "events")


with DAG(
    dag_id="job2_hourly_kafka_to_sqlite_clean",
    description="Job 2: Hourly batch: Kafka -> clean (pandas) -> SQLite (events)",
    start_date=days_ago(1),
    schedule="@hourly",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["final_project", "job2", "kafka", "sqlite", "cleaning"],
) as dag:

    @task
    def consume_clean_store():
        from src.job2_cleaner import consume_clean_store_batch

        consume_clean_store_batch(
            kafka_bootstrap=KAFKA_BOOTSTRAP,
            topic=KAFKA_TOPIC_RAW,
            group_id=KAFKA_GROUP_ID,
            sqlite_path=SQLITE_PATH,
            events_table=EVENTS_TABLE,
        )

    consume_clean_store()
