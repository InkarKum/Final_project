from __future__ import annotations

import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.decorators import task
import sys
# from airflow.utils.dates import days_ago

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)


# --- Config via env (safe for GitHub) ---
API_URL = os.getenv("API_URL", "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "raw_events")

# pseudo-stream loop settings
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))          # fetch every 60s
RUN_SECONDS = int(os.getenv("RUN_SECONDS", "3600"))          # run for 1 hour per trigger


with DAG(
    dag_id="job1_ingestion_api_to_kafka",
    description="Job 1: Pseudo-stream ingestion: API -> Kafka (raw JSON)",
    start_date=pendulum.datetime(2025, 12, 17, tz="UTC"),
    schedule=None,  # trigger manually in UI (or via API); acts as long-running job
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        # if you prefer: "execution_timeout": timedelta(hours=2),
    },
    tags=["final_project", "job1", "kafka", "ingestion"],
) as dag:

    @task
    def produce_raw_events():
        # Import here so Airflow parsing doesn't require deps at parse-time
        from job1_producer import run_producer_loop
        print("KAFKA_BOOTSTRAP=", os.getenv("KAFKA_BOOTSTRAP"), flush=True)
        print("API_URL=", os.getenv("API_URL"), flush=True)
        run_producer_loop(
            api_url=API_URL,
            kafka_bootstrap=KAFKA_BOOTSTRAP,
            topic=KAFKA_TOPIC_RAW,
            poll_seconds=POLL_SECONDS,
            run_seconds=RUN_SECONDS,
        )

    produce_raw_events()
