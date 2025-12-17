from __future__ import annotations
import pendulum
import os
from datetime import timedelta
import sys
from airflow import DAG
from airflow.decorators import task
# from airflow.utils.dates import days_ago


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)    

SQLITE_PATH = os.getenv("SQLITE_PATH", "data/app.db")
EVENTS_TABLE = os.getenv("EVENTS_TABLE", "events")
SUMMARY_TABLE = os.getenv("SUMMARY_TABLE", "daily_summary")


with DAG(
    dag_id="job3_daily_sqlite_analytics_summary",
    description="Job 3: Daily analytics: SQLite(events) -> aggregates -> SQLite(daily_summary)",
    start_date=pendulum.datetime(2025, 12, 17, tz="UTC"),
    schedule="@daily",  
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["final_project", "job3", "analytics", "sqlite"],
) as dag:

    @task
    def compute_daily_summary():
        from job3_analytics import compute_and_store_daily_summary

        compute_and_store_daily_summary(
            sqlite_path=SQLITE_PATH,
            events_table=EVENTS_TABLE,
            summary_table=SUMMARY_TABLE,
        )

    compute_daily_summary()
