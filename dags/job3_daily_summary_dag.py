from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago


SQLITE_PATH = os.getenv("SQLITE_PATH", "data/app.db")
EVENTS_TABLE = os.getenv("EVENTS_TABLE", "events")
SUMMARY_TABLE = os.getenv("SUMMARY_TABLE", "daily_summary")


with DAG(
    dag_id="job3_daily_sqlite_analytics_summary",
    description="Job 3: Daily analytics: SQLite(events) -> aggregates -> SQLite(daily_summary)",
    start_date=days_ago(1),
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
        from src.job3_analytics import compute_and_store_daily_summary

        compute_and_store_daily_summary(
            sqlite_path=SQLITE_PATH,
            events_table=EVENTS_TABLE,
            summary_table=SUMMARY_TABLE,
        )

    compute_daily_summary()
