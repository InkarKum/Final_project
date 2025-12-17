from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional
import pandas as pd

from db_utils import init_db, sqlite_conn, upsert_daily_summary_rows


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def compute_and_store_daily_summary(
    sqlite_path: str,
    events_table: str = "events",
    summary_table: str = "daily_summary",
    target_day: Optional[str] = None,  # "YYYY-MM-DD" (optional)
) -> None:
    """
    Job3: read SQLite(events) -> compute daily aggregates -> upsert into daily_summary

    If target_day is None, we compute for yesterday (UTC) to ensure full-day batch.
    """
    init_db(sqlite_path, events_table=events_table, summary_table=summary_table)

    if target_day is None:
        target_day = (date.today() - timedelta(days=1)).isoformat()

    computed_at = _utc_iso()

    with sqlite_conn(sqlite_path) as conn:
        # Read one day of data (SQL filter)
        query = f"""
        SELECT symbol, price, event_time
        FROM {events_table}
        WHERE date(event_time) = ?
        """
        df = pd.read_sql_query(query, conn, params=[target_day])

        if df.empty:
            print(f"[job3] no events for day={target_day}")
            return

        # Cleaning for analytics (still Pandas, no loops)
        df = (
            df.dropna(subset=["symbol", "price", "event_time"])
              .assign(
                  symbol=lambda x: x["symbol"].astype(str).str.upper().str.strip(),
                  price=lambda x: pd.to_numeric(x["price"], errors="coerce"),
              )
              .dropna(subset=["price"])
        )

        # Aggregations (Pandas groupby)
        agg = (
            df.groupby("symbol", as_index=False)
              .agg(
                  avg_price=("price", "mean"),
                  min_price=("price", "min"),
                  max_price=("price", "max"),
                  n_records=("price", "size"),
              )
        )

        # Prepare rows for upsert into summary table
        out = agg.assign(day=target_day, computed_at=computed_at)[
            ["day", "symbol", "avg_price", "min_price", "max_price", "n_records", "computed_at"]
        ]

        rows = list(out.itertuples(index=False, name=None))
        upsert_daily_summary_rows(conn, summary_table=summary_table, rows=rows)

    print(f"[job3] upserted_rows={len(rows)} into {sqlite_path}:{summary_table} for day={target_day}")
