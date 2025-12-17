from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional


def ensure_parent_dir(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def sqlite_conn(db_path: str) -> Iterator[sqlite3.Connection]:
    ensure_parent_dir(db_path)
    conn = sqlite3.connect(db_path)
    try:
        # safer defaults
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: str, events_table: str = "events", summary_table: str = "daily_summary") -> None:
    events_sql = f"""
    CREATE TABLE IF NOT EXISTS {events_table} (
        event_id TEXT PRIMARY KEY,
        symbol TEXT NOT NULL,
        price REAL NOT NULL,
        event_time TEXT NOT NULL,              -- ISO datetime string
        source TEXT NOT NULL,
        ingested_at TEXT NOT NULL              -- ISO datetime string
    );
    """

    summary_sql = f"""
    CREATE TABLE IF NOT EXISTS {summary_table} (
        day TEXT NOT NULL,                     -- YYYY-MM-DD
        symbol TEXT NOT NULL,
        avg_price REAL NOT NULL,
        min_price REAL NOT NULL,
        max_price REAL NOT NULL,
        n_records INTEGER NOT NULL,
        computed_at TEXT NOT NULL,             -- ISO datetime string
        PRIMARY KEY (day, symbol)
    );
    """

    with sqlite_conn(db_path) as conn:
        conn.execute(events_sql)
        conn.execute(summary_sql)


def upsert_daily_summary_rows(conn: sqlite3.Connection, summary_table: str, rows: list[tuple]) -> None:
    """
    rows: [(day, symbol, avg_price, min_price, max_price, n_records, computed_at), ...]
    """
    sql = f"""
    INSERT INTO {summary_table}
        (day, symbol, avg_price, min_price, max_price, n_records, computed_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(day, symbol) DO UPDATE SET
        avg_price=excluded.avg_price,
        min_price=excluded.min_price,
        max_price=excluded.max_price,
        n_records=excluded.n_records,
        computed_at=excluded.computed_at;
    """
    conn.executemany(sql, rows)
