from __future__ import annotations

from datetime import datetime, timezone, date, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from db_utils import init_db, sqlite_conn


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _recreate_daily_summary_schema(conn, summary_table: str) -> None:
    # Полный rebuild: удаляем таблицу и создаём заново с нужными колонками
    conn.execute(f"DROP TABLE IF EXISTS {summary_table}")
    conn.execute(
        f"""
        CREATE TABLE {summary_table} (
            day TEXT NOT NULL,
            symbol TEXT NOT NULL,

            n_records INTEGER,
            first_event_time TEXT,
            last_event_time TEXT,

            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,

            avg_price REAL,
            min_price REAL,
            max_price REAL,

            avg_volume REAL,
            sum_volume REAL,
            avg_quote_volume REAL,
            sum_quote_volume REAL,

            avg_count REAL,
            sum_count REAL,

            avg_price_change REAL,
            avg_price_change_pct REAL,

            avg_spread REAL,
            avg_spread_pct REAL,

            daily_return REAL,
            log_return REAL,
            volatility REAL,

            computed_at TEXT NOT NULL,

            PRIMARY KEY (day, symbol)
        )
        """
    )
    conn.commit()



def rebuild_daily_summary(
    sqlite_path: str,
    events_table: str = "events",
    summary_table: str = "daily_summary",
    include_today: bool = True,
    lookback_days: Optional[int] = None,
) -> None:
    """
    Recomputes analytics for ALL days in scope and rebuilds daily_summary:
      - DELETE FROM daily_summary
      - INSERT freshly computed rows

    include_today=True => includes current UTC day
    lookback_days=None => uses full history present in events
    """
    init_db(sqlite_path, events_table=events_table, summary_table=summary_table)
    computed_at = _utc_iso()

    with sqlite_conn(sqlite_path) as conn:
        _recreate_daily_summary_schema(conn, summary_table)

        # Determine time window
        if lookback_days is None:
            min_et, max_et = conn.execute(
                f"SELECT MIN(event_time), MAX(event_time) FROM {events_table}"
            ).fetchone()
            if not min_et or not max_et:
                print("[job3] events table is empty; nothing to summarize", flush=True)
                conn.execute(f"DELETE FROM {summary_table}")
                conn.commit()
                return

            start_day = pd.to_datetime(min_et, utc=True, errors="coerce").date()
            end_day = pd.to_datetime(max_et, utc=True, errors="coerce").date()
        else:
            end_day = date.today() if include_today else (date.today() - timedelta(days=1))
            start_day = end_day - timedelta(days=lookback_days - 1)

        # Detect available columns
        cols_in_db = [r[1] for r in conn.execute(f"PRAGMA table_info({events_table})").fetchall()]
        want = [
            "symbol", "price", "event_time",
            "volume", "quoteVolume", "count",
            "priceChange", "priceChangePercent",
            "bidPrice", "askPrice",
        ]
        use = [c for c in want if c in cols_in_db]

        q = f"""
        SELECT {", ".join(use)}
        FROM {events_table}
        WHERE date(event_time) BETWEEN ? AND ?
        """
        df = pd.read_sql_query(q, conn, params=[start_day.isoformat(), end_day.isoformat()])

        if df.empty:
            print(f"[job3] no events in range {start_day}..{end_day}", flush=True)
            conn.execute(f"DELETE FROM {summary_table}")
            conn.commit()
            return

        # Normalize types
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df = df.dropna(subset=["event_time", "symbol", "price"])

        df["day"] = df["event_time"].dt.date.astype(str)

        for c in ["volume", "quoteVolume", "count", "priceChange", "priceChangePercent", "bidPrice", "askPrice"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # Spread metrics (if bid/ask exist)
        if "bidPrice" in df.columns and "askPrice" in df.columns:
            df["spread"] = df["askPrice"] - df["bidPrice"]
            df["spread_pct"] = df["spread"] / df["price"]
        else:
            df["spread"] = np.nan
            df["spread_pct"] = np.nan

        # Sort for OHLC/returns
        df = df.sort_values(["symbol", "day", "event_time"])

        # Group and compute
        rows = []
        for (d, sym), g in df.groupby(["day", "symbol"], sort=False):
            g = g.sort_values("event_time")
            open_p = float(g["price"].iloc[0])
            close_p = float(g["price"].iloc[-1])
            high_p = float(g["price"].max())
            low_p = float(g["price"].min())

            daily_ret = (close_p / open_p - 1.0) if open_p > 0 else np.nan
            log_ret = np.log(close_p / open_p) if (open_p > 0 and close_p > 0) else np.nan
            vol = float(g["price"].pct_change().std()) if len(g) > 2 else np.nan

            out = {
                "day": d,
                "symbol": sym,
                "n_records": int(len(g)),
                "first_event_time": g["event_time"].iloc[0].isoformat(),
                "last_event_time": g["event_time"].iloc[-1].isoformat(),

                "open_price": open_p,
                "high_price": high_p,
                "low_price": low_p,
                "close_price": close_p,

                "avg_price": float(g["price"].mean()),
                "min_price": float(g["price"].min()),
                "max_price": float(g["price"].max()),

                "avg_spread": float(pd.to_numeric(g["spread"], errors="coerce").mean()) if g["spread"].notna().any() else np.nan,
                "avg_spread_pct": float(pd.to_numeric(g["spread_pct"], errors="coerce").mean()) if g["spread_pct"].notna().any() else np.nan,

                "daily_return": float(daily_ret) if not pd.isna(daily_ret) else np.nan,
                "log_return": float(log_ret) if not pd.isna(log_ret) else np.nan,
                "volatility": vol,

                "computed_at": computed_at,
            }

            # Optional aggregates
            if "volume" in g.columns:
                out["avg_volume"] = float(g["volume"].mean())
                out["sum_volume"] = float(g["volume"].sum())
            else:
                out["avg_volume"] = np.nan
                out["sum_volume"] = np.nan

            if "quoteVolume" in g.columns:
                out["avg_quote_volume"] = float(g["quoteVolume"].mean())
                out["sum_quote_volume"] = float(g["quoteVolume"].sum())
            else:
                out["avg_quote_volume"] = np.nan
                out["sum_quote_volume"] = np.nan

            if "count" in g.columns:
                out["avg_count"] = float(g["count"].mean())
                out["sum_count"] = float(g["count"].sum())
            else:
                out["avg_count"] = np.nan
                out["sum_count"] = np.nan

            if "priceChange" in g.columns:
                out["avg_price_change"] = float(g["priceChange"].mean())
            else:
                out["avg_price_change"] = np.nan

            if "priceChangePercent" in g.columns:
                out["avg_price_change_pct"] = float(g["priceChangePercent"].mean())
            else:
                out["avg_price_change_pct"] = np.nan

            rows.append(out)

        agg = pd.DataFrame(rows)

        # Rebuild summary table
        # conn.execute(f"DELETE FROM {summary_table}")
        # conn.commit()

        agg.to_sql(summary_table, conn, if_exists="append", index=False)

    print(
        f"[job3] rebuilt {summary_table}: rows={len(agg)} days={start_day}..{end_day} include_today={include_today}",
        flush=True,
    )
