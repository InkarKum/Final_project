from __future__ import annotations
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import pandas as pd
from kafka import KafkaConsumer
from db_utils import init_db, sqlite_conn


BASE_COLS = ["event_id", "source", "ingested_at", "symbol", "event_time"]

TICKER_COLS = [
    "priceChange",
    "priceChangePercent",
    "weightedAvgPrice",
    "prevClosePrice",
    "lastPrice",
    "lastQty",
    "bidPrice",
    "bidQty",
    "askPrice",
    "askQty",
    "openPrice",
    "highPrice",
    "lowPrice",
    "volume",
    "quoteVolume",
    "openTime",
    "closeTime",
    "firstId",
    "lastId",
    "count",
]

FINAL_COLS = [
    "event_id",
    "symbol",
    "price",
    "event_time",
    "source",
    "ingested_at",
] + TICKER_COLS

FLOAT_COLS = [
    "price",
    "priceChange",
    "priceChangePercent",
    "weightedAvgPrice",
    "prevClosePrice",
    "lastPrice",
    "lastQty",
    "bidPrice",
    "bidQty",
    "askPrice",
    "askQty",
    "openPrice",
    "highPrice",
    "lowPrice",
    "volume",
    "quoteVolume",
]
INT_COLS = ["openTime", "closeTime", "firstId", "lastId", "count"]


# Helping with errors
def _ms_to_iso(ms: Any):
    try:
        if ms is None or (isinstance(ms, float) and pd.isna(ms)):
            return None
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _is_klines_payload(x: Any):
    return isinstance(x, list) and (len(x) == 0 or isinstance(x[0], list))


def _is_ticker_payload(x: Any):
    return isinstance(x, dict) and ("symbol" in x) and ("lastPrice" in x or "priceChange" in x)


def _ensure_columns_sqlite(conn, table: str, df: pd.DataFrame):
    cur = conn.cursor()
    existing = [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
    missing = [c for c in df.columns if c not in existing]

    for c in missing:
        if c in INT_COLS:
            col_type = "INTEGER"
        elif c in FLOAT_COLS:
            col_type = "REAL"
        else:
            col_type = "TEXT"
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {c} {col_type}")

    if missing:
        conn.commit()


def _finalize_and_clean(df: pd.DataFrame):
    if df.empty:
        return pd.DataFrame(columns=FINAL_COLS)
    for c in FINAL_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    for c in FLOAT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in INT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["event_time"] = df["event_time"].astype(str)
    df = df.dropna(subset=["symbol", "price", "event_time"])
    df = df[df["price"] > 0]
    df = df.drop_duplicates(subset=["event_id"])
    df = df.drop_duplicates(subset=["symbol", "event_time"])

    return df[FINAL_COLS]

# Extraction
def _extract_rows_from_envelope(df_env: pd.DataFrame):
    if df_env.empty or "payload" not in df_env.columns:
        return pd.DataFrame(columns=FINAL_COLS)

    payload = df_env["payload"]
    kl_mask = payload.apply(_is_klines_payload)
    tk_mask = payload.apply(_is_ticker_payload)

    frames: list[pd.DataFrame] = []

    if tk_mask.any():
        dft = df_env[tk_mask].copy()

        payload_df = pd.json_normalize(dft["payload"])

        out = pd.concat(
            [
                dft[["event_id", "source", "ingested_at"]].reset_index(drop=True),
                payload_df.reset_index(drop=True),
            ],
            axis=1,
        )
        if "lastPrice" in out.columns:
            out["price"] = out["lastPrice"]
        else:
            out["price"] = pd.NA
        if "closeTime" in out.columns:
            out["event_time"] = out["closeTime"].apply(_ms_to_iso)
        elif "openTime" in out.columns:
            out["event_time"] = out["openTime"].apply(_ms_to_iso)
        else:
            out["event_time"] = out["ingested_at"]

        # Ensure required cols
        if "symbol" not in out.columns:
            out["symbol"] = pd.NA
        frames.append(out)
    if kl_mask.any():
        dfk = df_env[kl_mask].copy()
        dfk = dfk.explode("payload", ignore_index=True)

        dfk["symbol"] = dfk.get("symbol", pd.NA)
        dfk["price"] = dfk["payload"].apply(lambda k: k[4] if isinstance(k, list) and len(k) > 4 else None)
        dfk["openTime"] = dfk["payload"].apply(lambda k: k[0] if isinstance(k, list) and len(k) > 0 else None)
        dfk["event_time"] = dfk["openTime"].apply(_ms_to_iso)

        for c in TICKER_COLS:
            if c not in dfk.columns:
                dfk[c] = pd.NA

        dfk["lastPrice"] = pd.NA
        frames.append(dfk)

    if not frames:
        return pd.DataFrame(columns=FINAL_COLS)

    df_all = pd.concat(frames, ignore_index=True)
    return _finalize_and_clean(df_all)


def _read_kafka_batch(kafka_bootstrap: str,topic: str,group_id: str,max_messages: int = 20000,read_seconds: int = 30,poll_timeout_ms: int = 1000,):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[kafka_bootstrap],
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=5000,
        api_version=(2, 6),
    )

    consumer.poll(timeout_ms=0)

    msgs: list[dict] = []
    end_at = time.time() + read_seconds

    try:
        while time.time() < end_at and len(msgs) < max_messages:
            polled = consumer.poll(timeout_ms=poll_timeout_ms)
            if not polled:
                continue
            for _tp, records in polled.items():
                for r in records:
                    msgs.append(r.value)
                    if len(msgs) >= max_messages:
                        break
    except Exception as e:
        print(f"kafka read error: {e}", flush=True)

    return msgs, consumer


def _filter_existing_event_ids(df_events: pd.DataFrame, conn, events_table: str):
    if df_events.empty:
        return df_events
    existing = pd.read_sql_query(f"SELECT event_id FROM {events_table}", conn)
    if existing.empty:
        return df_events
    return df_events[~df_events["event_id"].isin(existing["event_id"])]



def consume_clean_store_batch(kafka_bootstrap: str,topic: str,group_id: str,sqlite_path: str,events_table: str = "events",):
    Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)

    init_db(sqlite_path, events_table=events_table, summary_table="daily_summary")

    msgs, consumer = _read_kafka_batch(
        kafka_bootstrap=kafka_bootstrap,
        topic=topic,
        group_id=group_id,
    )

    print(f"fetched_msgs={len(msgs)} group_id={group_id}", flush=True)

    if not msgs:
        print("nothing to get from Kafka", flush=True)
        consumer.close()
        return

    df_env = pd.DataFrame(msgs)
    df_events = _extract_rows_from_envelope(df_env)

    print(f"cleaned cleaned_rows={len(df_events)}", flush=True)

    if df_events.empty:
        consumer.close()
        return

    with sqlite_conn(sqlite_path) as conn:
        _ensure_columns_sqlite(conn, events_table, df_events[FINAL_COLS])

        df_events = _filter_existing_event_ids(df_events, conn, events_table)
        if df_events.empty:
            print("Inserting duplicates permited", flush=True)
            consumer.close()
            return

        df_events[FINAL_COLS].to_sql(events_table, conn, if_exists="append", index=False)

    try:
        consumer.commit()
    finally:
        consumer.close()

    print(f"cleaning done stored_rows={len(df_events)} into {sqlite_path}:{events_table}", flush=True)
