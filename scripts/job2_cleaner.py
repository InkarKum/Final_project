from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from kafka import KafkaConsumer, TopicPartition

from src.db_utils import init_db, sqlite_conn


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_rows_from_envelope(df_env: pd.DataFrame) -> pd.DataFrame:
    """
    Convert envelope(payload) -> flat rows with columns needed for events table.
    Supports Binance endpoints like:
      - /api/v3/ticker/price      -> {"symbol":"BTCUSDT","price":"123.45"}
      - /api/v3/ticker/price      -> [ {...}, {...} ]
      - /api/v3/ticker/24hr       -> dict or list (we will take symbol + lastPrice if present)
    """
    # df_env columns: event_id, source, ingested_at, payload
    payload_series = df_env["payload"]

    # Normalize payloads: each envelope may contain dict or list -> explode to rows
    payload_df = pd.json_normalize(payload_series)

    # If payload is a list-of-dicts, json_normalize gives columns like 0.symbol ... not ideal.
    # So handle list payloads explicitly:
    is_list_mask = payload_series.apply(lambda x: isinstance(x, list))
    if is_list_mask.any():
        # Expand list payloads into separate rows
        list_part = payload_series[is_list_mask].explode()
        list_df = pd.json_normalize(list_part).reset_index(drop=True)

        # Non-list part
        dict_part = payload_series[~is_list_mask]
        dict_df = pd.json_normalize(dict_part).reset_index(drop=True)

        # Align with envelopes: repeat envelope rows for exploded lists
        env_list = df_env[is_list_mask].loc[list_part.index].reset_index(drop=True)
        env_list = env_list.loc[env_list.index.repeat(1)].reset_index(drop=True)

        # Better: rebuild by repeating envelope rows exactly to explode length
        # We can do this without Python loops by using group sizes:
        sizes = df_env[is_list_mask]["payload"].apply(len).to_numpy()
        env_rep = df_env[is_list_mask].loc[df_env[is_list_mask].index.repeat(sizes)].reset_index(drop=True)
        payload_rep = pd.json_normalize(list_part.reset_index(drop=True))

        df_list_final = pd.concat([env_rep[["event_id", "source", "ingested_at"]].reset_index(drop=True), payload_rep], axis=1)
        df_dict_final = pd.concat([df_env[~is_list_mask][["event_id", "source", "ingested_at"]].reset_index(drop=True), dict_df], axis=1)

        df_flat = pd.concat([df_dict_final, df_list_final], ignore_index=True)
    else:
        df_flat = pd.concat([df_env[["event_id", "source", "ingested_at"]].reset_index(drop=True), payload_df], axis=1)

    # Map fields depending on endpoint
    # Prefer 'price' if exists; else use 'lastPrice' (24hr endpoint)
    df_flat = df_flat.rename(columns={"lastPrice": "price"})

    # Binance sometimes returns numeric as strings -> cast with pandas
    # Required columns
    needed = ["event_id", "source", "ingested_at", "symbol", "price"]
    # If symbol/price missing, produce empty frame with those columns
    for col in ["symbol", "price"]:
        if col not in df_flat.columns:
            df_flat[col] = pd.NA

    # Cleaning rules (Pandas only):
    cleaned = (
        df_flat[needed]
        .dropna(subset=["symbol", "price"])                   # remove missing
        .assign(
            symbol=lambda x: x["symbol"].astype(str).str.upper().str.strip(),
            price=lambda x: pd.to_numeric(x["price"], errors="coerce"),
            event_time=lambda x: x["ingested_at"],            # event_time = ingestion time (simple & valid)
        )
        .dropna(subset=["price"])
        .query("price > 0")                                   # filter invalid
        .drop_duplicates(subset=["event_id"])
        .drop_duplicates(subset=["symbol", "event_time"])      # avoid duplicates per tick time
    )

    # Keep final event columns
    cleaned = cleaned[["event_id", "symbol", "price", "event_time", "source", "ingested_at"]]
    return cleaned


def _read_kafka_batch(
    kafka_bootstrap: str,
    topic: str,
    group_id: str,
    poll_ms: int = 5000,
    max_messages: int = 20000,
) -> tuple[list[dict], KafkaConsumer]:
    """
    Pull all available messages in this run (bounded by max_messages).
    Returns (messages_as_dicts, consumer). We keep consumer open so we can commit after DB write.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap,
        group_id=group_id,
        enable_auto_commit=False,               # commit after successful SQLite write
        auto_offset_reset="earliest",           # first run reads from start; later runs continue
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=2000,               # stop iteration when no messages
        max_poll_records=5000,
    )

    msgs: list[dict] = []
    try:
        for m in consumer:
            msgs.append(m.value)
            if len(msgs) >= max_messages:
                break
    except Exception as e:
        print(f"[job2] kafka read error: {e}")

    return msgs, consumer


def consume_clean_store_batch(
    kafka_bootstrap: str,
    topic: str,
    group_id: str,
    sqlite_path: str,
    events_table: str = "events",
) -> None:
    """
    Job2: read Kafka -> clean with pandas -> write to SQLite(events)
    """
    init_db(sqlite_path, events_table=events_table, summary_table="daily_summary")

    msgs, consumer = _read_kafka_batch(
        kafka_bootstrap=kafka_bootstrap,
        topic=topic,
        group_id=group_id,
    )

    if not msgs:
        print("[job2] no new messages in Kafka")
        consumer.close()
        return

    # Build envelope DataFrame
    df_env = pd.DataFrame(msgs)

    # Ensure required envelope cols exist
    for col in ["event_id", "source", "ingested_at", "payload"]:
        if col not in df_env.columns:
            df_env[col] = pd.NA

    df_events = _extract_rows_from_envelope(df_env)

    if df_events.empty:
        print("[job2] after cleaning, no valid rows to store")
        consumer.close()
        return

    # Store to SQLite
    with sqlite_conn(sqlite_path) as conn:
        # Using pandas to_sql is allowed; we avoid python loops
        df_events.to_sql(events_table, conn, if_exists="append", index=False)

    # Commit Kafka offsets ONLY after DB write succeeded
    try:
        consumer.commit()
    finally:
        consumer.close()

    print(f"[job2] stored_rows={len(df_events)} into {sqlite_path}:{events_table}")
