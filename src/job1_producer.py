from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import requests
from kafka import KafkaProducer


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fetch_json(api_url: str, timeout: int = 10) -> Any:
    r = requests.get(api_url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _make_message(payload: Any, source: str = "binance") -> Dict[str, Any]:
    """
    Wrap raw API response into a single JSON event envelope.
    """
    now = _utc_iso()
    return {
        "event_id": str(uuid.uuid4()),
        "source": source,
        "ingested_at": now,
        "payload": payload,  # raw JSON from API
    }


def run_producer_loop(
    api_url: str,
    kafka_bootstrap: str,
    topic: str,
    poll_seconds: int = 60,
    run_seconds: int = 3600,
) -> None:
    """
    Pseudo-stream producer: fetch API frequently and push raw JSON into Kafka.

    This is designed to be called from Airflow DAG1 as a long-running task.
    """
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        acks="all",
        retries=5,
        linger_ms=50,
    )

    started = time.time()
    n_sent = 0

    while True:
        elapsed = time.time() - started
        if elapsed >= run_seconds:
            break

        try:
            payload = _fetch_json(api_url)
            msg = _make_message(payload=payload, source="binance")

            # try to use symbol as key if present (helps partitioning)
            key = None
            if isinstance(payload, dict) and "symbol" in payload:
                key = payload.get("symbol")
            producer.send(topic, key=key, value=msg)
            producer.flush(timeout=10)
            n_sent += 1

        except Exception as e:
            # keep running even if API/Kafka glitches
            print(f"[producer] error: {e}")

        time.sleep(max(1, poll_seconds))

    try:
        producer.flush(timeout=10)
    finally:
        producer.close()

    print(f"[producer] finished. sent={n_sent}, seconds={int(time.time()-started)}")
