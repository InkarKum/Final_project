from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import requests
from kafka import KafkaProducer


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_json(api_url: str, timeout: int = 10) -> Any:
    r = requests.get(api_url, timeout=timeout)
    r.raise_for_status()
    return r.json()   # <-- list[dict]


def make_message(payload: Any) -> Dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "source": "binance",
        "ingested_at": utc_iso(),
        "payload": payload,   # LIST of tickers
    }


def run_producer_loop(
    api_url: str,
    kafka_bootstrap: str,
    topic: str,
    poll_seconds: int = 30,
    run_seconds: int = 3600,
):
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    started = time.time()
    sent = 0

    while time.time() - started < run_seconds:
        try:
            payload = fetch_json(api_url)   # <-- ВСЕ символы
            msg = make_message(payload)
            producer.send(topic, value=msg)
            producer.flush()
            sent += 1
        except Exception as e:
            print("[job1] error:", e)

        time.sleep(poll_seconds)

    producer.close()
    print(f"[job1] finished, sent={sent}")
