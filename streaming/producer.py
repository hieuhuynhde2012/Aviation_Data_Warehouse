from __future__ import annotations

import argparse
import csv
import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Iterable

from kafka import KafkaProducer


BASE_DIR = Path(os.getenv("PROJECT_BASE_DIR", "/opt/airflow"))
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
BOOKINGS_PATH = Path(os.getenv("STREAM_BOOKINGS_PATH", DATA_DIR / "generated" / "bookings" / "bookings.csv"))
PAYMENTS_PATH = Path(os.getenv("STREAM_PAYMENTS_PATH", DATA_DIR / "generated" / "payments" / "payments.csv"))
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BOOKING_TOPIC = os.getenv("KAFKA_BOOKING_TOPIC", "booking_events")
PAYMENT_TOPIC = os.getenv("KAFKA_PAYMENT_TOPIC", "payment_events")
SCHEMA_VERSION = 1


BOOKING_EVENT_MAP = {
    "CONFIRMED": "BOOKING_CREATED",
    "CANCELLED": "BOOKING_CANCELLED",
    "PENDING": "BOOKING_UPDATED",
}

PAYMENT_EVENT_MAP = {
    "PAID": "PAYMENT_CAPTURED",
    "FAILED": "PAYMENT_FAILED",
    "REFUNDED": "PAYMENT_REFUNDED",
}


def normalize_status(value: str) -> str:
    return (value or "").strip().upper()


def read_rows(path: Path, limit: int) -> Iterable[tuple[int, dict]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing source file: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader, start=1):
            if index > limit:
                break
            yield index, row


def stable_event_id(prefix: str, row: dict, index: int) -> str:
    key = row.get("booking_id") or row.get("payment_id") or f"row-{index}"
    updated_at = row.get("updated_at") or row.get("created_at") or ""
    return f"{prefix}-{uuid.uuid5(uuid.NAMESPACE_URL, f'{key}|{updated_at}|{index}')}"


def envelope(event_id: str, event_type: str, event_time: str, entity_key: str, payload: dict) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": event_time,
        "source_system": "synthetic_streaming_demo",
        "schema_version": SCHEMA_VERSION,
        "entity_key": entity_key,
        "payload": payload,
        "produced_at": datetime.utcnow().isoformat(timespec="seconds"),
    }


def build_booking_event(index: int, row: dict) -> tuple[str, dict]:
    status = normalize_status(row.get("booking_status", ""))
    event = envelope(
        event_id=stable_event_id("booking", row, index),
        event_type=BOOKING_EVENT_MAP.get(status, "BOOKING_UPDATED"),
        event_time=row.get("updated_at") or row.get("booking_time") or row.get("created_at") or datetime.utcnow().isoformat(),
        entity_key=row.get("booking_id") or f"missing-booking-{index}",
        payload=row,
    )
    return event["entity_key"], event


def build_payment_event(index: int, row: dict) -> tuple[str, dict]:
    status = normalize_status(row.get("payment_status", ""))
    event = envelope(
        event_id=stable_event_id("payment", row, index),
        event_type=PAYMENT_EVENT_MAP.get(status, "PAYMENT_UPDATED"),
        event_time=row.get("updated_at") or row.get("payment_time") or row.get("created_at") or datetime.utcnow().isoformat(),
        entity_key=row.get("payment_id") or f"missing-payment-{index}",
        payload=row,
    )
    return event["entity_key"], event


def send_events(producer: KafkaProducer, topic: str, events: Iterable[tuple[str, dict]], sleep_ms: int) -> int:
    count = 0
    for key, event in events:
        producer.send(topic, key=key, value=event)
        count += 1
        if sleep_ms:
            time.sleep(sleep_ms / 1000)
    producer.flush()
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce synthetic booking/payment events to Kafka.")
    parser.add_argument("--limit", type=int, default=int(os.getenv("STREAM_EVENT_LIMIT", "1000")))
    parser.add_argument("--sleep-ms", type=int, default=int(os.getenv("STREAM_SLEEP_MS", "0")))
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda value: str(value).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        acks="all",
        retries=5,
    )

    booking_count = send_events(
        producer,
        BOOKING_TOPIC,
        (build_booking_event(index, row) for index, row in read_rows(BOOKINGS_PATH, args.limit)),
        args.sleep_ms,
    )
    payment_count = send_events(
        producer,
        PAYMENT_TOPIC,
        (build_payment_event(index, row) for index, row in read_rows(PAYMENTS_PATH, args.limit)),
        args.sleep_ms,
    )

    print(json.dumps({"booking_events": booking_count, "payment_events": payment_count, "limit": args.limit}, indent=2))


if __name__ == "__main__":
    main()
