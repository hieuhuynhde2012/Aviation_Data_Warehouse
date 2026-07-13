from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime
from typing import Any

import psycopg2.extras
from kafka import KafkaConsumer

from streaming.db import connect, ensure_streaming_tables


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BOOKING_TOPIC = os.getenv("KAFKA_BOOKING_TOPIC", "booking_events")
PAYMENT_TOPIC = os.getenv("KAFKA_PAYMENT_TOPIC", "payment_events")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "aviation-stream-loader")


REQUIRED_ENVELOPE_FIELDS = {
    "event_id",
    "event_type",
    "event_time",
    "source_system",
    "schema_version",
    "entity_key",
    "payload",
}


def parse_time(value: str) -> datetime:
    normalized = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def validate_event(event: dict[str, Any]) -> None:
    missing = sorted(field for field in REQUIRED_ENVELOPE_FIELDS if event.get(field) in (None, ""))
    if missing:
        raise ValueError(f"Missing required envelope fields: {', '.join(missing)}")
    if not isinstance(event["payload"], dict):
        raise ValueError("payload must be an object")
    parse_time(event["event_time"])


def target_table(topic: str) -> str:
    if topic == BOOKING_TOPIC:
        return "raw.raw_booking_events_stream"
    if topic == PAYMENT_TOPIC:
        return "raw.raw_payment_events_stream"
    raise ValueError(f"Unsupported topic: {topic}")


def insert_event(cur, message, event: dict[str, Any]) -> bool:
    table = target_table(message.topic)
    cur.execute(
        f"""
        insert into {table} (
            event_id,
            event_type,
            event_time,
            source_system,
            schema_version,
            entity_key,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            payload
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (event_id) do nothing
        """,
        (
            event["event_id"],
            event["event_type"],
            parse_time(event["event_time"]),
            event["source_system"],
            int(event["schema_version"]),
            event["entity_key"],
            message.topic,
            message.partition,
            message.offset,
            psycopg2.extras.Json(event["payload"]),
        ),
    )
    return cur.rowcount == 1


def insert_dlq(cur, message, error: Exception, raw_message: Any, event_id: str | None = None, entity_key: str | None = None) -> None:
    cur.execute(
        """
        insert into metadata.streaming_dlq (
            dlq_id,
            consumer_group,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            event_id,
            entity_key,
            error_type,
            error_message,
            raw_message
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(uuid.uuid4()),
            CONSUMER_GROUP,
            message.topic,
            message.partition,
            message.offset,
            event_id,
            entity_key,
            type(error).__name__,
            str(error),
            psycopg2.extras.Json(raw_message),
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume Kafka events into Postgres raw streaming tables.")
    parser.add_argument("--max-events", type=int, default=int(os.getenv("STREAM_CONSUME_MAX_EVENTS", "2000")))
    parser.add_argument("--timeout-ms", type=int, default=int(os.getenv("STREAM_CONSUME_TIMEOUT_MS", "30000")))
    args = parser.parse_args()

    ensure_streaming_tables()
    consumer = KafkaConsumer(
        BOOKING_TOPIC,
        PAYMENT_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=args.timeout_ms,
    )

    inserted = 0
    duplicates = 0
    dlq = 0
    seen = 0
    with connect() as conn:
        with conn.cursor() as cur:
            for message in consumer:
                if seen >= args.max_events:
                    break
                seen += 1
                raw_message = message.value
                try:
                    validate_event(raw_message)
                    did_insert = insert_event(cur, message, raw_message)
                    if did_insert:
                        inserted += 1
                    else:
                        duplicates += 1
                except Exception as exc:
                    dlq += 1
                    event_id = raw_message.get("event_id") if isinstance(raw_message, dict) else None
                    entity_key = raw_message.get("entity_key") if isinstance(raw_message, dict) else None
                    insert_dlq(cur, message, exc, raw_message, event_id, entity_key)
                if seen % 200 == 0:
                    conn.commit()
                    consumer.commit()
            conn.commit()
            consumer.commit()

    consumer.close()
    print(json.dumps({"seen": seen, "inserted": inserted, "duplicates": duplicates, "dlq": dlq}, indent=2))


if __name__ == "__main__":
    main()
