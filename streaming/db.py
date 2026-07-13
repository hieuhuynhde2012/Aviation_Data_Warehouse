from __future__ import annotations

import psycopg2


STREAMING_DDL = """
create table if not exists raw.raw_booking_events_stream (
    event_id text primary key,
    event_type text not null,
    event_time timestamp not null,
    source_system text not null,
    schema_version integer not null,
    entity_key text not null,
    kafka_topic text,
    kafka_partition integer,
    kafka_offset bigint,
    payload jsonb not null,
    ingested_at timestamp default current_timestamp
);

create table if not exists raw.raw_payment_events_stream (
    event_id text primary key,
    event_type text not null,
    event_time timestamp not null,
    source_system text not null,
    schema_version integer not null,
    entity_key text not null,
    kafka_topic text,
    kafka_partition integer,
    kafka_offset bigint,
    payload jsonb not null,
    ingested_at timestamp default current_timestamp
);

create table if not exists metadata.streaming_dlq (
    dlq_id text primary key,
    consumer_group text not null,
    kafka_topic text,
    kafka_partition integer,
    kafka_offset bigint,
    event_id text,
    entity_key text,
    error_type text not null,
    error_message text not null,
    raw_message jsonb,
    created_at timestamp default current_timestamp
);

create table if not exists metadata.streaming_reconciliation (
    reconciliation_id text primary key,
    run_id text not null,
    topic_name text not null,
    produced_events bigint not null,
    consumed_events bigint not null,
    duplicate_events bigint not null,
    dlq_events bigint not null,
    status text not null,
    difference bigint not null,
    created_at timestamp default current_timestamp
);
"""


def warehouse_config() -> dict:
    import os

    return {
        "host": os.getenv("WAREHOUSE_HOST", "postgres-warehouse"),
        "port": int(os.getenv("WAREHOUSE_PORT_INTERNAL", "5432")),
        "dbname": os.getenv("WAREHOUSE_DB", "aviation_dw"),
        "user": os.getenv("WAREHOUSE_USER", "aviation"),
        "password": os.getenv("WAREHOUSE_PASSWORD", "aviation"),
    }


def connect():
    return psycopg2.connect(**warehouse_config())


def ensure_streaming_tables() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(STREAMING_DDL)
