# Kafka Streaming Sidecar

This project keeps the batch warehouse as the core pipeline and adds Kafka as a focused event-driven sidecar for booking and payment events.

## Why Streaming Exists Here

Batch ingestion is still the best fit for large public flight/reference files. Kafka is added for operational business events that naturally arrive continuously:

- `booking_events`
- `payment_events`

The goal is to demonstrate near-real-time ingestion patterns without making the local stack unnecessarily heavy.

## Streaming Flow

```text
data/generated/bookings/bookings.csv
data/generated/payments/payments.csv
          |
          v
streaming/producer.py
          |
          v
Kafka topics
booking_events, payment_events
          |
          v
streaming/consumer.py
          |
          v
raw.raw_booking_events_stream
raw.raw_payment_events_stream
metadata.streaming_dlq
metadata.streaming_reconciliation
```

## Event Envelope

Each Kafka message uses an explicit event envelope:

| Field | Purpose |
| --- | --- |
| `event_id` | Stable idempotency key |
| `event_type` | Business event type such as `BOOKING_CREATED` or `PAYMENT_CAPTURED` |
| `event_time` | Business timestamp used for ordering and late-arrival reasoning |
| `source_system` | Source identifier |
| `schema_version` | Contract version |
| `entity_key` | Kafka key, usually `booking_id` or `payment_id` |
| `payload` | Source-shaped booking/payment row |
| `produced_at` | Producer timestamp |

## Data Engineering Controls

- Kafka messages are keyed by entity id to preserve ordering per booking/payment within a partition.
- Consumer inserts use `event_id` as the primary key, so reruns skip duplicates.
- Invalid events are written to `metadata.streaming_dlq`.
- Kafka topic counts can be reconciled against raw stream tables through `metadata.streaming_reconciliation`.
- Stream raw tables preserve payload JSON for replay/debugging while keeping queryable envelope columns.

## Local Demo Commands

Start Kafka and Kafka UI:

```powershell
docker compose up -d --build kafka kafka-ui airflow-webserver airflow-scheduler
```

Create streaming tables in an existing warehouse volume:

```powershell
Get-Content warehouse/04_streaming_tables.sql | docker compose exec -T postgres-warehouse psql -U aviation -d aviation_dw
```

Produce 500 booking events and 500 payment events:

```powershell
docker compose exec airflow-scheduler python -m streaming.producer --limit 500
```

Consume those events into Postgres:

```powershell
docker compose exec airflow-scheduler python -m streaming.consumer --max-events 1000 --timeout-ms 30000
```

Write a reconciliation record:

```powershell
docker compose exec airflow-scheduler python -m streaming.reconcile_streaming --run-id manual-stream-demo --booking-produced 500 --payment-produced 500
```

Or trigger the Airflow DAG:

```text
aviation_streaming_demo
```

Open Kafka UI:

```text
http://localhost:8089
```
