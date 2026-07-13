# Project Walkthrough

## Business Case

This project simulates an aviation analytics platform for answering operational and commercial questions:

- Which routes generate the most revenue?
- Which airports and airlines have the highest delay pressure?
- How do booking channels perform over time?
- Are source files loaded completely and only once?
- Which records failed data quality checks, and why?

The goal is not only to produce dashboards, but to show the data engineering controls needed before data becomes trusted.

## End-To-End Flow

1. Ingest public aviation operations data from BTS.
2. Ingest public airport reference data from OurAirports.
3. Generate synthetic bookings and payments linked to real flight routes.
4. Land raw/source files in MinIO buckets.
5. Validate files and records before loading.
6. Quarantine hard-error records and persist DQ events.
7. Load valid raw data into PostgreSQL with checksum-based idempotency.
8. Reconcile source rows against target raw rows.
9. Build staging, intermediate, dimensions, facts, and marts with dbt.
10. Run dbt tests.
11. Publish BI-ready datasets and dashboard assets to Superset.
12. Optionally replay booking/payment rows as Kafka events into raw streaming tables.
13. Optionally run Spark feature jobs over large source CSVs and land feature tables back into the warehouse.

## Production-Style Controls

The project includes controls commonly expected in real data platforms:

- Required-column checks before load.
- Controlled vocabulary standardization.
- Record-level DQ error logs.
- Quarantine instead of silent drops.
- Cross-source relationship validation for payments and bookings.
- Raw load audit with file checksums.
- Idempotent reruns that skip already-loaded files.
- CDC-style dedupe in staging using latest `updated_at`.
- Incremental partition replacement for reruns and backfills.
- Source-to-target reconciliation summary.
- dbt tests for uniqueness, non-null, accepted values, relationships, and numeric constraints.
- Kafka event keys, event envelopes, idempotent stream inserts, DLQ, and streaming reconciliation.
- Spark feature processing for route/airline delay aggregates and route booking features.

## Warehouse Model

The warehouse follows a layered model:

| Layer | Purpose |
| --- | --- |
| `raw` | Preserves source-shaped data that passed hard validation |
| `staging` | Standardizes types, values, casing, dedupe, and business rules |
| `intermediate` | Reusable joins and aggregations |
| `mart` | BI-ready facts, dimensions, and dashboard marts |
| `metadata` | Run logs, DQ errors, load audit, reconciliation |
| `quarantine` | Invalid records with raw JSON payloads and reasons |

Streaming events land in `raw.raw_booking_events_stream` and `raw.raw_payment_events_stream`. Invalid stream messages land in `metadata.streaming_dlq`.

Spark feature outputs land in `mart.spark_route_delay_features`, `mart.spark_route_booking_features`, and `metadata.spark_job_audit`.

## What To Demo

1. Airflow graph view for `aviation_daily_pipeline`.
2. MinIO raw bucket partitions.
3. `metadata.reconciliation_summary` showing `PASSED`.
4. `metadata.dq_record_errors` showing warnings and errors by rule.
5. `quarantine.invalid_records` showing invalid records were retained.
6. dbt docs lineage from source to mart.
7. Superset dashboard `Aviation Data Warehouse Operations`.
8. Kafka UI showing `booking_events` and `payment_events`.
9. Spark UI showing completed feature job stages.

## Interview Talking Points

- The data is intentionally dirty, so cleaning work is visible and testable.
- Hard errors are quarantined; soft issues are logged and standardized downstream.
- Reruns are safe because file checksums and partition replacement prevent duplicate loads.
- Reconciliation gives a clear source-to-target control before marts are trusted.
- Kafka demonstrates event-driven ingestion, idempotent event processing, and DLQ handling.
- Spark demonstrates distributed feature processing over large operational CSV data.
- The dashboard includes business KPIs and operational DQ observability.
