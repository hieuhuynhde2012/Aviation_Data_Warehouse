# Data Model

## Raw

Raw tables keep data close to source shape and add `source_file` plus `loaded_at`. Hard-invalid records are quarantined before raw load; dirty but correctable records remain available for staging standardization.

- `raw.raw_bts_flights`
- `raw.raw_airports`
- `raw.raw_countries`
- `raw.raw_regions`
- `raw.raw_bookings`
- `raw.raw_payments`

## dbt Staging

Staging models standardize names, cast types, and keep one source-oriented model per entity.

- `staging.stg_flights`
- `staging.stg_airports`
- `staging.stg_bookings`
- `staging.stg_payments`

Key cleaning rules:

- Trim and uppercase business keys and airport codes.
- Remove airport terminal suffix noise such as `-T1`.
- Map source status synonyms to canonical values.
- Normalize booking channel, customer segment, payment method, and currency.
- Dedupe booking and payment events by ID, keeping the latest `updated_at`.
- Convert missing customer IDs to `UNKNOWN_CUSTOMER`.
- Correct negative ticket/amount fields before mart usage.
- Ensure `updated_at >= created_at` for synthetic event corrections.

## Dimensional Layer

- `mart.dim_airport`
- `mart.dim_airline`
- `mart.dim_route`
- `mart.dim_customer`
- `mart.dim_date`

## Fact Layer

- `mart.fact_flight_status`
- `mart.fact_booking`
- `mart.fact_payment`

## Dashboard Marts

The mart layer is intentionally denormalized for Superset.

- Sales performance by date, airline, and route.
- Booking status and payment status snapshot.
- Daily/hourly booking trends by channel.
- Route performance combining revenue and delays.
- Airport performance for origin/destination volume and operational issues.
- Customer segment performance.
- Flight delay analysis by airline, airport, route, and cause.

## Metadata And Quality Tables

- `metadata.pipeline_file_registry`: file-level registry and checksums.
- `metadata.raw_load_audit`: idempotent load audit, inserted/skipped rows, checksum status.
- `metadata.dq_record_errors`: record-level DQ errors and warnings.
- `metadata.reconciliation_summary`: source-to-raw row-count reconciliation.
- `quarantine.invalid_records`: invalid records with raw payload for debugging/replay.
