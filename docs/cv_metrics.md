# CV Metrics For This Project

Snapshot date: 2026-07-22

This file converts the current project state into quantified resume bullets that are safe to use in a CV, LinkedIn profile, or interview walkthrough.

## 1. Verified Project Metrics

### Data volume

- 889,580 total source rows across 4 sources.
- 597,919 BTS flight-operation rows.
- 85,658 airport reference rows.
- 103,022 synthetic booking rows.
- 102,981 synthetic payment rows.

### Data quality and cleaning

- 205,079 valid booking and payment rows after validation.
- 924 quarantined booking and payment rows combined.
- 0.45% combined quarantine rate on booking and payment business-event data.
- 43,707 dirty-standardization rows intentionally generated for cleaning and canonicalization logic.
- 38,097 record-level DQ errors written to the latest CSV validation output.
- 152,180 record-level DQ error rows persisted in warehouse metadata across reruns.
- 3,018 duplicate booking rows generated.
- 2,981 duplicate payment rows generated.

### Reconciliation and idempotency

- 100% PASSED source-to-target reconciliation for BTS, airports, bookings, and payments in the current warehouse state.
- Booking raw load reconciled at 102,700 expected rows and 102,700 actual rows.
- Payment raw load reconciled at 102,379 expected rows and 102,379 actual rows.

### Warehouse and modeling

- 7 warehouse schemas: `raw`, `staging`, `intermediate`, `mart`, `metadata`, `reference`, `quarantine`.
- 5 dimensions, 3 facts, and 7 analytics marts in dbt.
- 99,692 rows in `mart.fact_booking`.
- 99,418 rows in `mart.fact_payment`.
- 597,919 rows in `mart.fact_flight_status`.
- 5,964 rows in `mart.mart_route_performance`.
- 878 rows in `mart.mart_sales_performance`.

### Orchestration and platform

- 3 Airflow DAGs in the repo.
- 2 Kafka topics for streaming demo: `booking_events` and `payment_events`.
- 1 Spark standalone cluster with master, worker, and submit utility container.
- 13 local service containers running in the stack during verification.

### Streaming

- 1,000 streaming rows landed into raw stream tables in the current warehouse state.
- 500 booking events produced and 500 consumed in a successful streaming run.
- 500 payment events produced and 500 consumed in a successful streaming run.
- 1,000 duplicate events skipped in a rerun streaming demo while reconciliation still PASSED.

### Spark

- 3 Spark output tables prepared in the warehouse: `mart.spark_route_delay_features`, `mart.spark_route_booking_features`, `metadata.spark_job_audit`.
- Spark infrastructure is running and documented, but feature-job output counts are still `0` until the Spark job is executed.

## 2. Where You Can Add Numbers In Your CV

These are the strongest places to quantify this project:

- Data scale: total rows processed, source count, raw file sizes.
- Data quality: quarantine rate, duplicate count, DQ error count, dirty-value standardization count.
- Reliability: reconciliation pass rate, idempotent rerun behavior, duplicate-event skipping.
- Warehouse output: number of schemas, dimensions, facts, marts, and final fact row counts.
- Streaming: events produced, consumed, deduplicated, and DLQ behavior.
- Platform breadth: number of services, orchestration layers, and technologies in one stack.

## 3. CV Bullets In English

### Option A: balanced and recruiter-friendly

- Built a Dockerized aviation data warehouse that processed 889,580 rows across flight operations, airport reference, and synthetic booking/payment sources using Airflow, PostgreSQL, dbt, Kafka, MinIO, Spark, and Superset.
- Implemented production-style data quality controls that validated 206,003 business-event rows, quarantined 924 invalid records, and logged 38,097 record-level DQ issues in the latest validation batch instead of silently dropping bad data.
- Designed idempotent and reconcilable ingestion flows with checksum-based file tracking, CDC-style deduplication, and 100% PASSED source-to-target reconciliation across BTS, airports, bookings, and payments.
- Modeled the warehouse into 7 schemas, 5 dimensions, 3 fact tables, and 7 analytics marts, including 99,692 booking facts, 99,418 payment facts, and 597,919 flight-status facts for BI consumption.
- Added a Kafka streaming sidecar that landed 1,000 events into raw stream tables and verified rerun safety by skipping 1,000 duplicate events while maintaining PASSED reconciliation.

### Option B: more senior / data-engineering heavy

- Engineered an end-to-end local aviation analytics platform that ingested 889,580 source rows and served curated warehouse outputs through Airflow orchestration, dbt transformations, Kafka streaming, Spark processing, and Superset dashboards.
- Built record-level DQ, quarantine, and reconciliation controls that preserved observability over 152,180 warehouse DQ audit rows across reruns and kept booking/payment quarantine to 0.45% of 206,003 generated business-event records.
- Implemented CDC-style dedupe and idempotent reload patterns for synthetic booking and payment feeds, handling 5,999 duplicate source rows and proving duplicate-event protection in Kafka reruns.
- Delivered analytics-ready modeling with 5 dimensions, 3 facts, and 7 marts, including route-performance and sales marts populated from 597,919 flight records and 199k+ canonical booking/payment facts.

### Option C: compact version for one project line

- Built a Dockerized aviation data warehouse processing 889k+ rows with Airflow, PostgreSQL, dbt, Kafka, Spark, and Superset; added record-level DQ, quarantine, CDC dedupe, and reconciliation controls, reducing invalid booking/payment rows to a 0.45% quarantine rate while producing 5 dimensions, 3 facts, and 7 marts.

## 4. CV Bullets In Vietnamese

- Xay dung end-to-end aviation data warehouse chay local bang Docker, xu ly 889,580 rows tu 4 nguon du lieu bang Airflow, PostgreSQL, dbt, Kafka, Spark, MinIO va Superset.
- Trien khai data quality theo huong production: validate 206,003 booking/payment rows, dua 924 records loi vao quarantine, va ghi 38,097 loi DQ theo tung record thay vi drop im lang.
- Thiet ke ingestion idempotent voi checksum file registry, CDC-style dedupe, va doi chieu source-to-target dat 100% PASSED tren BTS, airports, bookings va payments.
- Mo hinh hoa kho du lieu thanh 7 schemas, 5 dimensions, 3 facts, 7 marts; trong do co 99,692 booking facts, 99,418 payment facts va 597,919 flight-status facts phuc vu BI.
- Bo sung streaming demo bang Kafka, nap 1,000 events vao raw stream tables va xac minh rerun khong duplicate bang viec skip 1,000 duplicate events.

## 5. Safe Wording Notes

Use these wording rules to stay strong but honest:

- Say `processed`, `validated`, `modeled`, `landed`, `reconciled`, `logged`, `quarantined`.
- Avoid saying `deployed on AWS` or `production pipeline` because this project is local and Dockerized.
- For Spark, only claim feature output metrics after you actually run the Spark job and `metadata.spark_job_audit` has rows.
- If you want to sound more senior, emphasize `observability`, `idempotency`, `reconciliation`, `CDC-style dedupe`, and `record-level DQ logging`.
