# Performance Report

Snapshot date: 2026-07-22

Numbers below are taken from the current local project state, validated files, and warehouse tables.

| Metric | Value |
| --- | --- |
| Total source rows across 4 sources | 889,580 |
| BTS rows loaded | 597,919 |
| Airport rows loaded | 85,658 |
| Synthetic booking rows generated | 103,022 |
| Synthetic payment rows generated | 102,981 |
| Booking source rows | 103,022 |
| Booking quarantined rows | 322 |
| Booking quarantine rate | 0.31% |
| Booking expected/actual raw rows | 102,700 / 102,700 |
| Payment source rows | 102,981 |
| Payment quarantined rows | 602 |
| Payment quarantine rate | 0.58% |
| Payment expected/actual raw rows | 102,379 / 102,379 |
| Combined valid booking/payment rows | 205,079 |
| Combined quarantined booking/payment rows | 924 |
| Combined quarantine rate | 0.45% |
| Booking duplicate rows generated | 3,018 |
| Payment duplicate rows generated | 2,981 |
| Dirty-standardization rows generated | 43,707 |
| Latest validation record-level DQ errors | 38,097 |
| Warehouse-persisted DQ error rows across reruns | 152,180 |
| dbt canonical fact_booking rows | 99,692 |
| dbt canonical fact_payment rows | 99,418 |
| fact_flight_status rows | 597,919 |
| mart_route_performance rows | 5,964 |
| mart_sales_performance rows | 878 |
| Streaming events landed | 1,000 rows |
| Typical streaming reconciliation | 500 produced / 500 consumed per topic |
| Duplicate streaming handling verified | 1,000 duplicates skipped in rerun demo |
| Warehouse schemas | 7 (`raw`, `staging`, `intermediate`, `mart`, `metadata`, `reference`, `quarantine`) |
| Airflow DAGs | 3 |
| dbt dimensions / facts / marts | 5 / 3 / 7 |
| dbt custom SQL tests | 2 |
| Spark output tables pre-created | 3 |
| Local service containers running | 13 |

## CV-Ready Highlights

- Processed 889,580 rows across public flight operations, airport reference, and synthetic booking/payment sources in a Dockerized local warehouse stack.
- Preserved data quality visibility by logging 38,097 record-level DQ issues in the latest validation batch and 152,180 DQ audit rows across reruns instead of silently dropping bad records.
- Quarantined only 924 invalid business-event rows out of 206,003 booking/payment source rows while keeping source-to-target reconciliation at 100% PASSED for all four sources.
- Landed 1,000 Kafka events into raw stream tables and verified idempotent rerun behavior with 1,000 duplicate events skipped across booking and payment topics.

## Screenshot Checklist

- Airflow DAG graph success.
- MinIO raw bucket partitions.
- PostgreSQL schemas and tables.
- dbt lineage or docs.
- Superset dashboard tabs.
