# Performance Report

Fill this after the first successful local run.

| Metric | Value |
| --- | --- |
| BTS rows loaded | TBD |
| Airport rows loaded | TBD |
| Synthetic booking rows generated | TBD |
| Synthetic payment rows generated | TBD |
| Booking source rows | 103,022 |
| Booking quarantined rows | 322 |
| Booking expected/actual raw rows | 102,700 / 102,700 |
| Payment source rows | 102,981 |
| Payment quarantined rows | 602 |
| Payment expected/actual raw rows | 102,379 / 102,379 |
| dbt canonical fact_booking rows | 99,692 |
| dbt canonical fact_payment rows | 99,418 |
| Airflow DAG runtime | TBD |
| dbt run runtime | TBD |
| dbt test runtime | TBD |
| Failed dbt tests | TBD |
| Warehouse size | TBD |

## Screenshot Checklist

- Airflow DAG graph success.
- MinIO raw bucket partitions.
- PostgreSQL schemas and tables.
- dbt lineage or docs.
- Superset dashboard tabs.
