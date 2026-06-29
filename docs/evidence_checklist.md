# Evidence Checklist

Use this checklist when recording a portfolio demo or preparing screenshots for a recruiter.

## Runtime Proof

| Evidence | Where to capture | What it proves |
| --- | --- | --- |
| Airflow DAG success | <http://localhost:8080> | End-to-end orchestration runs successfully |
| Airflow task graph | `aviation_daily_pipeline` graph view | Pipeline has clear stages: generate, upload, validate, load, reconcile, dbt, metadata |
| Airflow custom image | `docker compose ps airflow-webserver airflow-scheduler` | Dependencies are baked into `aviation-airflow:2.9.3-python3.11` instead of installed at runtime |
| MinIO raw buckets | <http://localhost:9001> | Source files are landed in S3-style object storage |
| Superset dashboard | <http://localhost:8088> | Marts are consumed by BI dashboards |
| dbt docs lineage | `docker compose exec dbt dbt docs generate` | Transformations have lineage and documentation |
| PostgreSQL reconciliation query | `sql/demo_queries.sql` query 1 | Source-to-target row counts are reconciled |
| PostgreSQL DQ query | `sql/demo_queries.sql` query 2 | Record-level DQ errors are logged |
| PostgreSQL quarantine query | `sql/demo_queries.sql` query 3 | Invalid records are isolated, not silently dropped |
| Raw load audit query | `sql/demo_queries.sql` query 4 | Reruns are idempotent and checksum-aware |

## Current Local Run Snapshot

Latest observed Airflow runs:

| Run ID | State | Execution date | End date |
| --- | --- | --- | --- |
| `manual__2026-06-29T09:24:41+00:00` | success | `2026-06-29T09:24:41+00:00` | `2026-06-29T09:35:41.550129+00:00` |
| `manual__2026-06-29T08:22:55+00:00` | success | `2026-06-29T08:22:55+00:00` | `2026-06-29T08:29:51.628706+00:00` |
| `manual__2026-06-29T08:04:01.716430+00:00` | success | `2026-06-29T08:04:01.716430+00:00` | `2026-06-29T08:12:10.737845+00:00` |

Latest reconciliation snapshot:

| Source | Source rows | Quarantined | Expected raw | Actual raw | Status |
| --- | ---: | ---: | ---: | ---: | --- |
| airports | 85,658 | 0 | 85,658 | 85,658 | PASSED |
| bookings | 103,022 | 322 | 102,700 | 102,700 | PASSED |
| bts | 597,919 | 0 | 597,919 | 597,919 | PASSED |
| payments | 102,981 | 602 | 102,379 | 102,379 | PASSED |

Current warehouse object counts:

| Object | Rows |
| --- | ---: |
| `mart.fact_booking` | 99,692 |
| `mart.fact_payment` | 99,418 |
| `mart.mart_sales_performance` | 878 |
| `quarantine.invalid_records` | 2,564 |

Top DQ rules observed:

| Severity | Rule | Records |
| --- | --- | ---: |
| WARN | `standardization_required` | 85,665 |
| WARN | `duplicate_event` | 17,997 |
| WARN | `missing_optional` | 6,240 |
| WARN | `negative_value` | 1,197 |
| ERROR | `required_not_null` | 1,173 |
| ERROR | `valid_numeric` | 633 |
| WARN | `refund_status_mismatch` | 420 |
| ERROR | `relationship_missing` | 416 |
| ERROR | `valid_date` | 342 |

## Commands To Reproduce Evidence

```powershell
docker compose up -d --build
```

Trigger `aviation_daily_pipeline` in Airflow, then run:

```powershell
Get-Content superset/setup_assets.py | docker compose exec -T superset python -
docker compose exec dbt dbt docs generate
docker compose exec -T airflow-scheduler airflow dags list-runs -d aviation_daily_pipeline --no-backfill -o table
Get-Content sql/demo_queries.sql | docker compose exec -T postgres-warehouse psql -U aviation -d aviation_dw
```

If you prefer a SQL client, paste individual queries from `sql/demo_queries.sql` into DBeaver, DataGrip, psql, or Superset SQL Lab.

## Verified Local Runtime

The Airflow services were rebuilt and recreated with the custom image:

```text
aviation-data-warehouse-airflow-scheduler-1   aviation-airflow:2.9.3-python3.11
aviation-data-warehouse-airflow-webserver-1   aviation-airflow:2.9.3-python3.11
```

Package versions verified inside the scheduler:

```text
pandas 2.1.4
boto3 1.34.131
dbt 1.8.2
```

Airflow health endpoint returned HTTP 200 with healthy metadatabase and scheduler.
