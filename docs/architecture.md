# Architecture

![Dockerized Aviation Data Warehouse Pipeline architecture](architecture_diagram.svg)

## Local-To-Cloud Mapping

| Cloud-style component | Local replacement | Role |
| --- | --- | --- |
| Amazon S3 | MinIO | S3-compatible raw object storage |
| Redshift or Snowflake | PostgreSQL | Local analytical warehouse |
| Apache Airflow | Apache Airflow | Orchestration, scheduling, retries |
| AWS Glue | Python validation + dbt staging | Schema checks and standardization |
| EMR or Dataproc | Spark standalone | Distributed feature processing and large CSV aggregation |
| AWS Lambda | Airflow task | Event-style task execution |
| DynamoDB | PostgreSQL metadata schema | File and run tracking |
| Tableau | Apache Superset | BI dashboard |

## Raw Object Layout

```text
s3://aviation-raw/bts_on_time/year=2026/month=04/bts_on_time_2026_04.csv
s3://aviation-raw/ourairports/snapshot_date=<date>/airports.csv
s3://aviation-raw/booking_events/dt=<date>/bookings.csv
s3://aviation-raw/payment_events/dt=<date>/payments.csv
```

## Spark Processing

Spark is used as a focused processing layer for route-level feature generation:

```text
BTS CSV + generated bookings
        |
        v
Spark standalone cluster
        |
        v
mart.spark_route_delay_features
mart.spark_route_booking_features
metadata.spark_job_audit
```

Spark complements dbt rather than replacing it: Spark handles distributed file processing and feature aggregation; dbt handles warehouse modeling, tests, and marts.
