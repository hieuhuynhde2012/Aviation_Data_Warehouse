# Spark Processing Layer

Spark is added as a focused distributed processing layer for feature engineering and large CSV aggregation.

## Why Spark Is Useful In This Project

The project already uses dbt for warehouse modeling. Spark is not added to replace dbt. It is used for a different purpose:

- Read large raw/source files outside the warehouse.
- Compute route/airline-level delay features from BTS flight data.
- Compute route-level booking features from synthetic booking events.
- Write reusable feature tables back to PostgreSQL.
- Record Spark job audit metadata.

This creates a realistic split:

| Tool | Role |
| --- | --- |
| Airflow | Orchestration |
| Kafka | Streaming event ingestion |
| Spark | Distributed batch feature processing |
| dbt | Warehouse transformations and tests |
| PostgreSQL | Warehouse storage |
| Superset | BI/dashboard layer |

## Spark Services

| Service | URL / Purpose |
| --- | --- |
| `spark-master` | Spark standalone master |
| `spark-worker` | Spark worker |
| Spark Master UI | <http://localhost:8081> |
| Spark Worker UI | <http://localhost:8082> |
| `spark-submit` | Utility container for submitting jobs |

## Job

Main job:

```text
spark/jobs/aviation_feature_job.py
```

Inputs:

```text
data/input/bts/bts_on_time_2026_04.csv
data/generated/bookings/bookings.csv
```

Outputs:

```text
mart.spark_route_delay_features
mart.spark_route_booking_features
metadata.spark_job_audit
```

## Run Commands

Start Spark:

```powershell
docker compose up -d spark-master spark-worker spark-submit
```

Create tables in an existing warehouse volume:

```powershell
Get-Content warehouse/05_spark_tables.sql | docker compose exec -T postgres-warehouse psql -U aviation -d aviation_dw
```

Submit the Spark job:

```powershell
docker compose exec spark-submit spark-submit `
  --master spark://spark-master:7077 `
  --packages org.postgresql:postgresql:42.7.3 `
  --driver-memory 1g `
  --executor-memory 1g `
  /opt/bitnami/spark/jobs/aviation_feature_job.py
```

Check output:

```powershell
docker compose exec -T postgres-warehouse psql -U aviation -d aviation_dw -P pager=off -c "select status, source_flight_rows, source_booking_rows, output_route_delay_rows, output_route_booking_rows, completed_at from metadata.spark_job_audit order by completed_at desc limit 5;"
```

## Interview Talking Point

Spark is used here for distributed feature processing over large operational flight files. The output is not a toy print statement; it lands back into the warehouse as reusable mart/feature tables and writes an audit record for observability.
