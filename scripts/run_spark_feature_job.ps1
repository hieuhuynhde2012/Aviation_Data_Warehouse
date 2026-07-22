param(
    [string]$Master = "spark://spark-master:7077",
    [string]$PostgresPackage = "org.postgresql:postgresql:42.7.3"
)

$ErrorActionPreference = "Stop"

$DriverMemory = if ($env:SPARK_DRIVER_MEMORY) { $env:SPARK_DRIVER_MEMORY } else { "1g" }
$ExecutorMemory = if ($env:SPARK_EXECUTOR_MEMORY) { $env:SPARK_EXECUTOR_MEMORY } else { "1g" }

Write-Host "Starting Spark services..."
docker compose up -d spark-master spark-worker spark-submit

Write-Host "Creating Spark output tables..."
Get-Content warehouse/05_spark_tables.sql | docker compose exec -T postgres-warehouse psql -U aviation -d aviation_dw

Write-Host "Submitting Spark feature job..."
docker compose exec spark-submit /opt/spark/bin/spark-submit `
  --master $Master `
  --packages $PostgresPackage `
  --driver-memory $DriverMemory `
  --executor-memory $ExecutorMemory `
  /opt/spark/work-dir/jobs/aviation_feature_job.py

Write-Host "Latest Spark audit rows:"
docker compose exec -T postgres-warehouse psql -U aviation -d aviation_dw -P pager=off -c "select job_name, status, source_flight_rows, source_booking_rows, output_route_delay_rows, output_route_booking_rows, completed_at, error_message from metadata.spark_job_audit order by completed_at desc limit 5;"
