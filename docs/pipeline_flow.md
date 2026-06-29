# Pipeline Flow

Airflow DAG: `aviation_daily_pipeline`

```text
start
  -> generate_or_download_sources
  -> upload_raw_files_to_minio
  -> validate_raw_schema_and_row_count
  -> persist_dq_results
  -> load_raw_csv_to_postgres
  -> reconcile_source_to_raw
  -> run_dbt_staging
  -> run_dbt_intermediate
  -> run_dbt_marts
  -> run_dbt_tests
  -> update_pipeline_metadata
  -> refresh_superset_datasets
  -> send_pipeline_summary
end
```

## Failure Behavior

- Source download and generation tasks retry twice.
- Validation fails on missing required columns or zero-row files.
- Validation records dirty-data profiling counts for duplicates, missing optional fields, status drift, and value anomalies.
- Hard-error records are quarantined and logged at record level.
- Reconciliation fails the DAG when expected raw rows do not match actual loaded rows.
- Warehouse load uses a database transaction.
- dbt failures stop downstream mart/test tasks.
- Metadata update uses `all_done` so failed runs can still be traceable.
