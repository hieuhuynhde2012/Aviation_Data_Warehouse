from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


with DAG(
    dag_id="aviation_daily_pipeline",
    description="Dockerized aviation data warehouse pipeline: sources -> MinIO -> Postgres -> dbt marts.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 6, 29),
    schedule="0 2 * * *",
    catchup=False,
    tags=["aviation", "warehouse", "portfolio"],
) as dag:
    start = EmptyOperator(task_id="start")

    generate_or_download_sources = BashOperator(
        task_id="generate_or_download_sources",
        bash_command=(
            "cd /opt/airflow && "
            "python -m ingestion.download_sources && "
            "python -m ingestion.generate_booking_events"
        ),
    )

    upload_raw_files_to_minio = BashOperator(
        task_id="upload_raw_files_to_minio",
        bash_command="cd /opt/airflow && python -m ingestion.load_to_minio",
    )

    validate_raw_schema_and_row_count = BashOperator(
        task_id="validate_raw_schema_and_row_count",
        bash_command="cd /opt/airflow && python -m ingestion.validate_files",
    )

    persist_dq_results = BashOperator(
        task_id="persist_dq_results",
        bash_command="cd /opt/airflow && python -m ingestion.persist_quality_results",
    )

    load_raw_csv_to_postgres = BashOperator(
        task_id="load_raw_csv_to_postgres",
        bash_command="cd /opt/airflow && python -m ingestion.load_to_warehouse",
    )

    reconcile_source_to_raw = BashOperator(
        task_id="reconcile_source_to_raw",
        bash_command="cd /opt/airflow && python -m ingestion.reconcile_loads",
    )

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command="cd /opt/airflow/dbt && dbt run --select staging",
    )

    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command="cd /opt/airflow/dbt && dbt run --select intermediate dimensions facts",
    )

    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command="cd /opt/airflow/dbt && dbt run --select marts",
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /opt/airflow/dbt && dbt test",
    )

    update_pipeline_metadata = BashOperator(
        task_id="update_pipeline_metadata",
        bash_command="cd /opt/airflow && python -m ingestion.update_metadata",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    refresh_superset_datasets = EmptyOperator(task_id="refresh_superset_datasets")
    send_pipeline_summary = EmptyOperator(task_id="send_pipeline_summary")
    end = EmptyOperator(task_id="end")

    start >> generate_or_download_sources >> upload_raw_files_to_minio >> validate_raw_schema_and_row_count
    validate_raw_schema_and_row_count >> persist_dq_results >> load_raw_csv_to_postgres >> reconcile_source_to_raw
    reconcile_source_to_raw >> run_dbt_staging
    run_dbt_staging >> run_dbt_intermediate >> run_dbt_marts >> run_dbt_tests
    run_dbt_tests >> refresh_superset_datasets >> send_pipeline_summary >> end
    run_dbt_tests >> update_pipeline_metadata
