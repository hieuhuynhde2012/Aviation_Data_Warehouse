from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="aviation_backfill_pipeline",
    description="Backfill wrapper that can trigger the daily aviation pipeline for historical partitions.",
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    start_date=datetime(2026, 6, 29),
    schedule=None,
    catchup=False,
    tags=["aviation", "backfill"],
) as dag:
    TriggerDagRunOperator(
        task_id="trigger_daily_pipeline",
        trigger_dag_id="aviation_daily_pipeline",
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
    )
