from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

STREAM_LIMIT = "${STREAM_EVENT_LIMIT:-500}"
MAX_EVENTS = "$(( ${STREAM_EVENT_LIMIT:-500} * 2 ))"
PYTHONPATH_PREFIX = "export PYTHONPATH=/opt/airflow:${PYTHONPATH:-}; "


with DAG(
    dag_id="aviation_streaming_demo",
    description="Kafka booking/payment event streaming demo with idempotent landing and reconciliation.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["data-engineering", "streaming", "kafka"],
) as dag:
    ensure_streaming_tables = BashOperator(
        task_id="ensure_streaming_tables",
        bash_command=(
            PYTHONPATH_PREFIX
            + "python -c 'from streaming.db import ensure_streaming_tables; ensure_streaming_tables()'"
        ),
    )

    produce_booking_payment_events = BashOperator(
        task_id="produce_booking_payment_events",
        bash_command=PYTHONPATH_PREFIX + f"python -m streaming.producer --limit {STREAM_LIMIT}",
    )

    consume_to_raw_stream_tables = BashOperator(
        task_id="consume_to_raw_stream_tables",
        bash_command=PYTHONPATH_PREFIX + f"python -m streaming.consumer --max-events {MAX_EVENTS} --timeout-ms 30000",
    )

    reconcile_streaming_counts = BashOperator(
        task_id="reconcile_streaming_counts",
        bash_command=(
            PYTHONPATH_PREFIX
            +
            "python -m streaming.reconcile_streaming "
            "--run-id {{ run_id }} "
            f"--booking-produced {STREAM_LIMIT} "
            f"--payment-produced {STREAM_LIMIT}"
        ),
    )

    (
        ensure_streaming_tables
        >> produce_booking_payment_events
        >> consume_to_raw_stream_tables
        >> reconcile_streaming_counts
    )
