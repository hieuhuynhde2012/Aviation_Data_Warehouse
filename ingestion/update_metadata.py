import json
import os
import time
from pathlib import Path

import psycopg2

from ingestion.config import QUALITY_DIR, warehouse_config


def update_file_registry(status: str = "LOADED") -> int:
    report_path = QUALITY_DIR / "validation_report.json"
    if not report_path.exists():
        return 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    query = """
        insert into metadata.pipeline_file_registry (
            file_id, source_name, file_path, partition_key, file_size_bytes,
            row_count, checksum_md5, load_status, started_at, completed_at, error_message
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, current_timestamp, current_timestamp, %s)
        on conflict (file_id) do update set
            row_count = excluded.row_count,
            checksum_md5 = excluded.checksum_md5,
            load_status = excluded.load_status,
            completed_at = excluded.completed_at,
            error_message = excluded.error_message
    """
    with psycopg2.connect(**warehouse_config()) as conn, conn.cursor() as cur:
        for item in report:
            file_id = item["checksum_md5"]
            cur.execute(
                query,
                (
                    file_id,
                    item["source_name"],
                    item["file_path"],
                    Path(item["file_path"]).parent.name,
                    item["file_size_bytes"],
                    item["row_count"],
                    item["checksum_md5"],
                    status,
                    "; ".join(item.get("missing_columns", [])) or None,
                ),
            )
        conn.commit()
    return len(report)


def update_run_log(dbt_status: str = "SUCCESS", test_status: str = "SUCCESS") -> None:
    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or f"manual-{int(time.time())}"
    dag_id = os.getenv("AIRFLOW_CTX_DAG_ID", "manual")
    execution_date = os.getenv("AIRFLOW_CTX_EXECUTION_DATE")
    query = """
        insert into metadata.pipeline_run_log (
            run_id, dag_id, execution_date, source_count, total_input_rows,
            total_loaded_rows, dbt_status, test_status, runtime_seconds
        )
        select
            %s, %s, coalesce(%s::timestamp, current_timestamp),
            count(*), coalesce(sum(row_count), 0), coalesce(sum(row_count), 0),
            %s, %s, null
        from metadata.pipeline_file_registry
        on conflict (run_id) do update set
            total_input_rows = excluded.total_input_rows,
            total_loaded_rows = excluded.total_loaded_rows,
            dbt_status = excluded.dbt_status,
            test_status = excluded.test_status
    """
    with psycopg2.connect(**warehouse_config()) as conn, conn.cursor() as cur:
        cur.execute(query, (run_id, dag_id, execution_date, dbt_status, test_status))
        conn.commit()


def main() -> None:
    print(f"registry_rows={update_file_registry()}")
    update_run_log()


if __name__ == "__main__":
    main()
