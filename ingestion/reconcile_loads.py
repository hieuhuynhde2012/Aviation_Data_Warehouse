from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql

from ingestion.config import DATA_DIR, QUALITY_DIR, VALIDATED_DIR, warehouse_config


SOURCE_TABLES = {
    "bts": ("raw_bts_flights", lambda: sorted((DATA_DIR / "input" / "bts").glob("*.csv"))[-1]),
    "airports": ("raw_airports", lambda: DATA_DIR / "input" / "ourairports" / "airports.csv"),
    "bookings": ("raw_bookings", lambda: VALIDATED_DIR / "bookings" / "bookings_valid.csv"),
    "payments": ("raw_payments", lambda: VALIDATED_DIR / "payments" / "payments_valid.csv"),
}


def _count_raw_rows(conn, table: str, path: Path) -> int:
    query = sql.SQL("select count(*) from raw.{} where source_file = %s").format(sql.Identifier(table))
    with conn.cursor() as cur:
        cur.execute(query, (str(path),))
        return int(cur.fetchone()[0])


def reconcile() -> list[dict]:
    report_path = QUALITY_DIR / "validation_report.json"
    if not report_path.exists():
        raise FileNotFoundError(report_path)
    report = {item["source_name"]: item for item in json.loads(report_path.read_text(encoding="utf-8"))}
    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    rows = []
    with psycopg2.connect(**warehouse_config()) as conn, conn.cursor() as cur:
        for source_name, (table, path_fn) in SOURCE_TABLES.items():
            item = report[source_name]
            profile = item.get("quality_profile", {})
            source_rows = int(item["row_count"])
            quarantined_rows = int(profile.get("quarantined_rows", 0))
            expected_raw_rows = int(profile.get("valid_rows", source_rows))
            path = path_fn()
            actual_raw_rows = _count_raw_rows(conn, table, path)
            difference = actual_raw_rows - expected_raw_rows
            status = "PASSED" if difference == 0 else "FAILED"
            reconciliation_id = hashlib.md5(f"{run_id}|{source_name}|{table}".encode("utf-8")).hexdigest()
            cur.execute(
                """
                insert into metadata.reconciliation_summary (
                    reconciliation_id, run_id, source_name, source_file_rows,
                    quarantined_rows, expected_raw_rows, actual_raw_rows,
                    target_table, status, difference
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (reconciliation_id) do update set
                    source_file_rows = excluded.source_file_rows,
                    quarantined_rows = excluded.quarantined_rows,
                    expected_raw_rows = excluded.expected_raw_rows,
                    actual_raw_rows = excluded.actual_raw_rows,
                    status = excluded.status,
                    difference = excluded.difference,
                    created_at = current_timestamp
                """,
                (
                    reconciliation_id,
                    run_id,
                    source_name,
                    source_rows,
                    quarantined_rows,
                    expected_raw_rows,
                    actual_raw_rows,
                    f"raw.{table}",
                    status,
                    difference,
                ),
            )
            rows.append(
                {
                    "source_name": source_name,
                    "target_table": f"raw.{table}",
                    "source_file_rows": source_rows,
                    "quarantined_rows": quarantined_rows,
                    "expected_raw_rows": expected_raw_rows,
                    "actual_raw_rows": actual_raw_rows,
                    "status": status,
                }
            )
        conn.commit()
    failed = [row for row in rows if row["status"] != "PASSED"]
    if failed:
        raise RuntimeError(f"Reconciliation failed: {failed}")
    return rows


def main() -> None:
    for row in reconcile():
        print(row)


if __name__ == "__main__":
    main()
