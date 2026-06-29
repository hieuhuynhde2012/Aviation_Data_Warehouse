from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path

import psycopg2

from ingestion.config import QUALITY_DIR, QUARANTINE_DIR, warehouse_config


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def persist_record_errors(run_id: str) -> int:
    rows = _read_csv(QUALITY_DIR / "record_errors.csv")
    query = """
        insert into metadata.dq_record_errors (
            error_id, run_id, source_name, file_path, row_number, record_key,
            severity, rule_name, column_name, bad_value, error_message, action_taken
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (error_id) do nothing
    """
    with psycopg2.connect(**warehouse_config()) as conn, conn.cursor() as cur:
        for row in rows:
            error_id = hashlib.md5(f"{run_id}|{row['error_id']}".encode("utf-8")).hexdigest()
            cur.execute(
                query,
                (
                    error_id,
                    run_id,
                    row["source_name"],
                    row["file_path"],
                    int(row["row_number"]) if row.get("row_number") else None,
                    row.get("record_key"),
                    row["severity"],
                    row["rule_name"],
                    row.get("column_name"),
                    row.get("bad_value"),
                    row["error_message"],
                    row["action_taken"],
                ),
            )
        conn.commit()
    return len(rows)


def persist_quarantine(run_id: str) -> int:
    total = 0
    query = """
        insert into quarantine.invalid_records (
            quarantine_id, run_id, source_name, file_path, row_number, record_key, reason, raw_record
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        on conflict (quarantine_id) do nothing
    """
    with psycopg2.connect(**warehouse_config()) as conn, conn.cursor() as cur:
        for source_name in ["bookings", "payments"]:
            path = QUARANTINE_DIR / source_name / "invalid_records.csv"
            for row in _read_csv(path):
                raw_record = dict(row)
                quarantine_id = hashlib.md5(
                    f"{run_id}|{source_name}|{row.get('row_number')}|{row.get('record_key')}".encode("utf-8")
                ).hexdigest()
                cur.execute(
                    query,
                    (
                        quarantine_id,
                        run_id,
                        source_name,
                        str(path),
                        int(row["row_number"]) if row.get("row_number") else None,
                        row.get("record_key"),
                        row.get("reason"),
                        json.dumps(raw_record),
                    ),
                )
                total += 1
        conn.commit()
    return total


def main() -> None:
    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    print(f"dq_record_errors={persist_record_errors(run_id)}")
    print(f"quarantined_records={persist_quarantine(run_id)}")


if __name__ == "__main__":
    main()
