from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from ingestion.config import DATA_DIR, QUALITY_DIR, QUARANTINE_DIR, VALIDATED_DIR


REQUIRED_COLUMNS = {
    "bts": {"FlightDate", "Reporting_Airline", "Origin", "Dest", "Cancelled", "Diverted"},
    "airports": {"ident", "type", "name", "iso_country", "iso_region", "iata_code"},
    "bookings": {"booking_id", "customer_id", "flight_id", "flight_date", "ticket_price"},
    "payments": {"payment_id", "booking_id", "payment_status", "amount"},
}

BOOKING_STATUS_TERMS = {
    "CONFIRMED",
    "CONF",
    "BOOKED",
    "COMPLETE",
    "CANCELLED",
    "CANCELED",
    "VOID",
    "CNCL",
    "PENDING",
    "PEND",
    "IN_PROGRESS",
    "AWAITING_PAYMENT",
}
PAYMENT_STATUS_TERMS = {"PAID", "SUCCESS", "SETTLED", "FAILED", "DECLINED", "ERROR", "FAIL", "REFUNDED", "REFUND", "CHARGEBACK"}


def md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _error_id(source_name: str, row_number: int, rule_name: str, record_key: str, bad_value: str = "") -> str:
    raw = f"{source_name}|{row_number}|{rule_name}|{record_key}|{bad_value}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _normal_token(value: Optional[str]) -> str:
    return (value or "").strip().upper().replace(" ", "_")


def _is_dirty_token(value: str) -> bool:
    if value is None:
        return False
    stripped = value.strip()
    return bool(value != stripped or " " in stripped or stripped != stripped.upper())


def _is_date(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
        return True
    except (TypeError, ValueError):
        return False


def _is_number(value: str) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _dq_error(
    source_name: str,
    file_path: Path,
    row_number: int,
    record_key: str,
    severity: str,
    rule_name: str,
    column_name: str,
    bad_value: str,
    error_message: str,
    action_taken: str,
) -> dict:
    return {
        "error_id": _error_id(source_name, row_number, rule_name, record_key, bad_value),
        "source_name": source_name,
        "file_path": str(file_path),
        "row_number": row_number,
        "record_key": record_key,
        "severity": severity,
        "rule_name": rule_name,
        "column_name": column_name,
        "bad_value": bad_value,
        "error_message": error_message,
        "action_taken": action_taken,
    }


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def validate_booking_rows(path: Path) -> tuple[list[dict], list[dict], list[dict], dict]:
    valid_rows = []
    invalid_rows = []
    errors = []
    seen = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        for row_number, row in enumerate(reader, start=2):
            record_key = _normal_token(row.get("booking_id")) or f"row-{row_number}"
            row_errors = []
            if not row.get("booking_id", "").strip():
                row_errors.append(("ERROR", "required_not_null", "booking_id", row.get("booking_id", ""), "Missing booking_id", "QUARANTINED"))
            if not row.get("flight_id", "").strip():
                row_errors.append(("ERROR", "required_not_null", "flight_id", row.get("flight_id", ""), "Missing flight_id", "QUARANTINED"))
            if not _is_date(row.get("flight_date", "")):
                row_errors.append(("ERROR", "valid_date", "flight_date", row.get("flight_date", ""), "Invalid flight_date", "QUARANTINED"))
            if not _is_number(row.get("ticket_price", "")):
                row_errors.append(("ERROR", "valid_numeric", "ticket_price", row.get("ticket_price", ""), "Invalid ticket_price", "QUARANTINED"))

            status = _normal_token(row.get("booking_status"))
            payment_status = _normal_token(row.get("payment_status"))
            if status and status not in BOOKING_STATUS_TERMS:
                row_errors.append(("WARN", "controlled_vocabulary", "booking_status", row.get("booking_status", ""), "Unknown booking status", "STANDARDIZED_TO_PENDING"))
            if payment_status and payment_status not in PAYMENT_STATUS_TERMS:
                row_errors.append(("WARN", "controlled_vocabulary", "payment_status", row.get("payment_status", ""), "Unknown payment status", "STANDARDIZED_TO_FAILED"))
            if _is_dirty_token(row.get("booking_status", "")) or _is_dirty_token(row.get("booking_channel", "")):
                row_errors.append(("WARN", "standardization_required", "booking_status|booking_channel", "", "Casing/spacing drift detected", "STANDARDIZED_IN_DBT"))
            if _is_number(row.get("ticket_price", "")) and float(row.get("ticket_price", "0")) < 0:
                row_errors.append(("WARN", "negative_value", "ticket_price", row.get("ticket_price", ""), "Negative ticket price", "ABS_CORRECTION_IN_DBT"))
            if not row.get("customer_id", "").strip():
                row_errors.append(("WARN", "missing_optional", "customer_id", "", "Missing optional customer_id", "DEFAULT_UNKNOWN_CUSTOMER"))
            if record_key in seen:
                row_errors.append(("WARN", "duplicate_event", "booking_id", record_key, "Duplicate booking event", "CDC_KEEP_LATEST_IN_DBT"))
            seen.add(record_key)

            hard_errors = [item for item in row_errors if item[0] == "ERROR"]
            for severity, rule, column, bad_value, message, action in row_errors:
                errors.append(_dq_error("bookings", path, row_number, record_key, severity, rule, column, bad_value, message, action))
            if hard_errors:
                invalid_rows.append({"row_number": row_number, "record_key": record_key, "reason": "; ".join(item[4] for item in hard_errors), **row})
            else:
                valid_rows.append(row)
    return valid_rows, invalid_rows, errors, {"fieldnames": fieldnames, "duplicates": len(valid_rows) - len({r.get("booking_id", "").strip().upper() for r in valid_rows})}


def validate_payment_rows(path: Path) -> tuple[list[dict], list[dict], list[dict], dict]:
    valid_rows = []
    invalid_rows = []
    errors = []
    seen = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        for row_number, row in enumerate(reader, start=2):
            record_key = _normal_token(row.get("payment_id")) or f"row-{row_number}"
            row_errors = []
            if not row.get("payment_id", "").strip():
                row_errors.append(("ERROR", "required_not_null", "payment_id", row.get("payment_id", ""), "Missing payment_id", "QUARANTINED"))
            if not row.get("booking_id", "").strip():
                row_errors.append(("ERROR", "required_not_null", "booking_id", row.get("booking_id", ""), "Missing booking_id", "QUARANTINED"))
            if not _is_number(row.get("amount", "")):
                row_errors.append(("ERROR", "valid_numeric", "amount", row.get("amount", ""), "Invalid amount", "QUARANTINED"))

            payment_status = _normal_token(row.get("payment_status"))
            if payment_status and payment_status not in PAYMENT_STATUS_TERMS:
                row_errors.append(("WARN", "controlled_vocabulary", "payment_status", row.get("payment_status", ""), "Unknown payment status", "STANDARDIZED_TO_FAILED"))
            if _is_dirty_token(row.get("payment_status", "")) or _is_dirty_token(row.get("payment_method", "")):
                row_errors.append(("WARN", "standardization_required", "payment_status|payment_method", "", "Casing/spacing drift detected", "STANDARDIZED_IN_DBT"))
            if record_key in seen:
                row_errors.append(("WARN", "duplicate_event", "payment_id", record_key, "Duplicate payment event", "CDC_KEEP_LATEST_IN_DBT"))
            if _is_number(row.get("refund_amount", "")) and float(row.get("refund_amount", "0")) > 0 and "REFUND" not in payment_status:
                row_errors.append(("WARN", "refund_status_mismatch", "refund_amount|payment_status", row.get("payment_status", ""), "Refund amount without refund status", "FLAGGED_FOR_RECONCILIATION"))
            seen.add(record_key)

            hard_errors = [item for item in row_errors if item[0] == "ERROR"]
            for severity, rule, column, bad_value, message, action in row_errors:
                errors.append(_dq_error("payments", path, row_number, record_key, severity, rule, column, bad_value, message, action))
            if hard_errors:
                invalid_rows.append({"row_number": row_number, "record_key": record_key, "reason": "; ".join(item[4] for item in hard_errors), **row})
            else:
                valid_rows.append(row)
    return valid_rows, invalid_rows, errors, {"fieldnames": fieldnames, "duplicates": len(valid_rows) - len({r.get("payment_id", "").strip().upper() for r in valid_rows})}


def enforce_payment_booking_relationship(report: list[dict], errors: list[dict]) -> None:
    booking_valid_path = VALIDATED_DIR / "bookings" / "bookings_valid.csv"
    payment_valid_path = VALIDATED_DIR / "payments" / "payments_valid.csv"
    payment_quarantine_path = QUARANTINE_DIR / "payments" / "invalid_records.csv"
    if not booking_valid_path.exists() or not payment_valid_path.exists():
        return

    with booking_valid_path.open("r", encoding="utf-8-sig", newline="") as handle:
        booking_ids = {_normal_token(row.get("booking_id")) for row in csv.DictReader(handle)}

    with payment_valid_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        payment_fields = reader.fieldnames or []
        valid_payments = list(reader)

    existing_quarantine = []
    if payment_quarantine_path.exists():
        with payment_quarantine_path.open("r", encoding="utf-8-sig", newline="") as handle:
            existing_quarantine = list(csv.DictReader(handle))

    kept = []
    orphaned = []
    for row_number, row in enumerate(valid_payments, start=2):
        booking_id = _normal_token(row.get("booking_id"))
        if booking_id in booking_ids:
            kept.append(row)
            continue
        record_key = _normal_token(row.get("payment_id")) or f"row-{row_number}"
        reason = "Payment references a booking that failed booking DQ or is missing"
        orphaned_row = {"row_number": row_number, "record_key": record_key, "reason": reason, **row}
        orphaned.append(orphaned_row)
        errors.append(
            _dq_error(
                "payments",
                payment_valid_path,
                row_number,
                record_key,
                "ERROR",
                "relationship_missing",
                "booking_id",
                row.get("booking_id", ""),
                reason,
                "QUARANTINED",
            )
        )

    if orphaned:
        _write_csv(payment_valid_path, kept, payment_fields)
        _write_csv(payment_quarantine_path, [*existing_quarantine, *orphaned], ["row_number", "record_key", "reason", *payment_fields])
        for item in report:
            if item["source_name"] == "payments":
                profile = item["quality_profile"]
                profile["valid_rows"] = len(kept)
                profile["quarantined_rows"] = int(profile.get("quarantined_rows", 0)) + len(orphaned)
                profile["record_error_rows"] = int(profile.get("record_error_rows", 0)) + len(orphaned)
                item["validated_file_path"] = str(payment_valid_path)
                item["quarantine_file_path"] = str(payment_quarantine_path)


def inspect_csv(path: Path, source_name: str) -> tuple[dict, list[dict], list[dict]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        header = reader.fieldnames or []
        if source_name in {"bookings", "payments"}:
            rows = list(reader)
            row_count = len(rows)
        else:
            rows = []
            row_count = sum(1 for _ in reader)
    missing = sorted(REQUIRED_COLUMNS.get(source_name, set()) - set(header))
    errors = []
    quarantine_rows = []
    valid_rows = []
    valid_path = None
    quarantine_path = None

    if source_name == "bookings" and not missing:
        valid_rows, quarantine_rows, errors, _ = validate_booking_rows(path)
        valid_path = VALIDATED_DIR / "bookings" / "bookings_valid.csv"
        quarantine_path = QUARANTINE_DIR / "bookings" / "invalid_records.csv"
        _write_csv(valid_path, valid_rows, header)
        _write_csv(quarantine_path, quarantine_rows, ["row_number", "record_key", "reason", *header])
    elif source_name == "payments" and not missing:
        valid_rows, quarantine_rows, errors, _ = validate_payment_rows(path)
        valid_path = VALIDATED_DIR / "payments" / "payments_valid.csv"
        quarantine_path = QUARANTINE_DIR / "payments" / "invalid_records.csv"
        _write_csv(valid_path, valid_rows, header)
        _write_csv(quarantine_path, quarantine_rows, ["row_number", "record_key", "reason", *header])

    quality_profile = profile_rows(rows, source_name)
    if source_name in {"bookings", "payments"}:
        quality_profile["valid_rows"] = len(valid_rows)
        quality_profile["quarantined_rows"] = len(quarantine_rows)
        quality_profile["record_error_rows"] = len(errors)

    result = {
        "source_name": source_name,
        "file_path": str(path),
        "validated_file_path": str(valid_path) if valid_path else None,
        "quarantine_file_path": str(quarantine_path) if quarantine_path else None,
        "file_size_bytes": path.stat().st_size,
        "row_count": row_count,
        "checksum_md5": md5(path),
        "missing_columns": missing,
        "quality_profile": quality_profile,
        "status": "FAILED" if missing or row_count == 0 else "PASSED",
    }
    return result, errors, quarantine_rows


def profile_rows(rows: list[dict], source_name: str) -> dict:
    if source_name == "bookings":
        booking_ids = [row.get("booking_id", "").strip().upper() for row in rows if row.get("booking_id")]
        dirty_status_rows = sum(
            1
            for row in rows
            if _is_dirty_token(row.get("booking_status", ""))
            or _is_dirty_token(row.get("payment_status", ""))
            or _is_dirty_token(row.get("booking_channel", ""))
            or _is_dirty_token(row.get("origin_airport", ""))
            or _is_dirty_token(row.get("destination_airport", ""))
        )
        negative_ticket_rows = sum(1 for row in rows if _is_number(row.get("ticket_price", "")) and float(row.get("ticket_price") or 0) < 0)
        missing_customer_rows = sum(1 for row in rows if not row.get("customer_id", "").strip())
        out_of_order_update_rows = sum(
            1 for row in rows if row.get("updated_at") and row.get("created_at") and row["updated_at"] < row["created_at"]
        )
        return {
            "duplicate_booking_id_rows": len(booking_ids) - len(set(booking_ids)),
            "dirty_standardization_rows": dirty_status_rows,
            "negative_ticket_rows": negative_ticket_rows,
            "missing_customer_rows": missing_customer_rows,
            "out_of_order_update_rows": out_of_order_update_rows,
        }
    if source_name == "payments":
        payment_ids = [row.get("payment_id", "").strip().upper() for row in rows if row.get("payment_id")]
        dirty_status_rows = sum(
            1
            for row in rows
            if _is_dirty_token(row.get("payment_status", ""))
            or _is_dirty_token(row.get("payment_method", ""))
            or _is_dirty_token(row.get("currency", ""))
        )
        negative_amount_rows = sum(1 for row in rows if _is_number(row.get("amount", "")) and float(row.get("amount") or 0) < 0)
        refund_without_status_rows = sum(
            1
            for row in rows
            if _is_number(row.get("refund_amount", ""))
            and float(row.get("refund_amount") or 0) > 0
            and "REFUND" not in row.get("payment_status", "").upper()
        )
        return {
            "duplicate_payment_id_rows": len(payment_ids) - len(set(payment_ids)),
            "dirty_standardization_rows": dirty_status_rows,
            "negative_amount_rows": negative_amount_rows,
            "refund_without_refund_status_rows": refund_without_status_rows,
        }
    return {}


def validate_all() -> Path:
    candidates = [
        ("bts", sorted((DATA_DIR / "input" / "bts").glob("*.csv"))[-1]),
        ("airports", DATA_DIR / "input" / "ourairports" / "airports.csv"),
        ("bookings", DATA_DIR / "generated" / "bookings" / "bookings.csv"),
        ("payments", DATA_DIR / "generated" / "payments" / "payments.csv"),
    ]
    report = []
    all_errors = []
    for source, path in candidates:
        item, errors, _ = inspect_csv(path, source)
        report.append(item)
        all_errors.extend(errors)
    enforce_payment_booking_relationship(report, all_errors)

    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    output = QUALITY_DIR / "validation_report.json"
    errors_output = QUALITY_DIR / "record_errors.csv"
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_csv(
        errors_output,
        all_errors,
        [
            "error_id",
            "source_name",
            "file_path",
            "row_number",
            "record_key",
            "severity",
            "rule_name",
            "column_name",
            "bad_value",
            "error_message",
            "action_taken",
        ],
    )
    failed = [item for item in report if item["status"] != "PASSED"]
    if failed:
        raise RuntimeError(f"Validation failed: {failed}")
    return output


def main() -> None:
    print(validate_all())


if __name__ == "__main__":
    main()
