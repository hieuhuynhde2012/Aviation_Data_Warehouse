from __future__ import annotations

import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from ingestion.config import GENERATED_DIR, INPUT_DIR


def _latest_bts_path() -> Path:
    year = int(os.getenv("BTS_YEAR", "2026"))
    month = int(os.getenv("BTS_MONTH", "4"))
    path = INPUT_DIR / "bts" / f"bts_on_time_{year}_{month:02d}.csv"
    if path.exists():
        return path
    matches = sorted((INPUT_DIR / "bts").glob("*.csv"))
    if not matches:
        raise FileNotFoundError("No BTS CSV found. Run download_sources.py first.")
    return matches[-1]


def _read_bts_sample() -> list[dict]:
    path = _latest_bts_path()
    max_flights = int(os.getenv("MAX_FLIGHTS_FOR_SYNTHETIC", "2000"))
    sample = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not row.get("FlightDate") or not row.get("Reporting_Airline") or not row.get("Origin") or not row.get("Dest"):
                continue
            if float(row.get("Cancelled") or 0) != 0 or float(row.get("Diverted") or 0) != 0:
                continue
            sample.append(row)
            if len(sample) >= max_flights:
                break
    return sample


def _choice_weighted(rng: random.Random, values: list[str], weights: list[float]) -> str:
    return rng.choices(values, weights=weights, k=1)[0]


def _dirty_text(rng: random.Random, value: str, rate: float) -> str:
    if rng.random() >= rate:
        return value
    variant = rng.choice(["lower", "title", "pad", "space"])
    if variant == "lower":
        return value.lower()
    if variant == "title":
        return value.title()
    if variant == "pad":
        return f" {value} "
    return value.replace("_", " ")


def _dirty_airport(rng: random.Random, value: str, rate: float) -> str:
    if rng.random() >= rate:
        return value
    variant = rng.choice(["lower", "pad", "local_suffix"])
    if variant == "lower":
        return value.lower()
    if variant == "pad":
        return f" {value} "
    return f"{value}-T1"


def _dirty_status(rng: random.Random, value: str, rate: float, kind: str) -> str:
    if rng.random() >= rate:
        return value
    booking_map = {
        "CONFIRMED": ["confirmed", "CONF", "Booked", " complete "],
        "CANCELLED": ["cancelled", "CANCELED", "VOID", " cncl "],
        "PENDING": ["pending", "PEND", "IN_PROGRESS", " awaiting_payment "],
    }
    payment_map = {
        "PAID": ["paid", "SUCCESS", "SETTLED", " paid "],
        "FAILED": ["failed", "DECLINED", "ERROR", " fail "],
        "REFUNDED": ["refunded", "REFUND", "CHARGEBACK", " refunded "],
    }
    mapping = booking_map if kind == "booking" else payment_map
    return rng.choice(mapping[value])


def _write_with_optional_duplicate(
    writer: csv.DictWriter,
    row: dict,
    rng: random.Random,
    duplicate_rate: float,
    status_field: str,
    amount_field: Optional[str] = None,
) -> int:
    writer.writerow(row)
    written = 1
    if rng.random() < duplicate_rate:
        duplicate = dict(row)
        duplicate["updated_at"] = (
            datetime.fromisoformat(str(row["updated_at"])) + timedelta(hours=rng.randint(1, 48))
        ).isoformat(sep=" ")
        if status_field in duplicate:
            duplicate[status_field] = _dirty_text(rng, str(duplicate[status_field]), 1.0)
        if amount_field and amount_field in duplicate and str(duplicate[amount_field]) not in ("", "0"):
            try:
                duplicate[amount_field] = round(float(duplicate[amount_field]) + rng.choice([-1.0, 1.0, 2.5]), 2)
            except ValueError:
                pass
        writer.writerow(duplicate)
        written += 1
    return written


def generate_booking_events() -> tuple[Path, Path]:
    seed = int(os.getenv("SYNTHETIC_SEED", "42"))
    rng = random.Random(seed)
    flights = _read_bts_sample()
    min_n = int(os.getenv("BOOKINGS_PER_FLIGHT_MIN", "30"))
    max_n = int(os.getenv("BOOKINGS_PER_FLIGHT_MAX", "180"))
    target_rows = int(os.getenv("SYNTHETIC_TARGET_ROWS", "100000"))
    currency = os.getenv("CURRENCY", "USD")
    dirty_rate = float(os.getenv("DIRTY_DATA_RATE", "0.08"))
    duplicate_rate = float(os.getenv("DUPLICATE_EVENT_RATE", "0.03"))
    missing_optional_rate = float(os.getenv("MISSING_OPTIONAL_RATE", "0.02"))
    payment_mismatch_rate = float(os.getenv("PAYMENT_MISMATCH_RATE", "0.015"))
    critical_invalid_rate = float(os.getenv("CRITICAL_INVALID_RATE", "0.003"))
    customers = [f"CUST-{i:07d}" for i in range(max(1000, target_rows // 5))]

    booking_path = GENERATED_DIR / "bookings" / "bookings.csv"
    payment_path = GENERATED_DIR / "payments" / "payments.csv"
    booking_path.parent.mkdir(parents=True, exist_ok=True)
    payment_path.parent.mkdir(parents=True, exist_ok=True)

    booking_fields = [
        "booking_id",
        "customer_id",
        "flight_id",
        "flight_date",
        "airline",
        "origin_airport",
        "destination_airport",
        "route",
        "booking_time",
        "booking_channel",
        "customer_segment",
        "ticket_price",
        "currency",
        "booking_status",
        "payment_status",
        "is_refunded",
        "created_at",
        "updated_at",
    ]
    payment_fields = [
        "payment_id",
        "booking_id",
        "payment_time",
        "payment_method",
        "payment_status",
        "amount",
        "currency",
        "refund_amount",
        "created_at",
        "updated_at",
    ]

    count = 0
    with booking_path.open("w", encoding="utf-8", newline="") as booking_file, payment_path.open(
        "w", encoding="utf-8", newline=""
    ) as payment_file:
        booking_writer = csv.DictWriter(booking_file, fieldnames=booking_fields)
        payment_writer = csv.DictWriter(payment_file, fieldnames=payment_fields)
        booking_writer.writeheader()
        payment_writer.writeheader()

        for flight_idx, row in enumerate(flights):
            if count >= target_rows:
                break
            flight_date = datetime.fromisoformat(row["FlightDate"])
            distance = float(row.get("Distance") or 500)
            base_price = 65 + distance * 0.16
            route = f"{row['Origin']}-{row['Dest']}"
            flight_number = row.get("Flight_Number_Reporting_Airline") or row.get("Flight_Number_Operating_Airline") or "0"
            flight_id = f"{row['Reporting_Airline']}-{flight_number}-{flight_date:%Y%m%d}-{route}"
            for seat in range(rng.randint(min_n, max_n)):
                if count >= target_rows:
                    break
                booking_id = f"BKG-{uuid.uuid5(uuid.NAMESPACE_DNS, f'{seed}-{flight_idx}-{seat}')}"
                customer_id = rng.choice(customers)
                booking_time = flight_date - timedelta(days=rng.randint(1, 90), minutes=rng.randint(0, 1440))
                status = _choice_weighted(rng, ["CONFIRMED", "CANCELLED", "PENDING"], [0.88, 0.07, 0.05])
                pay_status = _choice_weighted(rng, ["PAID", "FAILED", "REFUNDED"], [0.9, 0.06, 0.04])
                if status == "CANCELLED" and rng.random() < 0.55:
                    pay_status = "REFUNDED"
                price = round(max(30, rng.gauss(base_price, base_price * 0.18)), 2)
                updated_at = booking_time + timedelta(hours=rng.randint(0, 72))
                refunded = pay_status == "REFUNDED"
                if rng.random() < missing_optional_rate:
                    customer_id = ""
                origin_airport = _dirty_airport(rng, row["Origin"], dirty_rate)
                destination_airport = _dirty_airport(rng, row["Dest"], dirty_rate)
                dirty_booking_status = _dirty_status(rng, status, dirty_rate, "booking")
                dirty_payment_status = _dirty_status(rng, pay_status, dirty_rate, "payment")
                channel = _dirty_text(
                    rng,
                    _choice_weighted(rng, ["WEB", "MOBILE_APP", "AGENCY", "CALL_CENTER"], [0.45, 0.34, 0.14, 0.07]),
                    dirty_rate,
                )
                segment = _dirty_text(
                    rng,
                    _choice_weighted(rng, ["NEW", "RETURNING", "BUSINESS", "LOYALTY"], [0.28, 0.42, 0.12, 0.18]),
                    dirty_rate,
                )
                dirty_currency = _dirty_text(rng, currency, dirty_rate / 2)
                booking_row = {
                    "booking_id": _dirty_text(rng, booking_id, dirty_rate / 3),
                    "customer_id": customer_id,
                    "flight_id": flight_id,
                    "flight_date": flight_date.date().isoformat(),
                    "airline": _dirty_text(rng, row["Reporting_Airline"], dirty_rate / 2),
                    "origin_airport": origin_airport,
                    "destination_airport": destination_airport,
                    "route": f"{origin_airport}-{destination_airport}",
                    "booking_time": booking_time.isoformat(sep=" "),
                    "booking_channel": channel,
                    "customer_segment": segment,
                    "ticket_price": round(-price, 2) if rng.random() < dirty_rate / 20 else price,
                    "currency": dirty_currency,
                    "booking_status": dirty_booking_status,
                    "payment_status": dirty_payment_status,
                    "is_refunded": rng.choice(["true", "TRUE", "1", "yes"]) if refunded and rng.random() < dirty_rate else str(refunded).lower(),
                    "created_at": booking_time.isoformat(sep=" "),
                    "updated_at": (booking_time - timedelta(hours=1)).isoformat(sep=" ") if rng.random() < dirty_rate / 20 else updated_at.isoformat(sep=" "),
                }
                payment_amount = price if pay_status in ("PAID", "REFUNDED") else 0
                if rng.random() < payment_mismatch_rate and payment_amount:
                    payment_amount = round(payment_amount + rng.choice([-5.0, -2.5, 3.0, 7.5]), 2)
                payment_row = {
                    "payment_id": _dirty_text(rng, f"PAY-{booking_id[4:]}", dirty_rate / 3),
                    "booking_id": booking_row["booking_id"],
                    "payment_time": (booking_time + timedelta(minutes=rng.randint(1, 180))).isoformat(sep=" "),
                    "payment_method": _dirty_text(
                        rng,
                        _choice_weighted(rng, ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"], [0.62, 0.19, 0.14, 0.05]),
                        dirty_rate,
                    ),
                    "payment_status": dirty_payment_status,
                    "amount": payment_amount,
                    "currency": dirty_currency,
                    "refund_amount": price if refunded else 0,
                    "created_at": booking_time.isoformat(sep=" "),
                    "updated_at": updated_at.isoformat(sep=" "),
                }
                if rng.random() < critical_invalid_rate:
                    issue = rng.choice(["missing_booking_id", "bad_flight_date", "bad_ticket_price"])
                    if issue == "missing_booking_id":
                        booking_row["booking_id"] = ""
                        payment_row["booking_id"] = ""
                    elif issue == "bad_flight_date":
                        booking_row["flight_date"] = "not-a-date"
                    else:
                        booking_row["ticket_price"] = "not-a-number"
                if rng.random() < critical_invalid_rate:
                    issue = rng.choice(["missing_payment_id", "missing_payment_booking_id", "bad_amount"])
                    if issue == "missing_payment_id":
                        payment_row["payment_id"] = ""
                    elif issue == "missing_payment_booking_id":
                        payment_row["booking_id"] = ""
                    else:
                        payment_row["amount"] = "not-a-number"
                _write_with_optional_duplicate(booking_writer, booking_row, rng, duplicate_rate, "booking_status", "ticket_price")
                _write_with_optional_duplicate(payment_writer, payment_row, rng, duplicate_rate, "payment_status", "amount")
                count += 1
    return booking_path, payment_path


def main() -> None:
    for path in generate_booking_events():
        print(path)


if __name__ == "__main__":
    main()
