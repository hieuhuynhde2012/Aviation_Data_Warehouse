import hashlib
import json
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql

from ingestion.config import DATA_DIR, QUALITY_DIR, VALIDATED_DIR, warehouse_config


BTS_RENAME = {
    "Year": "year",
    "Quarter": "quarter",
    "Month": "month",
    "DayofMonth": "day_of_month",
    "FlightDate": "flight_date",
    "Reporting_Airline": "reporting_airline",
    "Tail_Number": "tail_number",
    "Flight_Number_Reporting_Airline": "flight_number_reporting_airline",
    "Origin": "origin",
    "OriginCityName": "origin_city_name",
    "OriginState": "origin_state_abr",
    "Dest": "dest",
    "DestCityName": "dest_city_name",
    "DestState": "dest_state_abr",
    "CRSDepTime": "crs_dep_time",
    "DepTime": "dep_time",
    "DepDelay": "dep_delay",
    "DepDelayMinutes": "dep_delay_minutes",
    "TaxiOut": "taxi_out",
    "WheelsOff": "wheels_off",
    "WheelsOn": "wheels_on",
    "TaxiIn": "taxi_in",
    "CRSArrTime": "crs_arr_time",
    "ArrTime": "arr_time",
    "ArrDelay": "arr_delay",
    "ArrDelayMinutes": "arr_delay_minutes",
    "Cancelled": "cancelled",
    "CancellationCode": "cancellation_code",
    "Diverted": "diverted",
    "CRSElapsedTime": "crs_elapsed_time",
    "ActualElapsedTime": "actual_elapsed_time",
    "AirTime": "air_time",
    "Flights": "flights",
    "Distance": "distance",
    "CarrierDelay": "carrier_delay",
    "WeatherDelay": "weather_delay",
    "NASDelay": "nas_delay",
    "SecurityDelay": "security_delay",
    "LateAircraftDelay": "late_aircraft_delay",
}


RAW_COLUMNS = {
    "raw_bts_flights": list(BTS_RENAME.values()),
    "raw_airports": [
        "id",
        "ident",
        "type",
        "name",
        "latitude_deg",
        "longitude_deg",
        "elevation_ft",
        "continent",
        "iso_country",
        "iso_region",
        "municipality",
        "scheduled_service",
        "gps_code",
        "iata_code",
        "local_code",
        "home_link",
        "wikipedia_link",
        "keywords",
    ],
    "raw_countries": ["id", "code", "name", "continent", "wikipedia_link", "keywords"],
    "raw_regions": ["id", "code", "local_code", "name", "continent", "iso_country", "wikipedia_link", "keywords"],
    "raw_bookings": [
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
    ],
    "raw_payments": [
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
    ],
}

INTEGER_COLUMNS = {
    "raw_bts_flights": [
        "year",
        "quarter",
        "month",
        "day_of_month",
        "crs_dep_time",
        "dep_time",
        "wheels_off",
        "wheels_on",
        "crs_arr_time",
        "arr_time",
    ],
}

TABLE_SOURCE = {
    "raw_bts_flights": "bts",
    "raw_airports": "airports",
    "raw_countries": "countries",
    "raw_regions": "regions",
    "raw_bookings": "bookings",
    "raw_payments": "payments",
}


def md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_dataframe(conn, df: pd.DataFrame, table: str) -> int:
    temp_path = Path("/tmp") / f"{table}.csv"
    df.to_csv(temp_path, index=False, header=False, na_rep="")
    columns = list(df.columns)
    copy_sql = sql.SQL("copy raw.{} ({}) from stdin with (format csv, null '', quote '\"')").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(col) for col in columns),
    )
    with conn.cursor() as cur, temp_path.open("r", encoding="utf-8", newline="") as handle:
        cur.copy_expert(copy_sql, handle)
    temp_path.unlink(missing_ok=True)
    return len(df)


def _file_already_loaded(conn, checksum: str, table: str) -> bool:
    query = """
        select 1
        from metadata.raw_load_audit
        where checksum_md5 = %s
          and target_table = %s
          and load_status = 'LOADED'
        limit 1
    """
    with conn.cursor() as cur:
        cur.execute(query, (checksum, f"raw.{table}"))
        return cur.fetchone() is not None


def _audit_load(
    conn,
    source_name: str,
    table: str,
    path: Path,
    checksum: str,
    source_rows: int,
    valid_rows: int,
    quarantined_rows: int,
    inserted_rows: int,
    skipped_rows: int,
    status: str,
    error_message: str = None,
) -> None:
    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    load_audit_id = hashlib.md5(f"{run_id}|{source_name}|{table}|{checksum}".encode("utf-8")).hexdigest()
    query = """
        insert into metadata.raw_load_audit (
            load_audit_id, run_id, source_name, target_table, file_path, checksum_md5,
            source_rows, valid_rows, quarantined_rows, inserted_rows, skipped_rows,
            load_status, started_at, completed_at, error_message
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp, current_timestamp, %s)
        on conflict (load_audit_id) do update set
            inserted_rows = excluded.inserted_rows,
            skipped_rows = excluded.skipped_rows,
            load_status = excluded.load_status,
            completed_at = excluded.completed_at,
            error_message = excluded.error_message
    """
    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                load_audit_id,
                run_id,
                source_name,
                f"raw.{table}",
                str(path),
                checksum,
                source_rows,
                valid_rows,
                quarantined_rows,
                inserted_rows,
                skipped_rows,
                status,
                error_message,
            ),
        )


def _load_csv(
    conn,
    path: Path,
    table: str,
    rename: dict = None,
    source_rows: int = None,
    valid_rows: int = None,
    quarantined_rows: int = 0,
) -> int:
    source_name = TABLE_SOURCE[table]
    checksum = md5(path)
    if _file_already_loaded(conn, checksum, table):
        skipped_rows = valid_rows if valid_rows is not None else (source_rows or 0)
        _audit_load(conn, source_name, table, path, checksum, source_rows or skipped_rows, skipped_rows, quarantined_rows, 0, skipped_rows, "SKIPPED_ALREADY_LOADED")
        return 0

    df = pd.read_csv(path, low_memory=False)
    if rename:
        df = df.rename(columns=rename)
    keep = [col for col in RAW_COLUMNS[table] if col in df.columns]
    df = df[keep].copy()
    for col in INTEGER_COLUMNS.get(table, []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["source_file"] = str(path)
    with conn.cursor() as cur:
        if table == "raw_bookings":
            cur.execute(
                sql.SQL("delete from raw.{} where source_file like %s or source_file like %s").format(sql.Identifier(table)),
                ("%/generated/bookings/%", "%/validated/bookings/%"),
            )
        elif table == "raw_payments":
            cur.execute(
                sql.SQL("delete from raw.{} where source_file like %s or source_file like %s").format(sql.Identifier(table)),
                ("%/generated/payments/%", "%/validated/payments/%"),
            )
        else:
            cur.execute(sql.SQL("delete from raw.{} where source_file = %s").format(sql.Identifier(table)), (str(path),))
    inserted_rows = _copy_dataframe(conn, df, table)
    _audit_load(
        conn,
        source_name,
        table,
        path,
        checksum,
        source_rows if source_rows is not None else inserted_rows,
        valid_rows if valid_rows is not None else inserted_rows,
        quarantined_rows,
        inserted_rows,
        0,
        "LOADED",
    )
    return inserted_rows


def _validation_meta() -> dict:
    report_path = QUALITY_DIR / "validation_report.json"
    if not report_path.exists():
        return {}
    report = json.loads(report_path.read_text(encoding="utf-8"))
    return {item["source_name"]: item for item in report}


def load_all() -> dict:
    cfg = warehouse_config()
    validation = _validation_meta()
    year_month_files = sorted((DATA_DIR / "input" / "bts").glob("*.csv"))
    if not year_month_files:
        raise FileNotFoundError("No BTS CSV found.")
    booking_path = VALIDATED_DIR / "bookings" / "bookings_valid.csv"
    payment_path = VALIDATED_DIR / "payments" / "payments_valid.csv"
    files = {
        "raw_bts_flights": (year_month_files[-1], BTS_RENAME, validation.get("bts", {})),
        "raw_airports": (DATA_DIR / "input" / "ourairports" / "airports.csv", None, validation.get("airports", {})),
        "raw_countries": (DATA_DIR / "input" / "ourairports" / "countries.csv", None, {}),
        "raw_regions": (DATA_DIR / "input" / "ourairports" / "regions.csv", None, {}),
        "raw_bookings": (booking_path if booking_path.exists() else DATA_DIR / "generated" / "bookings" / "bookings.csv", None, validation.get("bookings", {})),
        "raw_payments": (payment_path if payment_path.exists() else DATA_DIR / "generated" / "payments" / "payments.csv", None, validation.get("payments", {})),
    }
    loaded = {}
    with psycopg2.connect(**cfg) as conn:
        for table, (path, rename, meta) in files.items():
            profile = meta.get("quality_profile", {})
            loaded[table] = _load_csv(
                conn,
                path,
                table,
                rename,
                source_rows=meta.get("row_count"),
                valid_rows=profile.get("valid_rows"),
                quarantined_rows=profile.get("quarantined_rows", 0),
            )
        conn.commit()
    return loaded


def main() -> None:
    for table, count in load_all().items():
        print(f"{table}: {count}")


if __name__ == "__main__":
    main()
