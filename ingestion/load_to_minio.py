import os
from pathlib import Path

import boto3

from ingestion.config import DATA_DIR, minio_config


def client():
    cfg = minio_config()
    return boto3.client(
        "s3",
        endpoint_url=cfg["endpoint_url"],
        aws_access_key_id=cfg["aws_access_key_id"],
        aws_secret_access_key=cfg["aws_secret_access_key"],
    )


def upload_file(path: Path, key: str, bucket: str = None) -> str:
    cfg = minio_config()
    bucket = bucket or cfg["raw_bucket"]
    client().upload_file(str(path), bucket, key)
    return f"s3://{bucket}/{key}"


def upload_all() -> list[str]:
    year = int(os.getenv("BTS_YEAR", "2026"))
    month = int(os.getenv("BTS_MONTH", "4"))
    outputs = []
    bts_path = DATA_DIR / "input" / "bts" / f"bts_on_time_{year}_{month:02d}.csv"
    outputs.append(upload_file(bts_path, f"bts_on_time/year={year}/month={month:02d}/{bts_path.name}"))

    snapshot = os.getenv("AIRFLOW_CTX_EXECUTION_DATE", "local").split("T")[0]
    for name in ["airports.csv", "countries.csv", "regions.csv"]:
        path = DATA_DIR / "input" / "ourairports" / name
        outputs.append(upload_file(path, f"ourairports/snapshot_date={snapshot}/{name}"))

    event_prefixes = {"bookings": "booking_events", "payments": "payment_events"}
    for entity, prefix in event_prefixes.items():
        path = DATA_DIR / "generated" / entity / f"{entity}.csv"
        outputs.append(upload_file(path, f"{prefix}/dt={snapshot}/{path.name}"))
    return outputs


def main() -> None:
    for uri in upload_all():
        print(uri)


if __name__ == "__main__":
    main()
