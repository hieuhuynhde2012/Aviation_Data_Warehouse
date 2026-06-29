import os
from pathlib import Path


BASE_DIR = Path(os.getenv("PROJECT_BASE_DIR", "/opt/airflow"))
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
INPUT_DIR = DATA_DIR / "input"
GENERATED_DIR = DATA_DIR / "generated"
QUALITY_DIR = DATA_DIR / "quality_reports"
VALIDATED_DIR = DATA_DIR / "validated"
QUARANTINE_DIR = DATA_DIR / "quarantine"


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def warehouse_config() -> dict:
    return {
        "host": env("WAREHOUSE_HOST", "postgres-warehouse"),
        "port": int(env("WAREHOUSE_PORT_INTERNAL", "5432")),
        "dbname": env("WAREHOUSE_DB", "aviation_dw"),
        "user": env("WAREHOUSE_USER", "aviation"),
        "password": env("WAREHOUSE_PASSWORD", "aviation"),
    }


def minio_config() -> dict:
    return {
        "endpoint_url": env("MINIO_ENDPOINT", "http://minio:9000"),
        "aws_access_key_id": env("MINIO_ROOT_USER", "minioadmin"),
        "aws_secret_access_key": env("MINIO_ROOT_PASSWORD", "minioadmin"),
        "raw_bucket": env("MINIO_BUCKET_RAW", "aviation-raw"),
        "archive_bucket": env("MINIO_BUCKET_ARCHIVE", "aviation-archive"),
        "quality_bucket": env("MINIO_BUCKET_QUALITY", "aviation-quality-reports"),
    }
