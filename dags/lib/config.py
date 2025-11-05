"""Shared configuration helpers for Airflow DAGs."""
from __future__ import annotations

import os
from typing import List


def _get_list(value: str | None) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


S3_BUCKET = os.environ.get("S3_DATA_BUCKET", "")
S3_PREFIX = os.environ.get("S3_DATA_PREFIX", "crypto_intel")
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

BINANCE_SYMBOLS = _get_list(os.environ.get("BINANCE_SYMBOLS"))

FRED_API_KEY = os.environ.get("FRED_API_KEY")
FRED_SERIES = _get_list(os.environ.get("FRED_SERIES")) or ["CPIAUCSL", "M2SL", "DGS10"]

NEWS_API_KEY = os.environ.get("NEWS_API_KEY")
NEWS_QUERY = os.environ.get("NEWS_QUERY", "bitcoin")

PUSHSHIFT_QUERY = os.environ.get("PUSHSHIFT_QUERY", "bitcoin")

MONGO_USER = os.environ.get("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.environ.get("MONGO_INITDB_ROOT_PASSWORD")
MONGO_DB = os.environ.get("MONGO_INITDB_DATABASE", "analytics")
MONGO_HOST = os.environ.get("MONGO_HOST", "mongo")
MONGO_PORT = int(os.environ.get("MONGO_PORT", "27017"))


def mongo_connection_uri(include_auth_db: bool = True) -> str:
    """Build the MongoDB connection URI."""
    if MONGO_USER and MONGO_PASSWORD:
        auth_db = f"/{MONGO_DB}" if include_auth_db else ""
        return f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}{auth_db}"
    return f"mongodb://{MONGO_HOST}:{MONGO_PORT}"
