"""Helpers for interacting with S3-compatible object storage."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import boto3
from botocore.config import Config as BotoConfig

from . import config


def get_s3_client():
    """Return a boto3 S3 client configured for the target endpoint."""
    session = boto3.session.Session(
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
    )
    kwargs: dict[str, Any] = {"config": BotoConfig(signature_version="s3v4")}
    if config.S3_ENDPOINT:
        kwargs["endpoint_url"] = config.S3_ENDPOINT
    return session.client("s3", **kwargs)


def build_s3_key(*parts: str, suffix: str = ".json") -> str:
    """Construct a key from prefix parts."""
    cleaned = [p.strip("/").replace(" ", "_") for p in parts if p]
    prefix = "/".join(cleaned)
    return f"{prefix}{suffix}"


def upload_json(payload: Any, *, key: str, content_type: str = "application/json") -> None:
    """Upload a JSON serialisable payload to S3."""
    if not config.S3_BUCKET:
        raise RuntimeError("S3_DATA_BUCKET is not configured")
    client = get_s3_client()
    body = payload if isinstance(payload, (bytes, bytearray)) else json.dumps(payload, default=_json_serializer).encode("utf-8")
    client.put_object(Bucket=config.S3_BUCKET, Key=key, Body=body, ContentType=content_type)


def _json_serializer(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)!r} is not JSON serialisable")
