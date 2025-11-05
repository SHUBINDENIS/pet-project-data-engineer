"""Helpers for interacting with MongoDB."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from pymongo import MongoClient

from . import config


def get_client() -> MongoClient:
    """Return a MongoDB client using settings from the environment."""
    uri = config.mongo_connection_uri(include_auth_db=False)
    return MongoClient(uri)


@contextmanager
def mongo_collection(collection: str) -> Iterator:
    """Context manager that yields a Mongo collection in the configured database."""
    client = get_client()
    try:
        db = client[config.MONGO_DB]
        yield db[collection]
    finally:
        client.close()
