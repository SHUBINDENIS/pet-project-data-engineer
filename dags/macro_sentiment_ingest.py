from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from fredapi import Fred

from lib import config, mongo, storage

logger = logging.getLogger(__name__)


def _fetch_fred_series(execution_date: datetime, **_: Any) -> None:
    if not config.FRED_API_KEY:
        raise AirflowSkipException("FRED_API_KEY is not configured")
    if not config.S3_BUCKET:
        raise AirflowSkipException("S3 bucket not configured (S3_DATA_BUCKET)")

    fred = Fred(api_key=config.FRED_API_KEY)
    end_date = execution_date.date()
    start_date = (execution_date - timedelta(days=365)).date()
    timestamp = execution_date.strftime("%H%M%S")
    date_prefix = execution_date.strftime("%Y/%m/%d")

    for series_id in config.FRED_SERIES:
        try:
            raw_series: pd.Series = fred.get_series(
                series_id,
                observation_start=start_date.isoformat(),
                observation_end=end_date.isoformat(),
            )
        except Exception as exc:  # pragma: no cover - network exception
            logger.warning("Failed to fetch FRED series %s: %s", series_id, exc)
            continue

        records: List[Dict[str, Any]] = []
        for idx, value in raw_series.fillna(method="ffill").items():
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue
            if isinstance(idx, pd.Timestamp):
                idx = idx.to_pydatetime()
            records.append(
                {
                    "date": idx.isoformat() if isinstance(idx, datetime) else str(idx),
                    "value": float(value),
                }
            )

        payload = {
            "series": series_id,
            "fetched_at": datetime.utcnow().isoformat(),
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "count": len(records),
            "data": records,
        }
        key = storage.build_s3_key(
            config.S3_PREFIX,
            "macro",
            "fred",
            series_id,
            date_prefix,
            f"series_{timestamp}",
        )
        storage.upload_json(payload, key=key)


def _fetch_news_articles(execution_date: datetime, **_: Any) -> None:
    if not config.NEWS_API_KEY:
        raise AirflowSkipException("NEWS_API_KEY is not configured")

    params = {
        "q": config.NEWS_QUERY,
        "sortBy": "publishedAt",
        "language": "en",
        "pageSize": 50,
        "apiKey": config.NEWS_API_KEY,
    }
    response = requests.get("https://newsapi.org/v2/everything", params=params, timeout=30)
    if response.status_code != 200:
        raise AirflowSkipException(f"News API responded with {response.status_code}: {response.text}")

    payload = response.json()
    articles: List[Dict[str, Any]] = payload.get("articles") or []
    if not articles:
        raise AirflowSkipException("News API returned no articles")

    ingested_at = datetime.utcnow().isoformat()
    for article in articles:
        article["_ingested_at"] = ingested_at
        article["_source"] = "newsapi"

    with mongo.mongo_collection("news_raw") as collection:
        collection.insert_many(articles, ordered=False)

    if config.S3_BUCKET:
        timestamp = execution_date.strftime("%H%M%S")
        date_prefix = execution_date.strftime("%Y/%m/%d")
        key = storage.build_s3_key(
            config.S3_PREFIX,
            "sentiment",
            "newsapi",
            date_prefix,
            f"articles_{timestamp}",
        )
        storage.upload_json(payload, key=key)


default_args = {
    "owner": "dlaze",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="macro_sentiment_ingest",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["macro", "sentiment", "ingest"],
) as dag:
    fetch_fred = PythonOperator(
        task_id="fetch_fred_series",
        python_callable=_fetch_fred_series,
    )

    fetch_news = PythonOperator(
        task_id="fetch_news_articles",
        python_callable=_fetch_news_articles,
    )

    fetch_fred >> fetch_news
