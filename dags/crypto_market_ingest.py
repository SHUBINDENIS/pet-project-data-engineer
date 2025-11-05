from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from binance.spot import Spot as BinanceSpot

from lib import config, storage


def _upload_binance_klines(execution_date: datetime, **_: Any) -> None:
    symbols = config.BINANCE_SYMBOLS
    if not symbols:
        raise AirflowSkipException("BINANCE_SYMBOLS is not configured")
    if not config.S3_BUCKET:
        raise AirflowSkipException("S3 bucket not configured (S3_DATA_BUCKET)")

    client = BinanceSpot()
    timestamp = execution_date.strftime("%H%M%S")
    date_prefix = execution_date.strftime("%Y/%m/%d")

    for symbol in symbols:
        klines = client.klines(symbol=symbol, interval="1m", limit=60)
        payload = {
            "symbol": symbol,
            "interval": "1m",
            "fetched_at": datetime.utcnow().isoformat(),
            "items": klines,
        }
        key = storage.build_s3_key(
            config.S3_PREFIX,
            "prices",
            "binance",
            symbol.lower(),
            date_prefix,
            f"klines_{timestamp}",
        )
        storage.upload_json(payload, key=key)


default_args = {
    "owner": "dlaze",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="crypto_market_ingest",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["crypto", "ingest"],
) as dag:
    PythonOperator(
        task_id="fetch_binance_prices",
        python_callable=_upload_binance_klines,
    )
