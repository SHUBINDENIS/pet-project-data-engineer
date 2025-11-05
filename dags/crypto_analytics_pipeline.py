from __future__ import annotations

import io
import logging
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from clickhouse_connect import get_client as get_clickhouse_client

from lib import config, storage

logger = logging.getLogger(__name__)


def _load_hourly_prices(execution_date: datetime, **_: Any) -> None:
    if not config.S3_BUCKET:
        raise AirflowSkipException("S3 bucket not configured (S3_DATA_BUCKET)")

    client = storage.get_s3_client()
    date_str = execution_date.strftime("%Y-%m-%d")
    prefix = f"{config.S3_PREFIX}/silver/prices_hourly/date={date_str}"
    response = client.list_objects_v2(Bucket=config.S3_BUCKET, Prefix=prefix)
    contents = response.get("Contents") or []
    parquet_keys = [obj["Key"] for obj in contents if obj["Key"].endswith(".parquet")]

    if not parquet_keys:
        raise AirflowSkipException(f"No parquet datasets found at s3://{config.S3_BUCKET}/{prefix}")

    frames: List[pd.DataFrame] = []
    for key in parquet_keys:
        body = client.get_object(Bucket=config.S3_BUCKET, Key=key)["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))

    combined = pd.concat(frames, ignore_index=True)
    if combined.empty:
        raise AirflowSkipException("Aggregated dataset is empty")

    combined["hour"] = pd.to_datetime(combined["hour"])
    records: List[Dict[str, Any]] = combined.to_dict(orient="records")

    ch_client = get_clickhouse_client(host="clickhouse", port=8123, username="default", password="", database="analytics")
    ch_client.command(
        """
        CREATE TABLE IF NOT EXISTS analytics.crypto_hourly (
            symbol String,
            hour DateTime,
            open_price Float64,
            high_price Float64,
            low_price Float64,
            close_price Float64,
            avg_close_price Float64,
            volume Float64
        )
        ENGINE = MergeTree
        ORDER BY (symbol, hour)
        """
    )
    ch_client.command(
        "ALTER TABLE analytics.crypto_hourly DELETE WHERE toDate(hour) = toDate(%(target)s)",
        parameters={"target": date_str},
    )
    ch_client.insert(
        "analytics.crypto_hourly",
        records,
        column_names=[
            "symbol",
            "hour",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "avg_close_price",
            "volume",
        ],
    )
    logger.info("Inserted %s rows into clickhouse.analytics.crypto_hourly", len(records))


default_args = {
    "owner": "dlaze",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="crypto_analytics_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["crypto", "analytics"],
) as dag:
    spark_task = SparkSubmitOperator(
        task_id="spark_aggregate_crypto",
        application="/opt/airflow/spark-apps/aggregate_crypto.py",
        name="spark_crypto_hourly_aggregation",
        conn_id="spark_default",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.611",
        application_args=[
            f"--bucket={config.S3_BUCKET}",
            f"--prefix={config.S3_PREFIX}",
            "--date={{ ds }}",
            f"--s3-endpoint={config.S3_ENDPOINT or ''}",
        ],
        conf={
            "spark.executor.instances": "1",
            "spark.executor.memory": "1G",
            "spark.executor.cores": "1",
            "spark.driver.memory": "1G",
        },
        env_vars={
            "AWS_ACCESS_KEY_ID": config.AWS_ACCESS_KEY_ID or "",
            "AWS_SECRET_ACCESS_KEY": config.AWS_SECRET_ACCESS_KEY or "",
            "AWS_DEFAULT_REGION": config.AWS_REGION,
        },
    )

    load_clickhouse = PythonOperator(
        task_id="load_clickhouse_hourly",
        python_callable=_load_hourly_prices,
    )

    spark_task >> load_clickhouse
