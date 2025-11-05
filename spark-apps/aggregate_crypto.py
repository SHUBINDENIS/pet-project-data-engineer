#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate Binance klines into hourly metrics.")
    parser.add_argument("--bucket", required=True, help="Target S3 bucket")
    parser.add_argument("--prefix", default="crypto_intel", help="Root prefix within the bucket")
    parser.add_argument("--date", required=True, help="Trading date (YYYY-MM-DD)")
    parser.add_argument("--s3-endpoint", default=None, help="Custom S3 endpoint URL")
    return parser.parse_args()


def configure_s3(spark: SparkSession, endpoint: str | None = None) -> None:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    if access_key and secret_key:
        hadoop_conf.set("fs.s3a.access.key", access_key)
        hadoop_conf.set("fs.s3a.secret.key", secret_key)
    if endpoint:
        hadoop_conf.set("fs.s3a.endpoint", endpoint)
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def transform(df: DataFrame) -> DataFrame:
    exploded = df.select("symbol", F.explode("items").alias("row"))
    kline = exploded.select(
        F.col("symbol"),
        (F.col("row")[0] / 1000).cast("timestamp").alias("open_time"),
        F.col("row")[1].cast("double").alias("open_price"),
        F.col("row")[2].cast("double").alias("high_price"),
        F.col("row")[3].cast("double").alias("low_price"),
        F.col("row")[4].cast("double").alias("close_price"),
        F.col("row")[5].cast("double").alias("volume"),
    ).filter(F.col("open_time").isNotNull())

    enriched = kline.withColumn("hour", F.date_trunc("hour", F.col("open_time")))

    agg = (
        enriched.groupBy("symbol", "hour")
        .agg(
            F.first("open_price", ignorenulls=True).alias("open_price"),
            F.max("high_price").alias("high_price"),
            F.min("low_price").alias("low_price"),
            F.last("close_price", ignorenulls=True).alias("close_price"),
            F.avg("close_price").alias("avg_close_price"),
            F.sum("volume").alias("volume"),
        )
        .orderBy("symbol", "hour")
    )
    return agg


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("crypto-hourly-aggregation").getOrCreate()
    try:
        configure_s3(spark, endpoint=args.s3_endpoint)
        date_path = "/".join(args.date.split("-"))
        input_path = f"s3a://{args.bucket}/{args.prefix}/prices/binance/*/{date_path}/klines_*.json"
        df = spark.read.option("multiline", "true").json(input_path)
        if df.rdd.isEmpty():
            spark.stop()
            return
        result = transform(df)
        output_path = f"s3a://{args.bucket}/{args.prefix}/silver/prices_hourly/date={args.date}"
        result.write.mode("overwrite").parquet(output_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
