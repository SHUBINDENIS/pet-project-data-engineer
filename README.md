## Crypto & Macro Sentiment Intelligence stack

This document summarises how to configure and run the extended platform that ingests
crypto market data, macro-economic indicators, and sentiment signals, then materialises
hourly analytics in ClickHouse for visualisation in Superset.

### 1. Configure credentials

Update the placeholders inside `config/project.env` with the real values:

```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
AWS_S3_ENDPOINT=https://s3.firstvds.ru
S3_DATA_BUCKET=crypto-intel
S3_DATA_PREFIX=crypto_intel

BINANCE_SYMBOLS=BTCUSDT,ETHUSDT
FRED_API_KEY=...
FRED_SERIES=CPIAUCSL,M2SL,DGS10
NEWS_API_KEY=...
NEWS_QUERY=(bitcoin OR ethereum) AND (price OR market)
PUSHSHIFT_QUERY=bitcoin

MONGO_INITDB_ROOT_USERNAME=airflow
MONGO_INITDB_ROOT_PASSWORD=...
MONGO_INITDB_DATABASE=analytics
```

Notes:

- Create the S3 bucket beforehand and verify it is reachable from the server (custom
  endpoint + certificate were configured during deployment).
- Binance public endpoints do not require API keys for klines.
- FRED and News APIs require registration to obtain keys.

### 2. Build the extended images

Custom Airflow and Superset images add Python dependencies for ClickHouse, MongoDB,
Spark, and data ingestion SDKs. Build them once:

```bash
docker compose -f docker-compose.yml -f docker-compose.override.yml build
```

### 3. Start the platform

```bash
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d
```

New services:

| Service | Description | Ports |
|---------|-------------|-------|
| `clickhouse` | Time-series analytics database | 8123 (HTTP), 9000 (native) |
| `mongo` | Sentiment/raw text document store | 27017 |
| `spark-master` / `spark-worker` | Spark 3.5 cluster for transformation jobs | 7077, 8080 |

Airflow containers mount `./spark-apps` so that `SparkSubmitOperator` can run bundled scripts.

### 4. Airflow DAGs

Once the stack is running, enable the new DAGs in the Airflow UI:

- **`crypto_market_ingest`** (15 min): fetches Binance klines and writes them to
  S3 (`prices/binance/...`).
- **`macro_sentiment_ingest`** (hourly): fetches FRED time series to S3 and pushes
  NewsAPI articles to MongoDB + S3.
- **`crypto_analytics_pipeline`** (hourly): submits a Spark job that aggregates the
  raw candles into hourly metrics (`silver/prices_hourly/...`) and loads them into
  `clickhouse.analytics.crypto_hourly`.

All tasks gracefully skip if required credentials are missing; check the task logs
for detailed messages.

### 5. Superset connections

1. Log in to Superset (`http://<host>:8088`).
2. Add a new database:
   - SQLAlchemy URI: `clickhouse+http://default:@clickhouse:8123/analytics`
   - Extra options:
     ```json
     {
       "engine_params": {
         "connect_timeout": 5,
         "send_receive_timeout": 30
       }
     }
     ```
3. Create datasets on `analytics.crypto_hourly` and any Postgres tables that summarise
   macro data. Build dashboards combining both sources.

### 6. Spark job internals

`spark-apps/aggregate_crypto.py` reads the raw Binance payload from S3 via the `s3a://`
connector, explodes and aggregates it to hourly granularity, and stores the result
back to `s3://<bucket>/<prefix>/silver/prices_hourly/date=<YYYY-MM-DD>/`.
Aggregation uses `first/last/avg/sum` metrics per symbol/hour.

The downstream Python task reads these parquet files, merges them with pandas, and
upserts the rows into ClickHouse.

### 7. Debugging tips

- Verify access to S3 from the host (`aws s3 ls --profile my-s3`).
- Spark UI available at `http://<host>:8080`.
- ClickHouse HTTP console: `docker compose exec clickhouse clickhouse-client`.
- Mongo shell: `docker compose exec mongo mongosh -u $MONGO_INITDB_ROOT_USERNAME -p $MONGO_INITDB_ROOT_PASSWORD`.
- Airflow + Spark logs live under `logs/` and `spark-logs/`.

This foundation can be extended with additional transformations, ML pipelines, and
Superset dashboards as the project evolves.
