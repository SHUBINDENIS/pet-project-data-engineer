from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.hooks.http import HttpHook

FX_HTTP_CONN_ID = "fx_api"
FX_DB_CONN_ID = "demo_db"

FX_BASE_CURRENCY = os.getenv("FX_BASE_CURRENCY", "USD").upper()
FX_TARGET_CURRENCIES = [
    code.strip().upper()
    for code in os.getenv("FX_TARGET_CURRENCIES", "EUR,GBP").split(",")
    if code.strip()
]
FX_API_ENDPOINT = os.getenv("FX_API_ENDPOINT", "/latest")

default_args = {
    "owner": "dlaze",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def fetch_rates(**context) -> Dict[str, Any]:
    """Fetch currency rates from the configured HTTP endpoint."""
    http_hook = HttpHook(method="GET", http_conn_id=FX_HTTP_CONN_ID)
    session = http_hook.get_conn()
    base_url = (http_hook.base_url or os.getenv("FX_API_BASE_URL", "")).rstrip("/")

    execution_date = context["logical_date"].astimezone(tz=None).date()
    endpoint_template = os.getenv(
        "FX_API_ENDPOINT", "/archive/{year}/{month}/{day}/daily_json.js"
    )
    endpoint = endpoint_template.format(
        year=f"{execution_date:%Y}",
        month=f"{execution_date:%m}",
        day=f"{execution_date:%d}",
    ).lstrip("/")
    request_url = f"{base_url}/{endpoint}"

    response = session.get(request_url, timeout=30)
    if response.status_code == 404:
        raise AirflowSkipException(
            f"CBR does not publish a rate for {execution_date}; skipping."
        )
    response.raise_for_status()

    payload = response.json()
    if "Valute" not in payload:
        raise AirflowSkipException(
            f"CBR payload for {execution_date} does not contain rates: {payload}"
        )

    valutes = payload["Valute"]
    filtered_rates: Dict[str, float] = {}
    for currency in FX_TARGET_CURRENCIES:
        valute = valutes.get(currency)
        if not valute:
            logging.warning(
                "Currency %s is missing in CBR payload for %s", currency, execution_date
            )
            continue
        nominal = valute.get("Nominal") or 1
        value = valute.get("Value")
        if value is None:
            logging.warning(
                "Currency %s has no rate value in CBR payload for %s",
                currency,
                execution_date,
            )
            continue
        filtered_rates[currency] = float(value) / float(nominal)

    if not filtered_rates:
        raise AirflowSkipException(
            f"No requested currencies found in payload for {execution_date}"
        )

    result = {
        "base": FX_BASE_CURRENCY,
        "date": execution_date.isoformat(),
        "rates": filtered_rates,
        "source": response.url,
    }
    logging.info(
        "Fetched FX payload for %s with %d rates from %s for %s",
        FX_BASE_CURRENCY,
        len(filtered_rates),
        result["source"],
        execution_date,
    )
    return result


def store_rates(**context) -> None:
    """Persist fetched currency data into Postgres with upsert semantics."""
    ti = context["ti"]
    rates_payload = ti.xcom_pull(task_ids="fetch_rates")
    if not rates_payload:
        raise ValueError("No FX payload found in XCom; aborting.")

    base_currency = rates_payload["base"]
    rate_date = datetime.fromisoformat(rates_payload["date"]).date()
    rates = rates_payload["rates"]
    source = rates_payload["source"]

    hook = PostgresHook(postgres_conn_id=FX_DB_CONN_ID)
    upsert_sql = """
        INSERT INTO fx_rates (base_currency, quote_currency, rate_date, rate, source)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (base_currency, quote_currency, rate_date)
        DO UPDATE SET
            rate = EXCLUDED.rate,
            source = EXCLUDED.source,
            fetched_at = NOW();
    """

    for quote_currency, rate in rates.items():
        hook.run(
            upsert_sql,
            parameters=(base_currency, quote_currency, rate_date, rate, source),
        )
    logging.info(
        "Upserted %d FX rows for %s on %s",
        len(rates),
        base_currency,
        rate_date,
    )


def log_row_count() -> None:
    """Log how many records we currently store for sanity checks."""
    hook = PostgresHook(postgres_conn_id=FX_DB_CONN_ID)
    count = hook.get_first(
        """
        SELECT COUNT(*)
        FROM fx_rates
        WHERE base_currency = %s;
        """,
        parameters=(FX_BASE_CURRENCY,),
    )[0]
    logging.info(
        "fx_rates now contains %s rows for base currency %s",
        count,
        FX_BASE_CURRENCY,
    )


with DAG(
    dag_id="fx_rates",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=True,
    default_args=default_args,
    tags=["fx", "postgres", "superset"],
) as dag:
    start = EmptyOperator(task_id="start")

    ensure_fx_table = PostgresOperator(
        task_id="ensure_fx_table",
        postgres_conn_id=FX_DB_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS fx_rates (
            id BIGSERIAL PRIMARY KEY,
            base_currency VARCHAR(3) NOT NULL,
            quote_currency VARCHAR(3) NOT NULL,
            rate_date DATE NOT NULL,
            rate NUMERIC(18, 8) NOT NULL,
            source TEXT NOT NULL,
            fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(base_currency, quote_currency, rate_date)
        );
        CREATE INDEX IF NOT EXISTS idx_fx_rates_date_currency
            ON fx_rates (rate_date, quote_currency);
        """,
    )

    fetch_fx_rates = PythonOperator(
        task_id="fetch_rates",
        python_callable=fetch_rates,
        provide_context=True,
    )

    upsert_rates = PythonOperator(
        task_id="upsert_rates",
        python_callable=store_rates,
        provide_context=True,
    )

    log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_row_count,
    )

    finish = EmptyOperator(task_id="finish")

    start >> ensure_fx_table >> fetch_fx_rates >> upsert_rates >> log_summary >> finish
