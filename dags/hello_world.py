from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "dlaze",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def insert_execution_event(**context) -> None:
    """Persist metadata about the current DAG run into Postgres."""
    hook = PostgresHook(postgres_conn_id="demo_db")
    dag_run = context["dag_run"]
    logical_date = context["logical_date"].in_timezone("UTC")

    hook.run(
        """
        INSERT INTO demo_events (run_id, dag_id, event_time, status)
        VALUES (%s, %s, %s, %s);
        """,
        parameters=(
            dag_run.run_id,
            dag_run.dag_id,
            logical_date.isoformat(),
            "success",
        ),
    )


def log_latest_event() -> None:
    """Fetch and log the latest record for quick validation in task logs."""
    hook = PostgresHook(postgres_conn_id="demo_db")
    row = hook.get_first(
        """
        SELECT run_id, dag_id, status, event_time, created_at
        FROM demo_events
        ORDER BY created_at DESC
        LIMIT 1;
        """
    )

    if row:
        run_id, dag_id, status, event_time, created_at = row
        logging.info(
            "Most recent event -> run_id=%s dag_id=%s status=%s event_time=%s created_at=%s",
            run_id,
            dag_id,
            status,
            event_time,
            created_at,
        )
    else:
        logging.info("demo_events пустая — выполните DAG для создания записей.")


with DAG(
    dag_id="hello_world",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["demo", "postgres"],
) as dag:
    start = EmptyOperator(task_id="start")

    ensure_table = PostgresOperator(
        task_id="ensure_table",
        postgres_conn_id="demo_db",
        sql="""
        CREATE TABLE IF NOT EXISTS demo_events (
            id SERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            dag_id TEXT NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
    )

    store_run = PythonOperator(
        task_id="store_run",
        python_callable=insert_execution_event,
    )

    show_latest = PythonOperator(
        task_id="show_latest",
        python_callable=log_latest_event,
    )

    finish = EmptyOperator(task_id="finish")

    start >> ensure_table >> store_run >> show_latest >> finish
