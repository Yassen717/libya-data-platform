"""Hourly GDELT DOC 2 poll for Libya-tagged articles."""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from ly_ingest.gdelt import run_once

with DAG(
    dag_id="gdelt_hourly",
    description="Poll the GDELT DOC 2 API for Libya articles into ly.events.raw.",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["libya", "ingest", "gdelt"],
) as dag:
    PythonOperator(task_id="gdelt_run_once", python_callable=run_once)
