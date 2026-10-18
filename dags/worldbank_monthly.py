"""Monthly World Bank indicator pull for Libya (LBY)."""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from ly_ingest.worldbank import run_once

with DAG(
    dag_id="worldbank_monthly",
    description="Pull configured World Bank indicators for LBY into ly.events.raw.",
    schedule="@monthly",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["libya", "ingest", "worldbank"],
) as dag:
    PythonOperator(task_id="worldbank_run_once", python_callable=run_once)
