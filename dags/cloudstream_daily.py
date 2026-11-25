"""Daily CloudStream load.

Credentials stay in the environment (see .env.example). This file only
names the variables the tasks read at runtime.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from airflow import DAG
from airflow.operators.python import PythonOperator


def _land(ds, **_):
    from cloudstream.config import Settings
    from cloudstream.landing import land_bronze

    land_bronze(Settings.from_env(), ds)


def _silver(ds, **_):
    from cloudstream.config import Settings
    from cloudstream.pipeline import run_silver

    run_silver(Settings.from_env(), ds)


def _gold(ds, **_):
    from cloudstream.config import Settings
    from cloudstream.pipeline import run_gold

    run_gold(Settings.from_env(), ds)


def _row_counts(ds, **_):
    from cloudstream.config import Settings
    from cloudstream.quality import check_row_counts
    from cloudstream.spark_session import build_spark

    settings = Settings.from_env()
    check_row_counts(build_spark(settings), settings, ds)


def _uniqueness(ds, **_):
    from cloudstream.config import Settings
    from cloudstream.quality import check_uniqueness
    from cloudstream.spark_session import build_spark

    settings = Settings.from_env()
    check_uniqueness(build_spark(settings), settings, ds)


def _load(ds, **_):
    from cloudstream.config import Settings
    from cloudstream.warehouse import load_star_schema

    load_star_schema(Settings.from_env(), ds)


with DAG(
    dag_id="cloudstream_daily",
    description="Land daily files, curate silver and gold, merge the star schema, then check it.",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "vineet",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "depends_on_past": False,
    },
    tags=["cloudstream", "lakehouse"],
) as dag:
    land_bronze = PythonOperator(task_id="land_bronze", python_callable=_land)
    bronze_to_silver = PythonOperator(task_id="bronze_to_silver", python_callable=_silver)
    silver_to_gold = PythonOperator(task_id="silver_to_gold", python_callable=_gold)
    dq_row_counts = PythonOperator(task_id="dq_row_counts", python_callable=_row_counts)
    dq_uniqueness = PythonOperator(task_id="dq_uniqueness", python_callable=_uniqueness)
    load_snowflake = PythonOperator(task_id="load_snowflake", python_callable=_load)

    (
        land_bronze
        >> bronze_to_silver
        >> silver_to_gold
        >> dq_row_counts
        >> dq_uniqueness
        >> load_snowflake
    )
