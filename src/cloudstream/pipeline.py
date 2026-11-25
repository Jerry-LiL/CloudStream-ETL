"""Run one ingest date from landing through the warehouse load."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from cloudstream.config import Settings
from cloudstream.gold import build_gold
from cloudstream.landing import land_bronze
from cloudstream.quality import check_row_counts, check_uniqueness
from cloudstream.silver import build_silver
from cloudstream.spark_session import build_spark
from cloudstream.warehouse import load_star_schema

logger = logging.getLogger(__name__)


def run_day(settings: Settings, ingest_date: str, spark: SparkSession | None = None) -> dict:
    land_bronze(settings, ingest_date)
    session = spark or build_spark(settings)
    summary = build_silver(settings, ingest_date, session)
    build_gold(settings, ingest_date, session)
    counts = check_row_counts(session, settings, ingest_date)
    check_uniqueness(session, settings, ingest_date)
    load_star_schema(settings, ingest_date)
    logger.info("Finished %s", ingest_date)
    return {"silver": summary, "row_counts": counts}


def run_silver(settings: Settings, ingest_date: str, spark: SparkSession | None = None):
    session = spark or build_spark(settings)
    return build_silver(settings, ingest_date, session)


def run_gold(settings: Settings, ingest_date: str, spark: SparkSession | None = None) -> None:
    session = spark or build_spark(settings)
    build_gold(settings, ingest_date, session)


def run_quality(settings: Settings, ingest_date: str, spark: SparkSession | None = None) -> dict:
    session = spark or build_spark(settings)
    counts = check_row_counts(session, settings, ingest_date)
    check_uniqueness(session, settings, ingest_date)
    return counts
