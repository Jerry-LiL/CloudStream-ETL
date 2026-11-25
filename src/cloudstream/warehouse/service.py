"""Pick the warehouse backend and load one partition."""

from __future__ import annotations

from cloudstream.config import Settings, validate_ingest_date
from cloudstream.warehouse.duckdb_loader import DuckDBLoader
from cloudstream.warehouse.snowflake_loader import SnowflakeLoader


def load_star_schema(settings: Settings, ingest_date: str) -> None:
    ingest_date = validate_ingest_date(ingest_date)
    if settings.warehouse_backend == "duckdb":
        DuckDBLoader(settings).load(ingest_date)
        return
    SnowflakeLoader(settings).load(ingest_date)
