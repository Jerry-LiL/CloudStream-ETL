"""Apply the MERGE scripts to a local DuckDB file."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from cloudstream.config import Settings, validate_ingest_date
from cloudstream.layout import LakeLayout
from cloudstream.quality import QualityError
from cloudstream.warehouse.columns import (
    CUSTOMER_COLUMNS,
    DATE_COLUMNS,
    EMPTY_TABLES,
    ORDER_COLUMNS,
    PRODUCT_COLUMNS,
)
from cloudstream.warehouse.sql import read_script

logger = logging.getLogger(__name__)


class DuckDBLoader:
    def __init__(self, settings: Settings):
        self.settings = settings

    def load(self, ingest_date: str) -> None:
        ingest_date = validate_ingest_date(ingest_date)
        layout = LakeLayout(self.settings)
        paths = {
            "dates": layout.materialize("gold", "dim_date"),
            "products": layout.materialize("silver", "products", f"ingest_date={ingest_date}"),
            "customers": layout.materialize("silver", "customers", f"ingest_date={ingest_date}"),
            "orders": layout.materialize("silver", "orders", f"ingest_date={ingest_date}"),
            "dim_products": layout.materialize("gold", "dim_products"),
            "dim_customers": layout.materialize("gold", "dim_customers"),
            "facts": layout.materialize("gold", "fact_orders"),
        }
        self.settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(self.settings.duckdb_path))
        try:
            connection.execute("SET TimeZone='UTC'")
            connection.execute("BEGIN TRANSACTION")
            try:
                self._apply(connection, paths)
            except Exception:
                connection.execute("ROLLBACK")
                raise
            else:
                connection.execute("COMMIT")
        finally:
            connection.close()
        logger.info("Loaded %s into %s", ingest_date, self.settings.duckdb_path)

    def _apply(self, connection, paths: dict[str, Path]) -> None:
        for statement in read_script("duckdb", "01_ddl.sql"):
            connection.execute(statement)
        _stage(connection, "staging_dates", paths["dates"], DATE_COLUMNS)
        for statement in read_script("merge", "02_merge_dim_date.sql"):
            connection.execute(statement)
        _stage(connection, "staging_products", paths["products"], PRODUCT_COLUMNS)
        for statement in read_script("merge", "03_merge_dim_products.sql"):
            connection.execute(statement)
        _stage(connection, "staging_customers", paths["customers"], CUSTOMER_COLUMNS)
        for statement in read_script("merge", "04_merge_dim_customers.sql"):
            connection.execute(statement)
        _stage(connection, "staging_orders", paths["orders"], ORDER_COLUMNS)
        for statement in read_script("merge", "05_resolve_and_merge_facts.sql"):
            connection.execute(statement)

        failures = _quality_failures(connection)
        failures.extend(_lake_mismatches(connection, paths))
        if failures:
            raise QualityError(failures)


def _stage(connection, table: str, path: Path, columns: list[str]) -> None:
    files = _parquet_files(path)
    select_list = ", ".join(columns)
    if not files:
        connection.execute(f"CREATE OR REPLACE TABLE {table} ({EMPTY_TABLES[table]})")
        return
    connection.execute(
        f"CREATE OR REPLACE TABLE {table} AS "
        f"SELECT {select_list} FROM read_parquet([{_file_list(files)}])"
    )


def _quality_failures(connection) -> list[str]:
    rows = connection.execute(read_script("merge", "06_quality.sql")[0]).fetchall()
    return [f"{name} ({count})" for name, count in rows if count]


def _lake_mismatches(connection, paths: dict[str, Path]) -> list[str]:
    comparisons = [
        ("dim_date", paths["dates"], "SELECT COUNT(*) FROM dim_date", None),
        ("dim_products", paths["dim_products"], "SELECT COUNT(*) FROM dim_products", None),
        ("dim_customers", paths["dim_customers"], "SELECT COUNT(*) FROM dim_customers", None),
        ("fact_orders", paths["facts"], "SELECT COUNT(*) FROM fact_orders", None),
        (
            "current dim_customers",
            paths["dim_customers"],
            "SELECT COUNT(*) FROM dim_customers WHERE is_current",
            "is_current",
        ),
    ]
    failures = []
    for label, path, warehouse_sql, where in comparisons:
        lake_count = _count_parquet(connection, path, where)
        warehouse_count = connection.execute(warehouse_sql).fetchone()[0]
        if lake_count != warehouse_count:
            failures.append(
                f"row count: lake {label} = {lake_count}, warehouse {label} = {warehouse_count}"
            )
    return failures


def _count_parquet(connection, path: Path, where: str | None) -> int:
    files = _parquet_files(path)
    if not files:
        return 0
    sql = f"SELECT COUNT(*) FROM read_parquet([{_file_list(files)}])"
    if where:
        sql += f" WHERE {where}"
    return connection.execute(sql).fetchone()[0]


def _parquet_files(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return sorted(
        item for item in path.rglob("*.parquet") if item.is_file() and item.stat().st_size > 0
    )


def _file_list(files: list[Path]) -> str:
    return ", ".join("'" + str(item).replace("'", "") + "'" for item in files)
