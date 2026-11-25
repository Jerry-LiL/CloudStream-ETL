"""Load a partition into Snowflake and run the same MERGE scripts.

The connection values come from Settings, which reads the environment.
This module does not embed account names, users, or passwords.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pyarrow.parquet as pq

from cloudstream.config import Settings, validate_ingest_date
from cloudstream.layout import LakeLayout
from cloudstream.quality import QualityError
from cloudstream.warehouse.columns import (
    CUSTOMER_COLUMNS,
    DATE_COLUMNS,
    ORDER_COLUMNS,
    PRODUCT_COLUMNS,
)
from cloudstream.warehouse.sql import read_script

logger = logging.getLogger(__name__)

_STAGING = (
    ("STAGING_DATES", "dates", DATE_COLUMNS),
    ("STAGING_PRODUCTS", "products", PRODUCT_COLUMNS),
    ("STAGING_CUSTOMERS", "customers", CUSTOMER_COLUMNS),
    ("STAGING_ORDERS", "orders", ORDER_COLUMNS),
)


def snowflake_connect_kwargs(settings: Settings) -> dict:
    required = {
        "account": settings.snowflake_account,
        "user": settings.snowflake_user,
        "password": settings.snowflake_password,
        "warehouse": settings.snowflake_warehouse,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        names = ", ".join(f"SNOWFLAKE_{name.upper()}" for name in missing)
        raise RuntimeError(f"Missing Snowflake settings: {names}")
    kwargs = {
        "account": settings.snowflake_account,
        "user": settings.snowflake_user,
        "password": settings.snowflake_password,
        "warehouse": settings.snowflake_warehouse,
        "database": settings.snowflake_database,
        "schema": settings.snowflake_schema,
    }
    if settings.snowflake_role:
        kwargs["role"] = settings.snowflake_role
    return kwargs


class SnowflakeLoader:
    def __init__(self, settings: Settings, connect=None):
        self.settings = settings
        self._connect = connect

    def load(self, ingest_date: str) -> None:
        ingest_date = validate_ingest_date(ingest_date)
        paths = _paths(self.settings, ingest_date)
        connection = self._open()
        try:
            if hasattr(connection, "autocommit"):
                connection.autocommit(False)
            cursor = connection.cursor()
            try:
                self._apply(connection, cursor, paths)
            except Exception:
                connection.rollback()
                raise
            else:
                connection.commit()
            finally:
                cursor.close()
        finally:
            connection.close()
        logger.info("Loaded %s into Snowflake %s", ingest_date, self.settings.snowflake_database)

    def _open(self):
        if self._connect is not None:
            return self._connect()
        import snowflake.connector

        return snowflake.connector.connect(**snowflake_connect_kwargs(self.settings))

    def _apply(self, connection, cursor, paths: dict[str, Path]) -> None:
        for statement in read_script("snowflake", "01_ddl.sql"):
            cursor.execute(statement)
        for statement in read_script("snowflake", "02_staging.sql"):
            cursor.execute(statement)
        for table_name, path_key, columns in _STAGING:
            frame = _read_frame(paths[path_key], columns)
            if frame.empty:
                continue
            _write_frame(connection, frame, table_name, self.settings)
        for script in (
            ("merge", "02_merge_dim_date.sql"),
            ("merge", "03_merge_dim_products.sql"),
            ("merge", "04_merge_dim_customers.sql"),
            ("merge", "05_resolve_and_merge_facts.sql"),
        ):
            for statement in read_script(*script):
                cursor.execute(statement)

        failures = _quality_failures(cursor)
        failures.extend(_lake_mismatches(cursor, paths))
        if failures:
            raise QualityError(failures)


def _paths(settings: Settings, ingest_date: str) -> dict[str, Path]:
    layout = LakeLayout(settings)
    return {
        "dates": layout.materialize("gold", "dim_date"),
        "products": layout.materialize("silver", "products", f"ingest_date={ingest_date}"),
        "customers": layout.materialize("silver", "customers", f"ingest_date={ingest_date}"),
        "orders": layout.materialize("silver", "orders", f"ingest_date={ingest_date}"),
        "dim_products": layout.materialize("gold", "dim_products"),
        "dim_customers": layout.materialize("gold", "dim_customers"),
        "facts": layout.materialize("gold", "fact_orders"),
    }


def _write_frame(connection, frame, table_name: str, settings: Settings) -> None:
    from snowflake.connector.pandas_tools import write_pandas

    write_pandas(
        connection,
        frame,
        table_name=table_name,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
        quote_identifiers=False,
        auto_create_table=False,
        overwrite=True,
    )


def _read_frame(path: Path, columns: list[str]):
    import pandas as pd

    files = _parquet_files(path)
    if not files:
        return pd.DataFrame(columns=columns)
    table = pq.read_table([str(item) for item in files], columns=columns)
    return table.to_pandas()


def _quality_failures(cursor) -> list[str]:
    cursor.execute(read_script("merge", "06_quality.sql")[0])
    return [f"{name} ({count})" for name, count in cursor.fetchall() if count]


def _lake_mismatches(cursor, paths: dict[str, Path]) -> list[str]:
    import pyarrow.compute as pc

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
    for label, path, warehouse_sql, flag in comparisons:
        cursor.execute(warehouse_sql)
        warehouse_count = int(cursor.fetchone()[0])
        lake_count = _arrow_count(path, flag, pc)
        if lake_count != warehouse_count:
            failures.append(
                f"row count: lake {label} = {lake_count}, warehouse {label} = {warehouse_count}"
            )
    return failures


def _arrow_count(path: Path, flag: str | None, pc) -> int:
    files = _parquet_files(path)
    if not files:
        return 0
    table = pq.read_table([str(item) for item in files])
    if flag:
        table = table.filter(pc.equal(table[flag], True))
    return table.num_rows


def _parquet_files(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return sorted(
        item for item in path.rglob("*.parquet") if item.is_file() and item.stat().st_size > 0
    )
