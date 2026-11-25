"""Declared schemas. Readers use these instead of inferring types."""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDER_STATUSES = ("PLACED", "SHIPPED", "CANCELLED", "RETURNED")

ORDER_FILE_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "order_ts",
    "quantity",
    "unit_price",
    "currency",
    "status",
    "updated_at",
]

CUSTOMER_FILE_COLUMNS = [
    "customer_id",
    "full_name",
    "email",
    "city",
    "region",
    "segment",
    "updated_at",
]

PRODUCT_FILE_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "brand",
    "list_price",
    "updated_at",
]

CUSTOMER_ATTRIBUTES = ["full_name", "email", "city", "region", "segment"]
PRODUCT_ATTRIBUTES = ["product_name", "category", "brand", "list_price"]


class SchemaError(ValueError):
    """The extract header does not match the contract for that dataset."""


def require_columns(actual: list[str], required: list[str], source: str) -> None:
    missing = [name for name in required if name not in actual]
    if missing:
        raise SchemaError(f"{source} is missing required columns: {', '.join(missing)}")


def _fields(*pairs: tuple) -> StructType:
    return StructType([StructField(name, dtype, nullable) for name, dtype, nullable in pairs])


def order_raw_schema() -> StructType:
    return _fields(
        ("order_id", StringType(), True),
        ("customer_id", StringType(), True),
        ("product_id", StringType(), True),
        ("order_ts", TimestampType(), True),
        ("quantity", IntegerType(), True),
        ("unit_price", DecimalType(12, 2), True),
        ("currency", StringType(), True),
        ("status", StringType(), True),
        ("updated_at", TimestampType(), True),
        ("_corrupt", StringType(), True),
    )


def customer_raw_schema() -> StructType:
    return _fields(
        ("customer_id", StringType(), True),
        ("full_name", StringType(), True),
        ("email", StringType(), True),
        ("city", StringType(), True),
        ("region", StringType(), True),
        ("segment", StringType(), True),
        ("updated_at", TimestampType(), True),
        ("_corrupt", StringType(), True),
    )


def product_raw_schema() -> StructType:
    return _fields(
        ("product_id", StringType(), True),
        ("product_name", StringType(), True),
        ("category", StringType(), True),
        ("brand", StringType(), True),
        ("list_price", DecimalType(12, 2), True),
        ("updated_at", TimestampType(), True),
        ("_corrupt", StringType(), True),
    )


def order_silver_schema() -> StructType:
    return _fields(
        ("order_id", StringType(), False),
        ("customer_id", StringType(), False),
        ("product_id", StringType(), False),
        ("order_ts", TimestampType(), False),
        ("quantity", IntegerType(), False),
        ("unit_price", DecimalType(12, 2), False),
        ("extended_amount", DecimalType(14, 2), False),
        ("currency", StringType(), False),
        ("status", StringType(), False),
        ("updated_at", TimestampType(), False),
        ("date_sk", IntegerType(), False),
        ("ingest_date", DateType(), False),
    )


def customer_silver_schema() -> StructType:
    return _fields(
        ("customer_sk", StringType(), False),
        ("customer_id", StringType(), False),
        ("full_name", StringType(), True),
        ("email", StringType(), True),
        ("city", StringType(), True),
        ("region", StringType(), True),
        ("segment", StringType(), True),
        ("attribute_hash", StringType(), False),
        ("updated_at", TimestampType(), False),
        ("ingest_date", DateType(), False),
    )


def product_silver_schema() -> StructType:
    return _fields(
        ("product_sk", StringType(), False),
        ("product_id", StringType(), False),
        ("product_name", StringType(), True),
        ("category", StringType(), True),
        ("brand", StringType(), True),
        ("list_price", DecimalType(12, 2), False),
        ("attribute_hash", StringType(), False),
        ("updated_at", TimestampType(), False),
        ("ingest_date", DateType(), False),
    )


def customer_dim_schema() -> StructType:
    return _fields(
        ("customer_sk", StringType(), False),
        ("customer_id", StringType(), False),
        ("full_name", StringType(), True),
        ("email", StringType(), True),
        ("city", StringType(), True),
        ("region", StringType(), True),
        ("segment", StringType(), True),
        ("attribute_hash", StringType(), False),
        ("effective_from", TimestampType(), False),
        ("effective_to", TimestampType(), True),
        ("is_current", BooleanType(), False),
    )


def product_dim_schema() -> StructType:
    return _fields(
        ("product_sk", StringType(), False),
        ("product_id", StringType(), False),
        ("product_name", StringType(), True),
        ("category", StringType(), True),
        ("brand", StringType(), True),
        ("list_price", DecimalType(12, 2), False),
        ("attribute_hash", StringType(), False),
        ("updated_at", TimestampType(), False),
    )


def date_dim_schema() -> StructType:
    return _fields(
        ("date_sk", IntegerType(), False),
        ("full_date", DateType(), False),
        ("year", IntegerType(), False),
        ("quarter", IntegerType(), False),
        ("month", IntegerType(), False),
        ("month_name", StringType(), False),
        ("day_of_month", IntegerType(), False),
        ("day_name", StringType(), False),
        ("is_weekend", BooleanType(), False),
    )


def fact_schema() -> StructType:
    return _fields(
        ("order_id", StringType(), False),
        ("customer_sk", StringType(), True),
        ("product_sk", StringType(), True),
        ("date_sk", IntegerType(), True),
        ("customer_id", StringType(), True),
        ("product_id", StringType(), True),
        ("order_ts", TimestampType(), True),
        ("quantity", IntegerType(), True),
        ("unit_price", DecimalType(12, 2), True),
        ("extended_amount", DecimalType(14, 2), True),
        ("currency", StringType(), True),
        ("status", StringType(), True),
        ("updated_at", TimestampType(), True),
        ("ingest_date", DateType(), True),
    )
