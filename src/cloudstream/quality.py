"""Lake data-quality checks: row-count reconciliation and key uniqueness."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from cloudstream.config import Settings, validate_ingest_date
from cloudstream.layout import LakeLayout
from cloudstream.schemas import (
    customer_dim_schema,
    customer_silver_schema,
    date_dim_schema,
    fact_schema,
    order_silver_schema,
    product_dim_schema,
    product_silver_schema,
)
from cloudstream.storage import read_parquet

logger = logging.getLogger(__name__)


class QualityError(RuntimeError):
    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__("Data-quality checks failed: " + "; ".join(failures))


def check_row_counts(spark: SparkSession, settings: Settings, ingest_date: str) -> dict[str, int]:
    ingest_date = validate_ingest_date(ingest_date)
    layout = LakeLayout(settings)
    orders = read_parquet(spark, layout.silver_uri("orders", ingest_date), order_silver_schema())
    customers = read_parquet(
        spark, layout.silver_uri("customers", ingest_date), customer_silver_schema()
    )
    products = read_parquet(
        spark, layout.silver_uri("products", ingest_date), product_silver_schema()
    )
    facts = read_parquet(spark, layout.gold_uri("fact_orders"), fact_schema())
    dim_customers = read_parquet(spark, layout.gold_uri("dim_customers"), customer_dim_schema())
    dim_products = read_parquet(spark, layout.gold_uri("dim_products"), product_dim_schema())

    silver_orders = orders.count()
    matched_orders = facts.join(
        orders.select("order_id").distinct(), "order_id", "left_semi"
    ).count()
    silver_customers = customers.select("customer_id").distinct().count()
    matched_customers = (
        dim_customers.join(customers.select("customer_id").distinct(), "customer_id", "left_semi")
        .select("customer_id")
        .distinct()
        .count()
    )
    silver_products = products.select("product_id").distinct().count()
    matched_products = (
        dim_products.join(products.select("product_id").distinct(), "product_id", "left_semi")
        .select("product_id")
        .distinct()
        .count()
    )

    counts = {
        "silver_orders": silver_orders,
        "fact_orders_for_partition": matched_orders,
        "silver_customers": silver_customers,
        "dim_customers_for_partition": matched_customers,
        "silver_products": silver_products,
        "dim_products_for_partition": matched_products,
    }
    failures = []
    if matched_orders != silver_orders:
        failures.append(
            f"row count: silver orders for {ingest_date} = {silver_orders}, "
            f"fact_orders rows for those keys = {matched_orders}"
        )
    if matched_customers != silver_customers:
        failures.append(
            f"row count: silver customers for {ingest_date} = {silver_customers}, "
            f"dim_customers keys found = {matched_customers}"
        )
    if matched_products != silver_products:
        failures.append(
            f"row count: silver products for {ingest_date} = {silver_products}, "
            f"dim_products keys found = {matched_products}"
        )
    if failures:
        raise QualityError(failures)
    logger.info("Row-count checks passed for %s: %s", ingest_date, counts)
    return counts


def check_uniqueness(spark: SparkSession, settings: Settings, ingest_date: str) -> None:
    validate_ingest_date(ingest_date)
    layout = LakeLayout(settings)
    facts = read_parquet(spark, layout.gold_uri("fact_orders"), fact_schema())
    customers = read_parquet(spark, layout.gold_uri("dim_customers"), customer_dim_schema())
    products = read_parquet(spark, layout.gold_uri("dim_products"), product_dim_schema())
    dates = read_parquet(spark, layout.gold_uri("dim_date"), date_dim_schema())

    failures: list[str] = []
    failures.extend(_duplicate_keys(facts, "order_id", "fact_orders.order_id"))
    failures.extend(_duplicate_keys(customers, "customer_sk", "dim_customers.customer_sk"))
    failures.extend(_duplicate_keys(products, "product_id", "dim_products.product_id"))
    failures.extend(_duplicate_keys(dates, "date_sk", "dim_date.date_sk"))

    current_dupes = (
        customers.filter(col("is_current"))
        .groupBy("customer_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )
    if current_dupes:
        failures.append(
            f"dim_customers has {current_dupes} customer_id values with multiple current rows"
        )

    missing_keys = facts.filter(
        col("customer_sk").isNull() | col("product_sk").isNull() | col("date_sk").isNull()
    ).count()
    if missing_keys:
        failures.append(f"fact_orders has {missing_keys} row(s) with a null dimension key")

    orphan_dates = facts.join(dates.select("date_sk"), "date_sk", "left_anti").count()
    if orphan_dates:
        failures.append(f"fact_orders has {orphan_dates} row(s) whose date_sk is not in dim_date")

    if failures:
        raise QualityError(failures)
    logger.info("Uniqueness checks passed for %s", ingest_date)


def _duplicate_keys(frame, key: str, label: str) -> list[str]:
    duplicates = frame.groupBy(key).count().filter(col("count") > 1).count()
    if duplicates:
        return [f"{label} has {duplicates} duplicated keys"]
    return []
