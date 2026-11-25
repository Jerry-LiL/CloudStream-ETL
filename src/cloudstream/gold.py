"""Build the gold star schema for one partition.

Customers are SCD Type 2. A changed attribute closes the open version and
inserts a new one. Replaying a partition is a no-op once that version is
current, and an older partition cannot overwrite a newer version.

Products are SCD Type 1: one row per product, latest update wins.
Facts are one row per order, also latest update wins, so a late replay of
yesterday's file will not put a stale status back.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    lit,
    month,
    quarter,
    row_number,
    year,
)
from pyspark.sql.window import Window

from cloudstream.config import Settings, validate_ingest_date
from cloudstream.layout import LakeLayout
from cloudstream.schemas import (
    customer_dim_schema,
    customer_silver_schema,
    fact_schema,
    order_silver_schema,
    product_dim_schema,
    product_silver_schema,
)
from cloudstream.storage import publish_parquet, read_parquet

logger = logging.getLogger(__name__)

_CUSTOMER_COLUMNS = [field.name for field in customer_dim_schema().fields]
_PRODUCT_COLUMNS = [field.name for field in product_dim_schema().fields]
_FACT_COLUMNS = [field.name for field in fact_schema().fields]


def build_gold(settings: Settings, ingest_date: str, spark: SparkSession) -> None:
    ingest_date = validate_ingest_date(ingest_date)
    layout = LakeLayout(settings)
    products = _build_products(spark, layout, ingest_date)
    customers = _build_customers(spark, layout, ingest_date)
    dates = _build_dates(spark, layout)
    facts = _build_facts(spark, layout, ingest_date, products, customers)
    logger.info(
        "Gold %s facts=%s customers=%s products=%s dates=%s",
        ingest_date,
        facts.count(),
        customers.count(),
        products.count(),
        dates.count(),
    )
    for frame in (products, customers, dates, facts):
        frame.unpersist()


def _build_products(spark: SparkSession, layout: LakeLayout, ingest_date: str) -> DataFrame:
    incoming = read_parquet(
        spark, layout.silver_uri("products", ingest_date), product_silver_schema()
    ).select(*_PRODUCT_COLUMNS)
    existing = read_parquet(spark, layout.gold_uri("dim_products"), product_dim_schema())
    winners = _latest(
        existing.unionByName(incoming),
        "product_id",
        [col("updated_at").desc_nulls_last(), col("attribute_hash").desc_nulls_last()],
    )
    return publish_parquet(winners, layout.gold_uri("dim_products"))


def _build_customers(spark: SparkSession, layout: LakeLayout, ingest_date: str) -> DataFrame:
    incoming = read_parquet(
        spark, layout.silver_uri("customers", ingest_date), customer_silver_schema()
    ).select(
        "customer_sk",
        "customer_id",
        "full_name",
        "email",
        "city",
        "region",
        "segment",
        "attribute_hash",
        col("updated_at").alias("effective_from"),
    )
    existing = read_parquet(spark, layout.gold_uri("dim_customers"), customer_dim_schema()).cache()
    historical = existing.filter(~col("is_current"))
    current = existing.filter(col("is_current"))
    paired = current.alias("c").join(
        incoming.alias("n"),
        col("c.customer_id") == col("n.customer_id"),
        "left",
    )

    # Keep the open row when this file does not contain a newer attribute change.
    unchanged = paired.filter(
        col("n.customer_id").isNull()
        | (col("n.attribute_hash") == col("c.attribute_hash"))
        | (col("n.effective_from") < col("c.effective_from"))
    ).select("c.*")

    closed = paired.filter(
        col("n.customer_id").isNotNull()
        & (col("n.attribute_hash") != col("c.attribute_hash"))
        & (col("n.effective_from") >= col("c.effective_from"))
    ).select(
        col("c.customer_sk").alias("customer_sk"),
        col("c.customer_id").alias("customer_id"),
        col("c.full_name").alias("full_name"),
        col("c.email").alias("email"),
        col("c.city").alias("city"),
        col("c.region").alias("region"),
        col("c.segment").alias("segment"),
        col("c.attribute_hash").alias("attribute_hash"),
        col("c.effective_from").alias("effective_from"),
        col("n.effective_from").alias("effective_to"),
        lit(False).alias("is_current"),
    )

    surviving_ids = unchanged.select("customer_id")
    opened = incoming.join(surviving_ids, "customer_id", "left_anti").select(
        "customer_sk",
        "customer_id",
        "full_name",
        "email",
        "city",
        "region",
        "segment",
        "attribute_hash",
        "effective_from",
        lit(None).cast("timestamp").alias("effective_to"),
        lit(True).alias("is_current"),
    )

    result = (
        historical.select(*_CUSTOMER_COLUMNS)
        .unionByName(unchanged.select(*_CUSTOMER_COLUMNS))
        .unionByName(closed.select(*_CUSTOMER_COLUMNS))
        .unionByName(opened.select(*_CUSTOMER_COLUMNS))
    )
    published = publish_parquet(result, layout.gold_uri("dim_customers"))
    existing.unpersist()
    return published


def _build_dates(spark: SparkSession, layout: LakeLayout) -> DataFrame:
    spine = spark.sql(
        "SELECT explode(sequence(to_date('2024-01-01'), to_date('2027-12-31'), interval 1 day)) "
        "AS full_date"
    )
    dates = spine.select(
        date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_sk"),
        col("full_date"),
        year(col("full_date")).alias("year"),
        quarter(col("full_date")).alias("quarter"),
        month(col("full_date")).alias("month"),
        date_format(col("full_date"), "MMMM").alias("month_name"),
        dayofmonth(col("full_date")).alias("day_of_month"),
        date_format(col("full_date"), "EEEE").alias("day_name"),
        dayofweek(col("full_date")).isin(1, 7).alias("is_weekend"),
    )
    return publish_parquet(dates, layout.gold_uri("dim_date"))


def _build_facts(
    spark: SparkSession,
    layout: LakeLayout,
    ingest_date: str,
    products: DataFrame,
    customers: DataFrame,
) -> DataFrame:
    orders = read_parquet(spark, layout.silver_uri("orders", ingest_date), order_silver_schema())
    resolved = (
        orders.alias("o")
        .join(
            products.alias("p"),
            col("o.product_id") == col("p.product_id"),
            "left",
        )
        .join(
            customers.alias("c"),
            (col("o.customer_id") == col("c.customer_id"))
            & (col("o.order_ts") >= col("c.effective_from"))
            & (col("c.effective_to").isNull() | (col("o.order_ts") < col("c.effective_to"))),
            "left",
        )
    )
    # If two customer versions both match, keep the one that started most recently.
    window = Window.partitionBy(col("o.order_id")).orderBy(
        col("c.effective_from").desc_nulls_last()
    )
    incoming = (
        resolved.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .select(
            col("o.order_id").alias("order_id"),
            col("c.customer_sk").alias("customer_sk"),
            col("p.product_sk").alias("product_sk"),
            col("o.date_sk").alias("date_sk"),
            col("o.customer_id").alias("customer_id"),
            col("o.product_id").alias("product_id"),
            col("o.order_ts").alias("order_ts"),
            col("o.quantity").alias("quantity"),
            col("o.unit_price").alias("unit_price"),
            col("o.extended_amount").alias("extended_amount"),
            col("o.currency").alias("currency"),
            col("o.status").alias("status"),
            col("o.updated_at").alias("updated_at"),
            col("o.ingest_date").alias("ingest_date"),
        )
    )
    existing = read_parquet(spark, layout.gold_uri("fact_orders"), fact_schema())
    winners = _latest(
        existing.unionByName(incoming.select(*_FACT_COLUMNS)),
        "order_id",
        [col("updated_at").desc_nulls_last(), col("ingest_date").desc_nulls_last()],
    )
    return publish_parquet(winners, layout.gold_uri("fact_orders"))


def _latest(frame: DataFrame, key: str, ordering: list) -> DataFrame:
    window = Window.partitionBy(key).orderBy(*ordering)
    return frame.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
