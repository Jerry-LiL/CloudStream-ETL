"""Clean and deduplicate one bronze partition into silver.

The Spark reader is given an explicit schema. Rows that fail to parse, or that
break a business rule, are written to quarantine instead of being coerced into
the curated tables.
"""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    date_format,
    lit,
    lower,
    regexp_replace,
    row_number,
    sha2,
    to_date,
    trim,
    upper,
    when,
)
from pyspark.sql.window import Window

from cloudstream.config import Settings, validate_ingest_date
from cloudstream.layout import LakeLayout, s3_client
from cloudstream.schemas import (
    CUSTOMER_ATTRIBUTES,
    ORDER_FILE_COLUMNS,
    ORDER_STATUSES,
    PRODUCT_ATTRIBUTES,
    customer_raw_schema,
    order_raw_schema,
    product_raw_schema,
    require_columns,
)
from cloudstream.storage import publish_parquet

logger = logging.getLogger(__name__)

Counts = dict[str, int]
_EMAIL = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def build_silver(settings: Settings, ingest_date: str, spark: SparkSession) -> dict[str, Counts]:
    ingest_date = validate_ingest_date(ingest_date)
    layout = LakeLayout(settings)
    summary = {
        "orders": _publish_orders(spark, settings, layout, ingest_date),
        "customers": _publish_customers(spark, settings, layout, ingest_date),
        "products": _publish_products(spark, settings, layout, ingest_date),
    }
    logger.info("Silver %s %s", ingest_date, summary)
    return summary


def _publish_orders(spark, settings, layout: LakeLayout, ingest_date: str) -> Counts:
    _check_header(settings, layout, "orders", ingest_date, ORDER_FILE_COLUMNS)
    raw = _read_csv(spark, layout.bronze_uri("orders", ingest_date), order_raw_schema())
    frame = raw.select(
        _text("order_id").alias("order_id"),
        _text("customer_id").alias("customer_id"),
        _text("product_id").alias("product_id"),
        col("order_ts"),
        col("quantity"),
        col("unit_price"),
        coalesce(_text("currency", case="upper"), lit("USD")).alias("currency"),
        _text("status", case="upper").alias("status"),
        col("updated_at"),
        col("_corrupt"),
    )
    reason = (
        when(col("_corrupt").isNotNull(), lit("corrupt_record"))
        .when(
            col("order_id").isNull() | col("customer_id").isNull() | col("product_id").isNull(),
            lit("missing_key"),
        )
        .when(col("order_ts").isNull() | col("updated_at").isNull(), lit("invalid_timestamp"))
        .when(col("quantity").isNull() | (col("quantity") <= 0), lit("invalid_quantity"))
        .when(col("unit_price").isNull() | (col("unit_price") < 0), lit("invalid_price"))
        .when(~col("status").isin(*ORDER_STATUSES), lit("invalid_status"))
    )
    kept, rejected, counts, labeled = _split(frame, reason, raw)
    curated = kept.select(
        "order_id",
        "customer_id",
        "product_id",
        "order_ts",
        "quantity",
        "unit_price",
        (col("quantity").cast("decimal(14,2)") * col("unit_price"))
        .cast("decimal(14,2)")
        .alias("extended_amount"),
        "currency",
        "status",
        "updated_at",
        date_format(col("order_ts"), "yyyyMMdd").cast("int").alias("date_sk"),
        to_date(lit(ingest_date)).alias("ingest_date"),
    )
    curated = _dedupe(curated, "order_id")
    _write_pair(layout, "orders", ingest_date, curated, rejected)
    labeled.unpersist()
    counts["silver"] = curated.count()
    _assert_unique(curated, "order_id", "orders")
    return counts


def _publish_customers(spark, settings, layout: LakeLayout, ingest_date: str) -> Counts:
    raw = _read_json(spark, layout.bronze_uri("customers", ingest_date), customer_raw_schema())
    frame = raw.select(
        _text("customer_id").alias("customer_id"),
        _name("full_name").alias("full_name"),
        _text("email", case="lower").alias("email"),
        _text("city").alias("city"),
        _text("region").alias("region"),
        _text("segment", case="lower").alias("segment"),
        col("updated_at"),
        col("_corrupt"),
    )
    reason = (
        when(col("_corrupt").isNotNull(), lit("corrupt_record"))
        .when(col("customer_id").isNull(), lit("missing_key"))
        .when(col("updated_at").isNull(), lit("invalid_timestamp"))
        .when(col("email").isNull() | ~col("email").rlike(_EMAIL), lit("invalid_email"))
    )
    kept, rejected, counts, labeled = _split(frame, reason, raw)
    hashed = kept.withColumn("attribute_hash", _hash(CUSTOMER_ATTRIBUTES)).withColumn(
        "updated_at_iso", date_format(col("updated_at"), "yyyy-MM-dd'T'HH:mm:ss")
    )
    curated = _dedupe(hashed, "customer_id").select(
        sha2(
            concat_ws("|", col("customer_id"), col("updated_at_iso"), col("attribute_hash")),
            256,
        ).alias("customer_sk"),
        "customer_id",
        "full_name",
        "email",
        "city",
        "region",
        "segment",
        "attribute_hash",
        "updated_at",
        to_date(lit(ingest_date)).alias("ingest_date"),
    )
    _write_pair(layout, "customers", ingest_date, curated, rejected)
    labeled.unpersist()
    counts["silver"] = curated.count()
    _assert_unique(curated, "customer_id", "customers")
    return counts


def _publish_products(spark, settings, layout: LakeLayout, ingest_date: str) -> Counts:
    raw = _read_json(spark, layout.bronze_uri("products", ingest_date), product_raw_schema())
    frame = raw.select(
        _text("product_id").alias("product_id"),
        _name("product_name").alias("product_name"),
        _text("category", case="lower").alias("category"),
        _text("brand").alias("brand"),
        col("list_price"),
        col("updated_at"),
        col("_corrupt"),
    )
    reason = (
        when(col("_corrupt").isNotNull(), lit("corrupt_record"))
        .when(col("product_id").isNull() | col("product_name").isNull(), lit("missing_key"))
        .when(col("updated_at").isNull(), lit("invalid_timestamp"))
        .when(col("list_price").isNull() | (col("list_price") < 0), lit("invalid_price"))
    )
    kept, rejected, counts, labeled = _split(frame, reason, raw)
    hashed = kept.withColumn("attribute_hash", _hash(PRODUCT_ATTRIBUTES))
    curated = _dedupe(hashed, "product_id").select(
        sha2(col("product_id"), 256).alias("product_sk"),
        "product_id",
        "product_name",
        "category",
        "brand",
        "list_price",
        "attribute_hash",
        "updated_at",
        to_date(lit(ingest_date)).alias("ingest_date"),
    )
    _write_pair(layout, "products", ingest_date, curated, rejected)
    labeled.unpersist()
    counts["silver"] = curated.count()
    _assert_unique(curated, "product_id", "products")
    return counts


def _split(
    frame: DataFrame, reason: Column, raw: DataFrame
) -> tuple[DataFrame, DataFrame, Counts, DataFrame]:
    labeled = frame.withColumn("reject_reason", reason).cache()
    rejected = labeled.filter(col("reject_reason").isNotNull())
    kept = labeled.filter(col("reject_reason").isNull()).drop("reject_reason", "_corrupt")
    raw_count = raw.count()
    rejected_count = rejected.count()
    kept_count = kept.count()
    if kept_count + rejected_count != raw_count:
        raise RuntimeError(
            f"Row accounting failed: {kept_count} kept + {rejected_count} rejected != {raw_count}"
        )
    counts: Counts = {"input": raw_count, "quarantine": rejected_count, "silver": 0}
    return kept, rejected.drop("_corrupt"), counts, labeled


def _dedupe(frame: DataFrame, key: str) -> DataFrame:
    window = Window.partitionBy(key).orderBy(col("updated_at").desc_nulls_last(), col(key))
    return frame.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")


def _write_pair(
    layout: LakeLayout, dataset: str, ingest_date: str, curated: DataFrame, rejected: DataFrame
) -> None:
    curated_out = publish_parquet(curated, layout.silver_uri(dataset, ingest_date))
    rejected_out = publish_parquet(rejected, layout.quarantine_uri(dataset, ingest_date))
    curated_out.unpersist()
    rejected_out.unpersist()


def _assert_unique(frame: DataFrame, key: str, dataset: str) -> None:
    duplicate_keys = frame.groupBy(key).count().filter(col("count") > 1).count()
    if duplicate_keys:
        raise RuntimeError(f"{dataset} silver still has duplicate {key}")


def _check_header(
    settings, layout: LakeLayout, dataset: str, ingest_date: str, required: list[str]
) -> None:
    if settings.storage_backend == "local":
        first = layout.bronze_file(dataset, ingest_date).read_text(encoding="utf-8").splitlines()[0]
    else:
        body = s3_client(settings).get_object(
            Bucket=settings.s3_bucket,
            Key=layout.bronze_key(dataset, ingest_date),
            Range="bytes=0-16383",
        )
        first = body["Body"].read().decode("utf-8").splitlines()[0]
    actual = [part.strip() for part in first.split(",")]
    require_columns(actual, required, f"bronze {dataset} {ingest_date}")


def _read_csv(spark: SparkSession, path: str, schema) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("enforceSchema", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
        .schema(schema)
        .csv(path)
    )


def _read_json(spark: SparkSession, path: str, schema) -> DataFrame:
    return (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
        .schema(schema)
        .json(path)
    )


def _text(name: str, *, case: str | None = None) -> Column:
    value = trim(col(name))
    value = when(value == "", lit(None)).otherwise(value)
    if case == "upper":
        return upper(value)
    if case == "lower":
        return lower(value)
    return value


def _name(name: str) -> Column:
    collapsed = regexp_replace(trim(col(name)), r"\s+", " ")
    return when(collapsed == "", lit(None)).otherwise(collapsed)


def _hash(columns: list[str]) -> Column:
    parts = [coalesce(col(name).cast("string"), lit("")) for name in columns]
    return sha2(concat_ws("||", *parts), 256)
