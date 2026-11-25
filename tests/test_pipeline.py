import hashlib
from datetime import datetime
from decimal import Decimal

import duckdb
from pyspark.sql.functions import col
from tests.conftest import make_settings

from cloudstream.pipeline import run_day
from cloudstream.quality import QualityError, check_row_counts, check_uniqueness
from cloudstream.schemas import SchemaError
from cloudstream.silver import build_silver
from cloudstream.storage import publish_parquet


def test_missing_order_column_is_rejected(spark, tmp_path):
    settings = make_settings(tmp_path)
    day = settings.lake_root / "bronze" / "orders" / "ingest_date=2026-03-01"
    day.mkdir(parents=True)
    (day / "orders.csv").write_text("order_id,customer_id\nA1,C1\n", encoding="utf-8")
    try:
        build_silver(settings, "2026-03-01", spark)
    except SchemaError as exc:
        assert "status" in str(exc)
    else:
        raise AssertionError("expected SchemaError")


def test_two_days_build_an_idempotent_star_schema(spark, tmp_path):
    settings = make_settings(tmp_path)
    first = run_day(settings, "2026-03-01", spark=spark)
    assert first["silver"]["orders"] == {"input": 5, "quarantine": 2, "silver": 2}
    assert first["silver"]["customers"]["silver"] == 2
    assert first["silver"]["customers"]["quarantine"] == 2
    assert first["silver"]["products"]["silver"] == 2

    run_day(settings, "2026-03-02", spark=spark)
    _assert_star(spark, settings)

    run_day(settings, "2026-03-01", spark=spark)
    run_day(settings, "2026-03-02", spark=spark)
    _assert_star(spark, settings)

    connection = duckdb.connect(str(settings.duckdb_path))
    try:
        assert connection.execute("select count(*) from fact_orders").fetchone()[0] == 3
        assert connection.execute("select count(*) from dim_customers").fetchone()[0] == 4
        assert (
            connection.execute("select count(*) from dim_customers where is_current").fetchone()[0]
            == 3
        )
        status = connection.execute(
            "select status from fact_orders where order_id = 'A101'"
        ).fetchone()[0]
        assert status == "CANCELLED"
        current_city = connection.execute(
            "select city from dim_customers where customer_id = 'C001' and is_current"
        ).fetchone()[0]
        assert current_city == "Denver"
        lake_keys = {
            row.customer_sk
            for row in spark.read.parquet(
                str(settings.lake_root / "gold" / "dim_customers")
            ).collect()
            if row.customer_id == "C001"
        }
        warehouse_keys = {
            row[0]
            for row in connection.execute(
                "select customer_sk from dim_customers where customer_id = 'C001'"
            ).fetchall()
        }
        assert lake_keys == warehouse_keys
    finally:
        connection.close()


def test_uniqueness_check_fails_on_a_duplicate_fact(spark, tmp_path):
    settings = make_settings(tmp_path)
    run_day(settings, "2026-03-01", spark=spark)
    facts = spark.read.parquet(str(settings.lake_root / "gold" / "fact_orders"))
    publish_parquet(
        facts.unionByName(facts.limit(1)), str(settings.lake_root / "gold" / "fact_orders")
    )
    try:
        check_uniqueness(spark, settings, "2026-03-01")
    except QualityError as exc:
        assert any("order_id" in item for item in exc.failures)
    else:
        raise AssertionError("expected QualityError")


def test_row_count_check_fails_when_a_fact_is_missing(spark, tmp_path):
    settings = make_settings(tmp_path)
    run_day(settings, "2026-03-01", spark=spark)
    facts = spark.read.parquet(str(settings.lake_root / "gold" / "fact_orders"))
    shortened = facts.filter(col("order_id") != "A100")
    publish_parquet(shortened, str(settings.lake_root / "gold" / "fact_orders"))
    try:
        check_row_counts(spark, settings, "2026-03-01")
    except QualityError as exc:
        assert any("A100" in item or "silver orders" in item for item in exc.failures)
    else:
        raise AssertionError("expected QualityError")


def _assert_star(spark, settings):
    lake = settings.lake_root
    facts = {
        row.order_id: row
        for row in spark.read.parquet(str(lake / "gold" / "fact_orders")).collect()
    }
    customers = spark.read.parquet(str(lake / "gold" / "dim_customers")).collect()
    products = {
        row.product_id: row
        for row in spark.read.parquet(str(lake / "gold" / "dim_products")).collect()
    }
    opening = (
        spark.read.parquet(str(lake / "gold" / "dim_date"))
        .filter(col("date_sk") == 20260301)
        .collect()[0]
    )
    quarantine = spark.read.parquet(
        str(lake / "quarantine" / "orders" / "ingest_date=2026-03-01")
    ).collect()
    silver_customers = spark.read.parquet(
        str(lake / "silver" / "customers" / "ingest_date=2026-03-01")
    ).collect()

    assert set(facts) == {"A100", "A101", "A200"}
    assert facts["A100"].status == "PLACED"
    assert facts["A100"].currency == "USD"
    assert facts["A100"].extended_amount == Decimal("50.00")
    assert facts["A101"].status == "CANCELLED"
    assert facts["A200"].extended_amount == Decimal("42.50")
    assert facts["A100"].updated_at == datetime(2026, 3, 1, 14, 20)
    assert facts["A101"].updated_at == datetime(2026, 3, 2, 9, 0)

    riya = [row for row in customers if row.customer_id == "C001"]
    assert len(riya) == 2
    austin = next(row for row in riya if row.city == "Austin")
    denver = next(row for row in riya if row.city == "Denver")
    assert austin.is_current is False
    assert denver.is_current is True
    assert austin.effective_to == denver.effective_from
    assert facts["A100"].customer_sk == austin.customer_sk
    assert facts["A200"].customer_sk == denver.customer_sk
    assert sum(1 for row in customers if row.customer_id == "C002" and row.is_current) == 1
    assert sum(1 for row in customers if row.customer_id == "C003" and row.is_current) == 1
    assert all(
        sum(1 for row in customers if row.customer_id == cid and row.is_current) == 1
        for cid in ("C001", "C002", "C003")
    )

    assert products["P200"].list_price == Decimal("42.50")
    assert products["P100"].list_price == Decimal("25.00")
    assert products["P200"].product_sk == hashlib.sha256(b"P200").hexdigest()
    assert "P999" not in products

    assert opening.full_date.isoformat() == "2026-03-01"
    assert opening.day_name == "Sunday"
    assert opening.month_name == "March"
    assert opening.is_weekend is True

    reasons = {row.order_id: row.reject_reason for row in quarantine if row.order_id}
    assert reasons["A102"] == "invalid_quantity"
    assert reasons["A103"] == "corrupt_record"
    riya_silver = next(row for row in silver_customers if row.customer_id == "C001")
    assert riya_silver.full_name == "Riya Shah"
    assert riya_silver.email == "riya.shah@example.com"
