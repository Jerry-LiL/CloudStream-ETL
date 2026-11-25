import filecmp

import boto3
from moto import mock_aws
from tests.conftest import ROOT, make_settings

from cloudstream.landing import land_bronze


def test_local_landing_copies_daily_files(tmp_path):
    settings = make_settings(tmp_path)
    written = land_bronze(settings, "2026-03-01")
    assert len(written) == 3
    bronze = settings.lake_root / "bronze"
    orders = bronze / "orders" / "ingest_date=2026-03-01" / "orders.csv"
    customers = bronze / "customers" / "ingest_date=2026-03-01" / "customers.json"
    products = bronze / "products" / "ingest_date=2026-03-01" / "products.json"
    assert filecmp.cmp(orders, ROOT / "sample_data" / "2026-03-01" / "orders.csv", shallow=False)
    assert filecmp.cmp(
        customers, ROOT / "sample_data" / "2026-03-01" / "customers.json", shallow=False
    )
    assert filecmp.cmp(
        products, ROOT / "sample_data" / "2026-03-01" / "products.json", shallow=False
    )
    land_bronze(settings, "2026-03-01")
    assert orders.is_file()


def test_s3_landing_writes_partition_keys(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    settings = make_settings(
        tmp_path,
        storage_backend="s3",
        s3_bucket="cloudstream-lake",
        s3_prefix="lake",
        aws_region="us-east-1",
    )
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="cloudstream-lake")
        written = land_bronze(settings, "2026-03-02")
        keys = set(written)
        assert keys == {
            "lake/bronze/orders/ingest_date=2026-03-02/orders.csv",
            "lake/bronze/customers/ingest_date=2026-03-02/customers.json",
            "lake/bronze/products/ingest_date=2026-03-02/products.json",
        }
        body = boto3.client("s3", region_name="us-east-1").get_object(
            Bucket="cloudstream-lake",
            Key="lake/bronze/orders/ingest_date=2026-03-02/orders.csv",
        )
        assert b"A200" in body["Body"].read()


def test_missing_extract_raises(tmp_path):
    settings = make_settings(tmp_path, source_dir=tmp_path / "empty")
    try:
        land_bronze(settings, "2026-03-01")
    except FileNotFoundError as exc:
        assert "orders.csv" in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")
