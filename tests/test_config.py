from pathlib import Path

import pytest

from cloudstream.config import Settings, validate_ingest_date
from cloudstream.schemas import SchemaError, require_columns
from cloudstream.warehouse.snowflake_loader import snowflake_connect_kwargs


def test_ingest_date_must_be_iso():
    assert validate_ingest_date("2026-03-01") == "2026-03-01"
    with pytest.raises(ValueError):
        validate_ingest_date("03/01/2026")


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "s3")
    monkeypatch.setenv("WAREHOUSE_BACKEND", "snowflake")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acme")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "from-env")
    monkeypatch.setenv("LAKE_ROOT", "/tmp/lake")
    settings = Settings.from_env()
    assert settings.storage_backend == "s3"
    assert settings.warehouse_backend == "snowflake"
    assert settings.lake_root == Path("/tmp/lake")
    assert settings.snowflake_password == "from-env"


def test_snowflake_kwargs_require_password():
    settings = Settings(warehouse_backend="snowflake", snowflake_account="acme")
    with pytest.raises(RuntimeError, match="SNOWFLAKE_PASSWORD"):
        snowflake_connect_kwargs(settings)


def test_header_contract():
    with pytest.raises(SchemaError, match="status"):
        require_columns(
            ["order_id", "customer_id"], ["order_id", "customer_id", "status"], "orders"
        )
