from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
from tests.conftest import make_settings

from cloudstream.quality import QualityError
from cloudstream.warehouse.snowflake_loader import SnowflakeLoader


class FakeCursor:
    def __init__(self):
        self.statements = []
        self._last = ""

    def execute(self, sql):
        self.statements.append(sql)
        self._last = sql
        return self

    def fetchall(self):
        if "check_name" in self._last:
            return [
                ("duplicate_order_id", 0),
                ("multiple_current_customer", 0),
                ("duplicate_customer_sk", 0),
                ("duplicate_product_id", 0),
                ("duplicate_date_sk", 0),
                ("staging_orders_missing_from_fact", 0),
                ("fact_rows_missing_keys", 0),
                ("current_customer_missing", 0),
            ]
        return []

    def fetchone(self):
        return (0,)

    def close(self):
        return None


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def autocommit(self, _enabled):
        return None

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def test_loader_runs_merge_scripts_and_stages_frames(tmp_path, monkeypatch):
    settings = make_settings(
        tmp_path,
        warehouse_backend="snowflake",
        snowflake_account="acme",
        snowflake_user="loader",
        snowflake_password="secret",
        snowflake_warehouse="compute_wh",
    )
    customer_dir = settings.lake_root / "silver" / "customers" / "ingest_date=2026-03-01"
    customer_dir.mkdir(parents=True)
    table = pa.table(
        {
            "customer_sk": ["sk-1"],
            "customer_id": ["C001"],
            "full_name": ["Riya Shah"],
            "email": ["riya.shah@example.com"],
            "city": ["Austin"],
            "region": ["TX"],
            "segment": ["consumer"],
            "attribute_hash": ["abc"],
            "updated_at": [datetime(2026, 3, 1, 8, 0, 0)],
        }
    )
    pq.write_table(table, customer_dir / "part.parquet")

    written = []

    def fake_write(connection, frame, table_name, **kwargs):
        written.append((table_name, kwargs["quote_identifiers"], list(frame.columns)))
        return (True, 1, len(frame), None)

    monkeypatch.setattr("snowflake.connector.pandas_tools.write_pandas", fake_write)
    connection = FakeConnection()
    SnowflakeLoader(settings, connect=lambda: connection).load("2026-03-01")

    script = "\n".join(connection.cursor_obj.statements).lower()
    assert "merge into dim_customers" in script
    assert "merge into dim_products" in script
    assert "merge into dim_date" in script
    assert "merge into fact_orders" in script
    assert written == [
        (
            "STAGING_CUSTOMERS",
            False,
            [
                "customer_sk",
                "customer_id",
                "full_name",
                "email",
                "city",
                "region",
                "segment",
                "attribute_hash",
                "updated_at",
            ],
        )
    ]
    assert connection.committed
    assert connection.closed
    assert "secret" not in script


def test_loader_rejects_a_lake_count_mismatch(tmp_path, monkeypatch):
    settings = make_settings(
        tmp_path,
        warehouse_backend="snowflake",
        snowflake_account="acme",
        snowflake_user="loader",
        snowflake_password="secret",
        snowflake_warehouse="compute_wh",
    )
    fact_dir = settings.lake_root / "gold" / "fact_orders"
    fact_dir.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "order_id": ["A100"],
                "customer_sk": ["sk"],
                "extended_amount": [Decimal("50.00")],
            }
        ),
        fact_dir / "part.parquet",
    )
    monkeypatch.setattr(
        "snowflake.connector.pandas_tools.write_pandas",
        lambda *args, **kwargs: (True, 1, 1, None),
    )
    connection = FakeConnection()
    try:
        SnowflakeLoader(settings, connect=lambda: connection).load("2026-03-01")
    except QualityError as exc:
        assert any("fact_orders" in item for item in exc.failures)
    else:
        raise AssertionError("expected QualityError")
    assert connection.rolled_back
    assert not connection.committed
