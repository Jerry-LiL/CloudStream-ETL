from pathlib import Path

from cloudstream.warehouse.sql import read_script

ROOT = Path(__file__).resolve().parents[1]


def test_merge_scripts_cover_the_star_schema():
    text = "\n".join(
        path.read_text(encoding="utf-8") for path in (ROOT / "sql" / "merge").glob("*.sql")
    )
    lowered = text.lower()
    for table in ("dim_date", "dim_products", "dim_customers", "fact_orders"):
        assert f"merge into {table}" in lowered
    assert "is_current" in lowered
    assert "effective_from" in lowered
    assert "effective_to" in lowered
    assert "when matched" in lowered
    assert "when not matched" in lowered
    customer_statements = read_script("merge", "04_merge_dim_customers.sql")
    assert len(customer_statements) == 2


def test_snowflake_ddl_creates_the_same_tables():
    ddl = (ROOT / "sql" / "snowflake" / "01_ddl.sql").read_text(encoding="utf-8").lower()
    for table in ("dim_date", "dim_products", "dim_customers", "fact_orders"):
        assert f"create table if not exists {table}" in ddl
    staging = (ROOT / "sql" / "snowflake" / "02_staging.sql").read_text(encoding="utf-8").lower()
    assert "temporary table staging_customers" in staging
    assert "temporary table staging_orders" in staging


def test_dag_keeps_credentials_out_of_source():
    source = (ROOT / "dags" / "cloudstream_daily.py").read_text(encoding="utf-8")
    for task_id in (
        "land_bronze",
        "bronze_to_silver",
        "silver_to_gold",
        "dq_row_counts",
        "dq_uniqueness",
        "load_snowflake",
    ):
        assert task_id in source
    assert ">>" in source
    assert "password =" not in source.lower()
    assert "AKIA" not in source
    assert "Settings.from_env()" in source
