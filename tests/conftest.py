import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-21-openjdk-amd64")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from cloudstream.config import Settings  # noqa: E402
from cloudstream.spark_session import build_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = build_spark(Settings(lake_root=Path("/tmp/cloudstream-spark-meta")))
    yield session
    session.stop()


def make_settings(tmp_path: Path, **overrides) -> Settings:
    values = {
        "lake_root": tmp_path / "lake",
        "source_dir": ROOT / "sample_data",
        "local_stage_dir": tmp_path / "stage",
        "duckdb_path": tmp_path / "warehouse" / "cloudstream.duckdb",
        "storage_backend": "local",
        "warehouse_backend": "duckdb",
    }
    values.update(overrides)
    return Settings(**values)
