"""Runtime settings. Secrets come from the environment, never from source."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


def project_root() -> Path:
    override = os.environ.get("CLOUDSTREAM_HOME")
    if override:
        return Path(override).resolve()
    return Path(__file__).resolve().parents[2]


def validate_ingest_date(value: str) -> str:
    from datetime import datetime

    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"ingest_date must be YYYY-MM-DD, got {value!r}") from exc
    return parsed.date().isoformat()


@dataclass(frozen=True)
class Settings:
    storage_backend: str = "local"
    lake_root: Path = field(default_factory=lambda: Path("data/lake"))
    source_dir: Path = field(default_factory=lambda: Path("sample_data"))
    local_stage_dir: Path = field(default_factory=lambda: Path("data/stage"))
    s3_bucket: str = "cloudstream-lake"
    s3_prefix: str = ""
    aws_region: str = "us-east-1"
    aws_endpoint_url: str | None = None
    warehouse_backend: str = "duckdb"
    duckdb_path: Path = field(default_factory=lambda: Path("data/warehouse/cloudstream.duckdb"))
    snowflake_account: str | None = None
    snowflake_user: str | None = None
    snowflake_password: str | None = None
    snowflake_warehouse: str | None = None
    snowflake_database: str = "CLOUDSTREAM"
    snowflake_schema: str = "ANALYTICS"
    snowflake_role: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "lake_root", Path(self.lake_root))
        object.__setattr__(self, "source_dir", Path(self.source_dir))
        object.__setattr__(self, "local_stage_dir", Path(self.local_stage_dir))
        object.__setattr__(self, "duckdb_path", Path(self.duckdb_path))
        object.__setattr__(self, "storage_backend", self.storage_backend.lower())
        object.__setattr__(self, "warehouse_backend", self.warehouse_backend.lower())
        if self.storage_backend not in {"local", "s3"}:
            raise ValueError("STORAGE_BACKEND must be 'local' or 's3'")
        if self.warehouse_backend not in {"duckdb", "snowflake"}:
            raise ValueError("WAREHOUSE_BACKEND must be 'duckdb' or 'snowflake'")

    @classmethod
    def from_env(cls) -> Settings:
        load_dotenv(override=False)

        def env(name: str, default: str | None = None) -> str | None:
            value = os.environ.get(name)
            if value is None or value.strip() == "":
                return default
            return value.strip()

        return cls(
            storage_backend=env("STORAGE_BACKEND", "local") or "local",
            lake_root=Path(env("LAKE_ROOT", "data/lake") or "data/lake"),
            source_dir=Path(env("SOURCE_DIR", "sample_data") or "sample_data"),
            local_stage_dir=Path(env("LOCAL_STAGE_DIR", "data/stage") or "data/stage"),
            s3_bucket=env("S3_BUCKET", "cloudstream-lake") or "cloudstream-lake",
            s3_prefix=env("S3_PREFIX", "") or "",
            aws_region=env("AWS_REGION", "us-east-1") or "us-east-1",
            aws_endpoint_url=env("AWS_ENDPOINT_URL"),
            warehouse_backend=env("WAREHOUSE_BACKEND", "duckdb") or "duckdb",
            duckdb_path=Path(
                env("DUCKDB_PATH", "data/warehouse/cloudstream.duckdb")
                or "data/warehouse/cloudstream.duckdb"
            ),
            snowflake_account=env("SNOWFLAKE_ACCOUNT"),
            snowflake_user=env("SNOWFLAKE_USER"),
            snowflake_password=env("SNOWFLAKE_PASSWORD"),
            snowflake_warehouse=env("SNOWFLAKE_WAREHOUSE"),
            snowflake_database=env("SNOWFLAKE_DATABASE", "CLOUDSTREAM") or "CLOUDSTREAM",
            snowflake_schema=env("SNOWFLAKE_SCHEMA", "ANALYTICS") or "ANALYTICS",
            snowflake_role=env("SNOWFLAKE_ROLE"),
        )
