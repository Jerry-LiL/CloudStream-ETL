"""Lake paths. Local disk and s3:// share the same partition layout."""

from __future__ import annotations

import shutil
from pathlib import Path

from cloudstream.config import Settings

DATASETS = {
    "orders": "orders.csv",
    "customers": "customers.json",
    "products": "products.json",
}


class LakeLayout:
    def __init__(self, settings: Settings):
        self.settings = settings

    def local(self, *parts: str) -> Path:
        return self.settings.lake_root.joinpath(*parts)

    def s3_key(self, *parts: str) -> str:
        prefix = self.settings.s3_prefix.strip("/")
        relative = "/".join(parts)
        if prefix:
            return f"{prefix}/{relative}"
        return relative

    def uri(self, *parts: str) -> str:
        """Path Spark can read: a filesystem path, or an s3a URI."""
        if self.settings.storage_backend == "local":
            return str(self.local(*parts))
        return f"s3a://{self.settings.s3_bucket}/{self.s3_key(*parts)}"

    def bronze_file(self, dataset: str, ingest_date: str) -> Path:
        filename = DATASETS[dataset]
        return self.local("bronze", dataset, f"ingest_date={ingest_date}", filename)

    def bronze_key(self, dataset: str, ingest_date: str) -> str:
        filename = DATASETS[dataset]
        return self.s3_key("bronze", dataset, f"ingest_date={ingest_date}", filename)

    def bronze_uri(self, dataset: str, ingest_date: str) -> str:
        filename = DATASETS[dataset]
        return self.uri("bronze", dataset, f"ingest_date={ingest_date}", filename)

    def silver_dir(self, dataset: str, ingest_date: str) -> Path:
        return self.local("silver", dataset, f"ingest_date={ingest_date}")

    def silver_uri(self, dataset: str, ingest_date: str) -> str:
        return self.uri("silver", dataset, f"ingest_date={ingest_date}")

    def quarantine_uri(self, dataset: str, ingest_date: str) -> str:
        return self.uri("quarantine", dataset, f"ingest_date={ingest_date}")

    def gold_dir(self, dataset: str) -> Path:
        return self.local("gold", dataset)

    def gold_uri(self, dataset: str) -> str:
        return self.uri("gold", dataset)

    def materialize(self, *parts: str) -> Path:
        """Return a local directory of parquet for the warehouse loader.

        S3 objects are downloaded into the stage directory. That mirrors a
        Snowflake COPY that pulls a partition out of the lake before MERGE.
        """
        local_path = self.local(*parts)
        if self.settings.storage_backend == "local":
            return local_path
        destination = self.settings.local_stage_dir.joinpath(*parts)
        if destination.exists():
            shutil.rmtree(destination)
        destination.mkdir(parents=True, exist_ok=True)
        download_prefix(self.settings, self.s3_key(*parts).rstrip("/") + "/", destination)
        return destination


def s3_client(settings: Settings):
    import boto3

    kwargs: dict = {"region_name": settings.aws_region}
    if settings.aws_endpoint_url:
        kwargs["endpoint_url"] = settings.aws_endpoint_url
    return boto3.client("s3", **kwargs)


def download_prefix(settings: Settings, key_prefix: str, destination: Path) -> int:
    client = s3_client(settings)
    paginator = client.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            relative = key[len(key_prefix) :].lstrip("/")
            target = destination / relative if relative else destination / Path(key).name
            target.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(settings.s3_bucket, key, str(target))
            count += 1
    return count
