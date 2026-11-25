"""Land a day's raw CSV and JSON into bronze partitions."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from cloudstream.config import Settings, validate_ingest_date
from cloudstream.layout import DATASETS, LakeLayout, s3_client

logger = logging.getLogger(__name__)


def land_bronze(
    settings: Settings,
    ingest_date: str,
    source_dir: str | Path | None = None,
) -> list[str]:
    ingest_date = validate_ingest_date(ingest_date)
    source_root = Path(source_dir) if source_dir is not None else settings.source_dir
    day_dir = source_root / ingest_date
    layout = LakeLayout(settings)
    written: list[str] = []

    for dataset, filename in DATASETS.items():
        source = day_dir / filename
        if not source.is_file():
            raise FileNotFoundError(f"Missing daily extract {source}")
        if settings.storage_backend == "local":
            destination = layout.bronze_file(dataset, ingest_date)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            written.append(str(destination))
        else:
            key = layout.bronze_key(dataset, ingest_date)
            s3_client(settings).upload_file(str(source), settings.s3_bucket, key)
            written.append(key)

    logger.info("Landed %s bronze files for %s", len(written), ingest_date)
    return written
