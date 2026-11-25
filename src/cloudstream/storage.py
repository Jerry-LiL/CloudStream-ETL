"""Parquet publish and read helpers."""

from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def empty_frame(spark: SparkSession, schema: StructType) -> DataFrame:
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)


def read_parquet(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    location = Path(path)
    if location.exists() and any(location.rglob("*.parquet")):
        return spark.read.schema(schema).parquet(path)
    # s3a URIs are not local paths. Let Spark try, and fall back if the path is missing.
    if path.startswith("s3a://"):
        try:
            return spark.read.schema(schema).parquet(path)
        except Exception:
            return empty_frame(spark, schema)
    return empty_frame(spark, schema)


def publish_parquet(frame: DataFrame, path: str) -> DataFrame:
    """Materialize a frame and swap it into place.

    Gold tables are read and then rewritten. Caching first means a failed
    writer cannot recompute the plan against the directory it just replaced.
    """
    frame = frame.cache()
    frame.count()
    if path.startswith("s3a://"):
        frame.write.mode("overwrite").parquet(path)
        return frame
    target = Path(path)
    temporary = target.parent / f".tmp-{target.name}-{uuid4().hex[:8]}"
    if temporary.exists():
        shutil.rmtree(temporary)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.write.mode("overwrite").parquet(str(temporary))
    if target.exists():
        shutil.rmtree(target)
    temporary.rename(target)
    return frame
