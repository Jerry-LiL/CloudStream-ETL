"""Local Spark session used by the silver and gold steps."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from cloudstream.config import Settings

# Matches the Hadoop build shipped with PySpark 3.5.
_S3A_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"


def build_spark(settings: Settings, app_name: str = "cloudstream") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config(
            "spark.driver.extraJavaOptions",
            "-Duser.timezone=UTC -Duser.language=en -Duser.country=US "
            "-Dderby.system.home=/tmp/cloudstream-derby",
        )
        .config("spark.sql.warehouse.dir", "/tmp/cloudstream-spark-warehouse")
    )
    if settings.storage_backend == "s3":
        builder = (
            builder.config("spark.jars.packages", _S3A_PACKAGES)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        )
        if settings.aws_endpoint_url:
            builder = (
                builder.config("spark.hadoop.fs.s3a.endpoint", settings.aws_endpoint_url)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config(
                    "spark.hadoop.fs.s3a.connection.ssl.enabled",
                    "false" if settings.aws_endpoint_url.startswith("http://") else "true",
                )
            )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
