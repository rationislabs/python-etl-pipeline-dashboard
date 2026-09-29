"""SparkSession with Delta Lake; the same code runs local[2] in tests and on the cluster."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession


def get_session(app_name: str = "weather-etl", master: str | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.master(master or os.environ.get("SPARK_MASTER", "local[2]"))
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        # VACUUM lists files with this many tasks (default 10,000: ~15 s per tiny table)
        .config(
            "spark.sql.sources.parallelPartitionDiscovery.parallelism",
            os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"),
        )
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "768m"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if driver_host := os.environ.get("SPARK_DRIVER_HOST"):
        builder = builder.config("spark.driver.host", driver_host).config(
            "spark.driver.bindAddress", "0.0.0.0"
        )
    if ivy := os.environ.get("SPARK_IVY_DIR"):
        builder = builder.config("spark.jars.ivy", ivy)
    return configure_spark_with_delta_pip(builder).getOrCreate()


def vacuum(spark: SparkSession, path: str, retention_hours: float | None = None) -> None:
    """Delete files no longer referenced by the Delta table (default retention: 7 days)."""
    DeltaTable.forPath(spark, path).vacuum(retention_hours)
