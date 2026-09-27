"""Landing JSON files -> bronze.weather_raw (append-only, raw payload kept)."""

from __future__ import annotations

import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from weather_etl.landing import landing_dir
from weather_etl.paths import table_path

LANDING_SCHEMA = (
    "city_id string, year int, request_url string, fetched_at string, "
    "payload string, payload_sha256 string"
)


def load_landing(spark: SparkSession, root: Path) -> int:
    files = sorted(str(p) for p in landing_dir(root).rglob("*.json"))
    if not files:
        return 0
    batch = (
        spark.read.schema(LANDING_SCHEMA)
        .json(files)
        .withColumn("fetched_at", F.to_timestamp("fetched_at"))
        .dropDuplicates(["payload_sha256"])
    )
    path = table_path(root, "bronze", "weather_raw")
    if DeltaTable.isDeltaTable(spark, path):
        seen = spark.read.format("delta").load(path).select("payload_sha256")
        batch = batch.join(seen, "payload_sha256", "left_anti")
    batch = batch.withColumn("loaded_at", F.current_timestamp()).cache()
    try:
        n = batch.count()
        if n:
            batch.write.format("delta").mode("append").partitionBy("city_id").save(path)
    finally:
        batch.unpersist()
    for f in files:
        os.remove(f)
    return n
