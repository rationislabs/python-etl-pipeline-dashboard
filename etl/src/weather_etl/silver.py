"""bronze.weather_raw -> silver.weather_hourly: typed, deduplicated, MERGEd on (city_id, ts_utc)."""

from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from weather_etl.config import City
from weather_etl.dq import check_silver_batch, raise_on_hard_failures, write_results
from weather_etl.openmeteo import HOURLY_VARS
from weather_etl.paths import table_path

SILVER = ("silver", "weather_hourly")

HOURLY_SCHEMA = T.StructType(
    [
        T.StructField(
            "hourly",
            T.StructType(
                [T.StructField("time", T.ArrayType(T.StringType()))]
                + [T.StructField(v, T.ArrayType(T.DoubleType())) for v in HOURLY_VARS]
            ),
        )
    ]
)


def cities_frame(spark: SparkSession, cities: list[City]) -> DataFrame:
    return spark.createDataFrame([(c.id, c.tz) for c in cities], "city_id string, tz string")


def parse_bronze(bronze: DataFrame, cities_df: DataFrame) -> DataFrame:
    meta = ["city_id", "fetched_at", "loaded_at"]
    arrays = bronze.select(*meta, F.from_json("payload", HOURLY_SCHEMA).alias("j")).select(
        *meta,
        F.col("j.hourly.time").alias("time"),
        *[F.col(f"j.hourly.{v}").alias(v) for v in HOURLY_VARS],
    )
    rows = arrays.select(*meta, F.explode(F.arrays_zip("time", *HOURLY_VARS)).alias("r")).select(
        *meta,
        F.to_timestamp("r.time", "yyyy-MM-dd'T'HH:mm").alias("ts_utc"),
        *[F.col(f"r.{v}").alias(v) for v in HOURLY_VARS],
    )
    latest = Window.partitionBy("city_id", "ts_utc").orderBy(F.col("fetched_at").desc())
    rows = rows.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn")
    return (
        rows.join(F.broadcast(cities_df), "city_id", "left")
        .withColumn("date_local", F.to_date(F.from_utc_timestamp("ts_utc", F.col("tz"))))
        .select(
            "city_id",
            "ts_utc",
            "date_local",
            *HOURLY_VARS,
            F.col("fetched_at").alias("ingested_at"),
            F.col("loaded_at").alias("bronze_loaded_at"),
        )
    )


def silver_exists(spark: SparkSession, root: Path) -> bool:
    return DeltaTable.isDeltaTable(spark, table_path(root, *SILVER))


def pending_bronze(spark: SparkSession, root: Path) -> DataFrame | None:
    bronze_path = table_path(root, "bronze", "weather_raw")
    if not DeltaTable.isDeltaTable(spark, bronze_path):
        return None
    bronze = spark.read.format("delta").load(bronze_path)
    if silver_exists(spark, root):
        watermark = (
            spark.read.format("delta")
            .load(table_path(root, *SILVER))
            .agg(F.max("bronze_loaded_at"))
            .first()[0]
        )
        if watermark is not None:
            bronze = bronze.filter(F.col("loaded_at") > F.lit(watermark))
    return bronze


def merge_silver(spark: SparkSession, root: Path, batch: DataFrame) -> None:
    path = table_path(root, *SILVER)
    if not DeltaTable.isDeltaTable(spark, path):
        batch.write.format("delta").partitionBy("city_id").save(path)
        return
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(batch.alias("s"), "t.city_id = s.city_id AND t.ts_utc = s.ts_utc")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def build_silver(spark: SparkSession, root: Path, cities: list[City], run_id: str) -> int:
    pending = pending_bronze(spark, root)
    if pending is None:
        return 0
    batch = parse_bronze(pending, cities_frame(spark, cities)).cache()
    try:
        results = check_silver_batch(batch, {c.id for c in cities})
        if not results:
            return 0
        write_results(root, run_id, results)
        raise_on_hard_failures(results)
        merge_silver(spark, root, batch)
        return batch.count()
    finally:
        batch.unpersist()
