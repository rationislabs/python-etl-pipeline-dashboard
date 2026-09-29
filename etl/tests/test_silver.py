from datetime import UTC, date, datetime

import pytest
from weather_etl.bronze import load_landing
from weather_etl.landing import write_landing
from weather_etl.openmeteo import HOURLY_VARS, FetchResult
from weather_etl.paths import table_path
from weather_etl.silver import build_silver, cities_frame, merge_silver, parse_bronze

pytestmark = pytest.mark.spark


def land(lake, city_id, payload, day=1):
    write_landing(
        lake,
        city_id,
        2023,
        FetchResult("fake://u", payload, datetime(2026, 1, day, tzinfo=UTC), 1.0),
    )


def silver_rows(spark, lake):
    df = spark.read.format("delta").load(table_path(lake, "silver", "weather_hourly"))
    return sorted(tuple(r) for r in df.drop("ingested_at", "bronze_loaded_at").collect())


def test_parse_types_and_local_date(spark, lake, cities, make_payload):
    land(lake, "sydney", make_payload(datetime(2023, 1, 1), 48))
    load_landing(spark, lake)
    bronze = spark.read.format("delta").load(table_path(lake, "bronze", "weather_raw"))
    df = parse_bronze(bronze, cities_frame(spark, cities))
    assert df.count() == 48
    assert dict(df.dtypes)["temperature_2m"] == "double"
    row = df.filter("ts_utc = timestamp'2023-01-01 14:00:00'").first()
    assert row.date_local == date(2023, 1, 2)  # Sydney is UTC+11 in January


def test_unknown_city_is_kept_with_null_local_date(spark, lake, cities, make_payload):
    land(lake, "atlantis", make_payload(datetime(2023, 1, 1), 2))
    load_landing(spark, lake)
    bronze = spark.read.format("delta").load(table_path(lake, "bronze", "weather_raw"))
    rows = parse_bronze(bronze, cities_frame(spark, cities)).collect()
    assert len(rows) == 2 and rows[0].date_local is None


def test_latest_fetch_wins_within_batch(spark, lake, cities, make_payload):
    land(lake, "vienna", make_payload(datetime(2023, 1, 1), 24), day=1)
    land(lake, "vienna", make_payload(datetime(2023, 1, 1), 24, temp=lambda ts: 42.0), day=2)
    load_landing(spark, lake)
    assert build_silver(spark, lake, cities, "run-1") == 24
    temps = {
        r.temperature_2m
        for r in spark.read.format("delta")
        .load(table_path(lake, "silver", "weather_hourly"))
        .collect()
    }
    assert temps == {42.0}


def test_merge_is_idempotent(spark, lake, cities, make_payload):
    land(lake, "vienna", make_payload(datetime(2023, 1, 1), 24))
    load_landing(spark, lake)
    bronze = spark.read.format("delta").load(table_path(lake, "bronze", "weather_raw"))
    batch = parse_bronze(bronze, cities_frame(spark, cities))
    merge_silver(spark, lake, batch)
    first = silver_rows(spark, lake)
    merge_silver(spark, lake, batch)
    assert silver_rows(spark, lake) == first
    assert len(first) == 24


def test_revised_values_update_in_place(spark, lake, cities, make_payload):
    land(lake, "vienna", make_payload(datetime(2023, 1, 1), 24), day=1)
    load_landing(spark, lake)
    build_silver(spark, lake, cities, "run-1")
    land(lake, "vienna", make_payload(datetime(2023, 1, 1), 24, temp=lambda ts: -5.0), day=2)
    load_landing(spark, lake)
    assert build_silver(spark, lake, cities, "run-2") == 24
    rows = spark.read.format("delta").load(table_path(lake, "silver", "weather_hourly")).collect()
    assert len(rows) == 24 and {r.temperature_2m for r in rows} == {-5.0}


def test_watermark_skips_already_merged_bronze(spark, lake, cities, make_payload):
    land(lake, "vienna", make_payload(datetime(2023, 1, 1), 24))
    load_landing(spark, lake)
    assert build_silver(spark, lake, cities, "run-1") == 24
    assert build_silver(spark, lake, cities, "run-2") == 0


def test_watermark_processes_only_new_bronze(spark, lake, cities, make_payload):
    land(lake, "vienna", make_payload(datetime(2023, 1, 1), 24), day=1)
    load_landing(spark, lake)
    assert build_silver(spark, lake, cities, "run-1") == 24
    land(lake, "vienna", make_payload(datetime(2023, 1, 2), 24), day=2)
    load_landing(spark, lake)
    assert build_silver(spark, lake, cities, "run-2") == 24
    assert (
        spark.read.format("delta").load(table_path(lake, "silver", "weather_hourly")).count() == 48
    )


def test_no_bronze_means_nothing_to_do(spark, lake, cities):
    assert build_silver(spark, lake, cities, "run-1") == 0


def test_trailing_null_hours_are_dropped(spark, lake, cities, make_payload):
    nulls = {v: [1.0] * 42 + [None] * 6 for v in HOURLY_VARS}
    nulls["temperature_2m"] = [5.0] * 42 + [None] * 6
    land(lake, "vienna", make_payload(datetime(2023, 1, 1), 48, **nulls))
    load_landing(spark, lake)
    bronze = spark.read.format("delta").load(table_path(lake, "bronze", "weather_raw"))
    df = parse_bronze(bronze, cities_frame(spark, cities))
    assert df.count() == 42
    assert df.filter("ts_utc > timestamp'2023-01-02 17:00:00'").count() == 0
