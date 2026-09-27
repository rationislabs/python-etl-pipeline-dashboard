from datetime import UTC, datetime

import pytest
from weather_etl.bronze import load_landing
from weather_etl.landing import landing_dir, write_landing
from weather_etl.openmeteo import FetchResult
from weather_etl.paths import table_path

pytestmark = pytest.mark.spark


def result(payload, day):
    return FetchResult("fake://u", payload, datetime(2026, 1, day, tzinfo=UTC), 1.0)


def test_load_landing_empty(spark, lake):
    assert load_landing(spark, lake) == 0


def test_load_landing_dedupes_by_payload_and_clears_files(spark, lake, make_payload):
    payload = make_payload(datetime(2023, 1, 1), 48)
    write_landing(lake, "vienna", 2023, result(payload, 1))
    write_landing(lake, "vienna", 2023, result(payload, 2))
    assert load_landing(spark, lake) == 1
    assert list(landing_dir(lake).rglob("*.json")) == []

    rows = spark.read.format("delta").load(table_path(lake, "bronze", "weather_raw")).collect()
    assert len(rows) == 1
    row = rows[0]
    assert row.city_id == "vienna" and row.year == 2023
    assert isinstance(row.fetched_at, datetime)
    assert row.loaded_at is not None

    write_landing(lake, "vienna", 2023, result(payload, 3))
    assert load_landing(spark, lake) == 0  # same payload already in bronze


def test_bronze_is_partitioned_by_city(spark, lake, make_payload):
    write_landing(lake, "vienna", 2023, result(make_payload(datetime(2023, 1, 1), 2), 1))
    write_landing(lake, "sydney", 2023, result(make_payload(datetime(2023, 1, 2), 2), 1))
    load_landing(spark, lake)
    parts = sorted(p.name for p in (lake / "bronze" / "weather_raw").glob("city_id=*"))
    assert parts == ["city_id=sydney", "city_id=vienna"]
