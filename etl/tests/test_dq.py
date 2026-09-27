from datetime import UTC, date, datetime

import pytest
from deltalake import DeltaTable
from weather_etl.bronze import load_landing
from weather_etl.dq import (
    DQError,
    check_gold,
    check_silver_batch,
    check_silver_table,
    raise_on_hard_failures,
    write_results,
)
from weather_etl.landing import write_landing
from weather_etl.openmeteo import FetchResult
from weather_etl.paths import table_path
from weather_etl.silver import build_silver, cities_frame, parse_bronze, silver_exists

pytestmark = pytest.mark.spark


def batch_from(spark, lake, cities, city_id, payload):
    write_landing(
        lake, city_id, 2023, FetchResult("fake://u", payload, datetime(2026, 1, 1, tzinfo=UTC), 1.0)
    )
    load_landing(spark, lake)
    bronze = spark.read.format("delta").load(table_path(lake, "bronze", "weather_raw"))
    return parse_bronze(bronze, cities_frame(spark, cities))


def by_name(results):
    return {r.check_name: r for r in results}


def test_clean_batch_passes_everything(spark, lake, cities, make_payload):
    res = check_silver_batch(
        batch_from(spark, lake, cities, "vienna", make_payload(datetime(2023, 1, 1), 24)),
        {"vienna", "sydney"},
    )
    assert res and all(r.passed for r in res)
    assert {r.severity for r in res} == {"hard", "soft"}


def test_duplicate_keys_fail_hard(spark, lake, cities, make_payload):
    df = batch_from(spark, lake, cities, "vienna", make_payload(datetime(2023, 1, 1), 24))
    res = by_name(check_silver_batch(df.unionByName(df.limit(3)), {"vienna"}))
    assert not res["unique_city_ts"].passed and res["unique_city_ts"].observed == 3


def test_out_of_range_temperature_fails_hard(spark, lake, cities, make_payload):
    df = batch_from(
        spark, lake, cities, "vienna", make_payload(datetime(2023, 1, 1), 24, temp=lambda ts: 70.0)
    )
    res = by_name(check_silver_batch(df, {"vienna"}))
    assert not res["range_temperature_2m"].passed and res["range_temperature_2m"].severity == "hard"


def test_unknown_city_fails_hard(spark, lake, cities, make_payload):
    df = batch_from(spark, lake, cities, "atlantis", make_payload(datetime(2023, 1, 1), 4))
    assert not by_name(check_silver_batch(df, {"vienna"}))["city_in_config"].passed


def test_null_share_is_soft(spark, lake, cities, make_payload):
    payload = make_payload(datetime(2023, 1, 1), 4, relative_humidity_2m=[None, None, 60.0, 60.0])
    res = by_name(
        check_silver_batch(batch_from(spark, lake, cities, "vienna", payload), {"vienna"})
    )
    r = res["null_share_relative_humidity_2m"]
    assert (r.passed, r.severity, r.observed) == (False, "soft", 0.5)


def test_empty_batch_returns_no_results(spark, lake, cities, make_payload):
    df = batch_from(spark, lake, cities, "vienna", make_payload(datetime(2023, 1, 1), 4))
    assert check_silver_batch(df.limit(0), {"vienna"}) == []


def test_hard_failure_blocks_silver_and_is_recorded(spark, lake, cities, make_payload):
    write_landing(
        lake,
        "vienna",
        2023,
        FetchResult(
            "fake://u",
            make_payload(datetime(2023, 1, 1), 24, temp=lambda ts: 70.0),
            datetime(2026, 1, 1, tzinfo=UTC),
            1.0,
        ),
    )
    load_landing(spark, lake)
    with pytest.raises(DQError, match="range_temperature_2m"):
        build_silver(spark, lake, cities, "run-bad")
    assert not silver_exists(spark, lake)
    dq = DeltaTable(table_path(lake, "ops", "dq_results")).to_pandas()
    failed = dq[(dq.run_id == "run-bad") & (~dq.passed)]
    assert "range_temperature_2m" in set(failed.check_name)


def test_silver_table_checks(spark, lake, cities, make_payload):
    write_landing(
        lake,
        "vienna",
        2023,
        FetchResult(
            "fake://u",
            make_payload(datetime(2023, 1, 1), 100),
            datetime(2026, 1, 1, tzinfo=UTC),
            1.0,
        ),
    )
    load_landing(spark, lake)
    build_silver(spark, lake, cities, "run-1")
    res = by_name(check_silver_table(spark, lake, date(2024, 1, 10), {"vienna", "sydney"}))
    assert (res["complete_years"].passed, res["complete_years"].observed) == (False, 1.0)
    assert (res["fresh_within_10_days"].passed, res["fresh_within_10_days"].observed) == (
        False,
        2.0,
    )
    assert {r.severity for r in res.values()} == {"soft"}


def test_check_gold_severity():
    res = {
        r.table_name: r for r in check_gold({"daily_city": 0, "trend_city": 0, "anomaly_annual": 5})
    }
    assert (res["gold.daily_city"].severity, res["gold.daily_city"].passed) == ("hard", False)
    assert (res["gold.trend_city"].severity, res["gold.trend_city"].passed) == ("soft", False)
    assert res["gold.anomaly_annual"].passed


def test_raise_on_hard_failures_ignores_soft():
    raise_on_hard_failures(check_gold({"trend_city": 0}))
    with pytest.raises(DQError):
        raise_on_hard_failures(check_gold({"daily_city": 0}))


def test_write_results_appends(lake):
    write_results(lake, "r1", check_gold({"daily_city": 1}))
    write_results(lake, "r2", check_gold({"daily_city": 2}))
    write_results(lake, "r3", [])
    assert sorted(DeltaTable(table_path(lake, "ops", "dq_results")).to_pandas().run_id) == [
        "r1",
        "r2",
    ]
