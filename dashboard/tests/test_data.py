from datetime import date

import pandas as pd
from deltalake import write_deltalake
from weather_dash import data


def test_missing_table_is_empty(lake_env):
    assert data.Lakehouse(lake_env).table("gold", "nope").empty


def test_table_cache_follows_delta_version(lake_env):
    path = str(lake_env / "gold" / "t")
    write_deltalake(path, pd.DataFrame({"x": [1]}))
    lake = data.Lakehouse(lake_env)
    first = lake.table("gold", "t")
    assert lake.table("gold", "t") is first
    write_deltalake(path, pd.DataFrame({"x": [2]}), mode="append")
    assert sorted(lake.table("gold", "t").x) == [1, 2]


def test_get_cities_reads_env(lake_env):
    cities = data.get_cities()
    assert list(cities.id) == ["singapore", "vienna", "london"]
    assert {"lat", "lon", "climate", "name"} <= set(cities.columns)


def test_load_cities_missing_file_is_empty(tmp_path):
    assert data.load_cities(tmp_path / "missing.yaml").empty


def test_first_year_and_baseline_label():
    assert data.first_year({}) == 1980
    assert data.first_year({"DEMO": "1"}) == 1991
    assert data.first_year({"DEMO": "1", "BASELINE_START": "2000"}) == 2000
    assert data.first_year({"FIRST_YEAR": "2024"}) == 2024
    assert data.first_year({"FIRST_YEAR": ""}) == 1980
    assert data.baseline_label({}) == "1991–2020"


def test_last_complete_year():
    assert data.last_complete_year(date(2026, 9, 27)) == 2025


def test_backfill_progress():
    log = pd.DataFrame({"status": ["complete", "complete", "partial"]})
    assert data.backfill_progress(log, n_cities=2, first=2000, last_complete=2001) == 50.0
    assert data.backfill_progress(log, n_cities=0, first=2000, last_complete=2001) == 100.0


def test_empty_message(lake_env):
    lake, cities = data.get_lakehouse(), data.get_cities()
    assert data.empty_message(lake, cities).startswith("No data yet")
    write_deltalake(
        str(lake_env / "ops" / "ingest_log"),
        pd.DataFrame(
            {"city_id": ["vienna"], "year": [2000], "status": ["complete"], "rows": [8760]}
        ),
    )
    assert data.empty_message(data.Lakehouse(lake_env), cities).startswith("Backfill in progress:")


def test_get_cities_honours_demo(lake_env, monkeypatch):
    monkeypatch.setenv("DEMO", "1")
    data.get_cities.cache_clear()
    assert list(data.get_cities().id) == ["singapore", "vienna"]
