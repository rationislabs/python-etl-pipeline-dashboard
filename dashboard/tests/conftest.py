from datetime import UTC, date, datetime, timedelta

import pandas as pd
import pytest
import yaml
from deltalake import write_deltalake

CITIES = [
    {
        "id": "singapore",
        "name": "Singapore",
        "country": "SG",
        "lat": 1.29,
        "lon": 103.85,
        "tz": "Asia/Singapore",
        "climate": "tropical",
        "demo": True,
    },
    {
        "id": "vienna",
        "name": "Vienna",
        "country": "AT",
        "lat": 48.21,
        "lon": 16.37,
        "tz": "Europe/Vienna",
        "climate": "temperate",
        "demo": True,
    },
    {
        "id": "london",
        "name": "London",
        "country": "GB",
        "lat": 51.51,
        "lon": -0.13,
        "tz": "Europe/London",
        "climate": "temperate",
    },
]
YEARS = list(range(2000, 2006))


@pytest.fixture
def lake_env(tmp_path, monkeypatch):
    from weather_dash import data

    lake = tmp_path / "lakehouse"
    lake.mkdir()
    cfg = tmp_path / "cities.yaml"
    cfg.write_text(yaml.safe_dump({"cities": CITIES}))
    for k, v in {
        "LAKEHOUSE": str(lake),
        "CITIES_FILE": str(cfg),
        "FIRST_YEAR": "2000",
        "BASELINE_START": "2000",
        "BASELINE_END": "2003",
    }.items():
        monkeypatch.setenv(k, v)
    data.get_lakehouse.cache_clear()
    data.get_cities.cache_clear()
    yield lake
    data.get_lakehouse.cache_clear()
    data.get_cities.cache_clear()


def _write(lake, layer, name, df):
    write_deltalake(str(lake / layer / name), df, mode="overwrite")


@pytest.fixture
def gold_lake(lake_env):
    lake = lake_env
    ids = [c["id"] for c in CITIES]
    annual = pd.DataFrame(
        [
            {
                "city_id": c,
                "year": y,
                "tmean": 10.0 + i,
                "anomaly": 0.05 * (y - 2000) + i * 0.1,
                "precip_mm": 800.0,
            }
            for i, c in enumerate(ids)
            for y in YEARS
        ]
    )
    _write(lake, "gold", "anomaly_annual", annual)
    _write(
        lake,
        "gold",
        "trend_city",
        pd.DataFrame(
            [
                {
                    "city_id": c,
                    "slope_c_per_decade": 0.5,
                    "intercept": -100.0,
                    "r2": 0.9,
                    "n_years": 6,
                }
                for c in ids
            ]
        ),
    )
    days = [date(2004, 1, 1) + timedelta(days=d) for d in range(366)]
    _write(
        lake,
        "gold",
        "daily_city",
        pd.DataFrame(
            [
                {
                    "city_id": "vienna",
                    "date": d,
                    "tmin": 0.0,
                    "tmean": 10.0,
                    "tmax": 20.0,
                    "precip_mm": 1.0,
                    "snow_cm": 0.0,
                    "gust_max": 30.0,
                    "rh_mean": 60.0,
                    "hours": 24,
                    "year": d.year,
                    "month": d.month,
                    "doy": d.timetuple().tm_yday,
                }
                for d in days
            ]
        ),
    )
    _write(
        lake,
        "gold",
        "climatology_doy",
        pd.DataFrame(
            [
                {"city_id": "vienna", "doy": d, "p10": 5.0, "p50": 10.0, "p90": 15.0, "n": 60}
                for d in range(1, 367)
            ]
        ),
    )
    _write(
        lake,
        "gold",
        "anomaly_monthly",
        pd.DataFrame(
            [
                {"city_id": "vienna", "year": y, "month": m, "tmean": 10.0, "anomaly": 0.1 * m}
                for y in YEARS
                for m in range(1, 13)
            ]
        ),
    )
    _write(
        lake,
        "gold",
        "extremes_annual",
        pd.DataFrame(
            [
                {
                    "city_id": c,
                    "year": y,
                    "hot_days": y - 1995,
                    "cold_nights": 3,
                    "heavy_rain_days": 2,
                    "gust_max": 80.0,
                }
                for c in ids
                for y in YEARS
            ]
        ),
    )
    _write(
        lake,
        "gold",
        "distribution_decade",
        pd.DataFrame(
            [
                {"city_id": "vienna", "decade": dec, "bin": b / 2, "days": 10 + b}
                for dec in (1990, 2000)
                for b in range(0, 40)
            ]
        ),
    )
    now = datetime(2026, 9, 27, tzinfo=UTC)
    _write(
        lake,
        "ops",
        "ingest_log",
        pd.DataFrame(
            [
                {"city_id": c, "year": y, "status": "complete", "rows": 8760, "fetched_at": now}
                for c in ids
                for y in YEARS
            ]
        ),
    )
    _write(
        lake,
        "ops",
        "dq_results",
        pd.DataFrame(
            [
                {
                    "run_id": "r1",
                    "run_at": now,
                    "table_name": "silver.weather_hourly",
                    "check_name": "unique_city_ts",
                    "severity": "hard",
                    "passed": True,
                    "observed": 0.0,
                    "threshold": 0.0,
                },
                {
                    "run_id": "r1",
                    "run_at": now,
                    "table_name": "gold.trend_city",
                    "check_name": "row_count_gt_0",
                    "severity": "soft",
                    "passed": False,
                    "observed": 0.0,
                    "threshold": 0.0,
                },
            ]
        ),
    )
    _write(
        lake,
        "ops",
        "pipeline_stats",
        pd.DataFrame(
            [
                {
                    "layer": "silver",
                    "table_name": "weather_hourly",
                    "row_count": 1000,
                    "max_ts": now,
                    "updated_at": now,
                },
                {
                    "layer": "gold",
                    "table_name": "daily_city",
                    "row_count": 366,
                    "max_ts": pd.NaT,
                    "updated_at": now,
                },
            ]
        ),
    )
    return lake
