from datetime import UTC, date, datetime, timedelta

import pytest
from weather_etl.bronze import load_landing
from weather_etl.gold import (
    GOLD_TABLES,
    anomaly_annual,
    anomaly_monthly,
    build_gold,
    climatology_doy,
    daily_city,
    distribution_decade,
    extremes_annual,
    trend_city,
)
from weather_etl.landing import write_landing
from weather_etl.openmeteo import FetchResult
from weather_etl.silver import build_silver

pytestmark = pytest.mark.spark

DAILY = (
    "city_id string, date date, tmin double, tmean double, tmax double, precip_mm double, "
    "snow_cm double, gust_max double, rh_mean double, hours long, year int, month int, doy int"
)


def daily_rows(
    city_id,
    years,
    tmean=lambda d: 10.0,
    tmin=lambda d: 5.0,
    tmax=lambda d: 15.0,
    precip=lambda d: 0.0,
    skip=lambda d: False,
):
    rows = []
    for y in years:
        d = date(y, 1, 1)
        while d.year == y:
            if not skip(d):
                rows.append(
                    (
                        city_id,
                        d,
                        float(tmin(d)),
                        float(tmean(d)),
                        float(tmax(d)),
                        float(precip(d)),
                        0.0,
                        20.0,
                        60.0,
                        24,
                        y,
                        d.month,
                        d.timetuple().tm_yday,
                    )
                )
            d += timedelta(days=1)
    return rows


def daily_df(spark, rows):
    return spark.createDataFrame(rows, DAILY)


def test_daily_city_aggregates_and_keeps_dst_days(spark):
    schema = (
        "city_id string, date_local date, temperature_2m double, precipitation double, "
        "snowfall double, wind_gusts_10m double, relative_humidity_2m double"
    )
    rows = [("v", date(2023, 1, 1), float(h), 0.5, 0.0, 10.0 + h, 50.0) for h in range(24)]
    rows += [("v", date(2023, 3, 26), 1.0, 0.0, 0.0, 5.0, 50.0) for _ in range(23)]
    rows += [("v", date(2023, 3, 27), 1.0, 0.0, 0.0, 5.0, 50.0) for _ in range(10)]
    out = {r.date: r for r in daily_city(spark.createDataFrame(rows, schema)).collect()}
    assert set(out) == {date(2023, 1, 1), date(2023, 3, 26)}
    jan1 = out[date(2023, 1, 1)]
    assert (jan1.tmin, jan1.tmean, jan1.tmax, jan1.precip_mm, jan1.gust_max) == (
        0.0,
        11.5,
        23.0,
        12.0,
        33.0,
    )
    assert (jan1.year, jan1.month, jan1.doy, jan1.hours) == (2023, 1, 1, 24)


def test_climatology_percentiles_over_15_day_window(spark):
    daily = daily_df(spark, daily_rows("v", [2000, 2001], tmean=lambda d: d.timetuple().tm_yday))
    row = climatology_doy(daily, 2000, 2001).filter("city_id = 'v' AND doy = 100").first()
    assert (row.p10, row.p50, row.p90, row.n) == (94.0, 100.0, 106.0, 30)


def test_anomaly_monthly_uses_month_baseline_and_complete_months(spark):
    rows = daily_rows("v", [2000, 2001], tmean=lambda d: 0.0 if d.month == 1 else 10.0)
    rows += daily_rows(
        "v",
        [2002],
        tmean=lambda d: 3.0 if d.month == 1 else 10.0,
        skip=lambda d: d == date(2002, 3, 15),
    )
    out = {
        (r.year, r.month): r for r in anomaly_monthly(daily_df(spark, rows), 2000, 2001).collect()
    }
    assert out[(2002, 1)].anomaly == pytest.approx(3.0)
    assert out[(2002, 2)].anomaly == pytest.approx(0.0)
    assert (2002, 3) not in out


def test_anomaly_annual_complete_years_only(spark):
    rows = daily_rows("v", [2000, 2001]) + daily_rows("v", [2002], tmean=lambda d: 12.0)
    rows += daily_rows("v", [2003], skip=lambda d: d.month > 6)
    out = {r.year: r for r in anomaly_annual(daily_df(spark, rows), 2000, 2001).collect()}
    assert sorted(out) == [2000, 2001, 2002]
    assert out[2002].anomaly == pytest.approx(2.0)
    assert out[2000].anomaly == pytest.approx(0.0)


def test_trend_slope_intercept_r2(spark):
    annual = spark.createDataFrame(
        [("v", y, 0.0, 0.03 * y - 60.0, 0.0) for y in range(2000, 2010)],
        "city_id string, year int, tmean double, anomaly double, precip_mm double",
    )
    r = trend_city(annual).first()
    assert r.slope_c_per_decade == pytest.approx(0.3)
    assert r.intercept == pytest.approx(-60.0)
    assert r.r2 == pytest.approx(1.0)
    assert r.n_years == 10


def test_trend_needs_two_years(spark):
    annual = spark.createDataFrame(
        [("v", 2000, 0.0, 0.1, 0.0)],
        "city_id string, year int, tmean double, anomaly double, precip_mm double",
    )
    assert trend_city(annual).count() == 0


def test_extremes_against_baseline_thresholds(spark):
    rows = daily_rows(
        "v",
        [2000, 2001],
        tmax=lambda d: 20.0,
        tmin=lambda d: 0.0,
        precip=lambda d: 2.0 if d.timetuple().tm_yday % 3 == 0 else 0.0,
    )
    rows += daily_rows(
        "v",
        [2002],
        tmax=lambda d: 30.0 if d.day <= 7 and d.month == 7 else 20.0,
        tmin=lambda d: -10.0 if d.day <= 3 and d.month == 1 else 0.0,
        precip=lambda d: 10.0 if d.day <= 4 and d.month == 5 else 0.0,
    )
    out = {r.year: r for r in extremes_annual(daily_df(spark, rows), 2000, 2001).collect()}
    assert (out[2002].hot_days, out[2002].cold_nights, out[2002].heavy_rain_days) == (7, 3, 4)
    assert (out[2000].hot_days, out[2000].heavy_rain_days) == (0, 0)


def test_distribution_bins(spark):
    temps = {date(2003, 1, 1): 10.2, date(2003, 1, 2): 10.4, date(2003, 1, 3): 10.7}
    rows = [r for r in daily_rows("v", [2003], tmean=lambda d: temps.get(d, 0.0)) if r[1] in temps]
    out = {(r.decade, r.bin): r.days for r in distribution_decade(daily_df(spark, rows)).collect()}
    assert out == {(2000, 10.0): 2, (2000, 10.5): 1}


def test_build_gold_from_silver(spark, lake, cities, make_payload):
    for year in (2022, 2023):
        start = datetime(year, 1, 1)
        hours = 8760
        write_landing(
            lake,
            "vienna",
            year,
            FetchResult(
                f"fake://{year}", make_payload(start, hours), datetime(2026, 1, 1, tzinfo=UTC), 1.0
            ),
        )
    write_landing(
        lake,
        "vienna",
        2024,
        FetchResult(
            "fake://2024",
            make_payload(datetime(2024, 1, 1), 24),
            datetime(2026, 1, 1, tzinfo=UTC),
            1.0,
        ),
    )
    load_landing(spark, lake)
    build_silver(spark, lake, cities, "run-1")
    counts = build_gold(spark, lake, 2022, 2023)
    assert set(counts) == set(GOLD_TABLES)
    assert all(n > 0 for n in counts.values()), counts
    assert counts["trend_city"] == 1
    assert counts["anomaly_annual"] == 2
