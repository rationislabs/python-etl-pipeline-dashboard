import pandas as pd
import pytest
from weather_dash import figures, theme

CITIES = pd.DataFrame(
    [
        {"id": "singapore", "name": "Singapore", "lat": 1.29, "lon": 103.85, "climate": "tropical"},
        {"id": "vienna", "name": "Vienna", "lat": 48.21, "lon": 16.37, "climate": "temperate"},
    ]
)
ANNUAL = pd.DataFrame(
    [
        {"city_id": c, "year": y, "tmean": 10.0, "anomaly": a, "precip_mm": 1.0}
        for c, a in (("singapore", 0.4), ("vienna", -1.2))
        for y in (2000, 2001, 2002)
    ]
)
TRENDS = pd.DataFrame(
    [{"city_id": "vienna", "slope_c_per_decade": 0.3, "intercept": -60.0, "r2": 1.0, "n_years": 3}]
)


def types(fig):
    return [t.type for t in fig.data]


@pytest.mark.parametrize("mode", ["light", "dark"])
def test_empty_figure_shows_message(mode):
    fig = figures.empty_figure("Backfill in progress: 10% complete", mode)
    assert fig.layout.annotations[0].text == "Backfill in progress: 10% complete"
    assert fig.data == ()


def test_overview_map_symmetric_diverging_scale():
    fig = figures.overview_map(ANNUAL, CITIES, "light")
    assert types(fig) == ["scattergeo"]
    marker = fig.data[0].marker
    assert (marker.cmin, marker.cmax) == (-1.2, 1.2)
    assert "2002" in fig.layout.title.text


def test_overview_map_empty():
    fig = figures.overview_map(ANNUAL.iloc[0:0], CITIES, "light", message="nothing")
    assert fig.layout.annotations[0].text == "nothing"


def test_overview_kpis():
    kpis = figures.overview_kpis(ANNUAL, TRENDS, CITIES)
    labels = [k["label"] for k in kpis]
    assert labels == [
        "Warmest year (mean of all cities)",
        "Fastest warming",
        "Latest complete year",
    ]
    assert kpis[1]["value"] == "Vienna"
    assert figures.overview_kpis(ANNUAL.iloc[0:0], TRENDS, CITIES) == []


def test_warming_figure_lines_trends_and_stripes():
    fig = figures.warming_figure(
        ANNUAL, TRENDS, CITIES, ["singapore", "vienna"], (2000, 2002), "light", "1991–2020"
    )
    assert types(fig) == ["scatter", "scatter", "scatter", "heatmap"]
    heat = fig.data[-1]
    assert list(heat.y) == ["Singapore", "Vienna"] and heat.zmid == 0
    assert fig.data[0].line.color == theme.series_style("singapore", CITIES, "light")[0]
    assert "1991–2020" in fig.layout.title.text


def test_warming_figure_caps_at_eight_cities_and_handles_no_match():
    fig = figures.warming_figure(
        ANNUAL, TRENDS, CITIES, ["atlantis"], (2000, 2002), "light", "b", message="none"
    )
    assert fig.layout.annotations[0].text == "none"


def test_seasonality_band():
    clim = pd.DataFrame(
        [
            {"city_id": "vienna", "doy": d, "p10": 1.0, "p50": 2.0, "p90": 3.0, "n": 30}
            for d in range(1, 367)
        ]
    )
    daily = pd.DataFrame(
        [{"city_id": "vienna", "year": 2002, "doy": d, "tmean": 2.5} for d in range(1, 366)]
    )
    fig = figures.seasonality_band(clim, daily, CITIES, "vienna", 2002, "light")
    assert types(fig) == ["scatter"] * 4
    assert fig.data[1].fill == "tonexty"
    assert fig.data[3].name == "2002"


def test_monthly_heatmap():
    monthly = pd.DataFrame(
        [
            {"city_id": "vienna", "year": y, "month": m, "tmean": 1.0, "anomaly": 0.1}
            for y in (2000, 2001)
            for m in range(1, 13)
        ]
    )
    fig = figures.monthly_heatmap(monthly, CITIES, "vienna", "dark")
    assert types(fig) == ["heatmap"]
    assert len(fig.data[0].y) == 12 and list(fig.data[0].x) == [2000, 2001]


def test_extremes_three_panels_one_legend_entry_per_city():
    ext = pd.DataFrame(
        [
            {
                "city_id": c,
                "year": y,
                "hot_days": 1,
                "cold_nights": 2,
                "heavy_rain_days": 3,
                "gust_max": 50.0,
            }
            for c in ("singapore", "vienna")
            for y in (2000, 2001)
        ]
    )
    fig = figures.extremes_figure(ext, CITIES, ["singapore", "vienna"], (2000, 2001), "light")
    assert len(fig.data) == 6
    assert sum(bool(t.showlegend) for t in fig.data) == 2


def test_ridgeline_two_traces_per_decade():
    dist = pd.DataFrame(
        [
            {"city_id": "vienna", "decade": d, "bin": b / 2, "days": 5}
            for d in (1990, 2000, 2010)
            for b in range(10)
        ]
    )
    fig = figures.ridgeline(dist, CITIES, "vienna", "light")
    assert len(fig.data) == 6
    assert list(fig.layout.yaxis.ticktext) == ["1990s", "2000s", "2010s"]


def test_backfill_grid_codes_statuses():
    log = pd.DataFrame(
        [
            {"city_id": "vienna", "year": 2001, "status": "complete", "rows": 8760},
            {"city_id": "singapore", "year": 2000, "status": "partial", "rows": 10},
        ]
    )
    fig = figures.backfill_grid(log, CITIES, 2000, 2001, "light")
    z = fig.data[0].z
    assert z[0][0] == 1 and z[1][1] == 2 and z[1][0] == 0
    assert list(fig.data[0].colorbar.ticktext) == ["missing", "partial", "complete", "failed"]


def test_backfill_grid_tolerates_missing_rows():
    log = pd.DataFrame([{"city_id": "vienna", "year": 2000, "status": "failed", "rows": None}])
    fig = figures.backfill_grid(log, CITIES, 2000, 2001, "light")
    assert fig.data[0].z[1][0] == 3
    assert fig.data[0].text[1][0] == "failed"


def test_dq_history_groups_by_outcome():
    now = pd.Timestamp("2026-09-27", tz="UTC")
    dq = pd.DataFrame(
        [
            {
                "run_at": now,
                "table_name": "s",
                "check_name": "a",
                "severity": "hard",
                "passed": True,
                "observed": 0.0,
                "threshold": 0.0,
            },
            {
                "run_at": now,
                "table_name": "s",
                "check_name": "b",
                "severity": "soft",
                "passed": False,
                "observed": 1.0,
                "threshold": 0.0,
            },
            {
                "run_at": now,
                "table_name": "s",
                "check_name": "c",
                "severity": "hard",
                "passed": False,
                "observed": 2.0,
                "threshold": 0.0,
            },
        ]
    )
    fig = figures.dq_history(dq, "light")
    assert [t.name for t in fig.data] == ["passed", "failed (soft)", "failed (hard)"]
