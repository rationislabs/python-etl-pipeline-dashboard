import importlib

import pytest


@pytest.fixture
def app_module(lake_env):
    return importlib.import_module("weather_dash.app")


def pages():
    from weather_dash.pages import extremes, overview, pipeline, seasonality, warming

    return overview, warming, seasonality, extremes, pipeline


def test_server_serves_index_and_layout(app_module):
    client = app_module.server.test_client()
    assert client.get("/").status_code == 200
    assert client.get("/_dash-layout").status_code == 200


def test_pages_registered(app_module):
    import dash

    assert [p["path"] for p in sorted(dash.page_registry.values(), key=lambda p: p["order"])] == [
        "/",
        "/warming",
        "/seasonality",
        "/extremes",
        "/pipeline",
    ]


def test_every_page_renders_with_empty_lakehouse(app_module):
    overview, warming, seasonality, extremes, pipeline = pages()
    for page in pages():
        page.layout()
    kpis, fig = overview.render("light")
    assert kpis == [] and fig.layout.annotations[0].text.startswith("No data yet")
    assert (
        warming.render("light", ["vienna"], [2000, 2005])
        .layout.annotations[0]
        .text.startswith("No data yet")
    )
    band, heat = seasonality.render("dark", "vienna", 2004)
    assert band.layout.annotations and heat.layout.annotations
    ext, ridge = extremes.render("light", ["vienna"], [2000, 2005], "vienna")
    assert ext.layout.annotations and ridge.layout.annotations
    grid, dq, stats = pipeline.render("light", 0)
    assert dq.layout.annotations


def test_pages_render_data(app_module, gold_lake):
    overview, warming, seasonality, extremes, pipeline = pages()
    kpis, fig = overview.render("light")
    assert len(kpis) == 3 and fig.data[0].type == "scattergeo"
    assert warming.render("dark", ["vienna", "london"], [2000, 2005]).data[-1].type == "heatmap"
    band, heat = seasonality.render("light", "vienna", 2004)
    assert band.data[-1].name == "2004" and heat.data[0].type == "heatmap"
    ext, ridge = extremes.render("light", ["vienna"], [2000, 2005], "vienna")
    assert len(ext.data) == 3 and len(ridge.data) == 4
    grid, dq, stats = pipeline.render("light", 1)
    assert grid.data[0].type == "heatmap"
    assert [t.name for t in dq.data] == ["passed", "failed (soft)"]
    assert "weather_hourly" in str(stats)


def test_seasonality_layout_offers_data_years(app_module, gold_lake):
    _, _, seasonality, _, _ = pages()
    assert "2004" in str(seasonality.layout())


def test_default_cities_prefer_cities_with_data():
    import pandas as pd
    from weather_dash.layout import default_cities

    cities = pd.DataFrame(
        {
            "id": ["singapore", "cairo", "vienna", "london", "yakutsk"],
            "climate": ["tropical", "arid", "temperate", "temperate", "continental"],
        }
    )
    # demo data: Cairo has none, London (not Vienna) is the temperate city with data
    assert default_cities(cities, {"singapore", "london", "yakutsk"}) == [
        "singapore",
        "london",
        "yakutsk",
    ]
    assert default_cities(cities, set()) == ["singapore", "cairo", "vienna", "yakutsk"]


def test_assets_are_compressed(app_module):
    client = app_module.server.test_client()
    r = client.get("/", headers={"Accept-Encoding": "gzip"})
    assert r.headers.get("Content-Encoding") == "gzip"  # plotly.js is 4.8 MB uncompressed
