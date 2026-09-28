import pandas as pd
from weather_dash import theme

CITIES = pd.DataFrame(
    [
        {"id": "singapore", "climate": "tropical"},
        {"id": "vienna", "climate": "temperate"},
        {"id": "london", "climate": "temperate"},
    ]
)


def test_palette_matches_validated_values():
    assert theme.SERIES["light"] == [
        "#2a78d6",
        "#eb6834",
        "#1baf7a",
        "#eda100",
        "#e87ba4",
        "#008300",
        "#4a3aa7",
        "#e34948",
    ]
    assert theme.SERIES["dark"][0] == "#3987e5"
    assert theme.CLIMATE_ORDER == ("tropical", "arid", "temperate", "continental", "polar edge")


def test_series_style_follows_entity_not_selection():
    assert theme.series_style("vienna", CITIES, "light") == ("#1baf7a", "solid")
    assert theme.series_style("london", CITIES, "light") == ("#1baf7a", "dash")
    assert theme.series_style("singapore", CITIES, "dark") == ("#3987e5", "solid")


def test_diverging_has_neutral_gray_midpoint():
    for mode in ("light", "dark"):
        scale = theme.diverging(mode)
        assert scale[2] == [0.5, theme.CHROME[mode]["mid"]]
        assert scale[0][0] == 0.0 and scale[-1][0] == 1.0
        assert scale[-1][1] == "#7e1414"


def test_template_uses_surface():
    t = theme.template("dark")
    assert t.layout.paper_bgcolor == theme.CHROME["dark"]["surface"]
    assert t.layout.plot_bgcolor == theme.CHROME["dark"]["surface"]


def test_rgba():
    assert theme.rgba("#2a78d6", 0.5) == "rgba(42,120,214,0.5)"
