"""Chart palette (dataviz skill reference instance, validated 2026-09-27) and Plotly templates."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

CHROME = {
    "light": {
        "surface": "#fcfcfb",
        "page": "#f9f9f7",
        "ink": "#0b0b0b",
        "ink2": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "axis": "#c3c2b7",
        "mid": "#f0efec",
    },
    "dark": {
        "surface": "#1a1a19",
        "page": "#0d0d0d",
        "ink": "#ffffff",
        "ink2": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "axis": "#383835",
        "mid": "#383835",
    },
}
# validate_palette.js: ALL CHECKS PASS in light and dark (contrast WARN on 3 light slots ->
# legends and hover labels always present).
SERIES = {
    "light": [
        "#2a78d6",
        "#eb6834",
        "#1baf7a",
        "#eda100",
        "#e87ba4",
        "#008300",
        "#4a3aa7",
        "#e34948",
    ],
    "dark": [
        "#3987e5",
        "#d95926",
        "#199e70",
        "#c98500",
        "#d55181",
        "#008300",
        "#9085e9",
        "#e66767",
    ],
}
CLIMATE_ORDER = ("tropical", "arid", "temperate", "continental", "polar edge")
DASHES = ("solid", "dash", "dot", "dashdot", "longdash", "longdashdot")
STATUS = {"complete": "#0ca30c", "partial": "#fab219", "failed": "#d03b3b"}
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]


def diverging(mode: str) -> list[list]:
    return [
        [0.0, "#104281"],
        [0.25, "#5598e7"],
        [0.5, CHROME[mode]["mid"]],
        [0.75, "#e66767"],
        [1.0, "#7e1414"],
    ]


def rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i : i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


def series_style(city_id: str, cities: pd.DataFrame, mode: str) -> tuple[str, str]:
    """Color = climate zone slot (fixed), dash = position within the zone in config order."""
    climate = cities.loc[cities["id"] == city_id, "climate"].iloc[0]
    zone = cities[cities["climate"] == climate]["id"].tolist()
    return SERIES[mode][CLIMATE_ORDER.index(climate)], DASHES[zone.index(city_id) % len(DASHES)]


def template(mode: str) -> go.layout.Template:
    c = CHROME[mode]
    axis = dict(
        gridcolor=c["grid"],
        linecolor=c["axis"],
        zerolinecolor=c["axis"],
        tickfont=dict(color=c["muted"]),
        title=dict(font=dict(color=c["ink2"])),
    )
    return go.layout.Template(
        layout=go.Layout(
            paper_bgcolor=c["surface"],
            plot_bgcolor=c["surface"],
            font=dict(family="Inter, system-ui, sans-serif", color=c["ink"], size=13),
            title=dict(font=dict(color=c["ink"], size=16), x=0.01),
            colorway=SERIES[mode],
            xaxis=axis,
            yaxis=axis,
            legend=dict(orientation="h", y=-0.12, font=dict(color=c["ink2"])),
            hoverlabel=dict(bgcolor=c["surface"], font=dict(color=c["ink"]), bordercolor=c["axis"]),
            margin=dict(l=64, r=24, t=56, b=48),
        )
    )
