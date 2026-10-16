"""App shell: header with navigation and theme switch, shared filters, footer attribution."""

from __future__ import annotations

import dash
import pandas as pd
from dash import dcc, html

from weather_dash import data
from weather_dash.theme import CLIMATE_ORDER


def default_cities(cities: pd.DataFrame, with_data: set[str] | None = None) -> list[str]:
    """First city of each climate zone that has data (a demo has only a few); before any data,
    the first configured city of each zone."""
    if with_data:
        cities = cities[cities["id"].isin(with_data)]
    picks = []
    for zone in CLIMATE_ORDER:
        match = cities[cities["climate"] == zone]
        if not match.empty:
            picks.append(str(match.iloc[0]["id"]))
    return picks


def shell() -> html.Div:
    cities = data.get_cities()
    trends = data.get_lakehouse().table("gold", "trend_city")
    with_data = set(trends["city_id"]) if not trends.empty else set()
    first, last = data.first_year(), data.last_complete_year()
    pages = sorted(dash.page_registry.values(), key=lambda p: p["order"])
    return html.Div(
        className="app",
        children=[
            dcc.Store(id="theme-applied"),
            html.Header(
                className="topbar",
                children=[
                    html.Div(
                        [
                            html.H1("Weather Lakehouse"),
                            html.P(
                                "ERA5 hourly reanalysis · Airflow · PySpark · Delta Lake",
                                className="subtitle",
                            ),
                        ]
                    ),
                    html.Nav(
                        [
                            dcc.Link(p["name"], href=p["relative_path"], className="navlink")
                            for p in pages
                        ]
                    ),
                    dcc.RadioItems(
                        id="theme",
                        value="light",
                        inline=True,
                        className="theme-toggle",
                        options=[
                            {"label": "Light", "value": "light"},
                            {"label": "Dark", "value": "dark"},
                        ],
                    ),
                ],
            ),
            html.Section(
                className="filters",
                children=[
                    html.Label(
                        className="filter",
                        children=[
                            html.Span("Cities (max 8 per chart)"),
                            dcc.Dropdown(
                                id="f-cities",
                                multi=True,
                                value=default_cities(cities, with_data),
                                options=[
                                    {"label": f"{r.name} · {r.climate}", "value": r.id}
                                    for r in cities.itertuples()
                                ],
                            ),
                        ],
                    ),
                    html.Label(
                        className="filter",
                        children=[
                            html.Span("Years"),
                            dcc.RangeSlider(
                                id="f-years",
                                min=first,
                                max=last,
                                step=1,
                                value=[first, last],
                                marks={
                                    y: str(y) for y in range(((first + 9) // 10) * 10, last + 1, 10)
                                },
                                tooltip={"placement": "bottom"},
                            ),
                        ],
                    ),
                ],
            ),
            html.Main(dash.page_container, className="content"),
            html.Footer(
                className="footer",
                children=[
                    "Weather data by ",
                    html.A("Open-Meteo.com", href="https://open-meteo.com/"),
                    " (CC BY 4.0), ERA5 reanalysis (Copernicus Climate Change Service).",
                ],
            ),
        ],
    )
