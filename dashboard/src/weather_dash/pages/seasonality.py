import dash
from dash import Input, Output, callback, dcc, html

from weather_dash import data, figures
from weather_dash.layout import default_cities


def layout(**_):
    cities, daily = data.get_cities(), data.get_lakehouse().table("gold", "daily_city")
    years = sorted({int(y) for y in daily["year"]}, reverse=True) if not daily.empty else []
    picks = default_cities(cities)
    return html.Div(
        [
            html.H2("Seasonality"),
            html.P(
                "A chosen year against the city's typical range for each day of the year.",
                className="lede",
            ),
            html.Div(
                className="controls",
                children=[
                    dcc.Dropdown(
                        id="se-city",
                        clearable=False,
                        value=picks[0] if picks else None,
                        options=[{"label": r.name, "value": r.id} for r in cities.itertuples()],
                    ),
                    dcc.Dropdown(
                        id="se-year",
                        clearable=False,
                        value=years[0] if years else None,
                        options=[{"label": str(y), "value": y} for y in years],
                    ),
                ],
            ),
            dcc.Graph(id="se-band", style={"height": "480px"}, config={"displaylogo": False}),
            dcc.Graph(id="se-heat", style={"height": "420px"}, config={"displaylogo": False}),
        ]
    )


def render(theme, city_id, year):
    lake, cities = data.get_lakehouse(), data.get_cities()
    msg = data.empty_message(lake, cities)
    band = figures.seasonality_band(
        lake.table("gold", "climatology_doy"),
        lake.table("gold", "daily_city"),
        cities,
        city_id,
        year,
        theme,
        message=msg,
    )
    heat = figures.monthly_heatmap(
        lake.table("gold", "anomaly_monthly"), cities, city_id, theme, message=msg
    )
    return band, heat


dash.register_page(__name__, path="/seasonality", name="Seasonality", order=2, layout=layout)
callback(
    Output("se-band", "figure"),
    Output("se-heat", "figure"),
    Input("theme", "value"),
    Input("se-city", "value"),
    Input("se-year", "value"),
)(render)
