import dash
from dash import Input, Output, callback, dcc, html

from weather_dash import data, figures
from weather_dash.layout import default_cities


def layout(**_):
    cities = data.get_cities()
    picks = default_cities(cities)
    return html.Div(
        [
            html.H2("Extremes"),
            html.P(
                "Days beyond each city's own baseline percentiles, and how the whole distribution "
                "of daily temperatures has shifted by decade.",
                className="lede",
            ),
            dcc.Graph(id="ex-fig", style={"height": "760px"}, config={"displaylogo": False}),
            html.Div(
                className="controls",
                children=[
                    dcc.Dropdown(
                        id="ex-city",
                        clearable=False,
                        value=picks[0] if picks else None,
                        options=[{"label": r.name, "value": r.id} for r in cities.itertuples()],
                    ),
                ],
            ),
            dcc.Graph(id="ex-ridge", style={"height": "520px"}, config={"displaylogo": False}),
        ]
    )


def render(theme, city_ids, years, ridge_city):
    lake, cities = data.get_lakehouse(), data.get_cities()
    city_ids, years = data.selection(city_ids, years, cities)
    msg = data.empty_message(lake, cities)
    ext = lake.table("gold", "extremes_annual")
    fig = (
        figures.empty_figure(msg, theme)
        if ext.empty
        else figures.extremes_figure(
            ext,
            cities,
            city_ids,
            years,
            theme,
            message="No complete years for this selection",
        )
    )
    ridge = figures.ridgeline(
        lake.table("gold", "distribution_decade"), cities, ridge_city, theme, message=msg
    )
    return fig, ridge


dash.register_page(__name__, path="/extremes", name="Extremes", order=3, layout=layout)
callback(
    Output("ex-fig", "figure"),
    Output("ex-ridge", "figure"),
    Input("theme", "value"),
    Input("f-cities", "value"),
    Input("f-years", "value"),
    Input("ex-city", "value"),
)(render)
