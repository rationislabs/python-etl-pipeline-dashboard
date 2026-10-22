import dash
from dash import Input, Output, callback, dcc, html

from weather_dash import data, figures


def layout(**_):
    return html.Div(
        [
            html.H2("Warming"),
            html.P(
                "Annual mean temperature anomaly per city with a linear trend; the stripes below share "
                "the same year axis. Line color = climate zone, dash = city.",
                className="lede",
            ),
            dcc.Graph(id="wm-fig", style={"height": "760px"}, config={"displaylogo": False}),
        ]
    )


def render(theme, city_ids, years):
    lake, cities = data.get_lakehouse(), data.get_cities()
    city_ids, years = data.selection(city_ids, years, cities)
    annual = lake.table("gold", "anomaly_annual")
    if annual.empty:
        return figures.empty_figure(data.empty_message(lake, cities), theme)
    return figures.warming_figure(
        annual,
        lake.table("gold", "trend_city"),
        cities,
        city_ids,
        years,
        theme,
        data.baseline_label(),
        message="No complete years for this selection",
    )


dash.register_page(__name__, path="/warming", name="Warming", order=1, layout=layout)
callback(
    Output("wm-fig", "figure"),
    Input("theme", "value"),
    Input("f-cities", "value"),
    Input("f-years", "value"),
)(render)
