import dash
from dash import Input, Output, callback, dcc, html

from weather_dash import data, figures


def layout(**_):
    return html.Div(
        [
            html.H2("Overview"),
            html.P(
                "How far each city's latest complete year sits above or below its baseline.",
                className="lede",
            ),
            html.Div(id="ov-kpis", className="kpis"),
            dcc.Graph(id="ov-map", style={"height": "560px"}, config={"displaylogo": False}),
        ]
    )


def render(theme):
    lake, cities = data.get_lakehouse(), data.get_cities()
    annual = lake.table("gold", "anomaly_annual")
    if annual.empty:
        return [], figures.empty_figure(data.empty_message(lake, cities), theme)
    kpis = [
        html.Div(
            className="kpi",
            children=[
                html.Span(k["label"], className="kpi-label"),
                html.Strong(k["value"], className="kpi-value"),
                html.Span(k["detail"], className="kpi-detail"),
            ],
        )
        for k in figures.overview_kpis(annual, lake.table("gold", "trend_city"), cities)
    ]
    return kpis, figures.overview_map(annual, cities, theme)


dash.register_page(__name__, path="/", name="Overview", order=0, layout=layout)
callback(Output("ov-kpis", "children"), Output("ov-map", "figure"), Input("theme", "value"))(render)
