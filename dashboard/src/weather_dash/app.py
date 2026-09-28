"""Dash entry point: `gunicorn weather_dash.app:server`."""

from dash import Dash, Input, Output

from weather_dash import layout

app = Dash(
    __name__,
    use_pages=True,
    pages_folder="",
    title="Weather Lakehouse",
    suppress_callback_exceptions=True,
)

from weather_dash.pages import extremes, overview, pipeline, seasonality, warming  # noqa: E402,F401

app.layout = layout.shell
app.clientside_callback(
    "function(t) { document.documentElement.dataset.theme = t; return t; }",
    Output("theme-applied", "data"),
    Input("theme", "value"),
)
server = app.server
