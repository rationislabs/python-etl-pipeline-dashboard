from datetime import UTC, datetime

import dash
import pandas as pd
from dash import Input, Output, callback, dcc, html

from weather_dash import data, figures


def layout(**_):
    return html.Div(
        [
            html.H2("Pipeline health"),
            html.P(
                "Backfill progress, data-quality results and table freshness. Refreshes every 30 s.",
                className="lede",
            ),
            dcc.Interval(id="pl-tick", interval=30_000),
            dcc.Graph(id="pl-grid", style={"height": "560px"}, config={"displaylogo": False}),
            dcc.Graph(id="pl-dq", config={"displaylogo": False}),
            html.Div(id="pl-stats"),
        ]
    )


def _stats_table(stats: pd.DataFrame) -> html.Table | html.P:
    if stats.empty:
        return html.P("No pipeline statistics yet.", className="muted")
    head = html.Tr([html.Th(h) for h in ("Layer", "Table", "Rows", "Latest timestamp", "Updated")])
    rows = [
        html.Tr(
            [
                html.Td(r.layer),
                html.Td(r.table_name),
                html.Td(f"{int(r.row_count):,}"),
                html.Td("–" if pd.isna(r.max_ts) else f"{r.max_ts:%Y-%m-%d %H:%M} UTC"),
                html.Td(f"{r.updated_at:%Y-%m-%d %H:%M} UTC"),
            ]
        )
        for r in stats.sort_values(["layer", "table_name"]).itertuples()
    ]
    return html.Table([html.Thead(head), html.Tbody(rows)], className="stats")


def render(theme, _tick):
    lake, cities = data.get_lakehouse(), data.get_cities()
    now_year = datetime.now(UTC).year
    grid = figures.backfill_grid(
        lake.table("ops", "ingest_log"),
        cities,
        data.first_year(),
        now_year,
        theme,
        message="No cities configured",
    )
    dq = figures.dq_history(lake.table("ops", "dq_results"), theme)
    return grid, dq, _stats_table(lake.table("ops", "pipeline_stats"))


dash.register_page(__name__, path="/pipeline", name="Pipeline health", order=4, layout=layout)
callback(
    Output("pl-grid", "figure"),
    Output("pl-dq", "figure"),
    Output("pl-stats", "children"),
    Input("theme", "value"),
    Input("pl-tick", "n_intervals"),
)(render)
