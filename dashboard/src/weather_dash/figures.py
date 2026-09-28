"""Pure DataFrame -> Plotly figure builders (no Dash, no I/O) so each one is unit-testable."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from weather_dash.theme import (
    BLUE_RAMP,
    CHROME,
    SERIES,
    STATUS,
    diverging,
    rgba,
    series_style,
    template,
)

MAX_SERIES = 8
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def empty_figure(message: str, mode: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(template=template(mode))
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.add_annotation(
        text=message,
        showarrow=False,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        font=dict(size=16, color=CHROME[mode]["ink2"]),
    )
    return fig


def _name(cities: pd.DataFrame, city_id: str) -> str:
    match = cities.loc[cities["id"] == city_id, "name"]
    return str(match.iloc[0]) if not match.empty else city_id


def _abs_max(values) -> float:
    arr = np.asarray(values, dtype=float)
    return max(0.5, float(np.nanmax(np.abs(arr)))) if arr.size else 0.5


def overview_map(annual, cities, mode, message="No data yet"):
    if annual.empty:
        return empty_figure(message, mode)
    year = int(annual["year"].max())
    latest = annual[annual["year"] == year].merge(cities, left_on="city_id", right_on="id")
    m = _abs_max(latest["anomaly"])
    c = CHROME[mode]
    fig = go.Figure(
        go.Scattergeo(
            lat=latest["lat"],
            lon=latest["lon"],
            text=latest["name"],
            customdata=latest[["anomaly"]].to_numpy(),
            mode="markers",
            marker=dict(
                size=14,
                color=latest["anomaly"],
                colorscale=diverging(mode),
                cmin=-m,
                cmax=m,
                line=dict(width=2, color=c["surface"]),
                colorbar=dict(title=dict(text="°C"), thickness=12),
            ),
            hovertemplate="<b>%{text}</b><br>%{customdata[0]:+.2f} °C<extra></extra>",
        )
    )
    fig.update_layout(
        template=template(mode),
        title=f"Temperature anomaly by city, {year}",
        geo=dict(
            projection_type="natural earth",
            showland=True,
            landcolor=c["grid"],
            bgcolor=c["surface"],
            showframe=False,
            coastlinecolor=c["axis"],
            showcountries=False,
        ),
    )
    return fig


def overview_kpis(annual, trends, cities) -> list[dict]:
    if annual.empty:
        return []
    by_year = annual.groupby("year")["anomaly"].mean()
    warmest = int(by_year.idxmax())
    latest = int(annual["year"].max())
    kpis = [
        {
            "label": "Warmest year (mean of all cities)",
            "value": str(warmest),
            "detail": f"{by_year[warmest]:+.2f} °C vs baseline",
        }
    ]
    if not trends.empty:
        top = trends.sort_values("slope_c_per_decade", ascending=False).iloc[0]
        kpis.append(
            {
                "label": "Fastest warming",
                "value": _name(cities, top["city_id"]),
                "detail": f"{top['slope_c_per_decade']:+.2f} °C per decade",
            }
        )
    else:
        kpis.append({"label": "Fastest warming", "value": "–", "detail": "needs 2+ complete years"})
    kpis.append(
        {
            "label": "Latest complete year",
            "value": str(latest),
            "detail": f"{by_year[latest]:+.2f} °C mean anomaly",
        }
    )
    return kpis


def warming_figure(
    annual, trends, cities, city_ids, years, mode, baseline_label, message="No data yet"
):
    ids = list(city_ids or [])[:MAX_SERIES]
    sel = annual[annual["city_id"].isin(ids) & annual["year"].between(*years)]
    if sel.empty:
        return empty_figure(message, mode)
    present = [c for c in ids if c in set(sel["city_id"])]
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, row_heights=[0.62, 0.38], vertical_spacing=0.06
    )
    for cid in present:
        d = sel[sel["city_id"] == cid].sort_values("year")
        color, dash = series_style(cid, cities, mode)
        name = _name(cities, cid)
        fig.add_trace(
            go.Scatter(
                x=d["year"],
                y=d["anomaly"],
                name=name,
                mode="lines",
                line=dict(color=color, dash=dash, width=2),
                hovertemplate=f"{name} %{{x}}: %{{y:+.2f}} °C<extra></extra>",
            ),
            row=1,
            col=1,
        )
        t = trends[trends["city_id"] == cid]
        if not t.empty:
            slope = float(t["slope_c_per_decade"].iloc[0]) / 10
            b = float(t["intercept"].iloc[0])
            xs = [int(d["year"].min()), int(d["year"].max())]
            fig.add_trace(
                go.Scatter(
                    x=xs,
                    y=[b + slope * x for x in xs],
                    mode="lines",
                    showlegend=False,
                    name=f"{name} trend",
                    hoverinfo="skip",
                    line=dict(color=color, width=1, dash="dot"),
                ),
                row=1,
                col=1,
            )
    pivot = sel.pivot_table(index="city_id", columns="year", values="anomaly").reindex(present)
    m = _abs_max(pivot.to_numpy())
    fig.add_trace(
        go.Heatmap(
            z=pivot.to_numpy(),
            x=pivot.columns.tolist(),
            y=[_name(cities, c) for c in present],
            colorscale=diverging(mode),
            zmid=0,
            zmin=-m,
            zmax=m,
            xgap=1,
            ygap=2,
            colorbar=dict(title=dict(text="°C"), len=0.38, y=0.19, thickness=12),
            hovertemplate="%{y} %{x}: %{z:+.2f} °C<extra></extra>",
            name="stripes",
        ),
        row=2,
        col=1,
    )
    fig.add_hline(y=0, line=dict(color=CHROME[mode]["axis"], width=1), row=1, col=1)
    fig.update_layout(
        template=template(mode), title=f"Annual temperature anomaly vs {baseline_label}"
    )
    fig.update_yaxes(title_text="°C", row=1, col=1)
    return fig


def _doy_dates(doy: pd.Series) -> pd.Series:
    return pd.Timestamp("2000-01-01") + pd.to_timedelta(doy.astype(int) - 1, unit="D")


def seasonality_band(clim, daily, cities, city_id, year, mode, message="No data yet"):
    if clim.empty:
        return empty_figure(message, mode)
    c = clim[clim["city_id"] == city_id].sort_values("doy")
    if c.empty:
        return empty_figure(message, mode)
    x = _doy_dates(c["doy"])
    band = BLUE_RAMP[1] if mode == "light" else BLUE_RAMP[5]
    fig = go.Figure(
        [
            go.Scatter(
                x=x,
                y=c["p90"],
                mode="lines",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip",
            ),
            go.Scatter(
                x=x,
                y=c["p10"],
                mode="lines",
                line=dict(width=0),
                fill="tonexty",
                fillcolor=rgba(band, 0.6),
                name="10th–90th percentile (baseline)",
                hoverinfo="skip",
            ),
            go.Scatter(
                x=x,
                y=c["p50"],
                mode="lines",
                line=dict(color=CHROME[mode]["muted"], width=1.5),
                name="Median (baseline)",
                hovertemplate="median %{y:.1f} °C<extra></extra>",
            ),
        ]
    )
    d = daily[(daily["city_id"] == city_id) & (daily["year"] == year)].sort_values("doy")
    if not d.empty:
        fig.add_trace(
            go.Scatter(
                x=_doy_dates(d["doy"]),
                y=d["tmean"],
                mode="lines",
                name=str(year),
                line=dict(color=SERIES[mode][1], width=2),
                hovertemplate=f"{year} %{{x|%b %d}}: %{{y:.1f}} °C<extra></extra>",
            )
        )
    fig.update_layout(
        template=template(mode),
        hovermode="x unified",
        title=f"{_name(cities, city_id)}: daily mean temperature vs baseline",
        xaxis=dict(tickformat="%b", dtick="M1"),
        yaxis=dict(title="°C"),
    )
    return fig


def monthly_heatmap(monthly, cities, city_id, mode, message="No data yet"):
    if monthly.empty:
        return empty_figure(message, mode)
    d = monthly[monthly["city_id"] == city_id]
    if d.empty:
        return empty_figure(message, mode)
    pivot = d.pivot_table(index="month", columns="year", values="anomaly").reindex(range(1, 13))
    m = _abs_max(pivot.to_numpy())
    fig = go.Figure(
        go.Heatmap(
            z=pivot.to_numpy(),
            x=pivot.columns.tolist(),
            y=MONTHS,
            colorscale=diverging(mode),
            zmid=0,
            zmin=-m,
            zmax=m,
            xgap=1,
            ygap=1,
            colorbar=dict(title=dict(text="°C"), thickness=12),
            hovertemplate="%{y} %{x}: %{z:+.2f} °C<extra></extra>",
        )
    )
    fig.update_layout(
        template=template(mode),
        title=f"{_name(cities, city_id)}: monthly anomaly",
        yaxis=dict(autorange="reversed"),
    )
    return fig


def extremes_figure(ext, cities, city_ids, years, mode, message="No data yet"):
    ids = list(city_ids or [])[:MAX_SERIES]
    sel = ext[ext["city_id"].isin(ids) & ext["year"].between(*years)]
    if sel.empty:
        return empty_figure(message, mode)
    panels = [
        ("hot_days", "Hot days (> baseline p95 of daily max)"),
        ("cold_nights", "Cold nights (< baseline p5 of daily min)"),
        ("heavy_rain_days", "Heavy-rain days (> baseline p95 of wet days)"),
    ]
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.07,
        subplot_titles=[t for _, t in panels],
    )
    for cid in [c for c in ids if c in set(sel["city_id"])]:
        d = sel[sel["city_id"] == cid].sort_values("year")
        color, dash = series_style(cid, cities, mode)
        name = _name(cities, cid)
        for row, (col, _) in enumerate(panels, start=1):
            fig.add_trace(
                go.Scatter(
                    x=d["year"],
                    y=d[col],
                    mode="lines",
                    name=name,
                    legendgroup=cid,
                    showlegend=row == 1,
                    line=dict(color=color, dash=dash, width=2),
                    hovertemplate=f"{name} %{{x}}: %{{y}} days<extra></extra>",
                ),
                row=row,
                col=1,
            )
    fig.update_layout(template=template(mode), title="Extremes per year")
    return fig


def ridgeline(dist, cities, city_id, mode, message="No data yet"):
    if dist.empty:
        return empty_figure(message, mode)
    d = dist[dist["city_id"] == city_id]
    if d.empty:
        return empty_figure(message, mode)
    decades = sorted(int(x) for x in d["decade"].unique())
    dens = d.assign(density=d["days"] / d.groupby("decade")["days"].transform("sum"))
    scale = 1.6 / float(dens["density"].max())
    ramp = BLUE_RAMP[2:] if mode == "light" else BLUE_RAMP[1:6]
    fig = go.Figure()
    for i, dec in enumerate(decades):
        g = dens[dens["decade"] == dec].sort_values("bin")
        color = ramp[min(i * len(ramp) // len(decades), len(ramp) - 1)]
        fig.add_trace(
            go.Scatter(
                x=g["bin"],
                y=[i] * len(g),
                mode="lines",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=g["bin"],
                y=i + g["density"] * scale,
                mode="lines",
                fill="tonexty",
                fillcolor=rgba(color, 0.55),
                line=dict(color=color, width=1.5),
                name=f"{dec}s",
                customdata=g["density"],
                hovertemplate=f"{dec}s %{{x:.1f}} °C: %{{customdata:.1%}} of days<extra></extra>",
            )
        )
    fig.update_layout(
        template=template(mode),
        showlegend=False,
        title=f"{_name(cities, city_id)}: distribution of daily mean temperature by decade",
        xaxis=dict(title="°C"),
        yaxis=dict(
            tickvals=list(range(len(decades))), ticktext=[f"{d}s" for d in decades], showgrid=False
        ),
    )
    return fig


def backfill_grid(log, cities, first, last, mode, message="No data yet"):
    ids = cities["id"].tolist()
    years = list(range(first, last + 1))
    if not ids or not years:
        return empty_figure(message, mode)
    code = {"partial": 1, "complete": 2, "failed": 3}
    z = np.zeros((len(ids), len(years)))
    text = [["missing"] * len(years) for _ in ids]
    for r in log.itertuples():
        if r.city_id in ids and first <= int(r.year) <= last:
            i, j = ids.index(r.city_id), int(r.year) - first
            z[i, j] = code.get(r.status, 0)
            text[i][j] = f"{r.status}, {int(r.rows):,} h" if pd.notna(r.rows) else str(r.status)
    c = CHROME[mode]
    colors = [c["grid"], STATUS["partial"], STATUS["complete"], STATUS["failed"]]
    scale = []
    for k, col in enumerate(colors):
        scale += [[k / 4, col], [(k + 1) / 4, col]]
    fig = go.Figure(
        go.Heatmap(
            z=z,
            x=years,
            y=[_name(cities, i) for i in ids],
            text=text,
            zmin=-0.5,
            zmax=3.5,
            colorscale=scale,
            xgap=1,
            ygap=1,
            colorbar=dict(
                tickvals=[0, 1, 2, 3],
                ticktext=["missing", "partial", "complete", "failed"],
                thickness=12,
            ),
            hovertemplate="%{y} %{x}: %{text}<extra></extra>",
        )
    )
    fig.update_layout(
        template=template(mode),
        title="Backfill progress (city × year)",
        yaxis=dict(autorange="reversed"),
    )
    return fig


def dq_history(dq, mode, message="No data-quality results yet"):
    if dq.empty:
        return empty_figure(message, mode)
    d = dq.sort_values("run_at").assign(label=lambda x: x["table_name"] + " · " + x["check_name"])
    groups = [
        ("passed", d["passed"], STATUS["complete"], "circle"),
        (
            "failed (soft)",
            ~d["passed"] & (d["severity"] == "soft"),
            STATUS["partial"],
            "triangle-up",
        ),
        ("failed (hard)", ~d["passed"] & (d["severity"] == "hard"), STATUS["failed"], "x"),
    ]
    fig = go.Figure()
    for name, mask, color, symbol in groups:
        s = d[mask]
        if s.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=s["run_at"],
                y=s["label"],
                mode="markers",
                name=name,
                marker=dict(color=color, symbol=symbol, size=10),
                customdata=s[["observed", "threshold"]].to_numpy(),
                hovertemplate="%{y}<br>%{x}<br>observed %{customdata[0]:.3g} "
                "(threshold %{customdata[1]:.3g})<extra></extra>",
            )
        )
    fig.update_layout(
        template=template(mode),
        title="Data-quality checks per run",
        height=max(320, 22 * d["label"].nunique() + 140),
    )
    return fig
