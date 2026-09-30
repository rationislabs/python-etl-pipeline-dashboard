# Weather Lakehouse

A local, Docker-only data platform for 45+ years of hourly ERA5 weather in 20 cities:

- **Airflow 3** orchestrates quota-aware ingestion from the Open-Meteo archive.
- **PySpark 4 on a standalone cluster** refines the data through **bronze → silver → gold Delta Lake** tables, with data-quality gates.
- A **Dash** app presents climate analytics and pipeline health.

```
cities.yaml ─┐
Open-Meteo ──► landing JSON ──► bronze.weather_raw ──► silver.weather_hourly ──► gold.* ──► Dash
 (ERA5)        (ingest tasks)   raw payload, append    typed, UTC + local date,  7 analytics
                                                       MERGE on (city, hour),    tables
                                                       DQ gate ──► ops.dq_results
```

| Service | URL |
|---|---|
| Dashboard | http://localhost:8050 |
| Airflow (no login, local only) | http://localhost:8080 |
| Spark master UI | http://localhost:8081 |

All ports bind to `127.0.0.1` only, so nothing is reachable from the LAN.

## Quick start

Requirements: Docker with Compose v2, about 8 GB of free RAM, and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run inv demo      # 3 cities (Singapore, Vienna, Yakutsk), 1991 → today
```

The demo takes about 15 minutes: about 6 of those are the API's per-minute limit, the rest is Spark. The dashboard fills in when the `refine` task finishes. Before that, every page shows its progress.

Stop the stack with `uv run inv down`. Your data in `./lakehouse` and the Airflow database are kept.

To watch the containers, images, volumes and logs in a terminal UI, run [lazydocker](https://github.com/jesseduffield/lazydocker) (optional):

```bash
lazydocker
```

## Full backfill

```bash
uv run inv up && uv run inv backfill
```

The full backfill covers 20 cities × 1980 → today, about 940 city-years, about 8M hourly rows and about 1 GB on disk. Open-Meteo's free tier allows 10,000 weighted calls per day, and one city-year costs about 31. So the backfill takes **about 3 days** and runs unattended:

- A local SQLite budget keeps calls under the per-minute, per-hour and per-day limits.
- A task that hits a limit is **rescheduled**, not failed. The run continues after the quota resets.
- The daily budget is a calendar UTC day: a task that exhausts it resumes at 00:05 UTC.
- City-years already complete are never fetched again, so re-triggering is always safe.

`daily_refresh` runs at 06:00 UTC. It starts with a `backfill_started` short-circuit, so it stays idle until a backfill has actually ingested something. Once one has, it re-fetches the last 14 days, and ERA5's preliminary values are overwritten by MERGE. After a long downtime the window reaches back to a week before the last fetch (never before 1 January), so gaps are refetched automatically. It then completes the previous year and rebuilds gold.

## Layers

| Table | Grain | Notes |
|---|---|---|
| `bronze.weather_raw` | one API response | raw JSON payload, SHA-256, fetch/load timestamps |
| `silver.weather_hourly` | city × UTC hour | 12 variables, `date_local` from each city's time zone, MERGE-upserted |
| `gold.daily_city` | city × local day | min/mean/max temperature, precipitation, snow, gusts, humidity |
| `gold.climatology_doy` | city × day of year | p10/p50/p90 of the baseline, ±7-day window |
| `gold.anomaly_monthly` / `anomaly_annual` | city × month / year | vs the baseline (default 1991–2020) |
| `gold.trend_city` | city | OLS slope °C/decade, R² |
| `gold.extremes_annual` | city × year | hot days, cold nights, heavy-rain days vs the city's own percentiles |
| `gold.distribution_decade` | city × decade × 0.5 °C | ridgeline input |
| `ops.ingest_log` / `dq_results` / `pipeline_stats` | | progress, check history, row counts and freshness |

Data-quality checks are plain PySpark:
- **Hard failures stop the silver MERGE:** duplicate keys, out-of-range values, unknown cities.
- **Soft failures only warn:** null share, incomplete years, stale cities.

## Configuring cities

Edit `config/cities.yaml`. Each entry needs `id`, `name`, `country`, `lat`, `lon`, an IANA `tz` and a `climate` zone. The config is validated at plan time. The next `inv backfill` fetches only the new cities. Restart the dashboard (`docker compose restart dash`) to see them.

## Testing

```bash
uv run inv test            # unit + Spark tests, offline after the first run (needs Java 17+)
uv run inv test --images   # also builds images and runs the DAG tests inside Airflow
uv run inv lint
uv run inv e2e             # real API, isolated stack, 1 city × 2024→now (stop the main stack first)
```

The first Spark run downloads the Delta Lake jars via Ivy; after that the tests need no network.

## Local development

- `uv sync` creates one `.venv` containing both packages: `etl/` (`weather_etl`) and `dashboard/` (`weather_dash`).
- The Spark tests need Java 17 or later on your PATH. The Docker images run everything on Java 21.
- Everything in `weather_etl` runs without Airflow. The DAGs in `airflow/dags` are thin wrappers.

## Known limits

- ERA5 is a ~25 km reanalysis grid. A "city" is the grid cell nearest its coordinates.
- The most recent days are preliminary, and ERA5 revises them for several days.
- The backfill speed is bound by the free API quota, not by Spark.
- Day-of-year charts place non-leap years on a leap-year axis, so dates after Feb 28 shift by one day.
- For cities east of UTC, the first local day of `FIRST_YEAR` is incomplete (its early hours fall in the previous UTC year) and is dropped, so that January and that year are also missing from the monthly and annual anomalies and their baselines.

## Data attribution

Weather data by [Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0). ERA5 reanalysis by the Copernicus Climate Change Service.
