"""Orchestrator-agnostic entry points; Airflow DAG tasks are thin wrappers around these."""

from __future__ import annotations

import contextlib
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

from weather_etl.config import City, Settings, active_cities, load_cities, load_settings
from weather_etl.ingest import ingest_city_year, ingest_recent, plan_backfill, plan_previous_year
from weather_etl.openmeteo import ArchiveClient
from weather_etl.ops import completed, read_ingest_log
from weather_etl.ratelimit import CallBudget

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def today_utc() -> date:
    return datetime.now(UTC).date()


def _context() -> tuple[Settings, list[City]]:
    settings = load_settings()
    return settings, active_cities(load_cities(settings.cities_file), settings.demo)


def _client(settings: Settings) -> ArchiveClient:
    return ArchiveClient(budget=CallBudget(settings.lakehouse / "_state" / "ratelimit.sqlite"))


def backfill_plan(today: date | None = None) -> list[dict]:
    settings, cities = _context()
    return plan_backfill(
        cities, settings.first_year, today or today_utc(), completed(settings.lakehouse)
    )


def previous_year_plan(today: date | None = None) -> list[dict]:
    settings, cities = _context()
    return plan_previous_year(cities, today or today_utc(), completed(settings.lakehouse))


def ingest_one(city_id: str, year: int, today: date | None = None, client=None) -> dict:
    settings, cities = _context()
    city = next((c for c in cities if c.id == city_id), None)
    if city is None:
        raise ValueError(f"unknown or inactive city_id {city_id!r}")
    with contextlib.nullcontext(client) if client else _client(settings) as c:
        row = ingest_city_year(city, year, settings.lakehouse, c, today or today_utc())
    return {**row, "fetched_at": row["fetched_at"].isoformat()}


def backfill_started() -> bool:
    """False until any city-year has been ingested, so daily_refresh stays idle on a fresh stack."""
    settings, _ = _context()
    return not read_ingest_log(settings.lakehouse).empty


def ingest_recent_all(today: date | None = None, client=None, days: int = 14) -> int:
    """Every city's trailing window. A city already fetched today is skipped: after a quota
    reschedule the task starts from the first city again."""
    settings, cities = _context()
    today = today or today_utc()
    log = read_ingest_log(settings.lakehouse)
    # The row ingest_recent itself bumps: the year of yesterday. A fetch of another year
    # today (closing last year, a backfill) does not cover the trailing window.
    year = (today - timedelta(days=1)).year
    done = (
        set(log.loc[(log["fetched_at"].dt.date == today) & (log["year"] == year), "city_id"])
        if not log.empty
        else set()
    )
    with contextlib.nullcontext(client) if client else _client(settings) as c:
        for city in cities:
            if city.id not in done:
                ingest_recent(city, settings.lakehouse, c, today, days)
    return len(cities)


def refine(today: date | None = None, spark: SparkSession | None = None) -> dict[str, int]:
    from weather_etl.bronze import load_landing
    from weather_etl.dq import check_gold, check_silver_table, raise_on_hard_failures, write_results
    from weather_etl.gold import GOLD_TABLES, build_gold
    from weather_etl.ops import write_pipeline_stats
    from weather_etl.paths import table_path
    from weather_etl.silver import SILVER, build_silver, silver_exists
    from weather_etl.spark import get_session, vacuum

    settings = load_settings()
    configured = load_cities(settings.cities_file)
    cities = active_cities(configured, settings.demo)
    root, today, run_id = settings.lakehouse, today or today_utc(), uuid4().hex
    owns_session = spark is None
    spark = spark or get_session("weather-refine")
    try:
        counts = {"bronze_loaded": load_landing(spark, root)}
        # All configured cities: bronze from an earlier full run must not fail a DEMO refine.
        counts["silver_merged"] = build_silver(spark, root, configured, run_id)
        if silver_exists(spark, root):
            write_results(
                root,
                run_id,
                check_silver_table(
                    spark, root, today, {c.id for c in cities}, first_year=settings.first_year
                ),
            )

            def check(gold_counts: dict[str, int]) -> None:
                results = check_gold(gold_counts)
                write_results(root, run_id, results)
                raise_on_hard_failures(results)  # before any gold table is overwritten

            gold = build_gold(
                spark,
                root,
                settings.baseline_start,
                settings.baseline_end,
                check=check,
                first_year=settings.first_year,
            )
            counts.update({f"gold_{name}": n for name, n in gold.items()})
            write_pipeline_stats(spark, root)
            for layer, name in [SILVER, *(("gold", n) for n in GOLD_TABLES)]:
                vacuum(spark, table_path(root, layer, name))
        return counts
    finally:
        if owns_session:
            spark.stop()


def reschedule_at(scope: str, now: datetime) -> datetime:
    if scope == "day":
        return (now + timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)
    if scope == "hour":
        return now + timedelta(minutes=61)
    return now + timedelta(minutes=2)
