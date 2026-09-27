"""Decide what to fetch and fetch it: no Spark involved."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from weather_etl.config import City
from weather_etl.landing import count_hours, write_landing
from weather_etl.ops import classify, upsert_ingest_log


def year_window(year: int, today: date) -> tuple[date, date] | None:
    start = date(year, 1, 1)
    end = min(date(year, 12, 31), today - timedelta(days=1))
    return (start, end) if end >= start else None


def plan_backfill(
    cities: list[City], first_year: int, today: date, done: set[tuple[str, int]]
) -> list[dict]:
    return [
        {"city_id": c.id, "year": y}
        for c in cities
        for y in range(first_year, today.year + 1)
        if (c.id, y) not in done and year_window(y, today) is not None
    ]


def plan_previous_year(cities: list[City], today: date, done: set[tuple[str, int]]) -> list[dict]:
    year = today.year - 1
    return [{"city_id": c.id, "year": year} for c in cities if (c.id, year) not in done]


def ingest_city_year(city: City, year: int, root: Path, client, today: date) -> dict:
    window = year_window(year, today)
    if window is None:
        raise ValueError(f"nothing to fetch for {year} on {today}")
    result = client.fetch_hourly(city.lat, city.lon, *window)
    write_landing(root, city.id, year, result)
    hours = count_hours(result.payload)
    row = {
        "city_id": city.id,
        "year": year,
        "status": classify(year, hours, today),
        "rows": hours,
        "fetched_at": result.fetched_at,
    }
    upsert_ingest_log(root, [row])
    return row


def ingest_recent(city: City, root: Path, client, today: date, days: int = 14) -> Path:
    start, end = today - timedelta(days=days), today - timedelta(days=1)
    result = client.fetch_hourly(city.lat, city.lon, start, end)
    return write_landing(root, city.id, end.year, result)
