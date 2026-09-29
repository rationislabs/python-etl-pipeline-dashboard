"""Read gold/ops Delta tables with delta-rs (no Spark) and small env-derived helpers."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import UTC, date, datetime
from functools import lru_cache
from pathlib import Path

import pandas as pd
import yaml
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

CITY_COLUMNS = ["id", "name", "country", "lat", "lon", "tz", "climate", "demo"]


class Lakehouse:
    def __init__(self, root: Path):
        self.root = Path(root)
        self._cache: dict[str, tuple[int, pd.DataFrame]] = {}

    def table(self, layer: str, name: str) -> pd.DataFrame:
        path = str(self.root / layer / name)
        try:
            dt = DeltaTable(path)
        except TableNotFoundError:
            return pd.DataFrame()
        version = dt.version()
        hit = self._cache.get(path)
        if hit and hit[0] == version:
            return hit[1]
        df = dt.to_pandas()
        self._cache[path] = (version, df)
        return df


def load_cities(path: Path) -> pd.DataFrame:
    if not Path(path).exists():
        return pd.DataFrame(columns=CITY_COLUMNS)
    rows = (yaml.safe_load(Path(path).read_text()) or {}).get("cities") or []
    df = pd.DataFrame(rows, columns=CITY_COLUMNS)
    df["demo"] = df["demo"].fillna(False).astype(bool)
    return df


@lru_cache(maxsize=1)
def get_lakehouse() -> Lakehouse:
    return Lakehouse(Path(os.environ.get("LAKEHOUSE", "lakehouse")))


@lru_cache(maxsize=1)
def get_cities() -> pd.DataFrame:
    """Active cities, mirroring weather_etl.config.active_cities: DEMO=1 keeps demo rows."""
    cities = load_cities(Path(os.environ.get("CITIES_FILE", "config/cities.yaml")))
    if os.environ.get("DEMO") == "1":
        cities = cities[cities["demo"].astype(bool)].reset_index(drop=True)
    return cities


def first_year(env: Mapping[str, str] | None = None) -> int:
    env = os.environ if env is None else env
    if env.get("FIRST_YEAR"):
        return int(env["FIRST_YEAR"])
    if env.get("DEMO") == "1":
        return int(env.get("BASELINE_START", "1991"))
    return 1980


def last_complete_year(today: date | None = None) -> int:
    return (today or datetime.now(UTC).date()).year - 1


def baseline_label(env: Mapping[str, str] | None = None) -> str:
    env = os.environ if env is None else env
    return f"{env.get('BASELINE_START', '1991')}–{env.get('BASELINE_END', '2020')}"


def backfill_progress(log: pd.DataFrame, n_cities: int, first: int, last_complete: int) -> float:
    total = n_cities * max(0, last_complete - first + 1)
    if total == 0:
        return 100.0
    done = int((log["status"] == "complete").sum()) if not log.empty else 0
    return min(100.0, 100.0 * done / total)


def empty_message(lake: Lakehouse, cities: pd.DataFrame) -> str:
    log = lake.table("ops", "ingest_log")
    if log.empty:
        return "No data yet. Run `inv demo`, or trigger backfill_era5 in Airflow."
    pct = backfill_progress(log, len(cities), first_year(), last_complete_year())
    return f"Backfill in progress: {pct:.0f}% complete"
