"""Raw API responses land here as one-line JSON files before Spark loads them into bronze."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from weather_etl.openmeteo import FetchResult


def landing_dir(root: Path) -> Path:
    return Path(root) / "landing" / "weather"


def write_landing(root: Path, city_id: str, year: int, result: FetchResult) -> Path:
    sha = hashlib.sha256(result.payload.encode()).hexdigest()
    record = {
        "city_id": city_id,
        "year": year,
        "request_url": result.url,
        "fetched_at": result.fetched_at.isoformat(),
        "payload": result.payload,
        "payload_sha256": sha,
    }
    folder = landing_dir(root) / city_id
    folder.mkdir(parents=True, exist_ok=True)
    final = folder / f"{year}_{result.fetched_at:%Y%m%dT%H%M%S%f}_{sha[:12]}.json"
    tmp = final.with_suffix(".tmp")
    tmp.write_text(json.dumps(record))
    os.replace(tmp, final)
    return final


def count_hours(payload: str, year: int | None = None) -> int:
    """Hours with a value; with `year`, only that year's (a window starts on Dec 31 before)."""
    hourly = json.loads(payload)["hourly"]
    prefix = f"{year}-" if year is not None else ""
    return sum(
        t is not None and ts.startswith(prefix)
        for ts, t in zip(hourly["time"], hourly["temperature_2m"], strict=True)
    )
