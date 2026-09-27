import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from weather_etl.config import City
from weather_etl.openmeteo import HOURLY_VARS, FetchResult


def _default_temp(ts: datetime) -> float:
    return round(10 + 10 * math.sin(2 * math.pi * ts.timetuple().tm_yday / 365.25), 2)


def build_payload(start: datetime, hours: int, temp=_default_temp, **overrides) -> str:
    """Open-Meteo-shaped hourly payload with plausible constant values."""
    times = [start + timedelta(hours=i) for i in range(hours)]
    hourly = {"time": [t.strftime("%Y-%m-%dT%H:%M") for t in times]}
    for v in HOURLY_VARS:
        hourly[v] = [0.0] * hours
    hourly["temperature_2m"] = [temp(t) for t in times]
    hourly["apparent_temperature"] = list(hourly["temperature_2m"])
    hourly["relative_humidity_2m"] = [60.0] * hours
    hourly["surface_pressure"] = [1013.0] * hours
    hourly["cloud_cover"] = [50.0] * hours
    hourly["wind_speed_10m"] = [10.0] * hours
    hourly["wind_gusts_10m"] = [20.0] * hours
    hourly.update(overrides)
    return json.dumps({"latitude": 0.0, "longitude": 0.0, "timezone": "GMT", "hourly": hourly})


class FakeClient:
    """Stands in for ArchiveClient: returns a synthetic payload covering the requested window."""

    def __init__(self, temp=_default_temp):
        self.calls = []
        self._temp = temp

    def fetch_hourly(self, lat, lon, start, end):
        hours = ((end - start).days + 1) * 24
        begin = datetime(start.year, start.month, start.day)
        self.calls.append((lat, lon, start, end))
        return FetchResult(
            url=f"fake://{lat},{lon}/{start}/{end}",
            payload=build_payload(begin, hours, self._temp),
            fetched_at=datetime.now(UTC) + timedelta(microseconds=len(self.calls)),
            weight=1.0,
        )


@pytest.fixture
def make_payload():
    return build_payload


@pytest.fixture
def fake_client():
    return FakeClient()


@pytest.fixture
def lake(tmp_path) -> Path:
    p = tmp_path / "lakehouse"
    p.mkdir()
    return p


@pytest.fixture
def cities() -> list[City]:
    return [
        City(
            id="vienna",
            name="Vienna",
            country="AT",
            lat=48.21,
            lon=16.37,
            tz="Europe/Vienna",
            climate="temperate",
            demo=True,
        ),
        City(
            id="sydney",
            name="Sydney",
            country="AU",
            lat=-33.87,
            lon=151.21,
            tz="Australia/Sydney",
            climate="temperate",
        ),
    ]
