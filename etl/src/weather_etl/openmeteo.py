"""Open-Meteo historical (ERA5) archive client with retry and quota handling."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Protocol

import httpx

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS: tuple[str, ...] = (
    "temperature_2m",
    "apparent_temperature",
    "dew_point_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "surface_pressure",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
)


class OpenMeteoError(RuntimeError):
    """Non-retryable API failure, or retries exhausted."""


class QuotaExceeded(RuntimeError):
    """A free-tier (or local budget) limit was hit; `scope` is minute, hour or day."""

    def __init__(self, scope: str, reason: str = ""):
        super().__init__(f"Open-Meteo {scope} quota exceeded" + (f": {reason}" if reason else ""))
        self.scope = scope


class Budget(Protocol):
    def acquire(self, weight: float) -> None: ...


@dataclass(frozen=True)
class FetchResult:
    url: str
    payload: str
    fetched_at: datetime
    weight: float


def call_weight(n_vars: int, start: date, end: date) -> float:
    days = (end - start).days + 1
    if days < 1:
        raise ValueError(f"end {end} is before start {start}")
    return max(1.0, n_vars / 10) * max(1.0, days / 14)


def _reason(resp: httpx.Response) -> str:
    try:
        return str(resp.json().get("reason", ""))
    except ValueError:
        return resp.text[:200]


class ArchiveClient:
    def __init__(
        self,
        http: httpx.Client | None = None,
        budget: Budget | None = None,
        max_attempts: int = 3,
        backoff_s: float = 20.0,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._owns_http = http is None
        self._http = http or httpx.Client(timeout=120)
        self._budget = budget
        self._max_attempts = max_attempts
        self._backoff_s = backoff_s
        self._sleep = sleep

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> ArchiveClient:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def fetch_hourly(self, lat: float, lon: float, start: date, end: date) -> FetchResult:
        weight = call_weight(len(HOURLY_VARS), start, end)
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "hourly": ",".join(HOURLY_VARS),
            "models": "era5",
            "timezone": "UTC",
        }
        last = "no attempt made"
        for attempt in range(1, self._max_attempts + 1):
            if self._budget is not None:
                self._budget.acquire(weight)
            try:
                resp = self._http.get(ARCHIVE_URL, params=params)
            except httpx.TransportError as e:
                last = f"transport error: {e}"
            else:
                if resp.status_code == 200:
                    return FetchResult(str(resp.url), resp.text, datetime.now(UTC), weight)
                reason = _reason(resp)
                last = f"HTTP {resp.status_code} {reason}"
                if resp.status_code == 429:
                    low = reason.lower()
                    if "daily" in low:
                        raise QuotaExceeded("day", reason)
                    if "hourly" in low:
                        raise QuotaExceeded("hour", reason)
                    if attempt == self._max_attempts:
                        # The minute limit (or an unnamed one) outlasted the backoff:
                        # a quota Airflow reschedules, not an error that spends retries.
                        raise QuotaExceeded("minute", reason)
                elif resp.status_code < 500:
                    raise OpenMeteoError(f"HTTP {resp.status_code}: {reason}")
            if attempt < self._max_attempts:
                self._sleep(self._backoff_s * 2 ** (attempt - 1))
        raise OpenMeteoError(f"gave up after {self._max_attempts} attempts: {last}")
