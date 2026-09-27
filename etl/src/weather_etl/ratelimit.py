"""Rolling weighted-call budget in SQLite, shared by all ingest processes on one host."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from pathlib import Path

from weather_etl.openmeteo import QuotaExceeded

# Stays below Open-Meteo free tier (600/min, 5,000/h, 10,000/day).
LIMITS: tuple[tuple[str, int, float], ...] = (
    ("minute", 60, 550.0),
    ("hour", 3600, 4500.0),
    ("day", 86400, 9500.0),
)


class CallBudget:
    def __init__(
        self,
        db_path: Path,
        limits: tuple[tuple[str, int, float], ...] = LIMITS,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        max_wait_s: float = 120.0,
    ):
        self._db = Path(db_path)
        self._db.parent.mkdir(parents=True, exist_ok=True)
        self._limits = limits
        self._horizon = max(window for _, window, _ in limits)
        self._clock = clock
        self._sleep = sleep
        self._max_wait_s = max_wait_s
        con = self._connect()
        try:
            con.execute("CREATE TABLE IF NOT EXISTS calls (ts REAL NOT NULL, weight REAL NOT NULL)")
        finally:
            con.close()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db, timeout=30, isolation_level=None)

    def acquire(self, weight: float) -> None:
        smallest = min(limit for _, _, limit in self._limits)
        if weight > smallest:
            raise ValueError(f"weight {weight} exceeds the smallest window limit {smallest}")
        waited = 0.0
        while True:
            wait = self._try_acquire(weight)
            if wait is None:
                return
            if waited + wait > self._max_wait_s:
                raise QuotaExceeded("minute", "local budget wait too long")
            self._sleep(wait)
            waited += wait

    def _try_acquire(self, weight: float) -> float | None:
        """Record the call and return None, or return seconds to wait (minute window only)."""
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            now = self._clock()
            con.execute("DELETE FROM calls WHERE ts <= ?", (now - self._horizon,))
            for scope, window, limit in self._limits:
                (used,) = con.execute(
                    "SELECT COALESCE(SUM(weight), 0) FROM calls WHERE ts > ?", (now - window,)
                ).fetchone()
                if used + weight <= limit:
                    continue
                if scope != "minute":
                    con.execute("ROLLBACK")
                    raise QuotaExceeded(scope, "local call budget")
                (oldest,) = con.execute(
                    "SELECT MIN(ts) FROM calls WHERE ts > ?", (now - window,)
                ).fetchone()
                con.execute("ROLLBACK")
                return max(0.5, oldest + window - now)
            con.execute("INSERT INTO calls (ts, weight) VALUES (?, ?)", (now, weight))
            con.execute("COMMIT")
            return None
        finally:
            con.close()
