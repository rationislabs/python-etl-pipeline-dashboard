"""City list and runtime settings."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

Climate = Literal["tropical", "arid", "temperate", "continental", "polar edge"]


class City(BaseModel, frozen=True):
    id: str = Field(pattern=r"^[a-z0-9_]+$")
    name: str
    country: str
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    tz: str
    climate: Climate
    demo: bool = False

    @field_validator("tz")
    @classmethod
    def _valid_tz(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except (ZoneInfoNotFoundError, ValueError) as e:
            raise ValueError(f"unknown IANA time zone: {v!r}") from e
        return v


class Settings(BaseModel, frozen=True):
    lakehouse: Path
    cities_file: Path
    demo: bool
    baseline_start: int
    baseline_end: int
    first_year: int

    @model_validator(mode="after")
    def _check_years(self) -> Settings:
        if self.baseline_start > self.baseline_end:
            raise ValueError("BASELINE_START must be <= BASELINE_END")
        if self.first_year > self.baseline_start:
            raise ValueError("FIRST_YEAR must be <= BASELINE_START so the baseline gets ingested")
        return self


def load_cities(path: Path) -> list[City]:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    cities = [City(**c) for c in raw.get("cities") or []]
    if not cities:
        raise ValueError(f"{path}: no cities defined")
    ids = [c.id for c in cities]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        raise ValueError(f"{path}: duplicate city ids: {', '.join(dupes)}")
    return cities


def active_cities(cities: list[City], demo: bool) -> list[City]:
    if not demo:
        return list(cities)
    chosen = [c for c in cities if c.demo]
    if not chosen:
        raise ValueError("DEMO=1 but no city has demo: true")
    return chosen


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    env = os.environ if env is None else env
    demo = env.get("DEMO", "0") == "1"
    baseline_start = int(env.get("BASELINE_START", "1991"))
    return Settings(
        lakehouse=Path(env.get("LAKEHOUSE", "lakehouse")),
        cities_file=Path(env.get("CITIES_FILE", "config/cities.yaml")),
        demo=demo,
        baseline_start=baseline_start,
        baseline_end=int(env.get("BASELINE_END", "2020")),
        first_year=int(env.get("FIRST_YEAR") or (baseline_start if demo else 1980)),
    )
