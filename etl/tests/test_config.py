from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError
from weather_etl.config import active_cities, load_cities, load_settings
from weather_etl.paths import table_path

REPO = Path(__file__).resolve().parents[2]

VIENNA = {
    "id": "vienna",
    "name": "Vienna",
    "country": "AT",
    "lat": 48.21,
    "lon": 16.37,
    "tz": "Europe/Vienna",
    "climate": "temperate",
    "demo": True,
}


def write(tmp_path, cities):
    p = tmp_path / "cities.yaml"
    p.write_text(yaml.safe_dump({"cities": cities}))
    return p


def test_shipped_config_is_valid():
    cities = load_cities(REPO / "config" / "cities.yaml")
    assert len(cities) == 20
    assert sorted(c.id for c in cities if c.demo) == ["singapore", "vienna", "yakutsk"]
    assert {c.climate for c in cities} == {
        "tropical",
        "arid",
        "temperate",
        "continental",
        "polar edge",
    }


def test_invalid_timezone_is_rejected(tmp_path):
    with pytest.raises(ValidationError, match="unknown IANA time zone"):
        load_cities(write(tmp_path, [{**VIENNA, "tz": "Europe/Nowhere"}]))


def test_duplicate_ids_are_rejected(tmp_path):
    with pytest.raises(ValueError, match="duplicate city ids: vienna"):
        load_cities(write(tmp_path, [VIENNA, VIENNA]))


def test_latitude_out_of_range_is_rejected(tmp_path):
    with pytest.raises(ValidationError):
        load_cities(write(tmp_path, [{**VIENNA, "lat": 91}]))


def test_empty_file_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="no cities defined"):
        load_cities(write(tmp_path, []))


def test_active_cities_demo_filters(tmp_path):
    cities = load_cities(write(tmp_path, [VIENNA, {**VIENNA, "id": "graz", "demo": False}]))
    assert [c.id for c in active_cities(cities, demo=True)] == ["vienna"]
    assert len(active_cities(cities, demo=False)) == 2


def test_active_cities_demo_without_demo_cities_fails(tmp_path):
    cities = load_cities(write(tmp_path, [{**VIENNA, "demo": False}]))
    with pytest.raises(ValueError, match="no city has demo: true"):
        active_cities(cities, demo=True)


def test_settings_defaults():
    s = load_settings({})
    assert (s.demo, s.baseline_start, s.baseline_end, s.first_year) == (False, 1991, 2020, 1980)
    assert s.lakehouse == Path("lakehouse")
    assert s.cities_file == Path("config/cities.yaml")


def test_settings_demo_starts_at_baseline():
    assert load_settings({"DEMO": "1"}).first_year == 1991


def test_settings_explicit_first_year():
    s = load_settings({"FIRST_YEAR": "2024", "BASELINE_START": "2024", "BASELINE_END": "2025"})
    assert (s.first_year, s.baseline_start, s.baseline_end) == (2024, 2024, 2025)


def test_settings_baseline_order_enforced():
    with pytest.raises(ValidationError, match="BASELINE_START must be <= BASELINE_END"):
        load_settings({"BASELINE_START": "2020", "BASELINE_END": "1991"})


def test_settings_first_year_must_cover_baseline():
    with pytest.raises(ValidationError, match="FIRST_YEAR must be <= BASELINE_START"):
        load_settings({"FIRST_YEAR": "2000"})


def test_settings_empty_first_year_uses_default():
    assert load_settings({"FIRST_YEAR": ""}).first_year == 1980


def test_table_path():
    assert table_path(Path("/lh"), "gold", "daily_city") == "/lh/gold/daily_city"
