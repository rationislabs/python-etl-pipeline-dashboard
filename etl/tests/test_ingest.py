import json
from datetime import UTC, date, datetime

from weather_etl.config import City
from weather_etl.ingest import (
    ingest_city_year,
    ingest_recent,
    plan_backfill,
    plan_previous_year,
    year_window,
)
from weather_etl.landing import count_hours, landing_dir, write_landing
from weather_etl.openmeteo import FetchResult
from weather_etl.ops import classify, completed, expected_hours, read_ingest_log, upsert_ingest_log

TODAY = date(2026, 9, 27)


def test_expected_hours_handles_leap_years():
    assert expected_hours(2023) == 8760
    assert expected_hours(2024) == 8784
    assert expected_hours(1900) == 8760
    assert expected_hours(2000) == 8784


def test_classify():
    assert classify(2023, 8760, TODAY) == "complete"
    assert classify(2023, 8000, TODAY) == "partial"
    assert classify(2026, 6000, TODAY) == "partial"  # current year never complete


def test_year_window():
    # From Dec 31 of the year before: a city east of UTC starts its local Jan 1 there.
    assert year_window(2023, TODAY) == (date(2022, 12, 31), date(2023, 12, 31))
    assert year_window(2026, TODAY) == (date(2025, 12, 31), date(2026, 9, 26))
    assert year_window(2026, date(2026, 1, 1)) is None


def test_hours_are_counted_within_the_year_only():
    payload = json.dumps(
        {"hourly": {"time": ["2022-12-31T23:00", "2023-01-01T00:00"], "temperature_2m": [1.0, 2.0]}}
    )
    assert count_hours(payload, 2023) == 1


def test_plan_backfill_skips_done_and_empty_current_year(cities):
    done = {("vienna", 2024)}
    plan = plan_backfill(cities[:1], 2024, date(2026, 1, 1), done)
    assert plan == [{"city_id": "vienna", "year": 2025}]


def test_plan_includes_newly_added_city(cities):
    done = {("vienna", y) for y in (2024, 2025)}
    plan = plan_backfill(cities, 2024, TODAY, done)
    assert {"city_id": "sydney", "year": 2024} in plan
    assert {"city_id": "vienna", "year": 2026} in plan
    assert not [p for p in plan if p["city_id"] == "vienna" and p["year"] < 2026]


def test_plan_previous_year(cities):
    assert plan_previous_year(cities, TODAY, {("vienna", 2025)}) == [
        {"city_id": "sydney", "year": 2025}
    ]


def test_count_hours_ignores_nulls(make_payload):
    payload = json.loads(make_payload(datetime(2023, 1, 1), 5))
    payload["hourly"]["temperature_2m"][2] = None
    assert count_hours(json.dumps(payload)) == 4


def test_write_landing_is_single_line_json(lake, make_payload):
    res = FetchResult(
        "u", make_payload(datetime(2023, 1, 1), 3), datetime(2026, 1, 1, tzinfo=UTC), 1.0
    )
    path = write_landing(lake, "vienna", 2023, res)
    assert path.parent == landing_dir(lake) / "vienna"
    text = path.read_text()
    assert "\n" not in text
    rec = json.loads(text)
    assert rec["city_id"] == "vienna" and rec["year"] == 2023 and len(rec["payload_sha256"]) == 64
    assert not list(path.parent.glob("*.tmp"))


def test_ingest_full_past_year_is_complete(lake, cities, fake_client):
    row = ingest_city_year(cities[0], 2023, lake, fake_client, TODAY)
    assert (row["status"], row["rows"]) == ("complete", 8760)
    assert fake_client.calls == [(48.21, 16.37, date(2022, 12, 31), date(2023, 12, 31))]
    assert completed(lake) == {("vienna", 2023)}
    assert len(list(landing_dir(lake).rglob("*.json"))) == 1


def test_ingest_current_year_is_partial_and_rerun_upserts(lake, cities, fake_client):
    ingest_city_year(cities[0], 2026, lake, fake_client, TODAY)
    ingest_city_year(cities[0], 2026, lake, fake_client, TODAY)
    log = read_ingest_log(lake)
    assert len(log) == 1
    assert log.iloc[0]["status"] == "partial"


def test_upsert_changes_status(lake):
    now = datetime(2026, 1, 1, tzinfo=UTC)
    upsert_ingest_log(
        lake,
        [{"city_id": "vienna", "year": 2023, "status": "partial", "rows": 5, "fetched_at": now}],
    )
    upsert_ingest_log(
        lake,
        [
            {
                "city_id": "vienna",
                "year": 2023,
                "status": "complete",
                "rows": 8760,
                "fetched_at": now,
            }
        ],
    )
    assert completed(lake) == {("vienna", 2023)}


def test_read_ingest_log_empty(lake):
    log = read_ingest_log(lake)
    assert log.empty and list(log.columns) == ["city_id", "year", "status", "rows", "fetched_at"]


def test_ingest_recent_fetches_trailing_window(lake, fake_client):
    city = City(id="x", name="X", country="X", lat=1, lon=2, tz="UTC", climate="arid")
    path = ingest_recent(city, lake, fake_client, date(2024, 1, 10))
    assert fake_client.calls == [(1, 2, date(2023, 12, 27), date(2024, 1, 9))]
    assert path.exists()
    assert read_ingest_log(lake).empty


def test_ingest_recent_refetches_gap_after_downtime(lake, fake_client):
    city = City(id="x", name="X", country="X", lat=1, lon=2, tz="UTC", climate="arid")
    last = datetime(2024, 5, 1, 6, tzinfo=UTC)  # 40 days before today
    row = {"city_id": "x", "year": 2024, "status": "partial", "rows": 2900, "fetched_at": last}
    upsert_ingest_log(lake, [row])
    ingest_recent(city, lake, fake_client, date(2024, 6, 10))
    assert fake_client.calls == [(1, 2, date(2024, 4, 24), date(2024, 6, 9))]
    log = read_ingest_log(lake).iloc[0]
    assert (log["status"], log["rows"]) == ("partial", 2900)
    assert log["fetched_at"] > last

    ingest_recent(city, lake, fake_client, date(2024, 6, 11))  # back to the normal window
    assert fake_client.calls[-1] == (1, 2, date(2024, 5, 28), date(2024, 6, 10))


def test_ingest_recent_never_reaches_before_jan_1(lake, fake_client):
    city = City(id="x", name="X", country="X", lat=1, lon=2, tz="UTC", climate="arid")
    last = datetime(2024, 1, 3, tzinfo=UTC)
    row = {"city_id": "x", "year": 2024, "status": "partial", "rows": 48, "fetched_at": last}
    upsert_ingest_log(lake, [row])
    ingest_recent(city, lake, fake_client, date(2024, 1, 20))
    assert fake_client.calls == [(1, 2, date(2024, 1, 1), date(2024, 1, 19))]
