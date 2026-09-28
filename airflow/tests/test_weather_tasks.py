from datetime import UTC, datetime

import pytest
import weather_tasks
from airflow.sdk.exceptions import AirflowRescheduleException
from weather_etl import pipeline
from weather_etl.openmeteo import QuotaExceeded


def test_daily_quota_becomes_reschedule_after_midnight(monkeypatch):
    def boom(city_id, year):
        raise QuotaExceeded("day", "Daily API request limit exceeded")

    monkeypatch.setattr(pipeline, "ingest_one", boom)
    with pytest.raises(AirflowRescheduleException) as exc:
        weather_tasks.ingest_city_year("vienna", 2020)
    when = exc.value.reschedule_date
    assert (when.hour, when.minute) == (0, 5)
    assert when > datetime.now(UTC)


def test_recent_hourly_quota_becomes_reschedule(monkeypatch):
    def boom():
        raise QuotaExceeded("hour")

    monkeypatch.setattr(pipeline, "ingest_recent_all", boom)
    with pytest.raises(AirflowRescheduleException):
        weather_tasks.ingest_recent()


def test_success_passes_through(monkeypatch):
    monkeypatch.setattr(
        pipeline, "ingest_one", lambda city_id, year: {"city_id": city_id, "year": year}
    )
    assert weather_tasks.ingest_city_year("vienna", 2020) == {"city_id": "vienna", "year": 2020}
