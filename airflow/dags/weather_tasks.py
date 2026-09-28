"""Airflow-facing wrappers: a quota error pauses the task (reschedule) instead of failing it."""

from datetime import UTC, datetime

from airflow.sdk.exceptions import AirflowRescheduleException
from weather_etl import pipeline
from weather_etl.openmeteo import QuotaExceeded


def _reschedule(err: QuotaExceeded) -> AirflowRescheduleException:
    return AirflowRescheduleException(
        reschedule_date=pipeline.reschedule_at(err.scope, datetime.now(UTC))
    )


def ingest_city_year(city_id: str, year: int) -> dict:
    try:
        return pipeline.ingest_one(city_id, year)
    except QuotaExceeded as e:
        raise _reschedule(e) from e


def ingest_recent() -> int:
    try:
        return pipeline.ingest_recent_all()
    except QuotaExceeded as e:
        raise _reschedule(e) from e
