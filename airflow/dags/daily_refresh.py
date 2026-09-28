"""Daily: re-fetch the trailing 14 days (revisions overwrite via MERGE), close last year, refine."""

from datetime import timedelta

import pendulum
import weather_tasks
from airflow.sdk import dag, task
from weather_etl import pipeline


@dag(
    dag_id="daily_refresh",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["weather"],
    doc_md="Trailing 14-day refresh per city, previous-year completion, then refine.",
)
def daily_refresh():
    @task.short_circuit
    def backfill_started() -> bool:
        return pipeline.backfill_started()

    @task
    def plan_previous_year() -> list[dict]:
        return pipeline.previous_year_plan()

    @task(pool="open_meteo", retries=3, retry_delay=timedelta(minutes=5), max_active_tis_per_dag=1)
    def ingest(city_id: str, year: int) -> dict:
        return weather_tasks.ingest_city_year(city_id, year)

    @task(pool="open_meteo", retries=3, retry_delay=timedelta(minutes=5))
    def ingest_recent() -> int:
        return weather_tasks.ingest_recent()

    @task(pool="spark_jobs", trigger_rule="all_done")
    def refine() -> dict:
        return pipeline.refine()

    started = backfill_started()
    plan = plan_previous_year()
    recent = ingest_recent()
    started >> plan
    started >> recent
    [ingest.expand_kwargs(plan), recent] >> refine()


daily_refresh()
