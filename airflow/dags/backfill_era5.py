"""One-off ERA5 backfill: one mapped task per (city, year), throttled by the open_meteo pool."""

from datetime import timedelta

import pendulum
import weather_tasks
from airflow.sdk import dag, task
from weather_etl import pipeline


@dag(
    dag_id="backfill_era5",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["weather"],
    doc_md="Fetch every missing city-year (quota-aware, resumable), then refine bronze → silver → gold.",
)
def backfill_era5():
    @task
    def plan() -> list[dict]:
        return pipeline.backfill_plan()

    @task(pool="open_meteo", retries=3, retry_delay=timedelta(minutes=5), max_active_tis_per_dag=1)
    def ingest(city_id: str, year: int) -> dict:
        return weather_tasks.ingest_city_year(city_id, year)

    @task(pool="spark_jobs", trigger_rule="all_done")
    def refine() -> dict:
        return pipeline.refine()

    ingest.expand_kwargs(plan()) >> refine()


backfill_era5()
