from pathlib import Path

import pytest
from airflow.configuration import conf
from airflow.dag_processing.dagbag import DagBag

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def bag():
    return DagBag(dag_folder=str(DAGS))


def test_no_import_errors(bag):
    assert bag.import_errors == {}
    assert set(bag.dags) == {"backfill_era5", "daily_refresh"}


def test_backfill_shape(bag):
    dag = bag.dags["backfill_era5"]
    assert {t.task_id for t in dag.tasks} == {"plan", "ingest", "refine"}
    assert dag.schedule is None and dag.max_active_runs == 1
    ingest, refine = dag.get_task("ingest"), dag.get_task("refine")
    assert ingest.pool == "open_meteo"
    assert refine.pool == "spark_jobs" and refine.trigger_rule == "all_done"
    assert "ingest" in refine.upstream_task_ids
    assert "plan" in ingest.upstream_task_ids


def test_daily_shape(bag):
    dag = bag.dags["daily_refresh"]
    assert {t.task_id for t in dag.tasks} == {
        "backfill_started",
        "plan_previous_year",
        "ingest",
        "ingest_recent",
        "refine",
    }
    assert dag.schedule == "0 6 * * *"
    refine = dag.get_task("refine")
    assert refine.upstream_task_ids == {"ingest", "ingest_recent"}
    assert dag.get_task("ingest_recent").pool == "open_meteo"
    assert dag.get_task("plan_previous_year").upstream_task_ids == {"backfill_started"}
    assert "backfill_started" in dag.get_task("ingest_recent").upstream_task_ids


def test_max_map_length_raised():
    assert conf.getint("core", "max_map_length") >= 4096
