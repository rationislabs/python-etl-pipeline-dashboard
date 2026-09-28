import importlib.util
import os
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "tasks", Path(__file__).resolve().parents[1] / "tasks.py"
)
tasks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tasks)


def test_compose_env_passes_host_uid():
    env = tasks.compose_env(DEMO="1")
    assert env["LOCAL_UID"] == str(os.getuid())
    assert env["DEMO"] == "1"


def test_e2e_env_is_isolated():
    env = tasks.e2e_env()
    assert env["LAKEHOUSE_DIR"] == "./lakehouse-e2e"
    assert env["CITIES_FILE"] == "/app/config/cities.e2e.yaml"
    assert (env["FIRST_YEAR"], env["BASELINE_START"], env["BASELINE_END"]) == (
        "2024",
        "2024",
        "2025",
    )


def test_ensure_lakehouse_creates_dir_and_gitkeep(tmp_path):
    lake = tmp_path / "lakehouse"
    tasks.ensure_lakehouse(lake)
    assert lake.is_dir()
    assert (lake / ".gitkeep").is_file()


def test_ensure_lakehouse_is_idempotent_with_existing_content(tmp_path):
    lake = tmp_path / "lakehouse"
    lake.mkdir()
    (lake / "bronze").mkdir()
    tasks.ensure_lakehouse(lake)
    assert (lake / "bronze").is_dir()
    assert (lake / ".gitkeep").is_file()


GRAPHVIZ_WARNING = (
    "Run `pip install graphviz` to attempt to install it. [py.warnings] "
    "category=UserWarning filename=/usr/local/lib/python3.12/site-packages/"
    "airflow/cli/commands/dag_command.py lineno=57"
)


def test_latest_run_state_skips_graphviz_warning_before_json():
    out = (
        GRAPHVIZ_WARNING
        + "\n"
        + '[{"dag_id": "backfill_era5", "run_id": "r1", "state": "running", '
        + '"start_date": "2026-09-28T13:00:51"}]'
    )
    assert tasks.latest_run_state(out) == "running"


def test_latest_run_state_picks_the_latest_start_date():
    out = (
        '[{"dag_id": "backfill_era5", "run_id": "r1", "state": "success", '
        '"start_date": "2026-09-28T10:00:00"}, '
        '{"dag_id": "backfill_era5", "run_id": "r2", "state": "running", '
        '"start_date": "2026-09-28T13:00:51"}]'
    )
    assert tasks.latest_run_state(out) == "running"


def test_latest_run_state_returns_none_without_json():
    assert tasks.latest_run_state(GRAPHVIZ_WARNING) is None


def test_latest_run_state_returns_none_for_empty_list():
    assert tasks.latest_run_state("[]") is None
