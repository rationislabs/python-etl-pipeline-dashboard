"""Developer tasks. List them with `uv run inv --list`."""

from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

from invoke import task

ROOT = Path(__file__).parent
E2E_PROJECT = "weather-e2e"
GOLD = (
    "daily_city",
    "climatology_doy",
    "anomaly_monthly",
    "anomaly_annual",
    "trend_city",
    "extremes_annual",
    "distribution_decade",
)
PAGES = ("/", "/warming", "/seasonality", "/extremes", "/pipeline")


def compose_env(**extra: str) -> dict[str, str]:
    env = {"LOCAL_UID": str(os.getuid())}
    env.update(extra)
    return env


def e2e_env() -> dict[str, str]:
    return compose_env(
        LAKEHOUSE_DIR="./lakehouse-e2e",
        CITIES_FILE="/app/config/cities.e2e.yaml",
        FIRST_YEAR="2024",
        BASELINE_START="2024",
        BASELINE_END="2025",
    )


def ensure_lakehouse(path: Path) -> None:
    """Create the bind-mounted lakehouse dir as the host user (with .gitkeep) before
    `docker compose up`, otherwise Docker creates it root-owned on a fresh clone."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    (path / ".gitkeep").touch()


def _compose(project: str | None = None) -> str:
    return f"docker compose -p {project}" if project else "docker compose"


def _trigger(c, env, project=None):
    c.run(
        f"{_compose(project)} exec -T airflow-scheduler airflow dags trigger backfill_era5", env=env
    )


def latest_run_state(out: str) -> str | None:
    """Pick the state of the most recently started run out of `airflow dags
    list-runs -o json` output. The CLI can print warnings (e.g. a missing
    graphviz warning containing a literal "[") to stdout before the JSON
    array, so scan for a "[" that actually starts a decodable JSON list of
    run dicts rather than trusting the first "[" in the text."""
    decoder = json.JSONDecoder()
    runs = None
    for i, ch in enumerate(out):
        if ch != "[":
            continue
        try:
            value, _ = decoder.raw_decode(out, i)
        except ValueError:
            continue
        if isinstance(value, list) and all(isinstance(r, dict) and "state" in r for r in value):
            runs = value
            break
    if not runs:
        return None
    latest = max(runs, key=lambda r: r.get("start_date") or "")
    return latest["state"]


def _wait_for_run(c, env, dag_id, timeout_s, project=None) -> str:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        out = c.run(
            f"{_compose(project)} exec -T airflow-scheduler airflow dags list-runs {dag_id} -o json",
            env=env,
            hide=True,
            warn=True,
        ).stdout
        state = latest_run_state(out)
        if state in ("success", "failed"):
            return state
        time.sleep(20)
    raise TimeoutError(f"{dag_id} did not finish within {timeout_s}s")


@task
def up(c):
    """Start the full stack (no backfill)."""
    ensure_lakehouse(ROOT / "lakehouse")
    c.run("docker compose up -d --build --wait", env=compose_env())


@task
def demo(c):
    """DEMO=1: 3 cities, baseline years to now; triggers the backfill."""
    ensure_lakehouse(ROOT / "lakehouse")
    env = compose_env(DEMO="1")
    c.run("docker compose up -d --build --wait", env=env)
    _trigger(c, env)
    print(
        f"Airflow http://localhost:{os.environ.get('AIRFLOW_PORT', '8080')}"
        f" · Spark http://localhost:{os.environ.get('SPARK_UI_PORT', '8081')}"
        f" · Dashboard http://localhost:{os.environ.get('DASH_PORT', '8050')}"
    )


@task
def backfill(c):
    """Trigger backfill_era5 on the running stack."""
    _trigger(c, compose_env())


@task
def down(c):
    c.run("docker compose down", env=compose_env())


@task
def reset(c):
    """Stop the stack and delete ./lakehouse contents and the Airflow database volume."""
    c.run("docker compose down -v", env=compose_env())
    shutil.rmtree(ROOT / "lakehouse", ignore_errors=True)
    ensure_lakehouse(ROOT / "lakehouse")


@task
def lint(c):
    c.run("uv run ruff check .")
    c.run("uv run ruff format --check .")


@task
def test(c, images=False):
    """Unit + Spark tests; --images also builds images and runs the DAG tests inside Airflow."""
    c.run("uv run pytest")
    if images:
        ensure_lakehouse(ROOT / "lakehouse")
        c.run("docker compose build", env=compose_env())
        c.run(
            "docker compose run --rm --no-deps -T airflow-scheduler pytest -q /opt/airflow/tests",
            env=compose_env(),
        )


@task
def e2e(c, timeout=1800):
    """Real API, isolated project: 1 city × 2024-now. Stop the main stack first (ports)."""
    import httpx
    from deltalake import DeltaTable

    env = e2e_env()
    lake = ROOT / "lakehouse-e2e"
    c.run(f"{_compose(E2E_PROJECT)} down -v", env=env, warn=True)
    shutil.rmtree(lake, ignore_errors=True)
    try:
        ensure_lakehouse(lake)
        c.run(f"{_compose(E2E_PROJECT)} up -d --build --wait", env=env)
        _trigger(c, env, E2E_PROJECT)
        state = _wait_for_run(c, env, "backfill_era5", timeout, E2E_PROJECT)
        assert state == "success", f"backfill_era5 finished {state}"
        for name in GOLD:
            n = len(DeltaTable(str(lake / "gold" / name)).to_pandas())
            assert n > 0, f"gold.{name} is empty"
            print(f"gold.{name}: {n} rows")
        for path in PAGES:
            assert httpx.get(f"http://localhost:8050{path}", timeout=30).status_code == 200
        print("E2E passed")
    finally:
        c.run(f"{_compose(E2E_PROJECT)} down -v", env=env, warn=True)
        shutil.rmtree(lake, ignore_errors=True)
