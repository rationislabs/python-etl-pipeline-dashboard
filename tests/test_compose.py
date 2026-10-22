from pathlib import Path

import yaml

COMPOSE = yaml.safe_load((Path(__file__).resolve().parents[1] / "docker-compose.yml").read_text())


def test_host_ports_can_be_changed():
    ports = {name: s.get("ports", []) for name, s in COMPOSE["services"].items()}
    # 8088 on the host: 8080 is the port other local apps most often hold.
    assert ports["airflow-apiserver"] == ["127.0.0.1:${AIRFLOW_PORT:-8088}:8080"]
    assert ports["spark-master"] == ["127.0.0.1:${SPARK_UI_PORT:-8081}:8081"]
    assert ports["dash"] == ["127.0.0.1:${DASH_PORT:-8050}:8050"]


def test_airflow_runs_a_triggerer():
    # Airflow's health view lists the triggerer; without one it shows red
    assert COMPOSE["services"]["airflow-triggerer"]["command"] == ["airflow", "triggerer"]


def test_the_jwt_secret_can_be_set():
    env = COMPOSE["services"]["airflow-apiserver"]["environment"]
    assert env["AIRFLOW__API_AUTH__JWT_SECRET"].startswith("${AIRFLOW_JWT_SECRET:-")
