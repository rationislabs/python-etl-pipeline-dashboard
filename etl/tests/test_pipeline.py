from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import yaml
from deltalake import DeltaTable
from weather_etl import pipeline
from weather_etl.ops import read_ingest_log
from weather_etl.paths import table_path

TODAY = date(2024, 1, 10)


@pytest.fixture
def env(monkeypatch, lake, tmp_path):
    cfg = tmp_path / "cities.yaml"
    cfg.write_text(
        yaml.safe_dump(
            {
                "cities": [
                    {
                        "id": "vienna",
                        "name": "Vienna",
                        "country": "AT",
                        "lat": 48.21,
                        "lon": 16.37,
                        "tz": "Europe/Vienna",
                        "climate": "temperate",
                    }
                ]
            }
        )
    )
    for k, v in {
        "LAKEHOUSE": str(lake),
        "CITIES_FILE": str(cfg),
        "DEMO": "0",
        "FIRST_YEAR": "2022",
        "BASELINE_START": "2022",
        "BASELINE_END": "2023",
    }.items():
        monkeypatch.setenv(k, v)
    return lake


def test_reschedule_at():
    now = datetime(2026, 9, 27, 13, 30, tzinfo=UTC)
    assert pipeline.reschedule_at("day", now) == datetime(2026, 9, 28, 0, 5, tzinfo=UTC)
    assert pipeline.reschedule_at("hour", now) == datetime(2026, 9, 27, 14, 31, tzinfo=UTC)
    assert pipeline.reschedule_at("minute", now) == datetime(2026, 9, 27, 13, 32, tzinfo=UTC)


def test_ingest_one_rejects_unknown_city(env, fake_client):
    with pytest.raises(ValueError, match="unknown or inactive city_id 'atlantis'"):
        pipeline.ingest_one("atlantis", 2023, today=TODAY, client=fake_client)


def test_ingest_one_returns_xcom_safe_row(env, fake_client):
    row = pipeline.ingest_one("vienna", 2023, today=TODAY, client=fake_client)
    assert row["status"] == "complete" and isinstance(row["fetched_at"], str)


@pytest.mark.spark
def test_refine_on_empty_lakehouse(spark, env):
    assert pipeline.refine(today=TODAY, spark=spark) == {"bronze_loaded": 0, "silver_merged": 0}


@pytest.mark.spark
def test_backfill_end_to_end_offline(spark, env, fake_client):
    plan = pipeline.backfill_plan(today=TODAY)
    assert plan == [{"city_id": "vienna", "year": y} for y in (2022, 2023, 2024)]
    for item in plan:
        pipeline.ingest_one(**item, today=TODAY, client=fake_client)

    counts = pipeline.refine(today=TODAY, spark=spark)
    assert counts["bronze_loaded"] == 3
    assert counts["silver_merged"] == 8760 * 2 + 9 * 24
    assert counts["gold_trend_city"] == 1 and counts["gold_daily_city"] > 700

    assert sorted(read_ingest_log(env).status) == ["complete", "complete", "partial"]
    assert pipeline.backfill_plan(today=TODAY) == [{"city_id": "vienna", "year": 2024}]

    silver_path = table_path(env, "silver", "weather_hourly")
    before = spark.read.format("delta").load(silver_path).count()
    again = pipeline.refine(today=TODAY, spark=spark)
    assert again["silver_merged"] == 0
    assert spark.read.format("delta").load(silver_path).count() == before

    stats = DeltaTable(table_path(env, "ops", "pipeline_stats")).to_pandas()
    assert {"bronze", "silver", "gold"} <= set(stats.layer)
    assert stats[stats.table_name == "weather_hourly"].row_count.iloc[0] == before
    assert not DeltaTable(table_path(env, "ops", "dq_results")).to_pandas().empty


@pytest.mark.spark
def test_refine_with_recent_data_only_succeeds(spark, env, fake_client):
    assert pipeline.ingest_recent_all(today=TODAY, client=fake_client) == 1
    counts = pipeline.refine(today=TODAY, spark=spark)
    assert counts["gold_daily_city"] > 0
    assert counts["gold_anomaly_annual"] == 0
    dq = DeltaTable(table_path(env, "ops", "dq_results")).to_pandas()
    assert dq[dq.severity == "hard"].passed.all()
    soft = dq[(dq.check_name == "row_count_gt_0") & (dq.table_name == "gold.anomaly_annual")]
    assert not soft.passed.iloc[0]


def test_backfill_started(env, fake_client):
    assert pipeline.backfill_started() is False
    pipeline.ingest_one("vienna", 2023, today=TODAY, client=fake_client)
    assert pipeline.backfill_started() is True


def test_previous_year_plan(env, fake_client):
    pipeline.ingest_one("vienna", 2023, today=TODAY, client=fake_client)
    assert pipeline.previous_year_plan(today=TODAY) == []
    assert pipeline.previous_year_plan(today=date(2025, 1, 2)) == [
        {"city_id": "vienna", "year": 2024}
    ]


@pytest.mark.spark
def test_vacuum_removes_dead_files(spark, lake):
    from weather_etl.spark import vacuum

    path = table_path(lake, "gold", "t")
    for n in (3, 5):
        spark.range(n).coalesce(1).write.format("delta").mode("overwrite").save(path)
    files = lambda: {p for p in Path(path).glob("*.parquet")}  # noqa: E731
    assert len(files()) == 2
    key = "spark.databricks.delta.retentionDurationCheck.enabled"
    old = spark.conf.get(key, "true")
    spark.conf.set(key, "false")
    try:
        vacuum(spark, path, retention_hours=0)
    finally:
        spark.conf.set(key, old)
    assert len(files()) == 1
    assert spark.read.format("delta").load(path).count() == 5


@pytest.mark.spark
def test_refine_demo_tolerates_bronze_of_inactive_city(
    spark, env, fake_client, tmp_path, monkeypatch
):
    cfg = tmp_path / "cities2.yaml"
    base = {"country": "X", "tz": "UTC", "climate": "temperate"}
    cfg.write_text(
        yaml.safe_dump(
            {
                "cities": [
                    {
                        "id": "vienna",
                        "name": "Vienna",
                        "lat": 48.2,
                        "lon": 16.4,
                        "demo": True,
                        **base,
                    },
                    {"id": "sydney", "name": "Sydney", "lat": -33.9, "lon": 151.2, **base},
                ]
            }
        )
    )
    monkeypatch.setenv("CITIES_FILE", str(cfg))
    pipeline.ingest_one("sydney", 2023, today=TODAY, client=fake_client)  # earlier full run
    monkeypatch.setenv("DEMO", "1")
    assert pipeline.ingest_recent_all(today=TODAY, client=fake_client) == 1
    counts = pipeline.refine(today=TODAY, spark=spark)
    assert counts["silver_merged"] == 8760 + 14 * 24
    dq = DeltaTable(table_path(env, "ops", "dq_results")).to_pandas()
    fresh = dq[dq.check_name == "fresh_within_10_days"]
    assert fresh.passed.all()
