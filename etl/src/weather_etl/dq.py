"""Custom PySpark data-quality checks; results land in ops.dq_results."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from weather_etl.openmeteo import HOURLY_VARS
from weather_etl.paths import table_path

SILVER_TABLE = "silver.weather_hourly"
RANGES: dict[str, tuple[float, float]] = {
    "temperature_2m": (-90.0, 60.0),
    "relative_humidity_2m": (0.0, 100.0),
    "cloud_cover": (0.0, 100.0),
    "precipitation": (0.0, math.inf),
    "wind_speed_10m": (0.0, math.inf),
}
NULL_SHARE_MAX = 0.01
HARD_GOLD = {"daily_city"}  # the rest depend on the baseline and fill in as the backfill runs

DQ_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("run_at", pa.timestamp("us", tz="UTC")),
        ("table_name", pa.string()),
        ("check_name", pa.string()),
        ("severity", pa.string()),
        ("passed", pa.bool_()),
        ("observed", pa.float64()),
        ("threshold", pa.float64()),
    ]
)


@dataclass(frozen=True)
class CheckResult:
    table_name: str
    check_name: str
    severity: str
    passed: bool
    observed: float
    threshold: float


class DQError(RuntimeError):
    def __init__(self, failed: list[CheckResult]):
        names = ", ".join(f"{r.table_name}:{r.check_name}" for r in failed)
        super().__init__(f"hard data-quality checks failed: {names}")
        self.failed = failed


def _out_of_range(col: str, lo: float, hi: float):
    cond = F.col(col) < lo if hi == math.inf else (F.col(col) < lo) | (F.col(col) > hi)
    return F.sum(F.coalesce(cond.cast("int"), F.lit(0)))


def check_silver_batch(df: DataFrame, city_ids: set[str]) -> list[CheckResult]:
    total = df.count()
    if total == 0:
        return []
    aggs = [
        F.sum(F.col("ts_utc").isNull().cast("int")).alias("null_ts"),
        F.sum((~F.col("city_id").isin(sorted(city_ids))).cast("int")).alias("unknown_city"),
    ]
    aggs += [_out_of_range(v, lo, hi).alias(f"range_{v}") for v, (lo, hi) in RANGES.items()]
    aggs += [F.sum(F.col(v).isNull().cast("int")).alias(f"null_{v}") for v in HOURLY_VARS]
    r = {k: (v or 0) for k, v in df.agg(*aggs).first().asDict().items()}
    dupes = df.groupBy("city_id", "ts_utc").count().filter(F.col("count") > 1).count()

    t = SILVER_TABLE
    results = [
        CheckResult(t, "unique_city_ts", "hard", dupes == 0, float(dupes), 0.0),
        CheckResult(t, "ts_not_null", "hard", r["null_ts"] == 0, float(r["null_ts"]), 0.0),
        CheckResult(
            t, "city_in_config", "hard", r["unknown_city"] == 0, float(r["unknown_city"]), 0.0
        ),
    ]
    for v in RANGES:
        n = r[f"range_{v}"]
        results.append(CheckResult(t, f"range_{v}", "hard", n == 0, float(n), 0.0))
    for v in HOURLY_VARS:
        share = r[f"null_{v}"] / total
        results.append(
            CheckResult(t, f"null_share_{v}", "soft", share < NULL_SHARE_MAX, share, NULL_SHARE_MAX)
        )
    return results


def check_silver_table(
    spark: SparkSession, root: Path, today: date, city_ids: set[str]
) -> list[CheckResult]:
    df = spark.read.format("delta").load(table_path(root, "silver", "weather_hourly"))
    year = F.year("ts_utc")
    per_year = df.filter(year < today.year).groupBy("city_id", year.alias("year")).count()
    leap = (F.col("year") % 4 == 0) & ((F.col("year") % 100 != 0) | (F.col("year") % 400 == 0))
    expected = F.when(leap, 8784).otherwise(8760)
    incomplete = per_year.filter(F.col("count") != expected).count()

    latest = {
        r.city_id: r.max_ts
        for r in df.groupBy("city_id").agg(F.max("ts_utc").cast("long").alias("max_ts")).collect()
    }
    cutoff = datetime.combine(today - timedelta(days=10), time(), UTC).timestamp()
    stale = sum(1 for c in city_ids if latest.get(c) is None or latest[c] < cutoff)
    return [
        CheckResult(
            SILVER_TABLE, "complete_years", "soft", incomplete == 0, float(incomplete), 0.0
        ),
        CheckResult(SILVER_TABLE, "fresh_within_10_days", "soft", stale == 0, float(stale), 0.0),
    ]


def check_gold(counts: dict[str, int]) -> list[CheckResult]:
    return [
        CheckResult(
            f"gold.{name}",
            "row_count_gt_0",
            "hard" if name in HARD_GOLD else "soft",
            n > 0,
            float(n),
            0.0,
        )
        for name, n in counts.items()
    ]


def write_results(root: Path, run_id: str, results: list[CheckResult]) -> None:
    if not results:
        return
    run_at = datetime.now(UTC)
    rows = [{"run_id": run_id, "run_at": run_at, **asdict(r)} for r in results]
    write_deltalake(
        table_path(root, "ops", "dq_results"),
        pa.Table.from_pylist(rows, schema=DQ_SCHEMA),
        mode="append",
    )


def raise_on_hard_failures(results: list[CheckResult]) -> None:
    failed = [r for r in results if r.severity == "hard" and not r.passed]
    if failed:
        raise DQError(failed)
