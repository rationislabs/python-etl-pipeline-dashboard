"""Operational tables written without Spark (delta-rs)."""

from __future__ import annotations

import calendar
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from weather_etl.paths import table_path

INGEST_LOG_SCHEMA = pa.schema(
    [
        ("city_id", pa.string()),
        ("year", pa.int32()),
        ("status", pa.string()),
        ("rows", pa.int64()),
        ("fetched_at", pa.timestamp("us", tz="UTC")),
    ]
)


def expected_hours(year: int) -> int:
    return 8784 if calendar.isleap(year) else 8760


def classify(year: int, hours: int, today: date) -> str:
    return "complete" if year < today.year and hours == expected_hours(year) else "partial"


def upsert_ingest_log(root: Path, rows: list[dict]) -> None:
    table = pa.Table.from_pylist(rows, schema=INGEST_LOG_SCHEMA)
    path = table_path(root, "ops", "ingest_log")
    if not DeltaTable.is_deltatable(path):
        write_deltalake(path, table)
        return
    (
        DeltaTable(path)
        .merge(
            source=table,
            predicate="t.city_id = s.city_id AND t.year = s.year",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def read_ingest_log(root: Path) -> pd.DataFrame:
    path = table_path(root, "ops", "ingest_log")
    if not DeltaTable.is_deltatable(path):
        return INGEST_LOG_SCHEMA.empty_table().to_pandas()
    return DeltaTable(path).to_pandas()


def completed(root: Path) -> set[tuple[str, int]]:
    log = read_ingest_log(root)
    done = log[log["status"] == "complete"]
    return {(str(r.city_id), int(r.year)) for r in done.itertuples()}
