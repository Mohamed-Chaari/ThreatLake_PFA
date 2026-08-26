"""Shared helper for building small, hand-computable SILVER_EVENT_SCHEMA
DataFrames directly (bypassing bronze/map_honeydb) - used by the gold and
feature/detector tests, each of which only cares about a handful of
fields.

Not a test module itself (no test_* name), so pytest won't collect it.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from threatlake.common.config import Settings

__all__ = ["build_silver_df", "silver_row", "write_silver_fixture"]

_DEFAULT_TIME = datetime(2026, 8, 2, 10, 0, 0, tzinfo=UTC)


def silver_row(**overrides: object) -> dict[str, object]:
    """A SILVER_EVENT_SCHEMA-conforming row, defaults overridable per field."""
    event_id = overrides.get("event_id", "default")
    row: dict[str, object] = {
        "event_id": "id-" + str(event_id),
        "event_time": _DEFAULT_TIME,
        "ingest_date": date(2026, 8, 2),
        "source_type": "honeydb",
        "source_event_type": "SSH.CONNECT",
        "src_ip": "203.0.113.42",
        "src_port": 51422,
        "dst_ip": "10.0.0.5",
        "dst_port": 2222,
        "protocol": "tcp",
        "attack_category": "connection",
        "severity": 1,
        "session_id": "s1",
        "credentials_attempted": False,
        "attempted_username": None,
        "attempted_password": None,
        "payload_hash": None,
        "description": None,
        "raw_ref": "honeydb:" + str(event_id),
    }
    row.update(overrides)
    return row


def build_silver_df(spark: SparkSession, *rows: dict[str, object]) -> DataFrame:
    from threatlake.transform.silver.schema import SILVER_EVENT_SCHEMA

    return spark.createDataFrame(list(rows), schema=SILVER_EVENT_SCHEMA)


def write_silver_fixture(spark: SparkSession, settings: Settings, *rows: dict[str, object]) -> None:
    """Write ``rows`` as the silver Delta table at ``settings``' silver_path.

    Used by gold/ML tests, which need a real silver table on disk for the
    function under test (which reads silver_path itself) to read from.
    """
    from threatlake.common.paths import silver_path

    build_silver_df(spark, *rows).write.format("delta").mode("overwrite").save(
        silver_path("events", settings)
    )
