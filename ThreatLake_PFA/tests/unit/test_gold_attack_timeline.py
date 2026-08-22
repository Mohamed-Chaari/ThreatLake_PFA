"""attack_timeline: hourly (attack_category, dst_port) buckets."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from threatlake.transform.gold.attack_timeline import build_attack_timeline
from tests.unit.silver_fixture_helpers import build_silver_df, silver_row

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_build_attack_timeline_buckets_by_hour_category_port(spark: SparkSession) -> None:
    rows = [
        silver_row(
            event_id="1", src_ip="1.1.1.1", dst_port=22, attack_category="connection",
            event_time=datetime(2026, 8, 2, 8, 3, tzinfo=UTC),
        ),
        silver_row(
            event_id="2", src_ip="2.2.2.2", dst_port=22, attack_category="connection",
            event_time=datetime(2026, 8, 2, 8, 47, tzinfo=UTC),  # same hour bucket as row 1
        ),
        silver_row(
            event_id="3", src_ip="1.1.1.1", dst_port=80, attack_category="connection",
            event_time=datetime(2026, 8, 2, 9, 5, tzinfo=UTC),  # different hour bucket
        ),
    ]
    df = build_silver_df(spark, *rows)
    timeline = {(r["hour"], r["attack_category"], r["dst_port"]): r for r in build_attack_timeline(df).collect()}

    bucket_08 = timeline[(datetime(2026, 8, 2, 8, 0), "connection", 22)]  # noqa: DTZ001
    assert bucket_08["event_count"] == 2  # noqa: PLR2004
    assert bucket_08["distinct_src_ip_count"] == 2  # noqa: PLR2004

    bucket_09 = timeline[(datetime(2026, 8, 2, 9, 0), "connection", 80)]  # noqa: DTZ001
    assert bucket_09["event_count"] == 1
    assert bucket_09["distinct_src_ip_count"] == 1


def test_build_attack_timeline_null_port_is_its_own_bucket(spark: SparkSession) -> None:
    """A cowrie.command.input event has no dst_port context - it groups
    under a null port rather than being dropped."""
    df = build_silver_df(
        spark,
        silver_row(
            event_id="1", src_ip="1.1.1.1", dst_port=None, attack_category="command_execution",
            event_time=datetime(2026, 8, 2, 8, 0, tzinfo=UTC),
        ),
    )
    row = build_attack_timeline(df).collect()[0]
    assert row["dst_port"] is None
    assert row["event_count"] == 1


def test_build_attack_timeline_drops_rows_with_no_event_time(spark: SparkSession) -> None:
    df = build_silver_df(spark, silver_row(event_time=None))
    assert build_attack_timeline(df).count() == 0
