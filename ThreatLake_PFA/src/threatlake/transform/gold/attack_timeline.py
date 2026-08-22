"""attack_timeline - hourly event counts by attack_category and dst_port.

GRAIN: one row per ``(hour, attack_category, dst_port)``. ``hour`` is
``event_time`` truncated to the top of the hour (UTC - every timestamp in
this project is, see docs/adr/0001-spark-session-timezone.md).

This is the finest-grained time-series gold table: rolling up to
``(hour, attack_category)`` or ``(hour, dst_port)`` alone is a single
``groupBy`` away from this table. The reverse - recovering the port
breakdown from a category-only rollup - is not, so this grain is kept rather
than pre-aggregating a dimension away.

``dst_port`` is nullable and kept that way: rows for application-layer
events with no destination context (e.g. a Cowrie command-input event - see
threatlake.transform.silver.cowrie) group together under a null port rather
than being dropped or assigned an invented sentinel value.

Idempotent write: see threatlake.transform.gold.writer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from threatlake.common.config import Settings
from threatlake.common.paths import silver_path
from threatlake.transform.gold.writer import write_gold_table

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

__all__ = ["build_attack_timeline", "write_attack_timeline"]


def build_attack_timeline(silver_df: DataFrame) -> DataFrame:
    """Aggregate ``silver_df`` into the hourly (category, port) grain described above."""
    base = silver_df.filter(F.col("event_time").isNotNull())

    timeline = base.groupBy(
        F.date_trunc("hour", F.col("event_time")).alias("hour"),
        F.col("attack_category"),
        F.col("dst_port"),
    ).agg(
        F.count(F.lit(1)).alias("event_count"),
        F.countDistinct("src_ip").alias("distinct_src_ip_count"),
    )

    return timeline.withColumn("date", F.to_date(F.col("hour"))).select(
        "date", "hour", "attack_category", "dst_port", "event_count", "distinct_src_ip_count"
    )


def write_attack_timeline(spark: SparkSession, settings: Settings | None = None) -> DataFrame:
    """Read silver, build attack_timeline, and idempotently overwrite the gold table."""
    silver_df = spark.read.format("delta").load(silver_path("events", settings))
    timeline = build_attack_timeline(silver_df)
    write_gold_table(timeline, "attack_timeline", settings, partition_by=["date"])
    return timeline
