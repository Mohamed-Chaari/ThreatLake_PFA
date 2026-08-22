"""Shared read helpers used by more than one router - kept out of any
single router so "what counts as an alert" and "how to handle a gold
table that hasn't been built yet" can't drift between endpoints that both
need them.

GOLD TABLES CAN LEGITIMATELY NOT EXIST YET: the gold tables and
``ml_scores`` only exist after ``run_pipeline.py`` has completed at least
one full run. Every gold-backed read here treats "table not found" as "no
data yet" (returns ``None``), not as a fault - routers turn that into an
empty response, never a 500.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from threatlake.common.paths import gold_path, silver_path

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from threatlake.common.config import Settings

__all__ = ["ALERT_COLUMNS", "read_alerts", "read_gold_table", "read_silver"]

#: Columns pulled from silver to enrich an alert row with attack context
#: (src_ip, port, severity, ...) that ml_scores itself doesn't carry.
_ALERT_SILVER_COLUMNS = (
    "event_id",
    "event_time",
    "src_ip",
    "dst_ip",
    "dst_port",
    "source_type",
    "severity",
    "attack_category",
)

#: read_alerts()'s full output projection, matching
#: threatlake.api.schemas.AlertItem field-for-field - shared by
#: threatlake.api.routers.alerts and threatlake.api.routers.attacker_profiles
#: (the /{ip} drill-down) so both select (and therefore serialize) an
#: alert row identically.
ALERT_COLUMNS = (
    "event_id",
    "scored_at",
    "alert_source",
    "anomaly_score",
    "is_anomaly",
    "event_time",
    "src_ip",
    "dst_ip",
    "dst_port",
    "source_type",
    "severity",
    "attack_category",
)


def read_gold_table(spark: SparkSession, table: str, settings: Settings) -> DataFrame | None:
    """Read a gold Delta table, or None if it hasn't been built yet."""
    try:
        return spark.read.format("delta").load(gold_path(table, settings))
    except Exception:
        return None


def read_silver(spark: SparkSession, settings: Settings) -> DataFrame | None:
    """Read the silver events table, or None if nothing has landed yet."""
    try:
        return spark.read.format("delta").load(silver_path("events", settings))
    except Exception:
        return None


def read_alerts(spark: SparkSession, settings: Settings) -> DataFrame | None:
    """``ml_scores`` joined with silver, filtered to rows either detector
    actually flagged.

    ``is_anomaly`` is a NOT NULL column in
    ``threatlake.ml.score_events.SCORES_SCHEMA`` (unlike ThreatLake AI's
    version, which also scores a classifier whose rows leave it null) -
    so this can filter on it directly rather than needing a
    null-coalescing OR.
    """
    scores = read_gold_table(spark, "ml_scores", settings)
    if scores is None:
        return None
    silver = read_silver(spark, settings)
    if silver is None:
        return None

    joined = scores.join(silver.select(*_ALERT_SILVER_COLUMNS), on="event_id", how="inner")
    return joined.filter(F.col("is_anomaly"))
