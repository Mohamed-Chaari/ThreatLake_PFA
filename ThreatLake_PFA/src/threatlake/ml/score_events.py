"""Score silver events with the trained IsolationForest model, combine that
with the port-scan rule, and append the results to the ``ml_scores`` Delta
table.

COMBINED ANOMALY DETECTION: an event is flagged (``is_anomaly=True``) if
EITHER detector fires - see ``threatlake.ml.train_anomaly``'s docstring
for why a fixed-threshold rule runs alongside a budget-constrained model
instead of being replaced by it. The two are deliberately NOT merged into
one opaque score: ``alert_source`` records exactly which one(s) fired
(``"rule"``, ``"ml"``, or ``"both"``), so a flagged event's cause is
always attributable rather than hidden behind a single number.

APPEND, NOT OVERWRITE: unlike ``attacker_profiles``/``attack_timeline``
(threatlake.transform.gold), this table is not a full recompute - it is a
growing log of scoring results, one row per scored event. Re-running
against a silver batch that was already scored is a no-op, not a
duplicate: this module reads back the event_ids already present in
``ml_scores`` and excludes them before scoring, so re-running the
pipeline produces no new rows rather than double-counting.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import joblib
import pandas as pd
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from threatlake.common.config import Settings, get_settings
from threatlake.common.paths import gold_path, silver_path
from threatlake.ml.features import ANOMALY_FEATURE_NAMES, build_features, validate_feature_frame
from threatlake.ml.rules import port_scan_rule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

__all__ = ["SCORES_SCHEMA", "ScoringResult", "score_silver_events", "write_scored_events"]

#: One row per scored event, appended to the ``ml_scores`` gold table.
#: ``anomaly_score`` is IsolationForest's own decision_function output
#: (lower = more anomalous); ``alert_source`` attributes ``is_anomaly`` to
#: "rule", "ml", or "both" - see module docstring.
SCORES_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("scored_at", TimestampType(), False),
        StructField("anomaly_score", DoubleType(), False),
        StructField("is_anomaly", BooleanType(), False),
        StructField("alert_source", StringType(), True),
    ]
)


@dataclass(frozen=True)
class ScoringResult:
    """Outcome of one scoring run."""

    n_candidates: int
    n_already_scored: int
    n_newly_scored: int
    n_flagged: int

    def summary(self) -> str:
        return (
            f"{self.n_candidates} events, {self.n_already_scored} already scored, "
            f"{self.n_newly_scored} newly scored, {self.n_flagged} flagged"
        )


def _already_scored_event_ids(spark: SparkSession, settings: Settings) -> set[str]:
    try:
        existing = spark.read.format("delta").load(gold_path("ml_scores", settings))
    except Exception:  # table not created yet is normal, not an error
        return set()
    rows = existing.select("event_id").distinct().collect()
    return {row["event_id"] for row in rows}


def score_silver_events(
    spark: SparkSession,
    settings: Settings | None = None,
    window_seconds: int = 60,
) -> tuple[DataFrame, ScoringResult]:
    """Score currently-unscored silver events with the trained IsolationForest
    plus the port-scan rule.

    Returns the scored rows (conforming to :data:`SCORES_SCHEMA`) and a
    summary. Does not write anything - see :func:`write_scored_events`.
    """
    settings = settings or get_settings()

    model_path = Path(settings.ml.model_path).expanduser().resolve()
    if not model_path.is_file():
        raise FileNotFoundError(
            f"No trained model at {model_path} - run threatlake.ml.train_anomaly.train_anomaly "
            "first."
        )
    model = joblib.load(model_path)

    silver = spark.read.format("delta").load(silver_path("events", settings))
    feature_sdf = build_features(silver, window_seconds=window_seconds)
    validate_feature_frame(feature_sdf.columns, expected=ANOMALY_FEATURE_NAMES)

    already_scored = _already_scored_event_ids(spark, settings)
    feature_sdf = feature_sdf.filter(~feature_sdf.event_id.isin(already_scored))

    pdf = feature_sdf.toPandas()
    n_candidates = len(pdf) + len(already_scored)
    scored_at = datetime.now(UTC)

    if pdf.empty:
        empty = spark.createDataFrame([], schema=SCORES_SCHEMA)
        result = ScoringResult(n_candidates, len(already_scored), 0, 0)
        return empty, result

    features_only = pdf[list(ANOMALY_FEATURE_NAMES)]
    anomaly_score = model.decision_function(features_only)
    is_ml_anomaly = model.predict(features_only) == -1
    is_rule_anomaly = pdf["distinct_dst_ports_touched_by_src_ip_in_window"].map(port_scan_rule)

    alert_source = pd.Series(
        [
            "both" if ml and rule else "ml" if ml else "rule" if rule else None
            for ml, rule in zip(is_ml_anomaly, is_rule_anomaly, strict=True)
        ],
        index=pdf.index,
    )
    is_anomaly = is_ml_anomaly | is_rule_anomaly

    scored_pdf = pd.DataFrame(
        {
            "event_id": pdf["event_id"],
            "scored_at": scored_at,
            "anomaly_score": anomaly_score.astype(float),
            "is_anomaly": is_anomaly,
            "alert_source": alert_source,
        }
    )
    scored = spark.createDataFrame(scored_pdf, schema=SCORES_SCHEMA)

    result = ScoringResult(
        n_candidates=n_candidates,
        n_already_scored=len(already_scored),
        n_newly_scored=len(pdf),
        n_flagged=int(is_anomaly.sum()),
    )
    return scored, result


def write_scored_events(
    spark: SparkSession,
    settings: Settings | None = None,
    window_seconds: int = 60,
) -> ScoringResult:
    """Score unscored silver events and append the results to ``ml_scores``."""
    settings = settings or get_settings()
    scored, result = score_silver_events(spark, settings, window_seconds)
    if result.n_newly_scored:
        scored.write.format("delta").mode("append").option("mergeSchema", "true").save(
            gold_path("ml_scores", settings)
        )
    return result
