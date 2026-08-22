"""Fit IsolationForest on UNLABELED honeypot silver events.

Live honeypot data has no ground-truth label - there is no analyst
sitting behind cowrie tagging each session "attack" or "not attack" - so
this module never fits a supervised model and never reports a supervised
accuracy/F1 figure. IsolationForest is unsupervised: it scores how easily
a point is isolated by random feature splits, under the assumption that
anomalies are "few and different" and so isolate in fewer splits than
normal points. That assumption, not a labeled test set, is this model's
only claim to validity - it is evaluated here by contamination rate and
score distribution, not by precision/recall against a truth PFA does not
have.

PERSISTENCE: a plain joblib file (``settings.ml.model_path``), not an
MLflow model registry. ThreatLake AI (the full project this is a subset
of) tracks every run through MLflow - parameters, metrics, a registered
model with staged versions - which is real, useful machinery for a system
with a retraining schedule and multiple models competing for a
"Staging"/"Production" slot. PFA trains one model, once, for one batch
pipeline run; a single file scikit-learn's own ``joblib.dump``/``load``
round-trips is the whole story, and is what a from-scratch reader can
open and reason about without also learning MLflow's tracking/registry
API. See ARCHITECTURE.md's "Future extensions" section.

COMBINED WITH A RULE, NOT INSTEAD OF ONE: IsolationForest's
``contamination`` parameter fixes a hard budget on how many events can be
flagged per batch. When two different attack shapes are both present in
one batch, the more extreme one (e.g. a tight brute-force burst) can use
up most of that budget, leaving a real but comparatively milder pattern
(e.g. a port scan) under-flagged even though the model was given a
feature that separates it cleanly from normal traffic
(``distinct_dst_ports_touched_by_src_ip_in_window`` - see
``threatlake.ml.features``). ``threatlake.ml.rules.port_scan_rule`` runs
alongside this model for exactly that reason: a fixed-threshold rule has
no such budget, so it can't be crowded out by a more extreme concurrent
pattern. See ``threatlake.ml.score_events`` for how the two are combined
into one ``is_anomaly`` flag with an ``alert_source`` that records which
one(s) fired.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest

from threatlake.common.config import Settings, get_settings
from threatlake.common.paths import silver_path
from threatlake.ml.features import ANOMALY_FEATURE_NAMES, build_features, validate_feature_frame

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

__all__ = ["AnomalyTrainingResult", "train_anomaly"]

_RANDOM_STATE = 42


@dataclass(frozen=True)
class AnomalyTrainingResult:
    """Outcome of one IsolationForest fit."""

    n_events: int
    n_flagged: int
    contamination: float
    model_path: str

    def summary(self) -> str:
        rate = self.n_flagged / self.n_events if self.n_events else 0.0
        return (
            f"events={self.n_events} flagged={self.n_flagged} "
            f"({rate:.1%}) contamination_param={self.contamination} "
            f"model_path={self.model_path}"
        )


def train_anomaly(
    spark: SparkSession,
    settings: Settings | None = None,
    contamination: float = 0.02,
    n_estimators: int = 200,
    window_seconds: int = 60,
) -> AnomalyTrainingResult:
    """Fit IsolationForest on the current silver table's feature space and
    save it to ``settings.ml.model_path``.

    ``contamination`` is IsolationForest's own prior on the expected
    anomaly fraction, not a measured rate - it directly controls how many
    points get flagged, which is why it is reported alongside the result
    rather than presented as a finding.
    """
    settings = settings or get_settings()

    silver = spark.read.format("delta").load(silver_path("events", settings))
    feature_df = build_features(silver, window_seconds=window_seconds)
    validate_feature_frame(feature_df.columns, expected=ANOMALY_FEATURE_NAMES)

    pdf: pd.DataFrame = feature_df.toPandas()
    if pdf.empty:
        raise ValueError("Silver table produced zero feature rows - nothing to train on.")

    features_only = pdf[list(ANOMALY_FEATURE_NAMES)]

    model = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        random_state=_RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(features_only)
    predictions = model.predict(features_only)  # -1 = anomaly, 1 = normal
    n_flagged = int((predictions == -1).sum())

    model_path = Path(settings.ml.model_path).expanduser().resolve()
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path)

    return AnomalyTrainingResult(
        n_events=len(pdf),
        n_flagged=n_flagged,
        contamination=contamination,
        model_path=str(model_path),
    )
