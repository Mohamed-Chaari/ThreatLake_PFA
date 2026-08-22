"""Feature engineering for the two PFA detectors: IsolationForest
(``threatlake.ml.train_anomaly``) and the port-scan rule
(``threatlake.ml.rules.port_scan_rule``).

Every feature here is TRAILING-WINDOW: for a given event, "how much
activity has this src_ip generated in the ``window_seconds`` before (and
including) this event?" - not a whole-batch total. A whole-batch total
can't tell "20 ports touched in one burst" from "20 ports touched over a
week"; a trailing window can, and it lets the exact same feature
computation run unchanged whether scoring one hour of silver data or one
year of it.

Implemented with a single Spark window function per feature -
``Window.partitionBy("src_ip").orderBy(event_time_unix).rangeBetween(-window_seconds, 0)`` -
no join, no self-union, no UDF: every row's window is computed directly
against that same src_ip's other rows, ordered by time.

ThreatLake AI's ``ml/features.py`` builds a much larger feature space,
deliberately shaped so it also works as a benchmark-classifier training
set (UNSW-NB15 transfer - see that module's own extensive docstring).
PFA has no classifier and no benchmark (see ARCHITECTURE.md), so this
module only builds what the two PFA detectors actually consume, and
carries none of that benchmark-transfer machinery.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = [
    "ANOMALY_FEATURE_NAMES",
    "DEFAULT_WINDOW_SECONDS",
    "FeatureValidationError",
    "build_features",
    "validate_feature_frame",
]

#: Default trailing window, in seconds, every feature below is computed
#: over - "how much has this src_ip done in the WINDOW_SECONDS before
#: (and including) this event?"
DEFAULT_WINDOW_SECONDS = 60

#: The exact feature columns, in order, IsolationForest is trained and
#: scored on. Order matters: scikit-learn has no column-name concept, it
#: trusts positional order - a mismatch here would silently feed the
#: model the wrong feature in the wrong slot. threatlake.ml.train_anomaly
#: and threatlake.ml.score_events both import this tuple rather than
#: hand-typing the column list, so there is exactly one place it's
#: defined.
#:
#: The four features cover the two attack patterns the two detectors
#: exist to catch: a brute-force burst shows up as high
#: events_by_src_ip_in_window / login_attempts_by_src_ip_in_window; a
#: port scan shows up as high distinct_dst_ports_touched_by_src_ip_in_window
#: with events spread across DIFFERENT ports rather than repeated ones.
#: is_login_attempt distinguishes the two shapes further, at the
#: individual-event level.
ANOMALY_FEATURE_NAMES: tuple[str, ...] = (
    "events_by_src_ip_in_window",
    "distinct_dst_ports_touched_by_src_ip_in_window",
    "login_attempts_by_src_ip_in_window",
    "is_login_attempt",
)


class FeatureValidationError(RuntimeError):
    """Raised when a feature DataFrame's columns don't match what a model expects."""


def build_features(
    silver_df: DataFrame, window_seconds: int = DEFAULT_WINDOW_SECONDS
) -> DataFrame:
    """Compute :data:`ANOMALY_FEATURE_NAMES` for every silver event that has
    both a src_ip and an event_time - the two columns every feature here
    is computed relative to. Rows missing either can't be attributed to an
    attacker's trailing activity and are dropped.

    Keeps ``event_id`` and ``src_ip`` alongside the features so callers can
    join scores back to the events they were computed for, and so
    ``threatlake.ml.rules.port_scan_rule`` can be applied per-row without a
    second pass over silver.
    """
    base = silver_df.filter(F.col("src_ip").isNotNull() & F.col("event_time").isNotNull())

    event_time_unix = F.unix_timestamp(F.col("event_time"))
    trailing_window = (
        Window.partitionBy("src_ip").orderBy(event_time_unix).rangeBetween(-window_seconds, 0)
    )
    # credentials_attempted is a NOT NULL boolean column (see
    # threatlake.transform.silver.schema) - always true/false, safe to
    # cast straight to 0/1.
    is_login_attempt = F.col("credentials_attempted").cast("int")

    return base.select(
        "event_id",
        "src_ip",
        F.count(F.lit(1)).over(trailing_window).alias("events_by_src_ip_in_window"),
        # collect_set (used here as a window aggregate) ignores nulls, so
        # events with no dst_port (e.g. a cowrie.command.input event - see
        # threatlake.transform.silver.cowrie) simply don't contribute a
        # port to the count, rather than counting as a fake "null port".
        F.size(F.collect_set("dst_port").over(trailing_window)).alias(
            "distinct_dst_ports_touched_by_src_ip_in_window"
        ),
        F.sum(is_login_attempt).over(trailing_window).alias("login_attempts_by_src_ip_in_window"),
        is_login_attempt.alias("is_login_attempt"),
    )


def validate_feature_frame(
    columns: list[str], expected: tuple[str, ...] = ANOMALY_FEATURE_NAMES
) -> None:
    """Raise if ``columns`` is missing any column in ``expected``.

    Guards the model-input boundary: threatlake.ml.train_anomaly and
    threatlake.ml.score_events both call this right before handing a
    pandas frame to scikit-learn, so a feature-computation bug surfaces as
    an immediate, specific error instead of a silently wrong model.
    """
    missing = [name for name in expected if name not in columns]
    if missing:
        raise FeatureValidationError(
            f"Feature frame is missing expected columns: {missing}. Got: {columns}."
        )
