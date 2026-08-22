"""threatlake.ml.features.build_features: hand-computed trailing-window
aggregates.

Fixture, src_ip "1.1.1.1" (a simulated port-scan), 3 connect events 10s
apart, all within the default 60s window, each touching a different port:
  t=0    port 10  -> events_in_window=1, distinct_ports=1
  t=10   port 11  -> events_in_window=2, distinct_ports=2
  t=20   port 12  -> events_in_window=3, distinct_ports=3

src_ip "2.2.2.2" (a simulated brute-force), 2 login.failed events on the
SAME port, one just inside the window and one just outside it:
  t=0    dst_port 22 -> events_in_window=1, login_attempts_in_window=1
  t=100  dst_port 22 -> 100s later, outside the 60s window from t=0, so
                         this event's own trailing window only contains
                         itself: events_in_window=1, login_attempts=1
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from threatlake.ml.features import ANOMALY_FEATURE_NAMES, build_features, validate_feature_frame
from tests.unit.silver_fixture_helpers import build_silver_df, silver_row

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_BASE = datetime(2026, 8, 2, 10, 0, 0, tzinfo=UTC)


def _features_by_offset(spark: SparkSession, rows: list[dict[str, object]]) -> list[dict[str, object]]:
    df = build_silver_df(spark, *rows)
    features = build_features(df)
    return [r.asDict() for r in features.orderBy("event_id").collect()]


def test_build_features_has_the_expected_columns(spark: SparkSession) -> None:
    df = build_silver_df(spark, silver_row())
    features = build_features(df)
    assert set(ANOMALY_FEATURE_NAMES).issubset(set(features.columns))
    validate_feature_frame(features.columns)  # should not raise


def test_build_features_port_scan_shape(spark: SparkSession) -> None:
    rows = [
        silver_row(
            event_id=f"scan-{i}",
            src_ip="1.1.1.1",
            dst_port=10 + i,
            event_time=_BASE + timedelta(seconds=i * 10),
            credentials_attempted=False,
        )
        for i in range(3)
    ]
    by_id = {r["event_id"]: r for r in _features_by_offset(spark, rows)}

    assert by_id["scan-0"]["events_by_src_ip_in_window"] == 1
    assert by_id["scan-1"]["events_by_src_ip_in_window"] == 2  # noqa: PLR2004
    assert by_id["scan-2"]["events_by_src_ip_in_window"] == 3  # noqa: PLR2004
    assert by_id["scan-2"]["distinct_dst_ports_touched_by_src_ip_in_window"] == 3  # noqa: PLR2004


def test_build_features_login_attempts_and_is_login_attempt_flag(spark: SparkSession) -> None:
    rows = [
        silver_row(
            event_id="brute-early",
            src_ip="2.2.2.2",
            dst_port=22,
            event_time=_BASE,
            credentials_attempted=True,
        ),
        silver_row(
            event_id="brute-late",
            src_ip="2.2.2.2",
            dst_port=22,
            event_time=_BASE + timedelta(seconds=100),
            credentials_attempted=True,
        ),
    ]
    by_id = {r["event_id"]: r for r in _features_by_offset(spark, rows)}

    # 100s later is outside the default 60s trailing window from t=0, so
    # the late event's own window contains only itself.
    assert by_id["brute-late"]["events_by_src_ip_in_window"] == 1
    assert by_id["brute-late"]["login_attempts_by_src_ip_in_window"] == 1
    assert by_id["brute-late"]["is_login_attempt"] == 1
    assert by_id["brute-early"]["is_login_attempt"] == 1


def test_build_features_null_dst_port_does_not_count_as_a_port(spark: SparkSession) -> None:
    """A cowrie.command.input event has no dst_port - it must not inflate
    distinct_dst_ports_touched (collect_set ignores nulls).
    """
    rows = [
        silver_row(event_id="a", src_ip="3.3.3.3", dst_port=22, event_time=_BASE),
        silver_row(event_id="b", src_ip="3.3.3.3", dst_port=None, event_time=_BASE + timedelta(seconds=5)),
    ]
    by_id = {r["event_id"]: r for r in _features_by_offset(spark, rows)}
    assert by_id["b"]["distinct_dst_ports_touched_by_src_ip_in_window"] == 1


def test_build_features_drops_rows_missing_src_ip_or_event_time(spark: SparkSession) -> None:
    rows = [
        silver_row(event_id="no-ip", src_ip=None),
        silver_row(event_id="no-time", event_time=None),
        silver_row(event_id="fine"),
    ]
    features = build_silver_df(spark, *rows).transform(build_features)
    assert features.count() == 1
    assert features.first()["event_id"] == "fine"
