"""threatlake.ml.score_events: combines the trained IsolationForest with
threatlake.ml.rules.port_scan_rule into one is_anomaly flag, attributed
via alert_source. Also covers the append-only "don't re-score" behavior."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from threatlake.common.paths import gold_path
from threatlake.ml.rules import PORT_SCAN_DISTINCT_PORT_THRESHOLD
from threatlake.ml.score_events import score_silver_events, write_scored_events
from threatlake.ml.train_anomaly import train_anomaly
from tests.unit.silver_fixture_helpers import silver_row, write_silver_fixture

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from threatlake.common.config import Settings

_BASE = datetime(2026, 8, 2, 10, 0, 0, tzinfo=UTC)
_PORT_SCAN_IP = "9.9.9.9"


def _fixture_rows() -> list[dict[str, object]]:
    """Ordinary background traffic on many src_ips, plus one src_ip that
    touches more distinct ports than PORT_SCAN_DISTINCT_PORT_THRESHOLD in
    one trailing window - the port-scan rule must fire for it regardless
    of what IsolationForest decides.
    """
    rows = [
        silver_row(event_id=f"bg{i}", src_ip=f"10.0.0.{i}", event_time=_BASE + timedelta(seconds=i))
        for i in range(20)
    ]
    n_ports = PORT_SCAN_DISTINCT_PORT_THRESHOLD + 3
    rows += [
        silver_row(
            event_id=f"scan{i}",
            src_ip=_PORT_SCAN_IP,
            dst_port=100 + i,
            event_time=_BASE + timedelta(seconds=i * 2),
        )
        for i in range(n_ports)
    ]
    return rows


def test_score_events_flags_the_port_scanner_via_the_rule(
    spark: SparkSession, tmp_lakehouse: Settings
) -> None:
    write_silver_fixture(spark, tmp_lakehouse, *_fixture_rows())
    train_anomaly(spark, tmp_lakehouse, contamination=0.05)

    scored, result = score_silver_events(spark, tmp_lakehouse)
    assert result.n_newly_scored == result.n_candidates

    rows = {r["event_id"]: r for r in scored.collect()}
    # distinct_dst_ports_touched_by_src_ip_in_window only crosses the
    # threshold once enough of this src_ip's OWN earlier connections have
    # landed in its trailing window - the LAST scan event is guaranteed to
    # have every one of this src_ip's ports in its window, so it's the one
    # event the rule is guaranteed to fire for.
    n_ports = PORT_SCAN_DISTINCT_PORT_THRESHOLD + 3
    last_scan_event = rows[f"scan{n_ports - 1}"]
    assert last_scan_event["is_anomaly"] is True
    assert last_scan_event["alert_source"] in ("rule", "both")


def test_write_scored_events_skips_already_scored_rows(
    spark: SparkSession, tmp_lakehouse: Settings
) -> None:
    write_silver_fixture(spark, tmp_lakehouse, *_fixture_rows())
    train_anomaly(spark, tmp_lakehouse, contamination=0.05)

    first = write_scored_events(spark, tmp_lakehouse)
    assert first.n_newly_scored > 0

    second = write_scored_events(spark, tmp_lakehouse)
    assert second.n_newly_scored == 0
    assert second.n_already_scored == first.n_newly_scored

    ml_scores = spark.read.format("delta").load(gold_path("ml_scores", tmp_lakehouse))
    assert ml_scores.count() == first.n_newly_scored
