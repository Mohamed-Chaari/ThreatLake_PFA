"""threatlake.ml.rules: a fixed-threshold rule with no batch-wide budget,
run alongside (not instead of) IsolationForest - see
threatlake.ml.score_events for how the two are combined and attributed.

Copied near-verbatim from ThreatLake AI's own test_ml_rules.py: the rule
itself (threatlake.ml.rules.port_scan_rule) is reused byte-for-byte
unmodified, so its test should be too.
"""

from __future__ import annotations

from threatlake.ml.rules import PORT_SCAN_DISTINCT_PORT_THRESHOLD, port_scan_rule


def test_port_scan_rule_does_not_fire_at_or_below_threshold() -> None:
    assert port_scan_rule(1) is False
    assert port_scan_rule(PORT_SCAN_DISTINCT_PORT_THRESHOLD) is False


def test_port_scan_rule_fires_above_threshold() -> None:
    assert port_scan_rule(PORT_SCAN_DISTINCT_PORT_THRESHOLD + 1) is True
    assert port_scan_rule(8) is True  # the observed injected-scan ceiling


def test_port_scan_rule_threshold_is_overridable() -> None:
    assert port_scan_rule(3, threshold=2) is True
    assert port_scan_rule(2, threshold=2) is False
