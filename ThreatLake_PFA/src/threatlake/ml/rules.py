"""Lightweight, explicit rule-based detection - runs ALONGSIDE IsolationForest
(threatlake.ml.train_anomaly), not instead of it. See
threatlake.ml.inference.score_silver_events for how the two are combined.

WHY A RULE, GIVEN A TRAINED MODEL ALREADY EXISTS: threatlake.ml.train_anomaly's
"KNOWN LIMITATION" section documents a specific, verified failure mode - on the
project's injection test corpus, IsolationForest's ``contamination`` parameter
fixes a hard budget on how many events can be flagged per batch, and a more
extreme pattern (credential brute-force) crowded out a less extreme but still
real one (port scan) for most of that budget, even after
threatlake.ml.features.ANOMALY_FEATURE_NAMES gave the model a fan-out feature
that separates port scans from normal traffic almost perfectly on its own.
A fixed threshold rule has no such budget: it fires per-event, independent of
what else is in the same batch, so it isn't subject to that crowding-out
failure mode. It is also not a replacement for the model - it has no answer
for anomaly patterns nobody has written a rule for yet, which is the entire
reason threatlake.ml.train_anomaly exists.
"""

from __future__ import annotations

__all__ = ["PORT_SCAN_DISTINCT_PORT_THRESHOLD", "port_scan_rule"]

#: Threshold for flagging port-scan-shaped fan-out: more than this many
#: DISTINCT dst_ports touched by one src_ip in the trailing window
#: (threatlake.ml.features.ANOMALY_FEATURE_NAMES's
#: distinct_dst_ports_touched_by_src_ip_in_window).
#:
#: Chosen from the observed separation in the project's injection test
#: corpus (threatlake.ml.train_anomaly's KNOWN LIMITATION section, same
#: corpus, same 60s window): every one of the 565 ordinary events in that
#: corpus has this feature exactly equal to 1 (including brute-force events,
#: which hammer a single port repeatedly); every injected port-scan event
#: reaches up to 8. 5 is a threshold with headroom on both sides of that
#: gap - well above the "1" normal traffic never exceeds, well below the
#: observed "8" ceiling of an actual scan - rather than a value fit exactly
#: to this dataset (e.g. 2, which would perfectly split 1 from 8 here but
#: is a description of this specific corpus, not a general claim about what
#: "many distinct ports" means for honeypot traffic).
PORT_SCAN_DISTINCT_PORT_THRESHOLD = 5


def port_scan_rule(
    distinct_dst_ports_touched: int, threshold: int = PORT_SCAN_DISTINCT_PORT_THRESHOLD
) -> bool:
    """True if a src_ip touched more than ``threshold`` distinct dst_ports in
    the trailing window - see :data:`PORT_SCAN_DISTINCT_PORT_THRESHOLD` for
    where the default comes from.
    """
    return distinct_dst_ports_touched > threshold
