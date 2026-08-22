"""Step 3: silver -> flagged IPs, using ONE detector (the port-scan rule).

The real project has three detectors that get combined: a supervised
classifier trained on a labeled benchmark dataset, an unsupervised
IsolationForest model trained on live honeypot data, and this fixed
threshold rule - all logged/loaded through MLflow. This study copy keeps
only the rule, because it is the one detector that needs no training
step at all: it is just a plain Python function you can read top to
bottom (see ml/rules.py, imported unmodified below).

port_scan_rule expects ONE number per src_ip: how many DISTINCT dst_port
values that IP touched. In the real pipeline, that number comes from a
TRAILING TIME WINDOW (src/threatlake/ml/features.py in the real repo,
not present in this copy) - "distinct ports touched in the last 60
seconds", recomputed for every event. That machinery (Spark Window +
rangeBetween) is exactly the kind of moving part this study copy is
trying to avoid. Instead, this file counts distinct ports per src_ip
across the WHOLE silver batch, once. That is a real simplification:
it can't tell "20 ports in one burst" from "20 ports over a week" the
way the real trailing-window version can. What it keeps is the shape
that actually matters for understanding the detector: silver rows in,
one behavioral number per attacker out, a fixed threshold decides.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from threatlake.ml.rules import port_scan_rule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def distinct_ports_per_src_ip(silver_df: DataFrame) -> dict[str, int]:
    """How many different dst_port values did each src_ip touch?

    Rows with no src_ip or no dst_port (e.g. a cowrie.command.input event,
    which has neither) can't contribute to this count and are dropped
    first. countDistinct does exactly what it says: for each src_ip, the
    number of DIFFERENT dst_port values seen across all its rows.

    Returns a plain Python dict (src_ip -> count) rather than a Spark
    DataFrame, because the next step (applying port_scan_rule) is a tiny
    amount of data by this point - one number per attacker - so pulling
    it onto the driver with .collect() and looping in plain Python is
    more readable here than writing a Spark UDF for it.
    """
    counted = (
        silver_df.filter(F.col("src_ip").isNotNull() & F.col("dst_port").isNotNull())
        .groupBy("src_ip")
        .agg(F.countDistinct("dst_port").alias("distinct_ports"))
        .collect()
    )
    return {row["src_ip"]: row["distinct_ports"] for row in counted}


def find_port_scanners(silver_df: DataFrame) -> dict[str, int]:
    """Apply port_scan_rule to every src_ip and return the ones it flags.

    This is the entire "detection" step, spelled out: for each src_ip,
    ask the real port_scan_rule function "is this many distinct ports
    suspicious?" - it answers True/False using the fixed threshold
    documented in ml/rules.py (5, chosen from real observed data - see
    that file's comment for exactly how). Nothing here is a model or a
    prediction; it is one comparison per attacker.
    """
    counts = distinct_ports_per_src_ip(silver_df)
    return {ip: n for ip, n in counts.items() if port_scan_rule(n)}
