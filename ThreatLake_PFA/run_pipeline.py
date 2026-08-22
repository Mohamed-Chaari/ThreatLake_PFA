"""Run the whole study pipeline end to end: bronze -> silver -> detector.

Usage (from this folder):

    python run_pipeline.py

See README.md for what this is, what got removed from the real project
to make it this small, and how to set up an environment to actually run
it (you need pyspark + delta-spark installed - either your own venv, or
you can point at the real ThreatLake_AI project's existing venv, since
this script never imports anything from it - see the sys.path comment
right below for exactly why that's safe).
"""

from __future__ import annotations

import os
import sys

# THIS IS THE LINE THAT KEEPS THIS FOLDER TRULY ISOLATED.
#
# Both this study copy and the real ThreatLake_AI project define a Python
# package called "threatlake" (see ./src/threatlake/ in each). If you run
# this script using the real project's virtualenv - which already has ITS
# OWN "threatlake" installed and importable from anywhere - Python would
# normally find that one first and quietly run the real project's code
# instead of this folder's stripped-down copy.
#
# Inserting THIS folder's own src/ at the very front of sys.path makes
# Python look here FIRST, no matter which Python interpreter you launch
# this with. That is what makes it safe to reuse the real project's venv
# for convenience (see README.md) without ever touching its code.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _THIS_DIR)
sys.path.insert(0, os.path.join(_THIS_DIR, "src"))

from study_pipeline.bronze import build_bronze  # noqa: E402 - see sys.path comment above
from study_pipeline.detector import find_port_scanners  # noqa: E402
from study_pipeline.silver import build_silver  # noqa: E402
from study_pipeline.spark_session import build_spark  # noqa: E402

LANDING_DIR = os.path.join(_THIS_DIR, "study_pipeline", "sample_landing", "cowrie")
BRONZE_PATH = os.path.join(_THIS_DIR, "data", "bronze_cowrie")
SILVER_PATH = os.path.join(_THIS_DIR, "data", "silver_events")


def _print_header(title: str) -> None:
    print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")


def main() -> None:
    spark = build_spark()

    # --- Step 1: bronze -------------------------------------------------
    _print_header("STEP 1 - BRONZE (landing zone -> parsed, stamped rows)")
    bronze_df = build_bronze(spark, LANDING_DIR, BRONZE_PATH)
    bronze_count = bronze_df.count()
    print(f"{bronze_count} lines read from landing (already excludes the one")
    print("deliberately malformed line - bronze.py explicitly filters it")
    print("out; see the long comment there for exactly why that's needed).")
    bronze_df.select("ingest_date", "source_type", "_record_hash", "cowrie.eventid").show(
        truncate=40
    )

    # --- Step 2: silver ---------------------------------------------------
    _print_header("STEP 2 - SILVER (mapped to the unified event shape)")
    silver_df = build_silver(bronze_df)
    silver_df.write.format("delta").mode("append").save(SILVER_PATH)
    silver_count = silver_df.count()
    print(f"{silver_count} silver rows (the malformed line is gone - see above).")
    silver_df.select(
        "event_id", "event_time", "src_ip", "dst_port", "attack_category", "severity"
    ).show(truncate=20)

    # --- Step 3: detector -------------------------------------------------
    _print_header("STEP 3 - DETECTOR (port_scan_rule from ml/rules.py)")
    flagged = find_port_scanners(silver_df)
    if flagged:
        print("Flagged as port scanners (distinct dst_ports touched > threshold):")
        for ip, n in sorted(flagged.items()):
            print(f"  {ip}  ->  {n} distinct ports")
    else:
        print("Nothing flagged.")

    spark.stop()


if __name__ == "__main__":
    main()
