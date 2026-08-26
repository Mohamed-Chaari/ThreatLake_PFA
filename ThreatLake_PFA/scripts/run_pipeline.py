#!/usr/bin/env python3
"""Run the whole ThreatLake PFA pipeline end to end, once:

    bronze -> silver -> gold -> train the anomaly detector -> score events

Usage (from the repo root, with the project installed - see README.md):

    python scripts/run_pipeline.py

Prints real row counts and samples at every stage, so a run of this
script is itself a demonstration that data actually flows through the
whole system, not just that each module imports cleanly.

SILVER IS A FULL RECOMPUTE, NOT AN INCREMENTAL APPEND: ThreatLake AI's
streaming silver step appends incrementally and therefore needs a
separate exact-match dedup pass (duplicate bronze rows - see
threatlake.ingestion.bronze_writer's at-least-once note - would otherwise
become duplicate silver rows). PFA's batch silver step instead reads the
WHOLE bronze table and OVERWRITES the whole silver table on every run,
the same idempotent-by-construction approach threatlake.transform.gold
already uses for gold tables. That means re-running this script never
produces duplicate silver rows, with no separate dedup module needed.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pyspark.sql import functions as F  # noqa: E402

from threatlake.common.paths import bronze_path, gold_path, silver_path  # noqa: E402
from threatlake.common.spark import get_spark, stop_spark  # noqa: E402
from threatlake.enrichment.geo import GeoEnricher  # noqa: E402
from threatlake.ingestion.bronze_writer import write_bronze  # noqa: E402
from threatlake.ml.score_events import write_scored_events  # noqa: E402
from threatlake.ml.train_anomaly import train_anomaly  # noqa: E402
from threatlake.transform.gold.attack_timeline import write_attack_timeline  # noqa: E402
from threatlake.transform.gold.attacker_profiles import write_attacker_profiles  # noqa: E402
from threatlake.transform.silver.honeydb import map_honeydb  # noqa: E402

# Real GeoLite2 production databases, manually downloaded from MaxMind
# (free account + email verification required - see
# https://www.maxmind.com/en/geolite2/signup - not something this script
# or an agent can do on its own) and placed at data/geoip/. Not gitignored
# by accident - see .gitignore's own data/ entry - these are re-downloaded
# once per machine, not shipped with the repo.
_GEOIP_CITY_DB = Path(__file__).resolve().parent.parent / "data" / "geoip" / "GeoLite2-City.mmdb"
_GEOIP_ASN_DB = Path(__file__).resolve().parent.parent / "data" / "geoip" / "GeoLite2-ASN.mmdb"


def _print_header(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def main() -> None:
    spark = get_spark()

    # --- Step 1: bronze ---------------------------------------------------
    _print_header("STEP 1 - BRONZE (landing zone -> parsed, stamped rows)")
    bronze_result = write_bronze("honeydb", spark)
    print(bronze_result)
    if bronze_result.written == 0 and bronze_result.quarantined == 0:
        print(
            "Nothing new in the landing zone. Run "
            "scripts/fetch_honeydb.py first, or check "
            "config/local.yaml's storage.landing path."
        )

    bronze_df = spark.read.format("delta").load(bronze_path("honeydb"))
    print(f"bronze_honeydb now has {bronze_df.count()} rows total.")
    bronze_df.select(
        "ingest_date", "source_type", "_record_hash", "honeydb.service", "honeydb.event"
    ).show(5, truncate=40)

    # --- Step 2: silver -----------------------------------------------------
    _print_header("STEP 2 - SILVER (bronze -> the unified event shape)")
    silver_df = map_honeydb(bronze_df)
    silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        silver_path("events")
    )
    silver_count = silver_df.count()
    print(f"{silver_count} silver rows written.")
    silver_df.select(
        "event_id", "event_time", "src_ip", "dst_port", "attack_category", "severity"
    ).show(10, truncate=20)

    # --- Step 3: gold ---------------------------------------------------------
    _print_header("STEP 3 - GOLD (attacker_profiles, attack_timeline)")
    with GeoEnricher(_GEOIP_CITY_DB, _GEOIP_ASN_DB) as geo_enricher:
        profiles = write_attacker_profiles(spark, geo_enricher=geo_enricher)
    total_profiles = profiles.count()
    resolved = profiles.filter(F.col("geo.latitude").isNotNull()).count()
    print(f"attacker_profiles: {total_profiles} rows (one per src_ip).")
    print(
        f"  geo: {resolved} of {total_profiles} src_ips resolved to a real lat/lon via "
        "MaxMind GeoLite2 - the rest are private/reserved ranges or outside the free "
        "tier's coverage, not an error (see ARCHITECTURE.md)."
    )
    profiles.orderBy(F.col("total_events").desc()).select(
        "src_ip", "total_events", "distinct_ports_hit", "geo.country", "geo.latitude",
        "geo.longitude", "attack_categories",
    ).show(10, truncate=40)

    timeline = write_attack_timeline(spark)
    print(f"attack_timeline: {timeline.count()} rows (one per hour/category/port bucket).")
    timeline.orderBy(F.col("hour").asc()).select(
        "hour", "attack_category", "dst_port", "event_count", "distinct_src_ip_count"
    ).show(10, truncate=20)

    # --- Step 4: train the anomaly detector ------------------------------------
    _print_header("STEP 4 - TRAIN (IsolationForest on silver's feature space)")
    training_result = train_anomaly(spark)
    print(training_result.summary())

    # --- Step 5: score events (IsolationForest + the port-scan rule) --------------
    _print_header("STEP 5 - SCORE (IsolationForest + threatlake.ml.rules.port_scan_rule)")
    scoring_result = write_scored_events(spark)
    print(scoring_result.summary())

    alerts = spark.read.format("delta").load(gold_path("ml_scores"))
    flagged = alerts.filter(F.col("is_anomaly")).join(
        silver_df.select("event_id", "src_ip", "dst_port", "attack_category"), on="event_id"
    )
    print(f"{flagged.count()} events currently flagged across all runs:")
    flagged.select("event_id", "src_ip", "dst_port", "alert_source", "anomaly_score").orderBy(
        F.col("anomaly_score").asc()
    ).show(20, truncate=20)

    stop_spark()
    print("\nPipeline run complete.")


if __name__ == "__main__":
    main()
