"""attacker_profiles - one row per attacker IP, aggregated across all silver history.

GRAIN: one row per ``src_ip``. Recomputed in full from the entire silver
table on each run (see ``threatlake.transform.gold.writer``) - an
attacker's ``first_seen``, top credentials, etc. can all be affected by
any new event for that IP anywhere in history, so there is no natural
incremental partition boundary for this table.

Columns:
  src_ip                    key.
  first_seen / last_seen     min/max(event_time).
  total_events                count(*).
  distinct_ports_hit           count(distinct dst_port).
  distinct_honeypots_hit        count(distinct source_type). Always 1 in
                              PFA (single-source, see ARCHITECTURE.md) -
                              kept as a real column rather than dropped,
                              since it is exactly what makes this table's
                              shape unchanged if a second source is added
                              later.
  top_credentials_tried          array of up to 5 ``{username, password,
                              count}`` structs, this IP's most frequent
                              credential pairs, ranked by count desc. Only
                              counts rows with both attempted_username AND
                              attempted_password non-null. Empty array
                              (not null) for an IP with no credential
                              attempts.
  attack_categories              sorted array of distinct attack_category
                              values this IP has triggered.
  geo                            ``{country, city, latitude, longitude, asn,
                              asn_org}``, looked up ONCE per distinct src_ip
                              via a GeoEnricher (threatlake.enrichment.geo),
                              not once per event. All fields null if no
                              GeoEnricher was supplied, or if this
                              particular IP isn't in MaxMind's database
                              (private/reserved ranges, or coverage gaps in
                              the free GeoLite2 tier - both normal, not
                              errors) - this table is fully usable without
                              geo data; it's best-effort enrichment, not a
                              hard dependency.

ThreatLake AI's version of this table also carries ``reputation_score``
(an AbuseIPDB lookup) - a second, genuinely optional enrichment PFA
leaves out since it needs a real (rate-limited, signup-gated) API key -
see ARCHITECTURE.md's "Future extensions" section.

Idempotent write: see threatlake.transform.gold.writer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as F

from threatlake.common.config import Settings
from threatlake.common.paths import silver_path
from threatlake.enrichment.geo import enrich_geo
from threatlake.transform.gold.writer import write_gold_table

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from threatlake.enrichment.geo import GeoEnricher

__all__ = ["build_attacker_profiles", "write_attacker_profiles"]

_TOP_CREDENTIALS_LIMIT = 5
_EMPTY_CREDENTIALS_TYPE = "array<struct<username:string,password:string,count:bigint>>"


def _top_credentials_by_ip(df: DataFrame) -> DataFrame:
    """One row per src_ip: its top _TOP_CREDENTIALS_LIMIT (username, password) pairs by count."""
    creds = df.filter(
        F.col("attempted_username").isNotNull() & F.col("attempted_password").isNotNull()
    )
    counted = creds.groupBy("src_ip", "attempted_username", "attempted_password").agg(
        F.count(F.lit(1)).alias("cred_count")
    )
    ranked = counted.withColumn(
        "rank",
        F.row_number().over(
            Window.partitionBy("src_ip").orderBy(
                F.col("cred_count").desc(), F.col("attempted_username")
            )
        ),
    ).filter(F.col("rank") <= _TOP_CREDENTIALS_LIMIT)

    return (
        ranked.groupBy("src_ip")
        .agg(
            F.sort_array(
                F.collect_list(F.struct("cred_count", "attempted_username", "attempted_password")),
                asc=False,
            ).alias("_ranked")
        )
        .withColumn(
            "top_credentials_tried",
            F.transform(
                F.col("_ranked"),
                lambda s: F.struct(
                    s["attempted_username"].alias("username"),
                    s["attempted_password"].alias("password"),
                    s["cred_count"].alias("count"),
                ),
            ),
        )
        .drop("_ranked")
    )


def build_attacker_profiles(
    silver_df: DataFrame,
    spark: SparkSession,
    geo_enricher: GeoEnricher | None = None,
) -> DataFrame:
    """Aggregate ``silver_df`` into the one-row-per-src_ip grain described above."""
    base = silver_df.filter(F.col("src_ip").isNotNull())

    profiles = base.groupBy("src_ip").agg(
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
        F.count(F.lit(1)).alias("total_events"),
        F.countDistinct("dst_port").alias("distinct_ports_hit"),
        F.countDistinct("source_type").alias("distinct_honeypots_hit"),
        F.sort_array(F.collect_set("attack_category")).alias("attack_categories"),
    )

    profiles = profiles.join(_top_credentials_by_ip(base), on="src_ip", how="left").withColumn(
        "top_credentials_tried",
        F.coalesce(F.col("top_credentials_tried"), F.array().cast(_EMPTY_CREDENTIALS_TYPE)),
    )

    if geo_enricher is not None:
        profiles = enrich_geo(profiles, spark, geo_enricher, ip_column="src_ip")
        profiles = profiles.withColumn(
            "geo",
            F.struct(
                F.col("geo_country").alias("country"),
                F.col("geo_city").alias("city"),
                F.col("geo_latitude").alias("latitude"),
                F.col("geo_longitude").alias("longitude"),
                F.col("geo_asn").alias("asn"),
                F.col("geo_asn_org").alias("asn_org"),
            ),
        ).drop("geo_country", "geo_city", "geo_latitude", "geo_longitude", "geo_asn", "geo_asn_org")
    else:
        # A present struct with null fields, not a null struct: this must
        # match the shape enrich_geo itself produces for an IP it can't
        # resolve, so `geo.latitude` etc. is always safe to select
        # downstream regardless of whether a GeoEnricher was supplied.
        profiles = profiles.withColumn(
            "geo",
            F.struct(
                F.lit(None).cast("string").alias("country"),
                F.lit(None).cast("string").alias("city"),
                F.lit(None).cast("double").alias("latitude"),
                F.lit(None).cast("double").alias("longitude"),
                F.lit(None).cast("int").alias("asn"),
                F.lit(None).cast("string").alias("asn_org"),
            ),
        )

    return profiles.select(
        "src_ip",
        "first_seen",
        "last_seen",
        "total_events",
        "distinct_ports_hit",
        "distinct_honeypots_hit",
        "top_credentials_tried",
        "attack_categories",
        "geo",
    )


def write_attacker_profiles(
    spark: SparkSession,
    settings: Settings | None = None,
    geo_enricher: GeoEnricher | None = None,
) -> DataFrame:
    """Read silver, build attacker_profiles, and idempotently overwrite the gold table."""
    silver_df = spark.read.format("delta").load(silver_path("events", settings))
    profiles = build_attacker_profiles(silver_df, spark, geo_enricher)
    write_gold_table(profiles, "attacker_profiles", settings)
    return profiles
