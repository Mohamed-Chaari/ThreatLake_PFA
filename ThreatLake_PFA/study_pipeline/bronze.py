"""Step 1: landing zone -> bronze.

What this does, in order:
  1. Read a folder of NDJSON files (one JSON object per line) as plain text.
  2. Stamp every line with "when did we ingest this" metadata.
  3. Parse the raw JSON text against Cowrie's known schema.
  4. Write the result as a Delta table.

This is deliberately simpler than the real project's bronze layer
(src/threatlake/ingestion/ in the real repo, not present in this copy):

  - ONE source, ONE table. The real project gives each honeypot type its
    own bronze table (so four independent honeypots can never contend for
    the same table's write lock) and reconstructs a "shared shape" for
    downstream code. With only cowrie here, there is nothing to keep
    separate, so we skip that whole layer.
  - Batch only. The real project also has a Structured Streaming version
    that watches the landing folder continuously. This script just reads
    whatever is in the folder once, when you run it.
  - NO quarantine table. The real project routes lines that fail to parse
    as JSON into a separate "quarantine" table, so a human can inspect
    them later without losing them, AND so a garbage line never reaches
    the mapper at all. This script does not build that table - instead
    it drops an unparseable line with one explicit, commented filter
    (see the bottom of build_bronze below). That filter is doing, by
    hand, in one line, what the real project's quarantine split does as
    a whole separate table - it exists here because of a real bug this
    script's own sample data tripped over the first time it was run
    without it: see that comment for exactly what happened and why.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from study_pipeline.cowrie_schema import SCHEMA as COWRIE_SCHEMA

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

#: The metadata columns every bronze row carries, on top of the parsed
#: Cowrie fields. Mirrors the real project's METADATA_COLUMNS, minus
#: _source_file (we don't bother tracking which exact input file a line
#: came from here - one more thing trimmed for "fits in your head").
_INGEST_DATE = "ingest_date"
_SOURCE_TYPE = "source_type"
_INGESTED_AT = "_ingested_at"
_RECORD_HASH = "_record_hash"
_RAW = "_raw"


def read_landing_lines(spark: SparkSession, landing_dir: str) -> DataFrame:
    """One row per line, across every file in ``landing_dir``.

    Spark's ``spark.read.text(...)`` already does exactly this: it reads
    every file in the folder and gives back one row per line, in a single
    column called "value". We just rename that column to "_raw" so its
    name matches what the rest of the pipeline (and the real project)
    calls it.
    """
    return spark.read.text(landing_dir).withColumnRenamed("value", _RAW)


def with_ingestion_metadata(df: DataFrame, ingested_at: datetime) -> DataFrame:
    """Stamp every row with source_type, ingest_date, and a content hash.

    ``ingested_at`` is computed ONCE by the caller and passed in, rather
    than each row calling "what time is it right now" independently.
    That way every row from this one run agrees on the same ingest_date,
    even if the read+write takes a few seconds to run.

    ``_record_hash`` is a SHA-256 hash of the raw line's exact text. This
    is what makes a row's identity depend on its CONTENT rather than on
    "which file, which line number" - two byte-identical lines (e.g. the
    same log line ingested twice by accident) always hash to the same
    value, which is what silver's event_id will be built from later.
    """
    return (
        df.withColumn(_SOURCE_TYPE, F.lit("cowrie"))
        .withColumn(_INGESTED_AT, F.lit(ingested_at))
        .withColumn(_INGEST_DATE, F.lit(ingested_at.date().isoformat()).cast("date"))
        .withColumn(_RECORD_HASH, F.sha2(F.col(_RAW), 256))
    )


def parse_cowrie_lines(df: DataFrame) -> DataFrame:
    """Parse ``_raw`` JSON text into a structured "cowrie" column.

    Spark's PERMISSIVE mode (the default for from_json) never raises on
    bad input - a line that isn't valid JSON at all still produces a
    "cowrie" STRUCT, it just comes back with every field inside it null
    (this is easy to get wrong by guessing - see build_bronze below for
    where that surprised this exact script the first time it ran).
    """
    return df.withColumn("cowrie", F.from_json(F.col(_RAW), COWRIE_SCHEMA))


def build_bronze(spark: SparkSession, landing_dir: str, bronze_path: str) -> DataFrame:
    """Run the three steps above and write the result as a Delta table.

    Returns the DataFrame that was written, so the caller can print it
    or count it without a second read from disk.
    """
    ingested_at = datetime.now(UTC)

    raw = read_landing_lines(spark, landing_dir)
    stamped = with_ingestion_metadata(raw, ingested_at)
    parsed = parse_cowrie_lines(stamped)

    bronze_df = parsed.select(
        _INGEST_DATE, _SOURCE_TYPE, _INGESTED_AT, _RECORD_HASH, _RAW, "cowrie"
    )

    # Drop rows whose "cowrie" struct is garbage: eventid null means the
    # line didn't parse as JSON at all (see parse_cowrie_lines above).
    #
    # THIS LINE EXISTS BECAUSE OF A REAL BUG, found by running this exact
    # script: without it, a garbage row reaches map_cowrie in silver.py
    # with eventid = null. map_cowrie checks things like
    # `eventid.isin("cowrie.login.success", "cowrie.login.failed")` to
    # decide credentials_attempted (true/false) - but in SQL,
    # `NULL.isin(...)` isn't false, it's NULL ("unknown" - three-valued
    # logic). That NULL then fails conform_to_silver_schema's check that
    # credentials_attempted can never be null, and the whole run crashes.
    #
    # In the real project this never happens: a garbage line gets routed
    # to the quarantine table (threatlake.ingestion.bronze_transform.
    # split_quarantine) BEFORE it ever reaches a mapper, so map_cowrie
    # never has to defend against eventid being null. Without a
    # quarantine step, this one filter is standing in for that guarantee.
    bronze_df = bronze_df.filter(F.col("cowrie.eventid").isNotNull())

    # append, not overwrite: this mirrors the real project's bronze layer,
    # which is a growing log of everything ever ingested, not a table
    # that gets replaced every run.
    bronze_df.write.format("delta").mode("append").save(bronze_path)

    return bronze_df
