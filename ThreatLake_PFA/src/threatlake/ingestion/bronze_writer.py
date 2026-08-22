"""Bronze-layer batch ingestion for cowrie honeypot JSON logs.

Reads newline-delimited JSON from the landing directory
(``threatlake.common.paths.landing_path("cowrie")``), parses each line
against cowrie's explicit schema (``threatlake.ingestion.schemas``), and
appends it to the bronze Delta table, partitioned by ``ingest_date``.

Lines whose raw text is not valid JSON never reach bronze - they are
routed to the quarantine table instead, and never raise out of
``write_bronze``. Lines that *are* valid JSON but missing optional fields
parse normally (nulls for the missing fields): "malformed" here means
"not parseable JSON", not "not schema-complete".

Processed-file tracking: the exact set of files present in the landing
directory is captured once, up front, and used for both the read and the
post-write move - never re-listed - so a file that lands mid-batch is
left alone for the next run rather than partially processed. After a
successful write, every file in that set is *moved* (not deleted) to
``<landing>/cowrie/_processed/<ingest_date>/``, preserving them as an
audit trail while making the landing directory empty for the next run.
This gives at-least-once, not exactly-once, ingestion: if the process
dies between the Delta write succeeding and the move completing, the
same file is re-ingested on the next run, producing duplicate bronze
rows. That duplicate is harmless downstream - each silver ``event_id`` is
a content hash of the bronze row (see
``threatlake.transform.silver.schema``), so re-ingesting the same file
twice produces the same event_id both times rather than two silver rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from threatlake.common import fs
from threatlake.common.config import Settings, get_settings
from threatlake.common.paths import bronze_path, landing_path, quarantine_path
from threatlake.ingestion.bronze_transform import (
    finalize_bronze,
    finalize_quarantine,
    parse_source,
    split_quarantine,
    with_ingestion_metadata,
)
from threatlake.ingestion.schemas import SOURCE_TYPES, SchemaError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

__all__ = ["BronzeWriteResult", "write_bronze"]

_PROCESSED_SUBDIR = "_processed"


@dataclass(frozen=True)
class BronzeWriteResult:
    """Row counts from a single bronze ingestion batch."""

    source_type: str
    written: int
    quarantined: int


def _read_landing_lines(spark: SparkSession, file_paths: list[str]) -> DataFrame:
    """One row per line across exactly the given files."""
    return (
        spark.read.text(file_paths)
        .withColumnRenamed("value", "_raw")
        .withColumn("_source_file", F.input_file_name())
    )


def _move_to_processed(
    spark: SparkSession, file_paths: list[str], landing_dir: str, ingest_date: str
) -> None:
    processed_dir = f"{landing_dir}/{_PROCESSED_SUBDIR}/{ingest_date}"
    for path in file_paths:
        filename = path.rsplit("/", 1)[-1]
        fs.move(spark, path, f"{processed_dir}/{filename}")


def write_bronze(
    source_type: str, spark: SparkSession, settings: Settings | None = None
) -> BronzeWriteResult:
    """Ingest one batch from ``source_type``'s landing directory into bronze.

    Idempotent in effect, not in mechanism: once a file is moved to
    ``_processed/``, re-running this against the same landing directory
    finds no files and does no work (no Spark job is even triggered).
    """
    if source_type not in SOURCE_TYPES:
        expected = ", ".join(SOURCE_TYPES)
        raise SchemaError(f"Unknown source type {source_type!r}. Expected one of: {expected}")

    settings = settings or get_settings()
    landing_dir = landing_path(source_type, settings)

    file_paths = fs.list_files(spark, landing_dir)
    if not file_paths:
        return BronzeWriteResult(source_type=source_type, written=0, quarantined=0)

    ingested_at = datetime.now(UTC)
    ingest_date = ingested_at.date().isoformat()

    raw = _read_landing_lines(spark, file_paths)
    raw = with_ingestion_metadata(raw, source_type, ingested_at)
    parsed = parse_source(raw, source_type)
    good, bad = split_quarantine(parsed)

    bronze_df = finalize_bronze(good, source_type).cache()
    quarantine_df = finalize_quarantine(bad).cache()

    written = bronze_df.count()
    quarantined = quarantine_df.count()

    bronze_df.write.format("delta").mode("append").partitionBy("ingest_date").option(
        "mergeSchema", "true"
    ).save(bronze_path(source_type, settings))

    if quarantined:
        quarantine_df.write.format("delta").mode("append").partitionBy("ingest_date").option(
            "mergeSchema", "true"
        ).save(quarantine_path(source_type, settings))

    bronze_df.unpersist()
    quarantine_df.unpersist()

    _move_to_processed(spark, file_paths, landing_dir, ingest_date)

    return BronzeWriteResult(source_type=source_type, written=written, quarantined=quarantined)
