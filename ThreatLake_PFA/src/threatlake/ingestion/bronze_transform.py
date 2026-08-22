"""Pure bronze transforms for cowrie ingestion.

Every function here is a plain column-expression transform - no
``.count()``, ``.collect()``, ``.first()``, or other Spark action, and no
I/O. Keeping this module action-free is what makes it safe to test
without a live SparkSession action being forced at import time, and keeps
the parse/quarantine-split logic in one place regardless of how many call
sites end up needing it.

ThreatLake AI runs this same shape of transform for four honeypot sources
sharing one silver-facing "unified bronze shape" (metadata + all four
sources' struct columns, three of them always null on any given row) -
see that project's own ``expand_to_unified_bronze_shape``. With exactly
one source (cowrie), that reconstruction step has nothing to do: bronze
rows are already metadata + a single populated ``cowrie`` struct column,
which is exactly the shape ``threatlake.transform.silver.cowrie.map_cowrie``
expects, so it is not carried over here.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from threatlake.ingestion.schemas import source_schema

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = [
    "CORRUPT_COLUMN",
    "METADATA_COLUMNS",
    "finalize_bronze",
    "finalize_quarantine",
    "parse_source",
    "split_quarantine",
    "with_ingestion_metadata",
]

#: Column Spark's PERMISSIVE JSON mode populates with the raw text of a line
#: that failed to parse against the source schema, and null otherwise.
CORRUPT_COLUMN = "_corrupt_record"

METADATA_COLUMNS = (
    "ingest_date",
    "source_type",
    "_ingested_at",
    "_source_file",
    "_record_hash",
    "_raw",
)


def with_ingestion_metadata(df: DataFrame, source_type: str, ingested_at: datetime) -> DataFrame:
    """Stamp every row with its source, ingestion instant, and content hash.

    ``ingested_at`` is a single Python-computed instant for the whole
    batch, not ``current_timestamp()`` per row - every row in one run
    should agree on one ``ingest_date``.
    """
    return (
        df.withColumn("source_type", F.lit(source_type))
        .withColumn("_ingested_at", F.lit(ingested_at))
        .withColumn("ingest_date", F.to_date(F.col("_ingested_at")))
        .withColumn("_record_hash", F.sha2(F.col("_raw"), 256))
    )


def parse_source(df: DataFrame, source_type: str) -> DataFrame:
    """Parse `_raw` against the source's schema, flagging unparseable JSON."""
    schema = source_schema(source_type)
    corrupt_field = StructField(CORRUPT_COLUMN, StringType(), True)
    schema_with_corrupt = StructType([*schema.fields, corrupt_field])
    options = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_COLUMN}
    return df.withColumn("_parsed", F.from_json(F.col("_raw"), schema_with_corrupt, options))


def split_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separate rows whose raw text failed to parse as JSON at all."""
    malformed = F.col("_parsed").isNull() | F.col(f"_parsed.{CORRUPT_COLUMN}").isNotNull()
    return df.filter(~malformed), df.filter(malformed)


def finalize_bronze(df: DataFrame, source_type: str) -> DataFrame:
    """Project to metadata + ``source_type``'s own populated struct column -
    the physical shape of the bronze table, and exactly what
    ``threatlake.transform.silver.cowrie.map_cowrie`` expects to read.
    """
    parsed_field_names = [f.name for f in source_schema(source_type).fields]
    populated = F.struct(*[F.col(f"_parsed.{name}").alias(name) for name in parsed_field_names])
    return df.select(*METADATA_COLUMNS, populated.alias(source_type))


def finalize_quarantine(df: DataFrame) -> DataFrame:
    return df.select(*METADATA_COLUMNS, F.col(f"_parsed.{CORRUPT_COLUMN}").alias("_error"))
