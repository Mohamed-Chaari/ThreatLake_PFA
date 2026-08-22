"""threatlake.ingestion.bronze_transform: parse/quarantine-split, and the
final bronze/quarantine projections - pure column-expression transforms,
no I/O."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from threatlake.ingestion.bronze_transform import (
    finalize_bronze,
    finalize_quarantine,
    parse_source,
    split_quarantine,
    with_ingestion_metadata,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_INGESTED_AT = datetime(2026, 8, 2, 12, 0, 0, tzinfo=UTC)


def test_with_ingestion_metadata_stamps_every_row(spark: SparkSession) -> None:
    df = spark.createDataFrame([("line-one",), ("line-two",)], ["_raw"])
    stamped = with_ingestion_metadata(df, "cowrie", _INGESTED_AT)
    rows = stamped.collect()
    assert all(r["source_type"] == "cowrie" for r in rows)
    assert all(r["ingest_date"].isoformat() == "2026-08-02" for r in rows)
    # Content hash is deterministic and distinguishes different raw lines.
    assert rows[0]["_record_hash"] != rows[1]["_record_hash"]


def test_split_quarantine_separates_malformed_json(spark: SparkSession) -> None:
    good_line = '{"eventid": "cowrie.session.connect", "src_ip": "1.2.3.4"}'
    bad_line = "not json at all"
    df = spark.createDataFrame([(good_line,), (bad_line,)], ["_raw"])
    df = with_ingestion_metadata(df, "cowrie", _INGESTED_AT)
    df = parse_source(df, "cowrie")
    good, bad = split_quarantine(df)
    assert good.count() == 1
    assert bad.count() == 1
    assert good.first()["_raw"] == good_line
    assert bad.first()["_raw"] == bad_line


def test_finalize_bronze_projects_metadata_and_source_struct(spark: SparkSession) -> None:
    line = '{"eventid": "cowrie.session.connect", "src_ip": "1.2.3.4"}'
    df = spark.createDataFrame([(line, "file:///landing/cowrie/x.ndjson")], ["_raw", "_source_file"])
    df = with_ingestion_metadata(df, "cowrie", _INGESTED_AT)
    df = parse_source(df, "cowrie")
    good, _bad = split_quarantine(df)
    bronze = finalize_bronze(good, "cowrie")
    row = bronze.collect()[0]
    assert row["cowrie"]["eventid"] == "cowrie.session.connect"
    assert row["cowrie"]["src_ip"] == "1.2.3.4"
    assert row["source_type"] == "cowrie"


def test_finalize_quarantine_keeps_the_error_and_raw_text(spark: SparkSession) -> None:
    bad_line = "definitely not json"
    df = spark.createDataFrame([(bad_line, "file:///landing/cowrie/x.ndjson")], ["_raw", "_source_file"])
    df = with_ingestion_metadata(df, "cowrie", _INGESTED_AT)
    df = parse_source(df, "cowrie")
    _good, bad = split_quarantine(df)
    quarantine = finalize_quarantine(bad)
    row = quarantine.collect()[0]
    assert row["_raw"] == bad_line
    assert row["_error"] is not None
