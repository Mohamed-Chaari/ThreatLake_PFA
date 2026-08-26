"""threatlake.transform.silver.honeydb.map_honeydb: field-by-field
mapping and the CONNECT/service_probe category-severity split.

Bronze rows are built with an EXPLICIT schema (metadata columns +
``source_schema("honeydb")`` for the nested struct), not left to Spark's
own type inference: several fields below are None in every test row,
which Spark's inference can't assign a type to on its own.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from threatlake.ingestion.schemas import source_schema
from threatlake.transform.silver.honeydb import map_honeydb

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_BRONZE_SCHEMA = StructType(
    [
        StructField("ingest_date", DateType(), True),
        StructField("source_type", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_record_hash", StringType(), True),
        StructField("_raw", StringType(), True),
        StructField("honeydb", source_schema("honeydb"), True),
    ]
)


def _bronze_row(**honeydb_fields: object) -> dict[str, object]:
    """A finalize_bronze()-shaped row: metadata + a populated 'honeydb' struct."""
    defaults: dict[str, object] = dict.fromkeys(f.name for f in source_schema("honeydb").fields)
    defaults["date_time"] = "2026-08-02 09:00:00.000"
    defaults["session"] = "sess-a1"
    defaults["protocol"] = "TCP"
    defaults["event"] = "CONNECT"
    defaults["service"] = "SSH"
    defaults["remote_host"] = "198.51.100.23"
    defaults["data"] = ""
    defaults["bytes"] = 0
    defaults["data_hash"] = "d41d8cd98f00b204e9800998ecf8427e"
    defaults.update(honeydb_fields)
    return {
        "ingest_date": date(2026, 8, 2),
        "source_type": "honeydb",
        "_ingested_at": None,
        "_source_file": "file:///landing/honeydb/x.ndjson",
        "_record_hash": "abc123",
        "_raw": '{"event": "CONNECT"}',
        "honeydb": defaults,
    }


def _bronze_df(spark: SparkSession, *rows: dict[str, object]) -> DataFrame:
    return spark.createDataFrame(list(rows), schema=_BRONZE_SCHEMA)


def test_map_honeydb_filters_to_honeydb_rows(spark: SparkSession) -> None:
    non_honeydb = {**_bronze_row(), "source_type": "other", "honeydb": None}
    df = _bronze_df(spark, _bronze_row(), non_honeydb)
    mapped = map_honeydb(df)
    assert mapped.count() == 1


def test_map_honeydb_maps_connect_fields(spark: SparkSession) -> None:
    df = _bronze_df(spark, _bronze_row())
    row = map_honeydb(df).collect()[0]
    assert row["src_ip"] == "198.51.100.23"
    assert row["protocol"] == "tcp"
    assert row["source_event_type"] == "SSH.CONNECT"
    assert row["session_id"] == "sess-a1"
    assert row["attack_category"] == "connection"
    assert row["severity"] == 1
    assert row["dst_ip"] is None
    assert row["dst_port"] is None
    assert row["src_port"] is None


def test_map_honeydb_never_reports_credentials(spark: SparkSession) -> None:
    """The community sensor-data feed never surfaces submitted credentials -
    unlike Cowrie, credentials_attempted is always False, regardless of event."""
    df = _bronze_df(spark, _bronze_row(event="RX", service="SSH"))
    row = map_honeydb(df).collect()[0]
    assert row["credentials_attempted"] is False
    assert row["attempted_username"] is None
    assert row["attempted_password"] is None


def test_map_honeydb_non_connect_event_is_service_probe(spark: SparkSession) -> None:
    df = _bronze_df(spark, _bronze_row(event="RX", service="FTP"))
    row = map_honeydb(df).collect()[0]
    assert row["attack_category"] == "service_probe"
    assert row["severity"] == 2  # noqa: PLR2004
    assert row["source_event_type"] == "FTP.RX"


def test_map_honeydb_payload_hash_is_data_hash(spark: SparkSession) -> None:
    df = _bronze_df(spark, _bronze_row(event="TX", data_hash="deadbeefdeadbeefdeadbeefdeadbeef"))
    row = map_honeydb(df).collect()[0]
    assert row["payload_hash"] == "deadbeefdeadbeefdeadbeefdeadbeef"


def test_map_honeydb_event_id_is_deterministic_hash(spark: SparkSession) -> None:
    """Re-mapping the same bronze row twice produces the same event_id -
    what makes exact-duplicate re-ingestion harmless downstream."""
    df = _bronze_df(spark, _bronze_row(), _bronze_row())
    ids = {row["event_id"] for row in map_honeydb(df).collect()}
    assert len(ids) == 1


def test_map_honeydb_raw_ref_resolves_back_to_bronze(spark: SparkSession) -> None:
    df = _bronze_df(spark, _bronze_row())
    row = map_honeydb(df).collect()[0]
    assert row["raw_ref"] == "honeydb:abc123"


def test_map_honeydb_event_time_parses_millisecond_precision(spark: SparkSession) -> None:
    df = _bronze_df(spark, _bronze_row(date_time="2026-08-02 09:00:01.500"))
    row = map_honeydb(df).collect()[0]
    assert row["event_time"].isoformat() == "2026-08-02T09:00:01.500000"
