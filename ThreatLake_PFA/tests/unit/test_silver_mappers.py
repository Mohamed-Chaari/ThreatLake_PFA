"""threatlake.transform.silver.cowrie.map_cowrie: field-by-field mapping,
category/severity assignment, and the login/file-event conditionals.

Bronze rows are built with an EXPLICIT schema (metadata columns +
``source_schema("cowrie")`` for the nested struct), not left to Spark's
own type inference: several fields below are None in every test row,
which Spark's inference can't assign a type to on its own.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from threatlake.ingestion.schemas import source_schema
from threatlake.transform.silver.cowrie import map_cowrie

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
        StructField("cowrie", source_schema("cowrie"), True),
    ]
)


def _bronze_row(**cowrie_fields: object) -> dict[str, object]:
    """A finalize_bronze()-shaped row: metadata + a populated 'cowrie' struct."""
    defaults: dict[str, object] = dict.fromkeys(f.name for f in source_schema("cowrie").fields)
    defaults["eventid"] = "cowrie.session.connect"
    defaults["sensor"] = "tpot-01"
    defaults["timestamp"] = "2026-08-02T09:00:00.000000Z"
    defaults["session"] = "s1"
    defaults["src_ip"] = "198.51.100.23"
    defaults["src_port"] = 51422
    defaults["dst_ip"] = "10.0.0.5"
    defaults["dst_port"] = 2222
    defaults["protocol"] = "ssh"
    defaults.update(cowrie_fields)
    return {
        "ingest_date": date(2026, 8, 2),
        "source_type": "cowrie",
        "_ingested_at": None,
        "_source_file": "file:///landing/cowrie/x.ndjson",
        "_record_hash": "abc123",
        "_raw": '{"eventid": "cowrie.session.connect"}',
        "cowrie": defaults,
    }


def _bronze_df(spark: SparkSession, *rows: dict[str, object]) -> DataFrame:
    return spark.createDataFrame(list(rows), schema=_BRONZE_SCHEMA)


def test_map_cowrie_filters_to_cowrie_rows(spark: SparkSession) -> None:
    non_cowrie = {**_bronze_row(), "source_type": "other", "cowrie": None}
    df = _bronze_df(spark, _bronze_row(), non_cowrie)
    mapped = map_cowrie(df)
    assert mapped.count() == 1


def test_map_cowrie_maps_connect_fields(spark: SparkSession) -> None:
    df = _bronze_df(spark, _bronze_row())
    row = map_cowrie(df).collect()[0]
    assert row["src_ip"] == "198.51.100.23"
    assert row["dst_port"] == 2222
    assert row["protocol"] == "ssh"
    assert row["attack_category"] == "connection"
    assert row["severity"] == 1
    assert row["credentials_attempted"] is False
    assert row["attempted_username"] is None


def test_map_cowrie_login_failed_is_low_severity_credential_access(spark: SparkSession) -> None:
    df = _bronze_df(
        spark, _bronze_row(eventid="cowrie.login.failed", username="admin", password="123456")
    )
    row = map_cowrie(df).collect()[0]
    assert row["attack_category"] == "credential_access"
    assert row["severity"] == 2  # noqa: PLR2004
    assert row["credentials_attempted"] is True
    assert row["attempted_username"] == "admin"
    assert row["attempted_password"] == "123456"


def test_map_cowrie_login_success_is_critical(spark: SparkSession) -> None:
    df = _bronze_df(
        spark, _bronze_row(eventid="cowrie.login.success", username="root", password="toor")
    )
    row = map_cowrie(df).collect()[0]
    assert row["attack_category"] == "credential_access"
    assert row["severity"] == 5  # noqa: PLR2004


def test_map_cowrie_file_download_carries_payload_hash(spark: SparkSession) -> None:
    df = _bronze_df(spark, _bronze_row(eventid="cowrie.session.file_download", shasum="deadbeef"))
    row = map_cowrie(df).collect()[0]
    assert row["attack_category"] == "malware_delivery"
    assert row["payload_hash"] == "deadbeef"
    # File events are not login events - no credentials fields populated.
    assert row["credentials_attempted"] is False


def test_map_cowrie_unknown_eventid_falls_back_to_other(spark: SparkSession) -> None:
    df = _bronze_df(spark, _bronze_row(eventid="cowrie.client.version"))
    row = map_cowrie(df).collect()[0]
    assert row["attack_category"] == "other"
    assert row["severity"] == 2  # noqa: PLR2004


def test_map_cowrie_event_id_is_deterministic_hash(spark: SparkSession) -> None:
    """Re-mapping the same bronze row twice produces the same event_id -
    what makes exact-duplicate re-ingestion harmless downstream.
    """
    df = _bronze_df(spark, _bronze_row(), _bronze_row())
    ids = {row["event_id"] for row in map_cowrie(df).collect()}
    assert len(ids) == 1


def test_map_cowrie_raw_ref_resolves_back_to_bronze(spark: SparkSession) -> None:
    df = _bronze_df(spark, _bronze_row())
    row = map_cowrie(df).collect()[0]
    assert row["raw_ref"] == "cowrie:abc123"
