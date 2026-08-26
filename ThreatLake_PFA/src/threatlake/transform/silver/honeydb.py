"""Bronze HoneyDB rows -> the unified silver event model.

Mapping (see threatlake.transform.silver.schema for shared field semantics):

  event_time            to_timestamp(honeydb.date_time) - HoneyDB's
                          payload carries no explicit timezone marker;
                          treated as UTC consistent with every other
                          timestamp this project handles (see
                          threatlake.common.spark's UTC verification) and
                          with standard honeypot/UTC logging convention -
                          not independently confirmed against HoneyDB's
                          own docs (their public API reference renders no
                          static content to read), flagged here rather
                          than asserted quietly.
  source_event_type     f"{service}.{event}" (e.g. "SSH.CONNECT",
                          "FTP.RX") - HoneyDB's own vocabulary, case
                          preserved exactly as the API returns it.
  src_ip                honeydb.remote_host - the attacker.
  dst_ip / dst_port      HoneyDB's sensor-data feed never reports which
                          local address/port a sensor is listening on -
                          left null rather than guessed from `service`
                          (e.g. inferring port 22 from "SSH" would be an
                          invented fact, not an observed one).
  src_port               Not present in the feed either - null.
  protocol               lower(honeydb.protocol) - "tcp"/"udp".
  session_id             honeydb.session
  credentials_attempted  Always False: the community sensor-data feed
                          never surfaces submitted credentials (unlike
                          Cowrie's login.success/login.failed), so
                          attempted_username/attempted_password are
                          always null too - a real, documented gap
                          against threatlake.transform.silver.cowrie's
                          shape, not a bug.
  payload_hash            honeydb.data_hash
  description             null - HoneyDB has no human-written free-text
                          field analogous to Cowrie's `message`; left
                          null rather than synthesizing one from other
                          fields and presenting it as source data.

attack_category / severity - a flat, original, intentionally simple
two-way split (see below); HoneyDB has no native concept of either, the
same position threatlake.transform.silver.cowrie is in for its own
eight-way mapping.

Dropped entirely (available via raw_ref -> bronze._raw): date, time,
millisecond (all redundant with date_time), data/bytes (the raw hex
payload and its length - real content, but out of scope for the unified
event model the same way Cowrie's ttylog/size are).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from threatlake.transform.silver.schema import conform_to_silver_schema

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = ["map_honeydb"]

#: HoneyDB's own event taxonomy has no severity concept; the two-way
#: split below is an original, documented, intentionally simple judgment
#: call (same honesty as threatlake.transform.silver.cowrie's
#: _CATEGORY_SEVERITY table, just flatter because HoneyDB's feed gives
#: far less to distinguish on - no login/file-transfer/command events,
#: only CONNECT/INFO/RX/TX against an emulated service).
_CONNECT_EVENT = "CONNECT"
_CONNECT_SEVERITY = 1
_PROBE_SEVERITY = 2


def map_honeydb(bronze_df: DataFrame) -> DataFrame:
    """Map bronze rows with ``source_type = 'honeydb'`` to the unified silver model."""
    df = bronze_df.filter((F.col("source_type") == "honeydb") & F.col("honeydb").isNotNull())

    is_connect = F.col("honeydb.event") == _CONNECT_EVENT
    attack_category = F.when(is_connect, F.lit("connection")).otherwise(F.lit("service_probe"))
    severity = F.when(is_connect, F.lit(_CONNECT_SEVERITY)).otherwise(F.lit(_PROBE_SEVERITY))

    mapped = df.select(
        F.sha2(F.concat(F.col("source_type"), F.lit(":"), F.col("_record_hash")), 256).alias(
            "event_id"
        ),
        F.to_timestamp(F.col("honeydb.date_time"), "yyyy-MM-dd HH:mm:ss.SSS").alias("event_time"),
        F.col("ingest_date").alias("ingest_date"),
        F.col("source_type").alias("source_type"),
        F.concat(F.col("honeydb.service"), F.lit("."), F.col("honeydb.event")).alias(
            "source_event_type"
        ),
        F.col("honeydb.remote_host").alias("src_ip"),
        F.lit(None).cast("int").alias("src_port"),
        F.lit(None).cast("string").alias("dst_ip"),
        F.lit(None).cast("int").alias("dst_port"),
        F.lower(F.col("honeydb.protocol")).alias("protocol"),
        attack_category.alias("attack_category"),
        severity.alias("severity"),
        F.col("honeydb.session").alias("session_id"),
        F.lit(False).alias("credentials_attempted"),
        F.lit(None).cast("string").alias("attempted_username"),
        F.lit(None).cast("string").alias("attempted_password"),
        F.col("honeydb.data_hash").alias("payload_hash"),
        F.lit(None).cast("string").alias("description"),
        F.concat(F.col("source_type"), F.lit(":"), F.col("_record_hash")).alias("raw_ref"),
    )
    return conform_to_silver_schema(mapped)
