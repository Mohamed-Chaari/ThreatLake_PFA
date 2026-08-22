"""Bronze Cowrie (SSH/Telnet) rows -> the unified silver event model.

Mapping (see threatlake.transform.silver.schema for shared field semantics):

  event_time            cowrie.timestamp
  source_event_type     cowrie.eventid (e.g. "cowrie.login.failed")
  src_ip/src_port        cowrie.src_ip/src_port - attacker
  dst_ip/dst_port        cowrie.dst_ip/dst_port - the honeypot. Only populated
                          on connection-context events (session.connect); null
                          elsewhere per Cowrie's own schema (see
                          config/schema/cowrie.py) - join on session_id to
                          recover it for a specific session's other events.
  protocol               lower(cowrie.protocol) - "ssh"/"telnet"
  session_id             cowrie.session
  credentials_attempted  true for cowrie.login.success / cowrie.login.failed
  attempted_username/    cowrie.username/password on those two eventids.
    password             Password auth only: pubkey-auth attempts carry
                          fingerprint/key instead, which are dropped here
                          (available via raw_ref).
  payload_hash            cowrie.shasum on file_download/file_upload
  description             cowrie.message (already human-written by Cowrie)

attack_category / severity - original, documented taxonomy (see below);
Cowrie has no native concept of either.

Dropped entirely (available via raw_ref -> bronze._raw): sensor, late,
realm, outfile/destfile/duplicate, ttylog, size, key/fingerprint.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from threatlake.transform.silver.schema import conform_to_silver_schema

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

__all__ = ["map_cowrie"]

_LOGIN_EVENTS = ("cowrie.login.success", "cowrie.login.failed")
_FILE_EVENTS = ("cowrie.session.file_download", "cowrie.session.file_upload")

# eventid -> (attack_category, severity). severity: 5=critical .. 1=informational.
_CATEGORY_SEVERITY = {
    "cowrie.session.connect": ("connection", 1),
    "cowrie.login.success": ("credential_access", 5),  # attacker got in
    "cowrie.login.failed": ("credential_access", 2),
    "cowrie.command.input": ("command_execution", 4),
    "cowrie.session.file_download": ("malware_delivery", 4),
    "cowrie.session.file_upload": ("malware_delivery", 4),
    "cowrie.session.closed": ("session_summary", 1),
    "cowrie.log.closed": ("session_summary", 1),
}
_DEFAULT_CATEGORY_SEVERITY = ("other", 2)


def _category_severity_columns() -> tuple[Column, Column]:
    # Each eventid is mutually exclusive, so iteration order doesn't matter -
    # every branch below only ever overrides the still-default value.
    category = F.lit(_DEFAULT_CATEGORY_SEVERITY[0])
    severity = F.lit(_DEFAULT_CATEGORY_SEVERITY[1])
    for eventid, (cat, sev) in _CATEGORY_SEVERITY.items():
        is_match = F.col("cowrie.eventid") == eventid
        category = F.when(is_match, F.lit(cat)).otherwise(category)
        severity = F.when(is_match, F.lit(sev)).otherwise(severity)
    return category, severity


def map_cowrie(bronze_df: DataFrame) -> DataFrame:
    """Map bronze rows with ``source_type = 'cowrie'`` to the unified silver model."""
    df = bronze_df.filter((F.col("source_type") == "cowrie") & F.col("cowrie").isNotNull())

    is_login_event = F.col("cowrie.eventid").isin(*_LOGIN_EVENTS)
    is_file_event = F.col("cowrie.eventid").isin(*_FILE_EVENTS)
    attack_category, severity = _category_severity_columns()

    mapped = df.select(
        F.sha2(F.concat(F.col("source_type"), F.lit(":"), F.col("_record_hash")), 256).alias(
            "event_id"
        ),
        F.to_timestamp(F.col("cowrie.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").alias(
            "event_time"
        ),
        F.col("ingest_date").alias("ingest_date"),
        F.col("source_type").alias("source_type"),
        F.col("cowrie.eventid").alias("source_event_type"),
        F.col("cowrie.src_ip").alias("src_ip"),
        F.col("cowrie.src_port").alias("src_port"),
        F.col("cowrie.dst_ip").alias("dst_ip"),
        F.col("cowrie.dst_port").alias("dst_port"),
        F.lower(F.col("cowrie.protocol")).alias("protocol"),
        attack_category.alias("attack_category"),
        severity.alias("severity"),
        F.col("cowrie.session").alias("session_id"),
        is_login_event.alias("credentials_attempted"),
        F.when(is_login_event, F.col("cowrie.username")).alias("attempted_username"),
        F.when(is_login_event, F.col("cowrie.password")).alias("attempted_password"),
        F.when(is_file_event, F.col("cowrie.shasum")).alias("payload_hash"),
        F.col("cowrie.message").alias("description"),
        F.concat(F.col("source_type"), F.lit(":"), F.col("_record_hash")).alias("raw_ref"),
    )
    return conform_to_silver_schema(mapped)
