"""The unified security event model - the one schema every honeypot source
gets mapped into.

See the module docstrings in ``threatlake.transform.silver.<source>`` for the
field-by-field mapping and drop rationale for each of the four sources. The
short version, documented once here rather than repeated in each mapper:

event_id
    ``sha2(source_type || ':' || _record_hash, 256)``. A content hash, not a
    random UUID: re-processing the same bronze row (e.g. after the
    at-least-once re-ingestion gap documented in
    ``threatlake.ingestion.bronze_writer``) always produces the same
    event_id, which is what makes exact-duplicate dedup
    (``threatlake.transform.silver.dedup``) possible.

event_time
    The event's own timestamp (UTC), not ingestion time. Null when a source's
    timestamp is missing or unparseable - the row is still kept, just without
    a confirmed event time.

attack_category
    A small, original, coarse taxonomy chosen for cross-source filtering, NOT
    derived from any external standard: recon, credential_access,
    command_execution, malware_delivery, network_intrusion, service_probe,
    service_start, session_summary, connection, network_flow, other. Suricata
    keeps its own much richer rule category/signature text in `description`;
    attack_category only says "an alert fired" (network_intrusion) so its
    vocabulary stays comparable to the other three sources.

severity
    Normalized to 1-5, 5 = most severe/critical, 1 = informational. Chosen
    deliberately opposite to Suricata's native scale, where 1 is the highest
    priority - seeing "severity 5" mean "worst" is the more common SOC/
    dashboard convention, so Suricata's severity is inverted on the way in
    (native 1 -> 5, 2 -> 3, 3 -> 1). The other three sources have no native
    severity concept at all; their mapping is an original, documented,
    intentionally simple per-eventid/connection-type judgment call - see each
    mapper.

credentials_attempted / attempted_username / attempted_password
    credentials_attempted is always true/false (never null): every source
    either has a concept of a credential-submission event or firmly doesn't
    (e.g. Suricata's alert/flow event types never carry one). The attempted
    username/password are a superset addition beyond the requested minimum
    fields - dropping attacker-submitted credentials would be a real loss on
    a project where one of the four sources (Heralding) exists specifically
    to harvest them. Note: Heralding's *confirmed* schema (see
    config/schema/heralding.py) only carries a count of auth attempts, not
    the credential values themselves - so credentials_attempted can be true
    for a Heralding row while attempted_username/password stay null.

payload_hash
    A hash of a captured file/binary artifact, sourced from the honeypot's
    own hash (never recomputed here - bronze never sees the raw bytes, only
    parsed metadata). Populated only for Cowrie file_download/file_upload
    events (`shasum`). Null for Dionaea despite malware capture being its
    core purpose: the confirmed Dionaea schema (config/schema/dionaea.py)
    does not include a download/artifact-hash field - see that module's
    docstring for why it was left out rather than guessed.

raw_ref
    ``source_type || ':' || _record_hash`` - unhashed and directly usable to
    look up the exact bronze row and its full raw JSON:
    ``SELECT * FROM bronze.raw_events WHERE source_type = split(raw_ref, ':')[0]
    AND _record_hash = split(raw_ref, ':')[1]``.

Dropped from every source (available via raw_ref, not carried into silver):
free-text fields with no analytic structure beyond what `description`
already summarizes (Suricata's full alert.metadata map, Cowrie's `message`,
Dionaea's p0f fingerprint detail, Suricata's raw L4 `proto` once app_proto is
known). None of this is destroyed - raw_ref always resolves back to the full
bronze row, `_raw` included.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = ["SILVER_EVENT_SCHEMA", "SilverSchemaError", "conform_to_silver_schema"]


class SilverSchemaError(RuntimeError):
    """Raised when mapped rows violate the unified schema's NOT NULL columns."""


#: Coarse, original cross-source attack taxonomy. See module docstring.
ATTACK_CATEGORIES = frozenset(
    {
        "recon",
        "credential_access",
        "command_execution",
        "malware_delivery",
        "network_intrusion",
        "service_probe",
        "service_start",
        "session_summary",
        "connection",
        "network_flow",
        "other",
    }
)

SILVER_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", TimestampType(), True),
        StructField("ingest_date", DateType(), False),
        StructField("source_type", StringType(), False),
        StructField("source_event_type", StringType(), True),
        StructField("src_ip", StringType(), True),
        StructField("src_port", IntegerType(), True),
        StructField("dst_ip", StringType(), True),
        StructField("dst_port", IntegerType(), True),
        StructField("protocol", StringType(), True),
        StructField("attack_category", StringType(), True),
        StructField("severity", IntegerType(), True),
        StructField("session_id", StringType(), True),
        StructField("credentials_attempted", BooleanType(), False),
        StructField("attempted_username", StringType(), True),
        StructField("attempted_password", StringType(), True),
        StructField("payload_hash", StringType(), True),
        StructField("description", StringType(), True),
        StructField("raw_ref", StringType(), False),
    ]
)


def conform_to_silver_schema(df: DataFrame) -> DataFrame:
    """Select and cast ``df`` to exactly :data:`SILVER_EVENT_SCHEMA` - column
    names, order, and types - then verify the schema's NOT NULL columns are
    actually non-null.

    That second step matters: ``.cast()`` only changes a column's *type*.
    Spark infers a DataFrame's nullability from the expression tree that
    produced it, not from a target StructType you select against - so
    without an explicit check, a mapper bug that leaves e.g. ``event_id``
    null would silently pass through instead of failing where it happened.
    """
    conformed = df.select(
        *[F.col(field.name).cast(field.dataType).alias(field.name) for field in SILVER_EVENT_SCHEMA]
    )
    _assert_not_null(conformed)
    return conformed


def _assert_not_null(df: DataFrame) -> None:
    required = [field.name for field in SILVER_EVENT_SCHEMA if not field.nullable]
    if not required:
        return
    null_counts = df.select(
        *[F.sum(F.col(name).isNull().cast("int")).alias(name) for name in required]
    ).first()
    assert null_counts is not None  # noqa: S101 - always exactly one row from an unconditional select
    violations = {name: null_counts[name] for name in required if null_counts[name]}
    if violations:
        raise SilverSchemaError(
            f"Silver rows violate NOT NULL columns: {violations}. This is a mapper "
            "bug, not bad input data - every mapper must always populate these columns."
        )
