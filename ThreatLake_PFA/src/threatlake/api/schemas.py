"""Pydantic response models for every API route.

Hand-written, not auto-derived from the Delta schemas
(threatlake.transform.silver.schema.SILVER_EVENT_SCHEMA,
threatlake.ml.score_events.SCORES_SCHEMA, threatlake.transform.gold's own
module docstrings): an API response is a deliberately curated,
presentation-shaped view - keeping it separate from the storage layer
means a storage-schema change doesn't silently change the API contract,
and vice versa.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

__all__ = [
    "AlertItem",
    "AlertsResponse",
    "AttackerDrilldown",
    "AttackerProfile",
    "AttackerProfilesResponse",
    "CopilotQueryRequest",
    "CopilotQueryResponse",
    "CredentialAttempt",
]


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------
class AlertItem(BaseModel):
    event_id: str
    scored_at: datetime
    alert_source: str | None
    anomaly_score: float | None
    is_anomaly: bool
    event_time: datetime | None
    src_ip: str | None
    dst_ip: str | None
    dst_port: int | None
    source_type: str | None
    severity: int | None
    attack_category: str | None


class AlertsResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AlertItem]


# ---------------------------------------------------------------------------
# Attacker profiles
# ---------------------------------------------------------------------------
class CredentialAttempt(BaseModel):
    username: str | None
    password: str | None
    count: int


class AttackerProfile(BaseModel):
    src_ip: str
    first_seen: datetime | None
    last_seen: datetime | None
    total_events: int
    distinct_ports_hit: int
    distinct_honeypots_hit: int
    top_credentials_tried: list[CredentialAttempt]
    attack_categories: list[str]
    # Flattened out of the gold table's richer `geo` struct (see
    # threatlake.transform.gold.attacker_profiles) - country/city/ASN
    # aren't surfaced here because nothing in this API currently uses
    # them; lat/lon are, for the dashboard's Map tab. Both null when the
    # IP didn't resolve (private/reserved range, or a MaxMind free-tier
    # coverage gap - both normal, not errors - see ARCHITECTURE.md).
    latitude: float | None
    longitude: float | None


class AttackerProfilesResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AttackerProfile]


class AttackerDrilldown(BaseModel):
    profile: AttackerProfile
    alerts: list[AlertItem]


# ---------------------------------------------------------------------------
# Copilot
# ---------------------------------------------------------------------------
class CopilotQueryRequest(BaseModel):
    question: str


class CopilotQueryResponse(BaseModel):
    """Exactly one of two shapes, discriminated by ``rejected``: success ->
    sql/rows/row_count populated, reason null; rejected/failed -> reason
    populated (always specific - see
    threatlake.copilot.guardrails.GuardrailRejectionError), rows/row_count
    null. ``sql`` may still be present on a guardrail rejection (the query
    that got rejected) but is null if generation itself never produced one.
    """

    rejected: bool
    reason: str | None
    sql: str | None
    rows: list[dict[str, object]] | None
    row_count: int | None
