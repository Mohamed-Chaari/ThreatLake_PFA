"""Bronze rows -> the unified silver event model.

ThreatLake AI maps four honeypot sources (cowrie, suricata, dionaea,
heralding) into this same schema through four mappers plus a combiner and
a dedup step. PFA is cowrie-only (see ARCHITECTURE.md), so only the
cowrie mapper and the shared schema it conforms to are here - both copied
byte-for-byte unmodified from ThreatLake AI.
"""

from threatlake.transform.silver.cowrie import map_cowrie
from threatlake.transform.silver.schema import SILVER_EVENT_SCHEMA, conform_to_silver_schema

__all__ = [
    "SILVER_EVENT_SCHEMA",
    "conform_to_silver_schema",
    "map_cowrie",
]
