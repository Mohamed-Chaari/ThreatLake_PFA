"""ThreatLake AI (study copy) - silver layer.

In the real project this package re-exports four mappers (cowrie, suricata,
dionaea, heralding) plus a combiner and a dedup step. This copy keeps only
the cowrie mapper and the shared schema it conforms to - see cowrie.py and
schema.py, and the top-level README.md for what was removed and why.
"""

from threatlake.transform.silver.cowrie import map_cowrie
from threatlake.transform.silver.schema import SILVER_EVENT_SCHEMA, conform_to_silver_schema

__all__ = [
    "SILVER_EVENT_SCHEMA",
    "conform_to_silver_schema",
    "map_cowrie",
]
