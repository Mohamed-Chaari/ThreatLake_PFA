"""Bronze rows -> the unified silver event model.

ThreatLake AI maps four honeypot sources (cowrie, suricata, dionaea,
heralding) into this same schema through four mappers plus a combiner and
a dedup step. PFA is single-source (see ARCHITECTURE.md): ``map_honeydb``
is the pipeline's active mapper, reading real HoneyDB data. ``map_cowrie``
(synthetic-data era) is kept fully working and exported here too - not
deleted, just not wired into ``scripts/run_pipeline.py`` any more.
"""

from threatlake.transform.silver.cowrie import map_cowrie
from threatlake.transform.silver.honeydb import map_honeydb
from threatlake.transform.silver.schema import SILVER_EVENT_SCHEMA, conform_to_silver_schema

__all__ = [
    "SILVER_EVENT_SCHEMA",
    "conform_to_silver_schema",
    "map_cowrie",
    "map_honeydb",
]
