"""The single source of truth for every location ThreatLake PFA reads or writes.

No other module builds a path by string concatenation - everything goes
through these helpers. Trimmed from ThreatLake AI's version: no
``checkpoint_path`` (no streaming here) and no ``cache_path``/``table_fqn``
(no enrichment cache, no Unity Catalog metastore - PFA is local-path-only).
"""

from __future__ import annotations

from pathlib import Path

from threatlake.common.config import LAYERS, Layer, Settings, get_settings

__all__ = [
    "bronze_path",
    "gold_path",
    "landing_path",
    "layer_path",
    "quarantine_path",
    "silver_path",
    "storage_root",
]


class PathError(ValueError):
    """Raised for table or checkpoint names that cannot form a safe path."""


def _normalise_root(location: str) -> str:
    """Return an absolute, trailing-slash-free root."""
    return str(Path(location).expanduser().resolve())


def _validate_segment(segment: str, kind: str) -> str:
    """Reject empty names and traversal, which would escape the storage root."""
    cleaned = segment.strip()
    if not cleaned:
        raise PathError(f"{kind} name must not be empty")
    if cleaned != segment:
        raise PathError(f"{kind} name {segment!r} has leading or trailing whitespace")
    if "/" in cleaned or "\\" in cleaned or cleaned in {".", ".."}:
        raise PathError(f"{kind} name {segment!r} must be a single path segment")
    return cleaned


def _join(root: str, *segments: str) -> str:
    return "/".join([root.rstrip("/"), *segments])


def storage_root(settings: Settings | None = None) -> str:
    """Absolute root of the lakehouse."""
    settings = settings or get_settings()
    return _normalise_root(settings.storage.root)


def layer_path(layer: Layer, table: str, settings: Settings | None = None) -> str:
    """Location of a Delta table in ``layer``.

    ``table`` is a *logical* key; the physical name comes from
    ``tables.<layer>`` in ``config/local.yaml``, falling back to the key itself.
    """
    if layer not in LAYERS:
        raise PathError(f"Unknown layer {layer!r}. Expected one of: {', '.join(LAYERS)}")
    settings = settings or get_settings()
    name = _validate_segment(settings.tables.resolve(layer, table), "table")
    return _join(storage_root(settings), layer, name)


def bronze_path(source_type: str, settings: Settings | None = None) -> str:
    """Location of a source type's own bronze table (only ``honeydb`` here)."""
    return layer_path("bronze", source_type, settings)


def silver_path(table: str, settings: Settings | None = None) -> str:
    """Location of the cleaned, conformed silver events table."""
    return layer_path("silver", table, settings)


def gold_path(table: str, settings: Settings | None = None) -> str:
    """Location of an aggregated, serving-ready gold table."""
    return layer_path("gold", table, settings)


def landing_path(source: str, settings: Settings | None = None) -> str:
    """Directory the honeydb log source drops raw NDJSON files into, pre-ingest."""
    settings = settings or get_settings()
    name = _validate_segment(source, "source")
    return _join(_normalise_root(settings.storage.landing), name)


def quarantine_path(table: str, settings: Settings | None = None) -> str:
    """Where records rejected by the schema gate are parked."""
    settings = settings or get_settings()
    name = _validate_segment(table, "table")
    return _join(_normalise_root(settings.storage.quarantine), name)
