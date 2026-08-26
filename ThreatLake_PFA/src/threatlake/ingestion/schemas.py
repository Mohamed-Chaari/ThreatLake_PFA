"""Registry of explicit source schemas for bronze ingestion.

The schema lives in ``config/schema/<source>.py`` rather than ``src/``
because, like ``config/local.yaml``, it describes *this deployment's*
known data shape and should be editable without a code change. It is
discovered through the same config-directory mechanism as
``threatlake.common.config``, then loaded as a plain Python module by
file path.

ThreatLake AI registers four source types (suricata, cowrie, dionaea,
heralding) here. PFA is single-source (see ARCHITECTURE.md), so
``SOURCE_TYPES`` has exactly one entry - but the loader mechanism itself
is unchanged, so adding a second source later is a one-line addition to
``SOURCE_TYPES``/``_MODULE_BY_SOURCE`` plus a new schema file, not a
redesign.

ACTIVE SOURCE: honeydb (real HoneyDB community sensor-data - see
ARCHITECTURE.md). cowrie was the original active source, backed by a
synthetic generator; it is kept fully working (schema, mapper, generator
all still here, still tested) and registered below so ``source_schema
("cowrie")`` still resolves, but it is deliberately absent from
``SOURCE_TYPES`` - not the pipeline's active path.
"""

from __future__ import annotations

import importlib.util
from functools import cache
from typing import TYPE_CHECKING

from threatlake.common.config import config_dir

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

__all__ = ["SOURCE_TYPES", "SchemaError", "source_schema"]

#: Honeypot source types the pipeline actively ingests.
SOURCE_TYPES: tuple[str, ...] = ("honeydb",)

_MODULE_BY_SOURCE = {
    "cowrie": "cowrie.py",
    "honeydb": "honeydb.py",
}


class SchemaError(RuntimeError):
    """Raised when a source schema cannot be located or loaded."""


def _load_schema(source_type: str) -> StructType:
    try:
        filename = _MODULE_BY_SOURCE[source_type]
    except KeyError as exc:
        raise SchemaError(
            f"Unknown source type {source_type!r}. Expected one of: {', '.join(SOURCE_TYPES)}"
        ) from exc

    path = config_dir() / "schema" / filename
    if not path.is_file():
        raise SchemaError(f"Schema file not found for {source_type!r}: {path}")

    spec = importlib.util.spec_from_file_location(f"threatlake_schema_{source_type}", path)
    if spec is None or spec.loader is None:
        raise SchemaError(f"Could not load schema module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    try:
        schema: StructType = module.SCHEMA
    except AttributeError as exc:
        raise SchemaError(f"{path} does not define SCHEMA") from exc
    return schema


@cache
def source_schema(source_type: str) -> StructType:
    """Return the explicit StructType for a source type.

    Cached: schema modules are loaded from disk once per process.
    """
    return _load_schema(source_type)
