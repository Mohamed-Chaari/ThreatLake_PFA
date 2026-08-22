"""Idempotent Delta writer shared by every gold table.

Every gold table in this package is a full recompute from silver on each
run - not an incremental/merge update. ``mode("overwrite")`` makes reruns
idempotent by construction: the previous version of the table is entirely
replaced by the newly computed one, so re-running against unchanged silver
data produces a byte-identical table, and re-running after new silver data
arrives produces the correct up-to-date aggregate with no leftover or
duplicate rows from a prior run.

This trades incremental-update efficiency for simplicity and unconditional
correctness. Reasonable at ThreatLake's current data volumes; a production
system at much larger scale would likely partition-scope these overwrites
(``replaceWhere``) or move to MERGE-based incremental updates instead of
recomputing full history on every run.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from threatlake.common.config import Settings, get_settings
from threatlake.common.paths import gold_path

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = ["write_gold_table"]


def write_gold_table(
    df: DataFrame,
    table: str,
    settings: Settings | None = None,
    partition_by: list[str] | None = None,
) -> None:
    """Fully overwrite the gold Delta table ``table`` with ``df``."""
    settings = settings or get_settings()
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(gold_path(table, settings))
