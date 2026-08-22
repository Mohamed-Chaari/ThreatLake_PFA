"""Step 2: bronze -> silver.

This step does the least amount of NEW work in this whole study copy,
on purpose: it just calls map_cowrie, which is the real project's own
cowrie mapper, imported unmodified from
src/threatlake/transform/silver/cowrie.py. Everything you see happen
here - field renames, the attack_category/severity taxonomy, the
event_id content hash - is the actual code the real pipeline runs, not
a simplified stand-in.

What IS missing, compared to the real project:

  - No dedup. The real project runs a two-stage deduplication pass right
    after mapping (src/threatlake/transform/silver/dedup.py in the real
    repo) - an exact-match pass (catches the same bronze line ingested
    twice) and a fuzzy, windowed pass (catches the same real connection
    seen by two different honeypot daemons). With one source and a
    handful of sample rows, there is nothing to deduplicate here, so
    that whole step is skipped rather than reproduced.
  - No union of multiple sources. The real project's combiner.py maps
    all four honeypot types and unions the results into one stream
    before deduplicating. With cowrie only, there is only one mapper to
    call - no union needed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from threatlake.transform.silver.cowrie import map_cowrie

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def build_silver(bronze_df: DataFrame) -> DataFrame:
    """Map bronze cowrie rows into the unified silver event shape.

    map_cowrie already does two things internally that are worth naming:
      1. It filters bronze_df down to rows where source_type == "cowrie"
         AND the "cowrie" struct column is non-null. In practice that
         second check rarely does anything by itself - from_json almost
         always returns a real (non-null) struct, just one with null
         fields inside it on bad input. The row that actually protects
         this pipeline from garbage input is bronze.py's own
         `.filter(F.col("cowrie.eventid").isNotNull())` - see the long
         comment there for the real bug that line exists to prevent.
      2. It calls conform_to_silver_schema at the end, which selects and
         casts every column to the shared 19-column silver schema, then
         verifies the columns that must never be null (event_id,
         ingest_date, source_type, credentials_attempted, raw_ref)
         actually aren't. If a mapper bug ever left one of those null,
         this would raise loudly right here - not silently later.
    """
    return map_cowrie(bronze_df)
