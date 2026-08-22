"""System prompt for the NL-to-SQL model, built from the REAL schema of the
5 gold tables at request time - not hand-typed, so it can never drift out
of sync with what threatlake.transform.gold actually produces. If a
column gets renamed or added there, the copilot's prompt reflects it on
the very next request, with no second place to remember to update.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from threatlake.common.paths import gold_path
from threatlake.copilot.guardrails import ALLOWED_TABLES, MAX_ROWS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from threatlake.common.config import Settings

__all__ = ["SchemaUnavailableError", "build_system_prompt"]

#: Short, hand-written grain hints - these are natural-language framing,
#: not schema facts, so unlike column lists they don't drift when a
#: pipeline module changes; each one is a paraphrase of that table's own
#: module docstring's GRAIN line.
_GRAIN_HINTS: dict[str, str] = {
    "attacker_profiles": "one row per attacker src_ip, aggregated across all history",
    "attack_timeline": "one row per (hour, attack_category, dst_port) bucket",
    "service_targeting": "one row per (day, dst_port) bucket",
    "credential_intelligence": (
        "one row per (username, password) pair tried, with a day-over-day trend"
    ),
    "campaign_candidates": (
        "one row per detected coordinated-campaign window; src_ips is an array column"
    ),
}


class SchemaUnavailableError(RuntimeError):
    """Raised when a gold table can't be read - most likely the gold
    scheduler (see docs/adr/0002-gold-batch-decoupled-from-streaming.md)
    hasn't completed its first run yet. Not a copilot bug: there is
    genuinely nothing to query yet.
    """


def _describe_table(spark: SparkSession, settings: Settings, table: str) -> str:
    try:
        df = spark.read.format("delta").load(gold_path(table, settings))
    except Exception as exc:
        raise SchemaUnavailableError(
            f"gold table {table!r} isn't available yet - the gold scheduler needs to "
            "complete at least one run before the copilot can answer questions"
        ) from exc

    columns = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields)
    grain = _GRAIN_HINTS.get(table, "")
    return f"- {table}({columns})\n  grain: {grain}"


def build_system_prompt(spark: SparkSession, settings: Settings) -> str:
    """Build the system prompt, with a live-read schema section for every
    table in :data:`threatlake.copilot.guardrails.ALLOWED_TABLES`.
    """
    schema_section = "\n".join(
        _describe_table(spark, settings, table) for table in sorted(ALLOWED_TABLES)
    )

    # Adjacent string literals below build an LLM PROMPT describing the
    # schema, not a SQL string that gets executed - nothing here ever
    # reaches spark.sql(). ruff's S608 can't tell the difference from an
    # f-string alone, hence the suppression on the closing line.
    return (
        "You are a SQL generator for a security operations dashboard called "  # noqa: S608
        "ThreatLake AI. Given a natural-language question from a security "
        "analyst, output EXACTLY ONE Spark SQL SELECT statement that answers "
        "it - nothing else.\n"
        "\n"
        "STRICT RULES, no exceptions:\n"
        f"1. You may reference ONLY these {len(ALLOWED_TABLES)} tables, using "
        "exactly these bare (unqualified) names. Never invent a table, never "
        "reference any other table, schema, catalog, or system view "
        "(including information_schema).\n"
        "2. Output ONLY a single SELECT statement (a UNION/INTERSECT/EXCEPT "
        "of SELECTs is fine). Never output INSERT, UPDATE, DELETE, DROP, "
        "ALTER, CREATE, TRUNCATE, GRANT, or any statement that is not a pure "
        "read.\n"
        "3. Never output more than one statement. Never use a semicolon.\n"
        "4. If the question cannot be answered from these tables, output "
        "exactly:\n"
        "   SELECT 'question cannot be answered from the available gold "
        "tables' AS error\n"
        f"5. Results are capped at {MAX_ROWS} rows server-side regardless of "
        "what you request - you do not need to add your own LIMIT, but you "
        'may add one if it makes the answer clearer (e.g. "top 5").\n'
        "6. Output ONLY the SQL, optionally inside a ```sql code fence. No "
        "prose, no explanation, no markdown other than the optional fence.\n"
        "\n"
        "AVAILABLE TABLES AND REAL COLUMNS (read directly from the live gold "
        "tables - these are exactly the columns that exist right now):\n"
        f"{schema_section}\n"
    )
