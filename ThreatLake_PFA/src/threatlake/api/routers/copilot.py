"""POST /copilot/query - natural language -> SQL -> real gold-table rows.

Three strictly separate steps, each of which can independently fail
without touching the next:
  1. threatlake.copilot.text_to_sql.generate_sql - ask the LLM for SQL.
     Never trusted.
  2. threatlake.copilot.guardrails.validate_and_bound - the ONLY thing
     that decides whether that SQL is allowed to run. Runs BEFORE any
     Spark call, on every request, no exceptions.
  3. Execute the validated SQL against temp views over the 5 gold tables
     only (see threatlake.copilot.guardrails.ALLOWED_TABLES) - not the
     live gold Delta tables directly, so nothing this endpoint does can
     ever touch bronze, silver, or ml_scores even if steps 1-2 had a bug.

Every rejection - config, provider, guardrail, or execution failure - is
logged with its specific reason and returned to the caller the same way,
never a bare 500: this is a read-only analyst tool, a failure here should
never look like a crash.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends

from threatlake.api.deps import get_settings_dep, get_spark_dep
from threatlake.api.schemas import CopilotQueryRequest, CopilotQueryResponse
from threatlake.common.paths import gold_path
from threatlake.copilot.guardrails import (
    ALLOWED_TABLES,
    GuardrailRejectionError,
    validate_and_bound,
)
from threatlake.copilot.prompts import SchemaUnavailableError
from threatlake.copilot.text_to_sql import CopilotConfigError, CopilotProviderError, generate_sql

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from threatlake.common.config import Settings

__all__ = ["router"]

_LOG = logging.getLogger(__name__)

router = APIRouter()


def _rejected(reason: str, sql: str | None = None) -> CopilotQueryResponse:
    return CopilotQueryResponse(rejected=True, reason=reason, sql=sql, rows=None, row_count=None)


@router.post("/copilot/query", response_model=CopilotQueryResponse)
def copilot_query(
    request: CopilotQueryRequest,
    spark: SparkSession = Depends(get_spark_dep),
    settings: Settings = Depends(get_settings_dep),
) -> CopilotQueryResponse:
    try:
        raw_sql = generate_sql(request.question, spark, settings)
    except SchemaUnavailableError as exc:
        _LOG.warning("copilot: gold schema unavailable for question=%r: %s", request.question, exc)
        return _rejected(str(exc))
    except CopilotConfigError as exc:
        _LOG.warning("copilot: not configured: %s", exc)
        return _rejected(str(exc))
    except CopilotProviderError as exc:
        _LOG.warning("copilot: provider error for question=%r: %s", request.question, exc)
        return _rejected(f"the language model call failed: {exc}")

    try:
        safe_sql = validate_and_bound(raw_sql)
    except GuardrailRejectionError as exc:
        _LOG.warning(
            "copilot: guardrail REJECTED question=%r generated_sql=%r reason=%s",
            request.question,
            raw_sql,
            exc.reason,
        )
        return _rejected(exc.reason, sql=raw_sql)

    # Temp views over exactly the 5 allowed gold tables - the guardrail
    # already confirmed safe_sql references nothing else, this is the
    # second, structural half of that guarantee: even a guardrail bug
    # couldn't make this endpoint resolve a name it hasn't registered.
    for table in ALLOWED_TABLES:
        try:
            spark.read.format("delta").load(gold_path(table, settings)).createOrReplaceTempView(
                table
            )
        except Exception as exc:
            _LOG.warning("copilot: gold table %r unavailable: %s", table, exc)
            return _rejected(
                f"gold table {table!r} isn't available yet - the gold scheduler needs to "
                "complete at least one run before the copilot can answer questions"
            )

    try:
        result_df = spark.sql(safe_sql)
        rows = [row.asDict(recursive=True) for row in result_df.collect()]
    except Exception as exc:
        _LOG.warning("copilot: execution failed sql=%r error=%s", safe_sql, exc)
        return _rejected(f"the query failed to execute: {exc}", sql=safe_sql)

    _LOG.info("copilot: question=%r sql=%r rows=%d", request.question, safe_sql, len(rows))
    return CopilotQueryResponse(
        rejected=False, reason=None, sql=safe_sql, rows=rows, row_count=len(rows)
    )
