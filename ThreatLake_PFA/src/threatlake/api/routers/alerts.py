"""GET /alerts (paginated, filterable).

See threatlake.api._tables.read_alerts for what counts as an "alert"
here: ml_scores joined with silver, filtered to rows either detector
flagged (is_anomaly=True). Reads directly from the ml_scores/silver
Delta tables built by run_pipeline.py - PFA is batch-only (see
ARCHITECTURE.md), so there is no live push endpoint here: results are as
fresh as the last pipeline run, not real-time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, Query
from pyspark.sql import functions as F

from threatlake.api._tables import ALERT_COLUMNS, read_alerts
from threatlake.api.deps import get_settings_dep, get_spark_dep
from threatlake.api.schemas import AlertItem, AlertsResponse

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from threatlake.common.config import Settings

__all__ = ["router"]

router = APIRouter()

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500


@router.get("/alerts", response_model=AlertsResponse)
def list_alerts(
    alert_source: str | None = Query(
        default=None, description="Filter to 'rule', 'ml', or 'both'."
    ),
    severity: int | None = Query(default=None, ge=1, le=5),
    limit: int = Query(default=_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    offset: int = Query(default=0, ge=0),
    spark: SparkSession = Depends(get_spark_dep),
    settings: Settings = Depends(get_settings_dep),
) -> AlertsResponse:
    alerts = read_alerts(spark, settings)
    if alerts is None:
        return AlertsResponse(total=0, limit=limit, offset=offset, items=[])

    if alert_source is not None:
        alerts = alerts.filter(F.col("alert_source") == alert_source)
    if severity is not None:
        alerts = alerts.filter(F.col("severity") == severity)

    alerts = alerts.orderBy(F.col("scored_at").desc())
    total = alerts.count()
    page = alerts.offset(offset).limit(limit).collect()
    items = [AlertItem(**{c: row[c] for c in ALERT_COLUMNS}) for row in page]
    return AlertsResponse(total=total, limit=limit, offset=offset, items=items)
