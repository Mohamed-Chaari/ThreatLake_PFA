"""GET /attacker_profiles (paginated) and GET /attacker_profiles/{ip} (drill-down).

Both read threatlake.transform.gold.attacker_profiles - a batch table
built by run_pipeline.py, so results are as fresh as the last pipeline
run. The /{ip} drill-down additionally pulls this IP's alerts
(threatlake.api._tables.read_alerts) so an analyst gets the profile and
its flagged activity in one call instead of two.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pyspark.sql import functions as F

from threatlake.api._tables import ALERT_COLUMNS, read_alerts, read_gold_table
from threatlake.api.deps import get_settings_dep, get_spark_dep
from threatlake.api.schemas import (
    AlertItem,
    AttackerDrilldown,
    AttackerProfile,
    AttackerProfilesResponse,
)

if TYPE_CHECKING:
    from pyspark.sql import Row, SparkSession

    from threatlake.common.config import Settings

__all__ = ["router"]

router = APIRouter()

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500

_PROFILE_COLUMNS = (
    "src_ip",
    "first_seen",
    "last_seen",
    "total_events",
    "distinct_ports_hit",
    "distinct_honeypots_hit",
    "top_credentials_tried",
    "attack_categories",
)


def _profile_from_row(row: Row) -> AttackerProfile:
    data: dict[str, Any] = row.asDict(recursive=True)
    return AttackerProfile(**data)


@router.get("/attacker_profiles", response_model=AttackerProfilesResponse)
def list_attacker_profiles(
    limit: int = Query(default=_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    offset: int = Query(default=0, ge=0),
    spark: SparkSession = Depends(get_spark_dep),
    settings: Settings = Depends(get_settings_dep),
) -> AttackerProfilesResponse:
    profiles = read_gold_table(spark, "attacker_profiles", settings)
    if profiles is None:
        return AttackerProfilesResponse(total=0, limit=limit, offset=offset, items=[])

    profiles = profiles.select(*_PROFILE_COLUMNS).orderBy(F.col("total_events").desc())
    total = profiles.count()
    page = profiles.offset(offset).limit(limit).collect()
    return AttackerProfilesResponse(
        total=total, limit=limit, offset=offset, items=[_profile_from_row(r) for r in page]
    )


@router.get("/attacker_profiles/{ip}", response_model=AttackerDrilldown)
def get_attacker_profile(
    ip: str,
    spark: SparkSession = Depends(get_spark_dep),
    settings: Settings = Depends(get_settings_dep),
) -> AttackerDrilldown:
    profiles = read_gold_table(spark, "attacker_profiles", settings)
    if profiles is None:
        raise HTTPException(status_code=404, detail=f"No attacker profile data for {ip!r} yet.")

    match = profiles.select(*_PROFILE_COLUMNS).filter(F.col("src_ip") == ip).collect()
    if not match:
        raise HTTPException(status_code=404, detail=f"No attacker profile for {ip!r}.")
    profile = _profile_from_row(match[0])

    alerts_df = read_alerts(spark, settings)
    alerts: list[AlertItem] = []
    if alerts_df is not None:
        rows = (
            alerts_df.filter(F.col("src_ip") == ip)
            .select(*ALERT_COLUMNS)
            .orderBy(F.col("scored_at").desc())
            .collect()
        )
        alerts = [AlertItem(**{c: row[c] for c in ALERT_COLUMNS}) for row in rows]

    return AttackerDrilldown(profile=profile, alerts=alerts)
