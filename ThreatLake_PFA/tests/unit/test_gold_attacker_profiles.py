"""attacker_profiles: hand-computed aggregation math and top-5 credential ranking.

Fixture for ip 1.1.1.1, hand-tallied below:
  1. 08:00 connection,        port 22
  2. 08:05 credential_access, port 22, admin/admin123
  3. 08:10 credential_access, port 22, admin/admin123 (same pair again)
  4. 08:15 credential_access, port 80, root/root123
  -> total_events=4, distinct_ports_hit={22,80}=2
     attack_categories = [connection, credential_access] (sorted, deduped)
     top_credentials_tried = [{admin,admin123,count=2}, {root,root123,count=1}]
     first_seen=08:00, last_seen=08:15

ip 2.2.2.2: one connection event, no credentials -> top_credentials_tried = [] (empty, not null).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from threatlake.common.paths import gold_path
from threatlake.enrichment.geo import GeoEnricher
from threatlake.transform.gold.attacker_profiles import (
    build_attacker_profiles,
    write_attacker_profiles,
)
from tests.conftest import GEOIP_ASN_TEST_DB, GEOIP_CITY_TEST_DB
from tests.unit.silver_fixture_helpers import build_silver_df, silver_row, write_silver_fixture

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from threatlake.common.config import Settings


def _fixture_rows() -> list[dict[str, object]]:
    return [
        silver_row(
            event_id="1", src_ip="1.1.1.1", dst_port=22, attack_category="connection",
            event_time=datetime(2026, 8, 2, 8, 0, tzinfo=UTC),
        ),
        silver_row(
            event_id="2", src_ip="1.1.1.1", dst_port=22, attack_category="credential_access",
            attempted_username="admin", attempted_password="admin123", credentials_attempted=True,
            event_time=datetime(2026, 8, 2, 8, 5, tzinfo=UTC),
        ),
        silver_row(
            event_id="3", src_ip="1.1.1.1", dst_port=22, attack_category="credential_access",
            attempted_username="admin", attempted_password="admin123", credentials_attempted=True,
            event_time=datetime(2026, 8, 2, 8, 10, tzinfo=UTC),
        ),
        silver_row(
            event_id="4", src_ip="1.1.1.1", dst_port=80, attack_category="credential_access",
            attempted_username="root", attempted_password="root123", credentials_attempted=True,
            event_time=datetime(2026, 8, 2, 8, 15, tzinfo=UTC),
        ),
        silver_row(
            event_id="5", src_ip="2.2.2.2", dst_port=22, attack_category="connection",
            event_time=datetime(2026, 8, 2, 9, 0, tzinfo=UTC),
        ),
    ]


def test_build_attacker_profiles_aggregation(spark: SparkSession) -> None:
    df = build_silver_df(spark, *_fixture_rows())
    profiles = {r["src_ip"]: r for r in build_attacker_profiles(df, spark).collect()}

    ip1 = profiles["1.1.1.1"]
    assert ip1["total_events"] == 4  # noqa: PLR2004
    assert ip1["distinct_ports_hit"] == 2  # noqa: PLR2004
    assert ip1["attack_categories"] == ["connection", "credential_access"]
    assert ip1["first_seen"] == datetime(2026, 8, 2, 8, 0)  # noqa: DTZ001
    assert ip1["last_seen"] == datetime(2026, 8, 2, 8, 15)  # noqa: DTZ001

    top_creds = [(c["username"], c["password"], c["count"]) for c in ip1["top_credentials_tried"]]
    assert top_creds == [("admin", "admin123", 2), ("root", "root123", 1)]

    ip2 = profiles["2.2.2.2"]
    assert ip2["top_credentials_tried"] == []

    # No GeoEnricher supplied - geo must still be a PRESENT struct with
    # null fields (never a null struct), so `geo.latitude` is always safe
    # to select downstream regardless of whether enrichment ran.
    assert ip1["geo"]["country"] is None
    assert ip1["geo"]["latitude"] is None


def test_build_attacker_profiles_drops_rows_with_no_src_ip(spark: SparkSession) -> None:
    df = build_silver_df(spark, silver_row(src_ip=None), silver_row(src_ip="3.3.3.3"))
    profiles = build_attacker_profiles(df, spark)
    assert profiles.count() == 1
    assert profiles.first()["src_ip"] == "3.3.3.3"


def test_build_attacker_profiles_with_a_real_geo_enricher(spark: SparkSession) -> None:
    """81.2.69.142 is a known-good MaxMind TEST-database entry (GB/London -
    see tests/unit/test_enrichment_geo.py's own docstring for how that was
    verified) - a real lookup, not a mock.
    """
    df = build_silver_df(
        spark,
        silver_row(event_id="1", src_ip="81.2.69.142", event_time=datetime(2026, 8, 2, 8, 0, tzinfo=UTC)),
        silver_row(event_id="2", src_ip="203.0.113.99", event_time=datetime(2026, 8, 2, 8, 0, tzinfo=UTC)),
    )
    with GeoEnricher(GEOIP_CITY_TEST_DB, GEOIP_ASN_TEST_DB) as enricher:
        profiles = {r["src_ip"]: r for r in build_attacker_profiles(df, spark, enricher).collect()}

    resolved = profiles["81.2.69.142"]["geo"]
    assert resolved["country"] == "GB"
    assert resolved["latitude"] == pytest.approx(51.5142)

    unresolved = profiles["203.0.113.99"]["geo"]
    assert unresolved["country"] is None
    assert unresolved["latitude"] is None


def test_write_attacker_profiles_is_idempotent(spark: SparkSession, tmp_lakehouse: Settings) -> None:
    write_silver_fixture(spark, tmp_lakehouse, *_fixture_rows())

    write_attacker_profiles(spark, tmp_lakehouse)
    first_count = (
        spark.read.format("delta").load(gold_path("attacker_profiles", tmp_lakehouse)).count()
    )
    write_attacker_profiles(spark, tmp_lakehouse)  # re-run against unchanged silver data
    second_count = (
        spark.read.format("delta").load(gold_path("attacker_profiles", tmp_lakehouse)).count()
    )

    assert first_count == second_count == 2  # noqa: PLR2004
