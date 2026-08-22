"""GeoEnricher against the real, official MaxMind test-data .mmdb fixtures
(not synthetic/mocked - see tests/conftest.py:GEOIP_CITY_TEST_DB/GEOIP_ASN_TEST_DB).

Reused byte-for-byte unmodified from ThreatLake_AI's own
threatlake.enrichment.geo - this is that module's own test, adapted only
to import GEOIP_CITY_TEST_DB/GEOIP_ASN_TEST_DB from PFA's conftest.

Known-good test entries used below (verified by directly enumerating the
fixture files - not guessed): 81.2.69.142 -> GB/London (City),
1.0.0.0/24 -> AS15169 Google Inc. (ASN).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

from threatlake.enrichment.geo import GeoEnricher, enrich_geo
from tests.conftest import GEOIP_ASN_TEST_DB, GEOIP_CITY_TEST_DB

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession


@pytest.fixture
def enricher() -> Iterator[GeoEnricher]:
    with GeoEnricher(GEOIP_CITY_TEST_DB, GEOIP_ASN_TEST_DB) as e:
        yield e


def test_missing_database_raises_at_construction(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="GeoLite2-City"):
        GeoEnricher(tmp_path / "nope.mmdb", GEOIP_ASN_TEST_DB)


def test_known_city_ip_resolves(enricher: GeoEnricher) -> None:
    info = enricher.lookup("81.2.69.142")
    assert info.country == "GB"
    assert info.city == "London"
    assert info.latitude == pytest.approx(51.5142)
    assert info.longitude == pytest.approx(-0.0931)


def test_known_asn_ip_resolves(enricher: GeoEnricher) -> None:
    info = enricher.lookup("1.0.0.1")
    assert info.asn == 15169  # noqa: PLR2004
    assert info.asn_org == "Google Inc."


def test_ip_not_in_either_database_yields_all_none(enricher: GeoEnricher) -> None:
    """A private/reserved-range IP - normal in a honeypot's own dst_ip - must
    never raise; it just isn't information the database has.
    """
    info = enricher.lookup("203.0.113.99")
    assert info.country is None
    assert info.latitude is None
    assert info.asn is None


def test_enrich_geo_joins_onto_a_dataframe(spark: SparkSession, enricher: GeoEnricher) -> None:
    df = spark.createDataFrame([("81.2.69.142",), ("203.0.113.99",)], schema=["src_ip"])
    enriched = enrich_geo(df, spark, enricher)
    rows = {r["src_ip"]: r for r in enriched.collect()}

    assert rows["81.2.69.142"]["geo_country"] == "GB"
    assert rows["81.2.69.142"]["geo_latitude"] == pytest.approx(51.5142)
    assert rows["203.0.113.99"]["geo_country"] is None
    # The join must not change row count or drop the unmatched IP.
    assert enriched.count() == 2  # noqa: PLR2004
