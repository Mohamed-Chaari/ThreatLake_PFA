"""Offline GeoIP enrichment via local MaxMind GeoLite2 ``.mmdb`` databases.

Fully offline by design: this module never makes a network call. It reads
locally downloaded ``.mmdb`` files (City and ASN) through the official
``geoip2`` client library. Obtaining those files - which requires a free
MaxMind account - is a one-time operational step outside this codebase; see
https://www.maxmind.com/en/accounts/current/geoip/downloads.

A missing or corrupt database file is treated as a *configuration* problem
and raises at :class:`GeoEnricher` construction time: unlike
``reputation.py``'s external API, there is no legitimate "temporarily
unavailable" state for a local file that is supposed to be there. An IP
simply absent from the database (private/reserved ranges, or one MaxMind
hasn't mapped) is normal and yields nulls, not an error.

Lookups happen driver-side (collect distinct IPs, look each up locally, join
back), the same pattern ``reputation.py`` uses for the external API. It isn't
required here - GeoIP lookups have no rate limit - but keeping both
enrichers' Spark integration uniform avoids a second, different story for
"how does a local .mmdb file get distributed to every executor" on a
multi-node cluster.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import geoip2.database
import geoip2.errors
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

__all__ = ["GeoEnricher", "GeoInfo", "enrich_geo"]

_RESULT_SCHEMA = StructType(
    [
        StructField("_geo_ip", StringType(), True),
        StructField("geo_country", StringType(), True),
        StructField("geo_city", StringType(), True),
        StructField("geo_latitude", DoubleType(), True),
        StructField("geo_longitude", DoubleType(), True),
        StructField("geo_asn", IntegerType(), True),
        StructField("geo_asn_org", StringType(), True),
    ]
)


@dataclass(frozen=True)
class GeoInfo:
    """Result of one IP lookup. All fields are null when the IP isn't found.

    ``latitude``/``longitude`` come from MaxMind's own City lookup (the same
    call that resolves ``country``/``city``) - not a second lookup, just a
    field on the result this module wasn't reading before.
    """

    country: str | None
    city: str | None
    latitude: float | None
    longitude: float | None
    asn: int | None
    asn_org: str | None


_NOT_FOUND = GeoInfo(country=None, city=None, latitude=None, longitude=None, asn=None, asn_org=None)


class GeoEnricher:
    """Wraps two local MaxMind readers: a City database and an ASN database."""

    def __init__(self, city_db_path: str | Path, asn_db_path: str | Path) -> None:
        for path, label in ((Path(city_db_path), "City"), (Path(asn_db_path), "ASN")):
            if not path.is_file():
                raise FileNotFoundError(
                    f"MaxMind GeoLite2-{label} database not found at {path}. Download it "
                    "from https://www.maxmind.com/en/accounts/current/geoip/downloads "
                    "(free account required)."
                )
        self._city_reader = geoip2.database.Reader(str(city_db_path))
        self._asn_reader = geoip2.database.Reader(str(asn_db_path))

    def lookup(self, ip: str) -> GeoInfo:
        """Look up one IP. Never raises for a not-found/private/reserved address."""
        country = city = None
        latitude = longitude = None
        asn = asn_org = None

        try:
            city_result = self._city_reader.city(ip)
            country = city_result.country.iso_code
            city = city_result.city.name
            latitude = city_result.location.latitude
            longitude = city_result.location.longitude
        except (geoip2.errors.AddressNotFoundError, ValueError):
            pass

        try:
            asn_result = self._asn_reader.asn(ip)
            asn = asn_result.autonomous_system_number
            asn_org = asn_result.autonomous_system_organization
        except (geoip2.errors.AddressNotFoundError, ValueError):
            pass

        return GeoInfo(
            country=country,
            city=city,
            latitude=latitude,
            longitude=longitude,
            asn=asn,
            asn_org=asn_org,
        )

    def close(self) -> None:
        self._city_reader.close()
        self._asn_reader.close()

    def __enter__(self) -> GeoEnricher:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def enrich_geo(
    df: DataFrame, spark: SparkSession, enricher: GeoEnricher, ip_column: str = "src_ip"
) -> DataFrame:
    """Left-join geo fields (country/city/lat/lon/ASN) onto ``df`` by
    ``ip_column``.

    Looks up each *distinct* IP once on the driver, not once per row, and
    never fails the batch: an IP that fails to parse or resolve just yields
    null geo fields for the rows that reference it.
    """
    distinct_ips = [
        row[ip_column] for row in df.select(ip_column).distinct().collect() if row[ip_column]
    ]

    def _row(ip: str) -> dict[str, object]:
        info = asdict(enricher.lookup(ip))
        return {
            "_geo_ip": ip,
            "geo_country": info["country"],
            "geo_city": info["city"],
            "geo_latitude": info["latitude"],
            "geo_longitude": info["longitude"],
            "geo_asn": info["asn"],
            "geo_asn_org": info["asn_org"],
        }

    geo_df = spark.createDataFrame([_row(ip) for ip in distinct_ips], schema=_RESULT_SCHEMA)

    return df.join(geo_df, on=F.col(ip_column) == F.col("_geo_ip"), how="left").drop("_geo_ip")
