"""ThreatLake PFA - enrichment layer.

ThreatLake AI also has an IP-reputation enricher (AbuseIPDB, needs a paid/
rate-limited API key) alongside this one. PFA keeps only GeoIP - offline,
free, no key required - see ARCHITECTURE.md's "Future extensions".
"""

from threatlake.enrichment.geo import GeoEnricher, GeoInfo, enrich_geo

__all__ = [
    "GeoEnricher",
    "GeoInfo",
    "enrich_geo",
]
