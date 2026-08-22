"""Shared pytest fixtures.

Tests never write into the developer's real lakehouse: every fixture that
needs storage redirects the roots through ``THREATLAKE_STORAGE__*``
environment overrides - the same mechanism a real deployment would use to
point at a different disk.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from threatlake.common.config import Settings, get_settings, reset_settings

# Repo root, so config/ is discoverable no matter where pytest is invoked from.
REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _clean_settings_cache() -> Iterator[None]:
    """Guarantee each test resolves settings from scratch."""
    reset_settings()
    yield
    reset_settings()


@pytest.fixture
def local_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force the local config directory regardless of the caller's shell."""
    monkeypatch.setenv("THREATLAKE_CONFIG_DIR", str(REPO_ROOT / "config"))


@pytest.fixture
def tmp_lakehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, local_env: None) -> Settings:
    """Point every storage root, and the ML model path, at an isolated temp directory."""
    monkeypatch.setenv("THREATLAKE_STORAGE__ROOT", str(tmp_path / "lakehouse"))
    monkeypatch.setenv("THREATLAKE_STORAGE__QUARANTINE", str(tmp_path / "quarantine"))
    monkeypatch.setenv("THREATLAKE_STORAGE__LANDING", str(tmp_path / "landing"))
    monkeypatch.setenv("THREATLAKE_ML__MODEL_PATH", str(tmp_path / "models" / "isolation_forest.joblib"))
    reset_settings()
    return get_settings()


#: Checked-in NDJSON fixtures for bronze ingestion tests.
FIXTURES_LANDING = REPO_ROOT / "tests" / "unit" / "fixtures" / "landing"

#: Official MaxMind test-data .mmdb files (small, public, made for exactly
#: this purpose) - see https://github.com/maxmind/MaxMind-DB/tree/main/test-data.
#: Deterministic and offline, unlike the real GeoLite2 databases (65MB/12MB,
#: gitignored under data/geoip/ - see ARCHITECTURE.md), so tests never
#: depend on those being present on disk.
GEOIP_CITY_TEST_DB = REPO_ROOT / "tests" / "unit" / "fixtures" / "geoip" / "GeoIP2-City-Test.mmdb"
GEOIP_ASN_TEST_DB = REPO_ROOT / "tests" / "unit" / "fixtures" / "geoip" / "GeoLite2-ASN-Test.mmdb"


@pytest.fixture
def bronze_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, local_env: None) -> Settings:
    """Bronze/quarantine/landing roots are all throwaway copies.

    write_bronze moves files out of the landing directory as it ingests
    them, so tests must never point THREATLAKE_STORAGE__LANDING at the
    checked-in fixtures directory directly - that would mutate (and
    eventually empty) tracked files in the repo. Each test gets its own
    fresh copy instead.
    """
    landing = tmp_path / "landing"
    shutil.copytree(FIXTURES_LANDING, landing)
    monkeypatch.setenv("THREATLAKE_STORAGE__ROOT", str(tmp_path / "lakehouse"))
    monkeypatch.setenv("THREATLAKE_STORAGE__QUARANTINE", str(tmp_path / "quarantine"))
    monkeypatch.setenv("THREATLAKE_STORAGE__LANDING", str(landing))
    reset_settings()
    return get_settings()


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """A session-scoped local SparkSession with Delta enabled.

    Session-scoped because JVM startup dominates the runtime of these tests.
    """
    from threatlake.common.spark import get_spark, stop_spark

    os.environ.setdefault("THREATLAKE_CONFIG_DIR", str(REPO_ROOT / "config"))
    reset_settings()
    session = get_spark()
    yield session
    stop_spark()
