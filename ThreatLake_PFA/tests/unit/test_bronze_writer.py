"""threatlake.ingestion.bronze_writer.write_bronze: end-to-end batch
ingestion against the checked-in NDJSON fixture
(tests/unit/fixtures/landing/cowrie/sample.ndjson - 8 valid lines, 1
deliberately malformed)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from threatlake.common.paths import bronze_path, landing_path, quarantine_path
from threatlake.ingestion.bronze_writer import write_bronze

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from threatlake.common.config import Settings


def test_write_bronze_writes_valid_rows_and_quarantines_malformed(
    spark: SparkSession, bronze_settings: Settings
) -> None:
    result = write_bronze("cowrie", spark, bronze_settings)
    assert result.written == 8  # noqa: PLR2004
    assert result.quarantined == 1

    bronze_df = spark.read.format("delta").load(bronze_path("cowrie", bronze_settings))
    assert bronze_df.count() == 8  # noqa: PLR2004

    quarantine_df = spark.read.format("delta").load(quarantine_path("cowrie", bronze_settings))
    assert quarantine_df.count() == 1


def test_write_bronze_moves_processed_files_out_of_landing(
    spark: SparkSession, bronze_settings: Settings
) -> None:
    from pathlib import Path

    landing_dir = Path(landing_path("cowrie", bronze_settings))
    assert list(landing_dir.glob("*.ndjson")), "fixture file should exist before the run"

    write_bronze("cowrie", spark, bronze_settings)

    assert not list(landing_dir.glob("*.ndjson")), "landing dir should be empty after ingestion"
    processed = list(landing_dir.glob("_processed/**/*.ndjson"))
    assert len(processed) == 1


def test_write_bronze_is_a_noop_on_an_empty_landing_directory(
    spark: SparkSession, bronze_settings: Settings
) -> None:
    write_bronze("cowrie", spark, bronze_settings)  # first run consumes the fixture file
    result = write_bronze("cowrie", spark, bronze_settings)  # second run: nothing left to read
    assert result.written == 0
    assert result.quarantined == 0
