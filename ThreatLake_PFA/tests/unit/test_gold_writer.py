"""threatlake.transform.gold.writer.write_gold_table: full-overwrite,
idempotent-by-construction writes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from threatlake.common.paths import gold_path
from threatlake.transform.gold.writer import write_gold_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from threatlake.common.config import Settings


def test_write_gold_table_overwrite_replaces_previous_contents(
    spark: SparkSession, tmp_lakehouse: Settings
) -> None:
    first = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    write_gold_table(first, "demo_table", tmp_lakehouse)
    assert spark.read.format("delta").load(gold_path("demo_table", tmp_lakehouse)).count() == 2  # noqa: PLR2004

    second = spark.createDataFrame([(3, "c")], ["id", "value"])
    write_gold_table(second, "demo_table", tmp_lakehouse)
    result = spark.read.format("delta").load(gold_path("demo_table", tmp_lakehouse)).collect()

    assert len(result) == 1
    assert result[0]["id"] == 3  # noqa: PLR2004


def test_write_gold_table_partition_by(spark: SparkSession, tmp_lakehouse: Settings) -> None:
    from datetime import date

    df = spark.createDataFrame([(date(2026, 8, 1), 1), (date(2026, 8, 2), 2)], ["date", "value"])
    write_gold_table(df, "partitioned_table", tmp_lakehouse, partition_by=["date"])

    from pathlib import Path

    table_dir = Path(gold_path("partitioned_table", tmp_lakehouse))
    partition_dirs = [p.name for p in table_dir.iterdir() if p.name.startswith("date=")]
    assert len(partition_dirs) == 2  # noqa: PLR2004
