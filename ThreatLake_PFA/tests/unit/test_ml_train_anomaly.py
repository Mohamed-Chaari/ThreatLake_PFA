"""threatlake.ml.train_anomaly.train_anomaly: fits IsolationForest against a
real silver table and persists it to settings.ml.model_path."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import joblib
from sklearn.ensemble import IsolationForest

from threatlake.ml.train_anomaly import train_anomaly
from tests.unit.silver_fixture_helpers import silver_row, write_silver_fixture

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from threatlake.common.config import Settings


def _fixture_rows(n: int = 30) -> list[dict[str, object]]:
    from datetime import UTC, datetime, timedelta

    base = datetime(2026, 8, 2, 10, 0, 0, tzinfo=UTC)
    return [
        silver_row(event_id=f"e{i}", src_ip=f"10.0.0.{i % 5}", event_time=base + timedelta(seconds=i))
        for i in range(n)
    ]


def test_train_anomaly_saves_a_usable_model(spark: SparkSession, tmp_lakehouse: Settings) -> None:
    write_silver_fixture(spark, tmp_lakehouse, *_fixture_rows())

    result = train_anomaly(spark, tmp_lakehouse, contamination=0.1)

    assert result.n_events == 30  # noqa: PLR2004
    model_path = Path(result.model_path)
    assert model_path.is_file()

    loaded = joblib.load(model_path)
    assert isinstance(loaded, IsolationForest)


def test_train_anomaly_raises_on_empty_silver_table(
    spark: SparkSession, tmp_lakehouse: Settings
) -> None:
    import pytest

    # Rows with no src_ip/event_time never reach the feature builder, so an
    # all-null-attribution silver table produces zero feature rows.
    write_silver_fixture(spark, tmp_lakehouse, silver_row(src_ip=None, event_time=None))
    with pytest.raises(ValueError, match="zero feature rows"):
        train_anomaly(spark, tmp_lakehouse)
