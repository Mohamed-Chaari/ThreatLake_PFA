"""threatlake.transform.silver.schema: conform_to_silver_schema's cast +
NOT NULL enforcement."""

from __future__ import annotations

import pytest

from threatlake.transform.silver.schema import (
    SILVER_EVENT_SCHEMA,
    SilverSchemaError,
    conform_to_silver_schema,
)
from tests.unit.silver_fixture_helpers import build_silver_df, silver_row


def test_conform_selects_exact_schema_columns(spark: object) -> None:
    df = build_silver_df(spark, silver_row())
    conformed = conform_to_silver_schema(df)
    assert conformed.schema == SILVER_EVENT_SCHEMA


def test_conform_casts_types(spark: object) -> None:
    df = build_silver_df(spark, silver_row(severity=3))
    conformed = conform_to_silver_schema(df)
    assert conformed.collect()[0]["severity"] == 3


def test_conform_rejects_null_in_not_null_column(spark: object) -> None:
    """Built from an all-nullable input schema, not SILVER_EVENT_SCHEMA
    itself: createDataFrame(schema=SILVER_EVENT_SCHEMA) would reject a
    null event_id at construction time, before conform_to_silver_schema
    ever runs. A real mapper bug instead reaches conform_to_silver_schema
    via a .select() chain, where Spark infers nullability from the
    expression tree and does NOT check it against the target schema -
    which is exactly why conform_to_silver_schema must verify this
    itself.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructField, StructType

    loose_schema = StructType([StructField(f.name, f.dataType, True) for f in SILVER_EVENT_SCHEMA])
    row = silver_row(event_id=None)
    df = spark.createDataFrame([row], schema=loose_schema).select(
        *[F.col(f.name) for f in SILVER_EVENT_SCHEMA]
    )
    with pytest.raises(SilverSchemaError, match="event_id"):
        conform_to_silver_schema(df)


def test_conform_allows_null_in_nullable_column(spark: object) -> None:
    df = build_silver_df(spark, silver_row(src_ip=None, dst_port=None))
    conformed = conform_to_silver_schema(df)
    row = conformed.collect()[0]
    assert row["src_ip"] is None
    assert row["dst_port"] is None
