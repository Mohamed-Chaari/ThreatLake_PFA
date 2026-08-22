"""threatlake.common.paths: every path helper resolves under storage.root,
and unsafe segments (empty, traversal) are rejected."""

from __future__ import annotations

import pytest

from threatlake.common.paths import (
    PathError,
    bronze_path,
    gold_path,
    landing_path,
    quarantine_path,
    silver_path,
    storage_root,
)


def test_bronze_path_resolves_under_storage_root(tmp_lakehouse: object) -> None:
    root = storage_root(tmp_lakehouse)
    assert bronze_path("cowrie", tmp_lakehouse) == f"{root}/bronze/bronze_cowrie"


def test_silver_path_resolves_logical_key(tmp_lakehouse: object) -> None:
    root = storage_root(tmp_lakehouse)
    assert silver_path("events", tmp_lakehouse) == f"{root}/silver/events"


def test_gold_path_resolves_logical_key(tmp_lakehouse: object) -> None:
    root = storage_root(tmp_lakehouse)
    assert gold_path("attacker_profiles", tmp_lakehouse) == f"{root}/gold/attacker_profiles"


def test_gold_path_unregistered_key_passes_through(tmp_lakehouse: object) -> None:
    """An ad-hoc/test table name not in config/local.yaml still resolves - by
    passing through unchanged, not by erroring.
    """
    root = storage_root(tmp_lakehouse)
    assert gold_path("some_new_table", tmp_lakehouse) == f"{root}/gold/some_new_table"


def test_landing_path_is_source_scoped(tmp_lakehouse: object) -> None:
    path = landing_path("cowrie", tmp_lakehouse)
    assert path.endswith("/landing/cowrie") or path.endswith("landing/cowrie")


def test_quarantine_path_is_table_scoped(tmp_lakehouse: object) -> None:
    path = quarantine_path("cowrie", tmp_lakehouse)
    assert path.endswith("/quarantine/cowrie")


@pytest.mark.parametrize("bad_segment", ["", "a/b", ".."])
def test_layer_path_rejects_unsafe_segments(tmp_lakehouse: object, bad_segment: str) -> None:
    with pytest.raises(PathError):
        gold_path(bad_segment, tmp_lakehouse)
