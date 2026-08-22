"""threatlake.common.spark's UTC-verification safeguard, tested against
hand-picked disagreeing values - see that module's own docstring for why
a genuinely divergent JVM timezone can't be reproduced on an
already-started test JVM, so the pure comparison logic is tested directly
instead of through a real broken SparkSession.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from threatlake.common.spark import (
    _PROBE_EXPECTED_DATETIME,
    _PROBE_WALL_CLOCK,
    SparkTimezoneError,
    _check_utc_timezone,
)


def test_utc_session_passes() -> None:
    _check_utc_timezone("UTC", _PROBE_WALL_CLOCK, _PROBE_EXPECTED_DATETIME)


def test_non_utc_session_timezone_raises() -> None:
    with pytest.raises(SparkTimezoneError, match="expected 'UTC'"):
        _check_utc_timezone("America/New_York", _PROBE_WALL_CLOCK, _PROBE_EXPECTED_DATETIME)


def test_sql_side_mismatch_raises() -> None:
    with pytest.raises(SparkTimezoneError, match="Spark SQL formatted"):
        _check_utc_timezone("UTC", "1970-01-01 05:00:01", _PROBE_EXPECTED_DATETIME)


def test_jvm_default_timezone_drift_raises() -> None:
    """session.timeZone=UTC and SQL formatting both agree, but the
    collected Python datetime is shifted - the exact failure mode this
    check exists to catch.
    """
    shifted = datetime(1970, 1, 1, 5, 0, 1)  # noqa: DTZ001 - deliberately naive, see module docstring
    with pytest.raises(SparkTimezoneError, match="JVM's OS-level default timezone"):
        _check_utc_timezone("UTC", _PROBE_WALL_CLOCK, shifted)
