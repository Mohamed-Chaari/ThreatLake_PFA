"""SparkSession factory for local Delta Lake batch jobs.

Trimmed from ThreatLake AI's version: that one also attaches to an
already-running Databricks-managed session when the code runs on a
cluster. PFA is local-only (see ARCHITECTURE.md), so only the local build
path is kept - but the UTC-verification safeguard below is real,
load-bearing logic, not boilerplate, and is kept intact unmodified.

Every session is verified to actually run in UTC before ``get_spark()``
returns it. This exists because ``spark.sql.session.timeZone=UTC`` alone
does not guarantee it: that setting governs Spark SQL's own computation,
but PySpark materializes a collected TimestampType into a Python
``datetime`` via the JVM's OS-level default timezone instead, which can
silently disagree. A session where that gap holds would shift every
collected timestamp by the host's UTC offset without raising anything -
this check exists so that happens loudly, at session start, instead of
silently corrupting every ``event_time`` this pipeline ever reads back.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import TYPE_CHECKING

from threatlake.common.config import Settings, get_settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

__all__ = ["SparkTimezoneError", "get_spark", "stop_spark"]

_LOG = logging.getLogger(__name__)


class SparkTimezoneError(RuntimeError):
    """Raised when a SparkSession cannot be confirmed to run in UTC.

    ThreatLake assumes every timestamp collected to Python is UTC
    wall-clock time - silver mappers, feature windows, and every test rely
    on it.
    """


_DELTA_CONF: dict[str, str] = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def _pin_python_interpreter() -> None:
    """Make Spark's Python workers use the same interpreter as the driver.

    Without this the JVM spawns workers with whatever ``python3`` is first
    on PATH, which fails with PYTHON_VERSION_MISMATCH whenever the active
    venv is not the system Python.
    """
    for var in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
        os.environ.setdefault(var, sys.executable)


def _pin_jvm_timezone() -> None:
    """Force the JVM's default timezone to UTC before it starts.

    ``spark.sql.session.timeZone`` (set below) only governs Spark SQL's own
    timestamp computation. When PySpark materializes a TimestampType into a
    Python ``datetime`` (e.g. ``.collect()``), that conversion goes through
    the JVM's OS-level default timezone instead - confirmed empirically:
    with session.timeZone=UTC but a non-UTC host, ``date_format()`` (pure
    SQL-side) shows the correct UTC instant while ``.collect()`` shows it
    shifted by the host's offset. Only the ``TZ`` process environment
    variable, set before the JVM is spawned, fixes this in local mode.
    """
    os.environ.setdefault("TZ", "UTC")


def _local_session(settings: Settings) -> SparkSession:
    """Build a Delta-enabled local session."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    _pin_python_interpreter()
    _pin_jvm_timezone()
    builder = SparkSession.builder.appName(settings.spark.app_name)
    if settings.spark.master:
        builder = builder.master(settings.spark.master)

    conf: dict[str, str] = {
        **_DELTA_CONF,
        "spark.sql.shuffle.partitions": str(settings.spark.shuffle_partitions),
        **settings.spark.conf,
    }
    for key, value in conf.items():
        builder = builder.config(key, value)

    return configure_spark_with_delta_pip(builder).getOrCreate()


#: The instant the live probe below asks Spark to parse and hand back.
#: Chosen arbitrarily; only its exact round-trip matters.
_PROBE_WALL_CLOCK = "1970-01-01 00:00:01"
_PROBE_EXPECTED_DATETIME = datetime(1970, 1, 1, 0, 0, 1)  # noqa: DTZ001 - deliberately naive, see below


def _check_utc_timezone(session_timezone: str | None, sql_side: str, collected: datetime) -> None:
    """Pure comparison logic behind the live probe, factored out from
    :func:`_verify_utc_timezone` so it can be unit tested directly: a
    genuinely divergent JVM default timezone can't be faked at runtime on
    an already-started JVM, so reproducing the failure mode for a test
    means calling this function with hand-picked disagreeing values
    instead of a real broken session.
    """
    if session_timezone != "UTC":
        raise SparkTimezoneError(
            f"spark.sql.session.timeZone is {session_timezone!r}, expected 'UTC'. "
            "ThreatLake PFA assumes every Spark session runs in UTC."
        )

    if sql_side != _PROBE_WALL_CLOCK:
        # Extremely unlikely - would mean session.timeZone itself doesn't do
        # what it says - but if Spark SQL's own formatting doesn't agree
        # with what was asked for, the Python-side comparison below is moot.
        raise SparkTimezoneError(
            f"Spark SQL formatted a UTC probe timestamp as {sql_side!r}, expected "
            f"{_PROBE_WALL_CLOCK!r}, despite spark.sql.session.timeZone=UTC."
        )

    if collected != _PROBE_EXPECTED_DATETIME:
        offset = collected - _PROBE_EXPECTED_DATETIME
        raise SparkTimezoneError(
            "spark.sql.session.timeZone reports UTC and Spark SQL agrees (a probe "
            f"timestamp formatted as {sql_side!r}), but materializing that same "
            f"timestamp to Python gives {collected} - off by {offset}. This means "
            "the JVM's OS-level default timezone is not UTC, which would silently "
            "shift every timestamp this pipeline collects to the driver."
        )


def _verify_utc_timezone(session: SparkSession) -> None:
    """Run the live probe against ``session`` and raise if it isn't UTC.

    Checks ``spark.sql.session.timeZone`` AND independently re-derives the
    answer by round-tripping a known instant through Spark SQL and back to
    Python - the conf value alone is not trusted.
    """
    from pyspark.sql import functions as F

    probe = session.createDataFrame([(1,)], ["_i"]).select(
        F.to_timestamp(F.lit(_PROBE_WALL_CLOCK)).alias("ts")
    )
    row = probe.select(
        F.col("ts"), F.date_format(F.col("ts"), "yyyy-MM-dd HH:mm:ss").alias("sql_side")
    ).first()
    assert row is not None  # noqa: S101 - always exactly one row from an unconditional select

    _check_utc_timezone(session.conf.get("spark.sql.session.timeZone"), row["sql_side"], row["ts"])


def get_spark(settings: Settings | None = None) -> SparkSession:
    """Return the local SparkSession, verified to run in UTC.

    Raises ``SparkTimezoneError`` rather than return a session that would
    silently corrupt every timestamp it collects.
    """
    settings = settings or get_settings()
    session = _local_session(settings)
    _verify_utc_timezone(session)
    session.sparkContext.setLogLevel(settings.spark.log_level)
    return session


def stop_spark() -> None:
    """Stop the active session, if any."""
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is not None:
        session.stop()
