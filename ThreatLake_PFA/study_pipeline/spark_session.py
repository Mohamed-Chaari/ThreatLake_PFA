"""Build a local SparkSession with Delta Lake turned on.

This is a trimmed-down version of the real project's common/spark.py.
The real one also knows how to attach to an already-running Databricks
cluster, and it runs a live round-trip probe after startup to PROVE the
session is really in UTC (not just trust the config setting - see that
file's docstring and docs/adr/0001 in the real repo for why that extra
proof step exists). This study copy only ever runs locally, so all of
that is cut down to the one trick that actually matters here.
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def build_spark(app_name: str = "threatlake-study") -> SparkSession:
    """Return a local SparkSession that can read/write Delta tables.

    Three things are set up here, in plain language:

    1. TZ=UTC, set as a process environment variable BEFORE Spark's Java
       process starts. Every timestamp this pipeline touches (event_time,
       ingest_date, ...) is meant to be UTC. Spark SQL has its own
       "session timezone" setting for that, but when a timestamp gets
       pulled out of Spark and into a plain Python value (which happens
       here, since we print rows to the terminal), that conversion goes
       through the underlying Java process's OWN default timezone - not
       Spark SQL's setting. If your laptop's system timezone isn't UTC,
       printed timestamps would silently be off by your UTC offset. This
       one line prevents that category of bug entirely, before Spark
       even starts.
    2. PYSPARK_PYTHON, pinned to the exact Python interpreter running
       this script. Spark launches separate worker processes to run our
       Python code in parallel; without telling it which interpreter to
       use, it might grab a different (or missing) "python3" from your
       system PATH and fail with a confusing version-mismatch error.
    3. Delta Lake's extensions, registered via the two spark.sql.*
       config keys below - this is what makes `.format("delta")` work
       on both read and write.
    """
    os.environ.setdefault("TZ", "UTC")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")  # 2 threads is plenty for a few sample rows
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    # configure_spark_with_delta_pip wires up the Delta Lake jar files that
    # actually implement the Delta table format - without this, "delta" is
    # just a format string Spark doesn't know how to handle.
    return configure_spark_with_delta_pip(builder).getOrCreate()
