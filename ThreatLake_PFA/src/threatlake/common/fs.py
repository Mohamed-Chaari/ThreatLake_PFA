"""File operations that work identically against a local filesystem and
Databricks storage (ABFSS, DBFS), for locations under the configured storage
roots.

Uses the JVM's ``org.apache.hadoop.fs.FileSystem`` (reached through Spark's
own Hadoop configuration) instead of Python's ``os``/``shutil``, because
storage roots can be plain local paths or cloud URIs - only the Hadoop FS
abstraction speaks both identically. This is also why ``dbutils`` is never
used here: ``FileSystem.rename`` already behaves the same against ``file://``
and ``abfss://``, with no cluster/notebook dependency.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.sql import SparkSession

__all__ = ["list_files", "move"]


def _hadoop_path(spark: SparkSession, path: str) -> JavaObject:
    # _jvm/_jsc are only None before a SparkContext exists; an active `spark`
    # session (this function's precondition) always has both populated.
    jvm = spark.sparkContext._jvm
    assert jvm is not None  # noqa: S101 - type narrowing, not a runtime guard
    result: JavaObject = jvm.org.apache.hadoop.fs.Path(path)
    return result


def _filesystem_for(spark: SparkSession, hadoop_path: JavaObject) -> JavaObject:
    jsc = spark.sparkContext._jsc
    assert jsc is not None  # noqa: S101 - type narrowing, not a runtime guard
    hadoop_conf = jsc.hadoopConfiguration()
    result: JavaObject = hadoop_path.getFileSystem(hadoop_conf)
    return result


def list_files(spark: SparkSession, directory: str) -> list[str]:
    """Non-recursive list of file (not subdirectory) paths directly under ``directory``.

    Returns ``[]`` if the directory does not exist yet - an empty/absent
    landing directory is a normal state, not an error.
    """
    path = _hadoop_path(spark, directory)
    fs = _filesystem_for(spark, path)
    if not fs.exists(path):
        return []
    return [
        status.getPath().toString()
        for status in fs.listStatus(path)
        if not status.isDirectory() and not status.getPath().getName().startswith((".", "_"))
    ]


def move(spark: SparkSession, source: str, destination: str) -> None:
    """Move a single file, creating destination parent directories as needed.

    A move, never a copy-and-delete-source performed from Python: this calls
    the filesystem's own atomic-where-possible rename, which is what makes it
    safe to use as an audit trail (the file never exists in two places at
    once from the caller's perspective, and nothing is ever deleted).
    """
    src_path = _hadoop_path(spark, source)
    fs = _filesystem_for(spark, src_path)
    dest_path = _hadoop_path(spark, destination)
    fs.mkdirs(dest_path.getParent())
    if not fs.rename(src_path, dest_path):
        raise OSError(f"Failed to move {source!r} -> {destination!r}")
