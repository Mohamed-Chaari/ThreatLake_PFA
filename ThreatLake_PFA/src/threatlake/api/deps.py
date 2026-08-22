"""FastAPI dependencies for the shared SparkSession and Settings.

Both are resolved exactly once, at app startup (see app.py's lifespan), and
handed to every route via ``Depends`` - never re-created per request.
Getting them through a dependency rather than a module-level global is what
lets tests bind a fresh app instance to an isolated ``tmp_lakehouse``
Settings/SparkSession pair per test, via ``create_app(settings=...)``,
without any of FastAPI's own request handling changing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import Request

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from threatlake.common.config import Settings

__all__ = ["get_settings_dep", "get_spark_dep"]


def get_settings_dep(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def get_spark_dep(request: Request) -> SparkSession:
    spark: SparkSession = request.app.state.spark
    return spark
