"""Typed, environment-driven configuration for ThreatLake PFA.

A single YAML file (``config/local.yaml``) describes the whole deployment.
Precedence, highest first:

1. explicit keyword arguments to :class:`Settings`
2. process environment (``THREATLAKE_`` prefix, ``__`` nests: e.g.
   ``THREATLAKE_STORAGE__ROOT=/some/other/path``)
3. ``config/local.yaml``
4. field defaults

ThreatLake AI (the full project this is a subset of) supports a second
"databricks" environment behind the same Settings shape, selected by a
``THREATLAKE_ENV`` variable. PFA is local-only by design (see
ARCHITECTURE.md's "Future extensions" section) so that branch is left out
here rather than kept as dead code.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

CONFIG_DIR_VAR = "THREATLAKE_CONFIG_DIR"
ENV_PREFIX = "THREATLAKE_"

Layer = Literal["bronze", "silver", "gold"]
LAYERS: tuple[Layer, ...] = ("bronze", "silver", "gold")


class ConfigError(RuntimeError):
    """Raised when configuration is missing, unreadable, or inconsistent."""


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------
class StorageSettings(BaseModel):
    """Physical locations of data - plain local filesystem paths."""

    model_config = {"extra": "forbid"}

    root: str = Field(description="Root of the lakehouse; bronze/silver/gold hang off this.")
    quarantine: str = Field(description="Root for records rejected by the schema gate.")
    landing: str = Field(description="Directory the cowrie log source drops raw NDJSON into.")

    @field_validator("root", "quarantine", "landing")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("storage locations must be non-empty")
        return value.rstrip("/")


class TableSettings(BaseModel):
    """Logical table key -> physical Delta table name, grouped by medallion layer."""

    model_config = {"extra": "forbid"}

    bronze: dict[str, str] = Field(default_factory=dict)
    silver: dict[str, str] = Field(default_factory=dict)
    gold: dict[str, str] = Field(default_factory=dict)

    def names(self, layer: Layer) -> dict[str, str]:
        """Return the logical -> physical table map for ``layer``."""
        mapping: dict[str, str] = getattr(self, layer)
        return dict(mapping)

    def resolve(self, layer: Layer, table: str) -> str:
        """Resolve a logical table key to its physical name.

        Unregistered keys pass through unchanged so ad-hoc/test tables work
        without editing ``config/local.yaml``.
        """
        return self.names(layer).get(table, table)


class SparkSettings(BaseModel):
    """Spark bootstrap knobs. ``conf`` is applied verbatim to the builder."""

    model_config = {"extra": "forbid"}

    app_name: str = "threatlake-pfa"
    master: str | None = Field(default="local[2]", description="Local Spark master.")
    conf: dict[str, str] = Field(default_factory=dict)
    shuffle_partitions: int = Field(default=8, ge=1)
    log_level: Literal["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"] = "WARN"


class CopilotSettings(BaseModel):
    """SOC Copilot's NL-to-SQL provider.

    Deliberately NOT a place for the API key: it belongs in that provider's
    own conventional env var (``GEMINI_API_KEY``), not a
    THREATLAKE_-prefixed setting or a committed YAML file -
    threatlake.copilot.text_to_sql reads it directly from the environment
    at call time.
    """

    model_config = {"extra": "forbid"}

    provider: Literal["gemini"] = "gemini"
    model: str = "gemini-flash-latest"
    max_output_tokens: int = Field(default=2048, ge=64)


class MLSettings(BaseModel):
    """Where the trained anomaly detector lives on disk."""

    model_config = {"extra": "forbid"}

    model_path: str = Field(description="Local path to the joblib-persisted IsolationForest.")


# ---------------------------------------------------------------------------
# YAML settings source
# ---------------------------------------------------------------------------
def _find_config_dir() -> Path:
    """Locate the ``config/`` directory.

    ``THREATLAKE_CONFIG_DIR`` wins. Otherwise walk up from the working
    directory (a repo checkout), then fall back to the installed package's
    project root (editable installs).
    """
    override = os.environ.get(CONFIG_DIR_VAR)
    if override:
        path = Path(override).expanduser()
        if not path.is_dir():
            raise ConfigError(f"{CONFIG_DIR_VAR}={override!r} is not a directory")
        return path

    candidates = [Path.cwd(), *Path.cwd().parents, *Path(__file__).resolve().parents]
    for base in candidates:
        candidate = base / "config"
        if candidate.is_dir():
            return candidate

    raise ConfigError(
        f"Could not locate a 'config' directory. Run from the repo root or set {CONFIG_DIR_VAR}."
    )


def config_dir() -> Path:
    """Public accessor for the resolved ``config/`` directory.

    Lets other config-driven assets (e.g. ``config/schema/cowrie.py``) use
    the same discovery rules as ``config/local.yaml`` itself.
    """
    return _find_config_dir()


class YamlSettingsSource(PydanticBaseSettingsSource):
    """Feed ``config/local.yaml`` into the settings model."""

    def __call__(self) -> dict[str, Any]:
        path = _find_config_dir() / "local.yaml"
        if not path.is_file():
            raise ConfigError(f"Config file not found: {path}")

        with path.open("rb") as handle:
            loaded = yaml.safe_load(handle) or {}
        if not isinstance(loaded, dict):
            raise ConfigError(f"{path} must contain a YAML mapping at the top level")

        loaded["config_file"] = str(path)
        return loaded

    def get_field_value(  # pragma: no cover - required by the ABC, unused
        self, field: Any, field_name: str
    ) -> tuple[Any, str, bool]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Root settings
# ---------------------------------------------------------------------------
class Settings(BaseSettings):
    """Fully-resolved ThreatLake PFA configuration."""

    model_config = SettingsConfigDict(
        env_prefix=ENV_PREFIX,
        env_nested_delimiter="__",
        extra="forbid",
        frozen=True,
    )

    env: str = "local"
    config_file: str | None = None

    storage: StorageSettings
    tables: TableSettings
    spark: SparkSettings = Field(default_factory=SparkSettings)
    copilot: CopilotSettings = Field(default_factory=CopilotSettings)
    ml: MLSettings

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Order sources: init > environment > YAML > defaults."""
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlSettingsSource(settings_cls),
            file_secret_settings,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings, loading them on first use."""
    return Settings()


def reset_settings() -> None:
    """Drop the cached settings. Used by tests that swap environments."""
    get_settings.cache_clear()
