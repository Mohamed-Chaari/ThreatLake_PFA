"""threatlake.common.config: YAML + env-var precedence, and the extra="forbid" guard."""

from __future__ import annotations

import pytest

from threatlake.common.config import ConfigError, Settings, get_settings


def test_get_settings_loads_local_yaml(local_env: None) -> None:
    settings = get_settings()
    assert settings.env == "local"
    assert settings.tables.bronze == {"cowrie": "bronze_cowrie"}
    assert settings.tables.gold["attacker_profiles"] == "attacker_profiles"


def test_env_var_overrides_yaml(local_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("THREATLAKE_SPARK__APP_NAME", "overridden-app-name")
    settings = get_settings()
    assert settings.spark.app_name == "overridden-app-name"


def test_missing_config_dir_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("THREATLAKE_CONFIG_DIR", "/nonexistent/path/for/sure")
    with pytest.raises(ConfigError):
        get_settings()


def test_unknown_field_is_rejected(local_env: None) -> None:
    """extra='forbid' catches config drift instead of silently ignoring a typo."""
    with pytest.raises(Exception):  # noqa: B017, PT011 - pydantic ValidationError
        Settings(env="local", storage={"root": "x", "quarantine": "y", "landing": "z"}, made_up_field=1)
