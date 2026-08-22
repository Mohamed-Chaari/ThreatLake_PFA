"""FastAPI app factory for ThreatLake PFA's small, read-only API.

Every route reads directly from local Delta tables (silver, gold,
ml_scores) through a single shared SparkSession, created once at app
startup - see threatlake.api.deps for why routes receive Settings/
SparkSession through a dependency rather than a module-level global.

Run it: ``uvicorn threatlake.api.app:app``. The module-level ``app``
below is bound to the process's default Settings (``get_settings()``);
tests use ``create_app(settings=...)`` directly to bind an isolated
instance instead.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from threatlake.api.routers import alerts, attacker_profiles, copilot
from threatlake.common.config import Settings, get_settings
from threatlake.common.spark import get_spark

__all__ = ["app", "create_app"]

_ROUTER_MODULES = (alerts, attacker_profiles, copilot)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build a FastAPI app bound to ``settings`` (or the process default).

    A factory, not a single module-level app, so tests can bind a fresh
    app instance to an isolated tmp-lakehouse Settings/SparkSession pair
    per test without any state leaking between tests.
    """
    resolved_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.settings = resolved_settings
        app.state.spark = get_spark(resolved_settings)
        yield

    api = FastAPI(
        title="ThreatLake PFA - Command Center API",
        description="Read-only API over ThreatLake PFA's local Delta lakehouse.",
        lifespan=lifespan,
    )
    for router_module in _ROUTER_MODULES:
        api.include_router(router_module.router)
    return api


app = create_app()
