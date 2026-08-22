"""End-to-end tests for the 3 API route groups (alerts, attacker_profiles,
copilot), each bound to an isolated tmp-lakehouse Settings/SparkSession
via threatlake.api.app.create_app - see that module's docstring for why
this is a factory rather than a single module-level app."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from fastapi.testclient import TestClient

from threatlake.api.app import create_app
from threatlake.common.paths import gold_path
from threatlake.ml.score_events import SCORES_SCHEMA
from threatlake.transform.gold.attacker_profiles import write_attacker_profiles
from tests.unit.silver_fixture_helpers import silver_row, write_silver_fixture

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from threatlake.common.config import Settings

_EVENT_TIME = datetime(2026, 8, 2, 8, 0, tzinfo=UTC)
_SCORED_AT = datetime(2026, 8, 2, 8, 1, tzinfo=UTC)


def _write_ml_scores(spark: SparkSession, settings: Settings, *rows: dict[str, object]) -> None:
    spark.createDataFrame(list(rows), schema=SCORES_SCHEMA).write.format("delta").mode(
        "overwrite"
    ).save(gold_path("ml_scores", settings))


@pytest.fixture
def seeded_app(spark: SparkSession, tmp_lakehouse: Settings, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)

    write_silver_fixture(
        spark,
        tmp_lakehouse,
        silver_row(
            event_id="flagged-1", src_ip="9.9.9.9", dst_port=2222, severity=4,
            attack_category="connection", event_time=_EVENT_TIME,
        ),
        silver_row(
            event_id="normal-1", src_ip="1.1.1.1", dst_port=22, severity=1,
            attack_category="connection", event_time=_EVENT_TIME,
        ),
    )
    write_attacker_profiles(spark, tmp_lakehouse)
    _write_ml_scores(
        spark,
        tmp_lakehouse,
        {
            "event_id": "flagged-1", "scored_at": _SCORED_AT, "anomaly_score": -0.2,
            "is_anomaly": True, "alert_source": "rule",
        },
        {
            "event_id": "normal-1", "scored_at": _SCORED_AT, "anomaly_score": 0.1,
            "is_anomaly": False, "alert_source": None,
        },
    )

    app = create_app(settings=tmp_lakehouse)
    with TestClient(app) as client:
        yield client


def test_alerts_empty_before_any_pipeline_run(spark: SparkSession, tmp_lakehouse: Settings) -> None:
    app = create_app(settings=tmp_lakehouse)
    with TestClient(app) as client:
        response = client.get("/alerts")
    assert response.status_code == 200  # noqa: PLR2004
    assert response.json() == {"total": 0, "limit": 50, "offset": 0, "items": []}


def test_alerts_only_returns_flagged_events(seeded_app: TestClient) -> None:
    response = seeded_app.get("/alerts")
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["event_id"] == "flagged-1"
    assert body["items"][0]["alert_source"] == "rule"


def test_alerts_severity_filter(seeded_app: TestClient) -> None:
    response = seeded_app.get("/alerts", params={"severity": 1})
    assert response.json()["total"] == 0  # the only alert has severity=4


def test_attacker_profiles_list(seeded_app: TestClient) -> None:
    response = seeded_app.get("/attacker_profiles")
    body = response.json()
    assert body["total"] == 2  # noqa: PLR2004
    ips = {item["src_ip"] for item in body["items"]}
    assert ips == {"9.9.9.9", "1.1.1.1"}


def test_attacker_profile_drilldown_includes_its_alerts(seeded_app: TestClient) -> None:
    response = seeded_app.get("/attacker_profiles/9.9.9.9")
    body = response.json()
    assert body["profile"]["src_ip"] == "9.9.9.9"
    assert len(body["alerts"]) == 1
    assert body["alerts"][0]["event_id"] == "flagged-1"


def test_attacker_profile_drilldown_404_for_unknown_ip(seeded_app: TestClient) -> None:
    response = seeded_app.get("/attacker_profiles/8.8.8.8")
    assert response.status_code == 404  # noqa: PLR2004


def test_copilot_query_rejected_when_no_api_key_configured(seeded_app: TestClient) -> None:
    response = seeded_app.post("/copilot/query", json={"question": "how many attackers are there"})
    body = response.json()
    assert body["rejected"] is True
    assert "GEMINI_API_KEY" in body["reason"]
    assert body["rows"] is None
