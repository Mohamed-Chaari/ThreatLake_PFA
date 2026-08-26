#!/usr/bin/env python3
"""Fetch REAL events from HoneyDB's community sensor-data feed and write
them into the landing zone as NDJSON, in the shape
``config/schema/honeydb.py`` and ``threatlake.ingestion.bronze_transform``
already expect.

ENDPOINT: ``GET https://honeydb.io/api/sensor-data`` (no ``/mydata`` -
that variant is scoped to one's own registered sensor, which this
project doesn't have; the plain path is the community-wide feed every
API key can read). Paginated via ``from-id``, ~1000 events per page -
confirmed against a real authenticated call, not HoneyDB's own docs
(their public API reference renders no static content a fetch can read).

AUTH: a direct ``requests`` call with the ``X-HoneyDb-ApiId``/
``X-HoneyDb-ApiKey`` headers, not the ``honeydb`` SDK package - the same
"small, auditable HTTP call over a new dependency" choice
``threatlake.copilot.text_to_sql`` already made for Gemini.

CREDENTIALS: read from ``HONEYDB_API_ID``/``HONEYDB_API_KEY`` in the
repo's ``.env`` file (gitignored - see .gitignore), loaded by this
script directly rather than pulling in python-dotenv for two variables.
Real env vars already set in the shell take priority over ``.env``.

Usage::

    python scripts/fetch_honeydb.py
    python scripts/fetch_honeydb.py --date 2026-08-25 --target-events 2000
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import requests  # noqa: E402

from threatlake.common.config import get_settings  # noqa: E402
from threatlake.common.paths import landing_path  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _REPO_ROOT / ".env"

_API_URL = "https://honeydb.io/api/sensor-data"
_REQUEST_TIMEOUT_SECONDS = 30
_EVENTS_PER_PAGE = 1000  # observed page size - not documented, confirmed empirically
_DEFAULT_TARGET_EVENTS = 3000
_MAX_PAGES = 10  # safety cap, independent of --target-events


class HoneyDBFetchError(RuntimeError):
    """Raised when HoneyDB credentials are missing or a request fails."""


def _load_dotenv(path: Path) -> None:
    """Load ``KEY=VALUE`` lines from ``path`` into os.environ.

    Minimal by design: two variables don't justify a python-dotenv
    dependency. Never overrides a variable already set in the real
    environment - same precedence a real .env loader would give.
    """
    import os

    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip().strip("'\"")
        os.environ.setdefault(key, value)


def _credentials() -> tuple[str, str]:
    import os

    _load_dotenv(_ENV_FILE)
    api_id = os.environ.get("HONEYDB_API_ID")
    api_key = os.environ.get("HONEYDB_API_KEY")
    if not api_id or not api_key:
        raise HoneyDBFetchError(
            "HONEYDB_API_ID / HONEYDB_API_KEY not set - add both to .env "
            "(see .env for the expected format) or export them before running this script."
        )
    return api_id, api_key


def _fetch_page(
    session: requests.Session, date: str, from_id: int | None
) -> tuple[list[dict[str, Any]], int | None]:
    params: dict[str, Any] = {"sensor-data-date": date}
    if from_id is not None:
        params["from-id"] = from_id
    response = session.get(_API_URL, params=params, timeout=_REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    # Real response shape: [{"data": [...]}, {"from_id": <int>}] - confirmed
    # against a live authenticated call, not assumed from any spec.
    events: list[dict[str, Any]] = payload[0]["data"]
    next_from_id = payload[1].get("from_id")
    return events, next_from_id


def fetch_events(date: str, target_events: int) -> list[dict[str, Any]]:
    """Page through HoneyDB's community feed for ``date`` until either
    ``target_events`` is reached, a page comes back short (that day's
    data is exhausted), or ``_MAX_PAGES`` is hit.
    """
    api_id, api_key = _credentials()
    session = requests.Session()
    session.headers.update({"X-HoneyDb-ApiId": api_id, "X-HoneyDb-ApiKey": api_key})

    events: list[dict[str, Any]] = []
    from_id: int | None = None
    for page in range(1, _MAX_PAGES + 1):
        page_events, next_from_id = _fetch_page(session, date, from_id)
        events.extend(page_events)
        print(f"  page {page}: {len(page_events)} events (from_id -> {next_from_id})")
        if len(page_events) < _EVENTS_PER_PAGE or len(events) >= target_events:
            break
        from_id = next_from_id
    return events


def write_landing_file(events: list[dict[str, Any]], date: str) -> Path:
    settings = get_settings()
    landing_dir = Path(landing_path("honeydb", settings))
    landing_dir.mkdir(parents=True, exist_ok=True)

    raw_lines = [json.dumps(event) for event in events]
    final_path = landing_dir / f"honeydb_{date}.ndjson"
    tmp_path = final_path.with_suffix(".tmp")
    tmp_path.write_text("\n".join(raw_lines) + "\n")
    tmp_path.replace(final_path)
    return final_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=(datetime.now(UTC) - timedelta(days=1)).date().isoformat(),
        help="sensor-data-date to fetch, YYYY-MM-DD (default: yesterday UTC - a "
        "full day's worth of events, unlike a partially-elapsed 'today').",
    )
    parser.add_argument(
        "--target-events",
        type=int,
        default=_DEFAULT_TARGET_EVENTS,
        help=f"stop once at least this many events are fetched (default: {_DEFAULT_TARGET_EVENTS}).",
    )
    args = parser.parse_args()

    print(f"Fetching real HoneyDB community sensor-data for {args.date}...")
    events = fetch_events(args.date, args.target_events)
    if not events:
        raise HoneyDBFetchError(
            f"HoneyDB returned zero events for {args.date} - try a different --date."
        )

    path = write_landing_file(events, args.date)
    print(f"\nWrote {len(events)} real events to {path}")
    print("\nSample event:")
    print(json.dumps(events[0], indent=2))


if __name__ == "__main__":
    main()
