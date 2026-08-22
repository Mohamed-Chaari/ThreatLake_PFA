#!/usr/bin/env python3
"""One-shot SYNTHETIC cowrie log generator, for local demo/test use only.

*** EVERY EVENT THIS SCRIPT WRITES IS FAKE. *** Every line is marked as
such in two places inside cowrie's own JSON schema, not in some extra
field a real T-Pot deployment doesn't have and PERMISSIVE JSON parsing
would silently drop: ``sensor`` is always ``"tpot-synthetic-generator"``
and the free-text ``message`` is always prefixed ``"[SYNTHETIC] "``.

Writes ONE NDJSON file into the configured landing zone
(threatlake.common.paths.landing_path("cowrie")), atomically (written to
a ``.tmp`` path, then renamed into place) - never appended-to in place.

Three kinds of content, all with FIXED historical timestamps (not
wall-clock "now" - PFA is a batch pipeline with no live/streaming path to
exercise, so there's no reason for the data to look like it just
happened):

  background noise    Many ordinary attacker IPs, one short session each
                       (connect, maybe one login attempt, close), spread
                       across a 2-hour window and a handful of common
                       ports - the population both detectors' baseline
                       should look like.

  port-scan burst      ONE src_ip connecting to many DISTINCT ports in a
                       tight time window - what
                       threatlake.ml.rules.port_scan_rule and the
                       distinct_dst_ports_touched_by_src_ip_in_window
                       feature (threatlake.ml.features) are built to
                       catch.

  brute-force burst    ONE src_ip making many repeated login attempts
                       against the SAME port in a tight time window,
                       ending in one success and a malware download - the
                       other pattern threatlake.ml.train_anomaly's own
                       docstring discusses (and the reason a fixed rule
                       runs alongside the model instead of replacing it -
                       see that module).

Plus one deliberately malformed line (not valid JSON at all), so
run_pipeline.py's bronze step has something real to quarantine.

Usage::

    python scripts/generate_synthetic_cowrie.py
"""

from __future__ import annotations

import json
import random
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from threatlake.common.config import get_settings  # noqa: E402
from threatlake.common.paths import landing_path  # noqa: E402

_SEED = 20260820
_SENSOR = "tpot-synthetic-generator"
_SYNTHETIC_PREFIX = "[SYNTHETIC] "
_HONEYPOT_DST_IP = "10.0.0.5"

#: Base wall-clock instant every offset below is computed from - fixed, so
#: re-running this script produces byte-identical output.
_BASE_TIME = datetime(2026, 8, 20, 8, 0, 0, tzinfo=UTC)

_COMMON_PORTS = (22, 23, 2222, 8080, 3389, 21, 25, 80)
_USERNAMES = ("root", "admin", "user", "test", "pi", "ubuntu", "oracle")
_PASSWORDS = ("123456", "password", "admin", "toor", "qwerty", "letmein", "changeme")

_N_BACKGROUND_ATTACKERS = 60
_PORT_SCAN_PORTS = (21, 22, 23, 25, 80, 443, 2222, 3306, 8080)
_BRUTE_FORCE_ATTEMPTS = 20


def _rand_ip(rng: random.Random) -> str:
    return f"{rng.randint(1, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"


def _ts(dt: datetime) -> str:
    """Cowrie's timestamp format: 6-digit microseconds + 'Z' - see
    threatlake.transform.silver.cowrie's own to_timestamp pattern.
    """
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _connect(src_ip: str, dst_port: int, event_time: datetime, session: str) -> dict[str, Any]:
    protocol = "telnet" if dst_port == 23 else "ssh"
    return {
        "eventid": "cowrie.session.connect",
        "timestamp": _ts(event_time),
        "session": session,
        "src_ip": src_ip,
        "src_port": random.randint(1024, 65000),  # noqa: S311
        "dst_ip": _HONEYPOT_DST_IP,
        "dst_port": dst_port,
        "protocol": protocol,
        "sensor": _SENSOR,
        "message": f"{_SYNTHETIC_PREFIX}New connection: {src_ip}:{dst_port}",
    }


def _login_failed(src_ip: str, event_time: datetime, session: str, rng: random.Random) -> dict[str, Any]:
    return {
        "eventid": "cowrie.login.failed",
        "timestamp": _ts(event_time),
        "session": session,
        "src_ip": src_ip,
        "username": rng.choice(_USERNAMES),
        "password": rng.choice(_PASSWORDS),
        "sensor": _SENSOR,
        "message": f"{_SYNTHETIC_PREFIX}login attempt failed",
    }


def _login_success(src_ip: str, event_time: datetime, session: str) -> dict[str, Any]:
    return {
        "eventid": "cowrie.login.success",
        "timestamp": _ts(event_time),
        "session": session,
        "src_ip": src_ip,
        "username": "root",
        "password": "toor",
        "sensor": _SENSOR,
        "message": f"{_SYNTHETIC_PREFIX}login attempt succeeded",
    }


def _file_download(src_ip: str, event_time: datetime, session: str) -> dict[str, Any]:
    return {
        "eventid": "cowrie.session.file_download",
        "timestamp": _ts(event_time),
        "session": session,
        "src_ip": src_ip,
        "url": "http://185.220.101.7/mirai.arm7",
        "outfile": "/tmp/cowrie/downloads/9f8e7d.bin",
        "shasum": uuid.uuid4().hex + uuid.uuid4().hex,
        "destfile": "mirai.arm7",
        "duplicate": False,
        "sensor": _SENSOR,
        "message": f"{_SYNTHETIC_PREFIX}Downloaded URL",
    }


def _session_closed(event_time: datetime, session: str, duration_ms: float) -> dict[str, Any]:
    return {
        "eventid": "cowrie.session.closed",
        "timestamp": _ts(event_time),
        "session": session,
        "duration_ms": duration_ms,
        "sensor": _SENSOR,
        "message": f"{_SYNTHETIC_PREFIX}Connection closed",
    }


def _background_noise(rng: random.Random) -> list[dict[str, Any]]:
    """Many ordinary attackers, one short session each, spread across a
    2-hour window.
    """
    lines: list[dict[str, Any]] = []
    for i in range(_N_BACKGROUND_ATTACKERS):
        src_ip = _rand_ip(rng)
        session = uuid.uuid4().hex[:8]
        port = rng.choice(_COMMON_PORTS)
        start = _BASE_TIME + timedelta(seconds=i * 90 + rng.randint(0, 60))

        lines.append(_connect(src_ip, port, start, session))
        if rng.random() < 0.5:  # about half of the background sessions attempt a login
            lines.append(_login_failed(src_ip, start + timedelta(seconds=2), session, rng))
        lines.append(_session_closed(start + timedelta(seconds=5), session, duration_ms=5000.0))
    return lines


def _port_scan_burst(rng: random.Random) -> tuple[list[dict[str, Any]], str]:
    """One src_ip, one connection per port, 2 seconds apart - the shape
    threatlake.ml.rules.port_scan_rule looks for.
    """
    src_ip = _rand_ip(rng)
    base = _BASE_TIME + timedelta(hours=1)
    lines: list[dict[str, Any]] = []
    for j, port in enumerate(_PORT_SCAN_PORTS):
        event_time = base + timedelta(seconds=j * 2)
        lines.append(_connect(src_ip, port, event_time, session=f"scan-{j}"))
    return lines, src_ip


def _brute_force_burst(rng: random.Random) -> tuple[list[dict[str, Any]], str]:
    """One src_ip, many login attempts against the same port, 3 seconds
    apart, ending in a success and a malware download.
    """
    src_ip = _rand_ip(rng)
    session = uuid.uuid4().hex[:8]
    base = _BASE_TIME + timedelta(hours=1, minutes=10)
    lines: list[dict[str, Any]] = [_connect(src_ip, 22, base, session)]

    for j in range(_BRUTE_FORCE_ATTEMPTS):
        event_time = base + timedelta(seconds=(j + 1) * 3)
        lines.append(_login_failed(src_ip, event_time, session, rng))

    success_time = base + timedelta(seconds=(_BRUTE_FORCE_ATTEMPTS + 1) * 3)
    lines.append(_login_success(src_ip, success_time, session))
    lines.append(_file_download(src_ip, success_time + timedelta(seconds=3), session))
    lines.append(_session_closed(success_time + timedelta(seconds=10), session, duration_ms=190000.0))
    return lines, src_ip


def generate() -> Path:
    """Build the full synthetic batch and write it as one NDJSON file."""
    rng = random.Random(_SEED)  # fixed seed: reproducible demo data, not security-sensitive

    lines = _background_noise(rng)
    scan_lines, scan_ip = _port_scan_burst(rng)
    brute_lines, brute_ip = _brute_force_burst(rng)
    lines.extend(scan_lines)
    lines.extend(brute_lines)

    settings = get_settings()
    landing_dir = Path(landing_path("cowrie", settings))
    landing_dir.mkdir(parents=True, exist_ok=True)

    raw_lines = [json.dumps(line) for line in lines]
    # One deliberately malformed line, exercising bronze's quarantine path
    # (threatlake.ingestion.bronze_transform.split_quarantine) - not valid
    # JSON at all, on purpose.
    raw_lines.append("{this line is not valid JSON at all - it exists on purpose}")

    final_path = landing_dir / "synthetic_cowrie_batch.ndjson"
    tmp_path = final_path.with_suffix(".tmp")
    tmp_path.write_text("\n".join(raw_lines) + "\n")
    tmp_path.replace(final_path)

    print(f"Wrote {len(raw_lines)} lines ({len(raw_lines) - 1} valid + 1 malformed) to {final_path}")
    print(f"  background attackers : {_N_BACKGROUND_ATTACKERS}")
    print(f"  port-scan attacker   : {scan_ip} ({len(_PORT_SCAN_PORTS)} distinct ports)")
    print(f"  brute-force attacker : {brute_ip} ({_BRUTE_FORCE_ATTEMPTS} failed logins)")
    return final_path


if __name__ == "__main__":
    generate()
