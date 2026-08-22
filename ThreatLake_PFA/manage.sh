#!/usr/bin/env bash
# Start/stop/status for the whole local ThreatLake PFA stack: the batch
# pipeline (bronze -> silver -> gold -> train -> score), the FastAPI
# server, and the dashboard dev server - one command instead of running
# each step by hand across several terminals.
#
# Usage:
#   ./manage.sh start     generate data (if none yet), run the pipeline,
#                         start the API + dashboard in the background
#   ./manage.sh stop      stop the API + dashboard (data on disk is never touched)
#   ./manage.sh status    report whether the API/dashboard are running, with PIDs
#   ./manage.sh restart   stop, then start
#
# Every path below is resolved relative to THIS SCRIPT's own location,
# not the caller's current directory, so `./manage.sh start` works the
# same whether you're in this folder or somewhere else entirely.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

VENV_DIR="$SCRIPT_DIR/.venv"
PYTHON="$VENV_DIR/bin/python"
UVICORN="$VENV_DIR/bin/uvicorn"

LANDING_DIR="$SCRIPT_DIR/data/landing"
LOG_DIR="$SCRIPT_DIR/logs"
RUN_DIR="$SCRIPT_DIR/.run"
API_PID_FILE="$RUN_DIR/api.pid"
DASHBOARD_PID_FILE="$RUN_DIR/dashboard.pid"
API_LOG="$LOG_DIR/api.log"
DASHBOARD_LOG="$LOG_DIR/dashboard.log"

API_PORT=8000
DASHBOARD_PORT=5173
API_URL="http://127.0.0.1:$API_PORT"
DASHBOARD_URL="http://127.0.0.1:$DASHBOARD_PORT"

# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

# True (exit 0) if the PID saved in $1 belongs to a live process. A
# pidfile that merely EXISTS is not enough - the process it named may
# have crashed or been killed by something else since, which is exactly
# the case start/stop must not be fooled by.
is_alive() {
    pid_file="$1"
    if [ ! -f "$pid_file" ]; then
        return 1
    fi
    pid="$(cat "$pid_file")"
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
        return 1
    fi
    return 0
}

require_venv() {
    if [ ! -x "$PYTHON" ]; then
        echo "No virtualenv at $VENV_DIR - set it up first:" >&2
        echo "" >&2
        echo "  cd \"$SCRIPT_DIR\"" >&2
        echo "  uv venv --python 3.11 .venv" >&2
        echo "  uv pip install --python .venv/bin/python -e \".[dev]\"" >&2
        exit 1
    fi
}

resolve_java_home() {
    if ! JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null)"; then
        echo "No Java 21 found (checked via /usr/libexec/java_home -v 21)." >&2
        echo "Spark needs it - install Java 21 (e.g. 'brew install openjdk@21') and retry." >&2
        exit 1
    fi
    export JAVA_HOME
}

# Poll a URL until it answers or the timeout elapses. Used so `start`
# reports what actually happened, not just "a process was launched".
wait_for_http() {
    url="$1"
    timeout_seconds="$2"
    elapsed=0
    while [ "$elapsed" -lt "$timeout_seconds" ]; do
        if curl -s -o /dev/null "$url"; then
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
        printf "."
    done
    return 1
}

# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------

start_pipeline() {
    mkdir -p "$LANDING_DIR/cowrie"

    # Recursive: bronze ingestion MOVES consumed files into
    # data/landing/cowrie/_processed/<date>/ (see
    # threatlake.ingestion.bronze_writer), so a plain top-level check
    # would find nothing after the first successful pipeline run and
    # regenerate a new synthetic batch on every single start. Looking
    # recursively means "has a batch ever been generated", which is the
    # actual thing this check is meant to answer.
    if find "$LANDING_DIR" -name '*.ndjson' 2>/dev/null | grep -q .; then
        echo "Synthetic cowrie data already exists under $LANDING_DIR - skipping generation."
    else
        echo "No synthetic cowrie data found - generating a batch..."
        "$PYTHON" "$SCRIPT_DIR/scripts/generate_synthetic_cowrie.py"
    fi

    echo ""
    echo "Running the pipeline (bronze -> silver -> gold -> train -> score)..."
    "$PYTHON" "$SCRIPT_DIR/scripts/run_pipeline.py"
}

start_api() {
    if is_alive "$API_PID_FILE"; then
        echo "API already running (pid $(cat "$API_PID_FILE")) - not starting another."
        return
    fi

    echo "Starting API on port $API_PORT (log: $API_LOG)..."
    nohup "$UVICORN" threatlake.api.app:app \
        --app-dir "$SCRIPT_DIR/src" \
        --port "$API_PORT" \
        >"$API_LOG" 2>&1 &
    echo $! >"$API_PID_FILE"

    printf "  waiting for it to come up"
    if wait_for_http "$API_URL/docs" 90; then
        echo " up (pid $(cat "$API_PID_FILE"))."
    else
        echo ""
        echo "  WARNING: API did not answer within 90s - check $API_LOG" >&2
    fi
}

start_dashboard() {
    if is_alive "$DASHBOARD_PID_FILE"; then
        echo "Dashboard already running (pid $(cat "$DASHBOARD_PID_FILE")) - not starting another."
        return
    fi

    if [ ! -x "$SCRIPT_DIR/dashboard/node_modules/.bin/vite" ]; then
        echo "Dashboard dependencies not installed - run 'npm install' in $SCRIPT_DIR/dashboard first." >&2
        exit 1
    fi

    echo "Starting dashboard on port $DASHBOARD_PORT (log: $DASHBOARD_LOG)..."
    # Invoke vite's own binary directly, not `npm run dev`: npm wraps the
    # actual vite process in a child of its own, and `kill` on npm's PID
    # is not reliably forwarded to that child on every npm version - the
    # dev server can be left running, bound to the port, after `stop`
    # thinks it succeeded. Calling vite directly means the PID we save IS
    # the process serving the dashboard.
    #
    # --host 127.0.0.1: vite's own default binds IPv6-only (::1) - it
    # answers `curl http://localhost:5173` but refuses
    # `curl http://127.0.0.1:5173` (connection refused, confirmed
    # directly). Binding IPv4 explicitly keeps this consistent with the
    # API (uvicorn's own default) and with DASHBOARD_URL above.
    (
        cd "$SCRIPT_DIR/dashboard"
        nohup node_modules/.bin/vite --host 127.0.0.1 --port "$DASHBOARD_PORT" >"$DASHBOARD_LOG" 2>&1 &
        echo $! >"$DASHBOARD_PID_FILE"
    )

    printf "  waiting for it to come up"
    if wait_for_http "$DASHBOARD_URL/" 30; then
        echo " up (pid $(cat "$DASHBOARD_PID_FILE"))."
    else
        echo ""
        echo "  WARNING: dashboard did not answer within 30s - check $DASHBOARD_LOG" >&2
    fi
}

cmd_start() {
    resolve_java_home
    export THREATLAKE_CONFIG_DIR="$SCRIPT_DIR/config"
    require_venv
    mkdir -p "$LOG_DIR" "$RUN_DIR"

    start_pipeline
    echo ""
    start_api
    start_dashboard

    echo ""
    echo "ThreatLake PFA is up:"
    echo "  API:       $API_URL  (docs at $API_URL/docs)"
    echo "  Dashboard: $DASHBOARD_URL"
    echo ""
    echo "Logs: $API_LOG , $DASHBOARD_LOG"
}

# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------

# Stop one saved process by name. Never touches anything under data/ -
# this only ever sends signals to a PID, nothing else.
stop_one() {
    name="$1"
    pid_file="$2"

    if [ ! -f "$pid_file" ]; then
        echo "$name: not running (no pidfile)."
        return
    fi

    pid="$(cat "$pid_file")"
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
        echo "$name: not running (stale pidfile removed)."
        rm -f "$pid_file"
        return
    fi

    kill -TERM "$pid" 2>/dev/null || true
    waited=0
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 10 ]; do
        sleep 1
        waited=$((waited + 1))
    done

    if kill -0 "$pid" 2>/dev/null; then
        echo "$name: still alive after SIGTERM - sending SIGKILL (pid $pid)."
        kill -KILL "$pid" 2>/dev/null || true
        sleep 1
    fi

    if kill -0 "$pid" 2>/dev/null; then
        echo "$name: FAILED to stop (pid $pid still alive)." >&2
    else
        echo "$name: stopped (pid $pid)."
        rm -f "$pid_file"
    fi
}

cmd_stop() {
    stop_one "API" "$API_PID_FILE"
    stop_one "Dashboard" "$DASHBOARD_PID_FILE"
}

# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

status_one() {
    name="$1"
    pid_file="$2"
    url="$3"

    if is_alive "$pid_file"; then
        pid="$(cat "$pid_file")"
        if curl -s -o /dev/null "$url"; then
            echo "$name: running (pid $pid) - $url is responding."
        else
            echo "$name: process running (pid $pid) but $url is NOT responding yet."
        fi
    else
        echo "$name: not running."
    fi
}

cmd_status() {
    status_one "API" "$API_PID_FILE" "$API_URL/docs"
    status_one "Dashboard" "$DASHBOARD_PID_FILE" "$DASHBOARD_URL/"
}

# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

case "${1:-}" in
    start)
        cmd_start
        ;;
    stop)
        cmd_stop
        ;;
    status)
        cmd_status
        ;;
    restart)
        cmd_stop
        echo ""
        cmd_start
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart}" >&2
        exit 1
        ;;
esac
