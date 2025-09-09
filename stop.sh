#!/usr/bin/env bash
set -euo pipefail

# --- Ports (change if you customized them) ---
API_PORT="${API_PORT:-8080}"
WORKER_LOG_PORT="${WORKER_LOG_PORT:-8793}"
TRIGGER_LOG_PORT="${TRIGGER_LOG_PORT:-8794}"

say() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

kill_pid_safely() {
  # Kill a single PID if it's still alive (quietly).
  local pid="$1" label="$2"
  if [[ -n "${pid:-}" ]] && ps -p "$pid" >/dev/null 2>&1; then
    kill "$pid" 2>/dev/null || true
    sleep 0.5
    if ps -p "$pid" >/dev/null 2>&1; then
      say "Force killing $label (PID $pid)"
      kill -9 "$pid" 2>/dev/null || true
    fi
  fi
}

kill_list() {
  # Kill a list of PIDs safely (accepts newline/space separated).
  local label="$1"; shift
  # Normalize to unique newline-separated list
  local pids norm
  pids="$(printf '%s\n' "$@" | tr ' ' '\n' | sed '/^$/d' | sort -u)"
  if [[ -z "$pids" ]]; then
    say "$label: nothing to kill"
    return
  fi
  say "Stopping $label (PIDs: $(echo "$pids" | tr '\n' ' '))"
  while IFS= read -r pid; do
    kill_pid_safely "$pid" "$label"
  done <<< "$pids"
}

kill_pidfile () {
  # Kill process from a pidfile, if it exists and is alive.
  local name="$1" pidfile="$2"
  if [[ -f "$pidfile" ]]; then
    local pid
    pid="$(cat "$pidfile" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && ps -p "$pid" >/dev/null 2>&1; then
      say "Stopping $name (PID $pid via $pidfile)"
      kill_pid_safely "$pid" "$name"
    else
      say "$name: pidfile present but process not running"
    fi
    rm -f "$pidfile" 2>/dev/null || true
  else
    say "$name: pidfile not found"
  fi
}

kill_by_pattern () {
  # Find & kill by process command-line pattern (pgrep -f).
  local name="$1" pattern="$2"
  mapfile -t arr < <(pgrep -f "$pattern" 2>/dev/null || true)
  if (( ${#arr[@]} == 0 )); then
    say "$name: not found by pattern"
    return
  fi
  kill_list "$name" "${arr[@]}"
}

kill_by_port () {
  # Kill anything listening on a TCP port (last resort).
  local port="$1"
  local name="port:${port}"

  # If lsof isn't available, skip quietly
  if ! command -v lsof >/dev/null 2>&1; then
    say "lsof not found; skipping port ${port}"
    return
  fi

  mapfile -t arr < <(lsof -ti TCP:"$port" -sTCP:LISTEN 2>/dev/null | sort -u || true)
  if (( ${#arr[@]} == 0 )); then
    say "No listeners on port ${port}"
    return
  fi
  say "Killing listeners on port ${port}: ${arr[*]}"
  kill_list "$name" "${arr[@]}"
}

say "Stopping Airflow components..."

# 1) Try PID files (only if your start.sh created them)
kill_pidfile "API Server"      ".airflow_api.pid"
kill_pidfile "Webserver"       ".airflow_web.pid"
kill_pidfile "Scheduler"       ".airflow_sched.pid"
kill_pidfile "DAG Processor"   ".airflow_proc.pid"
kill_pidfile "Triggerer"       ".airflow_trig.pid"

# 2) Fallback: search by patterns (covers API server & legacy webserver)
kill_by_pattern "API Server"              "airflow api start"
kill_by_pattern "API Server (gunicorn)"   "gunicorn .*airflow.*api"
kill_by_pattern "API Server (uvicorn)"    "uvicorn .*airflow.*api"

kill_by_pattern "Webserver"               "airflow webserver"
kill_by_pattern "Webserver (gunicorn)"    "gunicorn .*airflow.*webserver"

kill_by_pattern "Scheduler"               "airflow scheduler"
kill_by_pattern "DAG Processor"           "airflow dag-processor|airflow dag_processor"
kill_by_pattern "Triggerer"               "airflow triggerer"

# 3) Last resort: kill by important ports
kill_by_port "$API_PORT"
kill_by_port "$WORKER_LOG_PORT"
kill_by_port "$TRIGGER_LOG_PORT"

say "All components stopped."
echo " see you soon!"
