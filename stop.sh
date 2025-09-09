#!/usr/bin/env bash
# Stop Apache Airflow components safely.
# It uses PID files (from start.sh), falls back to pattern killing, and clears busy ports.
# Safe to run multiple times.

set -Eeuo pipefail

# --------- Paths & logging ----------
AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow}"
CTL_LOG_DIR="${CTL_LOG_DIR:-"$AIRFLOW_HOME/ctl-logs"}"
PID_DIR="${PID_DIR:-"$AIRFLOW_HOME/pids"}"
mkdir -p "$CTL_LOG_DIR" "$PID_DIR"

ts="$(date +%Y%m%dT%H%M%S)"
LOG_FILE="$CTL_LOG_DIR/stop_${ts}.log"
ln -sfn "$LOG_FILE" "$CTL_LOG_DIR/latest-stop.log"

# Mirror all output to log file
exec > >(tee -a "$LOG_FILE") 2>&1

# Prune old control logs (optional)
find "$CTL_LOG_DIR" -type f -name 'stop_*.log' -mtime +14 -delete 2>/dev/null || true

echo "[$(date +%F' '%T)] Stopping Airflow components…"

# --------- Helpers ----------
stop_by_pidfile() {
  local name="$1" pidfile="$2"
  if [[ ! -f "$pidfile" ]]; then
    echo "[$(date +%T)] $name: pidfile not found"
    return 1
  fi
  local pid
  pid="$(cat "$pidfile" 2>/dev/null || true)"
  if [[ -z "${pid:-}" ]]; then
    echo "[$(date +%T)] $name: pidfile empty"
    rm -f "$pidfile"
    return 1
  fi

  if kill -0 "$pid" 2>/dev/null; then
    echo "[$(date +%T)] Stopping $name (PID $pid)…"
    kill "$pid" 2>/dev/null || true
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
      echo "[$(date +%T)] Force killing $name (PID $pid)…"
      kill -9 "$pid" 2>/dev/null || true
      sleep 1
    fi
    echo "[$(date +%T)] $name stopped."
  else
    echo "[$(date +%T)] $name: process not running."
  fi
  rm -f "$pidfile" 2>/dev/null || true
}

stop_by_pattern() {
  local label="$1" pattern="$2"
  # Collect PIDs for this pattern (if any)
  local pids
  pids="$(pgrep -f "$pattern" || true)"
  if [[ -z "$pids" ]]; then
    echo "[$(date +%T)] $label: not found by pattern"
    return 0
  fi
  echo "[$(date +%T)] Stopping $label by pattern: $pattern (PIDs: $pids)"
  # Graceful
  xargs -r -n1 kill <<<"$pids" 2>/dev/null || true
  sleep 1
  # Force
  pids="$(pgrep -f "$pattern" || true)"
  if [[ -n "$pids" ]]; then
    echo "[$(date +%T)] Force killing $label (PIDs: $pids)"
    xargs -r -n1 kill -9 <<<"$pids" 2>/dev/null || true
  fi
}

kill_ports() {
  # Adjust if your ports differ:
  # 8080: API (or web)
  # 8793: worker log server
  # 8794: trigger log server
  # 8974: scheduler health (if enabled)
  local PORTS=(8080 8793 8794 8974)
  for port in "${PORTS[@]}"; do
    local pids
    pids="$(lsof -ti :"$port" 2>/dev/null || true)"
    if [[ -z "$pids" ]]; then
      echo "[$(date +%T)] No listeners on port $port"
      continue
    fi
    echo "[$(date +%T)] Killing processes on port $port: $pids"
    xargs -r -n1 kill <<<"$pids" 2>/dev/null || true
    sleep 1
    # If they survived, KILL
    pids="$(lsof -ti :"$port" 2>/dev/null || true)"
    if [[ -n "$pids" ]]; then
      xargs -r -n1 kill -9 <<<"$pids" 2>/dev/null || true
    fi
  done
}

# --------- Stop components (PID files first) ----------
stop_by_pidfile "API Server"     "$PID_DIR/api.pid"          || true
stop_by_pidfile "Webserver"      "$PID_DIR/webserver.pid"    || true
stop_by_pidfile "Scheduler"      "$PID_DIR/scheduler.pid"    || true
stop_by_pidfile "DAG Processor"  "$PID_DIR/dag_processor.pid"|| true
stop_by_pidfile "Triggerer"      "$PID_DIR/triggerer.pid"    || true

# --------- Fallback: pattern kill ----------
# API server variants (gunicorn/uvicorn)
stop_by_pattern "API Server" "airflow api" || true
stop_by_pattern "API Server (gunicorn)" "gunicorn.*airflow.*api" || true
stop_by_pattern "API Server (uvicorn)"  "uvicorn.*airflow.*api"  || true

# Webserver (legacy)
stop_by_pattern "Webserver" "airflow webserver" || true
stop_by_pattern "Webserver (gunicorn)" "gunicorn.*airflow.*webserver" || true

# Core daemons
stop_by_pattern "Scheduler" "airflow scheduler" || true
stop_by_pattern "DAG Processor" "airflow dag-processor|airflow dag_processor" || true
stop_by_pattern "Triggerer" "airflow triggerer" || true

# --------- Free ports (last resort) ----------
kill_ports

echo "[$(date +%F' '%T)] All components stopped."
echo " see you soon!"
