#!/usr/bin/env bash
set -Eeuo pipefail

# --- Settings (adjust if needed) ---
AIRFLOW_HOME="${AIRFLOW_HOME:-/home/tahmast/airflow}"
VENV_DIR="${VENV_DIR:-/home/tahmast/myenv}"
WAIT="${WAIT:-40}"   # seconds to wait before SIGKILL

# Optional: activate venv to ensure `airflow` CLI is on PATH
[[ -d "$VENV_DIR" ]] && source "$VENV_DIR/bin/activate" || true

log(){ echo "[$(date +'%F %T')] $*"; }

# Gracefully stop a list of PIDs (TERM, wait, then KILL if needed)
kill_gracefully(){ # name wait pids...
  local name="$1" wait="$2"; shift 2; local pids=("$@")
  [[ ${#pids[@]} -eq 0 ]] && return 0
  log "Stopping $name: ${pids[*]}"
  kill -TERM "${pids[@]}" 2>/dev/null || true
  local deadline=$(( $(date +%s) + wait ))
  while :; do
    local alive=0
    for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && alive=1 && break; done
    [[ $alive -eq 0 ]] && break
    if [[ $(date +%s) -ge $deadline ]]; then
      log "$name did not exit -> SIGKILL"
      kill -KILL "${pids[@]}" 2>/dev/null || true
      break
    fi
    sleep 1
  done
}

# Stop one Airflow component by PID file or by grep pattern
stop(){ # name pidfile pattern
  local name="$1" pidfile="$2" pattern="$3" pids=()
  # Try PID file first
  if [[ -f "$AIRFLOW_HOME/$pidfile" ]]; then
    pid="$(cat "$AIRFLOW_HOME/$pidfile" 2>/dev/null || true)"
    [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null && pids+=("$pid")
  fi
  # Fallback: grep for process
  [[ ${#pids[@]} -eq 0 ]] && mapfile -t pids < <(pgrep -f "$pattern" || true)
  # Stop and cleanup
  if [[ ${#pids[@]} -eq 0 ]]; then
    log "$name: not running"
  else
    kill_gracefully "$name" "$WAIT" "${pids[@]}"
  fi
  rm -f "$AIRFLOW_HOME/$pidfile" || true
}

# --- Airflow 3 components (adjust pidfile names to match your start.sh) ---
stop "API Server"      "airflow-api.pid"           '(^| )airflow api( |$)|uvicorn.*airflow\.api_fastapi'
stop "Scheduler"       "airflow-scheduler.pid"     '(^| )airflow scheduler( |$)'
stop "DAG Processor"   "airflow-dag-processor.pid" '(^| )airflow dag-processor( |$)|dag_processor'
stop "Triggerer"       "airflow-triggerer.pid"     '(^| )airflow triggerer( |$)'

# Optional: also kill running task runners (set KILL_TASK_RUNNERS=1 to enable)
[[ "${KILL_TASK_RUNNERS:-0}" == "1" ]] && stop "Task Runners" "airflow-task-runner.pid" 'airflow task( |$)|execution_time\.task_runner'

log "All components stopped."

echo " see you soon! "
