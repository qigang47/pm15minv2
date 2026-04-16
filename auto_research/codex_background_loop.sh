#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun}"
STATUS_PATH="${STATUS_PATH:-$AUTORUN_DIR/codex-background.status.json}"
PID_PATH="${PID_PATH:-$AUTORUN_DIR/codex-background.pid}"
LOG_PATH="${LOG_PATH:-$AUTORUN_DIR/codex-background.log}"
STOP_FLAG="${STOP_FLAG:-$AUTORUN_DIR/stop.flag}"
LAST_PROMPT_PATH="${LAST_PROMPT_PATH:-$AUTORUN_DIR/codex-last-prompt.md}"
LAST_OUTPUT_PATH="${LAST_OUTPUT_PATH:-$AUTORUN_DIR/codex-last-output.txt}"
FALLBACK_ENV_PATH="${FALLBACK_ENV_PATH:-$AUTORUN_DIR/codex-fallback.env}"
QUEUE_SUPERVISOR_SCRIPT="$ROOT_DIR/auto_research/experiment_queue_supervisor.sh"

SESSION_DIR="${SESSION_DIR:-}"
PROGRAM_PATH="${PROGRAM_PATH:-$ROOT_DIR/auto_research/program.md}"
LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-300}"
CODEX_MODEL="${CODEX_MODEL:-}"
CODEX_EXTRA_ARGS="${CODEX_EXTRA_ARGS:-}"
CODEX_SANDBOX_MODE="${CODEX_SANDBOX_MODE:-danger-full-access}"
CODEX_HOME_MODE="${CODEX_HOME_MODE:-isolated}"
CODEX_HOME_DIR="${CODEX_HOME_DIR:-$AUTORUN_DIR/codex-home}"
CODEX_SECONDARY_HOME_DIR="${CODEX_SECONDARY_HOME_DIR:-$AUTORUN_DIR/codex-home-secondary}"
CODEX_FALLBACK_HOME_DIR="${CODEX_FALLBACK_HOME_DIR:-$AUTORUN_DIR/codex-home-fallback}"
CODEX_OFFICIAL_HOME_DIR="${CODEX_OFFICIAL_HOME_DIR:-$AUTORUN_DIR/codex-home-official}"
CODEX_NETWORK_PROXY_MODE="${CODEX_NETWORK_PROXY_MODE:-direct}"
MAX_CONSECUTIVE_FAILURES="${MAX_CONSECUTIVE_FAILURES:-3}"
CODEX_ATTEMPT_TIMEOUT_SEC="${CODEX_ATTEMPT_TIMEOUT_SEC:-7200}"
CODEX_STARTUP_TIMEOUT_SEC="${CODEX_STARTUP_TIMEOUT_SEC:-90}"
CODEX_PROVIDER_FAILURE_ABORT_AFTER_RECONNECTS="${CODEX_PROVIDER_FAILURE_ABORT_AFTER_RECONNECTS:-3}"
REAL_HOME="$HOME"
FAILURE_COUNT=0
RUN_ONCE_SHOULD_STOP=0
STARTED_ATTEMPT_PID=""
declare -a CODEX_CMD=()

mkdir -p "$AUTORUN_DIR"

if [[ -f "$FALLBACK_ENV_PATH" ]]; then
  # shellcheck disable=SC1090
  source "$FALLBACK_ENV_PATH"
fi

resolve_session_dir() {
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$ROOT_DIR" "${SESSION_DIR:-}" "$PROGRAM_PATH"
from pathlib import Path
import sys

from pm15min.research.automation import resolve_autorun_session_dir

explicit = sys.argv[2] or None
print(
    resolve_autorun_session_dir(
        Path(sys.argv[1]),
        explicit_session_dir=explicit,
        program_path=Path(sys.argv[3]),
    )
)
PY
}

SESSION_DIR="$(resolve_session_dir)"

CODEX_SECONDARY_BASE_URL="${CODEX_SECONDARY_BASE_URL:-}"
CODEX_SECONDARY_API_KEY="${CODEX_SECONDARY_API_KEY:-}"
CODEX_FALLBACK_BASE_URL="${CODEX_FALLBACK_BASE_URL:-}"
CODEX_FALLBACK_API_KEY="${CODEX_FALLBACK_API_KEY:-}"
CODEX_OFFICIAL_AUTH_PATH="${CODEX_OFFICIAL_AUTH_PATH:-$AUTORUN_DIR/codex-official-auth.json}"

write_status() {
  python3 - <<'PY' "$STATUS_PATH" "$PID_PATH" "$SESSION_DIR" "$LAST_PROMPT_PATH" "$LAST_OUTPUT_PATH" "$1" "$2" "$3" "$4" "$5" "$6" "$7"
from __future__ import annotations
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

status_path = Path(sys.argv[1])
pid_path = Path(sys.argv[2])
session_dir = sys.argv[3]
last_prompt_path = sys.argv[4]
last_output_path = sys.argv[5]
state = sys.argv[6]
iteration = int(sys.argv[7])
last_exit_code = None if sys.argv[8] == "None" else int(sys.argv[8])
last_started_at = None if sys.argv[9] == "None" else sys.argv[9]
last_finished_at = None if sys.argv[10] == "None" else sys.argv[10]
started_at = None if sys.argv[11] == "None" else sys.argv[11]
failure_count = int(sys.argv[12])

payload = {
    "state": state,
    "pid": int(pid_path.read_text(encoding="utf-8").strip()) if pid_path.exists() else None,
    "session_dir": session_dir,
    "iteration": iteration,
    "started_at": started_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "last_started_at": last_started_at,
    "last_finished_at": last_finished_at,
    "last_exit_code": last_exit_code,
    "failure_count": failure_count,
    "last_prompt_path": last_prompt_path,
    "last_output_path": last_output_path,
}
status_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
PY
}

prepare_isolated_codex_home() {
  if [[ "$CODEX_HOME_MODE" != "isolated" ]]; then
    return 0
  fi
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$CODEX_HOME_DIR" "$REAL_HOME"
from pathlib import Path
import sys
from pm15min.research.automation import prepare_codex_home

prepare_codex_home(Path(sys.argv[1]), source_home=Path(sys.argv[2]))
PY
}

prepare_secondary_codex_home() {
  if [[ -z "$CODEX_SECONDARY_BASE_URL" || -z "$CODEX_SECONDARY_API_KEY" ]]; then
    return 1
  fi
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$CODEX_SECONDARY_HOME_DIR" "$REAL_HOME" "$CODEX_SECONDARY_BASE_URL" "$CODEX_SECONDARY_API_KEY"
from pathlib import Path
import sys
from pm15min.research.automation import apply_codex_provider_override, prepare_codex_home

prepare_codex_home(Path(sys.argv[1]), source_home=Path(sys.argv[2]))
apply_codex_provider_override(
    Path(sys.argv[1]),
    base_url=sys.argv[3],
    api_key=sys.argv[4],
)
PY
}

prepare_fallback_codex_home() {
  if [[ -z "$CODEX_FALLBACK_BASE_URL" || -z "$CODEX_FALLBACK_API_KEY" ]]; then
    return 1
  fi
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$CODEX_FALLBACK_HOME_DIR" "$REAL_HOME" "$CODEX_FALLBACK_BASE_URL" "$CODEX_FALLBACK_API_KEY"
from pathlib import Path
import sys
from pm15min.research.automation import apply_codex_provider_override, prepare_codex_home

prepare_codex_home(Path(sys.argv[1]), source_home=Path(sys.argv[2]))
apply_codex_provider_override(
    Path(sys.argv[1]),
    base_url=sys.argv[3],
    api_key=sys.argv[4],
)
PY
}

secondary_configured() {
  [[ -n "$CODEX_SECONDARY_BASE_URL" && -n "$CODEX_SECONDARY_API_KEY" ]]
}

fallback_configured() {
  [[ -n "$CODEX_FALLBACK_BASE_URL" && -n "$CODEX_FALLBACK_API_KEY" ]]
}

official_fallback_configured() {
  [[ -f "$CODEX_OFFICIAL_AUTH_PATH" ]]
}

list_live_autorun_pids() {
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$ROOT_DIR" "$LAST_OUTPUT_PATH"
from pathlib import Path
import sys
from pm15min.research.automation import find_live_autorun_processes

for item in find_live_autorun_processes(
    Path(sys.argv[1]),
    output_path=Path(sys.argv[2]),
):
    print(int(item["pid"]))
PY
}

terminate_live_autorun_processes() {
  local -a discovered_pids=()
  mapfile -t discovered_pids < <(list_live_autorun_pids)
  local -a target_pids=()
  local pid
  for pid in "${discovered_pids[@]}"; do
    [[ -n "$pid" ]] || continue
    if [[ "$pid" == "$$" ]]; then
      continue
    fi
    target_pids+=("$pid")
  done
  if [[ "${#target_pids[@]}" -eq 0 ]]; then
    return 0
  fi
  for pid in "${target_pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  sleep 1
  for pid in "${target_pids[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
done
}

list_child_pids() {
  local parent_pid="$1"
  ps -o pid= --ppid "$parent_pid" 2>/dev/null | awk '{print $1}'
}

terminate_current_instance_processes() {
  local parent_pid="${1:-}"
  [[ -n "$parent_pid" ]] || return 0
  if ! kill -0 "$parent_pid" >/dev/null 2>&1; then
    return 0
  fi

  local -a pending=("$parent_pid")
  local -a target_pids=()
  local current_pid child_pid
  while [[ "${#pending[@]}" -gt 0 ]]; do
    current_pid="${pending[0]}"
    pending=("${pending[@]:1}")
    target_pids+=("$current_pid")
    while read -r child_pid; do
      [[ -n "$child_pid" ]] || continue
      pending+=("$child_pid")
    done < <(list_child_pids "$current_pid")
  done

  local pid
  for pid in "${target_pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  sleep 1
  for pid in "${target_pids[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  done
}

prepare_official_codex_home() {
  if [[ ! -f "$CODEX_OFFICIAL_AUTH_PATH" ]]; then
    return 1
  fi
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$CODEX_OFFICIAL_HOME_DIR" "$REAL_HOME" "$CODEX_OFFICIAL_AUTH_PATH"
from pathlib import Path
import json
import sys
from pm15min.research.automation import apply_codex_auth_override, prepare_codex_home

prepare_codex_home(Path(sys.argv[1]), source_home=Path(sys.argv[2]))
payload = json.loads(Path(sys.argv[3]).read_text(encoding="utf-8"))
apply_codex_auth_override(
    Path(sys.argv[1]),
    auth_payload=payload,
)
PY
}

codex_output_is_provider_failure() {
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$1" "${2:-}"
from pathlib import Path
import sys
from pm15min.research.automation import is_transient_codex_provider_failure

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="ignore")
raise SystemExit(0 if is_transient_codex_provider_failure(text, base_url=sys.argv[2] or None) else 1)
PY
}

provider_failure_retry_exhausted() {
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$1" "${2:-}" "$CODEX_PROVIDER_FAILURE_ABORT_AFTER_RECONNECTS"
from pathlib import Path
import re
import sys
from pm15min.research.automation import is_transient_codex_provider_failure

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="ignore")
matches = [int(item) for item in re.findall(r"reconnecting\.\.\.\s+(\d+)/5", text, flags=re.IGNORECASE)]
threshold = max(1, int(sys.argv[3]))
limit_reached = bool(matches) and max(matches) >= threshold
raise SystemExit(0 if limit_reached and is_transient_codex_provider_failure(text, base_url=sys.argv[2] or None) else 1)
PY
}

compute_failure_state() {
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$1" "$2" "$3"
import sys
from pm15min.research.automation import next_autorun_failure_state

payload = next_autorun_failure_state(
    previous_failures=int(sys.argv[1]),
    exit_code=int(sys.argv[2]),
    max_consecutive_failures=int(sys.argv[3]),
)
print(f"{payload['failure_count']}\t{1 if payload['should_stop'] else 0}")
PY
}

build_prompt() {
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$ROOT_DIR" "$SESSION_DIR" "$PROGRAM_PATH" "$STATUS_PATH"
from pathlib import Path
import sys
from pm15min.research.automation import build_codex_cycle_prompt

print(
    build_codex_cycle_prompt(
        project_root=Path(sys.argv[1]),
        session_dir=Path(sys.argv[2]),
        program_path=Path(sys.argv[3]),
        status_path=Path(sys.argv[4]),
    )
)
PY
}

build_codex_command() {
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$ROOT_DIR" "$LAST_OUTPUT_PATH" "$CODEX_SANDBOX_MODE" "$CODEX_MODEL" "${CODEX_EXTRA_ARGS:-}"
from pathlib import Path
import sys
from pm15min.research.automation import build_codex_exec_command

for item in build_codex_exec_command(
    project_root=Path(sys.argv[1]),
    output_path=Path(sys.argv[2]),
    sandbox_mode=sys.argv[3],
    model=sys.argv[4] or None,
    extra_args=sys.argv[5] if len(sys.argv) > 5 else None,
):
    print(item)
PY
}

resolve_codex_path_prefix() {
  PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$ROOT_DIR"
from pathlib import Path
import sys
from pm15min.research.automation import resolve_codex_exec_path_prefix

print(resolve_codex_exec_path_prefix(Path(sys.argv[1])) or "")
PY
}

CODEX_PATH_PREFIX="${CODEX_PATH_PREFIX:-}"
if [[ -z "$CODEX_PATH_PREFIX" ]]; then
  CODEX_PATH_PREFIX="$(resolve_codex_path_prefix)"
fi

run_codex_command() {
  local home_root="$1"
  local output_log="$2"

  local -a env_prefix=(env)
  if [[ "${CODEX_NETWORK_PROXY_MODE:-direct}" == "direct" ]]; then
    env_prefix+=(-u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u NO_PROXY -u no_proxy -u http_proxy -u https_proxy -u all_proxy)
  fi
  if [[ -n "$home_root" ]]; then
    env_prefix+=("HOME=$home_root")
  fi
  if [[ -n "${CODEX_PATH_PREFIX:-}" ]]; then
    env_prefix+=("PATH=$CODEX_PATH_PREFIX:$PATH")
  fi
  exec "${env_prefix[@]}" "${CODEX_CMD[@]}" < "$LAST_PROMPT_PATH" > "$output_log" 2>&1
}

start_codex_attempt_process() {
  local home_root="$1"
  local output_log="$2"

  local -a env_prefix=(env)
  if [[ "${CODEX_NETWORK_PROXY_MODE:-direct}" == "direct" ]]; then
    env_prefix+=(-u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u NO_PROXY -u no_proxy -u http_proxy -u https_proxy -u all_proxy)
  fi
  if [[ -n "$home_root" ]]; then
    env_prefix+=("HOME=$home_root")
  fi
  if [[ -n "${CODEX_PATH_PREFIX:-}" ]]; then
    env_prefix+=("PATH=$CODEX_PATH_PREFIX:$PATH")
  fi
  setsid "${env_prefix[@]}" "${CODEX_CMD[@]}" < "$LAST_PROMPT_PATH" > "$output_log" 2>&1 &
  STARTED_ATTEMPT_PID="$!"
}

terminate_attempt_process_group() {
  local attempt_pid="$1"
  [[ -n "$attempt_pid" ]] || return 0
  kill -TERM -- -"$attempt_pid" >/dev/null 2>&1 || kill "$attempt_pid" >/dev/null 2>&1 || true
  sleep 1
  kill -9 -- -"$attempt_pid" >/dev/null 2>&1 || kill -9 "$attempt_pid" >/dev/null 2>&1 || true
  wait "$attempt_pid" >/dev/null 2>&1 || true
}

run_codex_attempt() {
  local home_root="$1"
  local output_log="$2"
  local timeout_sec="${CODEX_ATTEMPT_TIMEOUT_SEC:-0}"
  local startup_timeout_sec="${CODEX_STARTUP_TIMEOUT_SEC:-0}"
  local start_epoch
  start_epoch="$(date +%s)"
  local attempt_pid
  local startup_baseline_size=""
  local startup_progress=0

  STARTED_ATTEMPT_PID=""
  start_codex_attempt_process "$home_root" "$output_log"
  attempt_pid="$STARTED_ATTEMPT_PID"
  if [[ -z "$attempt_pid" ]]; then
    printf '[codex_background_loop] codex attempt failed to start\n' >> "$output_log"
    return 127
  fi

  while kill -0 "$attempt_pid" >/dev/null 2>&1; do
    local now_epoch
    now_epoch="$(date +%s)"
    local elapsed_sec=$((now_epoch - start_epoch))
    local current_size=0
    if [[ -f "$output_log" ]]; then
      current_size="$(wc -c < "$output_log" | tr -d ' ')"
    fi

    # Heavy first-pass prompts can think for a long time after printing the
    # banner and prompt header; any output at all means the attempt started.
    if [[ "$current_size" -gt 0 ]]; then
      startup_progress=1
    elif [[ -z "$startup_baseline_size" && "$elapsed_sec" -ge 5 ]]; then
      startup_baseline_size="$current_size"
    fi

    if provider_failure_retry_exhausted "$output_log"; then
      printf '[codex_background_loop] codex attempt aborted after %s reconnects\n' "$CODEX_PROVIDER_FAILURE_ABORT_AFTER_RECONNECTS" >> "$output_log"
      terminate_attempt_process_group "$attempt_pid"
      return 75
    fi
    if [[ "$startup_progress" -eq 0 && "$startup_timeout_sec" -gt 0 && "$elapsed_sec" -ge "$startup_timeout_sec" ]]; then
      printf '[codex_background_loop] codex attempt stalled during startup after %ss\n' "$startup_timeout_sec" >> "$output_log"
      terminate_attempt_process_group "$attempt_pid"
      return 74
    fi
    if [[ "$timeout_sec" -gt 0 ]]; then
      if (( now_epoch - start_epoch >= timeout_sec )); then
        printf '[codex_background_loop] codex attempt timed out after %ss\n' "$timeout_sec" >> "$output_log"
        terminate_attempt_process_group "$attempt_pid"
        return 124
      fi
    fi
    sleep 3
  done

  set +e
  wait "$attempt_pid"
  local attempt_exit_code="$?"
  set -e
  return "$attempt_exit_code"
}

run_once() {
  local iteration="$1"
  local started_at
  started_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  build_prompt > "$LAST_PROMPT_PATH"
  RUN_ONCE_SHOULD_STOP=0
  write_status "running" "$iteration" "None" "$started_at" "None" "${RUN_STARTED_AT:-None}" "$FAILURE_COUNT"

  mapfile -t CODEX_CMD < <(build_codex_command)

  local primary_attempt_log="$AUTORUN_DIR/codex-last-primary-attempt.log"
  local secondary_attempt_log="$AUTORUN_DIR/codex-last-secondary-attempt.log"
  local fallback_attempt_log="$AUTORUN_DIR/codex-last-fallback-attempt.log"
  local official_attempt_log="$AUTORUN_DIR/codex-last-official-attempt.log"
  : > "$primary_attempt_log"
  rm -f "$secondary_attempt_log"
  rm -f "$fallback_attempt_log"
  rm -f "$official_attempt_log"
  local last_attempt_log="$primary_attempt_log"

  set +e
  if [[ "$CODEX_HOME_MODE" == "isolated" ]]; then
    prepare_isolated_codex_home
    run_codex_attempt "$CODEX_HOME_DIR" "$primary_attempt_log"
  else
    run_codex_attempt "" "$primary_attempt_log"
  fi
  local exit_code="$?"
  set -e
  cat "$primary_attempt_log" >> "$LOG_PATH"

  if [[ "$exit_code" -ne 0 ]] && secondary_configured && { [[ "$exit_code" -eq 74 ]] || codex_output_is_provider_failure "$primary_attempt_log" "$CODEX_SECONDARY_BASE_URL"; }; then
    echo "[codex_background_loop] iteration=$iteration retrying with secondary fallback provider" >> "$LOG_PATH"
    : > "$secondary_attempt_log"
    set +e
    prepare_secondary_codex_home
    local prepare_secondary_exit_code="$?"
    if [[ "$prepare_secondary_exit_code" -eq 0 ]]; then
      run_codex_attempt "$CODEX_SECONDARY_HOME_DIR" "$secondary_attempt_log"
      exit_code="$?"
    else
      printf '[codex_background_loop] secondary fallback home preparation failed with exit=%s\n' "$prepare_secondary_exit_code" > "$secondary_attempt_log"
      exit_code="$prepare_secondary_exit_code"
    fi
    set -e
    cat "$secondary_attempt_log" >> "$LOG_PATH"
    last_attempt_log="$secondary_attempt_log"
  fi

  if [[ "$exit_code" -ne 0 ]] && fallback_configured && { [[ "$exit_code" -eq 74 ]] || codex_output_is_provider_failure "$last_attempt_log" "$CODEX_FALLBACK_BASE_URL"; }; then
    echo "[codex_background_loop] iteration=$iteration retrying with fallback provider" >> "$LOG_PATH"
    : > "$fallback_attempt_log"
    set +e
    prepare_fallback_codex_home
    local prepare_exit_code="$?"
    if [[ "$prepare_exit_code" -eq 0 ]]; then
      run_codex_attempt "$CODEX_FALLBACK_HOME_DIR" "$fallback_attempt_log"
      exit_code="$?"
    else
      printf '[codex_background_loop] fallback home preparation failed with exit=%s\n' "$prepare_exit_code" > "$fallback_attempt_log"
      exit_code="$prepare_exit_code"
    fi
    set -e
    cat "$fallback_attempt_log" >> "$LOG_PATH"
    last_attempt_log="$fallback_attempt_log"
  fi

  if [[ "$exit_code" -ne 0 ]] && official_fallback_configured && { [[ "$exit_code" -eq 74 ]] || codex_output_is_provider_failure "$last_attempt_log"; }; then
    echo "[codex_background_loop] iteration=$iteration retrying with official auth fallback" >> "$LOG_PATH"
    : > "$official_attempt_log"
    set +e
    prepare_official_codex_home
    local prepare_official_exit_code="$?"
    if [[ "$prepare_official_exit_code" -eq 0 ]]; then
      run_codex_attempt "$CODEX_OFFICIAL_HOME_DIR" "$official_attempt_log"
      exit_code="$?"
    else
      printf '[codex_background_loop] official fallback preparation failed with exit=%s\n' "$prepare_official_exit_code" > "$official_attempt_log"
      exit_code="$prepare_official_exit_code"
    fi
    set -e
    cat "$official_attempt_log" >> "$LOG_PATH"
  fi

  local finished_at
  finished_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  local failure_state
  failure_state="$(compute_failure_state "$FAILURE_COUNT" "$exit_code" "$MAX_CONSECUTIVE_FAILURES")"
  FAILURE_COUNT="$(printf '%s' "$failure_state" | cut -f1)"
  RUN_ONCE_SHOULD_STOP="$(printf '%s' "$failure_state" | cut -f2)"
  if [[ "$RUN_ONCE_SHOULD_STOP" == "1" ]]; then
    write_status "failed" "$iteration" "$exit_code" "$started_at" "$finished_at" "${RUN_STARTED_AT:-None}" "$FAILURE_COUNT"
  else
    write_status "idle" "$iteration" "$exit_code" "$started_at" "$finished_at" "${RUN_STARTED_AT:-None}" "$FAILURE_COUNT"
  fi
  return "$exit_code"
}

loop_body() {
  echo "$$" > "$PID_PATH"
  RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  local iteration=0
  FAILURE_COUNT=0
  write_status "idle" "$iteration" "None" "None" "None" "$RUN_STARTED_AT" "$FAILURE_COUNT"
  while true; do
    if [[ -f "$STOP_FLAG" ]]; then
      rm -f "$STOP_FLAG"
      write_status "stopped" "$iteration" "None" "None" "None" "$RUN_STARTED_AT" "$FAILURE_COUNT"
      rm -f "$PID_PATH"
      exit 0
    fi
    iteration=$((iteration + 1))
    if ! run_once "$iteration"; then
      echo "[codex_background_loop] iteration=$iteration failed" >> "$LOG_PATH"
      if [[ "$RUN_ONCE_SHOULD_STOP" == "1" ]]; then
        echo "[codex_background_loop] stopping after $FAILURE_COUNT consecutive failures" >> "$LOG_PATH"
        rm -f "$PID_PATH"
        exit 1
      fi
    fi
    sleep "$LOOP_SLEEP_SEC"
  done
}

ACTION="${1:-start}"

if [[ "$CODEX_HOME_MODE" != "isolated" && "$CODEX_HOME_MODE" != "inherit" ]]; then
  echo "CODEX_HOME_MODE must be 'isolated' or 'inherit'" >&2
  exit 2
fi

case "$ACTION" in
  start)
    if [[ -f "$PID_PATH" ]]; then
      existing_pid="$(cat "$PID_PATH")"
      if kill -0 "$existing_pid" >/dev/null 2>&1; then
        echo "Background loop already running with pid=$existing_pid"
        exit 0
      fi
      rm -f "$PID_PATH"
    fi
    rm -f "$STOP_FLAG"
    if [[ -x "$QUEUE_SUPERVISOR_SCRIPT" ]]; then
      "$QUEUE_SUPERVISOR_SCRIPT" start || true
    fi
    nohup "$SCRIPT_PATH" __run_loop >> "$LOG_PATH" 2>&1 &
    echo "Started background loop"
    ;;
  once)
    run_once 1
    ;;
  stop)
    touch "$STOP_FLAG"
    if [[ -f "$PID_PATH" ]]; then
      pid="$(cat "$PID_PATH")"
      terminate_current_instance_processes "$pid"
    fi
    rm -f "$PID_PATH"
    echo "Stop requested"
    ;;
  restart)
    "$SCRIPT_PATH" stop || true
    sleep 1
    "$SCRIPT_PATH" start
    ;;
  __run_loop)
    loop_body
    ;;
  *)
    echo "usage: codex_background_loop.sh {start|stop|restart|once}" >&2
    exit 2
    ;;
esac
