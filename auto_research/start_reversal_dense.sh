#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export PROGRAM_PATH="$ROOT_DIR/auto_research/program_reversal_dense.md"
export SESSION_DIR="$ROOT_DIR/sessions/deep_otm_baseline_reversal_dense_autoresearch"
export AUTORUN_DIR="$ROOT_DIR/var/research/autorun/reversal_dense"
export CODEX_OFFICIAL_AUTH_PATH="$ROOT_DIR/var/research/autorun/codex-official-auth.json"
export CODEX_NETWORK_PROXY_MODE="${CODEX_NETWORK_PROXY_MODE:-direct}"
export CODEX_OFFICIAL_NETWORK_PROXY_MODE="${CODEX_OFFICIAL_NETWORK_PROXY_MODE:-inherit}"
export MAX_LIVE_RUNS="${MAX_LIVE_RUNS:-16}"
export MAX_QUEUED_ITEMS="${MAX_QUEUED_ITEMS:-24}"
export TRACK_SLOT_CAPS_JSON="${TRACK_SLOT_CAPS_JSON:-{\"direction_dense\":8,\"reversal_dense\":8}}"
export LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-60}"
export CODEX_ATTEMPT_TIMEOUT_SEC="${CODEX_ATTEMPT_TIMEOUT_SEC:-600}"
export MAX_CONSECUTIVE_FAILURES="${MAX_CONSECUTIVE_FAILURES:-12}"

exec "$ROOT_DIR/auto_research/codex_background_loop.sh" "$@"
