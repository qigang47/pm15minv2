#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export PROGRAM_PATH="$ROOT_DIR/auto_research/program_direction_dense.md"
export SESSION_DIR="$ROOT_DIR/sessions/deep_otm_baseline_direction_dense_autoresearch"
export AUTORUN_DIR="$ROOT_DIR/var/research/autorun/direction_dense"

exec "$ROOT_DIR/auto_research/codex_background_loop.sh" "$@"
