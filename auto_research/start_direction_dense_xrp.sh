#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export PROGRAM_PATH="${PROGRAM_PATH:-$ROOT_DIR/auto_research/program_direction_dense_xrp.md}"
export SESSION_DIR="${SESSION_DIR:-$ROOT_DIR/sessions/deep_otm_baseline_direction_dense_xrp_autoresearch}"
export PM15MIN_ALLOWED_QUEUE_MARKETS="${PM15MIN_ALLOWED_QUEUE_MARKETS:-xrp}"

exec "$ROOT_DIR/auto_research/start_direction_dense.sh" "$@"
