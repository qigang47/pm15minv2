#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DENSE_AUTORUN_DIRS=(
  "$ROOT_DIR/var/research/autorun/direction_dense_sol_xrp"
  "$ROOT_DIR/var/research/autorun/reversal_dense_sol_xrp"
)

for autorun_dir in "${DENSE_AUTORUN_DIRS[@]}"; do
  name="$(basename "$autorun_dir")"
  echo "=== ${name} ==="
  AUTORUN_DIR="$autorun_dir" \
    "$ROOT_DIR/auto_research/status_autorun.sh" || true
  echo
done
