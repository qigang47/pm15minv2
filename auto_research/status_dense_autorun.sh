#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

for name in direction_dense reversal_dense; do
  echo "=== ${name} ==="
  AUTORUN_DIR="$ROOT_DIR/var/research/autorun/${name}" \
    "$ROOT_DIR/auto_research/status_autorun.sh" || true
  echo
done
