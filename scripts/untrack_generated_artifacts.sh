#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
cd "$ROOT_DIR"

echo "Staging .gitignore ..."
git add .gitignore

echo "Removing generated artifacts from the Git index (files stay on disk) ..."
git rm -r --cached --ignore-unmatch \
  data \
  var \
  research/feature_frames \
  research/label_frames \
  research/training_sets \
  research/training_runs \
  research/model_bundles \
  research/backtests \
  research/evaluations \
  research/experiments/runs \
  research/active_bundles

echo
echo "Done. Review the staged changes with:"
echo "  git status --short"
