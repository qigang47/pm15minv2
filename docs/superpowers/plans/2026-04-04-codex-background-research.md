# Codex Background Research Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a repo-local background research control plane where external `codex` decides the next experiment while the repository owns execution, results, state, and recovery.

**Architecture:** Add a small automation surface under `scripts/research/` plus a `program.md` instruction file and operator-visible runtime state under `var/research/autorun/`. Keep the existing `sessions/deep_otm_baseline_factor_weight_search_noret30/` session as the canonical research log instead of creating a second bookkeeping system.

**Tech Stack:** Bash, Python 3.11+, existing `pm15min` research CLI, repo-local markdown/JSON/TSV state files, external `codex exec`.

---

### Task 1: Research Prompt And Operator Docs

**Files:**
- Create: `program.md`
- Modify: `docs/README.md` (if needed for discovery)

- [ ] Write `program.md` so Codex has a single repo-local instruction source for the current deep OTM retrain loop.
- [ ] Include the current priorities: BTC `direction` recovery with offset-specific reversal weighting, BTC reversal feature-width comparison, and continued monitoring of ETH/SOL/XRP latest-window baselines.
- [ ] Document the required outputs Codex must update each cycle: session notes, results row, cycle summary, and any newly created suite spec.

### Task 2: One-Shot Experiment Runner

**Files:**
- Create: `scripts/research/run_one_experiment.sh`
- Reuse: `scripts/entrypoints/_python_env.sh`

- [ ] Write a thin shell wrapper around `python -m pm15min research experiment run-suite`.
- [ ] Support flags for suite name, run label, optional market, optional timeout, and optional log path.
- [ ] Standardize environment activation, `PYTHONPATH=src`, and `MPLCONFIGDIR`.
- [ ] Emit the resolved run directory path on success so later scripts can chain off it.

### Task 3: Experiment Summary Extraction

**Files:**
- Create: `scripts/research/summarize_experiment.py`
- Test: `tests/test_research_experiment_automation.py`

- [ ] Add a failing test that builds a tiny fake experiment run directory and expects a compact JSON summary.
- [ ] Implement summary extraction from experiment `summary.json` plus optional `leaderboard.csv`.
- [ ] Include suite/run identity, case counts, ROI/PnL/trade totals, and artifact paths.

### Task 4: Session Update Helper

**Files:**
- Create: `scripts/research/update_session.py`
- Test: `tests/test_research_experiment_automation.py`

- [ ] Add a failing test that starts from a minimal session tree and expects `results.tsv`, cycle markdown, and `session.md` updates.
- [ ] Implement append/update helpers for the existing session layout under `sessions/deep_otm_baseline_factor_weight_search_noret30/`.
- [ ] Keep this tool dumb: it writes supplied conclusions, it does not decide them.

### Task 5: Background Codex Loop

**Files:**
- Create: `scripts/research/codex_background_loop.sh`
- Create: `scripts/research/status_autorun.sh`

- [ ] Implement `start`, `stop`, `restart`, and `once` behavior for the background wrapper.
- [ ] Persist runtime metadata under `var/research/autorun/`.
- [ ] On each loop iteration, call `codex exec` once with a stable prompt pointing to `program.md` and the active session files.
- [ ] Save the exact prompt and Codex output for audit/debugging.
- [ ] Stop after repeated failures instead of spinning forever.

### Task 6: End-To-End Smoke Validation

**Files:**
- Test: `tests/test_research_experiment_automation.py`

- [ ] Add a lightweight smoke test for the Python helpers.
- [ ] Run the focused automation tests.
- [ ] Run the existing research experiment/training regression subset.

### Task 7: Final Session Integration

**Files:**
- Modify: `sessions/deep_otm_baseline_factor_weight_search_noret30/session.md`
- Modify: `sessions/deep_otm_baseline_factor_weight_search_noret30/results.tsv`
- Create: `sessions/deep_otm_baseline_factor_weight_search_noret30/cycles/007/eval-results.md`

- [ ] Record that the automation control plane was added.
- [ ] Record how operators should start/stop/check the Codex loop.
- [ ] Preserve the existing experimental conclusions and priorities.
