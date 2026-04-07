# Codex Background Research Design

## Goal

Build a repo-local automation control plane for `deep_otm` research where the repository owns experiment execution, result capture, keep/discard bookkeeping, and recovery, while an external `codex` CLI instance acts as the decision-maker.

## Scope

This design covers the minimum background automation layer for this repository:

- a single `program.md` prompt file for Codex
- one-shot experiment execution wrapper
- experiment result summarization
- session/status update helpers
- a background Codex loop wrapper
- a human status command

It does not try to embed agent logic into the repository. The repository remains an experiment control plane only.

## Architecture

### Layer 1: Repo Control Plane

The repository will expose a stable, file-based workflow:

- `program.md`
  - human-authored research objective and current priorities
- `sessions/<name>/...`
  - durable state, cycle history, run labels, conclusions
- `scripts/research/run_one_experiment.sh`
  - canonical one-shot suite executor
- `scripts/research/summarize_experiment.py`
  - canonical run summarizer for agent and operator consumption
- `scripts/research/update_session.py`
  - canonical state/log updater
- `scripts/research/status_autorun.sh`
  - read-only operator view into the background loop

This layer should be deterministic, restartable, and safe to run without any agent attached.

### Layer 2: External Codex Loop

The external agent is `codex exec`. The repo will include a wrapper script that launches Codex in repeated single-cycle runs:

- read `program.md`
- inspect latest session artifacts and summarized results
- decide the next experiment or code change
- execute one cycle only
- exit

The shell wrapper then records heartbeat metadata and starts the next cycle. This keeps recovery simple and avoids long-lived hidden agent state.

## State Model

The background loop needs a small repo-local runtime state in `var/research/autorun/`:

- `codex-background.pid`
- `codex-background.status.json`
- `codex-background.log`
- `codex-last-prompt.md`
- `codex-last-output.txt`
- `stop.flag` for graceful stop

This state is operational only. Canonical research conclusions still live under `sessions/`.

## Session Model

The current research session already uses:

- `sessions/deep_otm_baseline_factor_weight_search_noret30/session.md`
- `sessions/deep_otm_baseline_factor_weight_search_noret30/results.tsv`
- `sessions/deep_otm_baseline_factor_weight_search_noret30/cycles/...`

The new automation layer should default to this session instead of inventing a second parallel bookkeeping system.

## One-Shot Experiment Contract

`run_one_experiment.sh` should:

- load the canonical Python environment
- set `PYTHONPATH=src` and `MPLCONFIGDIR`
- run `python -m pm15min research experiment run-suite --suite ... --run-label ...`
- tee stdout/stderr to a run-scoped log file
- exit non-zero on failure

Inputs:

- suite name
- run label
- optional market override
- optional timeout

Outputs:

- experiment run directory path
- raw log path

## Summarization Contract

`summarize_experiment.py` should read an experiment run directory and emit a compact JSON summary containing:

- suite name
- run label
- completed/failed case counts
- top-level ROI/PnL/trade totals
- per-market breakdown if available
- pointer paths to `summary.json`, `report.md`, `leaderboard.csv`

For the current retrain loop it should also preserve raw summary fields so Codex can reason from them without reparsing markdown.

## Session Update Contract

`update_session.py` should append standardized cycle notes into the existing session structure, without trying to make strategic decisions. It should only:

- add a `results.tsv` row
- write `cycles/<NNN>/eval-results.md`
- update `session.md` sections

It should accept explicit text payloads from the caller rather than inventing conclusions itself.

## Background Loop Contract

`codex_background_loop.sh` should:

- ensure only one loop is active
- record PID and heartbeats
- support `start`, `stop`, `restart`, `once`
- call `codex exec` once per iteration
- pass a fixed prompt template that points Codex to:
  - `program.md`
  - current session files
  - current repo state
- store the exact prompt and full output for audit/debugging

The loop should not decide experiments itself. It only manages Codex process lifecycle.

## Safety Rules

- no destructive cleanup of research artifacts
- no automatic git commit/reset in shell wrappers
- background wrapper should stop on repeated failures instead of thrashing forever
- status output should make “stuck on first case” visible

## Success Criteria

The MVP is successful when:

1. An operator can run one command to start a background Codex loop.
2. Codex has one stable file (`program.md`) to read for instructions.
3. Each loop iteration leaves behind reproducible logs and updated session artifacts.
4. An operator can inspect status and stop the loop without digging through random files.
