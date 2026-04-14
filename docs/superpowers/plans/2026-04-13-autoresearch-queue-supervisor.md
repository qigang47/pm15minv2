# Autoresearch Queue Supervisor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a durable autoresearch queue and a live supervisor that keeps up to three formal experiments running, queues pre-decided work, and prioritizes failed-run repair ahead of new launches.

**Architecture:** Split decision production from execution. Codex background cycles write queue actions into repo-local state, and a dedicated supervisor reconciles queue state with live formal workers, marking done or repair and filling empty slots up to three live runs.

**Tech Stack:** Python 3.11+, Bash, repo-local JSON state, existing `pm15min` automation helpers, existing experiment wrappers under `scripts/research/`.

---

### Task 1: Add Queue State Tests

**Files:**
- Modify: `tests/test_research_experiment_automation.py`
- Create: `src/pm15min/research/automation/queue_state.py`

- [ ] Step 1: Write failing tests for queue item insert, queued-item replacement for the same market, repair-item protection, and running-item reconciliation.
- [ ] Step 2: Run the focused queue tests and verify they fail because queue helpers do not exist yet.
- [ ] Step 3: Implement the smallest queue helper surface needed for those tests in `queue_state.py`.
- [ ] Step 4: Re-run the same focused tests and verify they pass.

### Task 2: Add Supervisor Selection Tests

**Files:**
- Modify: `tests/test_research_experiment_automation.py`
- Modify: `src/pm15min/research/automation/__init__.py`
- Modify: `src/pm15min/research/automation/queue_state.py`

- [ ] Step 1: Write failing tests for selecting the next launch candidate with priority order `repair > resume > launch` and for enforcing one running item per market.
- [ ] Step 2: Run those tests and verify they fail for the expected missing behaviors.
- [ ] Step 3: Implement minimal candidate-selection helpers.
- [ ] Step 4: Re-run the focused tests and verify they pass.

### Task 3: Add Queue-Supervisor Script Tests

**Files:**
- Modify: `tests/test_research_experiment_automation.py`
- Create: `scripts/research/experiment_queue_supervisor.sh`

- [ ] Step 1: Write failing tests for supervisor-facing helpers that reconcile live workers, mark terminal runs done, mark vanished non-terminal runs repair, and decide whether a slot should launch queued work.
- [ ] Step 2: Run those tests and verify they fail before implementation.
- [ ] Step 3: Implement the smallest Python-backed reconciliation helpers plus a shell supervisor wrapper that supports `start|stop|restart|once|status`.
- [ ] Step 4: Re-run the focused tests and verify they pass.

### Task 4: Change Codex Loop From Direct Launch To Queue Writes

**Files:**
- Modify: `scripts/research/codex_background_loop.sh`
- Modify: `src/pm15min/research/automation/control_plane.py`
- Modify: `tests/test_research_experiment_automation.py`

- [ ] Step 1: Write failing tests for queue-aware prompt text and for any helper that records queue actions instead of requiring direct launch in the decision layer.
- [ ] Step 2: Run those tests and verify they fail for the expected reason.
- [ ] Step 3: Update the prompt and automation helpers so Codex can enqueue actions and so status output includes queue state.
- [ ] Step 4: Re-run the focused tests and verify they pass.

### Task 5: Wire Supervisor Launches Into Existing Experiment Wrapper

**Files:**
- Modify: `scripts/research/run_one_experiment_background.sh`
- Modify: `scripts/research/status_autorun.sh`
- Modify: `tests/test_research_experiment_automation.py`

- [ ] Step 1: Write failing tests for queue item transition from `queued` to `running` and for status output that shows queue state alongside live runs.
- [ ] Step 2: Run those tests and verify they fail before wiring.
- [ ] Step 3: Implement the minimal shell and status changes needed to launch from queue items and expose queue snapshots.
- [ ] Step 4: Re-run the focused tests and verify they pass.

### Task 6: Add Failure-To-Repair Coverage

**Files:**
- Modify: `tests/test_research_experiment_automation.py`
- Modify: `src/pm15min/research/automation/queue_state.py`

- [ ] Step 1: Write failing tests for vanished worker -> `repair`, terminal summary -> `done`, and repeated failed repair -> `dead`.
- [ ] Step 2: Run those tests and verify they fail before the state-machine logic exists.
- [ ] Step 3: Implement the minimal repair/dead transition logic.
- [ ] Step 4: Re-run the focused tests and verify they pass.

### Task 7: Verify End-To-End Automation Surface

**Files:**
- Modify: `scripts/research/README.md`
- Keep: `scripts/research/bootstrap_keepalive.sh`

- [ ] Step 1: Run the full automation test module.
- [ ] Step 2: Run shell syntax checks for the new or modified research scripts.
- [ ] Step 3: Update `scripts/research/README.md` so operators know the new queue supervisor is the active live-fill mechanism and the old bootstrap keepalive is legacy-only.
- [ ] Step 4: Verify the old bootstrap script is no longer referenced as the recommended path.

### Task 8: Server Rollout

**Files:**
- Modify on server: `/home/huatai/qigang/pm15min/v2/var/research/autorun/`
- Modify on server: `/home/huatai/qigang/pm15min/v2/scripts/research/`

- [ ] Step 1: Sync the new queue helpers and supervisor files to the server.
- [ ] Step 2: Start the queue supervisor without deleting the legacy bootstrap script.
- [ ] Step 3: Restart the Codex background loop so it produces queue actions instead of direct fanout.
- [ ] Step 4: Verify that when fewer than three formal runs are alive, the supervisor fills capacity from queue order and preserves repair priority.
