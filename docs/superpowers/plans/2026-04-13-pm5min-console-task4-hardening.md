# PM5Min Console Task 4 Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the pm5min console Task 4 split by locking runtime/task read routes to local service code and extracting runtime/task payload shaping out of `service.py`.

**Architecture:** Keep `handlers.py` and `compat.py` boundaries unchanged. Add one package-local runtime/task view module for payload shaping, then make `service.py` a thinner aggregator that forwards to it.

**Tech Stack:** Python 3.11+, `pytest`, package-local console modules under `src/pm5min/console`

---

### Task 1: Lock Runtime And Task Read Routes

**Files:**
- Modify: `tests/test_pm5min_cli.py`

- [ ] **Step 1: Write the failing tests**

Add one test that verifies `show-runtime-state`, `show-runtime-history`, `list-tasks`, and `show-task` route through `pm5min.console.handlers` local service imports rather than compat imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k 'runtime_and_task_read_commands_stay_local'`
Expected: FAIL because the new test does not exist yet.

- [ ] **Step 3: Keep the test failing for the right reason**

Run the same command after adding the test.
Expected: FAIL until implementation is updated to expose the runtime/task view helpers through the intended local layer.

### Task 2: Extract Runtime/Task Payload Shaping

**Files:**
- Create: `src/pm5min/console/runtime_views.py`
- Modify: `src/pm5min/console/service.py`

- [ ] **Step 1: Move runtime/task payload shaping into the new module**

Extract the task/runtime display helpers from `service.py` into `runtime_views.py`, keeping public helpers for:

- runtime state payload
- runtime history payload
- task list payload
- task detail payload

- [ ] **Step 2: Rewire service.py to stay thin**

Make `service.py` call the new module for runtime/task read shaping while preserving the existing return shape.

- [ ] **Step 3: Run focused tests**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k 'runtime_and_task_read_commands_stay_local or console_read_commands_stay_local or show_actions_and_build_action_stay_local or execute_and_serve_keep_explicit_compat_paths'`
Expected: PASS

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_console_tasks.py`
Expected: PASS

### Task 3: Verify No Console Regressions

**Files:**
- No new files

- [ ] **Step 1: Run broader console checks**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_console_read_models.py tests/test_pm5min_console_tasks.py`
Expected: PASS

Run: `PYTHONPATH=src pytest -q tests/test_console_cli.py -k 'show_home_and_serve or show_training_run_and_bundle'`
Expected: PASS

- [ ] **Step 2: Run architecture guards**

Run: `PYTHONPATH=src pytest -q tests/test_pmshared_architecture.py tests/test_architecture_guards.py`
Expected: PASS or existing skips only.
