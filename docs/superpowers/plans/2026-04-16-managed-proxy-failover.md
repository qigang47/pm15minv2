# Managed Proxy Failover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a health-aware managed proxy selector for the server's fixed `v2raya-lite` ports, expose the selected proxy through an env file, and let background research / foundation scripts opt into that env without forcing orderbook recording onto the same proxy path.

**Architecture:** Keep the failover logic in a small Python module under `src/pm15min/live/` so it is testable. Provide a thin maintenance script wrapper plus an installer script for a user-level systemd timer. Load the generated env file only from selected shell entrypoints, guarded by an explicit opt-in flag.

**Tech Stack:** Python 3.11+, `requests`, shell entrypoints, user-level systemd timer.

---

### Task 1: Proxy Selection Core

**Files:**
- Create: `src/pm15min/live/proxy_failover.py`
- Test: `tests/test_live_proxy_failover.py`

- [ ] Add failing tests for config parsing, active-port retention, fallback selection, and env rendering.
- [ ] Implement pure helpers for parsing `v2raya-lite` config, evaluating probe results, and rendering an env payload.
- [ ] Run the focused test module and make sure it passes.

### Task 2: Maintenance Wrapper

**Files:**
- Create: `scripts/maintenance/managed_proxy_failover.py`
- Create: `scripts/maintenance/install_managed_proxy_timer.sh`
- Test: `tests/test_live_proxy_failover.py`

- [ ] Add failing tests for wrapper-level state/env writing with a stub probe set.
- [ ] Implement a CLI wrapper that probes configured SOCKS ports, optionally invokes a refresh hook, writes `active_proxy.env`, and persists `state.json`.
- [ ] Add a small installer script that writes a user service/timer pointing at the wrapper.
- [ ] Re-run focused tests.

### Task 3: Shell Integration

**Files:**
- Modify: `scripts/entrypoints/_python_env.sh`
- Modify: `auto_research/run_one_experiment.sh`
- Modify: `scripts/entrypoints/start_v2_live_foundation.sh`
- Test: `tests/test_research_experiment_automation.py`

- [ ] Add failing shell-env coverage for loading a managed proxy env file only when explicitly enabled.
- [ ] Implement a helper in `_python_env.sh` to source the generated env file without overriding explicit manual proxy settings.
- [ ] Call that helper from `run_one_experiment.sh` and `start_v2_live_foundation.sh`.
- [ ] Run the focused shell-env tests.

### Task 4: Runbook

**Files:**
- Modify: `docs/LIVE_OPERATOR_RUNBOOK.md`

- [ ] Add a short operator section covering the managed proxy timer, state file, and the fact that orderbook stays on direct networking unless explicitly changed.
- [ ] Do a final focused verification pass across the touched tests.
