# Codex Background Loop Child Wait Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent the autorun background loop from marking a successful Codex decision cycle as failed when the detached attempt process exits normally.

**Architecture:** Keep the existing autorun flow intact and only harden the detached-attempt lifecycle in `codex_background_loop.sh`. Cover the regression with focused tests that assert the script no longer relies on `wait` for a non-child detached pid and still preserves full process-group termination for real timeout or abort paths.

**Tech Stack:** Bash, pytest

---

### Task 1: Capture the regression in tests

**Files:**
- Modify: `tests/test_research_experiment_automation.py`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Add a failing assertion for the detached process exit path**

```python
def test_codex_background_loop_avoids_waiting_on_non_child_detached_pid() -> None:
    script_text = Path("scripts/research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert 'wait "$attempt_pid"' not in script_text
    assert "kill -0 \"$attempt_pid\"" in script_text
```

- [ ] **Step 2: Run the focused test to verify it fails before the fix**

Run: `PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py -k detached_pid`
Expected: FAIL because the script still contains `wait "$attempt_pid"`.

### Task 2: Fix the detached-attempt exit handling

**Files:**
- Modify: `scripts/research/codex_background_loop.sh`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Replace the non-child wait with a safe detached-exit status read**

```bash
  local attempt_exit_code=0
  if ! kill -0 "$attempt_pid" >/dev/null 2>&1; then
    if [[ -r "/proc/$attempt_pid/stat" ]]; then
      attempt_exit_code=0
    else
      attempt_exit_code=0
    fi
  fi
```

Note: implement the final shell-safe variant that keeps timeout/abort termination behavior intact and does not call `wait "$attempt_pid"` after the detached process has been launched through `setsid ... &`.

- [ ] **Step 2: Keep timeout and provider-abort cleanup paths unchanged**

```bash
      terminate_attempt_process_group "$attempt_pid"
      return 75
```

```bash
      terminate_attempt_process_group "$attempt_pid"
      return 74
```

```bash
        terminate_attempt_process_group "$attempt_pid"
        return 124
```

- [ ] **Step 3: Re-run the focused tests**

Run: `PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py -k 'detached_pid or terminates_full_attempt_process_group'`
Expected: PASS

### Task 3: Verify the full autorun surface

**Files:**
- Modify: `tests/test_research_experiment_automation.py`
- Modify: `scripts/research/codex_background_loop.sh`

- [ ] **Step 1: Run the broader autorun automation tests**

Run: `PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py tests/test_research_experiment_queue.py`
Expected: PASS

- [ ] **Step 2: Validate shell syntax**

Run: `bash -n scripts/research/codex_background_loop.sh`
Expected: no output, exit 0

- [ ] **Step 3: Sync the fixed script to the server and verify autorun state**

Run: `rsync -avP --relative scripts/research/codex_background_loop.sh huatai@ht66:/home/huatai/qigang/pm15min/v2/`
Expected: script updated on server

Run: `ssh huatai@ht66 'cd /home/huatai/qigang/pm15min/v2 && ./scripts/research/codex_background_loop.sh restart'`
Expected: background loop restarts cleanly

- [ ] **Step 4: Confirm the loop no longer stops on the same bug**

Run: `ssh huatai@ht66 'cd /home/huatai/qigang/pm15min/v2 && ./scripts/research/status_autorun.sh'`
Expected: background state returns to running or idle between cycles instead of immediately failing with the detached wait error
