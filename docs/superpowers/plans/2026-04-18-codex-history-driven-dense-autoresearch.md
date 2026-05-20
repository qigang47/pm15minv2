# Codex History-Driven Dense Autoresearch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make dense autoresearch read a structured historical digest before each Codex cycle and automatically switch into a heavier analysis mode when a market keeps failing through repeated low-trade or zero-capture loops.

**Architecture:** Extend the prompt-building layer in `control_plane.py` so it derives a per-market historical digest from recent completed runs, session results, and recent cycle eval notes. Feed that digest into the Codex prompt ahead of occupancy snapshots, and expose a heavy-analysis mode whenever a market crosses repeat-failure thresholds.

**Tech Stack:** Python, pytest

---

### Task 1: Lock Prompt Expectations With Tests

**Files:**
- Modify: `tests/test_research_experiment_automation.py`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Write failing tests for the new prompt contract**

Add tests that require:
- a historical decision digest section in the prompt
- historical digest lines to appear before the machine decision summary
- heavy-analysis mode when a market has three consecutive very-low-trade runs without improving its best trade count
- session `results.tsv` notes to appear in the digest when they mention the market

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `pytest -q tests/test_research_experiment_automation.py -k 'historical_decision_digest or heavy_analysis'`
Expected: FAIL because the prompt still prioritizes the machine summary and does not build the new digest.

### Task 2: Build Historical Digest Helpers

**Files:**
- Modify: `src/pm15min/research/automation/control_plane.py`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Add helper functions for recent historical evidence**

Implement helpers to:
- read recent terminal runs per market for the active dense context
- read and filter session `results.tsv`
- read recent `cycles/*/eval-results.md`
- classify repeated failure modes and decide whether heavy analysis is required

- [ ] **Step 2: Keep the logic bounded**

Use small caps, for example:
- recent completed runs per market: limited window
- recent results.tsv rows per market: limited window
- recent eval files: limited window

### Task 3: Feed The Digest Into The Codex Prompt

**Files:**
- Modify: `src/pm15min/research/automation/control_plane.py`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Insert the historical digest into `build_codex_cycle_prompt`**

Place the new section after the dense program guidance and before the machine decision summary.

- [ ] **Step 2: Rewrite prompt instructions**

Change the instructions so that:
- the historical digest is required input, not optional context
- normal cycles use the short digest
- heavy-analysis cycles must explain the repeated failure loop and queue a materially different direction unless blocked by verified capacity constraints

### Task 4: Verify And Clean Up

**Files:**
- Modify: `src/pm15min/research/automation/control_plane.py`
- Modify: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Run focused tests**

Run: `pytest -q tests/test_research_experiment_automation.py -k 'historical_decision_digest or heavy_analysis or codex_cycle_prompt'`
Expected: PASS

- [ ] **Step 2: Run syntax check**

Run: `python3 -m py_compile src/pm15min/research/automation/control_plane.py`
Expected: PASS

- [ ] **Step 3: Commit if requested**

Do not commit automatically unless the operator explicitly asks for it.
