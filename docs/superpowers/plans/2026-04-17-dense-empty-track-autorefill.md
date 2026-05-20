# Dense Empty Track Auto-Refill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep dense autoresearch tracks from going idle by automatically re-queuing recent completed branches when a track has no live workers and no queued work.

**Architecture:** Add one queue-state helper that reseeds an empty dense track from recent `done` items that already proved they can produce quick-screen artifacts, then invoke it from the queue supervisor before launch selection. Keep the fallback conservative: only trigger for empty tracks, prefer one recent branch per market first, and reuse existing repair/relaunch mechanics.

**Tech Stack:** Python 3.11+, existing queue supervisor and queue state helpers, pytest.

---

### Task 1: Add failing queue-state tests

**Files:**
- Modify: `tests/test_research_experiment_queue.py`

- [ ] **Step 1: Write failing tests**
- [ ] **Step 2: Run targeted tests to confirm failure**
- [ ] **Step 3: Implement the minimal queue reseed helper**
- [ ] **Step 4: Re-run targeted tests until green**

### Task 2: Wire the fallback into supervise-once

**Files:**
- Modify: `auto_research/experiment_queue.py`
- Modify: `src/pm15min/research/automation/queue_state.py`

- [ ] **Step 1: Call the new reseed helper after reconcile and before launch selection**
- [ ] **Step 2: Include reseed results in supervisor output for visibility**
- [ ] **Step 3: Run targeted supervisor-related tests**

### Task 3: Verify end to end behavior

**Files:**
- Modify: `tests/test_research_experiment_automation.py` if needed for coverage only

- [ ] **Step 1: Run focused regression tests around queue reconciliation and dense autorun helpers**
- [ ] **Step 2: Sync changed files to server**
- [ ] **Step 3: Restart dense sessions and verify empty tracks auto-refill instead of staying idle**
