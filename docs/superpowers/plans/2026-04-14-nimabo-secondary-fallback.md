# Nimabo Secondary Fallback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a background-only second Nimabo API key fallback before the existing `ai.changyou.club` fallback, without changing the operator's normal manual Codex setup.

**Architecture:** Keep the primary path unchanged: the main/isolated Codex home still uses the existing Nimabo login or key. Extend the background loop with one extra provider-override layer that uses a separate isolated home and its own API key only when the primary attempt fails transiently. Preserve the current later fallbacks after that second Nimabo attempt.

**Tech Stack:** Bash, Python 3.11+, repo-local isolated Codex homes, pytest.

---

### Task 1: Add Red Tests For Secondary Nimabo Fallback

**Files:**
- Modify: `tests/test_research_experiment_automation.py`

- [ ] Step 1: Add tests that require a dedicated secondary Nimabo env pair and a distinct isolated home path.
- [ ] Step 2: Add tests that require the README to document the new order: primary Nimabo, secondary Nimabo key, `ai.changyou.club`, then official login fallback.
- [ ] Step 3: Run the focused tests and confirm they fail before implementation.

### Task 2: Implement Secondary Nimabo Fallback In The Background Loop

**Files:**
- Modify: `scripts/research/codex_background_loop.sh`

- [ ] Step 1: Add secondary background-only env vars and isolated home path defaults.
- [ ] Step 2: Add a prepare function for the secondary Nimabo provider override using the new key.
- [ ] Step 3: Insert the new retry layer ahead of the existing `CODEX_FALLBACK_*` provider layer.
- [ ] Step 4: Keep the later `ai.changyou.club` and official-login retries unchanged.

### Task 3: Document And Verify

**Files:**
- Modify: `scripts/research/README.md`

- [ ] Step 1: Document the new secondary Nimabo env vars and retry order.
- [ ] Step 2: Run focused pytest for the new behavior.
- [ ] Step 3: Run the full automation test module and shell syntax check.
- [ ] Step 4: Sync the changed files to the server and update only the background fallback env file with the new Nimabo key.
