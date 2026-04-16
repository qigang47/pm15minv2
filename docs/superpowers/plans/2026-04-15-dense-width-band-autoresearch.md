# Dense Width-Band Autoresearch Implementation Plan

> Plan complete for the approved width-band change. The goal is to stop dense
> autoresearch from behaving as if `40` were fixed, while still keeping width
> search bounded.

## File Map

- Modify: `auto_research/program_direction_dense.md`
- Modify: `auto_research/program_reversal_dense.md`
- Modify: `src/pm15min/research/automation/control_plane.py`
- Modify: `tests/test_research_experiment_automation.py`

## Task 1: Lock The Width-Band Rules Into Program Files

- [ ] Add failing tests that assert both dense program files now document:
  - width is not fixed to `40`
  - the allowed ladder `30 / 34 / 38 / 40 / 44 / 48`
  - one-bucket widening and narrowing only
- [ ] Run the focused test selection and confirm it fails before any edits.
- [ ] Update `auto_research/program_direction_dense.md` and
  `auto_research/program_reversal_dense.md` to include the width ladder and the
  adjacent-step rule.
- [ ] Re-run the focused tests and confirm they pass.

## Task 2: Expose Width Guidance In The Generated Prompt

- [ ] Add failing tests that assert `build_codex_cycle_prompt(...)` includes:
  - a width-band policy section
  - the explicit ladder `30 / 34 / 38 / 40 / 44 / 48`
  - wording that `40` is not fixed
  - wording that sparse outcomes prefer one-step widening
- [ ] Implement the minimal prompt helpers in
  `src/pm15min/research/automation/control_plane.py`.
- [ ] Keep the implementation prompt-only: no queue semantics or runtime launch
  behavior should change in this task.
- [ ] Re-run the focused tests and confirm they pass.

## Task 3: Run Regression Coverage

- [ ] Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py
```

- [ ] If prompt snapshots changed in expected ways, keep the new text and verify
  there are no unrelated failures.

## Task 4: Prepare Server Sync Follow-Up

- [ ] Summarize exactly which files changed.
- [ ] If tests pass locally, sync only those changed files to the server in the
  next execution step.
- [ ] After sync, verify the next generated prompt on the server contains the
  new width-band guidance.
