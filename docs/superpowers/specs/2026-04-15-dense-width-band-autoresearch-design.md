# Dense Width-Band Autoresearch Design

## Context

The current dense autoresearch loops no longer have the original track-mixing
bug, but they are still behaving as if `40` is the only meaningful feature-set
width. In practice, the recent `btc / eth / sol / xrp` dense runs are all still
spending their formal cycles on `40v*` descendants.

That does not match the intended operating model:

- factor count is not fixed to `40`
- width changes should be allowed as a first-class search action
- width search must still stay bounded so the agent does not drift toward
  arbitrary wide bundles

The user selected the “moderately aggressive” policy:

- allow a bounded width ladder rather than freezing at `40`
- keep search inside a practical band rather than reopening `60+` width
  expansion

## Goal

Teach dense autoresearch to treat feature-set width as an explicit search axis
with these properties:

- allowed widths are `30 / 34 / 38 / 40 / 44 / 48`
- `40` is no longer the implicit default frontier width
- the agent sees width guidance directly in `program.md` and in the collected
  prompt snapshot
- sparse dense results bias the next step toward one wider bucket
- over-dense low-quality results may bias the next step toward one narrower
  bucket
- the agent should not jump multiple width buckets in one bounded decision
  cycle

## Non-Goals

This change does not:

- reopen the `64`-factor live baseline as the dense search default
- authorize unconstrained widths outside `30-48`
- replace diagnosis-guided family swaps with blind count chasing
- change the dense trade gate thresholds
- change the server queueing or live-slot caps

## Width Policy

### Allowed Width Ladder

Dense autoresearch may only choose from:

- `30`
- `34`
- `38`
- `40`
- `44`
- `48`

The ladder is ordered and local:

- widening may move up by one bucket only
- narrowing may move down by one bucket only
- same-width family swaps remain valid

Examples:

- `40 -> 44` is allowed
- `40 -> 48` in one cycle is not allowed
- `34 -> 30` is allowed
- `34 -> 44` in one cycle is not allowed

### Decision Heuristic

Per coin, using the latest verified dense result:

- if the result is still `reject_sparse`, prefer the next wider bucket before
  repeating another same-width cosmetic swap
- if the result is `subtarget`, keep the current width or widen one bucket only
  when the evidence still looks structurally too sparse
- if the result is `on_target`, keep width stable and search inside families
- if the result is materially over-dense and quality degrades, allow a one-step
  narrower move

This keeps width as a real search dimension without turning each cycle into a
full-width restart.

## Prompting Changes

The agent should no longer infer this policy from memory or from old docs. The
prompt should state it directly in two places.

### Program Files

Both dense program files should explicitly declare:

- width is not fixed to `40`
- the allowed width ladder is `30 / 34 / 38 / 40 / 44 / 48`
- widening and narrowing move by one bucket per cycle only
- count is checked before ROI when deciding whether to widen, hold, or narrow

### Machine-Collected Snapshot

The prompt should also include a width policy section that is already computed
for the agent, including:

- the allowed width ladder
- whether the current line should interpret sparse outcomes as “widen first”
- whether on-target outcomes should prefer same-width family replacement

If available, coin-specific width hints should be shown from the latest run
labels or feature-family references.

## Reporting Expectations

The new width-band behavior should make the next cycles easier to interpret:

- when a run stays at the same width, the notes should say it is a same-width
  family replacement
- when a run changes width, the notes should say which adjacent bucket it moved
  to and why
- the session should stop implying that every frontier is another `40v*` by
  default

## Affected Files

- `auto_research/program_direction_dense.md`
- `auto_research/program_reversal_dense.md`
- `src/pm15min/research/automation/control_plane.py`
- `tests/test_research_experiment_automation.py`

## Success Criteria

- Dense program files document the width ladder and the “one bucket per cycle”
  rule.
- The generated Codex prompt explicitly says width is not fixed to `40`.
- The generated Codex prompt includes the width ladder and the widen/hold/narrow
  heuristic.
- Prompt-related tests cover the new width guidance so later prompt changes do
  not silently regress back to “40-only” behavior.
