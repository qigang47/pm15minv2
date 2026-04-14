# Codex Research Program

This is the canonical autoresearch entry for the `deep_otm_baseline` near-40-factor
`reversal` line under the frozen `2 USD / max5` judge.
`direction` remains paused for this session unless the program is explicitly
rewritten again.

## Canonical References

- Diagnosis for replacement priorities: `docs/DEEP_OTM_BASELINE_UP_DIAGNOSIS.md`
- Evaluation discipline and reporting format:
  `docs/DEEP_OTM_BASELINE_RETRAIN_PLAN.md`
- Active session: `sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/session.md`
- Active results log:
  `sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/results.tsv`
- Historical broader retrain evidence only:
  `sessions/deep_otm_baseline_retrain_autoresearch/session.md`

## Frozen Evaluation Slice

- training window through `2026-03-27`
- frozen decision / backtest window from `2026-03-28` through `2026-04-10`
- use this fixed slice until newer canonical labels, kline, and orderbook
  artifacts are available
- do not silently roll the decision end date past `2026-04-10` inside this
  session

## Current Objective

Run only this formal experiment class:

- Track R: near-40-factor `reversal`

This track uses this fixed judge:

- profile: `deep_otm_baseline`
- coins: `btc / eth / sol / xrp`
- label set: `truth`
- offsets: `7 / 8 / 9`
- stake: `2 USD`
- per-offset trade cap: `max_trades_per_market = 5`
- total per-market notional cap: `10 USD`
- `ret30` guard disabled everywhere

Allowed search axes inside this session:

- bounded factor replacement inside near-40 custom families
- bounded sample-weight comparison inside the fixed `reversal` track

Retired for this session:

- all near-40 `direction` follow-ups and weight sweeps
- baseline freeze as a controlling stage
- the old Stage 0 to Stage 5 retrain ladder
- wide feature-width family search outside near-40 custom families
- `hybrid` target search
- stale `1 USD / max3` or stale `2026-04-03` windows

## Track R: 40-Factor Reversal

Goal:

- continue the existing near-40 `reversal` search under the same frozen judge

Current references:

- active session evidence already stored under
  `sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/`
- current seed specs such as
  `research/experiments/suite_specs/baseline_focus_feature_search_*_reversal_40plus_2usd_5max_20260409.json`

Rules:

- target fixed to `reversal`
- resume incomplete current-line runs before launching a new follow-up
- when session artifacts and current run directories disagree, trust the current
  run directories
- a formal run is finished only when `completed_cases + failed_cases` reaches
  `cases`
- if `summary.json` exists but work remains, treat that run as a checkpointed
  current-line run that should be resumed instead of discarded
- when a coin slot has no live formal run and its latest current-line run is
  already finished, treat that slot as idle and choose the next follow-up for
  that coin instead of idling on stale session notes
- weighting is allowed, but only with `target = reversal` fixed for the whole
  suite
- treat older reversal-heavy results in this session as prior evidence, not as
  permanent winners that block new bounded replacements

## Factor Replacement Contract

Every new factor challenger must obey all of the following:

- start from the most recent non-rejected near-40 parent for the same coin and
  the `reversal` track
- keep the mainline family at `40` factors
- preferred move is one drop plus one add
- one pure drop or one pure add is allowed only when explicitly converting a
  `39` or `41` bridge candidate back to `40`
- default maximum change size is two columns total
- do not make larger multi-factor jumps unless the active session explicitly
  records why a tightly coupled pair must move together
- update `research/experiments/custom_feature_sets.json` before launching the
  formal suite
- name the new family as a fresh `focus_<coin>_40_*` slug that encodes the
  change
- record parent feature set, dropped factor(s), added factor(s), and the reason
  for the change in the active session
- prefer diagnosis-driven replacements:
  - drop redundant late-rebound, position-repair, or repeated momentum-family
    signals first
  - add stronger timing, persistence, strike-distance, or flip-feasibility
    information when supported by existing repo factors
- do not promote a `38` or `39` bridge result directly as the mainline winner
- convert the idea into an explicit `40`-factor candidate first, then judge it
  under the frozen line
- do not open or mix any `direction` run in this session

## Weight Contract

Weighting is the only extra search axis allowed beyond factor replacement.

Rules:

- weight comparison may happen inside the `reversal` track, but `target` must
  stay fixed as `reversal` for the whole suite
- prefer existing weight labels first:
  - `current_default`
  - `no_vol_weight`
  - one mild reversal-aligned offset variant
  - one strong reversal-aligned offset variant
- derive new weight variants from the existing reversal
  contrarian templates already used in the active session
- if a formal decision depends on a specific weight branch, the session must
  state that explicitly instead of crediting the whole factor family
- do not open a pure weight-only suite until the current factor parent is clear
  enough to deserve that extra pass

## Required Reporting

Every completed formal comparison must record these items in the active session
before the coin frontier advances:

- target track (`reversal`)
- feature family name and parent family name
- dropped / added factors
- weight branch used by the leading result
- total pnl, total roi, total trades
- `Up` pnl, roi, win rate, trades
- `Down` pnl, roi, win rate, trades
- per-coin totals
- per-coin `Up / Down` results
- daily or segmented stability notes
- whether the result depends on a few trades, one coin, or a weight branch that
  collapses trade count

Each coin decision must be classified as exactly one of:

- `pass`
- `keep_observing`
- `research_only`
- `reject`

## Hard Constraints

- do not reopen the old staged retrain ladder for this session
- do not reopen any `direction` suite in this session
- do not mix target changes with factor replacement
- do not use non-near-40 width search as the controlling path
- do not use stale `1 USD / max3` settings
- do not use stale `2026-04-03` decision windows
- do not accept `0 trades / 0 pnl / 0 roi` as a solved endpoint
- each active formal suite must keep `parallel_case_workers = 1`
- at most one active formal suite per coin
- four suite-level concurrent runs are allowed only when they all stay inside
  this near-40 reversal session and each run has a distinct coin slot
- the bounded cycle is the Codex decision window, not a mandatory timeout on
  every formal worker
- current-line formal runs should normally stay alive across decision cycles;
  use short bounded probes only for diagnostics, forced checkpoints, or stuck
  run inspection
- if fewer than four distinct coin slots are active and there is a clear next
  same-session follow-up for one or more idle coins, autoresearch should keep
  launching distinct follow-ups in the same bounded cycle until every allowed
  idle coin slot is filled or no clear next follow-up remains
- do not leave a coin idle solely because its latest result is thin-sample,
  tied, or still classified as `research_only`; when a slot is free and the
  frontier is unresolved, continue with the next bounded follow-up that most
  directly tests or strengthens that coin's current edge

## One-Cycle Contract

Each Codex background iteration must complete exactly one bounded cycle:

1. Read this file plus the active session artifacts.
2. Determine the current reversal frontier separately for each coin from the
   active session, not from stale broader retrain sessions.
3. Reconcile the active session notes against the current run directories before
   deciding what is still active, blocked, or already complete.
4. Inspect only the active, incomplete, or most recent completed runs needed to
   avoid duplicates and choose the next step.
5. Prefer resuming the current run for a coin slot over launching a new branch,
   but do not treat a completed run as resumable just because an older session
   note still says it is unresolved.
6. If a current `reversal` near-40 suite spec is missing for the frozen
   `2 USD / max5 / 2026-04-10` judge, create or update it from the nearest
   same-target template before launching.
7. If a new challenger requires factor replacement, update
   `research/experiments/custom_feature_sets.json` first and record the parent /
   drop / add plan in the active session.
8. If one or more coin slots are idle and the program still allows concurrent
   same-session runs, refill as many distinct idle coin slots as possible in
   this same bounded cycle rather than stopping after the first new launch.
   Thin-sample or tied evidence is not a reason to keep a coin idle when a
   bounded next follow-up is available.
9. Launching multiple distinct same-session follow-ups for different coins
   still counts as one bounded cycle as long as the launches happen inside the
   same decision pass.
10. If a current-line worker should keep running after this decision cycle,
    launch or resume it with `auto_research/run_one_experiment_background.sh`
    and leave it alive.
11. Use `auto_research/run_one_experiment.sh` only for intentionally
    bounded probes, diagnostics, or forced checkpoints.
12. Use `auto_research/summarize_experiment.py` after any completed, failed,
    or intentionally checkpointed formal run; do not stop a healthy live worker
    merely to summarize it.
13. If the allowed coin slots are already filled by healthy current-line live
    workers, a monitor-only cycle is valid and should not relaunch or stop them.
14. Update the active session with reversal status, frontier changes, blockers,
    and next-step notes.
15. Stop after this one bounded decision cycle and leave a plain-English
    summary; healthy formal workers may remain running after the cycle ends.
