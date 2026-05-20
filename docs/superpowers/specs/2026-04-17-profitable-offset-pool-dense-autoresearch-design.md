# Profitable Offset Pool Dense Autoresearch Design

## Context

The current dense autoresearch flow still decides mainly from full formal
backtest outputs. That is too slow for the user's current objective:

- first increase tradeable coverage
- then improve quality on top of that coverage
- keep total formal concurrency capped at `4`

The user selected a two-stage search policy:

- build a profitable offset pool per coin from the real strategy semantics
- use that pool as a fast-screen gate before spending a full dense formal slot

This is not a generic historical-win cache. The pool must come from the user's
actual entry rule, not from whichever model happened to win in prior runs.

## Goal

Add a profitable-offset-pool fast-screen stage ahead of dense formal validation
so autoresearch can reject low-coverage factor sets quickly and spend formal
capacity on candidates that already prove they can capture a large share of the
coin's profitable windows.

The design must preserve:

- `btc / eth / sol / xrp`
- both dense tracks: `direction_dense` and `reversal_dense`
- total queue-supervisor concurrency cap of `4`
- full formal validation as the final judge

## Non-Goals

This change does not:

- replace the existing formal orderbook backtest stage
- merge `direction` and `reversal` into one track
- change the queue-supervisor slot cap logic
- define final live promotion directly from fast-screen results
- reopen broader historical windows outside the frozen April window

## Frozen Pool Window

The profitable offset pool is fixed to:

- decision / backtest window: `2026-04-01` through `2026-04-15`
- stake: `2usd`

Training for formal runs may still end at `2026-03-31`, but the profitable
offset pool itself is built only from the April `2usd` replay window.

## Pool Definition

### Pool Scope

Each coin gets one shared profitable offset pool:

- `btc`
- `eth`
- `sol`
- `xrp`

The pool is shared by both dense tracks for that coin:

- `direction_dense`
- `reversal_dense`

The pool is not split into separate direction-only and reversal-only pool files.

### Atomic Unit

One pool item equals one exact `offset` within one exact cycle window.

The pool therefore tracks window identity at the same granularity the user
cares about:

- one `offset` == one exact window

### Inclusion Rule

An offset window enters the profitable pool only if all of the following are
true:

1. the window resolved with a valid final winner side
2. the winner-side entry quote was available
3. the winner-side entry quote was within the strategy entry cap:
   `<= 0.30`
4. the window therefore represented a real strategy-valid opportunity in the
   final settlement direction

Operationally this is the same semantic rule the user described:

- there was an entry opportunity below `0.3`
- the side that should be entered is the final settlement side

This means the pool is strategy-derived, not model-derived.

## Capture Rule

The fast-screen stage needs a precise definition for whether a candidate feature
set "captures" a profitable offset.

A profitable pool item counts as captured only when all of the following are
true:

1. the candidate bundle produces a final tradeable decision for that offset
   window under the current policy path
2. the chosen side matches the final winner side for the window
3. the winner-side entry quote for that tradeable window is still within the
   strategy entry cap: `<= 0.30`

This keeps the fast-screen aligned with the user's stricter rule:

- only windows that would really qualify for entry count as captured
- a merely correct directional guess is not enough

We still record supporting diagnostics so later policy tuning can distinguish:

- pool window existed but the candidate never reached a final trade
- candidate reached a final trade but picked the wrong side
- candidate reached a final trade on the correct winner-side window

## Fast-Screen Success Gate

The success target is coverage, not raw trade count.

Per coin:

- compute total profitable pool size
- compute how many pool items the candidate captured
- compute coverage ratio = `captured_pool_items / total_pool_items`

Dense fast-screen passes when coverage reaches about `70%` of that coin's
profitable pool.

This is intentionally coin-relative. The gate is not a hard fixed trade-count
number because the user wants the search to adapt to each coin's actual number
of valid profitable offsets.

## Decision Policy

### Stage Order

Dense search becomes:

1. generate or reuse candidate feature set
2. run profitable-offset-pool fast-screen for the coin
3. if the candidate fails pool coverage, keep it out of formal dense occupancy
4. if the candidate passes pool coverage, allow full formal orderbook validation
5. choose frontiers only from candidates that survive the full formal stage

### Fast-Screen Ranking

When multiple fast-screen candidates are compared for the same coin/track,
prefer in this order:

1. higher profitable-pool coverage ratio
2. more captured profitable pool items
3. more correctly traded profitable pool items
4. only then use downstream formal metrics when available

### Formal Frontier Rule

Fast-screen may qualify a candidate for formal work, but it may not by itself
make that candidate the coin frontier. Formal frontier promotion still requires
the existing full orderbook evaluation.

## Shared Coin Pool With Dual Tracks

The profitable pool is coin-level and shared, but the search tracks remain
separate.

That means:

- `direction_dense` and `reversal_dense` both measure themselves against the
  same profitable pool for that coin
- either track may discover the better coverage pattern first
- formal runs, sessions, and frontier choices remain track-local

This avoids maintaining duplicated pool files while still allowing the user to
keep direction and reversal as equal competing lines.

## Queue And Concurrency

The queue supervisor stays unchanged at a total live cap of `4`.

The new rule is:

- fast-screen work should be cheap enough that it does not monopolize a formal
  slot for long
- only candidates that pass fast-screen should be promoted into the formal queue

This reduces wasted full formal launches and should let the supervisor spend
more of the four live slots on candidates that already show pool coverage.

## Prompting Changes

Both dense program files and the machine-collected prompt snapshot should state
the new search policy directly.

They should explicitly say:

- first optimize profitable-offset-pool coverage
- the pool is coin-level and shared by both tracks
- the pool is built from `2026-04-01` through `2026-04-15`, `2usd`
- one offset equals one pool window
- pool inclusion means winner-side entry quote existed at `<= 0.30`
- formal validation comes only after pool coverage passes
- passing the pool gate does not itself make a frontier

The prompt snapshot should also show, per coin:

- profitable pool size
- latest candidate coverage count
- latest candidate coverage ratio
- whether the current active run is still before or after the fast-screen gate

## Artifacts

Add durable pool artifacts under repo runtime outputs so both the control plane
and the experiments can inspect them without rescanning broad history.

Expected artifact families:

- per-coin profitable offset pool table
- per-run fast-screen summary
- per-run leaderboard or comparable ranking summary

The pool artifact should be stable across repeated candidate runs until the
frozen April window is intentionally rebuilt.

## Affected Areas

Expected implementation touch points:

- `src/pm15min/research/automation/quick_screen.py`
- `src/pm15min/research/automation/control_plane.py`
- `src/pm15min/research/automation/dense_policy.py`
- `auto_research/program_direction_dense.md`
- `auto_research/program_reversal_dense.md`
- related experiment automation tests

## Success Criteria

- Each coin has a durable profitable offset pool derived from the user's entry
  rule over `2026-04-01` through `2026-04-15`, `2usd`.
- Fast-screen can report pool size, captured offsets, and coverage ratio for a
  candidate.
- Dense decision prompts mention the pool-first search policy directly.
- Dense search can keep total formal concurrency at `4` while avoiding obvious
  low-coverage full runs.
- Tests cover the pool inclusion rule, capture rule, ranking rule, and prompt
  text so the behavior does not silently regress.
