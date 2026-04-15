# Deep OTM Dual Dense Autoresearch Design

## Context

The current `deep_otm_baseline` autoresearch line is optimized around sparse
high-ROI outcomes under the frozen `2 USD / max5` judge. That line is still
useful as a reference, but it does not match the new operating goal:

- run `direction` and `reversal` as two equal first-class tracks
- allow both tracks to compete for production candidacy
- optimize toward much denser trading behavior
- target `10-20` trades per coin per day

Under the frozen evaluation slice of `2026-03-28` through `2026-04-10`, this
means each coin should aim for:

- minimum target band: `140-280` trades across the full window

The current sparse line should no longer define “best” by itself because it can
produce high-ROI results from only `1-3` trades, which is misaligned with the
new objective.

## Goal

Create a new dual-track dense autoresearch structure where:

- `direction` and `reversal` each have their own active session, results log,
  and queue identity
- both tracks keep the same frozen data window and comparable evaluation rules
- both tracks use trade-count gates before ROI-based ranking
- the server scheduler keeps both tracks active without exceeding current
  machine limits

## Non-Goals

This design does not:

- delete or rewrite the current sparse reversal session
- introduce a third `hybrid` production track yet
- widen the time window or silently change the frozen judge
- require eight simultaneous heavy workers on the current 24-core server

## Track Model

### Track A: Dense Direction

Purpose:

- become the primary dense-coverage search line
- recover frequent tradable opportunities before optimizing for peak ROI

Rules:

- target fixed to `direction`
- uses the same coins, cycle, frozen training window, and frozen decision
  window as the current formal line
- candidate ranking is only allowed after clearing dense trade-count gates
- sparse “spike winners” may be logged but cannot become track leaders

Expected behavior:

- higher coverage than the current sparse reversal line
- more forgiving toward opportunities that are tradable but not extreme

### Track B: Dense Reversal

Purpose:

- preserve the reversal semantic as a first-class candidate
- test whether reversal can be made dense enough to compete honestly with
  direction

Rules:

- target fixed to `reversal`
- same frozen evaluation slice and same coin set
- dense trade-count gates apply before ROI-based ranking
- factor search should focus on “timeliness / completion / false rebound
  rejection” rather than repeating late rebound signals

Expected behavior:

- fewer trades than direction is acceptable only if it still reaches the dense
  gate
- if reversal remains systematically sparse, it stays a valid line but loses
  leadership on dense-goal grounds rather than on anecdotal judgment

## Evaluation Contract

Both tracks must use the same evaluation ordering.

### Primary Gate: Trade Count

Per coin, over the frozen `2026-03-28` to `2026-04-10` window:

- reject: `< 56` trades
- keep researching only: `56-139` trades
- dense target band: `140-280` trades

Interpretation:

- `< 56` means the line is too sparse to be considered aligned with the new
  objective
- `56-139` means progress is real but still below the stated target
- `140-280` means the line is inside the intended operating band

### Secondary Gate: Stability

Once a case passes the trade-count gate, compare:

- total pnl
- total roi
- per-coin pnl / roi / trades
- `Up` and `Down` sub-results where applicable
- concentration risk:
  - whether result depends on a single coin
  - whether result depends on a few outsized trades

### Tertiary Gate: Relative Quality

Only after passing count and stability checks may a case be promoted as:

- track frontier
- next parent for bounded factor replacement
- production candidate for later online comparison

## Session Layout

Introduce two new active sessions instead of mutating the current sparse one:

- `sessions/deep_otm_baseline_direction_dense_autoresearch/`
- `sessions/deep_otm_baseline_reversal_dense_autoresearch/`

Each session should own:

- `session.md`
- `results.tsv`
- `bootstrap/`

Each session should have its own canonical program entry:

- `auto_research/program_direction_dense.md`
- `auto_research/program_reversal_dense.md`

The current sparse `auto_research/program.md` remains as the old sparse
reversal reference until the dense lines are proven.

## Program Behavior

### Shared Requirements

Both dense programs should explicitly state:

- the dense trade target: `10-20` trades per coin per day
- the corresponding frozen-window target: `140-280` trades per coin
- sparse winners cannot become frontiers
- dense count gates are checked before ROI
- sessions must report when a candidate improves ROI by sacrificing count

### Direction Dense Program

The direction program should tell the agent to:

- prioritize wider but still bounded candidates
- prefer parents that already show meaningful trade count
- demote candidates with high ROI but low count
- treat dense count failures as first-class rejection reasons

### Reversal Dense Program

The reversal program should tell the agent to:

- prefer replacements that improve timeliness, persistence, or fake-bounce
  rejection
- avoid repeatedly promoting late-rebound-heavy families
- explicitly mark when reversal remains structurally sparse even after bounded
  follow-ups

## Queue and Scheduling

The current server can sustain four heavy formal workers. Dense dual-track
autoresearch should therefore use:

- global formal-worker cap: `4`
- reserved occupancy:
  - `2` slots for dense direction
  - `2` slots for dense reversal

Fallback policy:

- if one track has fewer than two ready items, its free slot may be borrowed by
  the other track temporarily
- borrowed slots must be returned when the deprived track has a ready formal
  follow-up

This preserves track parity without requiring eight concurrent heavy workers.

## Candidate Selection Policy

### Parent Selection

For both tracks:

- select parents from the latest non-rejected run that best balances count and
  result quality
- do not pick a parent solely because it has the highest ROI
- prefer a “thicker” parent over a sparse spike winner when both are still
  profitable

### Frontier Definition

A track frontier is the best current candidate that:

- clears the trade-count gate for its stage
- is not dominated by another candidate with both higher count and better
  stability
- remains within the same frozen judge and coin universe

## Reporting Changes

Both dense sessions should require every completed formal comparison to record:

- total trades
- per-coin trades
- average trades per coin per day over the frozen window
- dense gate status:
  - `reject_sparse`
  - `subtarget`
  - `on_target`
- whether ROI improvement came with count loss or count gain

The summary language should make sparse failure explicit rather than leaving it
to manual interpretation.

## Migration Plan

### Stage 1: Documentation and Session Scaffolding

- create the two new dense program files
- create the two new session directories
- add dense-track naming and path documentation

### Stage 2: Control-Plane Support

- allow queue decisions to distinguish direction-dense vs reversal-dense
- add per-session trade-count-aware decision summaries
- ensure the supervisor can enforce the `2 + 2` slot policy

### Stage 3: Ranking Logic

- add dense gate evaluation to the results-reading path
- ensure sparse winners cannot be auto-promoted as frontiers

### Stage 4: Parallel Activation

- launch both dense sessions on the server
- keep the old sparse reversal session read-only for reference until dense
  tracks prove stable

## Risks

### Risk 1: Dense Direction Wins Too Easily

If direction produces high count quickly, reversal may appear weak by
comparison. This is acceptable as long as the comparison is honest and uses the
same dense gates.

Mitigation:

- keep reserved slots for both tracks
- require reversal to fail on dense grounds with evidence, not by neglect

### Risk 2: Dense Target Is Unrealistic Under Frozen Constraints

The current frozen judge may still be too restrictive for some coins to reach
`140-280` trades.

Mitigation:

- keep the reject / subtarget / on-target bands explicit
- if both tracks systematically fail dense gates, that becomes evidence that the
  judge itself must be revisited in a later, separate design

### Risk 3: Queue Logic Regresses Into Single-Track Occupancy

Without explicit per-track slot handling, the scheduler may refill only one
track.

Mitigation:

- implement explicit per-track occupancy accounting
- surface occupancy in the decision summary visible to Codex each cycle

## Testing Plan

Add tests covering:

- dual-session path resolution
- dense-gate classification from completed run summaries
- queue slot allocation with `2 + 2` reservation and temporary borrowing
- control-plane prompts that mention the dense gates before ROI
- frontier selection that prefers thicker profitable candidates over sparse
  spike winners

## Acceptance Criteria

This design is ready to implement when all of the following are agreed:

- `direction` and `reversal` are both treated as first-class dense tracks
- the old sparse line is demoted to reference status
- dense count gates are authoritative before ROI ranking
- the server scheduler uses the `2 + 2` slot model
- new sessions and programs are created instead of overloading the existing
  sparse session
