# Autoresearch Queue Supervisor Design

## Goal

Build a repo-local queue and supervisor layer so autoresearch can keep up to three formal experiments running continuously, queue pre-decided next steps, and prioritize repairing failed runs before opening new branches.

## Problem

The current background loop mixes two responsibilities:

- decide what should run next
- start work immediately in the same bounded cycle

That is good enough for one-shot iterations, but it is not a reliable always-full scheduler. If three formal runs are already active, a new decision cannot be held as durable pending work. If a run finishes or is killed by the kernel, the system depends on the next Codex cycle noticing the empty slot quickly enough. Failures are also mixed together with normal new candidates instead of being treated as first-class repair work.

## Scope

This design covers the dynamic autoresearch line under:

- `scripts/research/codex_background_loop.sh`
- `scripts/research/run_one_experiment_background.sh`
- `var/research/autorun/`
- the active session rooted at `sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/`

It does not try to redesign experiment internals, reduce per-run memory use, or change experiment scoring logic.

## Architecture

### Layer 1: Decision Producer

Codex continues to act as the decision maker, but its output changes shape. Instead of always directly launching work, each bounded cycle should primarily produce queue actions:

- enqueue a new formal launch
- enqueue a repair action
- mark a queued item dead
- update notes about why an item should be repaired or discarded

The Codex cycle remains bounded and auditable. It should not be responsible for maintaining occupancy in real time.

### Layer 2: Durable Queue

Add a small repo-local queue file under `var/research/autorun/` that stores the next runnable work items and their current state. This queue becomes the handoff point between decision making and execution.

Each queue item should include:

- stable item id
- market
- suite name
- run label
- action kind: `launch`, `resume`, or `repair`
- status: `queued`, `running`, `repair`, `done`, or `dead`
- priority
- reason / notes
- retry count
- timestamps
- optional parent item id

The queue is the source of truth for pending formal work. If the machine is already full, the next experiment waits here instead of being lost.

### Layer 3: Queue Supervisor

Add one long-running supervisor script dedicated to occupancy management. It should:

- inspect live formal workers
- inspect the queue
- keep at most three live formal runs
- launch the highest-priority eligible queued item whenever there is capacity
- mark queue items done when their run becomes terminal
- mark queue items repair when a running process disappears without terminal completion

The supervisor must not invent new research decisions. It only reconciles live processes with queue state and fills empty slots.

## Scheduling Rules

### Capacity Rule

- hard limit: three live formal runs
- one live formal run per market

This matches current machine limits better than the older four-slot behavior.

### Priority Rule

When at least one slot is free, pick the next item in this order:

1. `repair`
2. resumable current-line work
3. newly queued `launch`

### Market Rule

Per market:

- allow at most one `running` item
- if a `repair` item exists, do not launch a newer normal candidate for that market
- a newly decided normal candidate may replace an older queued normal candidate for the same market, but it must not replace a repair item

## Failure Handling

Failures should be promoted to explicit repair work instead of disappearing into logs.

A running item moves to `repair` when:

- its process exits unexpectedly
- the kernel kills it
- the wrapper exits non-zero before terminal completion
- required data/env dependencies are missing

A repair item stays ahead of new work for the same market. Only after Codex explicitly marks it non-viable should it move to `dead`, allowing a fresh normal candidate to take over.

## State Reconciliation

On each supervisor loop:

1. Read queue state.
2. Detect live formal workers by `suite` and `run_label`.
3. Reconcile queued `running` items against actual live workers.
4. For any missing worker, inspect the run directory:
   - terminal summary: mark `done`
   - partial or crashed: mark `repair`
5. Count active live formal runs.
6. Launch queued work until live count reaches three or no eligible item remains.

This makes restart recovery deterministic. If the supervisor dies and restarts, it can reconstruct occupancy from queue plus process table.

## Integration With Existing Files

### Keep

- `scripts/research/codex_background_loop.sh`
- `scripts/research/run_one_experiment_background.sh`
- existing session files and cycle logs

### Add

- queue data helper module in `src/pm15min/research/automation/`
- queue supervisor script under `scripts/research/`
- queue-aware status output under `scripts/research/status_autorun.sh`

### Stop Using

- `scripts/research/bootstrap_keepalive.sh` as the active supervisor

It is a legacy fixed-slot bootstrap launcher with hard-coded suite names. It should be retired after the new queue supervisor is stable.

## Operator Outcome

With this design in place:

- Codex can decide a next run even when all three slots are full
- the decision is stored durably in the queue
- a finished or killed run frees a slot immediately
- the supervisor launches the next queued or repair item without waiting for a new human action
- failed work is repaired before the system moves on to unrelated new branches

## Success Criteria

The design is successful when:

1. The machine keeps three live formal runs whenever at least three eligible items exist.
2. A new Codex decision can be queued without immediate launch.
3. A failed run becomes queued repair work instead of disappearing.
4. Restarting the supervisor does not duplicate already running experiments.
5. The old fixed bootstrap keepalive is no longer required for the dynamic autoresearch line.
