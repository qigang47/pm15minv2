# Dual Dense Autoresearch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build two equal dense autoresearch tracks for `direction` and `reversal`, with shared experiment execution, track-aware queueing, and trade-count-first ranking.

**Architecture:** Keep one shared formal experiment queue and one shared queue supervisor, but run two separate Codex decision loops with separate program files, session directories, status directories, and result logs. Add track metadata to queued work, enforce a `2 + 2` slot split in the shared queue, and classify completed runs by dense trade gates before any ROI-based frontier promotion.

**Tech Stack:** Bash entrypoints, Python 3.11+, pytest, repo-local autoresearch scripts, `src/pm15min/research/automation/*`

---

## File Map

### Runtime and Programs

- Create: `auto_research/program_direction_dense.md`
- Create: `auto_research/program_reversal_dense.md`
- Create: `auto_research/start_direction_dense.sh`
- Create: `auto_research/start_reversal_dense.sh`
- Create: `auto_research/status_dense_autorun.sh`
- Modify: `auto_research/codex_background_loop.sh`
- Modify: `auto_research/status_autorun.sh`
- Modify: `auto_research/README.md`

### Queue and Shared Scheduling

- Modify: `auto_research/experiment_queue.py`
- Modify: `auto_research/experiment_queue_supervisor.sh`
- Modify: `src/pm15min/research/automation/queue_state.py`
- Test: `tests/test_research_experiment_queue.py`

### Prompting and Track-Aware Status

- Modify: `src/pm15min/research/automation/control_plane.py`
- Modify: `src/pm15min/research/automation/__init__.py`
- Test: `tests/test_research_experiment_automation.py`
- Test: `tests/test_research_experiment_queue.py`

### Dense Gate Policy

- Create: `src/pm15min/research/automation/dense_policy.py`
- Test: `tests/test_research_experiment_dense_policy.py`

### Session and Path Docs

- Modify: `docs/SERVER_CORRECT_PATHS.md`

## Task 1: Add Dense Program Files And Instance Wrappers

**Files:**
- Create: `auto_research/program_direction_dense.md`
- Create: `auto_research/program_reversal_dense.md`
- Create: `auto_research/start_direction_dense.sh`
- Create: `auto_research/start_reversal_dense.sh`
- Create: `auto_research/status_dense_autorun.sh`
- Modify: `auto_research/README.md`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Write the failing tests for dual dense program scaffolding**

```python
def test_dense_program_files_exist_and_define_track_targets() -> None:
    direction_text = Path("auto_research/program_direction_dense.md").read_text(encoding="utf-8")
    reversal_text = Path("auto_research/program_reversal_dense.md").read_text(encoding="utf-8")

    assert "target fixed to `direction`" in direction_text
    assert "target fixed to `reversal`" in reversal_text
    assert "10-20 trades per coin per day" in direction_text
    assert "10-20 trades per coin per day" in reversal_text
    assert "140-280" in direction_text
    assert "140-280" in reversal_text


def test_dense_start_wrappers_bind_distinct_program_and_autorun_dirs() -> None:
    direction_text = Path("auto_research/start_direction_dense.sh").read_text(encoding="utf-8")
    reversal_text = Path("auto_research/start_reversal_dense.sh").read_text(encoding="utf-8")

    assert "program_direction_dense.md" in direction_text
    assert "program_reversal_dense.md" in reversal_text
    assert "var/research/autorun/direction_dense" in direction_text
    assert "var/research/autorun/reversal_dense" in reversal_text


def test_status_dense_autorun_reads_both_dense_instances() -> None:
    script_text = Path("auto_research/status_dense_autorun.sh").read_text(encoding="utf-8")

    assert "direction_dense" in script_text
    assert "reversal_dense" in script_text
    assert "status_autorun.sh" in script_text
```

- [ ] **Step 2: Run the focused tests to verify they fail**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py -k "dense_program_files_exist or dense_start_wrappers_bind or status_dense_autorun_reads"
```

Expected:

- failures because the dense program files and wrappers do not exist yet

- [ ] **Step 3: Add the two dense program files with explicit dense rules**

```md
# Codex Research Program

This is the canonical dense autoresearch entry for the `deep_otm_baseline`
`direction` line.

## Objective

- run only the dense `direction` track
- target `10-20` trades per coin per day
- under `2026-03-28` through `2026-04-10`, treat `140-280` trades per coin as
  the target band
- sparse winners cannot become frontiers

## Hard Constraints

- target fixed to `direction`
- reject `< 56` trades per coin over the frozen window
- classify `56-139` as `subtarget`
- classify `140-280` as `on_target`
- check count before roi in every formal decision
```

```bash
#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export PROGRAM_PATH="$ROOT_DIR/auto_research/program_direction_dense.md"
export SESSION_DIR="$ROOT_DIR/sessions/deep_otm_baseline_direction_dense_autoresearch"
export AUTORUN_DIR="$ROOT_DIR/var/research/autorun/direction_dense"
exec "$ROOT_DIR/auto_research/codex_background_loop.sh" "$@"
```

- [ ] **Step 4: Document both instances in the README and add a dual-status wrapper**

```bash
#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
for name in direction_dense reversal_dense; do
  echo "=== ${name} ==="
  AUTORUN_DIR="$ROOT_DIR/var/research/autorun/${name}" \
    "$ROOT_DIR/auto_research/status_autorun.sh" || true
  echo
done
```

- [ ] **Step 5: Run the focused tests again**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py -k "dense_program_files_exist or dense_start_wrappers_bind or status_dense_autorun_reads"
```

Expected:

- all selected tests pass

- [ ] **Step 6: Commit the scaffolding**

```bash
git add auto_research/program_direction_dense.md auto_research/program_reversal_dense.md auto_research/start_direction_dense.sh auto_research/start_reversal_dense.sh auto_research/status_dense_autorun.sh auto_research/README.md tests/test_research_experiment_automation.py
git commit -m "feat: add dense autoresearch program scaffolding"
```

## Task 2: Parameterize Background Loop Runtime Paths Per Dense Instance

**Files:**
- Modify: `auto_research/codex_background_loop.sh`
- Modify: `auto_research/status_autorun.sh`
- Modify: `src/pm15min/research/automation/control_plane.py`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Write the failing tests for instance-specific autorun state**

```python
def test_codex_background_loop_allows_autorun_dir_override() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert 'AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun}"' in script_text
    assert 'STATUS_PATH="$AUTORUN_DIR/codex-background.status.json"' in script_text
    assert 'LOG_PATH="$AUTORUN_DIR/codex-background.log"' in script_text


def test_status_autorun_allows_status_path_override() -> None:
    script_text = Path("auto_research/status_autorun.sh").read_text(encoding="utf-8")

    assert 'AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun}"' in script_text
    assert "build_autorun_status_report(" in script_text
    assert "status_path=" in script_text


def test_build_codex_cycle_prompt_accepts_status_path_override() -> None:
    source = Path("src/pm15min/research/automation/control_plane.py").read_text(encoding="utf-8")

    assert "def build_codex_cycle_prompt(" in source
    assert "status_path: Path | None = None" in source
    assert "build_autorun_status_report(root, log_tail_lines=5, max_incomplete_runs=5, status_path=status_path)" in source
```

- [ ] **Step 2: Run the focused tests to verify they fail**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py -k "autorun_dir_override or status_autorun_allows_status_path_override or build_codex_cycle_prompt_accepts_status_path_override"
```

Expected:

- failures because the current scripts still hardcode one runtime directory

- [ ] **Step 3: Update the background loop and status script to accept per-instance runtime dirs**

```bash
AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun}"
STATUS_PATH="${STATUS_PATH:-$AUTORUN_DIR/codex-background.status.json}"
PID_PATH="${PID_PATH:-$AUTORUN_DIR/codex-background.pid}"
LOG_PATH="${LOG_PATH:-$AUTORUN_DIR/codex-background.log}"
STOP_FLAG="${STOP_FLAG:-$AUTORUN_DIR/stop.flag}"
LAST_PROMPT_PATH="${LAST_PROMPT_PATH:-$AUTORUN_DIR/codex-last-prompt.md}"
LAST_OUTPUT_PATH="${LAST_OUTPUT_PATH:-$AUTORUN_DIR/codex-last-output.txt}"
FALLBACK_ENV_PATH="${FALLBACK_ENV_PATH:-$AUTORUN_DIR/codex-fallback.env}"
```

```python
def build_codex_cycle_prompt(
    *,
    project_root: Path,
    session_dir: Path,
    program_path: Path | None = None,
    status_path: Path | None = None,
) -> str:
    status_report = build_autorun_status_report(
        root,
        log_tail_lines=5,
        max_incomplete_runs=5,
        status_path=status_path,
    )
```

- [ ] **Step 4: Thread the instance status path through the prompt builder call**

```bash
print(
    build_codex_cycle_prompt(
        project_root=Path(sys.argv[1]),
        session_dir=Path(sys.argv[2]),
        program_path=Path(sys.argv[3]),
        status_path=Path(sys.argv[4]),
    )
)
```

Run the bash call site with:

```bash
PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$ROOT_DIR" "$SESSION_DIR" "$PROGRAM_PATH" "$STATUS_PATH"
```

- [ ] **Step 5: Run the focused tests again**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py -k "autorun_dir_override or status_autorun_allows_status_path_override or build_codex_cycle_prompt_accepts_status_path_override"
```

Expected:

- all selected tests pass

- [ ] **Step 6: Commit the runtime parameterization**

```bash
git add auto_research/codex_background_loop.sh auto_research/status_autorun.sh src/pm15min/research/automation/control_plane.py tests/test_research_experiment_automation.py
git commit -m "feat: parameterize dense autorun runtime state"
```

## Task 3: Add Track Metadata To Shared Queue And Enforce The 2+2 Split

**Files:**
- Modify: `src/pm15min/research/automation/queue_state.py`
- Modify: `auto_research/experiment_queue.py`
- Modify: `auto_research/experiment_queue_supervisor.sh`
- Test: `tests/test_research_experiment_queue.py`

- [ ] **Step 1: Write the failing queue tests for track-aware selection**

```python
def test_upsert_queue_item_keeps_one_normal_candidate_per_market_per_track(tmp_path: Path) -> None:
    root = tmp_path
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_direction_old",
            run_label="btc_direction_old",
            action="launch",
            status="queued",
            track="direction_dense",
        ),
    )
    state = upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_reversal_new",
            run_label="btc_reversal_new",
            action="launch",
            status="queued",
            track="reversal_dense",
        ),
    )

    queued = [item for item in state["items"] if item["status"] == "queued"]
    assert {item["track"] for item in queued} == {"direction_dense", "reversal_dense"}


def test_select_launchable_queue_items_respects_track_slot_caps(tmp_path: Path) -> None:
    root = tmp_path
    for market in ("btc", "eth", "sol", "xrp"):
        upsert_queue_item(
            root,
            build_queue_item(
                market=market,
                suite_name=f"{market}_direction",
                run_label=f"{market}_direction",
                action="launch",
                status="queued",
                track="direction_dense",
            ),
        )
    for market in ("btc", "eth", "sol", "xrp"):
        upsert_queue_item(
            root,
            build_queue_item(
                market=market,
                suite_name=f"{market}_reversal",
                run_label=f"{market}_reversal",
                action="launch",
                status="queued",
                track="reversal_dense",
            ),
        )

    payload = load_experiment_queue(root)
    payload["max_live_runs"] = 4
    payload["track_slot_caps"] = {"direction_dense": 2, "reversal_dense": 2}
    selected = select_launchable_queue_items(payload, max_live_runs=4, live_workers=[])

    counts = {}
    for item in selected:
        counts[item["track"]] = counts.get(item["track"], 0) + 1
    assert counts == {"direction_dense": 2, "reversal_dense": 2}
```

- [ ] **Step 2: Run the focused queue tests to verify they fail**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_queue.py -k "per_track or track_slot_caps"
```

Expected:

- failures because queue items do not yet carry track metadata or caps

- [ ] **Step 3: Extend queue items and payloads with track/session metadata**

```python
def build_queue_item(
    *,
    market: str,
    suite_name: str,
    run_label: str,
    action: str,
    status: str = "queued",
    priority: int = 100,
    reason: str = "",
    retry_count: int = 0,
    track: str | None = None,
    session_dir: str | None = None,
    program_path: str | None = None,
) -> dict[str, object]:
    return {
        "id": f"{normalized_market}:{normalized_track}:{normalized_suite}:{normalized_run}",
        "market": normalized_market,
        "track": normalized_track,
        "session_dir": normalized_session_dir,
        "program_path": normalized_program_path,
        ...
    }
```

```python
payload.setdefault("track_slot_caps", {"direction_dense": 2, "reversal_dense": 2})
```

- [ ] **Step 4: Update queue dedupe and selection rules**

```python
target_track = str(item.get("track") or "").strip().lower()
target_is_normal = target_status == "queued" and target_action in {"launch", "resume"}

if target_is_normal and entry_market == target_market and entry_track == target_track and entry_is_normal:
    continue
```

```python
track_caps = {
    str(key): int(value)
    for key, value in (payload.get("track_slot_caps") or {}).items()
}
running_by_track = _count_running_by_track(payload, live_workers=live_payload)
if track_caps.get(track) is not None and running_by_track.get(track, 0) >= track_caps[track]:
    continue
```

- [ ] **Step 5: Pass track/session/program information through the queue CLI**

```python
enqueue.add_argument("--track", required=True)
enqueue.add_argument("--session-dir", required=True)
enqueue.add_argument("--program-path", required=True)
```

```python
item = build_queue_item(
    market=args.market,
    suite_name=args.suite,
    run_label=args.run_label,
    action=args.action,
    priority=args.priority,
    reason=args.reason,
    track=args.track,
    session_dir=args.session_dir,
    program_path=args.program_path,
)
```

- [ ] **Step 6: Use per-item session metadata for bootstrap log placement**

```python
def _default_artifact_paths(root: Path, run_label: str, *, session_dir: str) -> dict[str, str]:
    session_root = Path(session_dir).resolve()
    bootstrap_dir = session_root / "bootstrap"
    queue_dir = root / "var" / "research" / "autorun" / "queue"
    ...
```

- [ ] **Step 7: Make the supervisor expose and honor the shared caps**

```bash
TRACK_SLOT_CAPS_JSON="${TRACK_SLOT_CAPS_JSON:-{\"direction_dense\":2,\"reversal_dense\":2}}"
PYTHONPATH="$ROOT_DIR/src" python3 "$ROOT_DIR/auto_research/experiment_queue.py" \
  --root "$ROOT_DIR" \
  supervise-once \
  --max-live-runs "$MAX_LIVE_RUNS" \
  --track-slot-caps-json "$TRACK_SLOT_CAPS_JSON" \
  --max-repair-attempts "$MAX_REPAIR_ATTEMPTS"
```

- [ ] **Step 8: Run the queue test module**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_queue.py
```

Expected:

- queue tests pass, including the new per-track slot cases

- [ ] **Step 9: Commit the shared queue upgrade**

```bash
git add src/pm15min/research/automation/queue_state.py auto_research/experiment_queue.py auto_research/experiment_queue_supervisor.sh tests/test_research_experiment_queue.py
git commit -m "feat: add track-aware dense queue scheduling"
```

## Task 4: Add Dense Gate Helpers And Thick-Parent Ranking

**Files:**
- Create: `src/pm15min/research/automation/dense_policy.py`
- Modify: `src/pm15min/research/automation/__init__.py`
- Test: `tests/test_research_experiment_dense_policy.py`

- [ ] **Step 1: Write the failing dense policy tests**

```python
from pm15min.research.automation.dense_policy import classify_dense_gate, prefer_dense_candidate


def test_classify_dense_gate_marks_sparse_subtarget_and_on_target() -> None:
    assert classify_dense_gate(total_trades=40, window_days=14) == "reject_sparse"
    assert classify_dense_gate(total_trades=80, window_days=14) == "subtarget"
    assert classify_dense_gate(total_trades=180, window_days=14) == "on_target"


def test_prefer_dense_candidate_beats_sparse_spike_when_roi_is_lower_but_count_is_healthier() -> None:
    sparse = {"roi_pct": 220.0, "trades": 3, "dense_gate": "reject_sparse"}
    thick = {"roi_pct": 90.0, "trades": 160, "dense_gate": "on_target"}

    assert prefer_dense_candidate(thick, sparse) is thick
```

- [ ] **Step 2: Run the dense policy tests to verify they fail**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_dense_policy.py
```

Expected:

- failure because the module does not exist yet

- [ ] **Step 3: Implement the dense gate helper module**

```python
from __future__ import annotations

from typing import Mapping, Any


def classify_dense_gate(*, total_trades: int, window_days: int) -> str:
    if total_trades < 56:
        return "reject_sparse"
    if total_trades < 140:
        return "subtarget"
    return "on_target"


def prefer_dense_candidate(left: Mapping[str, Any], right: Mapping[str, Any]) -> Mapping[str, Any]:
    gate_rank = {"reject_sparse": 0, "subtarget": 1, "on_target": 2}
    left_rank = gate_rank.get(str(left.get("dense_gate") or ""), -1)
    right_rank = gate_rank.get(str(right.get("dense_gate") or ""), -1)
    if left_rank != right_rank:
        return left if left_rank > right_rank else right
    left_trades = int(left.get("trades") or 0)
    right_trades = int(right.get("trades") or 0)
    if left_trades != right_trades:
        return left if left_trades > right_trades else right
    return left if float(left.get("roi_pct") or 0.0) >= float(right.get("roi_pct") or 0.0) else right
```

- [ ] **Step 4: Export the new helpers**

```python
from .dense_policy import classify_dense_gate, prefer_dense_candidate

__all__ = [
    ...,
    "classify_dense_gate",
    "prefer_dense_candidate",
]
```

- [ ] **Step 5: Run the dense policy tests again**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_dense_policy.py
```

Expected:

- tests pass

- [ ] **Step 6: Commit the dense policy helper**

```bash
git add src/pm15min/research/automation/dense_policy.py src/pm15min/research/automation/__init__.py tests/test_research_experiment_dense_policy.py
git commit -m "feat: add dense trade gate policy helpers"
```

## Task 5: Feed Dense Gates And Track Context Into Codex Prompts

**Files:**
- Modify: `src/pm15min/research/automation/control_plane.py`
- Modify: `tests/test_research_experiment_automation.py`
- Modify: `tests/test_research_experiment_queue.py`

- [ ] **Step 1: Write the failing prompt tests**

```python
def test_build_codex_cycle_prompt_mentions_dense_trade_gates(tmp_path: Path) -> None:
    prompt = build_codex_cycle_prompt(
        project_root=tmp_path,
        session_dir=tmp_path / "sessions" / "dense_direction",
        program_path=tmp_path / "auto_research" / "program_direction_dense.md",
        status_path=tmp_path / "var" / "research" / "autorun" / "direction_dense" / "codex-background.status.json",
    )

    assert "10-20 trades per coin per day" in prompt
    assert "140-280 trades per coin" in prompt
    assert "check count before roi" in prompt.lower()


def test_build_autorun_status_report_includes_track_for_queue_items(tmp_path: Path) -> None:
    state = upsert_queue_item(
        tmp_path,
        build_queue_item(
            market="btc",
            suite_name="btc_direction_suite",
            run_label="btc_direction_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=str(tmp_path / "sessions" / "dense_direction"),
            program_path=str(tmp_path / "auto_research" / "program_direction_dense.md"),
        ),
    )

    payload = build_autorun_status_report(tmp_path, log_tail_lines=0, max_incomplete_runs=2)
    assert payload["queue"]["items"][0]["track"] == "direction_dense"
```

- [ ] **Step 2: Run the prompt-focused tests to verify they fail**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py tests/test_research_experiment_queue.py -k "dense_trade_gates or includes_track_for_queue_items"
```

Expected:

- failures because the prompt builder does not yet emphasize dense gates

- [ ] **Step 3: Add dense-gate language to the machine decision summary**

```python
prompt_lines.extend(
    [
        "Dense trade target for this session: 10-20 trades per coin per day.",
        "Frozen-window dense target: 140-280 trades per coin per session coin.",
        "Do not promote sparse winners; check trade-count gate before ROI.",
        "A candidate below 56 trades is reject_sparse unless this cycle is only a bounded diagnostic.",
    ]
)
```

- [ ] **Step 4: Surface track metadata and current dense status in queue snapshots**

```python
lines.append(
    "- "
    + " / ".join(
        [
            f"track={item.get('track') or '?'}",
            f"market={item.get('market') or '?'}",
            f"status={item.get('status') or '?'}",
            f"suite={item.get('suite_name') or '?'}",
            f"run={item.get('run_label') or '?'}",
        ]
    )
)
```

- [ ] **Step 5: Run the prompt-focused tests again**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py tests/test_research_experiment_queue.py -k "dense_trade_gates or includes_track_for_queue_items"
```

Expected:

- selected tests pass

- [ ] **Step 6: Commit the prompt changes**

```bash
git add src/pm15min/research/automation/control_plane.py tests/test_research_experiment_automation.py tests/test_research_experiment_queue.py
git commit -m "feat: add dense trade guidance to autoresearch prompts"
```

## Task 6: Create Dense Sessions, Operator Docs, And End-To-End Verification

**Files:**
- Modify: `docs/SERVER_CORRECT_PATHS.md`
- Modify: `auto_research/README.md`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Write the failing documentation test**

```python
def test_research_readme_documents_dense_dual_track_startup() -> None:
    readme_text = Path("auto_research/README.md").read_text(encoding="utf-8")

    assert "start_direction_dense.sh" in readme_text
    assert "start_reversal_dense.sh" in readme_text
    assert "direction_dense" in readme_text
    assert "reversal_dense" in readme_text
```

- [ ] **Step 2: Run the focused doc test to verify it fails**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py -k "documents_dense_dual_track_startup"
```

Expected:

- failure because the operator docs do not cover the new dense topology

- [ ] **Step 3: Update path docs and startup instructions**

Add to `docs/SERVER_CORRECT_PATHS.md`:

```md
## Dense Dual-Track Sessions

- Direction dense session: `/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_direction_dense_autoresearch`
- Reversal dense session: `/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_reversal_dense_autoresearch`
- Direction dense runtime dir: `/home/huatai/qigang/pm15min/v2/var/research/autorun/direction_dense`
- Reversal dense runtime dir: `/home/huatai/qigang/pm15min/v2/var/research/autorun/reversal_dense`
- Shared dense queue: `/home/huatai/qigang/pm15min/v2/var/research/autorun/experiment-queue.json`
```

Add to `auto_research/README.md`:

```bash
./auto_research/start_direction_dense.sh start
./auto_research/start_reversal_dense.sh start
./auto_research/experiment_queue_supervisor.sh start
./auto_research/status_dense_autorun.sh
```

- [ ] **Step 4: Run the documentation test again**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py -k "documents_dense_dual_track_startup"
```

Expected:

- the selected test passes

- [ ] **Step 5: Run the full verification set**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py tests/test_research_experiment_queue.py tests/test_research_experiment_dense_policy.py
```

Expected:

- all tests pass

- [ ] **Step 6: Commit the documentation and verification pass**

```bash
git add docs/SERVER_CORRECT_PATHS.md auto_research/README.md tests/test_research_experiment_automation.py
git commit -m "docs: document dual dense autoresearch operations"
```

## Self-Review

### Spec Coverage

- dual-track sessions: covered by Task 1 and Task 6
- per-instance runtime state: covered by Task 2
- shared queue with `2 + 2` split: covered by Task 3
- dense gates before ROI: covered by Task 4 and Task 5
- prompt-level behavior and track parity: covered by Task 5

### Placeholder Scan

- no `TODO`, `TBD`, or “implement later” placeholders remain
- each task names exact files, tests, commands, and code snippets

### Type Consistency

- queue items use `track`, `session_dir`, and `program_path` consistently
- dense policy names are aligned across the plan:
  - `reject_sparse`
  - `subtarget`
  - `on_target`
- runtime dir names are aligned across the plan:
  - `direction_dense`
  - `reversal_dense`
