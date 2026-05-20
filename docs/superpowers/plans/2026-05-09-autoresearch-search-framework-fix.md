# Autoresearch Search Framework Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn autoresearch from a prompt-only iterative loop into a structured search controller that tracks what was tried, rejects stale-window evidence, forces real branch changes after repeated sparse failures, and keeps SOL/XRP quick-screen capacity supplied.

**Architecture:** Add a small structured layer between completed experiment artifacts and Codex prompts: canonical window audit, experiment lineage extraction, search-ledger summaries, and deterministic route-policy checks. Keep the existing runners and queue supervisor; strengthen their inputs and validation instead of replacing the experiment engine.

**Tech Stack:** Python 3.12+, pytest, existing `pm15min.research.automation` modules, JSON suite specs, repo-local queue state under `var/research/autorun`.

---

## File Structure

- Create `src/pm15min/research/automation/window_contract.py`
  - Owns the canonical train/eval window contract and parseable suite-spec audit helpers.
- Create `src/pm15min/research/automation/search_ledger.py`
  - Builds structured attempt records from completed runs, quick-screen summaries, suite specs, and custom feature-set metadata.
- Create `src/pm15min/research/automation/search_policy.py`
  - Decides when the next branch must change feature width, factor family, model family, or weighting.
- Modify `src/pm15min/research/automation/control_plane.py`
  - Uses the new window contract, ledger, and policy in the historical digest and Codex prompt.
- Modify `src/pm15min/research/automation/queue_state.py`
  - Reseeds underfilled tracks, not only fully empty tracks.
- Modify `auto_research/experiment_queue.py`
  - Stores optional research metadata on queue items and rejects clearly forbidden same-suite retries when policy requires a branch change.
- Modify `tests/test_research_experiment_automation.py`
  - Adds integration-style prompt, queue, and stale-window tests near the existing autoresearch tests.
- Add `tests/test_research_autoresearch_search_framework.py`
  - Unit tests for window contract, search ledger, and policy.
- Modify stale control files only if they are still active or still referenced by wrappers:
  - `auto_research/program.md`
  - `auto_research/program_direction_dense.md`
  - `auto_research/program_direction_dense_xrp.md`
  - `auto_research/program_reversal_dense_xrp.md`

---

### Task 1: Canonical Window Contract

**Files:**
- Create: `src/pm15min/research/automation/window_contract.py`
- Modify: `src/pm15min/research/automation/control_plane.py`
- Test: `tests/test_research_autoresearch_search_framework.py`

- [ ] **Step 1: Write failing tests for stale and canonical windows**

Add this test file:

```python
from __future__ import annotations

import json
from pathlib import Path

from pm15min.research.automation.window_contract import (
    CANONICAL_DECISION_END,
    CANONICAL_DECISION_START,
    CANONICAL_TRAIN_END,
    audit_suite_spec_windows,
    extract_suite_spec_window,
    suite_spec_uses_canonical_window,
)


def test_extract_suite_spec_window_reads_train_and_decision_bounds(tmp_path: Path) -> None:
    spec_path = tmp_path / "suite.json"
    spec_path.write_text(
        json.dumps(
            {
                "suite_name": "suite",
                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                "decision_start": "2026-04-15",
                "decision_end": "2026-04-30",
            }
        ),
        encoding="utf-8",
    )

    window = extract_suite_spec_window(spec_path)

    assert window.train_end == CANONICAL_TRAIN_END
    assert window.decision_start == CANONICAL_DECISION_START
    assert window.decision_end == CANONICAL_DECISION_END
    assert suite_spec_uses_canonical_window(spec_path)


def test_audit_suite_spec_windows_reports_stale_parseable_specs(tmp_path: Path) -> None:
    specs_dir = tmp_path / "research" / "experiments" / "suite_specs"
    specs_dir.mkdir(parents=True)
    (specs_dir / "good.json").write_text(
        json.dumps(
            {
                "suite_name": "good",
                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                "decision_start": "2026-04-15",
                "decision_end": "2026-04-30",
            }
        ),
        encoding="utf-8",
    )
    (specs_dir / "stale.json").write_text(
        json.dumps(
            {
                "suite_name": "stale",
                "window": {"start": "2025-10-27", "end": "2026-03-31"},
                "decision_start": "2026-04-01",
                "decision_end": "2026-04-23",
            }
        ),
        encoding="utf-8",
    )
    (specs_dir / "._metadata.json").write_text("not json", encoding="utf-8")

    audit = audit_suite_spec_windows(tmp_path)

    assert audit["parseable_specs"] == 2
    assert audit["canonical_specs"] == 1
    assert audit["stale_specs"] == ["stale.json"]
    assert audit["ignored_files"] == ["._metadata.json"]
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
pytest tests/test_research_autoresearch_search_framework.py -q
```

Expected: import fails because `window_contract.py` does not exist.

- [ ] **Step 3: Implement `window_contract.py`**

Create:

```python
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

CANONICAL_TRAIN_END = "2026-04-15"
CANONICAL_DECISION_START = "2026-04-15"
CANONICAL_DECISION_END = "2026-04-30"


@dataclass(frozen=True)
class SuiteWindow:
    train_end: str | None
    decision_start: str | None
    decision_end: str | None

    @property
    def is_canonical(self) -> bool:
        return (
            self.train_end == CANONICAL_TRAIN_END
            and self.decision_start == CANONICAL_DECISION_START
            and self.decision_end == CANONICAL_DECISION_END
        )


def extract_suite_spec_window(spec_path: Path) -> SuiteWindow:
    payload = _read_json_object(spec_path)
    window = payload.get("window") if isinstance(payload.get("window"), dict) else {}
    train_end = str(window.get("end") or payload.get("train_end") or "").strip() or None
    decision_start = str(payload.get("decision_start") or "").strip() or None
    decision_end = str(payload.get("decision_end") or "").strip() or None
    return SuiteWindow(
        train_end=train_end,
        decision_start=decision_start,
        decision_end=decision_end,
    )


def suite_spec_uses_canonical_window(spec_path: Path) -> bool:
    try:
        return extract_suite_spec_window(spec_path).is_canonical
    except (OSError, ValueError, json.JSONDecodeError):
        return False


def audit_suite_spec_windows(project_root: Path) -> dict[str, Any]:
    specs_dir = Path(project_root).resolve() / "research" / "experiments" / "suite_specs"
    parseable_specs = 0
    canonical_specs = 0
    stale_specs: list[str] = []
    ignored_files: list[str] = []
    if not specs_dir.exists():
        return {
            "parseable_specs": 0,
            "canonical_specs": 0,
            "stale_specs": [],
            "ignored_files": [],
        }

    for path in sorted(specs_dir.glob("*.json")):
        if path.name.startswith("._"):
            ignored_files.append(path.name)
            continue
        try:
            window = extract_suite_spec_window(path)
        except (OSError, ValueError, json.JSONDecodeError):
            ignored_files.append(path.name)
            continue
        parseable_specs += 1
        if window.is_canonical:
            canonical_specs += 1
        else:
            stale_specs.append(path.name)
    return {
        "parseable_specs": parseable_specs,
        "canonical_specs": canonical_specs,
        "stale_specs": stale_specs,
        "ignored_files": ignored_files,
    }


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"suite spec must be a JSON object: {path}")
    return payload
```

- [ ] **Step 4: Route `control_plane.py` stale-window filtering through the new helper**

In `src/pm15min/research/automation/control_plane.py`, import the canonical constants and replace hard-coded date assumptions in `_uses_post_train_decision_window` call paths with the semantic check:

```python
from pm15min.research.automation.window_contract import (
    CANONICAL_DECISION_END,
    CANONICAL_DECISION_START,
    CANONICAL_TRAIN_END,
)
```

Keep `_uses_post_train_decision_window` for older runs that only have manifests. Add one extra guard in `_experiment_run_window_payload` output consumers:

```python
def _uses_current_autoresearch_window(payload: dict[str, object]) -> bool:
    decision_start = str(payload.get("decision_start") or "").strip()
    decision_end = str(payload.get("decision_end") or "").strip()
    train_end = str(payload.get("train_end") or "").strip()
    if not decision_start or not decision_end or not train_end:
        return _uses_post_train_decision_window(payload)
    return (
        train_end == CANONICAL_TRAIN_END
        and decision_start == CANONICAL_DECISION_START
        and decision_end == CANONICAL_DECISION_END
    )
```

Replace filtering checks in:

```python
find_recent_completed_experiment_runs
find_latest_completed_experiment_runs_by_market
find_recent_completed_experiment_runs_by_market
find_best_completed_quick_screen_runs_by_market
```

from:

```python
if not _uses_post_train_decision_window(payload):
    continue
```

to:

```python
if not _uses_current_autoresearch_window(payload):
    continue
```

- [ ] **Step 5: Run tests**

Run:

```bash
pytest tests/test_research_autoresearch_search_framework.py tests/test_research_experiment_automation.py::test_midprice_prompt_ignores_completed_runs_from_stale_decision_window -q
```

Expected: all selected tests pass.

---

### Task 2: Structured Experiment Lineage

**Files:**
- Create: `src/pm15min/research/automation/search_ledger.py`
- Modify: `src/pm15min/research/automation/control_plane.py`
- Test: `tests/test_research_autoresearch_search_framework.py`

- [ ] **Step 1: Add tests for feature width, model family, and bottleneck extraction**

Append:

```python
from pm15min.research.automation.search_ledger import build_attempt_record


def test_build_attempt_record_extracts_feature_width_model_and_bottleneck(tmp_path: Path) -> None:
    root = tmp_path
    specs_dir = root / "research" / "experiments" / "suite_specs"
    specs_dir.mkdir(parents=True)
    (root / "research" / "experiments").mkdir(parents=True, exist_ok=True)
    (root / "research" / "experiments" / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_xrp_56_v1": {
                    "market": "xrp",
                    "width": 56,
                    "columns": ["ret_from_cycle_open", "ret_from_strike", "move_z"],
                }
            }
        ),
        encoding="utf-8",
    )
    (specs_dir / "suite_xrp.json").write_text(
        json.dumps(
            {
                "suite_name": "suite_xrp",
                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                "decision_start": "2026-04-15",
                "decision_end": "2026-04-30",
                "markets": {
                    "xrp": {
                        "groups": {
                            "direction": {
                                "runs": [
                                    {
                                        "run_name": "r1",
                                        "target": "direction",
                                        "model_family": "catboost",
                                        "feature_set_variants": [
                                            {"label": "frontier", "feature_set": "focus_xrp_56_v1"}
                                        ],
                                    }
                                ]
                            }
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    run_payload = {
        "suite_name": "suite_xrp",
        "run_label": "run_xrp",
        "market": "xrp",
        "decision_start": "2026-04-15",
        "decision_end": "2026-04-30",
        "train_end": "2026-04-15",
        "top_case": {
            "trades": 18,
            "roi_pct": -12.5,
            "feature_set": "focus_xrp_56_v1",
            "density_bottleneck": {"primary_bottleneck": "probability_gate"},
        },
    }

    attempt = build_attempt_record(root, run_payload, track="direction_dense")

    assert attempt["market"] == "xrp"
    assert attempt["track"] == "direction_dense"
    assert attempt["feature_sets"] == ["focus_xrp_56_v1"]
    assert attempt["widths"] == [56]
    assert attempt["model_families"] == ["catboost"]
    assert attempt["primary_bottleneck"] == "probability_gate"
    assert attempt["outcome"] == "sparse"
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
pytest tests/test_research_autoresearch_search_framework.py::test_build_attempt_record_extracts_feature_width_model_and_bottleneck -q
```

Expected: import fails because `search_ledger.py` does not exist.

- [ ] **Step 3: Implement `search_ledger.py`**

Create:

```python
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def build_attempt_record(
    project_root: Path,
    run_payload: dict[str, Any],
    *,
    track: str | None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    suite_name = str(run_payload.get("suite_name") or "").strip()
    top_case = run_payload.get("top_case") if isinstance(run_payload.get("top_case"), dict) else {}
    suite_payload = _read_suite_spec(root, suite_name)
    feature_sets = _extract_feature_sets(suite_payload)
    top_feature_set = str(top_case.get("feature_set") or "").strip()
    if top_feature_set and top_feature_set not in feature_sets:
        feature_sets.insert(0, top_feature_set)
    registry = _read_custom_feature_sets(root)
    widths = sorted(
        {
            int(registry[name]["width"])
            for name in feature_sets
            if name in registry and _is_int_like(registry[name].get("width"))
        }
    )
    model_families = sorted(set(_extract_model_families(suite_payload)))
    density = top_case.get("density_bottleneck") if isinstance(top_case.get("density_bottleneck"), dict) else {}
    trades = _int_or_none(top_case.get("trades") or top_case.get("trade_rows"))
    captures = _int_or_none(top_case.get("profitable_pool_capture_rows"))
    outcome = _classify_outcome(trades=trades, captures=captures)
    return {
        "market": str(run_payload.get("market") or "").strip().lower(),
        "track": str(track or "").strip().lower(),
        "suite_name": suite_name,
        "run_label": str(run_payload.get("run_label") or "").strip(),
        "train_end": str(run_payload.get("train_end") or "").strip(),
        "decision_start": str(run_payload.get("decision_start") or "").strip(),
        "decision_end": str(run_payload.get("decision_end") or "").strip(),
        "feature_sets": feature_sets,
        "widths": widths,
        "model_families": model_families,
        "trades": trades,
        "captures": captures,
        "primary_bottleneck": str(density.get("primary_bottleneck") or "").strip(),
        "recommended_route": str(density.get("recommended_route") or "").strip(),
        "outcome": outcome,
    }


def format_attempt_record_line(attempt: dict[str, Any]) -> str:
    return (
        f"{attempt.get('market')}: suite={attempt.get('suite_name')} / "
        f"run={attempt.get('run_label')} / outcome={attempt.get('outcome')} / "
        f"trades={attempt.get('trades')} / captures={attempt.get('captures')} / "
        f"widths={','.join(str(v) for v in attempt.get('widths') or []) or 'unknown'} / "
        f"models={','.join(str(v) for v in attempt.get('model_families') or []) or 'unknown'} / "
        f"features={','.join(str(v) for v in (attempt.get('feature_sets') or [])[:3]) or 'unknown'} / "
        f"bottleneck={attempt.get('primary_bottleneck') or 'unknown'}"
    )


def _read_suite_spec(root: Path, suite_name: str) -> dict[str, Any]:
    path = root / "research" / "experiments" / "suite_specs" / f"{suite_name}.json"
    if not suite_name or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _read_custom_feature_sets(root: Path) -> dict[str, dict[str, Any]]:
    path = root / "research" / "experiments" / "custom_feature_sets.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {str(k): v for k, v in payload.items() if isinstance(v, dict)} if isinstance(payload, dict) else {}


def _extract_feature_sets(payload: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            token = str(value.get("feature_set") or "").strip()
            if token and token not in seen:
                seen.add(token)
                out.append(token)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return out


def _extract_model_families(payload: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            token = str(value.get("model_family") or "").strip()
            if token and token not in seen:
                seen.add(token)
                out.append(token)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return out or ["deep_otm"]


def _classify_outcome(*, trades: int | None, captures: int | None) -> str:
    if trades is None:
        return "unknown"
    if trades < 56:
        return "sparse"
    if captures is not None and captures <= 0:
        return "no_capture"
    return "candidate"


def _is_int_like(value: Any) -> bool:
    try:
        int(value)
        return True
    except (TypeError, ValueError):
        return False


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
```

- [ ] **Step 4: Add ledger lines to the prompt digest**

In `control_plane.py`, import:

```python
from pm15min.research.automation.search_ledger import (
    build_attempt_record,
    format_attempt_record_line,
)
```

In `_format_historical_decision_digest`, after each market summary line, append up to three attempt lines:

```python
recent_attempts = [
    build_attempt_record(project_root, payload, track=context.get("track"))
    for payload in list(recent_by_market.get(market) or [])[:3]
]
for attempt in recent_attempts:
    lines.append("  attempt: " + format_attempt_record_line(attempt))
```

- [ ] **Step 5: Run tests**

Run:

```bash
pytest tests/test_research_autoresearch_search_framework.py::test_build_attempt_record_extracts_feature_width_model_and_bottleneck tests/test_research_experiment_automation.py::test_build_codex_cycle_prompt_includes_best_historical_quick_screen_digest -q
```

Expected: all selected tests pass, and the existing digest test still sees the old best quick-screen line.

---

### Task 3: Deterministic Search Policy

**Files:**
- Create: `src/pm15min/research/automation/search_policy.py`
- Modify: `src/pm15min/research/automation/control_plane.py`
- Modify: `auto_research/experiment_queue.py`
- Test: `tests/test_research_autoresearch_search_framework.py`

- [ ] **Step 1: Add tests for forced lever changes**

Append:

```python
from pm15min.research.automation.search_policy import choose_required_next_lever


def test_choose_required_next_lever_forces_width_after_same_width_sparse_loop() -> None:
    attempts = [
        {"outcome": "sparse", "widths": [40], "model_families": ["deep_otm"], "primary_bottleneck": "low_trade_density"},
        {"outcome": "sparse", "widths": [40], "model_families": ["deep_otm"], "primary_bottleneck": "low_trade_density"},
        {"outcome": "sparse", "widths": [40], "model_families": ["deep_otm"], "primary_bottleneck": "low_trade_density"},
    ]

    decision = choose_required_next_lever(attempts)

    assert decision["required_lever"] == "feature_width"
    assert decision["forbid_same_width"] is True
    assert decision["forbid_same_model"] is False


def test_choose_required_next_lever_forces_model_after_probability_gate_loop() -> None:
    attempts = [
        {"outcome": "sparse", "widths": [56], "model_families": ["deep_otm"], "primary_bottleneck": "probability_gate"},
        {"outcome": "sparse", "widths": [56], "model_families": ["deep_otm"], "primary_bottleneck": "probability_gate"},
        {"outcome": "sparse", "widths": [56], "model_families": ["deep_otm"], "primary_bottleneck": "probability_gate"},
    ]

    decision = choose_required_next_lever(attempts)

    assert decision["required_lever"] == "model_family"
    assert decision["forbid_same_width"] is False
    assert decision["forbid_same_model"] is True
```

- [ ] **Step 2: Run the failing policy tests**

Run:

```bash
pytest tests/test_research_autoresearch_search_framework.py::test_choose_required_next_lever_forces_width_after_same_width_sparse_loop tests/test_research_autoresearch_search_framework.py::test_choose_required_next_lever_forces_model_after_probability_gate_loop -q
```

Expected: import fails because `search_policy.py` does not exist.

- [ ] **Step 3: Implement `search_policy.py`**

Create:

```python
from __future__ import annotations

from typing import Any


def choose_required_next_lever(attempts: list[dict[str, Any]]) -> dict[str, Any]:
    recent = [dict(item) for item in attempts[:3] if isinstance(item, dict)]
    sparse = [item for item in recent if str(item.get("outcome") or "") in {"sparse", "no_capture"}]
    if len(sparse) < 3:
        return _decision("none", "not enough repeated sparse attempts")

    widths = {_first_value(item.get("widths")) for item in sparse}
    models = {_first_value(item.get("model_families")) for item in sparse}
    bottlenecks = {str(item.get("primary_bottleneck") or "").strip() for item in sparse}

    if bottlenecks == {"probability_gate"} and len(models) == 1:
        return _decision("model_family", "three sparse probability-gate attempts used the same model", forbid_same_model=True)
    if len(widths) == 1:
        return _decision("feature_width", "three sparse attempts used the same width", forbid_same_width=True)
    if len(models) == 1:
        return _decision("factor_family", "three sparse attempts used the same model and no width-only move is obvious")
    return _decision("factor_family", "three sparse attempts did not produce a frontier")


def format_policy_decision_line(market: str, decision: dict[str, Any]) -> str:
    return (
        f"{market}: required_next_lever={decision.get('required_lever')} / "
        f"reason={decision.get('reason')} / "
        f"forbid_same_width={int(bool(decision.get('forbid_same_width')))} / "
        f"forbid_same_model={int(bool(decision.get('forbid_same_model')))}"
    )


def _decision(
    required_lever: str,
    reason: str,
    *,
    forbid_same_width: bool = False,
    forbid_same_model: bool = False,
) -> dict[str, Any]:
    return {
        "required_lever": required_lever,
        "reason": reason,
        "forbid_same_width": bool(forbid_same_width),
        "forbid_same_model": bool(forbid_same_model),
    }


def _first_value(value: Any) -> str:
    if isinstance(value, list) and value:
        return str(value[0]).strip()
    return str(value or "").strip()
```

- [ ] **Step 4: Add required lever lines to prompt**

In `control_plane.py`, import:

```python
from pm15min.research.automation.search_policy import (
    choose_required_next_lever,
    format_policy_decision_line,
)
```

In `_format_historical_decision_digest`, after building `recent_attempts`, add:

```python
policy_decision = choose_required_next_lever(recent_attempts)
if policy_decision.get("required_lever") != "none":
    lines.append("  policy: " + format_policy_decision_line(market, policy_decision))
```

- [ ] **Step 5: Add queue metadata arguments**

In `auto_research/experiment_queue.py`, add optional enqueue args:

```python
enqueue.add_argument("--primary-lever", default="")
enqueue.add_argument("--feature-width", default="")
enqueue.add_argument("--model-family", default="")
enqueue.add_argument("--feature-set", default="")
```

When building the item, include:

```python
research_meta = {
    "primary_lever": args.primary_lever,
    "feature_width": args.feature_width,
    "model_family": args.model_family,
    "feature_set": args.feature_set,
}
item["research_meta"] = {k: v for k, v in research_meta.items() if str(v or "").strip()}
```

- [ ] **Step 6: Run policy and prompt tests**

Run:

```bash
pytest tests/test_research_autoresearch_search_framework.py tests/test_research_experiment_automation.py::test_midprice_prompt_forces_branch_change_after_repeated_sparse_same_suite -q
```

Expected: all selected tests pass.

---

### Task 4: Underfilled Queue Reseed

**Files:**
- Modify: `src/pm15min/research/automation/queue_state.py`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Run the existing failing test**

Run:

```bash
pytest tests/test_research_experiment_automation.py::test_reseed_empty_tracks_from_recent_done_refills_underfilled_track_markets -q
```

Expected before fix: fail because reseed only happens when the entire track is empty.

- [ ] **Step 2: Modify `reseed_empty_tracks_from_recent_done`**

In `src/pm15min/research/automation/queue_state.py`, replace:

```python
fully_empty_track = live_track_counts.get(track, 0) <= 0 and pending_track_counts.get(track, 0) <= 0
if not fully_empty_track:
    continue
```

with:

```python
_ = live_track_counts
```

The surrounding code already computes:

```python
track_usage = pending_track_counts.get(track, 0)
if track_usage >= track_cap:
    continue
refill_gap = max(0, track_cap - track_usage)
```

That is the desired condition: refill whenever the track is under its cap, while `_recent_done_reseed_candidates` avoids occupied markets.

- [ ] **Step 3: Run the targeted queue test**

Run:

```bash
pytest tests/test_research_experiment_automation.py::test_reseed_empty_tracks_from_recent_done_refills_underfilled_track_markets -q
```

Expected: pass.

- [ ] **Step 4: Run adjacent queue tests**

Run:

```bash
pytest tests/test_research_experiment_automation.py::test_experiment_queue_launcher_has_bounded_subprocess_timeout tests/test_research_experiment_automation.py::test_experiment_queue_supervisor_defaults_to_quick_screen_launch_mode tests/test_research_experiment_automation.py::test_build_codex_cycle_prompt_reports_queue_capacity_from_queue_state -q
```

Expected: all selected tests pass.

---

### Task 5: Prompt Becomes a Search Brief, Not a Memory Dump

**Files:**
- Modify: `src/pm15min/research/automation/control_plane.py`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Add a prompt test for structured search brief**

Append to `tests/test_research_experiment_automation.py`:

```python
def test_codex_prompt_includes_structured_search_policy_not_only_free_text(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_midprice_direction_btc_autoresearch"
    session_dir.mkdir(parents=True)
    program_dir = root / "auto_research"
    program_dir.mkdir()
    program_path = program_dir / "program_direction_midprice_btc.md"
    program_path.write_text(
        "\n".join(
            [
                "# BTC",
                "- coin: `btc`",
                "- target fixed to `direction`",
                "- suite seed: `btc_suite`",
                "Run full formal experiments only; do not use the shared SOL/XRP quick-screen queue.",
            ]
        ),
        encoding="utf-8",
    )
    specs_dir = root / "research" / "experiments" / "suite_specs"
    specs_dir.mkdir(parents=True)
    (specs_dir / "btc_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "btc_suite",
                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                "decision_start": "2026-04-15",
                "decision_end": "2026-04-30",
                "markets": {
                    "btc": {
                        "groups": {
                            "direction": {
                                "runs": [
                                    {
                                        "target": "direction",
                                        "model_family": "deep_otm",
                                        "feature_set_variants": [{"label": "a", "feature_set": "focus_btc_40_v1"}],
                                    }
                                ]
                            }
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    experiments_dir = root / "research" / "experiments"
    experiments_dir.mkdir(parents=True, exist_ok=True)
    (experiments_dir / "custom_feature_sets.json").write_text(
        json.dumps({"focus_btc_40_v1": {"market": "btc", "width": 40, "columns": ["ret_from_cycle_open"]}}),
        encoding="utf-8",
    )
    for idx in range(3):
        run_dir = root / "research" / "experiments" / "runs" / "suite=btc_suite" / f"run=run{idx}"
        run_dir.mkdir(parents=True)
        (run_dir / "summary.json").write_text(
            json.dumps({"suite_name": "btc_suite", "run_label": f"run{idx}", "cases": 1, "completed_cases": 1, "failed_cases": 0}),
            encoding="utf-8",
        )
        (run_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "spec": {
                        "markets": [
                            {
                                "market": "btc",
                                "decision_start": "2026-04-15",
                                "decision_end": "2026-04-30",
                                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                            }
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )
        with (run_dir / "leaderboard.csv").open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["market", "target", "feature_set", "trades", "roi_pct"])
            writer.writeheader()
            writer.writerow({"market": "btc", "target": "direction", "feature_set": "focus_btc_40_v1", "trades": "5", "roi_pct": "-1"})

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "attempt: btc: suite=btc_suite" in prompt
    assert "policy: btc: required_next_lever=feature_width" in prompt
    assert "do not launch another same-width" in prompt.lower() or "required_next_lever=feature_width" in prompt
```

- [ ] **Step 2: Run the failing prompt test**

Run:

```bash
pytest tests/test_research_experiment_automation.py::test_codex_prompt_includes_structured_search_policy_not_only_free_text -q
```

Expected before Task 2/3 integration is complete: fail because structured attempt and policy lines are missing.

- [ ] **Step 3: Keep search brief above operational snapshots**

In `build_codex_cycle_prompt`, keep this order:

```python
"Historical decision digest already collected for you:",
*(history_digest_lines or ["- no historical decision digest available"]),
"",
"Machine decision summary already collected for you:",
*(decision_summary_lines or ["- no machine decision summary available"]),
```

Do not move queue or worker snapshots above history. The search decision must see what has failed before it sees available capacity.

- [ ] **Step 4: Run prompt tests**

Run:

```bash
pytest tests/test_research_experiment_automation.py::test_codex_prompt_includes_structured_search_policy_not_only_free_text tests/test_research_experiment_automation.py::test_midprice_prompt_forces_branch_change_after_repeated_sparse_same_suite tests/test_research_experiment_automation.py::test_build_codex_cycle_prompt_includes_best_historical_quick_screen_digest -q
```

Expected: all selected tests pass.

---

### Task 6: Clean Active Control Files and Add Rollout Verification

**Files:**
- Modify: stale `auto_research/program*.md` files that are still reachable from wrappers.
- Modify: `auto_research/README.md`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Add a regression test for active control files**

Append:

```python
def test_active_autoresearch_programs_do_not_reference_stale_april_windows() -> None:
    active_programs = [
        Path("auto_research/program_direction_dense_sol_xrp.md"),
        Path("auto_research/program_reversal_dense_sol_xrp.md"),
        Path("auto_research/program_direction_midprice_btc.md"),
        Path("auto_research/program_direction_midprice_eth.md"),
    ]
    for path in active_programs:
        text = path.read_text(encoding="utf-8")
        assert "2026-04-15" in text
        assert "2026-04-30" in text
        assert "2026-04-01` through `2026-04-23" not in text
        assert "2026-04-01 through 2026-04-23" not in text
```

- [ ] **Step 2: Run the active-program test**

Run:

```bash
pytest tests/test_research_experiment_automation.py::test_active_autoresearch_programs_do_not_reference_stale_april_windows -q
```

Expected: pass for the four active wrappers. If it fails, update the listed active program files only.

- [ ] **Step 3: Mark older program files as legacy or update their windows**

Inspect wrappers:

```bash
rg -n "program_direction_dense\\.md|program_direction_dense_xrp\\.md|program_reversal_dense_xrp\\.md|program\\.md" auto_research/*.sh
```

If a wrapper still points at one of the older files, update that program file to the canonical window:

```text
training window ends at `2026-04-15`
decision / backtest window is `2026-04-15` through `2026-04-30`
```

If no active wrapper points at it, add this line near the top:

```text
Legacy control file: do not use for the current 2026-04-15 through 2026-04-30 autoresearch lines.
```

- [ ] **Step 4: Document the new framework behavior**

In `auto_research/README.md`, add a short section:

```markdown
## Search Discipline

Autoresearch now treats every completed run as a structured attempt. The prompt
receives the run window, feature-set width, model family, top-case outcome, and
dominant bottleneck before it sees queue capacity. This prevents stale-window
results and repeated same-suite sparse runs from steering the next cycle.

When three recent attempts are sparse, the search policy can require the next
branch to change feature width, factor family, model family, or weighting. Queue
refill should keep underfilled SOL/XRP quick-screen tracks supplied instead of
waiting until a track is completely empty.
```

- [ ] **Step 5: Run local verification**

Run:

```bash
pytest tests/test_research_autoresearch_search_framework.py tests/test_research_experiment_automation.py::test_reseed_empty_tracks_from_recent_done_refills_underfilled_track_markets tests/test_research_experiment_automation.py::test_codex_prompt_includes_structured_search_policy_not_only_free_text tests/test_research_experiment_automation.py::test_midprice_prompt_ignores_completed_runs_from_stale_decision_window tests/test_research_experiment_automation.py::test_midprice_prompt_forces_branch_change_after_repeated_sparse_same_suite -q
bash -n auto_research/run_one_experiment.sh auto_research/experiment_queue_supervisor.sh auto_research/codex_background_loop.sh
python -m py_compile src/pm15min/research/automation/control_plane.py src/pm15min/research/automation/window_contract.py src/pm15min/research/automation/search_ledger.py src/pm15min/research/automation/search_policy.py
```

Expected: all commands exit 0.

- [ ] **Step 6: Remote rollout only after local verification**

When SSH is available:

```bash
scripts/maintenance/compare_hashes_ht66.sh --upload-rsync
scripts/maintenance/compare_hashes_ht66.sh
```

Expected:

```text
diff_files=0
```

Restart control loops without killing orderbook:

```bash
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && auto_research/start_direction_midprice_btc.sh restart'
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && auto_research/start_direction_midprice_eth.sh restart'
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && auto_research/start_direction_dense_sol_xrp.sh restart'
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && auto_research/start_reversal_dense_sol_xrp.sh restart'
```

Verify prompt pickup:

```bash
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && grep -R -n -E "attempt:|policy:|required_next_lever|2026-04-15 through 2026-04-30" var/research/autorun/*/codex-last-prompt.md'
```

Expected: each active prompt contains the canonical window and at least one structured attempt or policy line when completed runs exist.

---

## Self-Review

- Spec coverage:
  - Result pollution is covered by Task 1 and Task 6.
  - Missing structured experiment memory is covered by Task 2 and Task 5.
  - Repeated local same-family search is covered by Task 3 and Task 5.
  - Underfilled quick-screen capacity is covered by Task 4.
  - Remote rollout and orderbook-safe restart are covered by Task 6.
- Placeholder scan:
  - No `TBD`, `TODO`, or unspecified test steps remain.
- Type consistency:
  - `build_attempt_record`, `format_attempt_record_line`, `choose_required_next_lever`, and `format_policy_decision_line` are defined before prompt integration uses them.
