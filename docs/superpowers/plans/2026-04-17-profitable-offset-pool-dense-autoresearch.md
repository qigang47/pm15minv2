# Profitable Offset Pool Dense Autoresearch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a strict profitable-offset-pool fast-screen metric that counts only real tradeable winner-side windows under the `<= 0.30` entry rule, then expose that policy directly to dense autoresearch prompts and program files.

**Architecture:** Extend the existing quick-screen summary instead of building a second replay path. The quick-screen layer will compute profitable-pool counts and coverage ratios from the same replay decisions it already builds, while the control-plane prompt and dense `program.md` files will explicitly tell Codex to optimize pool coverage before spending full formal capacity.

**Tech Stack:** Python 3.11, pandas, pytest, repo-local dense autoresearch markdown prompts

---

### Task 1: Lock the strict profitable-pool semantics in tests

**Files:**
- Modify: `tests/test_research_quick_screen.py`
- Modify: `tests/test_research_experiment_dense_policy.py`
- Modify: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Write the failing quick-screen summary test**

```python
def test_build_quick_screen_summary_reports_profitable_pool_coverage() -> None:
    decisions = pd.DataFrame(
        [
            {
                "resolved": True,
                "winner_side": "UP",
                "quote_status": "ok",
                "quote_up_ask": 0.15,
                "quote_down_ask": 0.82,
                "predicted_side": "UP",
                "policy_action": "trade",
            },
            {
                "resolved": True,
                "winner_side": "DOWN",
                "quote_status": "ok",
                "quote_up_ask": 0.77,
                "quote_down_ask": 0.20,
                "predicted_side": "DOWN",
                "policy_action": "reject",
            },
        ]
    )

    summary = build_quick_screen_summary(decisions, entry_price_min=0.01, entry_price_max=0.30)

    assert summary["profitable_pool_rows"] == 2
    assert summary["profitable_pool_capture_rows"] == 1
    assert summary["profitable_pool_coverage_ratio"] == pytest.approx(0.5)
```

- [ ] **Step 2: Run the quick-screen tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_research_quick_screen.py`
Expected: FAIL because the new profitable-pool summary fields do not exist yet.

- [ ] **Step 3: Write the failing dense prompt test**

```python
def test_build_codex_cycle_prompt_mentions_profitable_offset_pool_gate(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "dense_direction"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_dense.md"
    program_path.write_text(
        "\n".join(
            [
                "# dense direction program",
                "- target fixed to `direction`",
                "- profitable offset pool is coin-level and shared by both dense tracks",
                "- profitable offset pool window: `2026-04-01` through `2026-04-15`, `2usd`",
                "- only final tradeable winner-side entries at `<= 0.30` count as pool captures",
                "- prefer profitable-pool coverage before formal ROI comparisons",
            ]
        ),
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "profitable-offset-pool" in prompt.lower()
    assert "<= 0.30" in prompt or "<= 0.3" in prompt
    assert "coverage before formal roi" in prompt.lower()
```

- [ ] **Step 4: Run the automation tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py -k "dense_trade_gates or profitable_offset_pool"`
Expected: FAIL because the prompt guidance does not mention the profitable-offset-pool gate yet.

### Task 2: Implement profitable-pool coverage metrics in quick-screen

**Files:**
- Modify: `src/pm15min/research/automation/quick_screen.py`
- Modify: `src/pm15min/research/automation/__init__.py`
- Test: `tests/test_research_quick_screen.py`

- [ ] **Step 1: Add a helper that classifies pool membership and capture state**

```python
def build_profitable_offset_pool_frame(
    decisions: pd.DataFrame,
    *,
    entry_price_min: float | None,
    entry_price_max: float | None,
) -> pd.DataFrame:
    ...
```

The frame should include:

```python
[
    "decision_ts",
    "cycle_start_ts",
    "cycle_end_ts",
    "offset",
    "winner_side",
    "predicted_side",
    "policy_action",
    "policy_reason",
    "winner_entry_price",
    "profitable_pool_window",
    "profitable_pool_correct_side",
    "profitable_pool_capture",
    "profitable_pool_status",
]
```

- [ ] **Step 2: Update `build_quick_screen_summary()` to publish the new metrics**

Add these summary keys while keeping the existing keys for compatibility:

```python
{
    "profitable_pool_rows": ...,
    "profitable_pool_correct_side_rows": ...,
    "profitable_pool_capture_rows": ...,
    "profitable_pool_coverage_ratio": ...,
    "profitable_pool_status_counts": ...,
}
```

- [ ] **Step 3: Update `quick_screen_rank_tuple()` to rank by pool coverage first**

```python
def quick_screen_rank_tuple(summary: dict[str, object]) -> tuple[float, int, int, int, int]:
    return (
        float(summary.get("profitable_pool_coverage_ratio") or 0.0),
        int(summary.get("profitable_pool_capture_rows") or 0),
        int(summary.get("profitable_pool_correct_side_rows") or 0),
        int(summary.get("trade_rows") or 0),
        int(summary.get("profitable_pool_rows") or 0),
    )
```

- [ ] **Step 4: Export the new helper for reuse**

```python
from .quick_screen import build_profitable_offset_pool_frame
```

- [ ] **Step 5: Run the quick-screen tests and make them pass**

Run: `PYTHONPATH=src pytest -q tests/test_research_quick_screen.py`
Expected: PASS

### Task 3: Surface the new metrics to automation summaries and dense prompting

**Files:**
- Modify: `src/pm15min/research/automation/control_plane.py`
- Modify: `src/pm15min/research/automation/dense_policy.py`
- Test: `tests/test_research_experiment_automation.py`
- Test: `tests/test_research_experiment_dense_policy.py`

- [ ] **Step 1: Extend quick-screen top-case parsing**

Update `_read_quick_screen_top_case()` so leaderboard rows can surface:

```python
{
    "profitable_pool_rows": _optional_int(first_row.get("profitable_pool_rows")),
    "profitable_pool_capture_rows": _optional_int(first_row.get("profitable_pool_capture_rows")),
    "profitable_pool_coverage_ratio": _optional_float(first_row.get("profitable_pool_coverage_ratio")),
}
```

- [ ] **Step 2: Teach dense prompt guidance to mention the pool-first gate**

When the program file contains the profitable-pool language, append guidance like:

```python
[
    "Run the profitable-offset-pool fast-screen before spending a full formal slot.",
    "The profitable-offset pool is coin-level, shared by both dense tracks, and fixed to 2026-04-01 through 2026-04-15 at 2usd.",
    "Count a pool capture only when the candidate reaches a final tradeable winner-side entry at <= 0.30.",
    "Prefer higher profitable-pool coverage before formal ROI comparisons.",
]
```

- [ ] **Step 3: Add a dense-policy helper for strict pool preference**

Add:

```python
def prefer_dense_screen_candidate(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
) -> Mapping[str, Any]:
    ...
```

It should prefer:

```python
coverage_ratio -> capture_rows -> correct_side_rows -> trade_rows
```

- [ ] **Step 4: Export and test the new dense-policy helper**

Run: `PYTHONPATH=src pytest -q tests/test_research_experiment_dense_policy.py tests/test_research_experiment_automation.py`
Expected: PASS

### Task 4: Update the dense program files to encode the pool-first strategy

**Files:**
- Modify: `auto_research/program_direction_dense.md`
- Modify: `auto_research/program_reversal_dense.md`
- Test: `tests/test_research_experiment_automation.py`

- [ ] **Step 1: Add a dedicated profitable-offset-pool section to both dense program files**

The section should say:

```md
## Profitable Offset Pool Gate

- profitable offset pool is coin-level and shared by both dense tracks
- profitable offset pool window is `2026-04-01` through `2026-04-15` at `2usd`
- one `offset` equals one exact window
- count a capture only when the candidate reaches a final tradeable winner-side entry at `<= 0.30`
- target about `70%` profitable-pool coverage before spending a full formal slot
- formal frontier decisions still require full orderbook validation
```

- [ ] **Step 2: Extend the existing program-file regression test**

Assert both files contain:

```python
assert "Profitable Offset Pool Gate" in direction_text
assert "shared by both dense tracks" in direction_text
assert "2026-04-01" in direction_text and "2026-04-15" in direction_text
assert "<= 0.30" in direction_text or "`<= 0.30`" in direction_text
assert "70%" in direction_text
```

- [ ] **Step 3: Run the focused automation tests**

Run: `PYTHONPATH=src pytest -q tests/test_research_experiment_automation.py -k "dense or profitable"`
Expected: PASS

### Task 5: Final verification

**Files:**
- Modify: `docs/superpowers/specs/2026-04-17-profitable-offset-pool-dense-autoresearch-design.md` only if implementation wording drifted

- [ ] **Step 1: Run the full focused verification set**

Run: `PYTHONPATH=src pytest -q tests/test_research_quick_screen.py tests/test_research_experiment_dense_policy.py tests/test_research_experiment_automation.py`
Expected: PASS with all targeted tests green.

- [ ] **Step 2: If prompt strings changed materially, align the design doc wording**

Only adjust wording if the implementation made the user-approved phrasing more precise.
