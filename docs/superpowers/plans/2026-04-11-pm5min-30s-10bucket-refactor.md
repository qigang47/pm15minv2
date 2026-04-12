# PM5Min 30s 10-Bucket Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the 5-minute path so it uses 30-second buckets end-to-end, producing 10 buckets per 5-minute cycle while keeping shared data/research/live logic coherent.

**Architecture:** Keep `pm5min` as a thin entry package, but push all cycle-sensitive behavior behind one shared contract so research, live, and replay all read the same bucket rules. Remove heuristic cycle guessing from parity/guard paths where possible and propagate cycle explicitly from upstream config or replay data. The 5-minute profile in this plan uses six late-cycle buckets `(4, 5, 6, 7, 8, 9)`, which preserves the current “trade in the last 3 minutes” behavior while switching the internal clock to 30-second buckets.

**Tech Stack:** Python 3.11+, pandas, argparse CLI wrappers, pytest, existing `pm15min` shared stack with `pm5min` thin entry wrapper.

**Assumption Used In This Plan:** `deep_otm_5m` and `deep_otm_5m_baseline` move from minute offsets `(2, 3, 4)` to 30-second bucket offsets `(4, 5, 6, 7, 8, 9)`. If you want a different 5-minute trade window, only the profile/config task changes; the bucket infrastructure tasks remain the same.

---

## File Structure

- Modify: `src/pm15min/core/cycle_contracts.py`
  - Extend the cycle contract so cycle-sensitive logic can talk in bucket units, not only minute units.
- Modify: `src/pm15min/research/features/cycle.py`
  - Replace minute-based intra-cycle offsets with contract-driven bucket offsets.
- Modify: `src/pm15min/live/profiles/catalog.py`
  - Move the 5-minute profiles to the 30-second bucket offsets and matching tail-space settings.
- Modify: `src/pm15min/live/regime/controller.py`
  - Remove ambiguous “ret_5m column means 5m” guessing.
- Modify: `src/pm15min/research/backtests/guard_parity.py`
  - Require explicit/shared cycle context for replay guard parity instead of silent guessing.
- Modify: `src/pm15min/research/backtests/regime_parity.py`
  - Pass explicit cycle into replay/live-parity guard checks.
- Modify: `src/pm15min/research/backtests/live_state_parity.py`
  - Keep replay/live parity on the same explicit cycle contract.
- Modify: `src/pm15min/research/features/builders.py`
  - Keep live required feature columns aligned with the contract-driven regime return columns.
- Modify: `src/pm15min/live/guards/features.py`
  - Read the contract-selected long-return column for directional guard checks.
- Modify: `src/pm15min/live/guards/__init__.py`
  - Thread cycle through guard evaluation.
- Modify: `src/pm15min/live/signal/decision.py`
  - Pass cycle into guard evaluation.
- Modify: `src/pm15min/live/signal/scoring_bundle.py`
  - Keep live feature requests aligned with the selected cycle.
- Modify: `src/pm15min/data/pipelines/source_ingest.py`
  - Default legacy settlement discovery by cycle-specific file name so `pm5min` defaults stay usable.
- Modify: `src/pm5min/cli.py`
  - Keep the thin wrapper, but treat it as config-only; no strategy logic should creep in.
- Modify: `docs/README.md`
  - Document that `pm5min` is a 30-second bucketized 5-minute view over shared logic.
- Test: `tests/test_pm5min_cycle_contracts.py`
  - Lock the 30-second bucket contract.
- Test: `tests/test_research_feature_builders.py`
  - Lock 5-minute bucket offsets and the first-half anchor behavior.
- Test: `tests/test_live_guards.py`
  - Lock the 5-minute directional return guard with the new bucketized 5-minute profile.
- Test: `tests/test_live_regime.py`
  - Lock cycle-aware live feature requests.
- Test: `tests/test_research_backtest_regime_parity.py`
  - Lock replay guard parity for 5-minute bucketized data without heuristic misclassification.
- Test: `tests/test_research_backtest_live_state_parity.py`
  - Keep replay/live parity aligned with the explicit cycle contract.
- Test: `tests/test_pm5min_cli.py`
  - Keep `pm5min` as a thin defaulting wrapper over the shared implementation.
- Test: `tests/test_data_direct_sync.py`
  - Lock default 5-minute legacy settlement discovery by file name.

### Task 1: Extend The Shared Cycle Contract To Describe 30-Second Buckets

**Files:**
- Modify: `src/pm15min/core/cycle_contracts.py`
- Test: `tests/test_pm5min_cycle_contracts.py`

- [ ] **Step 1: Write the failing test**

```python
from pm15min.core.cycle_contracts import resolve_cycle_contract


def test_resolve_cycle_contract_tracks_bucket_geometry() -> None:
    contract_15m = resolve_cycle_contract("15m")
    assert contract_15m.cycle == "15m"
    assert contract_15m.cycle_minutes == 15
    assert contract_15m.bucket_seconds == 60
    assert contract_15m.bucket_count == 15
    assert contract_15m.entry_offsets == (7, 8, 9)
    assert contract_15m.first_half_anchor_offset == 7
    assert contract_15m.regime_return_columns == ("ret_15m", "ret_30m")

    contract_5m = resolve_cycle_contract("5m")
    assert contract_5m.cycle == "5m"
    assert contract_5m.cycle_minutes == 5
    assert contract_5m.bucket_seconds == 30
    assert contract_5m.bucket_count == 10
    assert contract_5m.entry_offsets == (4, 5, 6, 7, 8, 9)
    assert contract_5m.first_half_anchor_offset == 4
    assert contract_5m.regime_return_columns == ("ret_5m", "ret_15m")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cycle_contracts.py -k bucket_geometry`
Expected: FAIL because `CycleContract` does not yet expose `bucket_seconds` and `bucket_count`.

- [ ] **Step 3: Write minimal implementation**

```python
@dataclass(frozen=True)
class CycleContract:
    cycle: str
    cycle_minutes: int
    bucket_seconds: int
    bucket_count: int
    entry_offsets: tuple[int, ...]
    first_half_anchor_offset: int
    regime_return_columns: tuple[str, str]


def resolve_cycle_contract(cycle: str | int) -> CycleContract:
    normalized = normalize_cycle(cycle)
    if normalized == "5m":
        return CycleContract(
            cycle="5m",
            cycle_minutes=5,
            bucket_seconds=30,
            bucket_count=10,
            entry_offsets=(4, 5, 6, 7, 8, 9),
            first_half_anchor_offset=4,
            regime_return_columns=("ret_5m", "ret_15m"),
        )
    return CycleContract(
        cycle="15m",
        cycle_minutes=15,
        bucket_seconds=60,
        bucket_count=15,
        entry_offsets=(7, 8, 9),
        first_half_anchor_offset=7,
        regime_return_columns=("ret_15m", "ret_30m"),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cycle_contracts.py -k bucket_geometry`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pm15min/core/cycle_contracts.py tests/test_pm5min_cycle_contracts.py
git commit -m "feat: add 30s bucket metadata to cycle contracts"
```

### Task 2: Replace Minute Offsets With Contract-Driven Bucket Offsets

**Files:**
- Modify: `src/pm15min/research/features/cycle.py`
- Test: `tests/test_research_feature_builders.py`

- [ ] **Step 1: Write the failing tests**

```python
import pandas as pd

from pm15min.research.features.cycle import append_cycle_features, append_decision_cycle_metadata


def test_append_decision_cycle_metadata_uses_30s_offsets_for_5m() -> None:
    frame = pd.DataFrame(
        {
            "decision_ts": pd.to_datetime(
                [
                    "2026-04-01T00:00:00Z",
                    "2026-04-01T00:02:30Z",
                    "2026-04-01T00:04:30Z",
                ],
                utc=True,
            )
        }
    )

    out = append_decision_cycle_metadata(frame, cycle="5m")

    assert out["offset"].astype("Int64").tolist() == [0, 5, 9]


def test_append_cycle_features_uses_5m_bucket_anchor() -> None:
    frame = pd.DataFrame(
        {
            "decision_ts": pd.date_range("2026-04-01T00:00:00Z", periods=10, freq="30s", tz="UTC"),
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0],
            "rv_30": [0.01] * 10,
        }
    )

    out = append_cycle_features(frame, cycle="5m", requested_columns={"first_half_ret"})

    anchor_row = out.loc[out["offset"].eq(4)].iloc[0]
    assert round(float(anchor_row["first_half_ret"]), 6) == 0.04
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_research_feature_builders.py -k '30s_offsets_for_5m or 5m_bucket_anchor'`
Expected: FAIL because the current code still uses `offset_seconds // 60`.

- [ ] **Step 3: Write minimal implementation**

```python
contract = resolve_cycle_contract(cycle)
bucket_seconds = contract.bucket_seconds
anchor_offset = contract.first_half_anchor_offset

offset_seconds = (decision_ts - cycle_start).dt.total_seconds()
bucket_in_cycle = pd.Series(pd.NA, index=out.index, dtype="Int64")
valid_cycle = decision_ts.notna() & cycle_start.notna() & offset_seconds.notna()
if bool(valid_cycle.any()):
    bucket_in_cycle.loc[valid_cycle] = (offset_seconds.loc[valid_cycle] // bucket_seconds).astype("int64")

out["offset"] = bucket_in_cycle
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_research_feature_builders.py -k '30s_offsets_for_5m or 5m_bucket_anchor'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pm15min/research/features/cycle.py tests/test_research_feature_builders.py
git commit -m "feat: use contract-driven bucket offsets in cycle features"
```

### Task 3: Move The 5-Minute Profiles To Six Late-Cycle Buckets

**Files:**
- Modify: `src/pm15min/live/profiles/catalog.py`
- Test: `tests/test_live_regime.py`
- Test: `tests/test_live_guards.py`

- [ ] **Step 1: Write the failing tests**

```python
from pm15min.live.profiles import resolve_live_profile_spec


def test_deep_otm_5m_profile_uses_six_late_cycle_buckets() -> None:
    spec = resolve_live_profile_spec("deep_otm_5m")
    assert spec.offsets == (4, 5, 6, 7, 8, 9)


def test_deep_otm_5m_baseline_profile_uses_same_bucket_window() -> None:
    spec = resolve_live_profile_spec("deep_otm_5m_baseline")
    assert spec.offsets == (4, 5, 6, 7, 8, 9)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_live_regime.py -k 'six_late_cycle_buckets or same_bucket_window'`
Expected: FAIL because the profiles still use `(2, 3, 4)`.

- [ ] **Step 3: Write minimal implementation**

```python
DEEP_OTM_5M_LIVE_PROFILE_SPEC = LiveProfileSpec(
    **(
        DEEP_OTM_LIVE_PROFILE_SPEC.to_dict()
        | {
            "profile": "deep_otm_5m",
            "offsets": (4, 5, 6, 7, 8, 9),
            "min_dir_prob_by_offset": {
                "sol": {4: 0.62},
                "xrp": {},
            },
            "min_net_edge_by_offset": {4: 0.012, 5: 0.012, 6: 0.015, 7: 0.015, 8: 0.018, 9: 0.018},
            "tail_space_max_move_z_by_offset": {4: 1.85, 5: 1.85, 6: 2.05, 7: 2.05, 8: 2.30, 9: 2.30},
        }
    )
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_live_regime.py -k 'six_late_cycle_buckets or same_bucket_window'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pm15min/live/profiles/catalog.py tests/test_live_regime.py tests/test_live_guards.py
git commit -m "feat: remap 5m live profiles to 30s bucket windows"
```

### Task 4: Remove Ambiguous Cycle Guessing And Propagate Cycle Explicitly

**Files:**
- Modify: `src/pm15min/live/regime/controller.py`
- Modify: `src/pm15min/research/backtests/regime_parity.py`
- Modify: `src/pm15min/research/backtests/live_state_parity.py`
- Modify: `src/pm15min/research/backtests/guard_parity.py`
- Test: `tests/test_research_backtest_regime_parity.py`
- Test: `tests/test_research_backtest_live_state_parity.py`

- [ ] **Step 1: Write the failing tests**

```python
import pandas as pd

from pm15min.live.regime.controller import infer_regime_cycle
from pm15min.research.backtests.guard_parity import apply_live_guard_parity


def test_infer_regime_cycle_does_not_flip_15m_to_5m_just_because_ret_5m_exists() -> None:
    features = pd.DataFrame([{"cycle": "15m", "offset": 7, "ret_5m": 0.001, "ret_15m": 0.002}])
    assert infer_regime_cycle(features=features) == "15m"


def test_apply_live_guard_parity_uses_explicit_cycle_context_from_rows() -> None:
    decisions = pd.DataFrame(
        [
            {
                "cycle": "5m",
                "offset": 4,
                "p_up": 0.20,
                "p_down": 0.80,
                "score_valid": True,
                "score_reason": "",
                "policy_action": "trade",
                "policy_reason": "trade",
                "trade_decision": True,
                "quote_status": "ok",
                "quote_up_ask": 0.71,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.70,
                "quote_down_bid": 0.28,
                "ret_5m": 0.001,
                "ret_15m": 0.02,
            }
        ]
    )

    out, summary = apply_live_guard_parity(market="xrp", profile="deep_otm_5m", decisions=decisions)

    assert summary.blocked_rows == 1
    assert out.loc[0, "guard_primary_reason"] == "ret30m_down_ceiling"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_research_backtest_regime_parity.py -k 'ret_5m_exists or explicit_cycle_context_from_rows'`
Expected: FAIL because `infer_regime_cycle()` still guesses from the `ret_5m` column and replay guard parity does not yet guarantee explicit/shared cycle propagation.

- [ ] **Step 3: Write minimal implementation**

```python
def infer_regime_cycle(
    *,
    cycle: str | int | None = None,
    features: pd.DataFrame | None = None,
    offsets: tuple[int, ...] | None = None,
) -> str:
    del offsets
    if cycle is not None:
        return resolve_cycle_contract(cycle).cycle
    if isinstance(features, pd.DataFrame) and "cycle" in features.columns:
        values = [str(value).strip().lower() for value in features["cycle"].dropna().tolist() if str(value).strip()]
        if values:
            return resolve_cycle_contract(values[-1]).cycle
    if isinstance(features, pd.DataFrame) and {"cycle_start_ts", "cycle_end_ts"}.issubset(features.columns):
        rows = features[["cycle_start_ts", "cycle_end_ts"]].dropna()
        if not rows.empty:
            delta = (pd.to_datetime(rows.iloc[-1]["cycle_end_ts"], utc=True) - pd.to_datetime(rows.iloc[-1]["cycle_start_ts"], utc=True)).total_seconds()
            if delta == 300:
                return "5m"
            if delta == 900:
                return "15m"
    return "15m"
```

```python
cycle = infer_regime_cycle(cycle=cycle, features=decisions, offsets=spec.offsets)
out, summary = apply_live_guard_parity(
    market=market,
    cycle=cycle,
    profile=profile,
    decisions=decisions,
    profile_spec=profile_spec,
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_research_backtest_regime_parity.py tests/test_research_backtest_live_state_parity.py -k 'ret_5m_exists or explicit_cycle_context_from_rows'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pm15min/live/regime/controller.py src/pm15min/research/backtests/regime_parity.py src/pm15min/research/backtests/live_state_parity.py src/pm15min/research/backtests/guard_parity.py tests/test_research_backtest_regime_parity.py tests/test_research_backtest_live_state_parity.py
git commit -m "fix: make cycle propagation explicit in parity paths"
```

### Task 5: Keep Live Guards, Required Features, And Legacy 5m Defaults In Sync

**Files:**
- Modify: `src/pm15min/research/features/builders.py`
- Modify: `src/pm15min/live/guards/features.py`
- Modify: `src/pm15min/live/guards/__init__.py`
- Modify: `src/pm15min/live/signal/decision.py`
- Modify: `src/pm15min/live/signal/scoring_bundle.py`
- Modify: `src/pm15min/data/pipelines/source_ingest.py`
- Modify: `src/pm5min/cli.py`
- Modify: `docs/README.md`
- Test: `tests/test_live_guards.py`
- Test: `tests/test_live_regime.py`
- Test: `tests/test_data_direct_sync.py`
- Test: `tests/test_pm5min_cli.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_prepare_live_features_and_states_requests_5m_regime_return_columns() -> None:
    cfg = LiveConfig.build(market="sol", profile="deep_otm_5m", cycle_minutes=5)
    captured: dict[str, object] = {}

    def fake_build_live_feature_frame_fn(
        _cfg,
        *,
        feature_set,
        retain_offsets,
        allow_preview_open_bar,
        required_feature_columns,
    ):
        captured["required_feature_columns"] = set(required_feature_columns)
        return pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-20T00:03:00+00:00",
                    "ret_5m": 0.0020,
                    "ret_15m": 0.0030,
                }
            ]
        )

    prepare_live_features_and_states(
        cfg,
        builder_feature_set="v6_user_core",
        persist=False,
        build_live_feature_frame_fn=fake_build_live_feature_frame_fn,
    )

    required_feature_columns = captured["required_feature_columns"]
    assert "ret_5m" in required_feature_columns
    assert "ret_15m" in required_feature_columns


def test_decision_rejects_when_5m_long_return_guard_fails() -> None:
    row = _base_signal_row()
    row["offset"] = 4
    row["feature_snapshot"] = {
        "ret_5m": 0.001,
        "ret_15m": 0.02,
        "ret_from_strike": -0.002,
        "move_z": 1.0,
    }
    payload = {
        "market": "xrp",
        "profile": "deep_otm_5m",
        "cycle": "5m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    out = build_decision_snapshot(payload)

    assert out["rejected_offsets"][0]["guard_reasons"] == ["ret30m_down_ceiling"]


def test_import_legacy_settlement_truth_discovers_default_5m_source(tmp_path, monkeypatch) -> None:
    cfg = DataConfig.build(market="eth", cycle="5m", root=tmp_path / "v2")
    shared_root = tmp_path / "data" / "markets" / "_shared" / "oracle"
    shared_root.mkdir(parents=True, exist_ok=True)
    discovered = shared_root / "polymarket_5m_settlement_truth.csv"
    pd.DataFrame(
        [
            {
                "asset": "eth",
                "end_ts": 1766032200,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "slug": "eth-updown-5m-1766031900",
                "question": "Ethereum Up or Down",
                "resolution_source": "legacy-5m",
                "winner_side": "UP",
                "label_updown": "UP",
                "onchain_resolved": True,
                "stream_match_exact": True,
                "full_truth": True,
                "stream_price": 2042.5,
                "stream_extra_ts": 1766032200,
            }
        ]
    ).to_csv(discovered, index=False)
    monkeypatch.setattr("pm15min.data.pipelines.source_ingest.workspace_root", lambda: tmp_path)

    summary = import_legacy_settlement_truth(cfg)

    assert summary["rows_imported"] == 1


def test_rewrite_pm5min_argv_covers_every_cycle_capable_data_command() -> None:
    for path in sorted(_data_cycle_capable_command_paths()):
        rewritten = rewrite_pm5min_argv(["data", *path])

    assert "--cycle" in rewritten
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_live_regime.py tests/test_live_guards.py tests/test_data_direct_sync.py tests/test_pm5min_cli.py -k '5m_regime_return_columns or 5m_long_return_guard_fails or discovers_default_5m_source or covers_every_cycle_capable_data_command'`
Expected: FAIL until the live guard, required-column selection, and 5-minute default discovery all line up.

- [ ] **Step 3: Write minimal implementation**

```python
short_return, long_return = resolve_cycle_contract(cycle).regime_return_columns
requested_columns = {
    short_return,
    long_return,
    "ret_from_cycle_open",
    "ret_from_strike",
    "move_z",
    "move_z_strike",
}
```

```python
_, long_return_column = resolve_cycle_contract(cycle).regime_return_columns
long_return = float_or_none(feature_snapshot.get(long_return_column))
```

```python
source_path = source_path or discover_legacy_settlement_truth_csv(cfg.cycle)
```

```python
if domain == "data" and _data_command_supports_cycle(out) and not _has_flag(out, "--cycle"):
    out.extend(["--cycle", DEFAULT_CYCLE])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_live_regime.py tests/test_live_guards.py tests/test_data_direct_sync.py tests/test_pm5min_cli.py -k '5m_regime_return_columns or 5m_long_return_guard_fails or discovers_default_5m_source or covers_every_cycle_capable_data_command'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pm15min/research/features/builders.py src/pm15min/live/guards/features.py src/pm15min/live/guards/__init__.py src/pm15min/live/signal/decision.py src/pm15min/live/signal/scoring_bundle.py src/pm15min/data/pipelines/source_ingest.py src/pm5min/cli.py docs/README.md tests/test_live_guards.py tests/test_live_regime.py tests/test_data_direct_sync.py tests/test_pm5min_cli.py
git commit -m "fix: align 5m live and data defaults with shared bucket contracts"
```

### Task 6: Run The Merge-Readiness Regression On The 30s Bucket Refactor

**Files:**
- Test: `tests/test_pm5min_cycle_contracts.py`
- Test: `tests/test_research_feature_builders.py`
- Test: `tests/test_live_regime.py`
- Test: `tests/test_live_guards.py`
- Test: `tests/test_research_backtest_regime_parity.py`
- Test: `tests/test_research_backtest_live_state_parity.py`
- Test: `tests/test_data_direct_sync.py`
- Test: `tests/test_pm5min_cli.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Run the focused bucket regression**

Run:

```bash
PYTHONPATH=src pytest -q \
  tests/test_pm5min_cycle_contracts.py \
  tests/test_research_feature_builders.py \
  tests/test_live_regime.py \
  tests/test_live_guards.py \
  tests/test_research_backtest_regime_parity.py \
  tests/test_research_backtest_live_state_parity.py \
  tests/test_data_direct_sync.py \
  tests/test_pm5min_cli.py
```

Expected: PASS

- [ ] **Step 2: Run the broader CLI/regression set**

Run:

```bash
PYTHONPATH=src pytest -q \
  tests/test_pm5min_cycle_contracts.py \
  tests/test_pm5min_cli.py \
  tests/test_data_direct_sync.py \
  tests/test_data_foundation_runtime.py \
  tests/test_research_feature_builders.py \
  tests/test_live_regime.py \
  tests/test_research_backtest_live_state_parity.py \
  tests/test_cli.py \
  tests/test_live_guards.py::test_decision_rejects_when_ret_30m_guard_fails \
  tests/test_live_guards.py::test_decision_rejects_when_5m_long_return_guard_fails \
  tests/test_research_backtest_regime_parity.py::test_apply_live_guard_parity_keeps_tail_space_guard_from_row_features \
  tests/test_research_backtest_regime_parity.py::test_apply_live_guard_parity_uses_5m_long_return_guard \
  tests/test_research_backtest_phase_b.py::test_apply_live_guard_parity_blocks_trade_when_quote_surface_is_missing \
  -k 'not test_live_score_latest and not test_live_check_and_decide_latest and not test_live_quote_latest_reports_missing_inputs'
```

Expected: PASS with the same 3 external-connector smoke tests deselected unless their network dependency is separately stabilized.

- [ ] **Step 3: Commit any last doc/test-only adjustments**

```bash
git add docs/README.md tests/test_pm5min_cli.py tests/test_live_regime.py tests/test_live_guards.py tests/test_research_backtest_regime_parity.py tests/test_data_direct_sync.py
git commit -m "test: lock 30s bucketized pm5min regressions"
```

## Self-Review

- Spec coverage:
  - 30-second 10-bucket geometry is covered by Task 1 and Task 2.
  - 5-minute profile remap is covered by Task 3.
  - explicit cycle propagation and the current heuristic-guessing bug are covered by Task 4.
  - live/data/replay default alignment is covered by Task 5.
  - merge-readiness verification is covered by Task 6.
- Placeholder scan:
  - No `TODO`/`TBD` placeholders remain.
  - All tasks list concrete files, code blocks, commands, and expected outcomes.
- Type consistency:
  - The plan keeps the current `offset` column name for compatibility, but redefines it as a contract-driven intra-cycle bucket index.
  - `cycle` is treated as explicit shared context in parity/guard paths rather than a guessed property of feature columns.

## Notes For The Implementer

- Do not create a copied `pm5min` logic tree. Keep `pm5min` as defaults + argv rewriting only.
- Do not rely on `ret_5m` column presence to infer the cycle. `deep_otm_v1` already carries `ret_5m` in 15-minute research data, so that shortcut is unsafe.
- If the user later wants a different 5-minute trade window than `(4, 5, 6, 7, 8, 9)`, change only the profile/catalog and contract values; keep the 30-second bucket infrastructure intact.

**Plan complete and saved to `docs/superpowers/plans/2026-04-11-pm5min-30s-10bucket-refactor.md`. Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
