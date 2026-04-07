# PM5Min Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `pm5min` package that runs a 5-minute variant of the existing stack by isolating the true 5-minute differences and reusing the remaining `pm15min` logic.

**Architecture:** Do not fork the whole `pm15min` tree. First extract cycle-sensitive differences into one shared contract module, then make the data, research, backtest, and live seams read those contracts. Finally add a thin `pm5min` entry package that defaults to `5m` and the 5-minute live profile names while delegating to the shared implementation.

**Tech Stack:** Python 3.11+, existing `pm15min` package, `pytest`, argparse-based CLI wrappers, existing data/research/live pipelines.

---

## File Structure

- Create: `src/pm15min/core/cycle_contracts.py`
  - Single source of truth for cycle-sensitive differences: cycle minutes, default entry offsets, first-half anchor offset, and regime return columns.
- Create: `src/pm5min/defaults.py`
  - `pm5min` package defaults for cycle, cycle-minutes, and default live profile names.
- Create: `src/pm5min/cli.py`
  - Thin argv rewriter that injects 5-minute defaults and forwards to `pm15min.cli.main`.
- Create: `src/pm5min/__main__.py`
  - `python -m pm5min` entrypoint.
- Create: `tests/test_pm5min_cycle_contracts.py`
  - New focused tests for the shared 5m/15m difference ledger.
- Create: `tests/test_pm5min_cli.py`
  - New focused tests for the `pm5min` wrapper behavior.
- Modify: `src/pm15min/research/features/cycle.py`
  - Replace the hard-coded mid-cycle anchor with the shared cycle contract.
- Modify: `src/pm15min/data/pipelines/binance_klines.py`
  - Remove the 15-minute-only guard from 1m kline syncing.
- Modify: `src/pm15min/data/pipelines/direct_sync.py`
  - Allow 5-minute settlement-truth sync from RPC.
- Modify: `src/pm15min/data/pipelines/source_ingest.py`
  - Allow 5-minute settlement-truth import from legacy CSV.
- Modify: `src/pm15min/data/pipelines/foundation_runtime.py`
  - Remove the 15-minute-only runtime guard and rely on already-present cycle-aware boundary logic.
- Modify: `src/pm15min/live/profiles/catalog.py`
  - Add `deep_otm_5m` and `deep_otm_5m_baseline`.
- Modify: `src/pm15min/live/regime/controller.py`
  - Read short/long regime returns from the cycle contract instead of assuming `ret_15m` and `ret_30m`.
- Modify: `src/pm15min/live/regime/state.py`
  - Pass cycle-aware regime returns into the controller.
- Modify: `src/pm15min/research/backtests/regime_parity.py`
  - Keep backtest regime parity aligned with live cycle-aware regime return selection.
- Modify: `src/pm15min/research/backtests/live_state_parity.py`
  - Keep replay/live-state parity aligned with the same regime-return selection.
- Modify: `tests/test_data_direct_sync.py`
  - Extend existing sync coverage from 15m-only to 5m.
- Modify: `tests/test_data_foundation_runtime.py`
  - Add a 5m live-foundation smoke test.
- Modify: `tests/test_research_feature_builders.py`
  - Add a 5m first-half-anchor regression test.
- Modify: `tests/test_live_regime.py`
  - Add a 5m regime-pressure regression test.
- Modify: `tests/test_cli.py`
  - Add a top-level `pm5min` wrapper smoke test.
- Modify: `docs/README.md`
  - Add a short note explaining that `pm5min` is a 5-minute entry package over shared logic, not a full code fork.

### Task 1: Extract The Shared 5m/15m Difference Ledger

**Files:**
- Create: `src/pm15min/core/cycle_contracts.py`
- Test: `tests/test_pm5min_cycle_contracts.py`

- [ ] **Step 1: Write the failing test**

```python
from pm15min.core.cycle_contracts import resolve_cycle_contract


def test_resolve_cycle_contract_for_15m_and_5m() -> None:
    contract_15m = resolve_cycle_contract("15m")
    assert contract_15m.cycle == "15m"
    assert contract_15m.cycle_minutes == 15
    assert contract_15m.entry_offsets == (7, 8, 9)
    assert contract_15m.first_half_anchor_offset == 7
    assert contract_15m.regime_return_columns == ("ret_15m", "ret_30m")

    contract_5m = resolve_cycle_contract("5m")
    assert contract_5m.cycle == "5m"
    assert contract_5m.cycle_minutes == 5
    assert contract_5m.entry_offsets == (2, 3, 4)
    assert contract_5m.first_half_anchor_offset == 2
    assert contract_5m.regime_return_columns == ("ret_5m", "ret_15m")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cycle_contracts.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'pm15min.core.cycle_contracts'`

- [ ] **Step 3: Write minimal implementation**

```python
from __future__ import annotations

from dataclasses import dataclass

from pm15min.data.layout.helpers import normalize_cycle


@dataclass(frozen=True)
class CycleContract:
    cycle: str
    cycle_minutes: int
    entry_offsets: tuple[int, ...]
    first_half_anchor_offset: int
    regime_return_columns: tuple[str, str]


def resolve_cycle_contract(cycle: str | int) -> CycleContract:
    normalized = normalize_cycle(cycle)
    if normalized == "5m":
        return CycleContract(
            cycle="5m",
            cycle_minutes=5,
            entry_offsets=(2, 3, 4),
            first_half_anchor_offset=2,
            regime_return_columns=("ret_5m", "ret_15m"),
        )
    return CycleContract(
        cycle="15m",
        cycle_minutes=15,
        entry_offsets=(7, 8, 9),
        first_half_anchor_offset=7,
        regime_return_columns=("ret_15m", "ret_30m"),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cycle_contracts.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_pm5min_cycle_contracts.py src/pm15min/core/cycle_contracts.py
git commit -m "feat: add shared cycle contracts"
```

### Task 2: Add The `pm5min` Wrapper Package And CLI Defaults

**Files:**
- Create: `src/pm5min/defaults.py`
- Create: `src/pm5min/cli.py`
- Create: `src/pm5min/__main__.py`
- Test: `tests/test_pm5min_cli.py`

- [ ] **Step 1: Write the failing test**

```python
from pm5min.cli import rewrite_pm5min_argv


def test_rewrite_pm5min_argv_injects_5m_defaults() -> None:
    assert rewrite_pm5min_argv(["layout", "--market", "sol"]) == [
        "layout",
        "--market",
        "sol",
        "--cycle",
        "5m",
    ]
    assert rewrite_pm5min_argv(["research", "show-layout", "--market", "sol"]) == [
        "research",
        "show-layout",
        "--market",
        "sol",
        "--cycle",
        "5m",
    ]
    assert rewrite_pm5min_argv(["live", "show-config", "--market", "sol"]) == [
        "live",
        "show-config",
        "--market",
        "sol",
        "--cycle-minutes",
        "5",
        "--profile",
        "deep_otm_5m",
    ]
    assert rewrite_pm5min_argv(
        ["live", "show-config", "--market", "sol", "--profile", "custom_5m"]
    ) == [
        "live",
        "show-config",
        "--market",
        "sol",
        "--profile",
        "custom_5m",
        "--cycle-minutes",
        "5",
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'pm5min.cli'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm5min/defaults.py
DEFAULT_CYCLE = "5m"
DEFAULT_CYCLE_MINUTES = 5
DEFAULT_LIVE_PROFILE = "deep_otm_5m"


# src/pm5min/cli.py
from __future__ import annotations

import sys

from pm15min.cli import main as pm15min_main
from pm5min.defaults import DEFAULT_CYCLE, DEFAULT_CYCLE_MINUTES, DEFAULT_LIVE_PROFILE


def rewrite_pm5min_argv(argv: list[str]) -> list[str]:
    out = list(argv)
    if not out:
        return out
    domain = out[0]
    if domain in {"layout", "data", "research", "console"} and "--cycle" not in out:
        out.extend(["--cycle", DEFAULT_CYCLE])
    if domain == "live":
        if "--cycle-minutes" not in out:
            out.extend(["--cycle-minutes", str(DEFAULT_CYCLE_MINUTES)])
        if "--profile" not in out:
            out.extend(["--profile", DEFAULT_LIVE_PROFILE])
    return out


def main(argv: list[str] | None = None) -> int:
    return pm15min_main(rewrite_pm5min_argv(list(sys.argv[1:] if argv is None else argv)))


# src/pm5min/__main__.py
from __future__ import annotations

import sys

from pm5min.cli import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py`
Expected: PASS

- [ ] **Step 5: Run a wrapper smoke check**

Run: `PYTHONPATH=src python -m pm5min layout --market sol --json`
Expected: PASS and JSON output with `"cycle": "5m"`

- [ ] **Step 6: Commit**

```bash
git add tests/test_pm5min_cli.py src/pm5min/defaults.py src/pm5min/cli.py src/pm5min/__main__.py
git commit -m "feat: add pm5min cli wrapper"
```

### Task 3: Enable 5m In The Data Foundation And Settlement Pipelines

**Files:**
- Modify: `src/pm15min/data/pipelines/binance_klines.py`
- Modify: `src/pm15min/data/pipelines/direct_sync.py`
- Modify: `src/pm15min/data/pipelines/source_ingest.py`
- Modify: `src/pm15min/data/pipelines/foundation_runtime.py`
- Modify: `tests/test_data_direct_sync.py`
- Modify: `tests/test_data_foundation_runtime.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_sync_settlement_truth_from_rpc_supports_5m(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="eth", cycle="5m", root=tmp_path / "v2")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "eth",
                    "cycle": "5m",
                    "cycle_start_ts": 1766031900,
                    "cycle_end_ts": 1766032200,
                    "token_up": "token-up",
                    "token_down": "token-down",
                }
            ]
        ),
        cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "eth",
                    "tx_hash": "0xabc",
                    "observation_ts": 1766032200,
                    "extra_ts": 1766032200,
                    "benchmark_price_raw": 1.1e21,
                    "price": 1100.0,
                    "perform_idx": 0,
                    "value_idx": 0,
                    "source_file": "rpc",
                    "ingested_at": "2026-03-28T10:00:00Z",
                }
            ]
        ),
        cfg.layout.streams_partition_path(2026, 3),
    )

    summary = sync_settlement_truth_from_rpc(cfg, rpc=_FakeRpc())
    assert summary["rows_imported"] == 1


def test_run_live_data_foundation_accepts_5m(tmp_path: Path, monkeypatch) -> None:
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path / "v2")
    monkeypatch.setattr(
        "pm15min.data.pipelines.foundation_runtime._build_foundation_task_specs",
        lambda **kwargs: [],
    )
    summary = run_live_data_foundation(cfg, iterations=1, loop=False)
    assert summary["status"] == "ok"
    assert summary["cycle"] == "5m"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_data_direct_sync.py tests/test_data_foundation_runtime.py -k '5m or foundation_accepts_5m'`
Expected: FAIL with `ValueError` messages that the runtime currently requires `cycle=15m`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm15min/data/pipelines/binance_klines.py
def sync_binance_klines_1m(...):
    resolved_symbol = str(symbol or cfg.asset.binance_symbol).strip().upper()
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    ...


# src/pm15min/data/pipelines/direct_sync.py
def sync_settlement_truth_from_rpc(...):
    markets = load_market_catalog(cfg)
    ...


# src/pm15min/data/pipelines/source_ingest.py
def import_legacy_settlement_truth(...):
    source_path = source_path or discover_legacy_settlement_truth_csv()
    ...


# src/pm15min/data/pipelines/foundation_runtime.py
def run_live_data_foundation(...):
    if cfg.surface != "live":
        raise ValueError("live foundation runtime currently requires surface=live.")
    ...

def run_live_data_foundation_shared(...):
    for cfg in cfgs:
        if cfg.surface != "live":
            raise ValueError("shared live foundation currently requires surface=live.")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_data_direct_sync.py tests/test_data_foundation_runtime.py -k '5m or foundation_accepts_5m'`
Expected: PASS

- [ ] **Step 5: Run a focused data regression subset**

Run: `PYTHONPATH=src pytest -q tests/test_data_direct_sync.py tests/test_data_foundation_runtime.py tests/test_data_pipelines.py`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm15min/data/pipelines/binance_klines.py src/pm15min/data/pipelines/direct_sync.py src/pm15min/data/pipelines/source_ingest.py src/pm15min/data/pipelines/foundation_runtime.py tests/test_data_direct_sync.py tests/test_data_foundation_runtime.py
git commit -m "feat: enable 5m data foundation paths"
```

### Task 4: Make Research Feature Anchors And Offsets Cycle-Aware

**Files:**
- Modify: `src/pm15min/research/features/cycle.py`
- Modify: `tests/test_research_feature_builders.py`

- [ ] **Step 1: Write the failing test**

```python
def test_build_feature_frame_uses_5m_first_half_anchor() -> None:
    features = build_feature_frame(
        _raw_klines(rows=15),
        feature_set="deep_otm_v1",
        oracle_prices=pd.DataFrame(
            [
                {
                    "cycle_start_ts": int(pd.Timestamp("2026-03-20T00:00:00Z").timestamp()),
                    "cycle_end_ts": int(pd.Timestamp("2026-03-20T00:05:00Z").timestamp()),
                    "price_to_beat": 100.0,
                    "final_price": 101.0,
                }
            ]
        ),
        cycle="5m",
        requested_columns={"ret_from_cycle_open", "first_half_ret", "second_half_ret_proxy"},
    )

    anchor_row = features.loc[features["offset"].eq(2)].iloc[0]
    late_row = features.loc[features["offset"].eq(4)].iloc[0]

    assert anchor_row["first_half_ret"] == anchor_row["ret_from_cycle_open"]
    assert late_row["second_half_ret_proxy"] > 0.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest -q tests/test_research_feature_builders.py -k 5m_first_half_anchor`
Expected: FAIL because `first_half_ret` is still anchored to minute `7`

- [ ] **Step 3: Write minimal implementation**

```python
from pm15min.core.cycle_contracts import resolve_cycle_contract


def append_cycle_features(...):
    ...
    contract = resolve_cycle_contract(cycle)
    anchor_offset = contract.first_half_anchor_offset
    ...
    if needs("first_half_ret", "second_half_ret_proxy"):
        first_half_close = close.where(minute_in_cycle.eq(anchor_offset)).groupby(cycle_start).ffill().fillna(close)
        if needs("first_half_ret"):
            out["first_half_ret"] = first_half_close / cycle_open - 1.0
        if needs("second_half_ret_proxy"):
            second_half_anchor = first_half_close.where(minute_in_cycle.ge(anchor_offset), close)
            out["second_half_ret_proxy"] = close / second_half_anchor - 1.0
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest -q tests/test_research_feature_builders.py -k 5m_first_half_anchor`
Expected: PASS

- [ ] **Step 5: Run the feature-builder regression subset**

Run: `PYTHONPATH=src pytest -q tests/test_research_feature_builders.py`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm15min/research/features/cycle.py tests/test_research_feature_builders.py
git commit -m "feat: make cycle features 5m-aware"
```

### Task 5: Add 5m Live Profiles And Cycle-Aware Regime Return Selection

**Files:**
- Modify: `src/pm15min/live/profiles/catalog.py`
- Modify: `src/pm15min/live/regime/controller.py`
- Modify: `src/pm15min/live/regime/state.py`
- Modify: `src/pm15min/research/backtests/regime_parity.py`
- Modify: `src/pm15min/research/backtests/live_state_parity.py`
- Modify: `tests/test_live_regime.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_build_regime_state_snapshot_uses_5m_regime_return_columns(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_regime_snapshot_labels(monkeypatch, ["2026-03-20T00-03-00Z"])
    cfg = LiveConfig.build(market="sol", profile="deep_otm_5m", cycle_minutes=5)

    payload = build_regime_state_snapshot(
        cfg,
        features=pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-20T00:03:00+00:00",
                    "offset": 2,
                    "ret_5m": 0.0020,
                    "ret_15m": 0.0030,
                }
            ]
        ),
        liquidity_payload={
            "snapshot_ts": "2026-03-20T00-02-59Z",
            "status": "ok",
            "reason": "ok",
            "blocked": False,
            "reason_codes": ["ok"],
            "metrics": {
                "spot_quote_ratio": 1.0,
                "perp_quote_ratio": 1.0,
                "spot_trades_ratio": 1.0,
                "perp_trades_ratio": 1.0,
                "soft_fail_count": 0,
                "hard_fail_count": 0,
            },
        },
        persist=False,
    )

    assert payload["pressure"] == "up"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest -q tests/test_live_regime.py -k 5m_regime_return_columns`
Expected: FAIL because `deep_otm_5m` is missing and the regime code still reads `ret_15m` and `ret_30m` unconditionally

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm15min/live/profiles/catalog.py
DEEP_OTM_5M_LIVE_PROFILE_SPEC = LiveProfileSpec(
    **(
        DEEP_OTM_LIVE_PROFILE_SPEC.to_dict()
        | {
            "profile": "deep_otm_5m",
            "offsets": (2, 3, 4),
            "min_net_edge_by_offset": {2: 0.012, 3: 0.015, 4: 0.018},
        }
    )
)

DEEP_OTM_5M_BASELINE_LIVE_PROFILE_SPEC = LiveProfileSpec(
    **(
        DEEP_OTM_5M_LIVE_PROFILE_SPEC.to_dict()
        | {
            "profile": "deep_otm_5m_baseline",
            "min_net_edge_default": 0.0,
            "min_net_edge_by_offset": {2: 0.0, 3: 0.0, 4: 0.0},
        }
    )
)

LIVE_PROFILE_SPECS = {
    ...
    "deep_otm_5m": DEEP_OTM_5M_LIVE_PROFILE_SPEC,
    "deep_otm_5m_baseline": DEEP_OTM_5M_BASELINE_LIVE_PROFILE_SPEC,
}


# src/pm15min/live/regime/controller.py
from pm15min.core.cycle_contracts import resolve_cycle_contract

def latest_regime_returns(features: pd.DataFrame | None, *, cycle: str | int) -> tuple[float | None, float | None]:
    if not isinstance(features, pd.DataFrame) or features.empty:
        return None, None
    short_col, long_col = resolve_cycle_contract(cycle).regime_return_columns
    rows = features.sort_values("decision_ts") if "decision_ts" in features.columns else features
    row = rows.tail(1)
    return float_or_none(row.get(short_col).iloc[-1] if short_col in row.columns else None), float_or_none(
        row.get(long_col).iloc[-1] if long_col in row.columns else None
    )


# src/pm15min/live/regime/state.py
ret_15m, ret_30m = latest_regime_returns(features, cycle=f"{int(cfg.cycle_minutes)}m")


# src/pm15min/research/backtests/regime_parity.py
ret_15m, ret_30m = latest_regime_returns(features, cycle=cycle)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest -q tests/test_live_regime.py -k 5m_regime_return_columns`
Expected: PASS

- [ ] **Step 5: Run the live/regime regression subset**

Run: `PYTHONPATH=src pytest -q tests/test_live_regime.py tests/test_research_backtest_live_state_parity.py`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm15min/live/profiles/catalog.py src/pm15min/live/regime/controller.py src/pm15min/live/regime/state.py src/pm15min/research/backtests/regime_parity.py src/pm15min/research/backtests/live_state_parity.py tests/test_live_regime.py
git commit -m "feat: add 5m live profile support"
```

### Task 6: End-To-End Verification And Discovery Docs

**Files:**
- Modify: `tests/test_cli.py`
- Modify: `docs/README.md`

- [ ] **Step 1: Add the top-level smoke test to the main CLI regression file**

```python
def test_pm5min_layout_command(capsys) -> None:
    from pm5min.cli import main

    rc = main(["layout", "--market", "sol", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["cycle"] == "5m"
```

- [ ] **Step 2: Update the repo discovery docs**

```markdown
<!-- docs/README.md -->
- `pm5min/`
  - 5-minute entry package.
  - Reuses shared `pm15min` implementation where behavior is identical.
  - Keeps only cycle-sensitive defaults and 5m-specific profiles separate.
```

- [ ] **Step 3: Run the smoke and focused migration suite**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cycle_contracts.py tests/test_pm5min_cli.py tests/test_cli.py -k 'pm5min or 5m'`
Expected: PASS

- [ ] **Step 4: Run the full targeted regression slice**

Run: `PYTHONPATH=src pytest -q tests/test_data_direct_sync.py tests/test_data_foundation_runtime.py tests/test_research_feature_builders.py tests/test_live_regime.py tests/test_research_backtest_live_state_parity.py tests/test_cli.py`
Expected: PASS

- [ ] **Step 5: Run one manual CLI check per surface**

Run: `PYTHONPATH=src python -m pm5min layout --market sol --json`
Expected: PASS and JSON output with `"cycle": "5m"`

Run: `PYTHONPATH=src python -m pm5min research show-layout --market sol`
Expected: PASS and output paths rooted under `research/.../cycle=5m/...`

Run: `PYTHONPATH=src python -m pm5min live show-config --market sol`
Expected: PASS and output with `"cycle_minutes": 5` plus `"profile": "deep_otm_5m"`

- [ ] **Step 6: Commit**

```bash
git add tests/test_cli.py docs/README.md
git commit -m "docs: document pm5min entry package"
```
