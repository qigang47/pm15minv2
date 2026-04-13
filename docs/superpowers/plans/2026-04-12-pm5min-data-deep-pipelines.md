# PM5Min Data Deep Pipeline Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish splitting `pm5min/data` so the remaining deep pipeline execution paths no longer depend on `pm15min.data.pipelines.*`.

**Architecture:** Keep the existing `cli.py -> handlers.py -> local modules` structure. Continue moving the deepest logic in thin, testable slices: first the medium-size source pipelines, then the write-heavy sync chains, then the orderbook runtime stack, and finally the multi-step orchestration runtimes. Do not reintroduce a generic fallback path.

**Tech Stack:** Python 3.11+, argparse CLI, `pytest`, package-local modules under `src/pm5min/data`, shared IO helpers under `src/pmshared/io`.

---

## Current Boundary After The Latest Split

- `pm5min/data` already owns:
  - parser / handlers / command routing
  - service-layer summary and coverage reporting
  - query loaders for canonical reads
  - truth / oracle / export table builders
- `pm5min/data` still borrows from `pm15min.data.pipelines.*` through `src/pm5min/data/compat.py` for:
  - `market_catalog`
  - `binance_klines`
  - `source_ingest`
  - `direct_sync`
  - `direct_oracle_prices`
  - `orderbook_recording`
  - `orderbook_runtime`
  - `orderbook_fleet`
  - `backtest_refresh`
  - `foundation_runtime`

## File Structure

- Create: `src/pm5min/data/pipelines/market_catalog.py`
  - Local 5m market catalog sync and snapshot writes.
- Create: `src/pm5min/data/pipelines/binance_klines.py`
  - Local 5m Binance 1m ingestion and latest-tail marker writes.
- Create: `src/pm5min/data/pipelines/source_ingest.py`
  - Local 5m legacy CSV / NDJSON import paths.
- Create: `src/pm5min/data/pipelines/direct_sync.py`
  - Local 5m RPC source ingestion for streams, datafeeds, and settlement truth.
- Create: `src/pm5min/data/pipelines/direct_oracle_prices.py`
  - Local 5m direct oracle fetch and backfill flow.
- Create: `src/pm5min/data/pipelines/orderbook_recent.py`
  - Local helper for recent orderbook index refresh.
- Create: `src/pm5min/data/pipelines/orderbook_recording.py`
  - Local 5m orderbook capture, persistence, and index compaction flow.
- Create: `src/pm5min/data/pipelines/orderbook_runtime.py`
  - Local 5m recorder loop and async persistence control.
- Create: `src/pm5min/data/pipelines/orderbook_fleet.py`
  - Local 5m multi-market orderbook runner.
- Create: `src/pm5min/data/pipelines/backtest_refresh.py`
  - Local 5m backtest refresh orchestration.
- Create: `src/pm5min/data/pipelines/foundation_shared.py`
  - Local shared helpers for live foundation scheduling.
- Create: `src/pm5min/data/pipelines/foundation_runtime.py`
  - Local 5m live foundation orchestration.
- Modify: `src/pm5min/data/compat.py`
  - Shrink wrappers after each local pipeline lands; delete wrapper once unused.
- Modify: `src/pm5min/data/handlers.py`
  - Point handlers at package-local pipelines instead of compat wrappers as each task completes.
- Modify: `tests/test_pm5min_cli.py`
  - Extend guard tests so `pm5min` must use local pipeline modules for each newly split command.
- Create: `tests/test_pm5min_data_pipelines.py`
  - Add focused unit tests for local builder and source pipeline behavior without CLI setup noise.
- Create: `tests/test_pm5min_data_orderbooks.py`
  - Add focused tests for orderbook capture/runtime/fleet pieces.
- Create: `tests/test_pm5min_data_foundation.py`
  - Add focused tests for backtest refresh and live foundation orchestration.

## Task 1: Split The Medium-Size Source Pipelines

**Files:**
- Create: `src/pm5min/data/pipelines/market_catalog.py`
- Create: `src/pm5min/data/pipelines/binance_klines.py`
- Create: `src/pm5min/data/pipelines/source_ingest.py`
- Modify: `src/pm5min/data/handlers.py`
- Modify: `src/pm5min/data/compat.py`
- Modify: `tests/test_pm5min_cli.py`
- Create: `tests/test_pm5min_data_pipelines.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pm5min_data_pipelines.py
from pm5min.data.config import DataConfig
from pm5min.data.pipelines.market_catalog import sync_market_catalog
from pm5min.data.pipelines.binance_klines import sync_binance_klines_1m
from pm5min.data.pipelines.source_ingest import import_legacy_streams


def test_pm5min_market_catalog_pipeline_uses_5m_layout(tmp_path, monkeypatch):
    cfg = DataConfig.build(market="sol", cycle="5m", root=tmp_path)
    payload = sync_market_catalog(cfg, start_ts=1, end_ts=2, client=_FakeGamma(), now=_fixed_now())
    assert payload["cycle"] == "5m"


def test_pm5min_binance_pipeline_writes_under_5m_root(tmp_path, monkeypatch):
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path)
    payload = sync_binance_klines_1m(cfg, client=_FakeBinance(), now=_fixed_now(), lookback_minutes=60, batch_limit=10)
    assert payload["target_path"].endswith("data/live/sources/binance/klines_1m/symbol=SOLUSDT/data.parquet")


def test_pm5min_legacy_streams_import_uses_5m_surface_root(tmp_path):
    cfg = DataConfig.build(market="sol", cycle="5m", root=tmp_path)
    payload = import_legacy_streams(cfg, source_path=_write_legacy_streams_csv(tmp_path))
    assert payload["target_root"].endswith("cycle=5m/asset=sol")
```

```python
# tests/test_pm5min_cli.py
def test_pm5min_cli_does_not_reference_pm15min_source_pipeline_modules() -> None:
    compat_text = Path(... / "src" / "pm5min" / "data" / "compat.py").read_text(encoding="utf-8")
    assert "pm15min.data.pipelines.market_catalog" not in compat_text
    assert "pm15min.data.pipelines.binance_klines" not in compat_text
    assert "pm15min.data.pipelines.source_ingest" not in compat_text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_data_pipelines.py tests/test_pm5min_cli.py -k 'market_catalog_pipeline or binance_pipeline or legacy_streams_import or source_pipeline_modules'`
Expected: FAIL because the new local modules do not exist yet and `compat.py` still imports these `pm15min` pipeline modules.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm5min/data/pipelines/market_catalog.py
from pmshared.io.parquet import upsert_parquet, write_parquet_atomic
from pmshared.time import utc_snapshot_label


def sync_market_catalog(cfg, *, start_ts, end_ts, client=None, now=None, selection_mode=None):
    ...
```

```python
# src/pm5min/data/pipelines/binance_klines.py
from pmshared.io.json_files import write_json_atomic
from pmshared.io.parquet import read_parquet_if_exists, upsert_parquet


def sync_binance_klines_1m(cfg, *, client=None, now=None, lookback_minutes=1440, batch_limit=1000, symbol=None):
    ...
```

```python
# src/pm5min/data/pipelines/source_ingest.py
def import_legacy_streams(cfg, *, source_path=None):
    ...


def import_legacy_market_catalog(cfg, *, source_path=None):
    ...


def import_legacy_orderbook_depth(cfg, *, source_paths=None, date_from=None, date_to=None, overwrite=False):
    ...


def import_legacy_settlement_truth(cfg, *, source_path=None):
    ...
```

```python
# src/pm5min/data/handlers.py
from .pipelines import binance_klines as binance_klines_pipeline
from .pipelines import market_catalog as market_catalog_pipeline
from .pipelines import source_ingest as source_ingest_pipeline
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_data_pipelines.py tests/test_pm5min_cli.py -k 'market_catalog_pipeline or binance_pipeline or legacy_streams_import or source_pipeline_modules'`
Expected: PASS

- [ ] **Step 5: Run baseline smoke**

Run: `PYTHONPATH=src pytest -q tests/test_cli.py -k 'top_level_layout_command or live_show_config'`
Expected: PASS

Run: `PYTHONPATH=src pytest -q tests/test_live_service.py -k 'prewarm_live_signal_inputs or score_live_latest'`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm5min/data/pipelines/market_catalog.py src/pm5min/data/pipelines/binance_klines.py src/pm5min/data/pipelines/source_ingest.py src/pm5min/data/handlers.py src/pm5min/data/compat.py tests/test_pm5min_data_pipelines.py tests/test_pm5min_cli.py
git commit -m "feat: split pm5min source data pipelines"
```

## Task 2: Split RPC And Direct Oracle Chains

**Files:**
- Create: `src/pm5min/data/pipelines/direct_sync.py`
- Create: `src/pm5min/data/pipelines/direct_oracle_prices.py`
- Modify: `src/pm5min/data/handlers.py`
- Modify: `src/pm5min/data/compat.py`
- Modify: `tests/test_pm5min_cli.py`
- Modify: `tests/test_pm5min_data_pipelines.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_pm5min_direct_sync_pipeline_uses_5m_layout(tmp_path, monkeypatch):
    cfg = DataConfig.build(market="sol", cycle="5m", root=tmp_path)
    payload = sync_streams_from_rpc(cfg, start_ts=1, end_ts=2, rpc=_FakePolygonRpc())
    assert payload["target_root"].endswith("cycle=5m/asset=sol")


def test_pm5min_direct_oracle_pipeline_rebuilds_5m_outputs(tmp_path, monkeypatch):
    cfg = DataConfig.build(market="sol", cycle="5m", root=tmp_path)
    payload = backfill_direct_oracle_prices(cfg, workers=1)
    assert payload["cycle"] == "5m"
```

```python
def test_pm5min_cli_does_not_reference_pm15min_direct_pipeline_modules() -> None:
    compat_text = Path(... / "src" / "pm5min" / "data" / "compat.py").read_text(encoding="utf-8")
    assert "pm15min.data.pipelines.direct_sync" not in compat_text
    assert "pm15min.data.pipelines.direct_oracle_prices" not in compat_text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_data_pipelines.py tests/test_pm5min_cli.py -k 'direct_sync_pipeline or direct_oracle_pipeline or direct_pipeline_modules'`
Expected: FAIL because these pm5min pipeline modules are still missing and compat still imports pm15min direct pipeline modules.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm5min/data/pipelines/direct_sync.py
def sync_streams_from_rpc(cfg, *, start_ts, end_ts, rpc=None, include_block_timestamp=False, chunk_blocks=1000, sleep_sec=0.02):
    ...


def sync_datafeeds_from_rpc(cfg, *, start_ts, end_ts, rpc=None, chunk_blocks=5000, sleep_sec=0.02):
    ...


def sync_settlement_truth_from_rpc(cfg, *, rpc=None, start_ts=None, end_ts=None, chunk_blocks=3000, sleep_sec=0.01):
    ...
```

```python
# src/pm5min/data/pipelines/direct_oracle_prices.py
def sync_polymarket_oracle_prices_direct(cfg, *, start_ts=None, end_ts=None, ...):
    ...


def backfill_direct_oracle_prices(cfg, *, workers=1, flush_every=200, timeout_sec=30.0, max_retries=6, sleep_sec=0.0):
    ...
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_data_pipelines.py tests/test_pm5min_cli.py -k 'direct_sync_pipeline or direct_oracle_pipeline or direct_pipeline_modules'`
Expected: PASS

- [ ] **Step 5: Run baseline smoke**

Run: `PYTHONPATH=src pytest -q tests/test_cli.py -k 'top_level_layout_command or live_show_config'`
Expected: PASS

Run: `PYTHONPATH=src pytest -q tests/test_live_service.py -k 'prewarm_live_signal_inputs or score_live_latest'`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm5min/data/pipelines/direct_sync.py src/pm5min/data/pipelines/direct_oracle_prices.py src/pm5min/data/handlers.py src/pm5min/data/compat.py tests/test_pm5min_data_pipelines.py tests/test_pm5min_cli.py
git commit -m "feat: split pm5min rpc and direct oracle pipelines"
```

## Task 3: Split Orderbook Capture And Runtime

**Files:**
- Create: `src/pm5min/data/pipelines/orderbook_recent.py`
- Create: `src/pm5min/data/pipelines/orderbook_recording.py`
- Create: `src/pm5min/data/pipelines/orderbook_runtime.py`
- Modify: `src/pm5min/data/handlers.py`
- Modify: `src/pm5min/data/compat.py`
- Create: `tests/test_pm5min_data_orderbooks.py`
- Modify: `tests/test_pm5min_cli.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_pm5min_orderbook_recording_persists_under_5m_root(tmp_path, monkeypatch):
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path)
    summary = persist_captured_orderbooks_once(cfg, batch=_fake_batch())
    assert "cycle=5m/asset=sol" in summary["depth_path"]


def test_pm5min_orderbook_runtime_reports_5m_cycle(tmp_path, monkeypatch):
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path)
    payload = run_orderbook_recorder(cfg, iterations=1, loop=False, provider=_FakeOrderbookProvider())
    assert payload["cycle"] == "5m"
```

```python
def test_pm5min_cli_does_not_reference_pm15min_orderbook_pipeline_modules() -> None:
    compat_text = Path(... / "src" / "pm5min" / "data" / "compat.py").read_text(encoding="utf-8")
    assert "pm15min.data.pipelines.orderbook_recording" not in compat_text
    assert "pm15min.data.pipelines.orderbook_runtime" not in compat_text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_data_orderbooks.py tests/test_pm5min_cli.py -k 'orderbook_recording or orderbook_runtime or orderbook_pipeline_modules'`
Expected: FAIL because pm5min orderbook runtime modules do not exist yet and compat still points at pm15min.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm5min/data/pipelines/orderbook_recording.py
def capture_orderbooks_once(...):
    ...


def persist_captured_orderbooks_once(cfg, *, batch):
    ...


def build_orderbook_index_from_depth(cfg, *, date_str):
    ...
```

```python
# src/pm5min/data/pipelines/orderbook_runtime.py
def run_orderbook_recorder(cfg, *, client=None, provider=None, iterations=1, loop=False, sleep_sec=None, ...):
    ...
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_data_orderbooks.py tests/test_pm5min_cli.py -k 'orderbook_recording or orderbook_runtime or orderbook_pipeline_modules'`
Expected: PASS

- [ ] **Step 5: Run baseline smoke**

Run: `PYTHONPATH=src pytest -q tests/test_cli.py -k 'top_level_layout_command or live_show_config'`
Expected: PASS

Run: `PYTHONPATH=src pytest -q tests/test_live_service.py -k 'prewarm_live_signal_inputs or score_live_latest'`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm5min/data/pipelines/orderbook_recent.py src/pm5min/data/pipelines/orderbook_recording.py src/pm5min/data/pipelines/orderbook_runtime.py src/pm5min/data/handlers.py src/pm5min/data/compat.py tests/test_pm5min_data_orderbooks.py tests/test_pm5min_cli.py
git commit -m "feat: split pm5min orderbook runtime"
```

## Task 4: Split Orderbook Fleet And Backtest Refresh

**Files:**
- Create: `src/pm5min/data/pipelines/orderbook_fleet.py`
- Create: `src/pm5min/data/pipelines/backtest_refresh.py`
- Modify: `src/pm5min/data/handlers.py`
- Modify: `src/pm5min/data/compat.py`
- Modify: `tests/test_pm5min_cli.py`
- Modify: `tests/test_pm5min_data_orderbooks.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_pm5min_orderbook_fleet_process_command_uses_pm5min_runtime(monkeypatch):
    payload = run_orderbook_recorder_fleet(markets="sol", cycle="5m", surface="live", iterations=1)
    assert payload["cycle"] == "5m"


def test_pm5min_backtest_refresh_defaults_to_5m(tmp_path):
    payload = run_backtest_data_refresh(markets=["sol"], root=tmp_path)
    assert payload["cycle"] == "5m"
```

```python
def test_pm5min_cli_does_not_reference_pm15min_fleet_refresh_modules() -> None:
    compat_text = Path(... / "src" / "pm5min" / "data" / "compat.py").read_text(encoding="utf-8")
    assert "pm15min.data.pipelines.orderbook_fleet" not in compat_text
    assert "pm15min.data.pipelines.backtest_refresh" not in compat_text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_data_orderbooks.py tests/test_pm5min_cli.py -k 'orderbook_fleet or backtest_refresh or fleet_refresh_modules'`
Expected: FAIL because these orchestration modules still come from pm15min compat wrappers.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm5min/data/pipelines/orderbook_fleet.py
def run_orderbook_recorder_fleet(...):
    ...
```

```python
# src/pm5min/data/pipelines/backtest_refresh.py
@dataclass(frozen=True)
class BacktestRefreshOptions:
    cycle: str = "5m"
    ...


def run_backtest_data_refresh(*, markets, root=None, options=None):
    ...
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_data_orderbooks.py tests/test_pm5min_cli.py -k 'orderbook_fleet or backtest_refresh or fleet_refresh_modules'`
Expected: PASS

- [ ] **Step 5: Run baseline smoke**

Run: `PYTHONPATH=src pytest -q tests/test_cli.py -k 'top_level_layout_command or live_show_config'`
Expected: PASS

Run: `PYTHONPATH=src pytest -q tests/test_live_service.py -k 'prewarm_live_signal_inputs or score_live_latest'`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm5min/data/pipelines/orderbook_fleet.py src/pm5min/data/pipelines/backtest_refresh.py src/pm5min/data/handlers.py src/pm5min/data/compat.py tests/test_pm5min_data_orderbooks.py tests/test_pm5min_cli.py
git commit -m "feat: split pm5min orderbook fleet and backtest refresh"
```

## Task 5: Split The Live Foundation Last

**Files:**
- Create: `src/pm5min/data/pipelines/foundation_shared.py`
- Create: `src/pm5min/data/pipelines/foundation_runtime.py`
- Modify: `src/pm5min/data/handlers.py`
- Modify: `src/pm5min/data/compat.py`
- Create: `tests/test_pm5min_data_foundation.py`
- Modify: `tests/test_pm5min_cli.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_pm5min_foundation_runtime_reports_5m_cycle(tmp_path, monkeypatch):
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path)
    payload = run_live_data_foundation(cfg, iterations=1, loop=False)
    assert payload["cycle"] == "5m"


def test_pm5min_foundation_runtime_uses_pm5min_local_steps(tmp_path, monkeypatch):
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path)
    payload = run_live_data_foundation(cfg, iterations=1, loop=False)
    assert "oracle_prices_5m" in json.dumps(payload)
```

```python
def test_pm5min_cli_does_not_reference_pm15min_foundation_runtime() -> None:
    compat_text = Path(... / "src" / "pm5min" / "data" / "compat.py").read_text(encoding="utf-8")
    assert "pm15min.data.pipelines.foundation_runtime" not in compat_text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_data_foundation.py tests/test_pm5min_cli.py -k 'foundation_runtime or pm15min_foundation_runtime'`
Expected: FAIL because the final orchestration still routes through compat.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm5min/data/pipelines/foundation_shared.py
def run_live_data_foundation_shared(...):
    ...
```

```python
# src/pm5min/data/pipelines/foundation_runtime.py
def run_live_data_foundation(cfg, *, iterations=1, loop=False, sleep_sec=1.0, ...):
    ...
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_data_foundation.py tests/test_pm5min_cli.py -k 'foundation_runtime or pm15min_foundation_runtime'`
Expected: PASS

- [ ] **Step 5: Run full boundary verification**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py tests/test_pmshared_architecture.py tests/test_architecture_guards.py tests/test_cli.py tests/test_live_service.py`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm5min/data/pipelines/foundation_shared.py src/pm5min/data/pipelines/foundation_runtime.py src/pm5min/data/handlers.py src/pm5min/data/compat.py tests/test_pm5min_data_foundation.py tests/test_pm5min_cli.py
git commit -m "feat: split pm5min live foundation runtime"
```

## Task 6: Delete Obsolete Compat Hooks And Recheck Boundaries

**Files:**
- Modify: `src/pm5min/data/compat.py`
- Modify: `tests/test_pm5min_cli.py`
- Modify: `tests/test_architecture_guards.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_pm5min_data_compat_has_no_pm15min_pipeline_imports() -> None:
    text = Path("src/pm5min/data/compat.py").read_text(encoding="utf-8")
    assert "pm15min.data.pipelines" not in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k 'compat_has_no_pm15min_pipeline_imports'`
Expected: FAIL until all remaining wrappers are removed.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm5min/data/compat.py
"""Temporary boundary adapters for pm5min/data.

Keep this file empty or delete it once the final pm15min pipeline wrapper is removed.
"""
```

- [ ] **Step 4: Run tests to verify it passes**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k 'compat_has_no_pm15min_pipeline_imports'`
Expected: PASS

- [ ] **Step 5: Run full boundary verification**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py tests/test_pmshared_architecture.py tests/test_architecture_guards.py tests/test_cli.py tests/test_live_service.py`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm5min/data/compat.py tests/test_pm5min_cli.py tests/test_architecture_guards.py
git commit -m "feat: remove final pm5min data pipeline compat hooks"
```
