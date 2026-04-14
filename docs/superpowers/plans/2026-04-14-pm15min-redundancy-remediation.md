# PM15Min Redundancy Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove true redundancy from `src/pm15min` without changing current `main` behavior, then narrow `pm15min` back to clear 15-minute ownership where shared logic already exists.

**Architecture:** Use a characterization-first refactor. First lock current behavior with focused regression tests. Then merge duplicated rules into one helper per concern, redirect internal callers, and only then delete thin wrappers or compatibility shells. Put logic into `pmshared` only when it is cycle-neutral and already duplicated across `pm15min` and `pm5min`; keep live/research orchestration inside `pm15min`.

**Tech Stack:** Python 3.11+, existing `pm15min` / `pm5min` / `pmshared` packages, `pytest`, current CLI/runtime entrypoints, existing layout and manifest conventions.

---

## Scope

- In scope: duplicated orderbook normalization, repeated runtime IO helpers, repeated live gateway-resolution glue, repeated research backtest helpers, thin compatibility shells already covered by `pmshared`, and `pm15min` / `pm5min` parallel helper modules that are effectively the same code.
- Out of scope: strategy behavior changes, profile tuning, file layout changes for runtime artifacts, quote or fill rule changes, or any redesign of the public CLI surface.

## Guardrails

- Do not change behavior and refactor structure in the same step unless characterization tests already cover that path.
- Do not delete public entry packages such as `src/pm15min/live/liquidity/__init__.py` or `src/pm15min/live/regime/__init__.py` in the first pass; they are thin, but they still serve stable imports.
- Do not remove `build_oracle_prices_15m` or `build_truth_15m` until all internal callers and tests stop depending on them.
- Prefer extracting one shared helper per duplicated rule over a broad “cleanup” commit that moves many concepts at once.
- Keep `pm15min` and `pm5min` behavior stable while shared extraction is in flight.

## File Structure

- Create: `src/pmshared/orderbook.py`
  - Single source of truth for orderbook level normalization and timestamp coercion used by CLOB fetch, provider cache, and recorder ingest.
- Create: `src/pm15min/data/pipelines/runtime_io.py`
  - Shared helper for data-runtime state writes, log appends, and UTC timestamp formatting.
- Create: `src/pm15min/live/trading/gateway_resolution.py`
  - Single helper for “use injected gateway or build one from adapter override” logic.
- Create: `src/pm15min/research/backtests/catalog_enrichment.py`
  - Shared helper for merging market catalog metadata back into replay/surface tables.
- Create: `src/pm15min/research/backtests/match_keys.py`
  - Shared helper for normalizing replay rows into stable matching keys.
- Create: `src/pm15min/research/locks.py`
  - Shared exclusive-lock context used by freshness repair and backtest fallback rebuilds.
- Create: `src/pmshared/data_layout_paths.py`
  - Shared layout-path builders if `pm15min` and `pm5min` path helpers remain structurally identical after verification.
- Create: `src/pmshared/research_layout_helpers.py`
  - Shared research layout token, date-window, and slug helpers if the duplicated helper modules stay aligned.
- Modify: `src/pm15min/data/sources/polymarket_clob.py`
  - Replace local normalization helpers with the shared orderbook helper.
- Modify: `src/pm15min/data/sources/orderbook_provider.py`
  - Replace local timestamp and level normalization with the shared orderbook helper.
- Modify: `src/pm15min/data/pipelines/orderbook_recording.py`
  - Replace local timestamp and level normalization with the shared orderbook helper.
- Modify: `src/pm15min/data/pipelines/orderbook_runtime.py`
  - Replace duplicated runtime state/log helpers with `runtime_io.py`.
- Modify: `src/pm15min/data/pipelines/foundation_runtime.py`
  - Replace duplicated runtime state/log helpers with `runtime_io.py`.
- Modify: `src/pm15min/live/service/__init__.py`
  - Collapse unnecessary relay layers while preserving the public module API.
- Modify: `src/pm15min/live/service/facade_helpers.py`
  - Remove if all call sites can bind directly without this extra relay layer.
- Modify: `src/pm15min/live/service/wiring.py`
  - Remove if direct dict construction inside the public facade is simpler and covered.
- Modify: `src/pm15min/live/service/operation.py`
  - Remove duplicated gateway-resolution logic in favor of `gateway_resolution.py`.
- Modify: `src/pm15min/live/actions/service.py`
  - Delete or inline if it remains a pure pass-through after gateway cleanup.
- Modify: `src/pm15min/live/runner/service.py`
  - Delete if it remains a pure two-line relay.
- Modify: `src/pm15min/live/runner/api.py`
  - Reuse `gateway_resolution.py` instead of open-coded gateway selection.
- Modify: `src/pm15min/live/redeem/__init__.py`
  - Reuse `gateway_resolution.py` instead of open-coded gateway selection.
- Modify: `src/pm15min/live/operator/actions.py`
  - Stop wrapping followup builders if the wrapper adds no behavior.
- Modify: `src/pm15min/live/operator/followups.py`
  - Delete if it remains a pure re-export layer.
- Modify: `src/pm15min/core/assets.py`
  - Keep as a temporary compatibility shim only if non-internal callers still need it; otherwise delete after import migration.
- Modify: `src/pm15min/data/io/json_files.py`
  - Migrate callers to `pmshared.io.json_files`, then delete or keep as a short-lived shim.
- Modify: `src/pm15min/data/io/parquet.py`
  - Migrate callers to `pmshared.io.parquet`, then delete or keep as a short-lived shim.
- Modify: `src/pm15min/data/io/ndjson_zst.py`
  - Convert to a thin wrapper over `pmshared.io.ndjson_zst`, then delete after callers move.
- Modify: `src/pm15min/data/layout/helpers.py`
  - Migrate callers to `pmshared.time`, then delete or keep as a short-lived shim.
- Modify: `src/pm15min/research/backtests/depth_replay.py`
  - Use shared catalog enrichment helper.
- Modify: `src/pm15min/research/backtests/orderbook_surface.py`
  - Use shared catalog enrichment helper.
- Modify: `src/pm15min/research/backtests/fills.py`
  - Use shared match-key helper.
- Modify: `src/pm15min/research/backtests/decision_quote_surface.py`
  - Use shared match-key helper.
- Modify: `src/pm15min/research/freshness.py`
  - Use `research/locks.py`; later remove transitional build aliases.
- Modify: `src/pm15min/research/backtests/data_surface_fallback.py`
  - Use `research/locks.py`.
- Modify: `src/pm15min/research/bundles/loader.py`
  - Collapse repeated “fixed filename + exists check + JSON read” helpers.
- Modify: `src/pm15min/data/layout/paths.py`
  - Compare with `pm5min` twin; extract if still identical.
- Modify: `src/pm5min/data/layout/paths.py`
  - Point to the shared path helper if extraction is still justified.
- Modify: `src/pm15min/research/layout_helpers.py`
  - Compare with `pm5min` twin; extract if still identical.
- Modify: `src/pm5min/research/layout_helpers.py`
  - Point to the shared research-layout helper if extraction is still justified.
- Modify: `src/pm15min/data/pipelines/oracle_prices.py`
  - Remove `build_oracle_prices_15m` only in the final alias cleanup phase.
- Modify: `src/pm15min/data/pipelines/truth.py`
  - Remove `build_truth_15m` only in the final alias cleanup phase.
- Modify: targeted tests under `tests/`
  - Extend focused regression coverage before each cleanup phase.

## Recommended Execution Order

1. Behavior locks
2. Data duplication cleanup
3. Live wrapper collapse
4. Research helper consolidation
5. Shared-shell migration
6. `pm15min` / `pm5min` parallel-helper extraction
7. Alias removal and full-suite verification

### Task 1: Lock Current Behavior With Characterization Tests

**Files:**
- Modify: `tests/test_data_orderbook.py`
- Modify: `tests/test_data_orderbook_provider.py`
- Modify: `tests/test_data_recorder_runtime.py`
- Modify: `tests/test_data_foundation_runtime.py`
- Modify: `tests/test_live_service.py`
- Modify: `tests/test_live_actions.py`
- Modify: `tests/test_live_runner.py`
- Modify: `tests/test_research_backtest_depth_replay.py`
- Modify: `tests/test_research_backtest_decision_quote_surface.py`
- Modify: `tests/test_research_backtest_fills.py`
- Modify: `tests/test_research_freshness.py`
- Modify: `tests/test_research_bundle_parity.py`

- [ ] **Step 1: Add focused tests that pin duplicated behavior before moving code**
  - Add orderbook tests that assert the same input payload yields the same normalized levels and timestamp regardless of whether the path is CLOB fetch, provider cache, or recorder ingest.
  - Add live tests that assert public imports from `pm15min.live.service`, `pm15min.live.actions`, and `pm15min.live.runner` still behave the same after wrapper collapse.
  - Add research tests that assert replay/catalog enrichment and match-key generation still produce the same rows after helper extraction.
  - Add freshness and bundle-loader tests that pin lock behavior and file-loading error messages.

- [ ] **Step 2: Run the focused characterization suite and confirm the baseline**

```bash
PYTHONPATH=src pytest -q \
  tests/test_data_orderbook.py \
  tests/test_data_orderbook_provider.py \
  tests/test_data_recorder_runtime.py \
  tests/test_data_foundation_runtime.py \
  tests/test_live_service.py \
  tests/test_live_actions.py \
  tests/test_live_runner.py \
  tests/test_research_backtest_depth_replay.py \
  tests/test_research_backtest_decision_quote_surface.py \
  tests/test_research_backtest_fills.py \
  tests/test_research_freshness.py \
  tests/test_research_bundle_parity.py
```

Expected: PASS on current `main` before any cleanup work starts.

- [ ] **Step 3: Commit the test-only safety net**

```bash
git add \
  tests/test_data_orderbook.py \
  tests/test_data_orderbook_provider.py \
  tests/test_data_recorder_runtime.py \
  tests/test_data_foundation_runtime.py \
  tests/test_live_service.py \
  tests/test_live_actions.py \
  tests/test_live_runner.py \
  tests/test_research_backtest_depth_replay.py \
  tests/test_research_backtest_decision_quote_surface.py \
  tests/test_research_backtest_fills.py \
  tests/test_research_freshness.py \
  tests/test_research_bundle_parity.py
git commit -m "test: add redundancy characterization coverage"
```

### Task 2: Merge Data-Side Duplicate Rules

**Files:**
- Create: `src/pmshared/orderbook.py`
- Create: `src/pm15min/data/pipelines/runtime_io.py`
- Modify: `src/pm15min/data/sources/polymarket_clob.py`
- Modify: `src/pm15min/data/sources/orderbook_provider.py`
- Modify: `src/pm15min/data/pipelines/orderbook_recording.py`
- Modify: `src/pm15min/data/pipelines/orderbook_runtime.py`
- Modify: `src/pm15min/data/pipelines/foundation_runtime.py`
- Modify: `tests/test_data_orderbook.py`
- Modify: `tests/test_data_orderbook_provider.py`
- Modify: `tests/test_data_recorder_runtime.py`
- Modify: `tests/test_data_foundation_runtime.py`

- [ ] **Step 1: Extract one shared orderbook normalization helper**
  - Move timestamp coercion and level normalization into `src/pmshared/orderbook.py`.
  - Keep the helper cycle-neutral and payload-format-neutral.
  - Preserve current behavior for invalid rows, zero-size rows, numeric timestamps, and ISO timestamps.

- [ ] **Step 2: Redirect the three data paths to the shared helper**
  - Replace local normalization functions in `polymarket_clob.py`, `orderbook_provider.py`, and `orderbook_recording.py`.
  - Remove only the duplicated local helpers that are now fully covered by the shared implementation.

- [ ] **Step 3: Extract shared runtime IO helpers for recorder and foundation**
  - Move UTC timestamp formatting, state writes, and JSONL log appends into `src/pm15min/data/pipelines/runtime_io.py`.
  - Update both `orderbook_runtime.py` and `foundation_runtime.py` to use that shared helper.

- [ ] **Step 4: Run focused data tests**

```bash
PYTHONPATH=src pytest -q \
  tests/test_data_orderbook.py \
  tests/test_data_orderbook_provider.py \
  tests/test_data_recorder_runtime.py \
  tests/test_data_foundation_runtime.py \
  tests/test_data_pipelines.py
```

Expected: PASS with no output-schema changes.

- [ ] **Step 5: Commit the data cleanup**

```bash
git add \
  src/pmshared/orderbook.py \
  src/pm15min/data/pipelines/runtime_io.py \
  src/pm15min/data/sources/polymarket_clob.py \
  src/pm15min/data/sources/orderbook_provider.py \
  src/pm15min/data/pipelines/orderbook_recording.py \
  src/pm15min/data/pipelines/orderbook_runtime.py \
  src/pm15min/data/pipelines/foundation_runtime.py \
  tests/test_data_orderbook.py \
  tests/test_data_orderbook_provider.py \
  tests/test_data_recorder_runtime.py \
  tests/test_data_foundation_runtime.py
git commit -m "refactor: merge duplicated data orderbook helpers"
```

### Task 3: Collapse Live Thin Wrappers Without Breaking Public Imports

**Files:**
- Create: `src/pm15min/live/trading/gateway_resolution.py`
- Modify: `src/pm15min/live/service/__init__.py`
- Modify: `src/pm15min/live/service/facade_helpers.py`
- Modify: `src/pm15min/live/service/wiring.py`
- Modify: `src/pm15min/live/service/operation.py`
- Modify: `src/pm15min/live/actions/service.py`
- Modify: `src/pm15min/live/runner/service.py`
- Modify: `src/pm15min/live/runner/api.py`
- Modify: `src/pm15min/live/redeem/__init__.py`
- Modify: `src/pm15min/live/operator/actions.py`
- Modify: `src/pm15min/live/operator/followups.py`
- Modify: `tests/test_live_service.py`
- Modify: `tests/test_live_actions.py`
- Modify: `tests/test_live_runner.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Extract one shared gateway-resolution helper**
  - Implement the common “use injected gateway or build from adapter override” rule once in `gateway_resolution.py`.
  - Reuse it in `service/operation.py`, `runner/api.py`, and `redeem/__init__.py`.

- [ ] **Step 2: Inline relay-only service layers**
  - Fold `facade_helpers.py` and `wiring.py` into `service/__init__.py` if they remain pure dict relays.
  - Delete `actions/service.py` and `runner/service.py` only after their callers point directly to the real implementations.
  - Keep `pm15min.live.service`, `pm15min.live.actions`, and `pm15min.live.runner` import surfaces stable.

- [ ] **Step 3: Remove followup re-export noise**
  - Stop re-wrapping followup builders in `operator/actions.py` if the wrapper adds no behavior.
  - Delete `operator/followups.py` only after direct imports are updated.

- [ ] **Step 4: Run focused live tests**

```bash
PYTHONPATH=src pytest -q \
  tests/test_live_service.py \
  tests/test_live_actions.py \
  tests/test_live_runner.py \
  tests/test_cli.py
```

Expected: PASS with no change to public command behavior.

- [ ] **Step 5: Commit the live cleanup**

```bash
git add \
  src/pm15min/live/trading/gateway_resolution.py \
  src/pm15min/live/service/__init__.py \
  src/pm15min/live/service/facade_helpers.py \
  src/pm15min/live/service/wiring.py \
  src/pm15min/live/service/operation.py \
  src/pm15min/live/actions/service.py \
  src/pm15min/live/runner/service.py \
  src/pm15min/live/runner/api.py \
  src/pm15min/live/redeem/__init__.py \
  src/pm15min/live/operator/actions.py \
  src/pm15min/live/operator/followups.py \
  tests/test_live_service.py \
  tests/test_live_actions.py \
  tests/test_live_runner.py \
  tests/test_cli.py
git commit -m "refactor: collapse redundant live wrapper layers"
```

### Task 4: Merge Research Duplicate Helpers

**Files:**
- Create: `src/pm15min/research/backtests/catalog_enrichment.py`
- Create: `src/pm15min/research/backtests/match_keys.py`
- Create: `src/pm15min/research/locks.py`
- Modify: `src/pm15min/research/backtests/depth_replay.py`
- Modify: `src/pm15min/research/backtests/orderbook_surface.py`
- Modify: `src/pm15min/research/backtests/fills.py`
- Modify: `src/pm15min/research/backtests/decision_quote_surface.py`
- Modify: `src/pm15min/research/freshness.py`
- Modify: `src/pm15min/research/backtests/data_surface_fallback.py`
- Modify: `src/pm15min/research/bundles/loader.py`
- Modify: `tests/test_research_backtest_depth_replay.py`
- Modify: `tests/test_research_backtest_decision_quote_surface.py`
- Modify: `tests/test_research_backtest_fills.py`
- Modify: `tests/test_research_freshness.py`
- Modify: `tests/test_research_bundle_parity.py`

- [ ] **Step 1: Extract shared replay/catalog enrichment helper**
  - Move the “merge market catalog metadata back into result tables” rule into `catalog_enrichment.py`.
  - Use it from both `depth_replay.py` and `orderbook_surface.py`.

- [ ] **Step 2: Extract shared match-key helper**
  - Move the replay-row-to-match-key normalization logic into `match_keys.py`.
  - Use it from both `fills.py` and `decision_quote_surface.py`.

- [ ] **Step 3: Extract the exclusive-lock helper**
  - Move the identical `_exclusive_lock` context into `src/pm15min/research/locks.py`.
  - Update `freshness.py` and `data_surface_fallback.py` to import it instead of re-declaring it.

- [ ] **Step 4: Simplify bundle loader file reads**
  - Collapse repeated “build fixed file path, check existence, read JSON/manifest” patterns in `bundles/loader.py`.
  - Keep error messages stable where tests already assert them.

- [ ] **Step 5: Run focused research tests**

```bash
PYTHONPATH=src pytest -q \
  tests/test_research_backtest_depth_replay.py \
  tests/test_research_backtest_decision_quote_surface.py \
  tests/test_research_backtest_fills.py \
  tests/test_research_freshness.py \
  tests/test_research_bundle_parity.py
```

Expected: PASS with unchanged replay and bundle-loading outputs.

- [ ] **Step 6: Commit the research cleanup**

```bash
git add \
  src/pm15min/research/backtests/catalog_enrichment.py \
  src/pm15min/research/backtests/match_keys.py \
  src/pm15min/research/locks.py \
  src/pm15min/research/backtests/depth_replay.py \
  src/pm15min/research/backtests/orderbook_surface.py \
  src/pm15min/research/backtests/fills.py \
  src/pm15min/research/backtests/decision_quote_surface.py \
  src/pm15min/research/freshness.py \
  src/pm15min/research/backtests/data_surface_fallback.py \
  src/pm15min/research/bundles/loader.py \
  tests/test_research_backtest_depth_replay.py \
  tests/test_research_backtest_decision_quote_surface.py \
  tests/test_research_backtest_fills.py \
  tests/test_research_freshness.py \
  tests/test_research_bundle_parity.py
git commit -m "refactor: merge duplicated research helpers"
```

### Task 5: Retire `pm15min` Shells Already Covered By `pmshared`

**Files:**
- Modify: `src/pm15min/core/assets.py`
- Modify: `src/pm15min/data/io/json_files.py`
- Modify: `src/pm15min/data/io/parquet.py`
- Modify: `src/pm15min/data/io/ndjson_zst.py`
- Modify: `src/pm15min/data/layout/helpers.py`
- Modify: internal importers under `src/pm15min/` and `src/pm5min/`
- Modify: `tests/test_data_json_files.py`
- Modify: `tests/test_data_exports.py`
- Modify: `tests/test_data_layout.py`
- Modify: any other tests still importing the old shim paths

- [ ] **Step 1: Move internal callers to the real shared modules**
  - Replace internal imports of `pm15min.core.assets` with `pmshared.assets`.
  - Replace internal imports of `pm15min.data.io.json_files` and `pm15min.data.io.parquet` with `pmshared.io.*`.
  - Replace internal imports of `pm15min.data.layout.helpers` with `pmshared.time`.
  - Convert `pm15min.data.io.ndjson_zst` into a temporary wrapper over `pmshared.io.ndjson_zst`.

- [ ] **Step 2: Decide per module whether to delete or deprecate**
  - Delete pure shells immediately if only internal callers used them.
  - Keep a short-lived shim only if test fixtures or documented external imports still rely on that path.

- [ ] **Step 3: Run targeted shared-shell tests**

```bash
PYTHONPATH=src pytest -q \
  tests/test_data_json_files.py \
  tests/test_data_exports.py \
  tests/test_data_layout.py \
  tests/test_data_foundation_runtime.py \
  tests/test_live_quotes.py \
  tests/test_live_execution.py
```

Expected: PASS with no file-format or layout regression.

- [ ] **Step 4: Commit the import migration**

```bash
git add \
  src/pm15min/core/assets.py \
  src/pm15min/data/io/json_files.py \
  src/pm15min/data/io/parquet.py \
  src/pm15min/data/io/ndjson_zst.py \
  src/pm15min/data/layout/helpers.py \
  tests/test_data_json_files.py \
  tests/test_data_exports.py \
  tests/test_data_layout.py
git commit -m "refactor: migrate pm15min shared shells to pmshared"
```

### Task 6: Extract Parallel `pm15min` / `pm5min` Helper Modules Only Where The Code Is Still Identical

**Files:**
- Create: `src/pmshared/data_layout_paths.py`
- Create: `src/pmshared/research_layout_helpers.py`
- Modify: `src/pm15min/data/layout/paths.py`
- Modify: `src/pm5min/data/layout/paths.py`
- Modify: `src/pm15min/research/layout_helpers.py`
- Modify: `src/pm5min/research/layout_helpers.py`
- Modify: `tests/test_data_layout.py`
- Modify: `tests/test_research_layout.py`
- Modify: `tests/test_pm5min_data_pipelines.py`
- Modify: `tests/test_pm5min_research_service.py`

- [ ] **Step 1: Re-check both twins before extraction**
  - Verify the `pm15min` and `pm5min` helper files are still structurally identical except for formatting.
  - If a semantic difference appears, stop and keep them separate.

- [ ] **Step 2: Extract only the truly shared helper code**
  - Move shared data-layout path builders into `pmshared/data_layout_paths.py`.
  - Move shared research layout token/date helpers into `pmshared/research_layout_helpers.py`.
  - Keep package-specific wrappers only if they add package-local naming or import convenience.

- [ ] **Step 3: Run the layout-focused regression suite**

```bash
PYTHONPATH=src pytest -q \
  tests/test_data_layout.py \
  tests/test_research_layout.py \
  tests/test_pm5min_data_pipelines.py \
  tests/test_pm5min_research_service.py
```

Expected: PASS with no path-shape changes.

- [ ] **Step 4: Commit the cross-package shared extraction**

```bash
git add \
  src/pmshared/data_layout_paths.py \
  src/pmshared/research_layout_helpers.py \
  src/pm15min/data/layout/paths.py \
  src/pm5min/data/layout/paths.py \
  src/pm15min/research/layout_helpers.py \
  src/pm5min/research/layout_helpers.py \
  tests/test_data_layout.py \
  tests/test_research_layout.py
git commit -m "refactor: extract shared pm15min pm5min layout helpers"
```

### Task 7: Remove Transitional Aliases And Run Full Verification

**Files:**
- Modify: `src/pm15min/data/pipelines/oracle_prices.py`
- Modify: `src/pm15min/data/pipelines/truth.py`
- Modify: `src/pm15min/research/freshness.py`
- Modify: CLI/data/research callers still using the old names
- Modify: `tests/test_data_builders.py`
- Modify: `tests/test_research_builders.py`
- Modify: `tests/test_research_freshness.py`
- Modify: any remaining tests importing the alias names

- [ ] **Step 1: Remove the final naming debt**
  - Replace `build_oracle_prices_15m` with `build_oracle_prices_table` at call sites.
  - Replace `build_truth_15m` with `build_truth_table` at call sites.
  - Remove the last alias-only lines from `oracle_prices.py`, `truth.py`, and `freshness.py`.

- [ ] **Step 2: Run alias-focused tests**

```bash
PYTHONPATH=src pytest -q \
  tests/test_data_builders.py \
  tests/test_research_builders.py \
  tests/test_research_freshness.py \
  tests/test_data_foundation_runtime.py
```

Expected: PASS with no stale alias imports left.

- [ ] **Step 3: Run the full test suite**

```bash
PYTHONPATH=src pytest -q tests
```

Expected: PASS with no regression relative to current `main`.

- [ ] **Step 4: Commit the final cleanup**

```bash
git add \
  src/pm15min/data/pipelines/oracle_prices.py \
  src/pm15min/data/pipelines/truth.py \
  src/pm15min/research/freshness.py \
  tests/test_data_builders.py \
  tests/test_research_builders.py \
  tests/test_research_freshness.py
git commit -m "refactor: remove final redundancy aliases"
```

## Acceptance Criteria

- There is exactly one normalization implementation for orderbook levels and orderbook timestamps.
- Recorder and foundation runtimes no longer each own their own state/log helper copies.
- Live gateway resolution exists in one helper, not repeated across service, runner, and redeem paths.
- `pm15min.live.service` no longer depends on multiple relay-only layers that add no behavior.
- Research replay/catalog enrichment and match-key generation each exist in one helper.
- The exclusive lock helper exists in one module.
- Pure shells already covered by `pmshared` are either removed or explicitly retained as short-lived shims with no internal callers.
- Shared helper twins across `pm15min` and `pm5min` are extracted only if they remain truly identical.
- The full test suite passes after the final alias cleanup.

## Risks And Stop Conditions

- Stop if any extracted helper starts forcing `pm15min` and `pm5min` to share behavior that is only accidentally similar today.
- Stop if deleting a shim would break documented external imports that are still intentionally supported.
- Stop if the live wrapper collapse starts changing import paths or CLI output instead of only reducing indirection.
- Stop if replay/fill helper extraction changes row order, key shape, or manifest/report payloads without explicit test coverage.
