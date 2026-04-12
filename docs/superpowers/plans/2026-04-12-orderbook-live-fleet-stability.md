# Orderbook Live Fleet Stability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep only the `15m live` and `5m live` orderbook fleets, prevent unbounded backlog growth, avoid deliberate orderbook drops, and reduce write-side pressure enough for the server to stay stable.

**Architecture:** Preserve the existing recorder and fleet structure, but change the async persistence behavior from "drop oldest when full" to "small bounded queue with backpressure". Reduce persistence pressure by lowering compression cost and by throttling expensive recent-cache rewrites without changing data layout. Deploy only the recorder-related changes to the server and restart the live fleets without touching the active research loop.

**Tech Stack:** Python 3.11+, pandas/parquet, zstd-compressed ndjson, bash entrypoint scripts, pytest.

---

### Task 1: Define the recorder behavior with tests first

**Files:**
- Modify: `tests/test_data_recorder_runtime.py`
- Modify: `tests/test_data_pipelines.py`

- [ ] **Step 1: Add a recorder test that proves bounded async mode can block instead of dropping**

```python
def test_run_orderbook_recorder_async_persist_bounded_queue_can_backpressure_without_drop(...):
    ...
    summary = run_orderbook_recorder(
        cfg,
        iterations=3,
        loop=False,
        async_persist=True,
        max_pending_batches=1,
        drop_oldest_when_full=False,
        ...
    )
    assert summary["dropped_batches"] == 0
```

- [ ] **Step 2: Add a recent-cache test that proves throttled rewrites still keep the newest rows**

```python
def test_update_recent_orderbook_index_can_skip_rewrite_when_flush_not_due(...):
    ...
    first = update_recent_orderbook_index(...)
    second = update_recent_orderbook_index(...)
    assert len(second) >= len(first)
```

- [ ] **Step 3: Run the focused tests and confirm the new expectations fail before implementation**

Run: `PYTHONPATH=src pytest -q tests/test_data_recorder_runtime.py tests/test_data_pipelines.py`

Expected: at least the newly added tests fail because the code does not yet guarantee the new behavior.

### Task 2: Implement safer async persistence and cheaper write cadence

**Files:**
- Modify: `src/pm15min/data/pipelines/orderbook_runtime.py`
- Modify: `src/pm15min/data/pipelines/orderbook_recording.py`
- Modify: `src/pm15min/data/pipelines/orderbook_recent.py`

- [ ] **Step 1: Keep bounded async persistence, but use backpressure when `drop_oldest_when_full=False`**

```python
try:
    pending_batches.put_nowait(batch)
except queue.Full:
    if drop_oldest:
        ...
    else:
        pending_batches.put(batch)
```
This preserves the existing safe path while making the non-drop path explicit and test-covered.

- [ ] **Step 2: Lower the raw depth compression cost for live recorder appends**

```python
append_ndjson_zst(
    cfg.layout.orderbook_depth_path(batch.date_str),
    batch.snapshot_rows,
    level=_orderbook_depth_compression_level(),
)
```
Add a small helper with a lower default for live-friendly append speed while keeping an env override.

- [ ] **Step 3: Throttle recent-cache parquet rewrites**

```python
recent_df = update_recent_orderbook_index(
    path=cfg.layout.orderbook_recent_path,
    incoming=index_df,
    now_ts_ms=batch.captured_ts_ms,
    window_minutes=batch.recent_window_minutes,
    persist_interval_seconds=...,
)
```
Keep the in-memory recent index current on every batch, but only rewrite the parquet file when due or when explicitly forced.

- [ ] **Step 4: Keep the latest-full snapshot behavior unchanged unless tests show it is the dominant bottleneck**

No scope creep here. We only change this if verification proves it is still the limiting factor after the cheaper writes above.

- [ ] **Step 5: Run the focused tests again and make sure they pass**

Run: `PYTHONPATH=src pytest -q tests/test_data_recorder_runtime.py tests/test_data_pipelines.py`

Expected: PASS

### Task 3: Switch the fleet entrypoint to the new steady-state server mode

**Files:**
- Modify: `scripts/entrypoints/start_v2_orderbook_fleet.sh`

- [ ] **Step 1: Keep the global stop logic that clears stale duplicate recorders**

```bash
stop_all_orderbook_recorders
```

- [ ] **Step 2: Make the recommended env contract explicit for steady-state live fleets**

Document and preserve support for:

```bash
PM15MIN_ORDERBOOK_ASYNC_PERSIST=1
PM15MIN_ORDERBOOK_ASYNC_MAX_PENDING_BATCHES=1
PM15MIN_ORDERBOOK_ASYNC_DROP_OLDEST_WHEN_FULL=0
```

- [ ] **Step 3: Shell-check the entrypoint**

Run: `bash -n scripts/entrypoints/start_v2_orderbook_fleet.sh`

Expected: exit 0

### Task 4: Verify locally, sync only the needed files, and restart the server fleets

**Files:**
- Sync: `src/pm15min/data/pipelines/orderbook_runtime.py`
- Sync: `src/pm15min/data/pipelines/orderbook_recording.py`
- Sync: `src/pm15min/data/pipelines/orderbook_recent.py`
- Sync: `scripts/entrypoints/start_v2_orderbook_fleet.sh`

- [ ] **Step 1: Run the targeted local verification**

Run:

```bash
PYTHONPATH=src pytest -q tests/test_data_recorder_runtime.py tests/test_data_pipelines.py
bash -n scripts/entrypoints/start_v2_orderbook_fleet.sh
```

Expected: all pass.

- [ ] **Step 2: Sync only the changed recorder files to the server**

Run:

```bash
rsync -avR \
  src/pm15min/data/pipelines/orderbook_runtime.py \
  src/pm15min/data/pipelines/orderbook_recording.py \
  src/pm15min/data/pipelines/orderbook_recent.py \
  scripts/entrypoints/start_v2_orderbook_fleet.sh \
  ht66:/home/huatai/qigang/pm15min/v2/
```

- [ ] **Step 3: Restart live recorder fleets on the server in two passes**

Run `15m live`, then `5m live`, both with:

```bash
PM15MIN_ORDERBOOK_ASYNC_PERSIST=1
PM15MIN_ORDERBOOK_ASYNC_MAX_PENDING_BATCHES=1
PM15MIN_ORDERBOOK_ASYNC_DROP_OLDEST_WHEN_FULL=0
```

### Task 5: Server verification

**Files:**
- Inspect only: server state and logs under `var/live/`

- [ ] **Step 1: Verify only eight recorder processes remain**

Run:

```bash
ps -eo pid,etime,cmd | grep run_orderbook_recorder | grep -v grep
```

Expected: exactly `15m live` and `5m live` for `btc/eth/sol/xrp`.

- [ ] **Step 2: Verify no deliberate drops are being reported**

Run:

```bash
python3 - <<'PY'
...
print(state["dropped_batches"])
PY
```

Expected: `dropped_batches == 0` for the live state files after steady state is reached.

- [ ] **Step 3: Verify resource recovery**

Run:

```bash
free -h
vmstat 1 3
```

Expected: memory remains healthy, swap is not climbing again, and CPU no longer reflects duplicate recorder pressure.
