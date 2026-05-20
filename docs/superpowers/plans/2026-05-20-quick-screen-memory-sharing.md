# Quick Screen Memory Sharing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 降低 SOL/XRP 快筛在高并发下的内存增长，让 10 个逻辑快筛任务可以长期稳定运行，同时不改变交易策略、盘口规则、窗口、阈值和“每个观察点最多 20 笔”的约束。

**Architecture:** 先用 PSS/独占内存量化当前问题，再做两层优化：第一层把当前快筛里会放大内存的读取和中间对象改成流式、窄列、可及时释放；第二层把快筛调度从“多个独立 Python 进程各自加载大数据”升级为“同一 track 下一个常驻 pool 统一预热数据，再派发子任务”。保留现有 formal 实验、orderbook recorder、suite/backtest 入口，所有行为差异都用测试和服务器压测确认。

**Tech Stack:** Python 3.12/3.13, pandas, pyarrow, pytest, Linux `/proc/<pid>/smaps_rollup`, existing `pm15min.research` experiment/backtest stack, existing `auto_research` queue supervisor.

---

## Non-Goals

- 不改任何交易策略、概率阈值、窗口定义、盘口正式回测规则。
- 不把“每个观察点最多 20 笔”调小。
- 不停 BTC/ETH formal；只让它们与 SOL/XRP 快筛更稳定地共存。
- 不依赖人工手动清进程作为长期方案。

## Current Diagnosis

当前快筛已经有批次内清理：`scripts/research/run_quick_screen_queue_batch.py` 每跑完一个 item 会清 scoring cache、backtest cache、`gc.collect()` 和 `malloc_trim()`。

当前快筛没有真正跨进程共享内存：多个 `run_quick_screen_queue_batch.py` 是多个独立 Python 进程，各自加载 feature、label、bundle replay、盘口 replay。因此并发数升高时，内存会上升。

当前 `auto_research/run_quick_screen_queue_batch.sh` 默认关闭 backtest runtime cache：

```bash
PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES=0
PM15MIN_BACKTEST_SURFACE_RUNTIME_CACHE_MAX_ENTRIES=0
```

这避免了单个进程长期囤内存，但也意味着不同任务之间不会复用已构建的 backtest/runtime surface。

---

## File Structure

- Create `scripts/monitoring/report_quick_screen_memory.py`
  - 读取快筛、formal、orderbook 相关进程的 RSS/PSS/共享页/独占页，输出 JSON 和人类可读表格。
- Create `tests/test_quick_screen_memory_report.py`
  - 用假的 `smaps_rollup` 文件验证 PSS 解析和进程分类。
- Modify `src/pm15min/core/orderbook_index.py`
  - 增加按 batch 读取 orderbook index 的 helper，避免一次性把大 index 全读进 pandas。
- Modify `src/pm15min/research/backtests/depth_replay.py`
  - fallback 到 orderbook index 时改成流式迭代，不再一次性形成完整大 DataFrame。
- Modify `tests/test_research_backtest_depth_replay.py`
  - 覆盖流式 index fallback 与原输出一致。
- Create `src/pm15min/research/backtests/shared_surfaces.py`
  - 管理快筛共享 surface 的 key、manifest、预热、清理和只读加载。
- Create `tests/test_research_backtest_shared_surfaces.py`
  - 验证 shared surface key 稳定、过期判断、只读加载、窗口隔离。
- Create `scripts/research/run_quick_screen_pool.py`
  - 一个 track 一个 pool，统一领取队列、预热共享数据、派发 worker，并记录每轮内存。
- Create `auto_research/run_quick_screen_pool.sh`
  - 设置 pool 的 Python 环境、内存保护、线程数和默认 worker 数。
- Modify `auto_research/experiment_queue_supervisor.sh`
  - 增加可配置入口：默认仍可走旧 batch，打开开关后走 pool。
- Modify `auto_research/start_dense_stack.sh`
  - 让 SOL/XRP dense stack 可以启动 pool 版本。
- Modify `tests/test_research_experiment_queue.py`
  - 验证 pool 模式不会重复领取同一 queue item，失败时会正确回到 repair/dead。
- Modify `docs/RESEARCH_TECHNICAL_PLAN.md`
  - 补一小节说明快筛内存共享的长期运行边界。

---

### Task 1: Add Real Memory Measurement

**Files:**
- Create: `scripts/monitoring/report_quick_screen_memory.py`
- Create: `tests/test_quick_screen_memory_report.py`

- [ ] **Step 1: Write tests for smaps parsing**

Add tests that create fake files under `tmp_path/proc/<pid>/smaps_rollup`:

```python
from pathlib import Path

from scripts.monitoring.report_quick_screen_memory import parse_smaps_rollup


def test_parse_smaps_rollup_reads_pss_and_private_kb(tmp_path: Path) -> None:
    smaps = tmp_path / "smaps_rollup"
    smaps.write_text(
        "\n".join(
            [
                "Rss:             204800 kB",
                "Pss:             102400 kB",
                "Shared_Clean:     51200 kB",
                "Shared_Dirty:      1024 kB",
                "Private_Clean:    65536 kB",
                "Private_Dirty:    87040 kB",
            ]
        ),
        encoding="utf-8",
    )

    parsed = parse_smaps_rollup(smaps)

    assert parsed["rss_kb"] == 204800
    assert parsed["pss_kb"] == 102400
    assert parsed["private_kb"] == 152576
    assert parsed["shared_kb"] == 52224
```

- [ ] **Step 2: Implement the script**

The script must:

- classify command lines containing `run_quick_screen_queue_batch.py` or `run_quick_screen_pool.py` as `quick_screen`;
- classify `research experiment run-suite` as `formal`;
- classify orderbook recorder processes as `orderbook`;
- parse `/proc/<pid>/smaps_rollup` when available;
- fall back to RSS from `ps` if PSS is unavailable;
- output both table and JSON with `--json`.

- [ ] **Step 3: Run local tests**

Run:

```bash
PYTHONPATH=src pytest tests/test_quick_screen_memory_report.py -q
```

Expected: all tests pass.

- [ ] **Step 4: Run on ht66**

Run:

```bash
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && PYTHONPATH=src python3 scripts/monitoring/report_quick_screen_memory.py'
```

Expected: prints quick_screen/formal/orderbook rows with PSS when `/proc` permits it.

---

### Task 2: Stream Orderbook Index Fallback

**Files:**
- Modify: `src/pm15min/core/orderbook_index.py`
- Modify: `src/pm15min/research/backtests/depth_replay.py`
- Modify: `tests/test_research_backtest_depth_replay.py`

- [ ] **Step 1: Write a regression test for streaming fallback**

Add a test that writes an orderbook index parquet with multiple `market_id` values, forces `build_raw_depth_replay_frame()` to use the index fallback, and asserts:

- only rows for replay market IDs are matched;
- output `depth_snapshot_status`, prices and summary counts match the existing fallback behavior;
- the helper is called in batch mode rather than full-frame mode.

- [ ] **Step 2: Add a batch iterator in `orderbook_index.py`**

Add a helper with this interface:

```python
def iter_orderbook_index_record_batches(
    *,
    index_path: Path,
    columns: list[str] | tuple[str, ...],
    filters: ParquetFilters | None = None,
    batch_size: int = 100_000,
) -> Iterator[pd.DataFrame]:
    ...
```

Implementation requirements:

- use `pyarrow.parquet.ParquetFile.iter_batches()` when possible;
- apply available columns and filter columns before converting to pandas;
- return empty iterator if required filter columns are missing;
- keep the existing `load_orderbook_index_frame()` API unchanged for callers that still need a DataFrame.

- [ ] **Step 3: Use the iterator in `depth_replay.py`**

Change `_iter_orderbook_index_records()` so it loops over `iter_orderbook_index_record_batches()` and yields row dicts batch by batch.

Keep these output fields unchanged:

```python
captured_ts_ms
market_id
token_id
side
asks
```

- [ ] **Step 4: Run backtest depth tests**

Run:

```bash
PYTHONPATH=src pytest tests/test_research_backtest_depth_replay.py -q
```

Expected: all tests pass.

---

### Task 3: Add Shared Surface Manifests

**Files:**
- Create: `src/pm15min/research/backtests/shared_surfaces.py`
- Create: `tests/test_research_backtest_shared_surfaces.py`

- [ ] **Step 1: Write tests for stable keys**

The shared surface key must include:

- market;
- cycle;
- source surface;
- feature set;
- label set;
- profile;
- target;
- decision window;
- available offsets;
- orderbook depth mode.

Two suites with the same data surface but different run labels must produce the same key. Two suites with different windows or feature sets must produce different keys.

- [ ] **Step 2: Implement manifest model**

Create dataclasses:

```python
@dataclass(frozen=True)
class SharedSurfaceKey:
    market: str
    cycle: str
    source_surface: str
    feature_set: str
    label_set: str
    profile: str
    target: str
    decision_start: str
    decision_end: str
    offsets: tuple[int, ...]
    orderbook_mode: str


@dataclass(frozen=True)
class SharedSurfaceManifest:
    key: SharedSurfaceKey
    root: Path
    created_at: str
    source_mtimes: tuple[tuple[str, int | None], ...]
```

Manifest files live under:

```text
var/research/cache/quick_screen_surfaces/<key_hash>/manifest.json
```

- [ ] **Step 3: Implement freshness checking**

A shared surface is usable only when all source mtimes match:

- feature frame parquet;
- label frame parquet;
- relevant orderbook index/depth files;
- model bundle manifest or bundle offset files used by the suite.

If freshness fails, the surface is rebuilt before reuse.

- [ ] **Step 4: Run tests**

Run:

```bash
PYTHONPATH=src pytest tests/test_research_backtest_shared_surfaces.py -q
```

Expected: all tests pass.

---

### Task 4: Wire Shared Surfaces Into Backtest Runtime

**Files:**
- Modify: `src/pm15min/research/backtests/engine.py`
- Modify: `src/pm15min/research/backtests/runtime_cache.py`
- Modify: `tests/test_research_backtest_runtime_cache.py`
- Modify: `tests/test_research_backtest_memory_optimizations.py`

- [ ] **Step 1: Add an opt-in runtime flag**

Use an environment variable:

```bash
PM15MIN_QUICK_SCREEN_SHARED_SURFACES=1
```

When unset, behavior must stay exactly as it is now.

- [ ] **Step 2: Add tests that prove opt-in only**

Tests must assert:

- default path does not read shared-surface manifests;
- opt-in path reads or builds the shared surface;
- stale manifest triggers rebuild;
- final backtest summary fields remain unchanged.

- [ ] **Step 3: Reuse shared surface before heavy replay build**

Inside `engine.py`, before building feature/label/replay/depth surfaces, resolve the shared surface key. If a valid surface exists, load the compact runtime surface and skip rebuilding the heavy intermediate objects.

If no valid surface exists, build normally and persist the compact surface after successful replay construction.

- [ ] **Step 4: Keep strategy-equivalent outputs**

The opt-in path must preserve:

- trades count;
- rejects count;
- accepted decisions;
- fill prices;
- `orderbook_preflight_summary`;
- `shared_runtime_cache_status` with a new value such as `shared_surface_reused`.

- [ ] **Step 5: Run targeted tests**

Run:

```bash
PYTHONPATH=src pytest \
  tests/test_research_backtest_runtime_cache.py \
  tests/test_research_backtest_memory_optimizations.py \
  tests/test_research_backtest_parity.py \
  -q
```

Expected: all tests pass.

---

### Task 5: Add Quick-Screen Pool Runner

**Files:**
- Create: `scripts/research/run_quick_screen_pool.py`
- Create: `auto_research/run_quick_screen_pool.sh`
- Modify: `tests/test_research_experiment_queue.py`

- [ ] **Step 1: Write queue-claim tests**

Tests must cover:

- pool claims no more than `--max-items`;
- claimed items move to `running`;
- completed item moves to `done`;
- failed retryable item moves to `repair`;
- non-retryable unsupported feature-set item moves to `dead`;
- pool never claims BTC/ETH formal work when configured for SOL/XRP quick-screen tracks.

- [ ] **Step 2: Implement pool runner CLI**

Required CLI:

```bash
PYTHONPATH=src python3 scripts/research/run_quick_screen_pool.py \
  --root /home/huatai/qigang/pm15min/v2 \
  --tracks direction_dense,reversal_dense \
  --markets sol,xrp \
  --max-items 10 \
  --workers 10 \
  --memory-report-interval-sec 60
```

- [ ] **Step 3: Implement pool behavior**

The pool must:

- claim queue items in one coordinator process;
- group items by market/feature/label/window where possible;
- set `PM15MIN_QUICK_SCREEN_SHARED_SURFACES=1` for workers;
- run workers with Linux `fork` context when available;
- on macOS or unsupported platforms, fall back to serial execution with the same queue state transitions;
- write a JSONL heartbeat under `var/research/autorun/logs/quick_screen_pool.<track>.jsonl`.

- [ ] **Step 4: Implement wrapper shell**

`auto_research/run_quick_screen_pool.sh` must mirror the existing Python/env setup from `run_quick_screen_queue_batch.sh`, including:

```bash
PM15MIN_MEMORY_GUARD_ENABLE=1
PM15MIN_MIN_AVAILABLE_MEM_GB=1
PM15MIN_EXPERIMENT_CPU_THREADS
MALLOC_ARENA_MAX=2
PYTHONMALLOC=malloc
```

It must default to:

```bash
PM15MIN_QUICK_SCREEN_POOL_WORKERS=10
PM15MIN_QUICK_SCREEN_POOL_MAX_ITEMS=10
PM15MIN_QUICK_SCREEN_SHARED_SURFACES=1
```

- [ ] **Step 5: Run tests**

Run:

```bash
PYTHONPATH=src pytest tests/test_research_experiment_queue.py -q
```

Expected: all tests pass.

---

### Task 6: Integrate Pool Into Dense Stack

**Files:**
- Modify: `auto_research/experiment_queue_supervisor.sh`
- Modify: `auto_research/start_dense_stack.sh`
- Modify: `auto_research/README.md`
- Modify: `docs/RESEARCH_TECHNICAL_PLAN.md`

- [ ] **Step 1: Add opt-in switch**

Add:

```bash
PM15MIN_QUICK_SCREEN_USE_POOL=1
```

When unset, supervisor still launches the existing batch runner. This makes rollback one environment change.

- [ ] **Step 2: Keep track slot semantics**

The existing track capacities remain logical capacities:

```json
{"direction_dense": 5, "reversal_dense": 5}
```

or the current deployed value if the running stack has already changed it.

The pool consumes those same queue items; it does not redefine how many experiments are required.

- [ ] **Step 3: Document operator commands**

Add commands for:

```bash
auto_research/start_dense_stack.sh
scripts/monitoring/report_quick_screen_memory.py
scripts/research/status_autorun.sh
```

Document rollback:

```bash
PM15MIN_QUICK_SCREEN_USE_POOL=0 auto_research/start_dense_stack.sh
```

- [ ] **Step 4: Run shell syntax checks**

Run:

```bash
bash -n auto_research/run_quick_screen_pool.sh
bash -n auto_research/experiment_queue_supervisor.sh
bash -n auto_research/start_dense_stack.sh
```

Expected: no syntax errors.

---

### Task 7: Server Rollout And Memory Validation

**Files:**
- Read remote state under `/home/huatai/qigang/pm15min/v2`
- No local code files beyond previous tasks

- [ ] **Step 1: Sync code**

Use the existing server sync path for this repo. After sync, verify remote files:

```bash
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && test -f scripts/research/run_quick_screen_pool.py && test -f scripts/monitoring/report_quick_screen_memory.py && echo ok'
```

Expected: prints `ok`.

- [ ] **Step 2: Run remote targeted tests**

Run:

```bash
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && PYTHONPATH=src pytest tests/test_research_backtest_depth_replay.py tests/test_research_backtest_shared_surfaces.py tests/test_research_experiment_queue.py -q'
```

Expected: all selected tests pass.

- [ ] **Step 3: Start with 2 workers**

Run the pool with 2 workers for 15 minutes:

```bash
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && PM15MIN_QUICK_SCREEN_USE_POOL=1 PM15MIN_QUICK_SCREEN_POOL_WORKERS=2 auto_research/start_dense_stack.sh'
```

Measure:

```bash
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && PYTHONPATH=src python3 scripts/monitoring/report_quick_screen_memory.py --json'
```

Expected:

- orderbook recorder remains alive;
- BTC/ETH formal remains alive if it was running before;
- no OOM kill in `dmesg`;
- `MemAvailable` stays above 1 GiB.

- [ ] **Step 4: Increase to 5 workers**

Run for 30 minutes:

```bash
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && PM15MIN_QUICK_SCREEN_USE_POOL=1 PM15MIN_QUICK_SCREEN_POOL_WORKERS=5 auto_research/start_dense_stack.sh'
```

Expected:

- SOL/XRP quick-screen keeps completing items;
- total quick-screen PSS grows slower than old one-process-per-batch mode;
- stale `running` queue items do not accumulate.

- [ ] **Step 5: Increase to 10 workers**

Run for 60 minutes:

```bash
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && PM15MIN_QUICK_SCREEN_USE_POOL=1 PM15MIN_QUICK_SCREEN_POOL_WORKERS=10 auto_research/start_dense_stack.sh'
```

Expected success criteria:

- 10 logical quick-screen slots are being supplied;
- no process is killed by memory pressure;
- `MemAvailable` remains above 1 GiB;
- orderbook writes remain fresh;
- BTC/ETH formal is not pushed out of memory;
- quick-screen result artifacts continue to be produced.

- [ ] **Step 6: Roll back if needed**

If memory still climbs uncontrollably or workers die:

```bash
ssh ht66 'cd /home/huatai/qigang/pm15min/v2 && PM15MIN_QUICK_SCREEN_USE_POOL=0 auto_research/start_dense_stack.sh'
```

Then keep the memory report output and pool JSONL logs for diagnosis.

---

## Verification Matrix

Run locally before sync:

```bash
PYTHONPATH=src pytest \
  tests/test_quick_screen_memory_report.py \
  tests/test_research_backtest_depth_replay.py \
  tests/test_research_backtest_shared_surfaces.py \
  tests/test_research_backtest_runtime_cache.py \
  tests/test_research_backtest_memory_optimizations.py \
  tests/test_research_experiment_queue.py \
  -q
```

Run on server after sync:

```bash
PYTHONPATH=src pytest \
  tests/test_research_backtest_depth_replay.py \
  tests/test_research_backtest_shared_surfaces.py \
  tests/test_research_experiment_queue.py \
  -q
```

Live health checks:

```bash
PYTHONPATH=src python3 scripts/monitoring/report_quick_screen_memory.py
scripts/research/status_autorun.sh
python3 scripts/monitoring/report_orderbook_capture_health.py
```

---

## Success Criteria

- SOL/XRP 快筛可以维持 10 个逻辑任务持续补满。
- 快筛总 PSS 明显低于旧多进程重复加载模式。
- BTC/ETH formal 不被挤掉。
- orderbook recorder 正常写入。
- 服务器不再因为快筛扩并发而进入 SSH 难以连接、OOM kill 或大面积进程消失状态。
- 快筛与 formal 的交易结果在同一 suite、同一输入窗口、同一盘口数据下保持一致。

## Rollback Plan

Pool 模式是 opt-in。如果上线后异常，直接关掉：

```bash
PM15MIN_QUICK_SCREEN_USE_POOL=0 auto_research/start_dense_stack.sh
```

共享 surface cache 是派生产物，可以清理：

```bash
rm -rf var/research/cache/quick_screen_surfaces
```

清 cache 不影响长期研究产物；它只会让下一轮快筛重新构建 surface。
