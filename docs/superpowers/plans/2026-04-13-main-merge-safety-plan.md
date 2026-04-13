# Main Merge Safety Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the known merge blockers between the current `pm-split-dual-track` worktree and `main` so the branch can merge without changing existing `main` behavior or shipping broken `pm5min` command surfaces.

**Architecture:** Keep the `pm5min`/`pm15min` split direction, but restore behavior parity where the branch regressed from `main`. For new `pm5min` control surfaces, make execution, async tasks, and HTTP serve use the same 5m semantics as read/build paths instead of silently delegating back to 15m semantics.

**Tech Stack:** Python 3.11+, package-local CLI/console/data/research modules under `src/`, `pytest`, existing `pm15min.console.http` server shell with injected handlers, existing `pm15min.console.tasks` task manager with injected planner/executor.

---

### Task 1: Stabilize The Merge Verification Baseline

**Files:**
- Modify: `tests/test_pm5min_cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Replace the stale pm5min gamma backfill test**

Update the stale test so it patches the local 5m pipeline instead of the old 15m pipeline hook.

```python
def test_pm5min_backfill_cycle_labels_gamma_uses_5m_cycle(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm5min.data.pipelines.backtest_refresh.backfill_cycle_labels_from_gamma",
        lambda **kwargs: {
            "dataset": "backfill_cycle_labels_gamma",
            "markets": kwargs.get("markets"),
            "cycle": kwargs.get("cycle"),
            "surface": kwargs.get("surface"),
        },
    )

    rc = main(["data", "run", "backfill-cycle-labels-gamma", "--markets", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["markets"] == ["sol"]
```

- [ ] **Step 2: Verify the stale test fails before the fix**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k 'pm5min_backfill_cycle_labels_gamma_uses_5m_cycle'`
Expected: FAIL against the old monkeypatch target or the unfixed test body.

- [ ] **Step 3: Isolate the live network smoke test from merge gating**

Make the external-network live score test opt-in so merge gating does not depend on live Polymarket availability.

```python
import os
import pytest


@pytest.mark.network
def test_live_score_latest(capsys, monkeypatch, tmp_path: Path) -> None:
    if os.getenv("PM_RUN_NETWORK_TESTS") != "1":
        pytest.skip("set PM_RUN_NETWORK_TESTS=1 to run live network smoke tests")
    _patch_v2_roots(monkeypatch, tmp_path / "v2")
    rc = main(["live", "score-latest", "--market", "sol"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["cycle_minutes"] == 5
```

- [ ] **Step 4: Verify the stabilized baseline**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py tests/test_cli.py -k 'not live_score_latest'`
Expected: PASS

### Task 2: Make pm5min Console Execution And Serve Use 5m Semantics End-To-End

**Files:**
- Create: `src/pm5min/console/action_runner.py`
- Create: `src/pm5min/console/http.py`
- Modify: `src/pm5min/console/compat.py`
- Modify: `tests/test_pm5min_cli.py`
- Modify: `tests/test_console_cli.py`

- [ ] **Step 1: Write the failing end-to-end console consistency tests**

Add one test for sync execute, one for async task submit, and one for HTTP handlers using the real 5m planner/executor path.

```python
def test_pm5min_console_execute_action_uses_pm5min_planner_and_pm5min_main(monkeypatch) -> None:
    from pm5min.console.action_runner import execute_console_action

    calls: list[list[str]] = []

    monkeypatch.setattr(
        "pm5min.console.action_runner._default_main_fn",
        lambda argv: calls.append(list(argv or [])) or 0,
    )

    payload = execute_console_action(
        action_id="data_sync",
        request={"sync_command": "legacy-settlement-truth", "market": "sol"},
    )

    assert payload["status"] == "ok"
    assert calls
    assert calls[0][:4] == ["data", "sync", "legacy-settlement-truth", "--market"]


def test_pm5min_console_async_task_uses_pm5min_planner(monkeypatch, tmp_path: Path) -> None:
    from pm5min.console.compat import submit_console_action_task

    monkeypatch.setattr(
        "pm5min.console.action_runner.execute_console_action",
        lambda **kwargs: {"dataset": "console_action_execution", "status": "ok", **kwargs},
    )

    payload = submit_console_action_task(
        action_id="research_activate_bundle",
        request={"market": "sol", "bundle_label": "demo"},
    )

    assert payload["action_id"] == "research_activate_bundle"
    assert payload["request"]["market"] == "sol"


def test_pm5min_console_http_defaults_are_5m() -> None:
    from pm5min.console.http import build_pm5min_console_http_handlers
    from pm15min.console.http.app import route_console_http_request

    response = route_console_http_request(
        method="GET",
        target="/api/console/data-overview",
        handlers=build_pm5min_console_http_handlers(),
    )

    assert response.status_code == 200
    payload = json.loads(response.body_bytes().decode("utf-8"))
    assert payload["cycle"] == "5m"
```

- [ ] **Step 2: Verify the new tests fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py tests/test_console_cli.py -k 'pm5min_console_execute_action_uses_pm5min_planner_and_pm5min_main or pm5min_console_async_task_uses_pm5min_planner or pm5min_console_http_defaults_are_5m'`
Expected: FAIL because compat still routes execution/tasks/serve through pm15min.

- [ ] **Step 3: Add a pm5min-local action runner**

Create a 5m action runner that mirrors the 15m result shape but uses the 5m planner and `pm5min.cli.main`.

```python
from pm5min.console.actions import build_console_action_request


def execute_console_action(
    *,
    action_id: str,
    request: Mapping[str, object] | None = None,
    main_fn: Callable[[list[str] | None], int] | None = None,
) -> dict[str, object]:
    plan = build_console_action_request(action_id, request)
    resolved_main_fn = main_fn or _default_main_fn
    stdout_buffer = StringIO()
    stderr_buffer = StringIO()
    return_code = 1
    try:
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            return_code = int(resolved_main_fn(list(plan["pm15min_args"])))
    except Exception as exc:
        stderr_buffer.write(f"{exc.__class__.__name__}: {exc}")
        return_code = 1
    return {
        "dataset": "console_action_execution",
        "action_id": str(plan["action_id"]),
        "normalized_request": dict(plan["normalized_request"]),
        "pm15min_args": list(plan["pm15min_args"]),
        "command_preview": str(plan["command_preview"]),
        "return_code": int(return_code),
        "status": "ok" if int(return_code) == 0 else "error",
        "succeeded": int(return_code) == 0,
        "stdout": stdout_buffer.getvalue(),
        "stderr": stderr_buffer.getvalue(),
    }


def _default_main_fn(argv: list[str] | None) -> int:
    from pm5min.cli import main as pm5min_main
    return int(pm5min_main(argv))
```

- [ ] **Step 4: Reuse the existing task manager with a 5m planner/executor**

Create compat wrappers that use `pm15min.console.tasks.submit_console_task` but inject the 5m planner and 5m executor.

```python
from pm15min.console.tasks import submit_console_task
from pm5min.console.action_runner import execute_console_action as execute_pm5min_console_action
from pm5min.console.actions import build_console_action_request as build_pm5min_console_action_request


def submit_console_action_task(*, action_id: str, request: dict[str, object]) -> dict[str, object]:
    return submit_console_task(
        action_id=action_id,
        request=request,
        planner=build_pm5min_console_action_request,
        executor=lambda context: execute_pm5min_console_action(
            action_id=context.action_id,
            request=context.request,
        ),
    )
```

- [ ] **Step 5: Reuse the existing HTTP server shell with pm5min handlers**

Create a local `pm5min.console.http` module that builds injected handlers and serves through the shared HTTP server shell.

```python
from pm15min.console.http import ConsoleHttpHandlers, serve_console_http as serve_shared_console_http
from pm15min.console.http.action_routes import (
    execute_console_action_payload,
)
from pm5min.console import service as console_service


def build_pm5min_console_http_handlers() -> ConsoleHttpHandlers:
    return ConsoleHttpHandlers(
        health_handler=lambda: {"domain": "console_http", "dataset": "health", "status": "ok"},
        console_handler=lambda query: console_service.load_console_home(root=_optional_path(query.get("root"))),
        section_handlers={
            "/api/console/home": lambda query: console_service.load_console_home(root=_optional_path(query.get("root"))),
            "/api/console/runtime-state": lambda query: console_service.load_console_runtime_state(root=_optional_path(query.get("root"))),
            "/api/console/runtime-history": lambda query: console_service.load_console_runtime_history(root=_optional_path(query.get("root"))),
            "/api/console/data-overview": lambda query: console_service.load_console_data_overview(
                market=_string_value(query.get("market")) or "sol",
                cycle=_string_value(query.get("cycle")) or "5m",
                surface=_string_value(query.get("surface")) or "backtest",
                root=_optional_path(query.get("root")),
            ),
            "/api/console/training-runs": lambda query: console_service.list_console_training_runs(
                market=_string_value(query.get("market")) or "sol",
                cycle=_string_value(query.get("cycle")) or "5m",
                model_family=_string_value(query.get("model_family")),
                target=_string_value(query.get("target")),
                prefix=_string_value(query.get("prefix")),
                root=_optional_path(query.get("root")),
            ),
            "/api/console/bundles": lambda query: console_service.list_console_bundles(
                market=_string_value(query.get("market")) or "sol",
                cycle=_string_value(query.get("cycle")) or "5m",
                profile=_string_value(query.get("profile")),
                target=_string_value(query.get("target")),
                prefix=_string_value(query.get("prefix")),
                root=_optional_path(query.get("root")),
            ),
            "/api/console/backtests": lambda query: console_service.list_console_backtests(
                market=_string_value(query.get("market")) or "sol",
                cycle=_string_value(query.get("cycle")) or "5m",
                profile=_string_value(query.get("profile")),
                spec_name=_string_value(query.get("spec")),
                prefix=_string_value(query.get("prefix")),
                root=_optional_path(query.get("root")),
            ),
            "/api/console/experiments": lambda query: console_service.list_console_experiments(
                suite_name=_string_value(query.get("suite")),
                prefix=_string_value(query.get("prefix")),
                root=_optional_path(query.get("root")),
            ),
            "/api/console/actions": lambda query: console_service.load_console_action_catalog(
                for_section=_string_value(query.get("for_section")),
                shell_enabled=_optional_bool(query.get("shell_enabled")),
            ),
            "/api/console/tasks": lambda query: console_service.list_console_tasks(
                action_id=_string_value(query.get("action_id")),
                action_ids=_optional_csv_strings(query.get("action_ids")),
                status=_string_value(query.get("status")),
                status_group=_string_value(query.get("status_group")),
                marker=_string_value(query.get("marker")),
                group_by=_string_value(query.get("group_by")),
                limit=_optional_int(query.get("limit"), default=20),
                root=_optional_path(query.get("root")),
            ),
        },
        action_execute_handler=lambda body: execute_console_action_payload(
            body,
            executor=execute_pm5min_console_action,
            task_submitter=submit_console_action_task,
        ),
    )


def serve_console_http(*, host: str, port: int, poll_interval: float) -> None:
    serve_shared_console_http(
        host=host,
        port=port,
        poll_interval=poll_interval,
        handlers=build_pm5min_console_http_handlers(),
    )
```

- [ ] **Step 6: Rewire pm5min compat to local implementations**

```python
from .action_runner import execute_console_action
from .http import serve_console_http
from .tasks import submit_console_action_task

__all__ = [
    "execute_console_action",
    "serve_console_http",
    "submit_console_action_task",
]
```

- [ ] **Step 7: Run focused console verification**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py tests/test_pm5min_console_read_models.py tests/test_pm5min_console_tasks.py tests/test_console_cli.py -k 'console or pm5min_console'`
Expected: PASS

### Task 3: Make pm5min Research Delegation Compatible With Existing Runners

**Files:**
- Modify: `src/pm5min/research/layout.py`
- Modify: `tests/test_pm5min_cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Write failing runner-compatibility tests**

Add tests that call the 5m layout using the parameter names expected by the 15m runners.

```python
def test_pm5min_research_layout_supports_training_runner_signature(tmp_path: Path) -> None:
    from pm5min.research.config import ResearchConfig

    cfg = ResearchConfig.build(market="sol", cycle="5m", profile="deep_otm_5m", root=tmp_path)
    run_dir = cfg.layout.training_run_dir(
        model_family="deep_otm",
        target="direction",
        run_label_text="demo",
    )

    assert str(run_dir).endswith("run=demo")


def test_pm5min_research_layout_supports_bundle_builder_signature(tmp_path: Path) -> None:
    from pm5min.research.config import ResearchConfig

    cfg = ResearchConfig.build(market="sol", cycle="5m", profile="deep_otm_5m", root=tmp_path)
    bundle_dir = cfg.layout.bundle_dir(
        profile="deep_otm_5m",
        target="direction",
        bundle_label_text="demo",
    )

    assert str(bundle_dir).endswith("bundle=demo")


def test_pm5min_research_layout_supports_backtest_runner_signature(tmp_path: Path) -> None:
    from pm5min.research.config import ResearchConfig

    cfg = ResearchConfig.build(market="sol", cycle="5m", profile="deep_otm_5m", root=tmp_path)
    run_dir = cfg.layout.backtest_run_dir(
        profile="deep_otm_5m",
        spec_name="baseline_truth",
        run_label_text="demo",
    )

    assert str(run_dir).endswith("run=demo")
```

- [ ] **Step 2: Verify the compatibility tests fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k 'research_layout_supports_training_runner_signature or research_layout_supports_bundle_builder_signature or research_layout_supports_backtest_runner_signature'`
Expected: FAIL because `run_label_text`/`bundle_label_text` and `bundle_dir` are not yet supported.

- [ ] **Step 3: Add compatibility aliases on the 5m research layout**

Keep the current 5m methods, but add the runner-facing aliases expected by the delegated 15m code.

```python
def training_run_dir(
    self,
    *,
    model_family: str,
    target: str,
    run_label: str | None = None,
    run_label_text: str | None = None,
) -> Path:
    resolved_label = run_label if run_label is not None else run_label_text
    return (
        self.training_runs_root
        / f"model_family={slug_token(model_family)}"
        / f"target={normalize_target(target)}"
        / f"run={slug_token(resolved_label or utc_run_label())}"
    )


def bundle_dir(self, *, profile: str, target: str, bundle_label_text: str) -> Path:
    return self.model_bundle_dir(profile=profile, target=target, bundle_label=bundle_label_text)


def backtest_run_dir(
    self,
    *,
    profile: str,
    spec_name: str,
    run_label: str | None = None,
    run_label_text: str | None = None,
    target: str | None = None,
) -> Path:
    resolved_label = run_label if run_label is not None else run_label_text
    resolved_target = normalize_target(target or "direction")
    return (
        self.backtests_root
        / f"profile={slug_token(profile)}"
        / f"target={resolved_target}"
        / f"spec={slug_token(spec_name)}"
        / f"run={slug_token(resolved_label or utc_run_label())}"
    )
```

- [ ] **Step 4: Add one real delegated smoke test**

Use the existing CLI test harness to run one representative delegated command end to end.

```python
def test_pm5min_research_backtest_run_cli_smoke(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")
    rc = main(
        [
            "research",
            "backtest",
            "run",
            "--market",
            "sol",
            "--profile",
            "deep_otm_5m",
            "--spec",
            "baseline_truth",
            "--run-label",
            "demo",
        ]
    )
    assert rc == 0
```

- [ ] **Step 5: Run focused research verification**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py tests/test_cli.py -k 'pm5min_research or research_backtest_run_cli_smoke'`
Expected: PASS

### Task 4: Restore pm15min Data Behavior Parity With main

**Files:**
- Modify: `src/pm15min/data/pipelines/direct_oracle_prices.py`
- Modify: `src/pm15min/data/pipelines/oracle_prices.py`
- Modify: `src/pm15min/data/sources/polygon_rpc.py`
- Modify: `src/pm15min/data/sources/chainlink_rpc.py`
- Modify: `src/pm15min/data/pipelines/orderbook_recording.py`
- Modify: `tests/test_data_builders.py`
- Recreate or modify: `tests/test_chainlink_rpc.py`
- Recreate or modify: `tests/test_polygon_rpc.py`

- [ ] **Step 1: Restore environment-aware Polygon RPC resolution**

Put back the `main` behavior that reads configured RPC URLs from env before falling back to public defaults.

```python
def _resolve_rpc_urls(urls: list[str] | None = None) -> list[str]:
    _load_dotenv_if_enabled()
    if urls is not None:
        resolved = [url.strip() for url in urls if str(url).strip()]
        if not resolved:
            raise ValueError("No RPC URLs configured.")
        return list(dict.fromkeys(resolved))
    candidates: list[str] = []
    for key in ("RPC_URL", "POLYGON_RPC", "POLYGON_RPC_URL", "WEB3_PROVIDER_URI"):
        candidates.extend(_split_rpc_list(os.getenv(key)))
    for key in ("RPC_URL_BACKUPS", "POLYGON_RPC_BACKUPS", "RPC_FALLBACKS", "POLYGON_RPC_FALLBACKS"):
        candidates.extend(_split_rpc_list(os.getenv(key)))
    candidates.extend(DEFAULT_RPC_URLS)
    return _dedupe_keep_order(candidates)
```

- [ ] **Step 2: Restore tolerant Chainlink log decoding**

Bring back per-transaction exception shielding so one malformed payload does not abort the full scan.

```python
try:
    if selector == TRANSMIT_SELECTOR:
        rows = _decode_transmit_rows(input_hex)
        for row in rows:
            report_feed_id = str(row.get("report_feed_id") or "").lower()
            if report_feed_id and report_feed_id != DEFAULT_FEEDS[asset].lower():
                continue
            decoded_rows.append(
                {
                    "asset": asset,
                    "tx_hash": tx_hash,
                    "block_number": block_number,
                    "block_timestamp": block_ts,
                    "requester": meta["requester"],
                    "feed_id_log": meta["feed_id_log"],
                    "report_feed_id": report_feed_id,
                    "perform_idx": row.get("perform_idx"),
                    "value_idx": row.get("value_idx"),
                    "extra_code": row.get("extra_code"),
                    "extra_ts": row.get("extra_ts"),
                    "valid_from_ts": row.get("valid_from_ts"),
                    "observation_ts": row.get("observation_ts"),
                    "expires_at_ts": row.get("expires_at_ts"),
                    "benchmark_price_raw": row.get("benchmark_price_raw"),
                    "bid_raw": row.get("bid_raw"),
                    "ask_raw": row.get("ask_raw"),
                    "path": "keeper_transmit",
                }
            )
    else:
        payload, _parameter_payload = decode(["bytes", "bytes"], bytes.fromhex(input_hex[10:]))
        row = _decode_signed_report_payload(payload)
        report_feed_id = str(row.get("report_feed_id") or "").lower()
        if report_feed_id and report_feed_id != DEFAULT_FEEDS[asset].lower():
            continue
        decoded_rows.append(
            {
                "asset": asset,
                "tx_hash": tx_hash,
                "block_number": block_number,
                "block_timestamp": block_ts,
                "requester": meta["requester"],
                "feed_id_log": meta["feed_id_log"],
                "report_feed_id": report_feed_id,
                "perform_idx": 0,
                "value_idx": 0,
                "extra_code": None,
                "extra_ts": None,
                "valid_from_ts": row.get("valid_from_ts"),
                "observation_ts": row.get("observation_ts"),
                "expires_at_ts": row.get("expires_at_ts"),
                "benchmark_price_raw": row.get("benchmark_price_raw"),
                "bid_raw": row.get("bid_raw"),
                "ask_raw": row.get("ask_raw"),
                "path": "direct_verify",
            }
        )
except Exception:
    continue
```

- [ ] **Step 3: Restore field-level direct-oracle merge semantics**

Bring back the `main` behavior that tracks and merges `source_price_to_beat` and `source_final_price` independently instead of collapsing everything into one `source`.

```python
DIRECT_ORACLE_COLUMNS = [
    "asset",
    "cycle",
    "cycle_start_ts",
    "cycle_end_ts",
    "price_to_beat",
    "final_price",
    "has_price_to_beat",
    "has_final_price",
    "has_both",
    "completed",
    "incomplete",
    "cached",
    "api_timestamp_ms",
    "http_status",
    "source_price_to_beat",
    "source_final_price",
    "source_priority",
    "fetched_at",
]


# Restore these exact helper blocks from `main`:
# - _field_source
# - _should_replace_field
# - _merge_rows_fieldwise
# - _merge_candidate
# - _resolve_chainlink_client
# - _build_direct_candidate
# - _apply_chainlink_fallback


def _field_source(row: dict[str, object], *, field: str) -> str:
    specific_key = "source_price_to_beat" if field == "price_to_beat" else "source_final_price"
    specific_value = str(row.get(specific_key) or "").strip()
    if specific_value:
        return specific_value
    generic_value = str(row.get("source") or "").strip()
    return generic_value
```

- [ ] **Step 4: Restore official Chainlink fallback for incomplete windows**

Reintroduce the official fallback client path from `main` so missing open/close values can still be completed before downstream tables are rebuilt.

```python
from ..sources.chainlink_streams_api import ChainlinkDataStreamsApiClient


# Restore the exact `main` implementations of `_resolve_chainlink_client`
# and `_apply_chainlink_fallback`, including the field-level
# `source_price_to_beat` / `source_final_price` updates.


def _apply_chainlink_fallback(
    *,
    cfg: DataConfig,
    candidate: dict[str, object],
    cycle_start_ts: int,
    chainlink_client: ChainlinkDataStreamsApiClient | None,
) -> dict[str, object]:
    # Use the `main` implementation verbatim so incomplete windows can be
    # completed via Chainlink without changing downstream source semantics.
    return _apply_chainlink_fallback_from_main(
        cfg=cfg,
        candidate=candidate,
        cycle_start_ts=cycle_start_ts,
        chainlink_client=chainlink_client,
    )
```

- [ ] **Step 5: Restore orderbook refresh fallback**

Put back the `main` behavior that keeps using an existing market table when a refresh attempt fails during recording.

```python
try:
    sync_market_catalog(cfg)
except Exception:
    if cfg.layout.market_catalog_table_path.exists():
        markets = load_market_catalog(cfg)
    else:
        raise
```

- [ ] **Step 6: Restore deleted parity tests**

Recreate the deleted tests for:

- RPC env resolution
- malformed Chainlink payload tolerance
- direct-oracle field-level source tracking
- official fallback completion
- orderbook refresh fallback

```python
def test_polygon_rpc_prefers_env_urls(monkeypatch) -> None:
    monkeypatch.setenv("POLYGON_RPC_URL", "https://primary.example")
    client = PolygonRpcClient()
    assert client.urls[0] == "https://primary.example"


def test_decode_streams_from_logs_skips_bad_payload(monkeypatch) -> None:
    class FakeRpc:
        def call(self, method, params, retries=5):
            if method == "eth_getTransactionByHash":
                tx_hash = params[0]
                if tx_hash == "0xbad":
                    return {"input": "0xdeadbeef", "blockNumber": "0x1"}
                return {"input": GOOD_VERIFY_INPUT, "blockNumber": "0x1"}
            raise AssertionError(method)

        def eth_block_timestamp(self, block_number, cache=None):
            return 1_700_000_000

    source = ChainlinkRpcSource(FakeRpc())
    rows = source.decode_streams_from_logs(
        asset="sol",
        logs=[
            {"tx_hash": "0xbad", "requester": "0x1", "feed_id_log": DEFAULT_FEEDS["sol"]},
            {"tx_hash": "0xgood", "requester": "0x1", "feed_id_log": DEFAULT_FEEDS["sol"]},
        ],
        include_block_timestamp=False,
    )
    assert any(row["tx_hash"] == "0xgood" for row in rows)


def test_direct_oracle_keeps_field_level_sources(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", root=tmp_path)
    direct = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 100,
                "cycle_end_ts": 1000,
                "price_to_beat": 1.0,
                "final_price": pd.NA,
                "source_price_to_beat": "polymarket_api_past_results",
                "source_final_price": "",
            }
        ]
    )
    write_parquet_atomic(direct, cfg.layout.direct_oracle_source_path)
    payload = build_oracle_prices_table(cfg)
    assert payload["rows_written"] == 1
```

- [ ] **Step 7: Run data regression verification**

Run: `PYTHONPATH=src pytest -q tests/test_data_builders.py tests/test_data_pipelines.py tests/test_pm5min_data_foundation.py tests/test_pm5min_data_orderbooks.py tests/test_pm5min_data_pipelines.py tests/test_chainlink_rpc.py tests/test_polygon_rpc.py`
Expected: PASS

### Task 5: Restore pm15min Research Behavior Parity With main

**Files:**
- Modify: `src/pm15min/research/backtests/engine.py`
- Modify: `src/pm15min/research/backtests/fills.py`
- Modify: `src/pm15min/research/datasets/loaders.py`
- Modify: `src/pm15min/research/datasets/training_sets.py`
- Modify: `src/pm15min/research/experiments/runner.py`
- Modify: `src/pm15min/research/automation/control_plane.py`
- Modify or recreate: `tests/test_research_backtest_fills.py`
- Modify or recreate: `tests/test_research_backtest_memory_optimizations.py`
- Modify or recreate: `tests/test_research_experiment_automation.py`
- Modify or recreate: `tests/test_research_experiment_runtime_resume.py`
- Modify or recreate: `tests/test_research_training_datasets_parity.py`

- [ ] **Step 1: Restore the defense trade-cap wiring**

Put `regime_defense_max_trades_per_market` back into the fill config path and the fill rejection logic.

```python
fill_config = CanonicalFillConfig(
    base_stake=fill_base_stake,
    max_stake=fill_max_stake,
    fee_bps=50.0,
    raw_depth_fak_refresh_enabled=(
        True if spec.parity.raw_depth_fak_refresh_enabled is None else bool(spec.parity.raw_depth_fak_refresh_enabled)
    ),
    fill_model="canonical_quote_depth",
    profile_spec=profile_spec,
    max_filled_trades_per_offset=spec.parity.regime_defense_max_trades_per_market,
)
```

```python
if (
    side == "defense"
    and config.regime_defense_max_trades_per_market is not None
    and defense_trade_count >= int(config.regime_defense_max_trades_per_market)
):
    rejects.append(
        {
            "reason": "max_filled_trades_per_offset",
            "offset": offset,
            "cycle_start_ts": row.get("cycle_start_ts"),
            "entry_side": side,
        }
    )
    continue
```

- [ ] **Step 2: Restore bounded dataset reads**

Put back the `main`-style filtered parquet reads for training and backtest windows instead of always loading full tables first.

```python
features = read_parquet_if_exists(
    cfg.layout.feature_frame_path(feature_set, source_surface=cfg.source_surface),
    filters=[("cycle_start_ts", ">=", window_start_ts), ("cycle_start_ts", "<=", window_end_ts)],
)
```

- [ ] **Step 3: Restore experiment cache clearing**

Put back the cleanup step after each experiment group run so cache does not grow unbounded across suites.

```python
clear_backtest_shared_cache()
```

- [ ] **Step 4: Restore automation control-plane summary semantics**

Bring back the `main` behavior for:

- incomplete official runs still being summarized
- old stuck runs being ignored when newer completed results exist
- completed frontier/status hints staying in output

```python
# Restore the exact `main` implementations of:
# - summarize_experiment_run
# - _summarize_incomplete_formal_run
# - _read_formal_top_case
# - _read_quick_screen_top_case
# - any helper that suppresses obsolete stuck runs when a newer completed run exists
```

- [ ] **Step 5: Restore the deleted research parity tests**

Recreate the `main` coverage for:

- defense trade caps
- bounded parquet reads / memory behavior
- automation control-plane status semantics
- experiment runtime resume

```python
def test_regime_defense_respects_max_trades_per_market(tmp_path: Path) -> None:
    spec = _build_backtest_spec(tmp_path, regime_defense_max_trades_per_market=1)
    result = run_backtest(spec)
    rejects = pd.read_parquet(Path(result["rejects_path"]))
    assert "max_filled_trades_per_offset" in set(rejects["reason"].astype(str))


def test_training_set_loader_uses_window_filters(monkeypatch, tmp_path: Path) -> None:
    calls: list[object] = []
    monkeypatch.setattr(
        "pm15min.research.datasets.loaders._read_required_parquet",
        lambda path, **kwargs: calls.append(kwargs.get("filters")) or pd.DataFrame(),
    )
    build_training_set_dataset(_sample_research_cfg(tmp_path))
    assert any(filters for filters in calls)


def test_control_plane_ignores_obsolete_stuck_runs(tmp_path: Path) -> None:
    root = _build_control_plane_fixture_with_new_completed_and_old_stuck_run(tmp_path)
    payload = summarize_experiment_run(root / "runs" / "suite=demo" / "run=current")
    assert payload["completed_cases"] is not None
```

- [ ] **Step 6: Run research regression verification**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_research_service.py tests/test_research_backtest_fills.py tests/test_research_backtest_memory_optimizations.py tests/test_research_experiment_automation.py tests/test_research_experiment_runtime_resume.py tests/test_research_training_datasets_parity.py`
Expected: PASS

### Task 6: Restore pm15min CLI Lazy Loading And Final Merge Gate

**Files:**
- Modify: `src/pm15min/cli.py`
- Recreate or modify: `tests/test_cli_lazy_loading.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Restore the requested-domain lazy-loading pattern**

Bring back the `main` behavior that only imports the requested domain CLI when needed.

```python
_DOMAIN_LOADERS = {
    "console": ("pm15min.console.cli", "attach_console_subcommands", "run_console_command"),
    "data": ("pm15min.data.cli", "attach_data_subcommands", "run_data_command"),
    "live": ("pm15min.live.cli", "attach_live_subcommands", "run_live_command"),
    "research": ("pm15min.research.cli", "attach_research_subcommands", "run_research_command"),
}


def _requested_domain(argv: list[str]) -> str | None:
    for token in argv:
        if token.startswith("-"):
            continue
        if token in _DOMAIN_LOADERS or token == "layout":
            return token
        break
    return None


def build_parser(requested_domain: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pm15min",
        description="pm15min v2 clean-room CLI",
    )
    subparsers = parser.add_subparsers(dest="domain")
    layout_parser = subparsers.add_parser("layout", help="Show the canonical v2 market layout.")
    layout_parser.add_argument("--market", default="btc", help="Market slug: btc/eth/sol/xrp.")
    layout_parser.add_argument("--cycle", default="15m", help="Cycle slug: 15m only.")
    layout_parser.add_argument("--json", action="store_true", help="Print JSON instead of key/value lines.")
    for domain in ("live", "research", "data", "console"):
        if requested_domain == domain:
            attach_subcommands, _ = _load_domain_cli(domain)
            attach_subcommands(subparsers)
        else:
            subparsers.add_parser(domain, help=_DOMAIN_HELP[domain])
    return parser
```

- [ ] **Step 2: Restore the deleted lazy-loading regression test**

```python
def test_data_command_does_not_load_live_domain(monkeypatch, capsys) -> None:
    data_cli = types.ModuleType("pm15min.data.cli")
    live_cli = types.ModuleType("pm15min.live.cli")
    research_cli = types.ModuleType("pm15min.research.cli")
    console_cli = types.ModuleType("pm15min.console.cli")
    data_cli.attach_data_subcommands = attach_data_subcommands
    data_cli.run_data_command = run_data_command
    live_cli.attach_live_subcommands = _boom
    live_cli.run_live_command = _boom
    research_cli.attach_research_subcommands = _boom
    research_cli.run_research_command = _boom
    console_cli.attach_console_subcommands = _boom
    console_cli.run_console_command = _boom
    monkeypatch.setitem(sys.modules, "pm15min.data.cli", data_cli)
    monkeypatch.setitem(sys.modules, "pm15min.live.cli", live_cli)
    monkeypatch.setitem(sys.modules, "pm15min.research.cli", research_cli)
    monkeypatch.setitem(sys.modules, "pm15min.console.cli", console_cli)
    import pm15min.cli as cli_module
    cli_module = importlib.reload(cli_module)
    rc = cli_module.main(["data", "show-summary", "--market", "sol"])
    assert rc == 0
```

- [ ] **Step 3: Run final merge gate**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py tests/test_pm5min_console_read_models.py tests/test_pm5min_console_tasks.py tests/test_pm5min_data_foundation.py tests/test_pm5min_data_orderbooks.py tests/test_pm5min_data_pipelines.py tests/test_pm5min_research_service.py tests/test_cli.py tests/test_data_builders.py tests/test_data_pipelines.py tests/test_research_backtest_fills.py tests/test_research_backtest_memory_optimizations.py tests/test_research_experiment_automation.py tests/test_research_experiment_runtime_resume.py tests/test_research_training_datasets_parity.py tests/test_architecture_guards.py tests/test_pmshared_architecture.py -k 'not live_score_latest'`
Expected: PASS

- [ ] **Step 4: Run opt-in live network smoke separately**

Run: `PM_RUN_NETWORK_TESTS=1 PYTHONPATH=src pytest -q tests/test_cli.py -k 'live_score_latest'`
Expected: PASS when external network and Polymarket Gamma are reachable.
