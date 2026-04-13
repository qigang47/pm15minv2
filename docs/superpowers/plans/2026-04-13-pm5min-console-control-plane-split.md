# PM5Min Console Control Plane Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `pm5min/console` so read-only console queries, action planning, and task/runtime reads no longer depend on `pm15min.console.service`, while keeping execution and HTTP serving on an explicit narrow compat path for now.

**Architecture:** Keep the existing `parser.py -> handlers.py -> service/read-model modules` shape, but make `pm5min.console` own its read models, action catalog/builders, and read-only task/runtime views. Only three fallback entry points should remain in `compat.py`: synchronous action execution, async task submission, and HTTP serving. Shared logic is acceptable only at the low-level utility layer; `pm5min.console` must not import `pm15min.console.service` or `pm15min.console.read_models.*`.

**Tech Stack:** Python 3.11+, `argparse`, `pathlib`, `json`, package-local console modules under `src/pm5min/console`, existing `pm5min.data` / `pm5min.research` services and layouts, `pytest`.

---

## Scope And Non-Goals

- This plan covers:
  - `show-home`
  - `show-runtime-state`
  - `show-runtime-history`
  - `show-actions`
  - `build-action`
  - `list-tasks`
  - `show-task`
  - `show-data-overview`
  - `list-training-runs`
  - `show-training-run`
  - `list-bundles`
  - `show-bundle`
  - `list-backtests`
  - `show-backtest`
  - `list-experiments`
  - `show-experiment`
- This plan does **not** cover:
  - local reimplementation of console action execution
  - local reimplementation of async task execution workers
  - local reimplementation of HTTP routing/server code
  - UI surface redesign
- After this plan:
  - read paths and action planning are package-local in `pm5min.console`
  - `execute-action` still uses compat for sync/async execution
  - `serve` still uses compat to reach the existing HTTP server

## Current Boundary

- `src/pm5min/console/parser.py` is already local and already injects 5m defaults.
- `src/pm5min/console/cli.py` is already a thin local wrapper.
- `src/pm5min/console/handlers.py` is local command routing, but every operation still reaches into `.compat`.
- `src/pm5min/console/compat.py` still routes to `pm15min.console.service` and `pm15min.console.http`, so the actual console control plane is still borrowed.

## File Structure

- Create: `src/pm5min/console/service.py`
  - Local service aggregator for all read-only console commands and action planning.
- Create: `src/pm5min/console/actions.py`
  - Local action catalog and request builders with `pm5min` command previews and 5m defaults.
- Create: `src/pm5min/console/tasks.py`
  - Local read-only task/runtime loaders, history scanning, and summary shaping.
- Create: `src/pm5min/console/read_models/__init__.py`
  - Local exports for console read models.
- Create: `src/pm5min/console/read_models/common.py`
  - Local JSON/path normalization helpers.
- Create: `src/pm5min/console/read_models/data_overview.py`
  - Local console data overview loader backed by `pm5min.data.service`.
- Create: `src/pm5min/console/read_models/training_runs.py`
  - Local training-run list/detail read model backed by `pm5min.research.service`.
- Create: `src/pm5min/console/read_models/bundles.py`
  - Local model-bundle list/detail read model backed by `pm5min.research.service` and local bundle helpers.
- Create: `src/pm5min/console/read_models/backtests.py`
  - Local backtest list/detail read model backed by `pm5min.research` layout.
- Create: `src/pm5min/console/read_models/experiments.py`
  - Local experiment list/detail read model backed by `pm5min.research` layout.
- Modify: `src/pm5min/console/handlers.py`
  - Use local `service.py` for read paths and action planning; leave compat only for execution and HTTP.
- Modify: `src/pm5min/console/compat.py`
  - Remove `pm15min.console.service` loading; keep only execution/task-submit/server fallbacks.
- Create: `tests/test_pm5min_console_read_models.py`
  - Focused tests for local data/research console read models.
- Create: `tests/test_pm5min_console_tasks.py`
  - Focused tests for local action planning and read-only task/runtime views.
- Modify: `tests/test_pm5min_cli.py`
  - Add guard tests proving `pm5min.console` no longer delegates to `pm15min.console.service` and local CLI read commands stay local.

## Task 1: Split Data / Training / Bundle Read Models

**Files:**
- Create: `src/pm5min/console/read_models/__init__.py`
- Create: `src/pm5min/console/read_models/common.py`
- Create: `src/pm5min/console/read_models/data_overview.py`
- Create: `src/pm5min/console/read_models/training_runs.py`
- Create: `src/pm5min/console/read_models/bundles.py`
- Create: `tests/test_pm5min_console_read_models.py`
- Modify: `tests/test_pm5min_cli.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pm5min_console_read_models.py
from __future__ import annotations

import json
from pathlib import Path

from pm5min.console.read_models.bundles import load_console_model_bundle
from pm5min.console.read_models.data_overview import load_data_overview
from pm5min.console.read_models.training_runs import list_console_training_runs
from pm5min.research.config import ResearchConfig


def test_pm5min_console_data_overview_uses_pm5min_data_service(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.data.service.show_data_summary",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("delegated to pm15min.data.service.show_data_summary")),
    )

    payload = load_data_overview(
        market="sol",
        cycle="5m",
        surface="backtest",
        root=tmp_path,
        prefer_persisted=False,
    )

    assert payload["dataset"] == "console_data_overview"
    assert payload["cycle"] == "5m"


def test_pm5min_console_training_runs_use_pm5min_research_service(tmp_path, monkeypatch) -> None:
    cfg = ResearchConfig.build(
        market="sol",
        cycle="5m",
        profile="deep_otm_5m",
        target="direction",
        model_family="deep_otm",
        root=tmp_path,
    )
    run_dir = cfg.layout.training_run_dir(model_family="deep_otm", target="direction", run_label="console-local")
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "summary.json").write_text(
        json.dumps({"market": "sol", "cycle": "5m", "model_family": "deep_otm", "target": "direction", "run_label": "console-local"}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "pm15min.research.service.list_training_runs",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("delegated to pm15min.research.service.list_training_runs")),
    )

    rows = list_console_training_runs(
        market="sol",
        cycle="5m",
        model_family="deep_otm",
        target="direction",
        root=tmp_path,
    )

    assert rows[0]["cycle"] == "5m"
    assert rows[0]["run_label"] == "console-local"


def test_pm5min_console_bundle_detail_uses_pm5min_bundle_registry(tmp_path, monkeypatch) -> None:
    cfg = ResearchConfig.build(
        market="sol",
        cycle="5m",
        profile="deep_otm_5m",
        target="direction",
        model_family="deep_otm",
        root=tmp_path,
    )
    bundle_dir = cfg.layout.model_bundle_dir(profile="deep_otm_5m", target="direction", bundle_label="console-bundle")
    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "summary.json").write_text(
        json.dumps({"market": "sol", "cycle": "5m", "profile": "deep_otm_5m", "target": "direction", "bundle_label": "console-bundle"}),
        encoding="utf-8",
    )
    (bundle_dir / "manifest.json").write_text(
        json.dumps({"object_type": "model_bundle", "market": "sol", "cycle": "5m", "spec": {"profile": "deep_otm_5m", "target": "direction", "bundle_label": "console-bundle"}}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "pm15min.research.service.get_active_bundle_selection",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("delegated to pm15min.research.service.get_active_bundle_selection")),
    )

    payload = load_console_model_bundle(
        market="sol",
        cycle="5m",
        profile="deep_otm_5m",
        target="direction",
        bundle_label="console-bundle",
        root=tmp_path,
    )

    assert payload["cycle"] == "5m"
    assert payload["bundle_label"] == "console-bundle"
```

```python
# tests/test_pm5min_cli.py
def test_pm5min_console_service_module_does_not_delegate_to_pm15min_console_service() -> None:
    text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "console" / "service.py").read_text(
        encoding="utf-8"
    )
    assert "pm15min.console.service" not in text
    assert "pm15min.console.read_models" not in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_console_read_models.py tests/test_pm5min_cli.py -k 'console_data_overview or console_training_runs or console_bundle_detail or console_service_module'`
Expected: FAIL because the local console read-model modules and `src/pm5min/console/service.py` do not exist yet.

- [ ] **Step 3: Write the minimal local read-model implementation**

```python
# src/pm5min/console/read_models/common.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def read_json_object(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None
```

```python
# src/pm5min/console/read_models/data_overview.py
from pm5min.data.config import DataConfig
from pm5min.data.service import describe_data_runtime, show_data_summary

from .common import json_ready, read_json_object


def load_data_overview(*, market: str, cycle: str | int = "5m", surface: str = "backtest", root=None, prefer_persisted: bool = True, now=None) -> dict[str, object]:
    cfg = DataConfig.build(market=market, cycle=cycle, surface=surface, root=root)
    return load_data_overview_from_config(cfg, prefer_persisted=prefer_persisted, now=now)


def load_data_overview_from_config(cfg: DataConfig, *, prefer_persisted: bool = True, now=None) -> dict[str, object]:
    summary_payload = _load_persisted_summary(cfg) if prefer_persisted else None
    if summary_payload is None:
        summary_payload = show_data_summary(cfg, persist=False)
    return json_ready(
        {
            "domain": "console",
            "dataset": "console_data_overview",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "surface": cfg.surface,
            "runtime": describe_data_runtime(cfg),
            "summary": dict(summary_payload.get("summary") or {}),
            "audit": dict(summary_payload.get("audit") or {}),
            "completeness": dict(summary_payload.get("completeness") or {}),
            "issues": list(summary_payload.get("issues") or []),
            "datasets": dict(summary_payload.get("datasets") or {}),
            "latest_manifest": read_json_object(cfg.layout.latest_summary_manifest_path),
        }
    )
```

```python
# src/pm5min/console/read_models/training_runs.py
from pm5min.research.config import ResearchConfig, normalize_label_set
from pm5min.research.manifests import read_manifest
from pm5min.research.service import list_training_runs as _list_training_runs

from .common import json_ready, read_json_object


def list_console_training_runs(*, market: str, cycle: str | int = "5m", model_family: str | None = None, target: str | None = None, prefix: str | None = None, root=None) -> list[dict[str, object]]:
    cfg = ResearchConfig.build(
        market=market,
        cycle=cycle,
        profile="deep_otm_5m",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target=target or "direction",
        model_family=model_family or "deep_otm",
        root=root,
    )
    rows = _list_training_runs(cfg, model_family=model_family, target=target, prefix=prefix)
    return json_ready([_build_training_run_row(Path(str(row["path"]))) for row in rows])


def load_console_training_run(*, market: str, cycle: str | int = "5m", model_family: str | None = None, target: str | None = None, run_label: str | None = None, run_dir=None, root=None) -> dict[str, object]:
    cfg = ResearchConfig.build(
        market=market,
        cycle=cycle,
        profile="deep_otm_5m",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target=target or "direction",
        model_family=model_family or "deep_otm",
        root=root,
    )
    resolved_run_dir = _resolve_training_run_dir(cfg, model_family=model_family, target=target, run_label=run_label, run_dir=run_dir)
    row = _build_training_run_row(resolved_run_dir)
    offset_details = _build_offset_details(resolved_run_dir)
    return json_ready({"domain": "console", "dataset": "console_training_run", **row, "offset_details": offset_details})
```

```python
# src/pm5min/console/read_models/bundles.py
from pm5min.research.bundles.loader import read_bundle_summary, read_model_bundle_manifest, resolve_model_bundle_dir
from pm5min.research.config import ResearchConfig, normalize_label_set
from pm5min.research.service import get_active_bundle_selection as _get_active_bundle_selection
from pm5min.research.service import list_model_bundles as _list_model_bundles

from .common import json_ready, read_json_object


def list_console_model_bundles(*, market: str, cycle: str | int = "5m", profile: str | None = None, target: str | None = None, prefix: str | None = None, root=None) -> list[dict[str, object]]:
    cfg = ResearchConfig.build(
        market=market,
        cycle=cycle,
        profile=profile or "deep_otm_5m",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target=target or "direction",
        model_family="deep_otm",
        root=root,
    )
    active = _get_active_bundle_selection(cfg, profile=profile or cfg.profile, target=target or cfg.target)
    rows = _list_model_bundles(cfg, profile=profile, target=target, prefix=prefix)
    return json_ready([_with_active_bundle_row(_build_bundle_row(Path(str(row["path"]))), active) for row in rows])


def load_console_model_bundle(*, market: str, cycle: str | int = "5m", profile: str, target: str, bundle_label: str | None = None, bundle_dir=None, root=None) -> dict[str, object]:
    cfg = ResearchConfig.build(
        market=market,
        cycle=cycle,
        profile=profile,
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target=target,
        model_family="deep_otm",
        root=root,
    )
    resolved_bundle_dir = Path(bundle_dir) if bundle_dir is not None else resolve_model_bundle_dir(cfg, profile=profile, target=target, bundle_label=bundle_label)
    row = _build_bundle_row(resolved_bundle_dir)
    return json_ready(
        {
            "domain": "console",
            "dataset": "console_model_bundle",
            **row,
            "active_selection": _get_active_bundle_selection(cfg, profile=profile, target=target),
        }
    )
```

```python
# src/pm5min/console/read_models/__init__.py
from .backtests import describe_console_backtest_run, list_console_backtest_runs
from .bundles import list_console_model_bundles, load_console_model_bundle
from .data_overview import load_data_overview
from .experiments import describe_console_experiment_run, list_console_experiment_runs
from .training_runs import list_console_training_runs, load_console_training_run
```

Port the helper bodies from the matching `src/pm15min/console/read_models/*.py` files, but make these changes while copying:

- switch every `pm15min.data.*` import to the `pm5min.data.*` equivalent
- switch every `pm15min.research.*` import to the `pm5min.research.*` equivalent
- change default cycles from `"15m"` to `"5m"`
- keep row/detail payload formats stable so existing console consumers continue to work
- do **not** import any module from `pm15min.console.*`

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_console_read_models.py tests/test_pm5min_cli.py -k 'console_data_overview or console_training_runs or console_bundle_detail or console_service_module'`
Expected: PASS

- [ ] **Step 5: Run a compatibility smoke test**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k 'pm5min_console_parser_uses_5m_defaults or pm5min_console_show_data_overview_uses_5m_cycle or pm5min_console_show_bundle_uses_5m_profile_and_cycle'`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm5min/console/read_models/__init__.py src/pm5min/console/read_models/common.py src/pm5min/console/read_models/data_overview.py src/pm5min/console/read_models/training_runs.py src/pm5min/console/read_models/bundles.py tests/test_pm5min_console_read_models.py tests/test_pm5min_cli.py
git commit -m "feat: split pm5min console data and research read models"
```

## Task 2: Split Backtest / Experiment Read Models

**Files:**
- Create: `src/pm5min/console/read_models/backtests.py`
- Create: `src/pm5min/console/read_models/experiments.py`
- Modify: `src/pm5min/console/read_models/__init__.py`
- Modify: `tests/test_pm5min_console_read_models.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pm5min_console_read_models.py
def test_pm5min_console_backtest_read_models_use_5m_layout(tmp_path) -> None:
    root = tmp_path / "v2"
    run_dir = (
        root
        / "research"
        / "backtests"
        / "cycle=5m"
        / "asset=sol"
        / "profile=deep_otm_5m"
        / "spec=baseline_truth"
        / "run=bt_console"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "summary.json").write_text(
        json.dumps({"market": "sol", "cycle": "5m", "profile": "deep_otm_5m", "spec_name": "baseline_truth", "trades": 5}),
        encoding="utf-8",
    )

    rows = list_console_backtest_runs(
        market="sol",
        cycle="5m",
        profile="deep_otm_5m",
        root=root,
    )

    assert rows[0]["cycle"] == "5m"
    assert rows[0]["profile"] == "deep_otm_5m"


def test_pm5min_console_experiment_read_models_use_pm5min_layout(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.research.layout.ResearchLayout.discover",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("delegated to pm15min.research.layout.ResearchLayout")),
    )

    root = tmp_path / "v2"
    run_dir = root / "research" / "experiments" / "runs" / "suite=console_suite" / "run=exp_console"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "summary.json").write_text(
        json.dumps({"suite_name": "console_suite", "run_label": "exp_console", "cases": 4}),
        encoding="utf-8",
    )

    rows = list_console_experiment_runs(suite_name="console_suite", root=root)

    assert rows[0]["suite_name"] == "console_suite"
    assert rows[0]["run_label"] == "exp_console"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_console_read_models.py -k 'console_backtest_read_models or console_experiment_read_models'`
Expected: FAIL because `backtests.py` and `experiments.py` do not exist yet.

- [ ] **Step 3: Port the backtest and experiment readers**

```python
# src/pm5min/console/read_models/backtests.py
from pm5min.research.config import ResearchConfig
from pm5min.research.manifests import read_manifest


def list_console_backtest_runs(*, market: str, cycle: str | int = "5m", profile: str | None = None, spec_name: str | None = None, prefix: str | None = None, root=None) -> list[dict[str, object]]:
    cfg = ResearchConfig.build(market=market, cycle=cycle, profile=profile or "deep_otm_5m", root=root)
    runs_root = cfg.layout.backtests_root
    rows = [path for path in runs_root.glob("profile=*/spec=*/run=*") if path.is_dir()]
    return [_build_backtest_run_row(path) for path in sorted(rows, key=lambda path: (path.stat().st_mtime_ns, path.name), reverse=True)]


def describe_console_backtest_run(*, market: str, profile: str, spec_name: str, run_label: str, cycle: str | int = "5m", root=None) -> dict[str, object]:
    cfg = ResearchConfig.build(market=market, cycle=cycle, profile=profile, root=root)
    run_dir = cfg.layout.backtest_run_dir(profile=profile, spec_name=spec_name, run_label_text=run_label)
    return _build_backtest_run_detail(run_dir)
```

```python
# src/pm5min/console/read_models/experiments.py
from pm5min.research.layout import ResearchLayout
from pm5min.research.layout_helpers import slug_token
from pm5min.research.manifests import read_manifest


def list_console_experiment_runs(*, suite_name: str | None = None, prefix: str | None = None, root=None) -> list[dict[str, object]]:
    storage = ResearchLayout.discover(root=root)
    rows = [path for path in storage.experiment_runs_root.glob("suite=*/run=*") if path.is_dir()]
    return [_build_experiment_run_row(path) for path in sorted(rows, key=lambda path: (path.stat().st_mtime_ns, path.name), reverse=True)]


def describe_console_experiment_run(*, suite_name: str, run_label: str, root=None) -> dict[str, object]:
    storage = ResearchLayout.discover(root=root)
    return _build_experiment_run_detail(storage.experiment_run_dir(suite_name, run_label))
```

While porting from the `pm15min` versions, make these exact changes:

- use `pm5min.research.config.ResearchConfig` for backtest path resolution
- use `pm5min.research.layout.ResearchLayout` and `pm5min.research.layout_helpers.slug_token` for experiment roots
- keep the preview-building helpers and returned field names unchanged
- keep default profile and cycle handling aligned with 5m CLI defaults

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_console_read_models.py -k 'console_backtest_read_models or console_experiment_read_models'`
Expected: PASS

- [ ] **Step 5: Run the existing analysis-read smoke tests**

Run: `PYTHONPATH=src pytest -q tests/test_console_analysis_runs.py -k 'backtest_read_models or experiment_read_models'`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm5min/console/read_models/backtests.py src/pm5min/console/read_models/experiments.py src/pm5min/console/read_models/__init__.py tests/test_pm5min_console_read_models.py
git commit -m "feat: split pm5min console analysis read models"
```

## Task 3: Split Local Action Planning And Read-Only Task Views

**Files:**
- Create: `src/pm5min/console/actions.py`
- Create: `src/pm5min/console/tasks.py`
- Create: `tests/test_pm5min_console_tasks.py`
- Modify: `tests/test_pm5min_cli.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pm5min_console_tasks.py
from __future__ import annotations

import json

from pm5min.console.actions import build_console_action_request, load_console_action_catalog
from pm5min.console.tasks import list_console_tasks, load_console_runtime_history, load_console_runtime_summary, load_console_task


def test_pm5min_console_action_plan_uses_pm5min_command_preview() -> None:
    payload = build_console_action_request(
        action_id="research_activate_bundle",
        request={"market": "sol", "bundle_label": "main"},
    )

    assert payload["command_preview"].startswith("PYTHONPATH=src python -m pm5min")
    assert payload["normalized_request"]["cycle"] == "5m"
    assert payload["normalized_request"]["profile"] == "deep_otm_5m"


def test_pm5min_console_task_reads_stay_local(tmp_path, monkeypatch) -> None:
    task_path = tmp_path / "var" / "console" / "tasks" / "task_demo.json"
    task_path.parent.mkdir(parents=True, exist_ok=True)
    task_path.write_text(
        json.dumps(
            {
                "task_id": "task_demo",
                "action_id": "research_train_run",
                "status": "succeeded",
                "created_at": "2026-04-13T00:00:00+00:00",
                "updated_at": "2026-04-13T00:01:00+00:00",
                "started_at": "2026-04-13T00:00:10+00:00",
                "finished_at": "2026-04-13T00:01:00+00:00",
                "request": {"market": "sol", "cycle": "5m"},
                "command_preview": "PYTHONPATH=src python -m pm5min research train build-datasets --market sol --cycle 5m",
                "result": {"status": "ok"},
                "progress": {"summary": "Completed", "current_stage": "finished", "progress_pct": 100, "heartbeat": "2026-04-13T00:01:00+00:00"},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "pm15min.console.tasks.list_console_tasks",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("delegated to pm15min.console.tasks.list_console_tasks")),
    )
    monkeypatch.setattr(
        "pm15min.console.tasks.load_console_task",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("delegated to pm15min.console.tasks.load_console_task")),
    )

    rows = list_console_tasks(root=tmp_path)
    detail = load_console_task(task_id="task_demo", root=tmp_path)

    assert rows[0]["task_id"] == "task_demo"
    assert detail["task_id"] == "task_demo"


def test_pm5min_console_runtime_views_recover_from_task_files(tmp_path) -> None:
    task_root = tmp_path / "var" / "console" / "tasks"
    task_root.mkdir(parents=True, exist_ok=True)
    (task_root / "task_demo.json").write_text(
        json.dumps(
            {
                "task_id": "task_demo",
                "action_id": "data_refresh_summary",
                "status": "running",
                "created_at": "2026-04-13T00:00:00+00:00",
                "updated_at": "2026-04-13T00:00:30+00:00",
                "started_at": "2026-04-13T00:00:05+00:00",
                "finished_at": None,
                "request": {"market": "sol", "cycle": "5m"},
                "command_preview": "PYTHONPATH=src python -m pm5min data show-summary --market sol --cycle 5m --write-state",
                "result": None,
                "error": None,
                "progress": {"summary": "Running", "current_stage": "running", "progress_pct": 40, "heartbeat": "2026-04-13T00:00:30+00:00"},
            }
        ),
        encoding="utf-8",
    )

    summary = load_console_runtime_summary(root=tmp_path)
    history = load_console_runtime_history(root=tmp_path)

    assert summary["task_count"] == 1
    assert summary["status_group_counts"]["active"] == 1
    assert history["row_count"] == 1
```

```python
# tests/test_pm5min_cli.py
def test_pm5min_console_compat_does_not_reference_pm15min_console_service() -> None:
    text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "console" / "compat.py").read_text(
        encoding="utf-8"
    )
    assert "pm15min.console.service" not in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_console_tasks.py tests/test_pm5min_cli.py -k 'action_plan or task_reads_stay_local or runtime_views_recover or console_compat_does_not_reference'`
Expected: FAIL because local action and task modules do not exist yet.

- [ ] **Step 3: Port local action planning**

```python
# src/pm5min/console/actions.py
from collections.abc import Mapping

from pm5min.core.assets import resolve_asset
from pm5min.data.layout import normalize_cycle as normalize_data_cycle
from pm5min.data.layout import normalize_surface
from pm5min.research.contracts import BacktestRunSpec, DateWindow, ExperimentRunSpec, ModelBundleSpec, TrainingRunSpec
from pm5min.research.config import normalize_label_set
from pm5min.research.layout_helpers import normalize_target, slug_token


DEFAULT_COMMAND_PREFIX = ("PYTHONPATH=src", "python", "-m", "pm5min")


def load_console_action_catalog(*, for_section: str | None = None, shell_enabled: bool | None = None) -> dict[str, object]:
    descriptors = list_console_action_descriptors(for_section=for_section, shell_enabled=shell_enabled)
    return {
        "domain": "console",
        "dataset": "console_action_catalog",
        "for_section": for_section,
        "shell_enabled": shell_enabled,
        "action_count": len(descriptors),
        "actions": descriptors,
    }


def build_console_action_request(action_id: str, request: Mapping[str, object] | None = None) -> dict[str, object]:
    payload = {} if request is None else {str(key): value for key, value in request.items()}
    definition = _ACTION_BY_ID[str(action_id).strip()]
    return definition.builder(payload).to_dict()
```

Port `src/pm15min/console/actions.py` into this new file, but make these concrete replacements while copying:

- `DEFAULT_COMMAND_PREFIX = ("PYTHONPATH=src", "python", "-m", "pm5min")`
- default data/research cycles become `"5m"`
- default console profile becomes `"deep_otm_5m"`
- all imports must come from `pm5min.*`
- keep action ids, descriptor shapes, normalized request keys, and validation behavior stable

- [ ] **Step 4: Port local read-only task/runtime views**

```python
# src/pm5min/console/tasks.py
from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from pm5min.core.layout import rewrite_root


def default_console_tasks_root() -> Path:
    return rewrite_root() / "var" / "console" / "tasks"


def default_console_runtime_state_root() -> Path:
    return rewrite_root() / "var" / "console" / "state"


def load_console_task(*, task_id: str, root=None) -> dict[str, object]:
    record = get_console_task(task_id, root=root)
    if record is None:
        raise FileNotFoundError(f"Console task not found: {task_id}")
    return _task_detail_payload(record, root=root)


def list_console_tasks(*, action_id: str | None = None, action_ids=None, status: str | None = None, status_group: str | None = None, limit: int = 20, root=None) -> list[dict[str, object]]:
    rows = _scan_task_history(_resolve_tasks_root(root)).records
    return _filtered_task_rows(rows, action_id=action_id, action_ids=action_ids, status=status, status_group=status_group, limit=limit)


def load_console_runtime_summary(*, root=None) -> dict[str, object]:
    tasks_root = _resolve_tasks_root(root)
    return _load_or_rebuild_runtime_summary(tasks_root)


def load_console_runtime_history(*, root=None) -> dict[str, object]:
    tasks_root = _resolve_tasks_root(root)
    return _load_or_rebuild_runtime_history(tasks_root)
```

Port only the read-path support from `src/pm15min/console/tasks.py`:

- keep the record/progress dataclasses and scan helpers
- keep persisted-summary/history recovery behavior
- keep list/filter semantics for `status`, `status_group`, and `action_ids`
- remove submission/execution code from this local file
- do **not** import `pm15min.console.tasks`

- [ ] **Step 5: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_console_tasks.py tests/test_pm5min_cli.py -k 'action_plan or task_reads_stay_local or runtime_views_recover or console_compat_does_not_reference'`
Expected: PASS

- [ ] **Step 6: Run baseline console task tests**

Run: `PYTHONPATH=src pytest -q tests/test_console_tasks.py -k 'submit_get_and_persist or runtime_summary_persists_active_and_terminal_history'`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/pm5min/console/actions.py src/pm5min/console/tasks.py tests/test_pm5min_console_tasks.py tests/test_pm5min_cli.py
git commit -m "feat: split pm5min console actions and task readers"
```

## Task 4: Rewire Service / Handlers / Compat And Lock The Boundary

**Files:**
- Create: `src/pm5min/console/service.py`
- Modify: `src/pm5min/console/handlers.py`
- Modify: `src/pm5min/console/compat.py`
- Modify: `tests/test_pm5min_cli.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pm5min_cli.py
def test_pm5min_console_read_commands_stay_local(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "pm15min.console.service.load_console_data_overview",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pm5min console read command delegated to pm15min.console.service")),
    )
    monkeypatch.setattr(
        "pm15min.console.service.load_console_bundle",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pm5min console bundle command delegated to pm15min.console.service")),
    )
    monkeypatch.setattr("pm5min.core.layout.rewrite_root", lambda: tmp_path / "v2")
    monkeypatch.setattr("pm5min.data.layout.rewrite_root", lambda: tmp_path / "v2")
    monkeypatch.setattr("pm5min.research.layout.rewrite_root", lambda: tmp_path / "v2")

    rc = main(["console", "show-data-overview", "--market", "sol"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_pm5min_console_show_actions_and_build_action_stay_local(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.console.service.load_console_action_catalog",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pm5min console action catalog delegated to pm15min.console.service")),
    )

    rc = main(["console", "show-actions"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_action_catalog"

    rc = main(["console", "build-action", "--action-id", "research_activate_bundle", "--request-json", '{"market":"sol","bundle_label":"main"}'])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command_preview"].startswith("PYTHONPATH=src python -m pm5min")


def test_pm5min_console_execute_and_serve_keep_explicit_compat_paths(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm5min.console.compat.execute_console_action",
        lambda action_id, request: {"dataset": "console_action_execution", "action_id": action_id, "request": request},
    )
    monkeypatch.setattr(
        "pm5min.console.compat.serve_console_http",
        lambda host, port, poll_interval: None,
    )

    rc = main(["console", "execute-action", "--action-id", "research_activate_bundle", "--request-json", '{"bundle_label":"main"}'])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_action_execution"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k 'console_read_commands_stay_local or show_actions_and_build_action_stay_local or execute_and_serve_keep_explicit_compat_paths'`
Expected: FAIL because `handlers.py` still routes all commands through compat and `compat.py` still references `pm15min.console.service`.

- [ ] **Step 3: Create the local service aggregator**

```python
# src/pm5min/console/service.py
from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from .actions import build_console_action_request, load_console_action_catalog
from .read_models.backtests import describe_console_backtest_run, list_console_backtest_runs
from .read_models.bundles import list_console_model_bundles, load_console_model_bundle
from .read_models.data_overview import load_data_overview
from .read_models.experiments import describe_console_experiment_run, list_console_experiment_runs
from .read_models.training_runs import list_console_training_runs, load_console_training_run
from .tasks import list_console_tasks, load_console_runtime_history, load_console_runtime_summary, load_console_task


def load_console_home(*, root: Path | None = None) -> dict[str, object]:
    runtime_summary = load_console_runtime_state(root=root)
    sections = list_console_sections()
    return {
        "domain": "console",
        "dataset": "console_home",
        "read_only": True,
        "sections": sections,
        "section_count": len(sections),
        "runtime_summary": runtime_summary,
        "action_catalog": load_console_action_catalog(),
    }


def load_console_runtime_state(*, root: Path | None = None) -> dict[str, object]:
    return _runtime_state_payload(dict(load_console_runtime_summary(root=root)))
```

Port the payload-shaping helpers from `src/pm15min/console/service.py`, but keep this file limited to read-only aggregation:

- section definitions
- runtime/task summary shaping
- action catalog and action-plan shaping
- read-model dispatch

Do **not** put execution or HTTP server code in this file.

- [ ] **Step 4: Rewire handlers and shrink compat**

```python
# src/pm5min/console/handlers.py
from .compat import execute_console_action, serve_console_http, submit_console_action_task
from .service import (
    build_console_action_request,
    list_console_backtests,
    list_console_bundles,
    list_console_experiments,
    list_console_tasks,
    list_console_training_runs,
    load_console_action_catalog,
    load_console_backtest,
    load_console_bundle,
    load_console_data_overview,
    load_console_experiment,
    load_console_home,
    load_console_runtime_history,
    load_console_runtime_state,
    load_console_task,
    load_console_training_run,
)
```

```python
# src/pm5min/console/compat.py
def execute_console_action(*, action_id: str, request: dict[str, object]) -> dict[str, object]:
    module = importlib.import_module("pm15min.console.action_runner")
    return getattr(module, "execute_console_action")(action_id=action_id, request=request)


def submit_console_action_task(*, action_id: str, request: dict[str, object]) -> dict[str, object]:
    module = importlib.import_module("pm15min.console.tasks")
    return getattr(module, "submit_console_action_task")(action_id=action_id, request=request)


def serve_console_http(*, host: str, port: int, poll_interval: float) -> None:
    module = importlib.import_module("pm15min.console.http")
    return getattr(module, "serve_console_http")(host=host, port=port, poll_interval=poll_interval)
```

After this step, `src/pm5min/console/compat.py` must not contain `pm15min.console.service` or `pm15min.console.read_models`.

- [ ] **Step 5: Run focused verification**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_console_read_models.py tests/test_pm5min_console_tasks.py tests/test_pm5min_cli.py -k 'console'`
Expected: PASS

Run: `PYTHONPATH=src pytest -q tests/test_pmshared_architecture.py tests/test_architecture_guards.py`
Expected: PASS or existing skips only

- [ ] **Step 6: Run broader console smoke tests**

Run: `PYTHONPATH=src pytest -q tests/test_console_cli.py -k 'show_home_and_serve or show_training_run_and_bundle'`
Expected: PASS

Run: `PYTHONPATH=src pytest -q tests/test_console_research_assets.py tests/test_console_analysis_runs.py`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/pm5min/console/service.py src/pm5min/console/handlers.py src/pm5min/console/compat.py tests/test_pm5min_cli.py
git commit -m "feat: split pm5min console control plane"
```

## Final Verification Checklist

- [ ] `src/pm5min/console/compat.py` references only:
  - `pm15min.console.action_runner`
  - `pm15min.console.tasks`
  - `pm15min.console.http`
- [ ] `src/pm5min/console/service.py` does not import any `pm15min.console.*` module
- [ ] `src/pm5min/console/read_models/*.py` use `pm5min.data.*` / `pm5min.research.*` only
- [ ] `build-action` previews point to `python -m pm5min`
- [ ] `show-data-overview`, `show-bundle`, `list-training-runs`, `list-backtests`, and `list-experiments` return `cycle == "5m"` or the expected 5m profile defaults
- [ ] `execute-action` and `serve` still work through explicit compat fallbacks
