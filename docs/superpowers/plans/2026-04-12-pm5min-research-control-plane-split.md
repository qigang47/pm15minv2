# PM5Min Research Control Plane Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the `pm5min/research` control plane so artifact registry reads/writes and lightweight research CLI commands no longer depend on `pm15min.research.service`.

**Architecture:** Keep `parser.py` as the argument contract, introduce a local `handlers.py` command router, and move bundle-selection / artifact-discovery logic into `pm5min/research` local helper modules. Do not try to split the full training/backtest/evaluation execution stack in this plan; deep execution commands may keep an explicit compat fallback for now, but lightweight commands and services must become package-local.

**Tech Stack:** Python 3.11+, `argparse`, `pathlib`, `json`, package-local layout/config modules under `src/pm5min/research`, `pytest`.

---

## Scope And Non-Goals

- This plan covers:
  - `list-runs`
  - `list-bundles`
  - `show-active-bundle`
  - `activate-bundle`
  - local `pm5min.research.service`
  - local research command routing for the commands above plus `show-config` / `show-layout`
- This plan does **not** cover:
  - full research execution builders under `build/train/bundle/backtest/experiment/evaluate`
  - training datasets, model training, backtest replay, experiment runners, or evaluation methods
- After this plan, `pm5min/research` should still be allowed to keep a narrow compat path for deep execution commands, but it should no longer import or call `pm15min.research.service`.

## File Structure

- Create: `src/pm5min/research/manifests.py`
  - Local manifest read/write helpers used by research bundle metadata.
- Create: `src/pm5min/research/bundles/__init__.py`
  - Local package marker for bundle helpers.
- Create: `src/pm5min/research/bundles/active_registry.py`
  - Local active-bundle selection path resolution, read, write, and bundle-dir resolution.
- Create: `src/pm5min/research/bundles/loader.py`
  - Local training-run / bundle directory resolution plus manifest/summary readers.
- Modify: `src/pm5min/research/service.py`
  - Replace compat-backed artifact registry helpers with package-local implementations.
- Create: `src/pm5min/research/handlers.py`
  - Local research command dispatcher for lightweight commands and explicit compat fallback for deep execution commands.
- Modify: `src/pm5min/research/cli.py`
  - Make CLI a thin wrapper around parser + handlers, matching the `pm5min/data` and `pm5min/console` shape.
- Modify: `src/pm5min/research/compat.py`
  - Remove service-module loading; keep only the deep execution CLI fallback helpers.
- Create: `tests/test_pm5min_research_service.py`
  - Focused tests for local research artifact discovery and active bundle activation.
- Modify: `tests/test_pm5min_cli.py`
  - Add guard tests proving lightweight research commands stay local and no longer route through `pm15min.research.service`.

## Task 1: Split Local Research Artifact Registry Support

**Files:**
- Create: `src/pm5min/research/manifests.py`
- Create: `src/pm5min/research/bundles/__init__.py`
- Create: `src/pm5min/research/bundles/active_registry.py`
- Create: `src/pm5min/research/bundles/loader.py`
- Modify: `src/pm5min/research/service.py`
- Create: `tests/test_pm5min_research_service.py`
- Modify: `tests/test_pm5min_cli.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pm5min_research_service.py
from __future__ import annotations

import json

from pm5min.research.config import ResearchConfig
from pm5min.research.service import (
    activate_model_bundle,
    get_active_bundle_selection,
    list_model_bundles,
    list_training_runs,
)


def _write_bundle_manifest(bundle_dir, *, market: str, cycle: str, bundle_label: str, source_run_dir: str) -> None:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "object_type": "model_bundle",
                "object_id": f"{market}-{cycle}-{bundle_label}",
                "market": market,
                "cycle": cycle,
                "path": str(bundle_dir),
                "created_at": "2026-04-12T00:00:00Z",
                "spec": {
                    "bundle_label": bundle_label,
                    "usage": "manual_activation",
                    "source_training_run": source_run_dir,
                    "offsets": [2, 3, 4],
                    "feature_set": "deep_otm_v1",
                    "label_set": "truth",
                    "source_group": "control_plane_test",
                },
                "inputs": [],
                "outputs": [],
                "metadata": {
                    "allowed_blacklist_columns": [],
                },
            },
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_pm5min_research_service_lists_training_runs_from_5m_root(tmp_path, monkeypatch) -> None:
    cfg = ResearchConfig.build(market="sol", cycle="5m", root=tmp_path)
    run_dir = cfg.layout.training_run_dir(model_family="deep_otm", target="direction", run_label="demo")
    run_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "pm15min.research.service.list_training_runs",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pm5min research service delegated to pm15min")),
    )

    rows = list_training_runs(cfg, model_family="deep_otm", target="direction")

    assert rows == [
        {
            "object_type": "training_run",
            "market": "sol",
            "cycle": "5m",
            "model_family": "deep_otm",
            "target": "direction",
            "run": "demo",
        }
    ] or rows[0]["cycle"] == "5m"


def test_pm5min_research_service_lists_bundles_from_5m_root(tmp_path, monkeypatch) -> None:
    cfg = ResearchConfig.build(market="sol", cycle="5m", root=tmp_path)
    bundle_dir = cfg.layout.model_bundle_dir(profile="deep_otm_5m", target="direction", bundle_label="demo")
    _write_bundle_manifest(bundle_dir, market="sol", cycle="5m", bundle_label="demo", source_run_dir="run=demo")

    monkeypatch.setattr(
        "pm15min.research.service.list_model_bundles",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pm5min research bundle listing delegated to pm15min")),
    )

    rows = list_model_bundles(cfg, profile="deep_otm_5m", target="direction")

    assert rows[0]["cycle"] == "5m"
    assert rows[0]["bundle"] == "demo"


def test_pm5min_research_service_reads_active_bundle_selection_from_5m_root(tmp_path, monkeypatch) -> None:
    cfg = ResearchConfig.build(market="sol", cycle="5m", root=tmp_path, profile="deep_otm_5m")
    selection_path = cfg.layout.active_bundle_selection_path(profile="deep_otm_5m", target="direction")
    selection_path.parent.mkdir(parents=True, exist_ok=True)
    selection_path.write_text(
        json.dumps(
            {
                "market": "sol",
                "cycle": "5m",
                "profile": "deep_otm_5m",
                "target": "direction",
                "bundle_label": "demo",
                "bundle_dir": str(cfg.layout.model_bundle_dir(profile="deep_otm_5m", target="direction", bundle_label="demo")),
                "source_run_dir": "run=demo",
                "usage": "manual_activation",
                "activated_at": "2026-04-12T00:00:00Z",
                "notes": "",
                "metadata": {},
            },
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "pm15min.research.service.get_active_bundle_selection",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pm5min active bundle lookup delegated to pm15min")),
    )

    payload = get_active_bundle_selection(cfg, profile="deep_otm_5m", target="direction")

    assert payload["cycle"] == "5m"
    assert payload["selection"]["bundle_label"] == "demo"


def test_pm5min_research_service_activates_bundle_under_5m_root(tmp_path, monkeypatch) -> None:
    cfg = ResearchConfig.build(market="sol", cycle="5m", root=tmp_path, profile="deep_otm_5m")
    run_dir = cfg.layout.training_run_dir(model_family="deep_otm", target="direction", run_label="demo")
    run_dir.mkdir(parents=True, exist_ok=True)
    bundle_dir = cfg.layout.model_bundle_dir(profile="deep_otm_5m", target="direction", bundle_label="demo")
    _write_bundle_manifest(bundle_dir, market="sol", cycle="5m", bundle_label="demo", source_run_dir=str(run_dir))

    monkeypatch.setattr(
        "pm15min.research.service.activate_model_bundle",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pm5min activate bundle delegated to pm15min")),
    )

    payload = activate_model_bundle(
        cfg,
        profile="deep_otm_5m",
        target="direction",
        bundle_label="demo",
    )

    assert payload["cycle"] == "5m"
    assert payload["selection"]["bundle_label"] == "demo"
    assert payload["selection_path"].endswith("selection.json")
```

```python
# tests/test_pm5min_cli.py
def test_pm5min_research_service_module_does_not_delegate_to_pm15min_service() -> None:
    text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "research" / "service.py").read_text(
        encoding="utf-8"
    )
    assert "from .compat import" not in text
    assert "pm15min.research.service" not in text
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_research_service.py tests/test_pm5min_cli.py -k 'research_service or service_module_does_not_delegate'`

Expected: FAIL because `pm5min.research.service` still imports compat wrappers that call `pm15min.research.service`.

- [ ] **Step 3: Write the minimal implementation**

```python
# src/pm5min/research/manifests.py
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_SCHEMA_VERSION = "pm5min.research.v1"


def utc_manifest_timestamp(now: datetime | None = None) -> str:
    ts = datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_manifest_path(path: str | Path) -> Path:
    target = Path(path)
    return target if target.suffix.lower() == ".json" else target / "manifest.json"


@dataclass(frozen=True)
class ResearchManifest:
    object_type: str
    object_id: str
    market: str
    cycle: str
    path: str
    created_at: str
    spec: dict[str, Any] = field(default_factory=dict)
    inputs: list[dict[str, Any]] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: str = _SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def read_manifest(path: str | Path) -> ResearchManifest:
    payload = json.loads(resolve_manifest_path(path).read_text(encoding="utf-8"))
    return ResearchManifest(
        object_type=str(payload["object_type"]),
        object_id=str(payload["object_id"]),
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        path=str(payload["path"]),
        created_at=str(payload["created_at"]),
        spec=dict(payload.get("spec") or {}),
        inputs=list(payload.get("inputs") or []),
        outputs=list(payload.get("outputs") or []),
        metadata=dict(payload.get("metadata") or {}),
        schema_version=str(payload.get("schema_version") or _SCHEMA_VERSION),
    )
```

```python
# src/pm5min/research/bundles/__init__.py
from .active_registry import read_active_bundle_selection, write_active_bundle_selection
from .loader import read_model_bundle_manifest, resolve_model_bundle_dir

__all__ = [
    "read_active_bundle_selection",
    "write_active_bundle_selection",
    "read_model_bundle_manifest",
    "resolve_model_bundle_dir",
]
```

```python
# src/pm5min/research/bundles/active_registry.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..config import ResearchConfig
from ..layout_helpers import normalize_target, slug_token
from ..manifests import utc_manifest_timestamp


def read_active_bundle_selection(cfg: ResearchConfig, *, profile: str, target: str) -> dict[str, Any] | None:
    path = cfg.layout.active_bundle_selection_path(profile=profile, target=target)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_active_bundle_selection(
    cfg: ResearchConfig,
    *,
    profile: str,
    target: str,
    bundle_label: str,
    bundle_dir: str | Path,
    source_run_dir: str | Path,
    usage: str,
    notes: str | None = None,
    metadata: dict[str, Any] | None = None,
    activated_at: str | None = None,
) -> Path:
    path = cfg.layout.active_bundle_selection_path(profile=profile, target=target)
    payload = {
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "profile": slug_token(profile),
        "target": normalize_target(target),
        "bundle_label": slug_token(bundle_label, default="planned"),
        "bundle_dir": str(Path(bundle_dir)),
        "source_run_dir": str(Path(source_run_dir)),
        "usage": str(usage),
        "activated_at": str(activated_at or utc_manifest_timestamp()),
        "notes": str(notes or ""),
        "metadata": dict(metadata or {}),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return path
```

```python
# src/pm5min/research/bundles/loader.py
from __future__ import annotations

from pathlib import Path

from ..config import ResearchConfig
from ..manifests import read_manifest
from .active_registry import read_active_bundle_selection


def resolve_model_bundle_dir(
    cfg: ResearchConfig,
    *,
    profile: str,
    target: str,
    bundle_label: str | None = None,
) -> Path:
    root = cfg.layout.model_bundles_root / f"profile={profile}" / f"target={target}"
    if bundle_label:
        direct = root / f"bundle={bundle_label}"
        if direct.exists():
            return direct
        candidate = Path(bundle_label)
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"Model bundle not found: {bundle_label}")
    selection = read_active_bundle_selection(cfg, profile=profile, target=target)
    if selection:
        active_dir = Path(str(selection.get("bundle_dir") or ""))
        if active_dir.exists():
            return active_dir
    candidates = sorted([path for path in root.glob("bundle=*") if path.is_dir()], key=lambda path: (path.stat().st_mtime_ns, path.name))
    if not candidates:
        raise FileNotFoundError(f"No model bundles available under {root}")
    return candidates[-1]


def read_model_bundle_manifest(bundle_dir: Path):
    return read_manifest(bundle_dir / "manifest.json")
```

```python
# src/pm5min/research/service.py
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .bundles.active_registry import read_active_bundle_selection, write_active_bundle_selection
from .bundles.loader import read_model_bundle_manifest, resolve_model_bundle_dir


def _dir_row(path: Path, *, object_kind: str, market: str) -> dict[str, str]:
    updated = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row = {
        "object_type": object_kind,
        "market": market,
        "name": path.name,
        "path": str(path),
        "updated_at": updated,
    }
    for part in path.parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        row[key] = value
    return row


def list_training_runs(cfg, *, model_family=None, target=None, prefix=None) -> list[dict[str, str]]:
    root = cfg.layout.training_runs_root
    if not root.exists():
        return []
    runs = [path for path in root.glob("model_family=*/target=*/run=*") if path.is_dir()]
    if model_family:
        runs = [path for path in runs if path.parent.parent.name == f"model_family={model_family}"]
    if target:
        runs = [path for path in runs if path.parent.name == f"target={target}"]
    if prefix:
        runs = [path for path in runs if path.name.startswith(f"run={prefix}")]
    runs = sorted(runs, key=lambda path: (path.stat().st_mtime_ns, path.name))
    return [_dir_row(path, object_kind="training_run", market=cfg.asset.slug) for path in runs]


def list_model_bundles(cfg, *, profile=None, target=None, prefix=None) -> list[dict[str, str]]:
    root = cfg.layout.model_bundles_root
    if not root.exists():
        return []
    bundles = [path for path in root.glob("profile=*/target=*/bundle=*") if path.is_dir()]
    if profile:
        bundles = [path for path in bundles if path.parent.parent.name == f"profile={profile}"]
    if target:
        bundles = [path for path in bundles if path.parent.name == f"target={target}"]
    if prefix:
        bundles = [path for path in bundles if path.name.startswith(f"bundle={prefix}")]
    bundles = sorted(bundles, key=lambda path: (path.stat().st_mtime_ns, path.name))
    return [_dir_row(path, object_kind="model_bundle", market=cfg.asset.slug) for path in bundles]


def get_active_bundle_selection(cfg, *, profile=None, target=None) -> dict[str, object]:
    selected_profile = str(profile or cfg.profile)
    selected_target = str(target or cfg.target)
    payload = read_active_bundle_selection(cfg, profile=selected_profile, target=selected_target)
    return {
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "profile": selected_profile,
        "target": selected_target,
        "selection_path": str(cfg.layout.active_bundle_selection_path(profile=selected_profile, target=selected_target)),
        "selection": payload,
    }


def activate_model_bundle(cfg, *, profile: str, target: str, bundle_label: str | None = None, notes: str | None = None) -> dict[str, object]:
    bundle_dir = resolve_model_bundle_dir(cfg, profile=profile, target=target, bundle_label=bundle_label)
    manifest = read_model_bundle_manifest(bundle_dir)
    selection_path = write_active_bundle_selection(
        cfg,
        profile=profile,
        target=target,
        bundle_label=str(manifest.spec.get("bundle_label") or bundle_dir.name.split("=", 1)[-1]),
        bundle_dir=bundle_dir,
        source_run_dir=str(manifest.spec.get("source_training_run") or ""),
        usage=str(manifest.spec.get("usage") or "manual_activation"),
        notes=notes or str(manifest.spec.get("notes") or ""),
        metadata={
            "offsets": list(manifest.spec.get("offsets") or []),
            "feature_set": manifest.spec.get("feature_set"),
            "label_set": manifest.spec.get("label_set"),
            "source_group": manifest.spec.get("source_group"),
            "allowed_blacklist_columns": list(manifest.metadata.get("allowed_blacklist_columns") or []),
            "activated_from_bundle_manifest": str(bundle_dir / "manifest.json"),
        },
    )
    return {
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "profile": profile,
        "target": target,
        "bundle_dir": str(bundle_dir),
        "selection_path": str(selection_path),
        "bundle_manifest_path": str(bundle_dir / "manifest.json"),
        "selection": read_active_bundle_selection(cfg, profile=profile, target=target),
    }
```

Implementation notes for `service.py`:
- Keep `describe_research_runtime()` in the file; only replace the compat-backed artifact helpers.
- Use `cfg.layout.training_runs_root` and `cfg.layout.model_bundles_root`; never call compat.
- Use the local bundle helpers above for active selection and manifest loading.
- Preserve the current JSON shape already exposed by `pm5min.cli`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_research_service.py tests/test_pm5min_cli.py -k 'research_service or service_module_does_not_delegate'`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pm5min/research/manifests.py src/pm5min/research/bundles/__init__.py src/pm5min/research/bundles/active_registry.py src/pm5min/research/bundles/loader.py src/pm5min/research/service.py tests/test_pm5min_research_service.py tests/test_pm5min_cli.py
git commit -m "feat: split pm5min research artifact registry"
```

## Task 2: Introduce Local Research Handlers And Explicit Compat Fallback

**Files:**
- Create: `src/pm5min/research/handlers.py`
- Modify: `src/pm5min/research/cli.py`
- Modify: `src/pm5min/research/compat.py`
- Modify: `tests/test_pm5min_cli.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pm5min_cli.py
def test_pm5min_research_list_runs_stays_local_and_does_not_delegate_to_pm15min_handlers(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.research.cli_handlers.run_research_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pm5min list-runs delegated to pm15min cli handlers")),
    )
    monkeypatch.setattr(
        "pm5min.research.service.list_training_runs",
        lambda cfg, **kwargs: [{"market": cfg.asset.slug, "cycle": cfg.cycle}],
    )

    rc = main(["research", "list-runs", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["cycle"] == "5m"


def test_pm5min_research_list_bundles_stays_local_and_does_not_delegate_to_pm15min_handlers(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.research.cli_handlers.run_research_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pm5min list-bundles delegated to pm15min cli handlers")),
    )
    monkeypatch.setattr(
        "pm5min.research.service.list_model_bundles",
        lambda cfg, **kwargs: [{"market": cfg.asset.slug, "cycle": cfg.cycle, "profile": "deep_otm_5m"}],
    )

    rc = main(["research", "list-bundles", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["cycle"] == "5m"
    assert payload[0]["profile"] == "deep_otm_5m"


def test_pm5min_research_show_active_bundle_stays_local_and_does_not_delegate_to_pm15min_handlers(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.research.cli_handlers.run_research_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pm5min show-active-bundle delegated to pm15min cli handlers")),
    )
    monkeypatch.setattr(
        "pm5min.research.service.get_active_bundle_selection",
        lambda cfg, **kwargs: {"market": cfg.asset.slug, "cycle": cfg.cycle, "profile": "deep_otm_5m", "target": "direction"},
    )

    rc = main(["research", "show-active-bundle", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["profile"] == "deep_otm_5m"


def test_pm5min_research_activate_bundle_stays_local_and_does_not_delegate_to_pm15min_handlers(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.research.cli_handlers.run_research_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pm5min activate-bundle delegated to pm15min cli handlers")),
    )
    monkeypatch.setattr(
        "pm5min.research.service.activate_model_bundle",
        lambda cfg, **kwargs: {"market": cfg.asset.slug, "cycle": cfg.cycle, "profile": kwargs["profile"], "target": kwargs["target"], "bundle_label": kwargs["bundle_label"]},
    )

    rc = main(["research", "activate-bundle", "--market", "sol", "--bundle-label", "demo"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["bundle_label"] == "demo"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k 'research_list_runs_stays_local or research_list_bundles_stays_local or research_show_active_bundle_stays_local or research_activate_bundle_stays_local'`

Expected: FAIL because `pm5min.research.cli` still routes unmatched commands through compat directly and does not have its own local handler module.

- [ ] **Step 3: Write the minimal implementation**

```python
# src/pm5min/research/handlers.py
from __future__ import annotations

import argparse
import json

from .compat import build_pm15min_research_deps, run_pm15min_research_command
from .config import ResearchConfig
from .service import (
    activate_model_bundle,
    describe_research_runtime,
    get_active_bundle_selection,
    list_model_bundles,
    list_training_runs,
)


def _print_payload(payload: object, *, sort_keys: bool = True) -> int:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=sort_keys))
    return 0


def _build_config(args: argparse.Namespace) -> ResearchConfig:
    return ResearchConfig.build(
        market=args.market,
        cycle=getattr(args, "cycle", "5m"),
        profile=getattr(args, "profile", "default"),
        source_surface=getattr(args, "source_surface", "backtest"),
        feature_set=getattr(args, "feature_set", "deep_otm_v1"),
        label_set=getattr(args, "label_set", "truth"),
        target=getattr(args, "target", "direction"),
        model_family=getattr(args, "model_family", "deep_otm"),
        run_prefix=getattr(args, "run_prefix", None),
    )


def run_research_command(args: argparse.Namespace) -> int:
    if args.research_command == "show-config":
        return _print_payload(describe_research_runtime(_build_config(args)))
    if args.research_command == "show-layout":
        return _print_payload(_build_config(args).layout.to_dict())
    if args.research_command == "list-runs":
        return _print_payload(list_training_runs(_build_config(args), model_family=args.model_family, target=args.target, prefix=args.prefix), sort_keys=False)
    if args.research_command == "list-bundles":
        return _print_payload(list_model_bundles(_build_config(args), profile=args.profile, target=args.target, prefix=args.prefix), sort_keys=False)
    if args.research_command == "show-active-bundle":
        return _print_payload(get_active_bundle_selection(_build_config(args), profile=args.profile, target=args.target))
    if args.research_command == "activate-bundle":
        return _print_payload(activate_model_bundle(_build_config(args), profile=args.profile, target=args.target, bundle_label=args.bundle_label, notes=args.notes))

    deps = build_pm15min_research_deps(
        research_config_type=ResearchConfig,
        describe_runtime_fn=describe_research_runtime,
    )
    return run_pm15min_research_command(args, deps=deps)
```

```python
# src/pm5min/research/cli.py
from __future__ import annotations

import argparse

from .handlers import run_research_command as _run_research_command_impl
from .parser import attach_research_subcommands as _attach_research_subcommands_impl


def attach_research_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    _attach_research_subcommands_impl(subparsers)


def run_research_command(args: argparse.Namespace) -> int:
    return _run_research_command_impl(args)
```

```python
# src/pm5min/research/compat.py
from __future__ import annotations

import argparse
import importlib
from typing import Any


def _load_research_cli_module() -> object:
    return importlib.import_module("pm15min.research.cli")


def _load_research_cli_handlers_module() -> object:
    return importlib.import_module("pm15min.research.cli_handlers")


def run_pm15min_research_command(args: argparse.Namespace, *, deps: object) -> int:
    module = _load_research_cli_handlers_module()
    return getattr(module, "run_research_command")(args, deps=deps)


def build_pm15min_research_deps(*, research_config_type: type, describe_runtime_fn) -> object:
    module = _load_research_cli_module()
    base_deps = getattr(module, "_build_cli_deps")()
    deps_type = type(base_deps)
    payload: dict[str, Any] = dict(base_deps.__dict__)
    payload["ResearchConfig"] = research_config_type
    payload["describe_research_runtime"] = describe_runtime_fn
    return deps_type(**payload)
```

Implementation notes:
- `compat.py` must no longer define `_load_research_service_module`.
- `handlers.py` is the only place allowed to choose between local lightweight commands and deep compat fallback.
- `cli.py` should stop carrying business logic.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k 'research_list_runs_stays_local or research_list_bundles_stays_local or research_show_active_bundle_stays_local or research_activate_bundle_stays_local or pm5min_research_list_runs_uses_5m_defaults or pm5min_research_list_bundles_uses_5m_defaults or pm5min_research_show_active_bundle_uses_5m_profile_and_cycle or pm5min_research_activate_bundle_uses_5m_profile_and_cycle'`

Expected: PASS

- [ ] **Step 5: Run smoke verification**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_research_service.py tests/test_pm5min_cli.py -k 'research'`

Expected: PASS

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k 'pm5min_live_show_layout_uses_5m_profile_and_cycle or pm5min_live_show_config_uses_pm5min_layout_root'`

Expected: PASS

Run: `PYTHONPATH=src pytest -q tests/test_pmshared_architecture.py tests/test_architecture_guards.py`

Expected: PASS

Run: `PYTHONPATH=src pytest -q tests/test_cli.py -k 'research_list_runs_is_json or research_show_layout'`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pm5min/research/handlers.py src/pm5min/research/cli.py src/pm5min/research/compat.py tests/test_pm5min_cli.py tests/test_pm5min_research_service.py
git commit -m "feat: split pm5min research control plane"
```

## Follow-Up After This Plan

- The remaining compat boundary in `pm5min/research` should be explicit and narrow:
  - deep build/train/bundle/backtest/experiment/evaluate execution only
- Write the next plan separately for:
  - local research execution builders, or
  - `pm5min/console` control-plane split, depending on priority after this phase
