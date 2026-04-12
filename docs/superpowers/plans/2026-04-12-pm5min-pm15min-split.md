# PM5Min / PM15Min Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `pm15min` and `pm5min` into two fully independent applications, with only a thin `pmshared` layer for cycle-neutral primitives.

**Architecture:** Deliver the split in three waves. Wave 1 creates the new shared floor and independent top-level entrypoints. Wave 2 moves cycle-bearing `data / research / live` ownership into package-local modules. Wave 3 deletes 5-minute residue from `pm15min` and hardens the new package boundary with import guards and regression tests.

**Tech Stack:** Python 3.11+, argparse CLI, existing `pytest` suite, package-local domain modules under `src/pm15min` and `src/pm5min`, new `src/pmshared`.

---

## Execution Prerequisites

- Execute from a new branch created off current `main`, not from the earlier 5m experiment line.
- Execute in an isolated worktree so the current dirty main workspace is untouched.
- Keep commits wave-local; do not combine Wave 1, 2, and 3 into one commit.
- After every task, run both the task-local test and the 15m baseline smoke set:
  - `PYTHONPATH=src pytest -q tests/test_cli.py -k 'top_level_layout_command or live_show_config'`
  - `PYTHONPATH=src pytest -q tests/test_live_service.py -k 'prewarm_live_signal_inputs or score_live_latest'`

## File Structure

- Create: `src/pmshared/__init__.py`
  - Root package for cycle-neutral shared primitives only.
- Create: `src/pmshared/assets.py`
  - Shared asset resolution and `AssetSpec` definitions used by both apps.
- Create: `src/pmshared/time.py`
  - Shared cycle/surface normalization, snapshot-label helpers, and basic UTC helpers.
- Create: `src/pmshared/io/__init__.py`
  - Shared IO namespace.
- Create: `src/pmshared/io/json_files.py`
  - Shared JSON / JSONL read-write primitives.
- Create: `src/pmshared/io/parquet.py`
  - Shared parquet read-write primitives.
- Create: `src/pm5min/core/`
  - Package-local 5m config and layout ownership.
- Create: `src/pm5min/data/`
  - Package-local 5m data config, CLI, and pipelines entry ownership.
- Create: `src/pm5min/research/`
  - Package-local 5m research config, bundles, service, and CLI ownership.
- Create: `src/pm5min/live/`
  - Package-local 5m live config, profiles, signal, service, and CLI ownership.
- Modify: `src/pm5min/cli.py`
  - Replace the current `pm15min.cli` delegation with package-local domain loading.
- Modify: `src/pm5min/__main__.py`
  - Ensure `python -m pm5min` enters the new package-local CLI.
- Modify: `src/pm15min/cli.py`
  - Remove all 5m compatibility assumptions and keep it 15m-only.
- Modify: `src/pm15min/core/config.py`
  - Leave only 15m-facing config semantics and shared-base imports.
- Modify: `src/pm15min/data/config.py`
  - Leave only 15m-facing package ownership and shared-base imports.
- Modify: `src/pm15min/research/config.py`
  - Leave only 15m-facing package ownership and shared-base imports.
- Modify: `src/pm15min/live/profiles/catalog.py`
  - Remove `deep_otm_5m` and `deep_otm_5m_baseline`.
- Modify: `src/pm15min/live/service/__init__.py`
  - Remove research/live wiring that exists only to support 5m split personalities.
- Modify: `tests/test_pm5min_cli.py`
  - Stop testing `pm5min` as a thin wrapper; test it as an independent app.
- Modify: `tests/test_cli.py`
  - Keep `pm15min` locked to 15m-only behavior.
- Modify: `tests/test_architecture_guards.py`
  - Add package-boundary guards: `pm15min` cannot import `pm5min`, `pm5min` cannot import `pm15min`, both may import `pmshared`.
- Create: `tests/test_pmshared_architecture.py`
  - Ensure `pmshared` stays cycle-neutral and never imports `pm15min` or `pm5min`.
- Create: `tests/test_pm15min_only_profiles.py`
  - Lock `pm15min` to 15m profiles only.
- Create: `tests/test_pm5min_only_profiles.py`
  - Lock `pm5min` to 5m profiles only.

## Wave 1: Split The Floor And Entry Shells

### Task 1: Create The Shared Floor And Boundary Guards

**Files:**
- Create: `src/pmshared/__init__.py`
- Create: `src/pmshared/assets.py`
- Create: `src/pmshared/time.py`
- Create: `src/pmshared/io/__init__.py`
- Create: `src/pmshared/io/json_files.py`
- Create: `src/pmshared/io/parquet.py`
- Create: `tests/test_pmshared_architecture.py`
- Modify: `tests/test_architecture_guards.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pmshared_architecture.py
from pathlib import Path


def test_pmshared_does_not_import_application_packages() -> None:
    repo = Path(__file__).resolve().parents[1] / "src" / "pmshared"
    offending: list[str] = []
    for path in repo.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "from pm15min" in text or "import pm15min" in text:
            offending.append(str(path))
        if "from pm5min" in text or "import pm5min" in text:
            offending.append(str(path))
    assert offending == []


# tests/test_architecture_guards.py
def test_pm15min_and_pm5min_do_not_import_each_other() -> None:
    repo = Path(__file__).resolve().parents[1] / "src"
    offending: list[str] = []
    for root_name, forbidden in (("pm15min", "pm5min"), ("pm5min", "pm15min")):
        for path in (repo / root_name).rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            import_lines = "\n".join(
                line for line in text.splitlines() if line.lstrip().startswith(("import ", "from "))
            )
            if f"from {forbidden}" in import_lines or f"import {forbidden}" in import_lines:
                offending.append(str(path))
    assert offending == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pmshared_architecture.py tests/test_architecture_guards.py -k 'pmshared or import_each_other'`
Expected: FAIL because `src/pmshared/` does not exist and `src/pm5min/cli.py` still imports `pm15min.cli`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pmshared/__init__.py
"""Cycle-neutral shared primitives for pm15min and pm5min."""


# src/pmshared/time.py
from __future__ import annotations

from datetime import datetime, timezone

_CYCLE_ALIASES = {"5": "5m", "5m": "5m", "15": "15m", "15m": "15m", 5: "5m", 15: "15m"}
_SURFACE_ALIASES = {"live": "live", "backtest": "backtest"}


def normalize_cycle(cycle: str | int) -> str:
    key = cycle if isinstance(cycle, int) else str(cycle).strip().lower()
    return _CYCLE_ALIASES[key]


def normalize_surface(surface: str) -> str:
    return _SURFACE_ALIASES[str(surface or "backtest").strip().lower()]


def utc_snapshot_label(now: datetime | None = None) -> str:
    ts = datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H-%M-%SZ")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pmshared_architecture.py tests/test_architecture_guards.py -k 'pmshared or import_each_other'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pmshared tests/test_pmshared_architecture.py tests/test_architecture_guards.py
git commit -m "feat: add shared floor and split-package import guards"
```

### Task 2: Rebuild The Top-Level CLIs So They Stop Delegating Across Packages

**Files:**
- Modify: `src/pm15min/cli.py`
- Modify: `src/pm15min/__main__.py`
- Modify: `src/pm5min/cli.py`
- Modify: `src/pm5min/__main__.py`
- Modify: `tests/test_pm5min_cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pm5min_cli.py
from pathlib import Path


def test_pm5min_cli_does_not_delegate_to_pm15min_cli() -> None:
    text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "cli.py").read_text(encoding="utf-8")
    assert "from pm15min.cli import main as pm15min_main" not in text


# tests/test_cli.py
def test_pm15min_layout_defaults_to_15m(capsys, monkeypatch, tmp_path) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    rc = main(["layout", "--market", "sol", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "15m"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py tests/test_cli.py -k 'does_not_delegate_to_pm15min_cli or layout_defaults_to_15m'`
Expected: FAIL because `pm5min.cli` still imports `pm15min.cli`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm5min/cli.py
from __future__ import annotations

import argparse
import importlib
import json
import sys

from pm5min.data.layout import DataLayout

_DOMAIN_LOADERS = {
    "console": ("pm5min.console.cli", "attach_console_subcommands", "run_console_command"),
    "data": ("pm5min.data.cli", "attach_data_subcommands", "run_data_command"),
    "live": ("pm5min.live.cli", "attach_live_subcommands", "run_live_command"),
    "research": ("pm5min.research.cli", "attach_research_subcommands", "run_research_command"),
}


def _load_domain_cli(domain: str) -> tuple[object, object]:
    module_name, attach_name, run_name = _DOMAIN_LOADERS[domain]
    module = importlib.import_module(module_name)
    return getattr(module, attach_name), getattr(module, run_name)


def _requested_domain(argv: list[str]) -> str | None:
    for token in argv:
        if token.startswith("-"):
            continue
        if token in _DOMAIN_LOADERS or token == "layout":
            return token
        break
    return None


def build_parser(requested_domain: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m pm5min", description="pm5min CLI")
    subparsers = parser.add_subparsers(dest="domain")
    layout_parser = subparsers.add_parser("layout", help="Show the canonical 5m market layout.")
    layout_parser.add_argument("--market", default="btc")
    layout_parser.add_argument("--cycle", default="5m")
    layout_parser.add_argument("--json", action="store_true")
    for domain in ("live", "research", "data", "console"):
        if requested_domain == domain:
            attach_subcommands, _ = _load_domain_cli(domain)
            attach_subcommands(subparsers)
        else:
            subparsers.add_parser(domain)
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser(_requested_domain(argv))
    args = parser.parse_args(argv)
    if args.domain == "layout":
        payload = DataLayout.discover().for_market(args.market, args.cycle).to_dict()
        if args.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
        else:
            for key, value in payload.items():
                print(f"{key}: {value}")
        return 0
    if args.domain == "live":
        _, run_live_command = _load_domain_cli("live")
        return run_live_command(args)
    if args.domain == "research":
        _, run_research_command = _load_domain_cli("research")
        return run_research_command(args)
    if args.domain == "data":
        _, run_data_command = _load_domain_cli("data")
        return run_data_command(args)
    if args.domain == "console":
        _, run_console_command = _load_domain_cli("console")
        return run_console_command(args)
    parser.print_help()
    return 0
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py tests/test_cli.py -k 'does_not_delegate_to_pm15min_cli or layout_defaults_to_15m or pm5min_layout_command'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pm15min/cli.py src/pm15min/__main__.py src/pm5min/cli.py src/pm5min/__main__.py tests/test_pm5min_cli.py tests/test_cli.py
git commit -m "feat: split pm15min and pm5min top-level clis"
```

## Wave 2: Move Cycle-Bearing Ownership Into Package-Local Modules

### Task 3: Split Shared Config Shapes Away From Package-Specific Config Owners

**Files:**
- Create: `src/pmshared/config.py`
- Modify: `src/pm15min/core/config.py`
- Create: `src/pm5min/core/config.py`
- Modify: `src/pm15min/data/config.py`
- Create: `src/pm5min/data/config.py`
- Modify: `src/pm15min/research/config.py`
- Create: `src/pm5min/research/config.py`
- Create: `tests/test_pm15min_only_profiles.py`
- Create: `tests/test_pm5min_only_profiles.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pm15min_only_profiles.py
import pytest

from pm15min.live.profiles import resolve_live_profile_spec


def test_pm15min_rejects_5m_profiles() -> None:
    with pytest.raises(KeyError):
        resolve_live_profile_spec("deep_otm_5m")


# tests/test_pm5min_only_profiles.py
import pytest

from pm5min.live.profiles import resolve_live_profile_spec


def test_pm5min_rejects_15m_profiles() -> None:
    with pytest.raises(KeyError):
        resolve_live_profile_spec("deep_otm")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm15min_only_profiles.py tests/test_pm5min_only_profiles.py`
Expected: FAIL because `pm5min.live.profiles` does not exist and `pm15min` still exposes 5m profiles.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pmshared/config.py
from __future__ import annotations

from dataclasses import dataclass

from pmshared.assets import AssetSpec


@dataclass(frozen=True)
class BaseConfig:
    asset: AssetSpec
    cycle: str


# src/pm5min/core/config.py
from __future__ import annotations

from dataclasses import dataclass

from pmshared.config import BaseConfig


@dataclass(frozen=True)
class LiveConfig(BaseConfig):
    profile: str = "deep_otm_5m"
    cycle: str = "5m"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm15min_only_profiles.py tests/test_pm5min_only_profiles.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pmshared/config.py src/pm15min/core/config.py src/pm5min/core/config.py src/pm15min/data/config.py src/pm5min/data/config.py src/pm15min/research/config.py src/pm5min/research/config.py tests/test_pm15min_only_profiles.py tests/test_pm5min_only_profiles.py
git commit -m "feat: split package-local config ownership"
```

### Task 4: Split Research Ownership So PM5Min Stops Using PM15Min Research Modules

**Files:**
- Create: `src/pm5min/research/__init__.py`
- Create: `src/pm5min/research/cli.py`
- Create: `src/pm5min/research/bundles/__init__.py`
- Create: `src/pm5min/research/bundles/active_registry.py`
- Create: `src/pm5min/research/bundles/loader.py`
- Create: `src/pm5min/research/service.py`
- Modify: `src/pm15min/research/service.py`
- Modify: `tests/test_pm5min_cli.py`
- Modify: `tests/test_architecture_guards.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_pm5min_research_cli_loads_package_local_module() -> None:
    text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "cli.py").read_text(encoding="utf-8")
    assert '"pm5min.research.cli"' in text
    assert '"pm15min.research.cli"' not in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k package_local_module`
Expected: FAIL because the current `pm5min` package does not own `pm5min.research.cli`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm5min/research/service.py
from __future__ import annotations

from pm5min.research.bundles.active_registry import read_active_bundle_selection
from pm5min.research.bundles.loader import read_bundle_config, read_model_bundle_manifest, resolve_model_bundle_dir
from pm5min.research.config import ResearchConfig


def get_active_bundle_selection(
    cfg: ResearchConfig,
    *,
    profile: str,
    target: str,
) -> dict[str, object]:
    return read_active_bundle_selection(
        cfg.layout,
        profile=profile,
        target=target,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py -k package_local_module`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pm5min/research src/pm15min/research/service.py tests/test_pm5min_cli.py tests/test_architecture_guards.py
git commit -m "feat: split pm5min research ownership"
```

### Task 5: Split Live Ownership So PM5Min Stops Using PM15Min Live Modules

**Files:**
- Create: `src/pm5min/live/__init__.py`
- Create: `src/pm5min/live/cli/__init__.py`
- Create: `src/pm5min/live/profiles/__init__.py`
- Create: `src/pm5min/live/profiles/catalog.py`
- Create: `src/pm5min/live/service/__init__.py`
- Create: `src/pm5min/live/signal/service.py`
- Modify: `src/pm15min/live/profiles/catalog.py`
- Modify: `src/pm15min/live/service/__init__.py`
- Modify: `tests/test_pm5min_cli.py`
- Modify: `tests/test_cli.py`
- Modify: `tests/test_live_service.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_pm5min_live_show_layout_uses_pm5min_profile_catalog(capsys) -> None:
    from pm5min.cli import main as pm5min_main

    rc = pm5min_main(["live", "show-layout", "--market", "sol"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["profile"] == "deep_otm_5m"


def test_pm15min_live_show_config_rejects_5m_profile(capsys) -> None:
    rc = main(["live", "show-config", "--market", "sol", "--profile", "deep_otm_5m"])
    assert rc == 2
    assert "unknown profile" in capsys.readouterr().err.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py tests/test_cli.py -k 'pm5min_profile_catalog or rejects_5m_profile'`
Expected: FAIL because `pm15min` still knows 5m profiles and `pm5min` still leans on `pm15min` live ownership.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm5min/live/profiles/catalog.py
from __future__ import annotations

from pm5min.live.profiles.spec import LiveProfileSpec

DEEP_OTM_5M_LIVE_PROFILE_SPEC = LiveProfileSpec(
    **{
        "profile": "deep_otm_5m",
        "cycle": "5m",
        "target": "direction",
        "default_feature_set": "v6_user_core",
        "active_markets": ("btc", "eth", "sol", "xrp"),
        "offsets": (4, 5, 6, 7, 8, 9),
        "entry_price_min": 0.01,
        "entry_price_max": 0.30,
        "min_dir_prob_default": 0.60,
    }
)


# src/pm15min/live/profiles/catalog.py
LIVE_PROFILE_SPECS = {
    "default": DEFAULT_LIVE_PROFILE_SPEC,
    "deep_otm": DEEP_OTM_LIVE_PROFILE_SPEC,
    "deep_otm_baseline": DEEP_OTM_BASELINE_LIVE_PROFILE_SPEC,
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_pm5min_cli.py tests/test_cli.py -k 'pm5min_profile_catalog or rejects_5m_profile or pm5min_live_show_layout_uses_5m_profile_and_cycle'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pm5min/live src/pm15min/live/profiles/catalog.py src/pm15min/live/service/__init__.py tests/test_pm5min_cli.py tests/test_cli.py tests/test_live_service.py
git commit -m "feat: split pm5min live ownership"
```

## Wave 3: Remove Residue And Freeze The Boundary

### Task 6: Delete Remaining 5m Branches From PM15Min

**Files:**
- Modify: `src/pm15min/core/cycle_contracts.py`
- Modify: `src/pm15min/live/runtime.py`
- Modify: `src/pm15min/research/features/builders.py`
- Modify: `src/pm15min/live/signal/decision.py`
- Modify: `src/pm15min/live/signal/scoring.py`
- Modify: `src/pm15min/live/signal/scoring_bundle.py`
- Modify: `src/pm15min/live/signal/scoring_offsets.py`
- Modify: `src/pm15min/live/signal/service.py`
- Modify: `tests/test_cli.py`
- Modify: `tests/test_live_service.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_pm15min_runtime_is_canonical_15m_only() -> None:
    from pm15min.live.runtime import CANONICAL_LIVE_CYCLE

    assert CANONICAL_LIVE_CYCLE == "15m"


def test_pm15min_cli_has_no_pm5min_entrypoints() -> None:
    text = (Path(__file__).resolve().parents[1] / "src" / "pm15min" / "cli.py").read_text(encoding="utf-8")
    assert "deep_otm_5m" not in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_cli.py -k 'canonical_15m_only or no_pm5min_entrypoints'`
Expected: FAIL until all leftover 5m residue is removed from `pm15min`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/pm15min/live/runtime.py
CANONICAL_LIVE_CYCLE = "15m"
CANONICAL_LIVE_PROFILE = "deep_otm"


# src/pm15min/core/cycle_contracts.py
def resolve_cycle_contract(cycle: str | int) -> CycleContract:
    normalized = normalize_cycle(cycle)
    if normalized != "15m":
        raise ValueError(f"pm15min only supports 15m, got {cycle!r}")
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

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_cli.py -k 'canonical_15m_only or no_pm5min_entrypoints'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pm15min/core/cycle_contracts.py src/pm15min/live/runtime.py src/pm15min/research/features/builders.py src/pm15min/live/signal tests/test_cli.py tests/test_live_service.py
git commit -m "refactor: remove 5m residue from pm15min"
```

### Task 7: Harden The Final Architecture Guard And Finish The Docs

**Files:**
- Modify: `tests/test_architecture_guards.py`
- Modify: `docs/README.md`
- Modify: `docs/LIVE_TECHNICAL_PLAN.md`
- Modify: `docs/DATA_TECHNICAL_PLAN.md`
- Modify: `docs/superpowers/specs/2026-04-12-pm5min-pm15min-split-design.md`

- [ ] **Step 1: Write the failing tests**

```python
def test_pm5min_and_pm15min_may_only_share_through_pmshared() -> None:
    repo = Path(__file__).resolve().parents[1] / "src"
    offending: list[str] = []
    for root_name in ("pm15min", "pm5min"):
        for path in (repo / root_name).rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            import_lines = "\n".join(
                line for line in text.splitlines() if line.lstrip().startswith(("import ", "from "))
            )
            if "from pmshared" in import_lines or "import pmshared" in import_lines:
                continue
            other = "pm5min" if root_name == "pm15min" else "pm15min"
            if f"from {other}" in import_lines or f"import {other}" in import_lines:
                offending.append(str(path))
    assert offending == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_architecture_guards.py -k may_only_share_through_pmshared`
Expected: FAIL until every cross-package import is removed.

- [ ] **Step 3: Write minimal implementation**

```markdown
# docs/README.md
- `pm15min` is the dedicated 15-minute application.
- `pm5min` is the dedicated 5-minute application.
- `pmshared` contains only cycle-neutral primitives and may be imported by both apps.
```

- [ ] **Step 4: Run the final verification suite**

Run: `PYTHONPATH=src pytest -q tests/test_architecture_guards.py tests/test_pmshared_architecture.py tests/test_pm5min_cli.py tests/test_cli.py tests/test_live_service.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_architecture_guards.py docs/README.md docs/LIVE_TECHNICAL_PLAN.md docs/DATA_TECHNICAL_PLAN.md docs/superpowers/specs/2026-04-12-pm5min-pm15min-split-design.md
git commit -m "docs: finalize split-app boundary guidance"
```

## Final Verification

- Run: `PYTHONPATH=src pytest -q tests/test_architecture_guards.py tests/test_pmshared_architecture.py`
  - Expected: PASS
- Run: `PYTHONPATH=src pytest -q tests/test_cli.py tests/test_pm5min_cli.py`
  - Expected: PASS
- Run: `PYTHONPATH=src pytest -q tests/test_live_service.py tests/test_live_regime.py tests/test_live_actions.py`
  - Expected: PASS
- Run: `PYTHONPATH=src pytest -q tests/test_research_feature_builders.py tests/test_research_backtest_regime_parity.py tests/test_research_backtest_live_state_parity.py`
  - Expected: PASS

## Spec Coverage Check

- Split package ownership: covered by Tasks 1 through 5.
- Thin shared floor only: covered by Tasks 1 and 7.
- `pm5min` no longer delegates to `pm15min`: covered by Task 2.
- `pm15min` no longer carries 5m residue: covered by Task 6.
- Import boundary hardening: covered by Tasks 1 and 7.
