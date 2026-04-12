from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pm15min.core.layout import MarketLayout

_HAS_LEGACY_APPS = importlib.util.find_spec("apps") is not None
if _HAS_LEGACY_APPS:
    from apps import __main__ as apps_main
    from apps.live import main as apps_live_main


@pytest.mark.skipif(not _HAS_LEGACY_APPS, reason="legacy apps shim not present in standalone rewrite repo")
def test_apps_live_shim_points_to_v2_cli(capsys) -> None:
    rc = apps_live_main(["run"])
    captured = capsys.readouterr()
    assert rc == 2
    assert "deprecated" in captured.err.lower()
    assert "PYTHONPATH=v2/src python -m pm15min live" in captured.err


@pytest.mark.skipif(not _HAS_LEGACY_APPS, reason="legacy apps shim not present in standalone rewrite repo")
def test_apps_root_help_points_to_v2_cli(capsys) -> None:
    rc = apps_main.main(["--help"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "deprecated" in captured.out.lower()
    assert "PYTHONPATH=v2/src python -m pm15min" in captured.out


def test_market_layout_splits_runtime_and_legacy_reference_paths() -> None:
    payload = MarketLayout.for_market("sol").to_dict()
    assert payload["market"] == "sol"
    assert "rewrite_live_data_root" in payload
    assert "rewrite_research_root" in payload
    assert "legacy_reference_market_root" in payload
    assert "legacy_reference_artifacts_root" in payload
    assert "market_root" not in payload
    assert "artifacts_root" not in payload


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


def test_v2_legacy_imports_are_limited_to_explicit_boundaries() -> None:
    repo = Path(__file__).resolve().parents[1] / "src" / "pm15min"
    allow_live_trading = {
        repo / "live" / "trading" / "legacy_adapter.py",
    }
    forbidden_tokens = {
        "live_trading": ("from live_trading", "import live_trading"),
        "scripts": ("from scripts.", "import scripts."),
        "src": ("from src.", "import src."),
        "poly_eval": ("from poly_eval", "import poly_eval"),
    }

    for path in repo.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        import_lines = "\n".join(
            line for line in text.splitlines() if line.lstrip().startswith(("import ", "from "))
        )
        for patterns in forbidden_tokens.values():
            if path in allow_live_trading and any("live_trading" in pattern for pattern in patterns):
                continue
            for pattern in patterns:
                assert pattern not in import_lines, f"{path} should not import legacy token {pattern!r}"


def test_data_domain_does_not_import_research_domain() -> None:
    repo = Path(__file__).resolve().parents[1] / "src" / "pm15min" / "data"
    offending: list[str] = []
    for path in repo.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        import_lines = "\n".join(
            line for line in text.splitlines() if line.lstrip().startswith(("import ", "from "))
        )
        if "from pm15min.research." in import_lines or "import pm15min.research." in import_lines:
            offending.append(str(path))
    assert offending == [], f"data domain should not import research domain directly: {offending}"


def test_research_retry_contract_does_not_import_live_retry_policy() -> None:
    path = Path(__file__).resolve().parents[1] / "src" / "pm15min" / "research" / "backtests" / "retry_contract.py"
    text = path.read_text(encoding="utf-8")
    import_lines = "\n".join(
        line for line in text.splitlines() if line.lstrip().startswith(("import ", "from "))
    )
    assert "from pm15min.live.execution.retry_policy" not in import_lines


def test_research_orderbook_surface_does_not_import_live_orderbook_module() -> None:
    path = Path(__file__).resolve().parents[1] / "src" / "pm15min" / "research" / "backtests" / "orderbook_surface.py"
    text = path.read_text(encoding="utf-8")
    import_lines = "\n".join(
        line for line in text.splitlines() if line.lstrip().startswith(("import ", "from "))
    )
    assert "from pm15min.live.quotes.orderbook" not in import_lines
