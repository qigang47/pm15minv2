from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps import __main__ as apps_main
from apps.live import main as apps_live_main
from pm15min.core.layout import MarketLayout


def test_apps_live_shim_points_to_v2_cli(capsys) -> None:
    rc = apps_live_main(["run"])
    captured = capsys.readouterr()
    assert rc == 2
    assert "deprecated" in captured.err.lower()
    assert "PYTHONPATH=v2/src python -m pm15min live" in captured.err


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
