from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_memory_report_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "monitoring" / "report_quick_screen_memory.py"
    spec = importlib.util.spec_from_file_location("quick_screen_memory_report_under_test", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_smaps_rollup_reads_pss_and_private_kb(tmp_path: Path) -> None:
    module = _load_memory_report_module()
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

    parsed = module.parse_smaps_rollup(smaps)

    assert parsed["rss_kb"] == 204800
    assert parsed["pss_kb"] == 102400
    assert parsed["private_kb"] == 152576
    assert parsed["shared_kb"] == 52224


def test_classify_process_splits_quick_screen_formal_and_orderbook() -> None:
    module = _load_memory_report_module()

    assert module.classify_process("python scripts/research/run_quick_screen_queue_batch.py") == "quick_screen"
    assert module.classify_process("python scripts/research/run_quick_screen_pool.py") == "quick_screen"
    assert module.classify_process("python -m pm15min research experiment run-suite baseline") == "formal"
    assert module.classify_process("python -m pm15min data record orderbook-depth --market sol") == "orderbook"
    assert module.classify_process("python unrelated.py") is None
