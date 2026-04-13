from __future__ import annotations

from pathlib import Path


def test_pmshared_does_not_import_application_packages() -> None:
    repo = Path(__file__).resolve().parents[1] / "src" / "pmshared"
    assert repo.exists()
    assert (repo / "__init__.py").exists()
    offending: list[str] = []
    for path in repo.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "from pm15min" in text or "import pm15min" in text:
            offending.append(str(path))
        if "from pm5min" in text or "import pm5min" in text:
            offending.append(str(path))
    assert offending == []
