from __future__ import annotations

from pathlib import Path

from pm15min.core import layout as core_layout


def test_layout_roots_resolve_monorepo_structure(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    file_path = repo_root / "v2" / "src" / "pm15min" / "core" / "layout.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("", encoding="utf-8")
    (repo_root / "v2" / "src" / "pm15min").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(core_layout.Path, "resolve", lambda self: file_path)

    resolved = core_layout._resolve_layout_roots()

    assert resolved["workspace_root"] == repo_root
    assert resolved["rewrite_root"] == repo_root / "v2"


def test_layout_roots_resolve_standalone_v2_structure(tmp_path: Path, monkeypatch) -> None:
    v2_root = tmp_path / "standalone_v2"
    file_path = v2_root / "src" / "pm15min" / "core" / "layout.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("", encoding="utf-8")
    (v2_root / "src" / "pm15min").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(core_layout.Path, "resolve", lambda self: file_path)

    resolved = core_layout._resolve_layout_roots()

    assert resolved["workspace_root"] == v2_root
    assert resolved["rewrite_root"] == v2_root
