from __future__ import annotations

from pathlib import Path

from pm15min.research.backtests.shared_surfaces import (
    SharedSurfaceKey,
    shared_surface_dir,
    shared_surface_key_hash,
    shared_surface_manifest_is_current,
    snapshot_source_mtimes,
    write_shared_surface_manifest,
)


def _key(**overrides) -> SharedSurfaceKey:
    values = {
        "market": "sol",
        "cycle": "15m",
        "source_surface": "backtest",
        "feature_set": "focus",
        "label_set": "truth",
        "profile": "deep_otm",
        "target": "reversal",
        "decision_start": "2026-04-15",
        "decision_end": "2026-04-30",
        "offsets": (7, 8),
        "orderbook_mode": "full_depth",
    }
    values.update(overrides)
    return SharedSurfaceKey(**values)


def test_shared_surface_key_hash_ignores_run_label_but_keeps_window_and_feature_set() -> None:
    base = _key()

    assert shared_surface_key_hash(base) == shared_surface_key_hash(_key())
    assert shared_surface_key_hash(base) != shared_surface_key_hash(_key(feature_set="other"))
    assert shared_surface_key_hash(base) != shared_surface_key_hash(_key(decision_end="2026-05-07"))


def test_shared_surface_manifest_current_only_when_source_mtimes_match(tmp_path: Path) -> None:
    source = tmp_path / "feature.parquet"
    source.write_text("v1", encoding="utf-8")
    manifest = write_shared_surface_manifest(
        root=tmp_path,
        key=_key(),
        source_mtimes=snapshot_source_mtimes([source]),
    )

    assert shared_surface_manifest_is_current(manifest)

    source.write_text("v2", encoding="utf-8")

    assert not shared_surface_manifest_is_current(manifest)


def test_shared_surface_dir_lives_under_research_cache(tmp_path: Path) -> None:
    key = _key()
    path = shared_surface_dir(root=tmp_path, key=key)

    assert path.parent == tmp_path / "var" / "research" / "cache" / "quick_screen_surfaces"
    assert path.name == shared_surface_key_hash(key)
