from __future__ import annotations

from pathlib import Path

import pytest

from pm15min.research.manifests import build_manifest, read_manifest, write_manifest


def test_write_and_read_research_manifest(tmp_path) -> None:
    manifest = build_manifest(
        object_type="training_run",
        object_id="training_run:deep_otm:direction:planned",
        market="sol",
        cycle="15m",
        path=tmp_path / "run=planned",
        spec={"target": "direction", "offsets": [7, 8, 9]},
        inputs=[{"path": "v2/research/training_sets/.../data.parquet"}],
        outputs=[{"path": "v2/research/training_runs/.../summary.json"}],
        metadata={"phase": "phase1"},
    )

    target = write_manifest(tmp_path / "manifest.json", manifest)
    loaded = read_manifest(target)

    assert loaded.object_type == "training_run"
    assert loaded.market == "sol"
    assert loaded.cycle == "15m"
    assert loaded.spec["target"] == "direction"
    assert loaded.metadata["phase"] == "phase1"


def test_write_manifest_preserves_existing_file_on_write_failure(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_manifest = build_manifest(
        object_type="training_run",
        object_id="training_run:deep_otm:direction:stable",
        market="sol",
        cycle="15m",
        path=tmp_path / "run=stable",
        spec={"target": "direction"},
    )
    target = write_manifest(tmp_path / "manifest.json", original_manifest)
    original_write_text = Path.write_text

    def _failing_write_text(self: Path, data: str, *args, **kwargs):
        if self.parent == target.parent and self.name.startswith(target.name):
            original_write_text(self, "{", *args, **kwargs)
            raise RuntimeError("simulated manifest write failure")
        return original_write_text(self, data, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", _failing_write_text)

    with pytest.raises(RuntimeError, match="simulated manifest write failure"):
        write_manifest(
            target,
            build_manifest(
                object_type="training_run",
                object_id="training_run:deep_otm:direction:candidate",
                market="sol",
                cycle="15m",
                path=tmp_path / "run=candidate",
                spec={"target": "direction"},
            ),
        )

    loaded = read_manifest(target)
    assert loaded.object_id == original_manifest.object_id
