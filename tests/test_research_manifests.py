from __future__ import annotations

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
