from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading

from pm15min.data.io.json_files import write_json_atomic
from pm15min.data.pipelines.orderbook_recording import _write_json_atomic_compact


def test_write_json_atomic_handles_concurrent_writers(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "state.json"
    original_replace = Path.replace
    barrier = threading.Barrier(2)

    def _replace(self: Path, target_path: Path) -> Path:
        if self.name.startswith("state.json.") and self.name.endswith(".tmp"):
            barrier.wait(timeout=5)
        return original_replace(self, target_path)

    monkeypatch.setattr(Path, "replace", _replace)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(write_json_atomic, {"writer": idx}, target)
            for idx in range(2)
        ]
        for future in futures:
            future.result()

    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["writer"] in {0, 1}


def test_write_json_atomic_compact_handles_concurrent_writers(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "latest_full_depth.json"
    original_replace = Path.replace
    barrier = threading.Barrier(2)

    def _replace(self: Path, target_path: Path) -> Path:
        if self.name.startswith("latest_full_depth.json.") and self.name.endswith(".tmp"):
            barrier.wait(timeout=5)
        return original_replace(self, target_path)

    monkeypatch.setattr(Path, "replace", _replace)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(_write_json_atomic_compact, {"writer": idx}, target)
            for idx in range(2)
        ]
        for future in futures:
            future.result()

    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["writer"] in {0, 1}
