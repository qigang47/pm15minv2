from __future__ import annotations

import json
import os
from pathlib import Path
import time
from typing import Any


def atomic_json_tmp_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")


def write_json_atomic(payload: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = atomic_json_tmp_path(path)
    tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)
    return path


def append_jsonl(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return path
