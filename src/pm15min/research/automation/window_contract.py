from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

CANONICAL_TRAIN_END = "2026-04-15"
CANONICAL_DECISION_START = "2026-04-15"
CANONICAL_DECISION_END = "2026-05-07"


@dataclass(frozen=True)
class SuiteWindow:
    train_end: str | None
    decision_start: str | None
    decision_end: str | None

    @property
    def is_canonical(self) -> bool:
        return (
            self.train_end == CANONICAL_TRAIN_END
            and self.decision_start == CANONICAL_DECISION_START
            and self.decision_end == CANONICAL_DECISION_END
        )


def extract_suite_spec_window(spec_path: Path) -> SuiteWindow:
    payload = _read_json_object(spec_path)
    window = payload.get("window") if isinstance(payload.get("window"), dict) else {}
    train_end = str(window.get("end") or payload.get("train_end") or "").strip() or None
    decision_start = str(payload.get("decision_start") or "").strip() or None
    decision_end = str(payload.get("decision_end") or "").strip() or None
    return SuiteWindow(
        train_end=train_end,
        decision_start=decision_start,
        decision_end=decision_end,
    )


def suite_spec_uses_canonical_window(spec_path: Path) -> bool:
    try:
        return extract_suite_spec_window(spec_path).is_canonical
    except (OSError, ValueError, json.JSONDecodeError):
        return False


def audit_suite_spec_windows(project_root: Path) -> dict[str, Any]:
    specs_dir = Path(project_root).resolve() / "research" / "experiments" / "suite_specs"
    parseable_specs = 0
    canonical_specs = 0
    stale_specs: list[str] = []
    ignored_files: list[str] = []
    if not specs_dir.exists():
        return {
            "parseable_specs": 0,
            "canonical_specs": 0,
            "stale_specs": [],
            "ignored_files": [],
        }

    for path in sorted(specs_dir.glob("*.json")):
        if path.name.startswith("._"):
            ignored_files.append(path.name)
            continue
        try:
            window = extract_suite_spec_window(path)
        except (OSError, ValueError, json.JSONDecodeError):
            ignored_files.append(path.name)
            continue
        parseable_specs += 1
        if window.is_canonical:
            canonical_specs += 1
        else:
            stale_specs.append(path.name)
    return {
        "parseable_specs": parseable_specs,
        "canonical_specs": canonical_specs,
        "stale_specs": stale_specs,
        "ignored_files": ignored_files,
    }


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"suite spec must be a JSON object: {path}")
    return payload
