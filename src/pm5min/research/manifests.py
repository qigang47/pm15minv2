from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_SCHEMA_VERSION = "pm5min.research.v1"


def utc_manifest_timestamp(now: datetime | None = None) -> str:
    ts = datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_manifest_path(path: str | Path) -> Path:
    target = Path(path)
    if target.suffix.lower() == ".json":
        return target
    return target / "manifest.json"


@dataclass(frozen=True)
class ResearchManifest:
    object_type: str
    object_id: str
    market: str
    cycle: str
    path: str
    created_at: str
    spec: dict[str, Any] = field(default_factory=dict)
    inputs: list[dict[str, Any]] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: str = _SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_manifest(
    *,
    object_type: str,
    object_id: str,
    market: str,
    cycle: str,
    path: str | Path,
    spec: dict[str, Any] | None = None,
    inputs: list[dict[str, Any]] | None = None,
    outputs: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
    created_at: str | None = None,
) -> ResearchManifest:
    return ResearchManifest(
        object_type=str(object_type),
        object_id=str(object_id),
        market=str(market),
        cycle=str(cycle),
        path=str(path),
        created_at=str(created_at or utc_manifest_timestamp()),
        spec=dict(spec or {}),
        inputs=list(inputs or []),
        outputs=list(outputs or []),
        metadata=dict(metadata or {}),
    )


def write_manifest(path: str | Path, manifest: ResearchManifest | dict[str, Any]) -> Path:
    target = resolve_manifest_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = manifest.to_dict() if isinstance(manifest, ResearchManifest) else dict(manifest)
    target.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return target


def read_manifest(path: str | Path) -> ResearchManifest:
    payload = json.loads(resolve_manifest_path(path).read_text(encoding="utf-8"))
    return ResearchManifest(
        object_type=str(payload["object_type"]),
        object_id=str(payload["object_id"]),
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        path=str(payload["path"]),
        created_at=str(payload["created_at"]),
        spec=dict(payload.get("spec") or {}),
        inputs=list(payload.get("inputs") or []),
        outputs=list(payload.get("outputs") or []),
        metadata=dict(payload.get("metadata") or {}),
        schema_version=str(payload.get("schema_version") or _SCHEMA_VERSION),
    )
