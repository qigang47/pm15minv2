from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SharedSurfaceKey:
    market: str
    cycle: str
    source_surface: str
    feature_set: str
    label_set: str
    profile: str
    target: str
    decision_start: str
    decision_end: str
    offsets: tuple[int, ...]
    orderbook_mode: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["offsets"] = list(self.offsets)
        return payload


@dataclass(frozen=True)
class SharedSurfaceManifest:
    key: SharedSurfaceKey
    root: Path
    created_at: str
    source_mtimes: tuple[tuple[str, int | None], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key.to_dict(),
            "root": str(self.root),
            "created_at": self.created_at,
            "source_mtimes": [[path, mtime] for path, mtime in self.source_mtimes],
        }


def shared_surface_key_hash(key: SharedSurfaceKey) -> str:
    payload = json.dumps(key.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def shared_surface_dir(*, root: Path, key: SharedSurfaceKey) -> Path:
    return Path(root) / "var" / "research" / "cache" / "quick_screen_surfaces" / shared_surface_key_hash(key)


def shared_surface_manifest_path(*, root: Path, key: SharedSurfaceKey) -> Path:
    return shared_surface_dir(root=root, key=key) / "manifest.json"


def write_shared_surface_manifest(
    *,
    root: Path,
    key: SharedSurfaceKey,
    source_mtimes: tuple[tuple[str, int | None], ...],
) -> SharedSurfaceManifest:
    manifest = SharedSurfaceManifest(
        key=key,
        root=shared_surface_dir(root=root, key=key),
        created_at=datetime.now(timezone.utc).isoformat(),
        source_mtimes=tuple(source_mtimes),
    )
    manifest.root.mkdir(parents=True, exist_ok=True)
    (manifest.root / "manifest.json").write_text(
        json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return manifest


def read_shared_surface_manifest(path: Path) -> SharedSurfaceManifest | None:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    key_payload = payload.get("key")
    if not isinstance(key_payload, dict):
        return None
    try:
        key = SharedSurfaceKey(
            market=str(key_payload.get("market") or ""),
            cycle=str(key_payload.get("cycle") or ""),
            source_surface=str(key_payload.get("source_surface") or ""),
            feature_set=str(key_payload.get("feature_set") or ""),
            label_set=str(key_payload.get("label_set") or ""),
            profile=str(key_payload.get("profile") or ""),
            target=str(key_payload.get("target") or ""),
            decision_start=str(key_payload.get("decision_start") or ""),
            decision_end=str(key_payload.get("decision_end") or ""),
            offsets=tuple(int(value) for value in (key_payload.get("offsets") or [])),
            orderbook_mode=str(key_payload.get("orderbook_mode") or ""),
        )
    except Exception:
        return None
    raw_mtimes = payload.get("source_mtimes") or []
    source_mtimes: list[tuple[str, int | None]] = []
    for item in raw_mtimes:
        if not isinstance(item, list | tuple) or len(item) != 2:
            continue
        source_mtimes.append((str(item[0]), None if item[1] is None else int(item[1])))
    return SharedSurfaceManifest(
        key=key,
        root=Path(str(payload.get("root") or Path(path).parent)),
        created_at=str(payload.get("created_at") or ""),
        source_mtimes=tuple(source_mtimes),
    )


def shared_surface_manifest_is_current(manifest: SharedSurfaceManifest | Path) -> bool:
    loaded = read_shared_surface_manifest(manifest) if isinstance(manifest, Path) else manifest
    if loaded is None:
        return False
    return all(_path_mtime_ns(Path(raw_path)) == expected for raw_path, expected in loaded.source_mtimes)


def snapshot_source_mtimes(paths: list[Path] | tuple[Path, ...]) -> tuple[tuple[str, int | None], ...]:
    unique_paths = {str(path): path for path in paths if str(path)}
    return tuple(sorted((raw_path, _path_mtime_ns(path)) for raw_path, path in unique_paths.items()))


def _path_mtime_ns(path: Path) -> int | None:
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return None
    except Exception:
        return None


__all__ = [
    "SharedSurfaceKey",
    "SharedSurfaceManifest",
    "read_shared_surface_manifest",
    "shared_surface_dir",
    "shared_surface_key_hash",
    "shared_surface_manifest_is_current",
    "shared_surface_manifest_path",
    "snapshot_source_mtimes",
    "write_shared_surface_manifest",
]
