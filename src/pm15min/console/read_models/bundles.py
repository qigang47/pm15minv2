from __future__ import annotations

from pathlib import Path
from typing import Any

from pm15min.research.bundles.loader import (
    read_bundle_summary,
    read_model_bundle_manifest,
    resolve_model_bundle_dir,
)
from pm15min.research.config import ResearchConfig
from pm15min.research.labels.sources import normalize_label_set
from pm15min.research.manifests import read_manifest
from pm15min.research.service import (
    get_active_bundle_selection as _get_active_bundle_selection,
    list_model_bundles as _list_model_bundles,
)

from .common import json_ready, read_json_object


def list_console_model_bundles(
    *,
    market: str,
    cycle: str | int = "15m",
    profile: str | None = None,
    target: str | None = None,
    prefix: str | None = None,
    root: Path | None = None,
) -> list[dict[str, object]]:
    cfg = _build_cfg(
        market=market,
        cycle=cycle,
        profile=profile,
        target=target,
        root=root,
    )
    active_map = _active_bundle_lookup(cfg)
    rows = _list_model_bundles(
        cfg,
        profile=profile,
        target=target,
        prefix=prefix,
    )
    payload = []
    for row in rows:
        bundle_dir = Path(str(row["path"]))
        item = _build_bundle_row(bundle_dir)
        bundle_key = (
            str(item.get("profile") or ""),
            str(item.get("target") or ""),
        )
        active = active_map.get(bundle_key)
        item["is_active"] = bool(active is not None and str(active.get("bundle_dir") or "") == str(bundle_dir))
        item["active_selection"] = active
        payload.append(item)
    return json_ready(payload)


def load_console_model_bundle(
    *,
    market: str,
    cycle: str | int = "15m",
    profile: str,
    target: str,
    bundle_label: str | None = None,
    bundle_dir: str | Path | None = None,
    root: Path | None = None,
) -> dict[str, object]:
    cfg = _build_cfg(
        market=market,
        cycle=cycle,
        profile=profile,
        target=target,
        root=root,
    )
    resolved_bundle_dir = _resolve_bundle_dir(
        cfg,
        profile=profile,
        target=target,
        bundle_label=bundle_label,
        bundle_dir=bundle_dir,
    )
    row = _build_bundle_row(resolved_bundle_dir)
    active_selection = _get_active_bundle_selection(cfg, profile=profile, target=target)
    payload: dict[str, object] = {
        "domain": "console",
        "dataset": "console_model_bundle",
        **row,
        "report_text": _read_text_file(resolved_bundle_dir / "report.md"),
        "active_selection": active_selection,
        "is_active": bool(
            str(((active_selection.get("selection") or {}).get("bundle_dir") if isinstance(active_selection, dict) else "") or "")
            == str(resolved_bundle_dir)
        ),
        "offset_details": _build_offset_details(resolved_bundle_dir),
    }
    return json_ready(payload)


def _build_cfg(
    *,
    market: str,
    cycle: str | int,
    profile: str | None,
    target: str | None,
    root: Path | None,
) -> ResearchConfig:
    return ResearchConfig.build(
        market=market,
        cycle=cycle,
        profile=profile or "default",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target=target or "direction",
        model_family="deep_otm",
        root=root,
    )


def _resolve_bundle_dir(
    cfg: ResearchConfig,
    *,
    profile: str,
    target: str,
    bundle_label: str | None,
    bundle_dir: str | Path | None,
) -> Path:
    if bundle_dir is not None:
        path = Path(bundle_dir)
        if path.exists():
            return path
        raise FileNotFoundError(f"Model bundle not found: {path}")
    return resolve_model_bundle_dir(
        cfg,
        profile=profile,
        target=target,
        bundle_label=bundle_label,
    )


def _active_bundle_lookup(cfg: ResearchConfig) -> dict[tuple[str, str], dict[str, object]]:
    rows: dict[tuple[str, str], dict[str, object]] = {}
    root = cfg.layout.active_bundles_root
    if not root.exists():
        return rows
    for selection_path in root.glob("profile=*/target=*/selection.json"):
        payload = read_json_object(selection_path)
        if not isinstance(payload, dict):
            continue
        rows[(str(payload.get("profile") or ""), str(payload.get("target") or ""))] = {
            "selection_path": str(selection_path),
            "selection": payload,
            **payload,
        }
    return rows


def _build_bundle_row(bundle_dir: Path) -> dict[str, object]:
    summary_path = bundle_dir / "summary.json"
    report_path = bundle_dir / "report.md"
    manifest_path = bundle_dir / "manifest.json"

    summary = _read_bundle_summary(summary_path)
    manifest = _read_bundle_manifest(manifest_path)
    spec = dict(manifest.get("spec") or {}) if manifest is not None else {}
    offsets = _resolve_offsets(bundle_dir, summary=summary, spec=spec)
    market = _string_value(summary, "market") or _string_value(manifest, "market") or _part_value(bundle_dir, "asset")
    cycle = _string_value(summary, "cycle") or _string_value(manifest, "cycle") or _part_value(bundle_dir, "cycle")
    profile = _string_value(summary, "profile") or _string_value(spec, "profile") or _part_value(bundle_dir, "profile")
    target = _string_value(summary, "target") or _string_value(spec, "target") or _part_value(bundle_dir, "target")
    bundle_label = _string_value(summary, "bundle_label") or _string_value(spec, "bundle_label") or _part_value(bundle_dir, "bundle")
    feature_set = _string_value(summary, "feature_set") or _string_value(spec, "feature_set")
    label_set = normalize_label_set(_string_value(summary, "label_set") or _string_value(spec, "label_set") or "truth")
    model_family = _string_value(summary, "model_family") or _string_value(spec, "model_family")
    source_training_run_dir = _string_value(summary, "source_training_run_dir") or _string_value(spec, "source_training_run")
    allowed_blacklist_columns = _string_list(summary, "allowed_blacklist_columns")

    return {
        "object_type": "model_bundle",
        "market": market,
        "cycle": cycle,
        "profile": profile,
        "target": target,
        "bundle_label": bundle_label,
        "bundle_dir": str(bundle_dir),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(manifest_path),
        "summary_exists": summary_path.exists(),
        "report_exists": report_path.exists(),
        "manifest_exists": manifest_path.exists(),
        "feature_set": feature_set,
        "label_set": label_set,
        "model_family": model_family,
        "source_training_run_dir": source_training_run_dir,
        "offsets": offsets,
        "offset_count": len(offsets),
        "allowed_blacklist_columns": allowed_blacklist_columns,
        "action_context": {
            "market": market,
            "cycle": cycle,
            "profile": profile,
            "target": target,
            "bundle_label": bundle_label,
            "model_family": model_family,
            "feature_set": feature_set,
            "label_set": label_set,
            "offsets": offsets,
            "source_training_run_dir": source_training_run_dir,
            "bundle_dir": str(bundle_dir),
        },
        "summary": summary,
        "manifest": manifest,
    }


def _build_offset_details(bundle_dir: Path) -> list[dict[str, object]]:
    offsets_root = bundle_dir / "offsets"
    if not offsets_root.exists():
        return []

    rows: list[dict[str, object]] = []
    for offset_dir in sorted(
        [path for path in offsets_root.glob("offset=*") if path.is_dir()],
        key=lambda path: _offset_value(path),
    ):
        bundle_config_path = offset_dir / "bundle_config.json"
        feature_schema_path = offset_dir / "feature_schema.json"
        diagnostics_dir = offset_dir / "diagnostics"
        rows.append(
            {
                "offset": _offset_value(offset_dir),
                "offset_dir": str(offset_dir),
                "bundle_config_path": str(bundle_config_path),
                "feature_schema_path": str(feature_schema_path),
                "bundle_config_exists": bundle_config_path.exists(),
                "feature_schema_exists": feature_schema_path.exists(),
                "bundle_config": read_json_object(bundle_config_path),
                "feature_schema": _read_json_value(feature_schema_path),
                "model_files": _sorted_file_names(offset_dir / "models", "*.joblib"),
                "calibration_files": _sorted_file_names(offset_dir / "calibration", "*"),
                "diagnostic_files": _relative_file_names(diagnostics_dir),
                "diagnostics": {
                    "summary": read_json_object(diagnostics_dir / "summary.json"),
                    "metrics": read_json_object(diagnostics_dir / "metrics.json"),
                    "feature_pruning": read_json_object(diagnostics_dir / "feature_pruning.json"),
                    "probe": read_json_object(diagnostics_dir / "probe.json"),
                    "report_text": _read_text_file(diagnostics_dir / "report.md"),
                    "oof_predictions_exists": (diagnostics_dir / "oof_predictions.parquet").exists(),
                },
                "offset_report_text": _read_text_file(offset_dir / "reports" / "offset_report.md"),
            }
        )
    return rows


def _resolve_offsets(bundle_dir: Path, *, summary: dict[str, object] | None, spec: dict[str, object]) -> list[int]:
    if isinstance(summary, dict):
        raw = summary.get("offsets")
        if isinstance(raw, list):
            values = []
            for item in raw:
                try:
                    values.append(int(item))
                except Exception:
                    continue
            return sorted(dict.fromkeys(values))
    raw_spec = spec.get("offsets")
    if isinstance(raw_spec, list):
        values = []
        for item in raw_spec:
            try:
                values.append(int(item))
            except Exception:
                continue
        return sorted(dict.fromkeys(values))
    values = []
    offsets_root = bundle_dir / "offsets"
    if offsets_root.exists():
        for path in offsets_root.glob("offset=*"):
            if path.is_dir():
                values.append(_offset_value(path))
    return sorted(dict.fromkeys(values))


def _offset_value(path: Path) -> int:
    try:
        return int(path.name.split("=", 1)[1])
    except Exception:
        return -1


def _part_value(path: Path, key: str) -> str | None:
    token = f"{key}="
    for part in path.parts:
        if part.startswith(token):
            return part.split("=", 1)[1]
    return None


def _read_bundle_summary(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        return read_bundle_summary(path.parent)
    except Exception:
        return read_json_object(path)


def _read_bundle_manifest(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        return read_model_bundle_manifest(path.parent).to_dict()
    except Exception:
        try:
            return read_manifest(path).to_dict()
        except Exception:
            return None


def _read_text_file(path: Path) -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def _read_json_value(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        import json

        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _string_value(payload: dict[str, object] | None, key: str) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _string_list(payload: dict[str, object] | None, key: str) -> list[str]:
    if not isinstance(payload, dict):
        return []
    value = payload.get(key)
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item)]


def _sorted_file_names(directory: Path, pattern: str) -> list[str]:
    if not directory.exists():
        return []
    return sorted(path.name for path in directory.glob(pattern) if path.is_file())


def _relative_file_names(directory: Path) -> list[str]:
    if not directory.exists():
        return []
    rows = []
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            rows.append(str(path.relative_to(directory)))
    return rows
