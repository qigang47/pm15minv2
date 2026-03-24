from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class OffsetScoreContext:
    offset: int
    bundle_cfg: dict[str, object]
    feature_columns: list[str]
    effective_blacklist: list[str]
    not_allowed_blacklist: list[str]
    features: Any
    scored: Any


def resolve_offset_dirs(bundle_dir: Path) -> list[Path]:
    offset_dirs = sorted((bundle_dir / "offsets").glob("offset=*"))
    if not offset_dirs:
        raise FileNotFoundError(f"Bundle has no offsets: {bundle_dir}")
    return offset_dirs


def build_coverage(
    *,
    features,
    feature_columns: list[str],
    effective_blacklist: list[str],
    not_allowed_blacklist: list[str],
    nan_feature_columns: list[str],
    feature_coverage_fn,
) -> dict[str, object]:
    return feature_coverage_fn(
        available_columns=set(features.columns),
        required_columns=feature_columns,
        blacklisted_columns=effective_blacklist,
        not_allowed_blacklist_columns=not_allowed_blacklist,
        nan_feature_columns=nan_feature_columns,
    )


def build_offset_coverage(
    *,
    ctx: OffsetScoreContext,
    nan_feature_columns: list[str],
    feature_coverage_fn,
) -> dict[str, object]:
    return build_coverage(
        features=ctx.features,
        feature_columns=ctx.feature_columns,
        effective_blacklist=ctx.effective_blacklist,
        not_allowed_blacklist=ctx.not_allowed_blacklist,
        nan_feature_columns=nan_feature_columns,
        feature_coverage_fn=feature_coverage_fn,
    )


def prepare_offset_score_context(
    *,
    bundle_dir: Path,
    offset: int,
    base_features,
    profile_blacklist: list[str],
    read_bundle_config_fn,
    resolve_live_blacklist_fn,
    apply_live_blacklist_fn,
    score_bundle_offset_fn,
) -> OffsetScoreContext:
    bundle_cfg = read_bundle_config_fn(bundle_dir, offset=offset)
    feature_columns = list(bundle_cfg.get("feature_columns") or [])
    effective_blacklist, not_allowed_blacklist = resolve_live_blacklist_fn(
        profile_blacklist=profile_blacklist,
        bundle_allowed_blacklist=list(bundle_cfg.get("allowed_blacklist_columns") or []),
    )
    features = base_features.copy()
    apply_live_blacklist_fn(features, blacklist_columns=effective_blacklist)
    scored = score_bundle_offset_fn(bundle_dir, features, offset=offset)
    return OffsetScoreContext(
        offset=offset,
        bundle_cfg=bundle_cfg,
        feature_columns=feature_columns,
        effective_blacklist=effective_blacklist,
        not_allowed_blacklist=not_allowed_blacklist,
        features=features,
        scored=scored,
    )


def build_missing_score_signal(*, offset: int, coverage: dict[str, object]) -> dict[str, object]:
    return {
        "offset": offset,
        "status": "missing_score_row",
        "coverage": coverage,
    }


def normalize_score_validity(
    *,
    score_valid: bool,
    score_reason: str,
    nan_feature_columns: list[str],
) -> tuple[bool, str]:
    if nan_feature_columns:
        return False, "nan_features"
    return score_valid, score_reason


def build_scored_signal(
    *,
    selected_target: str,
    ctx: OffsetScoreContext,
    row,
    nan_feature_columns: list[str],
    coverage: dict[str, object],
    extract_feature_snapshot_fn,
    iso_or_none_fn,
) -> dict[str, object]:
    p_up = float(row["p_up"])
    p_down = float(row["p_down"])
    score_valid, score_reason = normalize_score_validity(
        score_valid=bool(row.get("score_valid", False)),
        score_reason=str(row.get("score_reason") or ""),
        nan_feature_columns=nan_feature_columns,
    )
    feature_snapshot = extract_feature_snapshot_fn(ctx.features, offset=ctx.offset, decision_ts=row.get("decision_ts"))
    return {
        "offset": ctx.offset,
        "decision_ts": iso_or_none_fn(row.get("decision_ts")),
        "cycle_start_ts": iso_or_none_fn(row.get("cycle_start_ts")),
        "cycle_end_ts": iso_or_none_fn(row.get("cycle_end_ts")),
        "signal_target": str(ctx.bundle_cfg.get("signal_target") or selected_target),
        "score_valid": score_valid,
        "score_reason": score_reason,
        "p_signal": float(row.get("p_signal", p_up)),
        "p_up": p_up,
        "p_down": p_down,
        "recommended_side": "UP" if p_up >= p_down else "DOWN",
        "confidence": max(p_up, p_down),
        "edge": abs(p_up - p_down),
        "applied_blacklist_columns": ctx.effective_blacklist,
        "not_allowed_blacklist_columns": ctx.not_allowed_blacklist,
        "feature_snapshot": feature_snapshot,
        "coverage": coverage,
    }


def build_offset_signal(
    *,
    selected_target: str,
    ctx: OffsetScoreContext,
    feature_coverage_fn,
    latest_nan_feature_columns_fn,
    extract_feature_snapshot_fn,
    iso_or_none_fn,
) -> dict[str, object]:
    latest_row = ctx.scored.sort_values("decision_ts").tail(1)
    base_coverage = build_offset_coverage(
        ctx=ctx,
        nan_feature_columns=[],
        feature_coverage_fn=feature_coverage_fn,
    )
    if latest_row.empty:
        return build_missing_score_signal(offset=ctx.offset, coverage=base_coverage)

    row = latest_row.iloc[0]
    nan_feature_columns = latest_nan_feature_columns_fn(
        features=ctx.features,
        offset=ctx.offset,
        decision_ts=row.get("decision_ts"),
        required_columns=ctx.feature_columns,
    )
    coverage = build_offset_coverage(
        ctx=ctx,
        nan_feature_columns=nan_feature_columns,
        feature_coverage_fn=feature_coverage_fn,
    )
    return build_scored_signal(
        selected_target=selected_target,
        ctx=ctx,
        row=row,
        nan_feature_columns=nan_feature_columns,
        coverage=coverage,
        extract_feature_snapshot_fn=extract_feature_snapshot_fn,
        iso_or_none_fn=iso_or_none_fn,
    )


def score_offset_signals(
    cfg,
    *,
    selected_target: str,
    profile_spec,
    bundle_dir: Path,
    base_features,
    read_bundle_config_fn,
    resolve_live_blacklist_fn,
    apply_live_blacklist_fn,
    score_bundle_offset_fn,
    feature_coverage_fn,
    latest_nan_feature_columns_fn,
    extract_feature_snapshot_fn,
    iso_or_none_fn,
) -> list[dict[str, object]]:
    profile_blacklist = list(profile_spec.blacklist_for(cfg.asset.slug))
    offset_signals: list[dict[str, object]] = []
    for offset_dir in resolve_offset_dirs(bundle_dir):
        offset = int(offset_dir.name.split("=", 1)[1])
        ctx = prepare_offset_score_context(
            bundle_dir=bundle_dir,
            offset=offset,
            base_features=base_features,
            profile_blacklist=profile_blacklist,
            read_bundle_config_fn=read_bundle_config_fn,
            resolve_live_blacklist_fn=resolve_live_blacklist_fn,
            apply_live_blacklist_fn=apply_live_blacklist_fn,
            score_bundle_offset_fn=score_bundle_offset_fn,
        )
        offset_signals.append(
            build_offset_signal(
                selected_target=selected_target,
                ctx=ctx,
                feature_coverage_fn=feature_coverage_fn,
                latest_nan_feature_columns_fn=latest_nan_feature_columns_fn,
                extract_feature_snapshot_fn=extract_feature_snapshot_fn,
                iso_or_none_fn=iso_or_none_fn,
            )
        )
    return offset_signals
