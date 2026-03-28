from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_OFFSET_WINDOW_SECONDS = 60.0


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
        "score_valid": False,
        "score_reason": "missing_score_row",
        "coverage": coverage,
    }


def build_inactive_score_signal(
    *,
    offset: int,
    coverage: dict[str, object],
    status: str,
    window_start_ts: str | None = None,
    window_end_ts: str | None = None,
    cycle_start_ts: str | None = None,
    cycle_end_ts: str | None = None,
    window_duration_seconds: float = float(DEFAULT_OFFSET_WINDOW_SECONDS),
) -> dict[str, object]:
    return {
        "offset": offset,
        "status": str(status or "inactive_score_row"),
        "score_valid": False,
        "score_reason": str(status or "inactive_score_row"),
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "window_duration_seconds": float(window_duration_seconds),
        "cycle_start_ts": cycle_start_ts,
        "cycle_end_ts": cycle_end_ts,
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
    decision_ts = iso_or_none_fn(row.get("decision_ts"))
    window_start_ts, window_end_ts, window_duration_seconds = build_offset_window(
        offset=ctx.offset,
        cycle_start_ts=row.get("cycle_start_ts"),
        cycle_end_ts=row.get("cycle_end_ts"),
        decision_ts=row.get("decision_ts"),
        iso_or_none_fn=iso_or_none_fn,
    )
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
        "decision_ts": decision_ts,
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "window_duration_seconds": window_duration_seconds,
        "cycle_start_ts": iso_or_none_fn(row.get("cycle_start_ts")),
        "cycle_end_ts": iso_or_none_fn(row.get("cycle_end_ts")),
        "signal_target": str(ctx.bundle_cfg.get("signal_target") or selected_target),
        "score_valid": score_valid,
        "score_reason": score_reason,
        "p_lgb": float(row.get("p_lgb", p_up)),
        "p_lr": float(row.get("p_lr", p_up)),
        "p_signal": float(row.get("p_signal", p_up)),
        "w_lgb": float(row.get("w_lgb", 0.5)),
        "w_lr": float(row.get("w_lr", 0.5)),
        "p_up": p_up,
        "p_down": p_down,
        "probability_mode": str(row.get("probability_mode") or ""),
        "model_context": row.get("model_context"),
        "recommended_side": "UP" if p_up >= p_down else "DOWN",
        "confidence": max(p_up, p_down),
        "edge": abs(p_up - p_down),
        "applied_blacklist_columns": ctx.effective_blacklist,
        "not_allowed_blacklist_columns": ctx.not_allowed_blacklist,
        "feature_snapshot": feature_snapshot,
        "coverage": coverage,
    }


def build_offset_window(
    *,
    offset: int,
    cycle_start_ts: object,
    cycle_end_ts: object,
    decision_ts: object,
    iso_or_none_fn,
) -> tuple[str | None, str | None, float]:
    decision_dt = pd.to_datetime(decision_ts, utc=True, errors="coerce")
    cycle_start_dt = pd.to_datetime(cycle_start_ts, utc=True, errors="coerce")
    cycle_end_dt = pd.to_datetime(cycle_end_ts, utc=True, errors="coerce")
    if decision_dt is not None and not pd.isna(decision_dt):
        start_dt = decision_dt
        end_dt = start_dt + pd.to_timedelta(DEFAULT_OFFSET_WINDOW_SECONDS, unit="s")
        if cycle_end_dt is not None and not pd.isna(cycle_end_dt):
            end_dt = min(end_dt, cycle_end_dt)
        return (
            iso_or_none_fn(start_dt),
            iso_or_none_fn(end_dt),
            float(DEFAULT_OFFSET_WINDOW_SECONDS),
        )
    if cycle_start_dt is not None and not pd.isna(cycle_start_dt):
        start_dt = cycle_start_dt + pd.to_timedelta(int(offset), unit="m")
        end_dt = start_dt + pd.to_timedelta(DEFAULT_OFFSET_WINDOW_SECONDS, unit="s")
        if cycle_end_dt is not None and not pd.isna(cycle_end_dt):
            end_dt = min(end_dt, cycle_end_dt)
        return (
            iso_or_none_fn(start_dt),
            iso_or_none_fn(end_dt),
            float(DEFAULT_OFFSET_WINDOW_SECONDS),
        )
    if decision_dt is None or pd.isna(decision_dt):
        return None, None, float(DEFAULT_OFFSET_WINDOW_SECONDS)
    start_dt = decision_dt
    end_dt = start_dt + pd.to_timedelta(DEFAULT_OFFSET_WINDOW_SECONDS, unit="s")
    return (
        iso_or_none_fn(start_dt),
        iso_or_none_fn(end_dt),
        float(DEFAULT_OFFSET_WINDOW_SECONDS),
    )


def _feature_cycle_context(
    *,
    ctx: OffsetScoreContext,
    now_utc: pd.Timestamp,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None, pd.Timestamp | None]:
    features = ctx.features
    if not isinstance(features, pd.DataFrame) or features.empty:
        return None, None, None

    decision_ts = pd.to_datetime(features.get("decision_ts"), utc=True, errors="coerce")
    latest_feature_decision_ts = None
    if isinstance(decision_ts, pd.Series):
        latest_feature_decision_ts = decision_ts.dropna().max()
        if pd.isna(latest_feature_decision_ts):
            latest_feature_decision_ts = None

    cycle_start = pd.to_datetime(features.get("cycle_start_ts"), utc=True, errors="coerce")
    cycle_end = pd.to_datetime(features.get("cycle_end_ts"), utc=True, errors="coerce")
    if not isinstance(cycle_start, pd.Series) or not isinstance(cycle_end, pd.Series):
        return None, None, latest_feature_decision_ts

    active = cycle_start.notna() & cycle_end.notna() & cycle_start.le(now_utc) & cycle_end.gt(now_utc)
    if not bool(active.any()):
        inferred_cycle = _infer_current_cycle_bounds(
            now_utc=now_utc,
            cycle_start=cycle_start,
            cycle_end=cycle_end,
        )
        if inferred_cycle is None:
            return None, None, latest_feature_decision_ts
        return inferred_cycle[0], inferred_cycle[1], latest_feature_decision_ts

    active_cycle_start = cycle_start.loc[active].max()
    active_cycle_end = cycle_end.loc[active & cycle_start.eq(active_cycle_start)].max()
    return active_cycle_start, active_cycle_end, latest_feature_decision_ts


def _infer_current_cycle_bounds(
    *,
    now_utc: pd.Timestamp,
    cycle_start: pd.Series,
    cycle_end: pd.Series,
) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    valid = cycle_start.notna() & cycle_end.notna() & cycle_end.gt(cycle_start)
    if not bool(valid.any()):
        return None
    durations = (cycle_end.loc[valid] - cycle_start.loc[valid]).dropna()
    if durations.empty:
        return None
    cycle_duration = durations.iloc[-1]
    total_seconds = float(cycle_duration.total_seconds())
    if total_seconds <= 0.0:
        return None
    minutes = total_seconds / 60.0
    if abs(minutes - round(minutes)) > 1e-9:
        return None
    floor_freq = f"{int(round(minutes))}min"
    current_cycle_start = now_utc.floor(floor_freq)
    current_cycle_end = current_cycle_start + pd.to_timedelta(total_seconds, unit="s")
    return current_cycle_start, current_cycle_end


def _resolve_latest_live_row(
    *,
    ctx: OffsetScoreContext,
    now_utc: pd.Timestamp,
    feature_coverage_fn,
    latest_nan_feature_columns_fn,
    iso_or_none_fn,
) -> tuple[object | None, dict[str, object], str | None]:
    base_coverage = build_offset_coverage(
        ctx=ctx,
        nan_feature_columns=[],
        feature_coverage_fn=feature_coverage_fn,
    )
    scored = ctx.scored.copy()
    if scored.empty or "decision_ts" not in scored.columns:
        return None, base_coverage, "missing_score_row"
    scored["decision_ts"] = pd.to_datetime(scored.get("decision_ts"), utc=True, errors="coerce")
    scored = scored.dropna(subset=["decision_ts"]).sort_values("decision_ts")
    if scored.empty:
        return None, base_coverage, "missing_score_row"

    active_cycle_start, active_cycle_end, latest_feature_decision_ts = _feature_cycle_context(
        ctx=ctx,
        now_utc=now_utc,
    )
    if active_cycle_start is not None:
        cycle_start = pd.to_datetime(scored.get("cycle_start_ts"), utc=True, errors="coerce")
        cycle_end = pd.to_datetime(scored.get("cycle_end_ts"), utc=True, errors="coerce")
        if isinstance(cycle_start, pd.Series) and isinstance(cycle_end, pd.Series):
            current_cycle_mask = cycle_start.eq(active_cycle_start)
            if active_cycle_end is not None:
                current_cycle_mask &= cycle_end.eq(active_cycle_end)
            current_cycle_scored = scored.loc[current_cycle_mask].copy()

            expected_open_ts = active_cycle_start + pd.to_timedelta(int(ctx.offset), unit="m")
            if latest_feature_decision_ts is None or latest_feature_decision_ts < expected_open_ts:
                return None, base_coverage, "offset_not_yet_open"
            if current_cycle_scored.empty:
                return None, base_coverage, "missing_score_row"
            scored = current_cycle_scored

    scored = scored.loc[scored["decision_ts"].le(now_utc)].copy()
    if scored.empty:
        return None, base_coverage, "offset_not_yet_open"
    row = scored.iloc[-1]
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
    _, window_end_ts, _ = build_offset_window(
        offset=ctx.offset,
        cycle_start_ts=row.get("cycle_start_ts"),
        cycle_end_ts=row.get("cycle_end_ts"),
        decision_ts=row.get("decision_ts"),
        iso_or_none_fn=iso_or_none_fn,
    )
    window_end_dt = pd.to_datetime(window_end_ts, utc=True, errors="coerce")
    if window_end_dt is None or pd.isna(window_end_dt):
        return row, coverage, None
    if now_utc >= window_end_dt:
        return None, coverage, "offset_window_expired"
    return row, coverage, None


def build_offset_signal(
    *,
    selected_target: str,
    ctx: OffsetScoreContext,
    feature_coverage_fn,
    latest_nan_feature_columns_fn,
    extract_feature_snapshot_fn,
    iso_or_none_fn,
    now_utc: pd.Timestamp,
) -> dict[str, object]:
    row, coverage, inactive_reason = _resolve_latest_live_row(
        ctx=ctx,
        now_utc=now_utc,
        feature_coverage_fn=feature_coverage_fn,
        latest_nan_feature_columns_fn=latest_nan_feature_columns_fn,
        iso_or_none_fn=iso_or_none_fn,
    )
    if row is None:
        if inactive_reason == "missing_score_row":
            return build_missing_score_signal(offset=ctx.offset, coverage=coverage)
        active_cycle_start, active_cycle_end, _ = _feature_cycle_context(
            ctx=ctx,
            now_utc=now_utc,
        )
        window_start_ts = None
        window_end_ts = None
        cycle_start_ts = iso_or_none_fn(active_cycle_start)
        cycle_end_ts = iso_or_none_fn(active_cycle_end)
        if active_cycle_start is not None:
            window_start_dt = active_cycle_start + pd.to_timedelta(int(ctx.offset), unit="m")
            window_end_dt = window_start_dt + pd.to_timedelta(DEFAULT_OFFSET_WINDOW_SECONDS, unit="s")
            if active_cycle_end is not None:
                window_end_dt = min(window_end_dt, active_cycle_end)
            window_start_ts = iso_or_none_fn(window_start_dt)
            window_end_ts = iso_or_none_fn(window_end_dt)
        return build_inactive_score_signal(
            offset=ctx.offset,
            coverage=coverage,
            status=str(inactive_reason or "inactive_score_row"),
            window_start_ts=window_start_ts,
            window_end_ts=window_end_ts,
            cycle_start_ts=cycle_start_ts,
            cycle_end_ts=cycle_end_ts,
        )

    nan_feature_columns = list(coverage.get("nan_feature_columns") or [])
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
    now_utc: pd.Timestamp | None = None,
) -> list[dict[str, object]]:
    profile_blacklist = list(profile_spec.blacklist_for(cfg.asset.slug))
    now_utc = pd.Timestamp(now_utc) if now_utc is not None else pd.Timestamp.now(tz="UTC")
    if now_utc.tzinfo is None:
        now_utc = now_utc.tz_localize("UTC")
    else:
        now_utc = now_utc.tz_convert("UTC")
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
                now_utc=now_utc,
            )
        )
    return offset_signals
