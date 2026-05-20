from __future__ import annotations

import gc
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io import write_json_atomic
from pm15min.data.io.parquet import read_parquet_if_exists, write_parquet_atomic
from pm15min.core.process_memory import trim_process_memory
from pm15min.research.backtests.replay_loader import build_replay_frame
from pm15min.research.bundles.loader import read_bundle_config
from pm15min.research.backtests.decision_engine_parity import (
    apply_decision_engine_parity,
    build_profile_decision_engine_parity_config,
)
from pm15min.research.backtests.engine import (
    _build_backtest_fill_config,
    _build_bundle_replay,
    _build_decision_depth_runtime,
    _build_guarded_policy_decisions,
    _bundle_offsets,
    _feature_frame_filters,
    _filter_replay_window,
    _label_frame_filters,
    _resolve_fill_depth_runtime,
)
from pm15min.research.backtests.fills import build_canonical_fills
from pm15min.research.backtests.orderbook_surface import attach_canonical_quote_surface
from pm15min.research.backtests.policy import BacktestPolicyConfig, build_policy_decisions, build_policy_reject_frame
from pm15min.research.backtests.regime_parity import resolve_backtest_profile_spec
from pm15min.research.backtests.settlement import settle_trade_fills
from pm15min.research.bundles.builder import build_model_bundle
from pm15min.research.bundles.loader import read_bundle_summary, read_training_run_summary
from pm15min.research.automation.dense_policy import classify_density_bottleneck
from pm15min.research.config import ResearchConfig
from pm15min.research._contracts_runs import BacktestParitySpec
from pm15min.research.contracts import ModelBundleSpec, TrainingRunSpec
from pm15min.research.layout_helpers import slug_token
from pm15min.research.datasets.loaders import load_feature_frame, load_label_frame
from pm15min.research.training.runner import train_research_run

_FEATURE_KEY_COLUMNS = ("decision_ts", "cycle_start_ts", "cycle_end_ts", "offset")
_LABEL_REQUIRED_COLUMNS = (
    "cycle_start_ts",
    "cycle_end_ts",
    "label_set",
    "resolved",
    "winner_side",
    "label_source",
    "settlement_source",
    "full_truth",
)
_REVERSAL_ANCHOR_COLUMNS = ("ret_from_strike", "ret_from_cycle_open")
_POOL_STATUS_ORDER = ("captured", "correct_side_no_trade", "missed", "traded_wrong_side")
_POOL_CACHE_COLUMNS = (*_FEATURE_KEY_COLUMNS, "winner_side", "winner_entry_price")
_PREWARM_FEATURE_COLUMNS = (*_FEATURE_KEY_COLUMNS, "market_id", "condition_id")
_PREWARM_DEFAULT_OFFSETS = (7, 8, 9)
_QUICK_SCREEN_RETAIN_MIN_TRADES = 56


def build_profitable_offset_pool_frame(
    decisions: pd.DataFrame,
    *,
    entry_price_min: float | None,
    entry_price_max: float | None,
    capture_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    frame = decisions
    capture_keys = _frame_key_set(capture_frame)
    out = pd.DataFrame(index=frame.index)
    for column in _FEATURE_KEY_COLUMNS:
        out[column] = frame[column] if column in frame.columns else pd.NA

    winner_side = _string_series(frame, "winner_side").str.upper()
    predicted_side = _string_series(frame, "predicted_side").str.upper()
    policy_action = _string_series(frame, "policy_action").str.lower()
    policy_reason = _string_series(frame, "policy_reason")
    quote_status = _string_series(frame, "quote_status")
    resolved = _bool_series(frame, "resolved")
    winner_entry_price = _winner_entry_price(frame, winner_side=winner_side)
    profitable_pool_window = (
        resolved
        & quote_status.eq("ok")
        & winner_side.isin(["UP", "DOWN"])
        & winner_entry_price.notna()
    )
    if entry_price_min is not None:
        profitable_pool_window &= winner_entry_price.ge(float(entry_price_min))
    if entry_price_max is not None:
        profitable_pool_window &= winner_entry_price.le(float(entry_price_max))

    final_trade_membership = _row_key_membership(frame, capture_keys) if capture_frame is not None else None
    trade_membership = policy_action.eq("trade") if final_trade_membership is None else final_trade_membership
    profitable_pool_correct_side = profitable_pool_window & predicted_side.eq(winner_side)
    profitable_pool_capture = profitable_pool_correct_side & trade_membership
    traded_wrong_side = profitable_pool_window & trade_membership & predicted_side.ne(winner_side)
    correct_side_no_trade = profitable_pool_correct_side & ~trade_membership
    missed = profitable_pool_window & ~(profitable_pool_capture | correct_side_no_trade | traded_wrong_side)

    profitable_pool_status = pd.Series("not_in_pool", index=frame.index, dtype="string")
    profitable_pool_status.loc[missed] = "missed"
    profitable_pool_status.loc[correct_side_no_trade] = "correct_side_no_trade"
    profitable_pool_status.loc[traded_wrong_side] = "traded_wrong_side"
    profitable_pool_status.loc[profitable_pool_capture] = "captured"

    out["winner_side"] = winner_side
    out["predicted_side"] = predicted_side
    out["policy_action"] = policy_action
    out["policy_reason"] = policy_reason
    out["quote_status"] = quote_status
    out["winner_entry_price"] = winner_entry_price
    out["profitable_pool_window"] = profitable_pool_window.astype(bool)
    out["profitable_pool_correct_side"] = profitable_pool_correct_side.astype(bool)
    out["profitable_pool_capture"] = profitable_pool_capture.astype(bool)
    out["profitable_pool_status"] = profitable_pool_status.astype(str)
    return out


def profitable_offset_pool_cache_paths(
    *,
    cfg: ResearchConfig,
    profile: str,
    decision_start: str | None,
    decision_end: str | None,
    stake_label: str = "2usd",
) -> tuple[Path, Path]:
    cache_dir = (
        cfg.layout.storage.cache_root
        / "profitable_offset_pools"
        / f"cycle={cfg.cycle}"
        / f"asset={cfg.asset.slug}"
        / f"profile={slug_token(profile)}"
        / f"decision_start={slug_token(str(decision_start or 'open'))}"
        / f"decision_end={slug_token(str(decision_end or 'open'))}"
        / f"stake={slug_token(stake_label)}"
    )
    return cache_dir / "data.parquet", cache_dir / "manifest.json"


def resolve_profitable_offset_pool_frame(
    *,
    cfg: ResearchConfig,
    profile: str,
    decision_start: str | None,
    decision_end: str | None,
    decisions: pd.DataFrame,
    entry_price_min: float | None,
    entry_price_max: float | None,
    stake_label: str = "2usd",
) -> tuple[pd.DataFrame, dict[str, object]]:
    data_path, manifest_path = profitable_offset_pool_cache_paths(
        cfg=cfg,
        profile=profile,
        decision_start=decision_start,
        decision_end=decision_end,
        stake_label=stake_label,
    )
    cached_pool = read_parquet_if_exists(data_path)
    pool_frame: pd.DataFrame | None = None
    cache_status = "reused"
    if cached_pool is None:
        cache_status = "built"
        built_pool = build_profitable_offset_pool_frame(
            decisions,
            entry_price_min=entry_price_min,
            entry_price_max=entry_price_max,
        )
        cached_pool = (
            built_pool.loc[built_pool.get("profitable_pool_window", False).astype(bool), list(_POOL_CACHE_COLUMNS)]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        pool_frame = built_pool
        data_path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet_atomic(cached_pool, data_path)
        write_json_atomic(
            {
                "market": cfg.asset.slug,
                "cycle": cfg.cycle,
                "profile": str(profile),
                "decision_start": decision_start,
                "decision_end": decision_end,
                "stake_label": stake_label,
                "entry_price_min": entry_price_min,
                "entry_price_max": entry_price_max,
                "pool_rows": int(len(cached_pool)),
                "data_path": str(data_path),
            },
            manifest_path,
        )
    if pool_frame is None:
        pool_frame = _apply_cached_profitable_pool(
            decisions=decisions,
            cached_pool=cached_pool.reset_index(drop=True),
        )
    return pool_frame, {
        "cache_status": cache_status,
        "data_path": str(data_path),
        "manifest_path": str(manifest_path),
        "pool_rows": int(len(cached_pool)),
    }


def build_quick_screen_summary(
    decisions: pd.DataFrame,
    *,
    entry_price_min: float | None,
    entry_price_max: float | None,
    profitable_pool_frame: pd.DataFrame | None = None,
    final_trades: pd.DataFrame | None = None,
    rejects: pd.DataFrame | None = None,
) -> dict[str, object]:
    frame = decisions
    if frame.empty:
        return {
            "rows": 0,
            "resolved_rows": 0,
            "quote_ready_rows": 0,
            "winner_in_band_rows": 0,
            "backed_winner_rows": 0,
            "trade_rows": 0,
            "traded_winner_rows": 0,
            "backed_winner_in_band_rows": 0,
            "traded_winner_in_band_rows": 0,
            "profitable_pool_rows": 0,
            "profitable_pool_correct_side_rows": 0,
            "profitable_pool_capture_rows": 0,
            "profitable_pool_coverage_ratio": 0.0,
            "profitable_pool_status_counts": {status: 0 for status in _POOL_STATUS_ORDER},
            "reject_reason_counts": {},
            "density_bottleneck": classify_density_bottleneck(
                total_rows=0,
                trade_rows=0,
                profitable_pool_rows=0,
                profitable_pool_capture_rows=0,
                profitable_pool_correct_side_rows=0,
                reject_reason_counts={},
                quote_missing_rows=0,
            ),
        }

    resolved = _bool_series(frame, "resolved")
    quote_ready = frame.get("quote_status", pd.Series("", index=frame.index, dtype="string")).astype("string").eq("ok")
    winner_side = frame.get("winner_side", pd.Series("", index=frame.index, dtype="string")).astype("string").str.upper()
    predicted_side = frame.get("predicted_side", pd.Series("", index=frame.index, dtype="string")).astype("string").str.upper()
    signal_trade_rows = frame.get("policy_action", pd.Series("", index=frame.index, dtype="string")).astype("string").eq("trade")
    final_trade_keys = _frame_key_set(final_trades)
    if final_trades is None:
        trade_rows = signal_trade_rows
    else:
        trade_rows = _row_key_membership(frame, final_trade_keys)

    winner_entry_price = _winner_entry_price(frame, winner_side=winner_side)
    winner_in_band = (
        resolved
        & quote_ready
        & winner_side.isin(["UP", "DOWN"])
        & winner_entry_price.notna()
    )
    if entry_price_min is not None:
        winner_in_band &= winner_entry_price.ge(float(entry_price_min))
    if entry_price_max is not None:
        winner_in_band &= winner_entry_price.le(float(entry_price_max))

    backed_winner = resolved & winner_side.isin(["UP", "DOWN"]) & predicted_side.eq(winner_side)
    traded_winner = trade_rows & backed_winner
    profitable_pool_frame = profitable_pool_frame if profitable_pool_frame is not None else build_profitable_offset_pool_frame(
        frame,
        entry_price_min=entry_price_min,
        entry_price_max=entry_price_max,
        capture_frame=final_trades,
    )
    profitable_pool_rows = int(_bool_series(profitable_pool_frame, "profitable_pool_window").sum())
    profitable_pool_correct_side_rows = int(_bool_series(profitable_pool_frame, "profitable_pool_correct_side").sum())
    profitable_pool_capture_rows = int(_bool_series(profitable_pool_frame, "profitable_pool_capture").sum())
    profitable_pool_status_counts = {status: 0 for status in _POOL_STATUS_ORDER}
    pool_only_status = profitable_pool_frame.loc[
        profitable_pool_frame.get("profitable_pool_window", pd.Series(False, index=profitable_pool_frame.index)).astype(bool),
        "profitable_pool_status",
    ].astype("string")
    for status, value in pool_only_status.value_counts().sort_index().items():
        profitable_pool_status_counts[str(status)] = int(value)
    profitable_pool_coverage_ratio = (
        float(profitable_pool_capture_rows) / float(profitable_pool_rows)
        if profitable_pool_rows > 0
        else 0.0
    )

    rejects = rejects if rejects is not None else build_policy_reject_frame(frame)
    reject_counts = (
        rejects.get("reason", pd.Series(dtype="string")).astype("string").fillna("").value_counts().sort_index()
    )
    reject_counts = reject_counts[reject_counts.index != ""]

    summary = {
        "rows": int(len(frame)),
        "resolved_rows": int(resolved.sum()),
        "quote_ready_rows": int(quote_ready.sum()),
        "winner_in_band_rows": int(winner_in_band.sum()),
        "backed_winner_rows": int(backed_winner.sum()),
        "trade_rows": int(trade_rows.sum()),
        "signal_trade_rows": int(signal_trade_rows.sum()),
        "traded_winner_rows": int(traded_winner.sum()),
        "backed_winner_in_band_rows": int((backed_winner & winner_in_band).sum()),
        "traded_winner_in_band_rows": int((traded_winner & winner_in_band).sum()),
        "profitable_pool_rows": profitable_pool_rows,
        "profitable_pool_correct_side_rows": profitable_pool_correct_side_rows,
        "profitable_pool_capture_rows": profitable_pool_capture_rows,
        "profitable_pool_coverage_ratio": profitable_pool_coverage_ratio,
        "profitable_pool_status_counts": profitable_pool_status_counts,
        "reject_reason_counts": {str(index): int(value) for index, value in reject_counts.items()},
        "metric_semantics": {
            "trade_rows": (
                "quick_screen_formal_filled_trade_rows"
                if final_trades is not None
                else "quick_screen_policy_signal_rows"
            ),
            "signal_trade_rows": "quick_screen_policy_signal_rows",
            "profitable_pool_capture_rows": (
                "quick_screen_profitable_pool_filled_captures"
                if final_trades is not None
                else "quick_screen_profitable_pool_signal_captures"
            ),
        },
    }
    summary["density_bottleneck"] = classify_density_bottleneck(
        total_rows=int(summary["rows"]),
        trade_rows=int(summary["trade_rows"]),
        profitable_pool_rows=int(summary["profitable_pool_rows"]),
        profitable_pool_capture_rows=int(summary["profitable_pool_capture_rows"]),
        profitable_pool_correct_side_rows=int(summary["profitable_pool_correct_side_rows"]),
        reject_reason_counts=dict(summary["reject_reason_counts"]),
        quote_missing_rows=int(summary.get("quote_missing_rows_surface") or 0),
    )
    return summary


def quick_screen_rank_tuple(summary: dict[str, object]) -> tuple[float, int, int, int, int]:
    return (
        float(summary.get("profitable_pool_coverage_ratio") or 0.0),
        int(summary.get("profitable_pool_capture_rows") or 0),
        int(summary.get("profitable_pool_correct_side_rows") or 0),
        int(summary.get("trade_rows") or 0),
        int(summary.get("profitable_pool_rows") or 0),
    )


def quick_screen_artifact_retention_decision(
    quick_summary: dict[str, object],
    *,
    mode: str | None = None,
    retain_min_trades: int | None = None,
    retain_min_captures: int | None = None,
) -> dict[str, object]:
    retention_mode = _quick_screen_artifact_retention_mode(mode)
    trade_floor = _positive_int(
        retain_min_trades,
        default=_env_int("PM15MIN_QUICK_SCREEN_RETAIN_MIN_TRADES", _QUICK_SCREEN_RETAIN_MIN_TRADES),
    )
    capture_floor = _positive_int(
        retain_min_captures,
        default=_env_int("PM15MIN_QUICK_SCREEN_RETAIN_MIN_CAPTURES", 0),
        allow_zero=True,
    )
    trade_rows = max(0, int(quick_summary.get("trade_rows") or 0))
    capture_rows = max(0, int(quick_summary.get("profitable_pool_capture_rows") or 0))
    density = quick_summary.get("density_bottleneck")
    density_sparse = bool(density.get("sparse_density")) if isinstance(density, dict) else trade_rows < trade_floor

    retained = True
    reason = "mode_retain_all"
    if retention_mode == "compact_all":
        retained = False
        reason = "mode_compact_all"
    elif retention_mode == "compact_rejects":
        if trade_rows >= trade_floor:
            retained = True
            reason = "trade_floor_met"
        elif capture_floor > 0 and capture_rows >= capture_floor:
            retained = True
            reason = "capture_floor_met"
        else:
            retained = False
            reason = "below_trade_floor" if density_sparse else "below_retention_floor"

    return {
        "artifact_retention_mode": retention_mode,
        "artifacts_retained": bool(retained),
        "retention_reason": reason,
        "retain_min_trades": int(trade_floor),
        "retain_min_captures": int(capture_floor),
        "trade_rows": int(trade_rows),
        "profitable_pool_capture_rows": int(capture_rows),
        "density_sparse": bool(density_sparse),
    }


def compact_quick_screen_artifacts(
    *,
    cfg: ResearchConfig,
    market_spec,
    train_result: dict[str, object],
    bundle_result: dict[str, object],
    quick_summary: dict[str, object],
    apply: bool = True,
) -> dict[str, object]:
    decision = quick_screen_artifact_retention_decision(quick_summary)
    if bool(decision["artifacts_retained"]):
        return {
            **decision,
            "removed_paths": [],
            "removed_path_count": 0,
            "skipped_paths": [],
        }

    root = cfg.layout.storage.rewrite_root
    candidates = _quick_screen_compaction_paths(
        cfg=cfg,
        market_spec=market_spec,
        train_result=train_result,
        bundle_result=bundle_result,
    )
    candidate_paths = _dedupe_compaction_candidates(candidates)
    if not apply:
        would_remove = [
            str(path)
            for path in candidate_paths
            if _is_compactable_existing_path(path, root=root)
        ]
        return {
            **decision,
            "removed_paths": [],
            "removed_path_count": 0,
            "would_remove_paths": would_remove,
            "would_remove_path_count": len(would_remove),
            "skipped_paths": [],
        }

    removed: list[str] = []
    skipped: list[str] = []
    for resolved in candidate_paths:
        try:
            if _remove_compactable_path(resolved, root=root):
                removed.append(str(resolved))
        except ValueError:
            skipped.append(str(resolved))

    return {
        **decision,
        "removed_paths": removed,
        "removed_path_count": len(removed),
        "would_remove_paths": [],
        "would_remove_path_count": 0,
        "skipped_paths": skipped,
    }


def run_bundle_quick_screen(
    *,
    cfg: ResearchConfig,
    bundle_dir: Path,
    profile: str,
    target: str,
    decision_start: str | None,
    decision_end: str | None,
    parity,
    stake_label: str = "2usd",
    stake_usd: float | None = None,
    max_notional_usd: float | None = None,
    return_decisions: bool = True,
) -> tuple[dict[str, object], pd.DataFrame]:
    available_offsets = _bundle_offsets(bundle_dir)
    features = _load_quick_screen_feature_frame(
        cfg=cfg,
        bundle_dir=bundle_dir,
        target=target,
        available_offsets=available_offsets,
        decision_start=decision_start,
        decision_end=decision_end,
    )
    labels = _load_quick_screen_label_frame(
        cfg=cfg,
        scoped_features=features,
    )
    feature_rows = int(len(features))
    label_rows = int(len(labels))
    replay, replay_summary, _available_offsets = _build_bundle_replay(
        bundle_dir=bundle_dir,
        features=features,
        labels=labels,
    )
    del features, labels
    _quick_screen_collect_memory()
    replay = _filter_replay_window(
        replay,
        decision_start=decision_start,
        decision_end=decision_end,
    )
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        surface=cfg.source_surface,
        root=cfg.layout.storage.rewrite_root,
    )
    profile_spec = resolve_backtest_profile_spec(
        market=cfg.asset.slug,
        profile=profile,
        parity=parity,
    )
    fill_config = _build_quick_screen_fill_config(
        stake_label=stake_label,
        stake_usd=stake_usd,
        max_notional_usd=max_notional_usd,
        parity=parity,
        profile_spec=profile_spec,
    )
    depth_replay, depth_replay_summary, depth_candidate_lookup = _build_decision_depth_runtime(
        replay=replay,
        data_cfg=data_cfg,
        fill_config=fill_config,
    )
    replay, quote_summary = attach_canonical_quote_surface(
        replay=replay,
        data_cfg=data_cfg,
    )
    decisions, _guard_summary, decision_quote_summary = _build_guarded_policy_decisions(
        replay=replay,
        market=cfg.asset.slug,
        profile=profile,
        profile_spec=profile_spec,
        model_source="primary",
        depth_replay=depth_replay,
        fill_config=fill_config,
    )
    del replay
    _quick_screen_collect_memory()
    policy_rejects = build_policy_reject_frame(decisions)
    accepted = decisions.loc[decisions["policy_action"].eq("trade")].copy()
    fill_depth_replay, fill_depth_candidate_lookup = _resolve_fill_depth_runtime(
        accepted=accepted,
        decision_depth_replay=depth_replay,
        decision_depth_candidate_lookup=depth_candidate_lookup,
        data_cfg=data_cfg,
    )
    del depth_replay, depth_candidate_lookup
    _quick_screen_collect_memory()
    fills, fill_rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=fill_config,
        profile_spec=profile_spec,
        depth_replay=fill_depth_replay,
        depth_candidate_lookup=fill_depth_candidate_lookup,
    )
    trades = settle_trade_fills(fills)
    formal_policy_signal_rows = int(len(accepted))
    formal_fill_reject_rows = int(len(fill_rejects))
    del accepted, fill_depth_replay, fill_depth_candidate_lookup, fills
    _quick_screen_collect_memory()
    rejects = pd.concat([policy_rejects, fill_rejects], ignore_index=True, sort=False).reset_index(drop=True)
    del policy_rejects, fill_rejects
    _quick_screen_collect_memory()
    profitable_pool_frame, pool_cache = resolve_profitable_offset_pool_frame(
        cfg=cfg,
        profile=profile,
        decision_start=decision_start,
        decision_end=decision_end,
        decisions=decisions,
        entry_price_min=profile_spec.entry_price_min,
        entry_price_max=profile_spec.entry_price_max,
        stake_label=stake_label,
    )
    profitable_pool_frame = _apply_final_trade_captures(
        profitable_pool_frame=profitable_pool_frame,
        decisions=decisions,
        final_trades=trades,
    )
    summary = build_quick_screen_summary(
        decisions,
        entry_price_min=profile_spec.entry_price_min,
        entry_price_max=profile_spec.entry_price_max,
        profitable_pool_frame=profitable_pool_frame,
        final_trades=trades,
        rejects=rejects,
    )
    summary.update(
        {
            "feature_rows": feature_rows,
            "label_rows": label_rows,
            "replay_rows": int(replay_summary.merged_rows),
            "ready_rows": int(replay_summary.ready_rows),
            "quote_ready_rows_surface": int(quote_summary.quote_ready_rows),
            "quote_missing_rows_surface": int(quote_summary.quote_missing_rows),
            "formal_filled_trade_rows": int(len(trades)),
            "formal_policy_signal_rows": formal_policy_signal_rows,
            "formal_fill_reject_rows": formal_fill_reject_rows,
            "raw_depth_snapshot_rows": int(getattr(depth_replay_summary, "snapshot_rows", 0)),
            "raw_depth_replay_rows_with_snapshots": int(
                getattr(depth_replay_summary, "replay_rows_with_snapshots", 0)
            ),
            "raw_depth_replay_rows_without_snapshots": int(
                getattr(depth_replay_summary, "replay_rows_without_snapshots", 0)
            ),
            "decision_quote_raw_depth_rows": int(getattr(decision_quote_summary, "raw_depth_rows", 0)),
            "profitable_pool_cache_status": str(pool_cache.get("cache_status") or ""),
            "profitable_pool_cache_path": str(pool_cache.get("data_path") or ""),
            "profitable_pool_manifest_path": str(pool_cache.get("manifest_path") or ""),
        }
    )
    if return_decisions:
        return summary, decisions
    return summary, pd.DataFrame()


def _quick_screen_collect_memory() -> None:
    gc.collect()
    trim_process_memory()


def prewarm_profitable_offset_pool_cache(
    *,
    cfg: ResearchConfig,
    profile: str,
    decision_start: str | None,
    decision_end: str | None,
    stake_label: str = "2usd",
    offsets: tuple[int, ...] = _PREWARM_DEFAULT_OFFSETS,
) -> dict[str, object]:
    scoped_offsets = tuple(sorted({int(value) for value in offsets}))
    features = load_feature_frame(
        cfg,
        feature_set=cfg.feature_set,
        columns=list(_PREWARM_FEATURE_COLUMNS),
    )
    if features.empty:
        pool_path, manifest_path = profitable_offset_pool_cache_paths(
            cfg=cfg,
            profile=profile,
            decision_start=decision_start,
            decision_end=decision_end,
            stake_label=stake_label,
        )
        return {
            "market": cfg.asset.slug,
            "cache_status": "missing_features",
            "pool_rows": 0,
            "feature_rows": 0,
            "replay_rows": 0,
            "quote_ready_rows": 0,
            "data_path": str(pool_path),
            "manifest_path": str(manifest_path),
        }

    features = _filter_quick_screen_seed_feature_frame(
        features=features,
        available_offsets=list(scoped_offsets),
        decision_start=decision_start,
        decision_end=decision_end,
    )
    labels = load_label_frame(
        cfg,
        label_set=cfg.label_set,
        columns=["cycle_start_ts", "cycle_end_ts", "market_id", "condition_id", "winner_side", "resolved"],
    )
    replay, replay_summary = build_replay_frame(
        features=features,
        labels=labels,
        score_frames=[],
        available_offsets=list(scoped_offsets),
        scoped_offsets=list(scoped_offsets),
    )
    replay = _filter_replay_window(
        replay,
        decision_start=decision_start,
        decision_end=decision_end,
    )
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        surface=cfg.source_surface,
        root=cfg.layout.storage.rewrite_root,
    )
    replay, quote_summary = attach_canonical_quote_surface(
        replay=replay,
        data_cfg=data_cfg,
    )
    profile_spec = resolve_backtest_profile_spec(
        market=cfg.asset.slug,
        profile=profile,
        parity=None,
    )
    _pool_frame, pool_cache = resolve_profitable_offset_pool_frame(
        cfg=cfg,
        profile=profile,
        decision_start=decision_start,
        decision_end=decision_end,
        decisions=replay,
        entry_price_min=profile_spec.entry_price_min,
        entry_price_max=profile_spec.entry_price_max,
        stake_label=stake_label,
    )
    return {
        "market": cfg.asset.slug,
        "cache_status": str(pool_cache.get("cache_status") or ""),
        "pool_rows": int(pool_cache.get("pool_rows") or 0),
        "feature_rows": int(len(features)),
        "replay_rows": int(replay_summary.merged_rows),
        "quote_ready_rows": int(quote_summary.quote_ready_rows),
        "data_path": str(pool_cache.get("data_path") or ""),
        "manifest_path": str(pool_cache.get("manifest_path") or ""),
    }


def _build_quick_screen_fill_config(
    *,
    stake_label: str,
    stake_usd: float | None = None,
    max_notional_usd: float | None = None,
    parity,
    profile_spec,
):
    label_stake_usd, label_max_notional_usd = _stake_values_from_label(stake_label)
    normalized_parity = parity if isinstance(parity, BacktestParitySpec) else BacktestParitySpec.from_mapping(
        parity if isinstance(parity, dict) else None
    )
    spec = _QuickScreenBacktestSpec(
        stake_usd=stake_usd if stake_usd is not None else label_stake_usd,
        max_notional_usd=(
            max_notional_usd
            if max_notional_usd is not None
            else label_max_notional_usd
        ),
        parity=normalized_parity,
    )
    return _build_backtest_fill_config(spec=spec, profile_spec=profile_spec)


class _QuickScreenBacktestSpec:
    def __init__(
        self,
        *,
        stake_usd: float | None,
        max_notional_usd: float | None,
        parity,
    ) -> None:
        self.stake_usd = stake_usd
        self.max_notional_usd = max_notional_usd
        self.parity = parity


def _stake_values_from_label(stake_label: str) -> tuple[float | None, float | None]:
    text = str(stake_label or "").strip().lower()
    stake_value = _extract_labeled_usd_value(text, labels=("stake_", "stake=", "stake"))
    max_value = _extract_labeled_usd_value(text, labels=("max_", "max=", "max"))
    if stake_value is None:
        stake_value = _extract_labeled_usd_value(text, labels=("",))
    if stake_value is None:
        return None, max_value
    if max_value is None:
        if text in {"1usd", "stake_1usd"}:
            max_value = 3.0
        elif text in {"2usd", "stake_2usd"}:
            max_value = 10.0
        else:
            max_value = stake_value
    return stake_value, max_value


def _extract_labeled_usd_value(text: str, *, labels: tuple[str, ...]) -> float | None:
    for label in labels:
        start = text.find(label)
        if start < 0:
            continue
        tail = text[start + len(label) :]
        usd_index = tail.find("usd")
        if usd_index <= 0:
            continue
        token = tail[:usd_index].strip("_=-")
        if not token:
            continue
        try:
            return float(token.replace("p", "."))
        except ValueError:
            continue
    return None


def _load_quick_screen_feature_frame(
    *,
    cfg: ResearchConfig,
    bundle_dir: Path,
    target: str,
    available_offsets: list[int],
    decision_start: str | None,
    decision_end: str | None,
) -> pd.DataFrame:
    columns = _required_quick_screen_feature_columns(bundle_dir=bundle_dir, target=target)
    features = load_feature_frame(
        cfg,
        feature_set=cfg.feature_set,
        columns=columns,
        filters=_feature_frame_filters(
            decision_start=decision_start,
            decision_end=decision_end,
        ),
    )
    if features.empty:
        return features

    decision_ts = pd.to_datetime(features.get("decision_ts"), utc=True, errors="coerce")
    offset_values = pd.to_numeric(features.get("offset"), errors="coerce")
    mask = decision_ts.notna() & offset_values.isin([int(offset) for offset in available_offsets])

    start_bound = _parse_window_bound(decision_start, is_end=False)
    if start_bound is not None:
        mask &= decision_ts.ge(start_bound)

    end_bound = _parse_window_bound(decision_end, is_end=True)
    if end_bound is not None:
        if _looks_like_date_only(decision_end):
            mask &= decision_ts.lt(end_bound)
        else:
            mask &= decision_ts.le(end_bound)

    return features.loc[mask].reset_index(drop=True)


def _filter_quick_screen_seed_feature_frame(
    *,
    features: pd.DataFrame,
    available_offsets: list[int],
    decision_start: str | None,
    decision_end: str | None,
) -> pd.DataFrame:
    if features.empty:
        return features.copy()

    decision_ts = pd.to_datetime(features.get("decision_ts"), utc=True, errors="coerce")
    offset_values = pd.to_numeric(features.get("offset"), errors="coerce")
    mask = decision_ts.notna()
    if available_offsets:
        mask &= offset_values.isin([int(offset) for offset in available_offsets])

    start_bound = _parse_window_bound(decision_start, is_end=False)
    if start_bound is not None:
        mask &= decision_ts.ge(start_bound)

    end_bound = _parse_window_bound(decision_end, is_end=True)
    if end_bound is not None:
        if _looks_like_date_only(decision_end):
            mask &= decision_ts.lt(end_bound)
        else:
            mask &= decision_ts.le(end_bound)

    return features.loc[mask].reset_index(drop=True)


def _load_quick_screen_label_frame(
    *,
    cfg: ResearchConfig,
    scoped_features: pd.DataFrame,
) -> pd.DataFrame:
    labels = load_label_frame(
        cfg,
        label_set=cfg.label_set,
        columns=_LABEL_REQUIRED_COLUMNS,
        filters=_label_frame_filters(scoped_features=scoped_features),
    )
    if labels.empty or scoped_features.empty:
        return labels.iloc[0:0].copy()

    feature_pairs = scoped_features.loc[:, ["cycle_start_ts", "cycle_end_ts"]].copy()
    feature_pairs["cycle_start_ts"] = pd.to_datetime(feature_pairs["cycle_start_ts"], utc=True, errors="coerce")
    feature_pairs["cycle_end_ts"] = pd.to_datetime(feature_pairs["cycle_end_ts"], utc=True, errors="coerce")
    feature_pairs = feature_pairs.dropna().drop_duplicates().reset_index(drop=True)
    if feature_pairs.empty:
        return labels.iloc[0:0].copy()

    label_start = pd.to_numeric(labels.get("cycle_start_ts"), errors="coerce")
    label_end = pd.to_numeric(labels.get("cycle_end_ts"), errors="coerce")
    valid = label_start.notna() & label_end.notna()
    if not bool(valid.any()):
        return labels.iloc[0:0].copy()

    label_pairs = pd.DataFrame(
        {
            "cycle_start_ts": pd.to_datetime(label_start.loc[valid].astype("int64"), unit="s", utc=True),
            "cycle_end_ts": pd.to_datetime(label_end.loc[valid].astype("int64"), unit="s", utc=True),
        }
    )
    allowed_pairs = pd.MultiIndex.from_frame(feature_pairs)
    keep_mask = pd.Series(False, index=labels.index, dtype=bool)
    keep_mask.loc[label_pairs.index] = pd.MultiIndex.from_frame(label_pairs).isin(allowed_pairs)
    return labels.loc[keep_mask].reset_index(drop=True)


def _required_quick_screen_feature_columns(*, bundle_dir: Path, target: str) -> list[str]:
    out: list[str] = list(_FEATURE_KEY_COLUMNS)
    seen = set(out)
    for offset in _bundle_offsets(bundle_dir):
        bundle_cfg = read_bundle_config(bundle_dir, offset=offset)
        for raw_column in list(bundle_cfg.get("feature_columns") or []):
            column = str(raw_column)
            if not column or column in seen:
                continue
            seen.add(column)
            out.append(column)
    if str(target).strip().lower() == "reversal":
        for column in _REVERSAL_ANCHOR_COLUMNS:
            if column in seen:
                continue
            seen.add(column)
            out.append(column)
    return out


def _parse_window_bound(value: str | None, *, is_end: bool) -> pd.Timestamp | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = pd.Timestamp(text)
    parsed = parsed.tz_localize("UTC") if parsed.tzinfo is None else parsed.tz_convert("UTC")
    if is_end and _looks_like_date_only(text):
        return parsed + pd.Timedelta(days=1)
    return parsed


def _looks_like_date_only(value: str | None) -> bool:
    text = str(value or "").strip()
    return len(text) == 10 and text[4:5] == "-" and text[7:8] == "-"


def ensure_training_and_bundle(
    *,
    cfg: ResearchConfig,
    market_spec,
    training_run_label: str,
    bundle_label: str,
) -> tuple[dict[str, object], dict[str, object]]:
    training_run_dir = cfg.layout.training_run_dir(
        model_family=market_spec.model_family,
        target=market_spec.target,
        run_label_text=training_run_label,
    )
    if (training_run_dir / "summary.json").exists():
        training_summary = read_training_run_summary(training_run_dir)
        train_result = {
            "run_dir": str(training_run_dir),
            "run_label": training_run_label,
            "summary_path": str(training_run_dir / "summary.json"),
            **training_summary,
        }
    else:
        train_result = train_research_run(
            cfg,
            TrainingRunSpec(
                model_family=market_spec.model_family,
                feature_set=market_spec.feature_set,
                label_set=market_spec.label_set,
                target=market_spec.target,
                window=market_spec.window,
                run_label=training_run_label,
                offsets=market_spec.offsets,
                parallel_workers=_quick_screen_training_parallel_workers(),
                weight_variant_label=getattr(market_spec, "weight_variant_label", "default"),
                balance_classes=getattr(market_spec, "balance_classes", None),
                weight_by_vol=getattr(market_spec, "weight_by_vol", None),
                inverse_vol=getattr(market_spec, "inverse_vol", None),
                contrarian_weight=getattr(market_spec, "contrarian_weight", None),
                contrarian_quantile=getattr(market_spec, "contrarian_quantile", None),
                contrarian_return_col=getattr(market_spec, "contrarian_return_col", None),
                winner_in_band_weight=getattr(market_spec, "winner_in_band_weight", None),
                offset_weight_overrides=getattr(market_spec, "offset_weight_overrides", None),
            ),
        )

    bundle_dir = cfg.layout.bundle_dir(
        profile=market_spec.profile,
        target=market_spec.target,
        bundle_label_text=bundle_label,
    )
    if (bundle_dir / "summary.json").exists():
        bundle_summary = read_bundle_summary(bundle_dir)
        bundle_result = {
            "bundle_dir": str(bundle_dir),
            "bundle_label": bundle_label,
            "summary_path": str(bundle_dir / "summary.json"),
            **bundle_summary,
        }
    else:
        bundle_result = build_model_bundle(
            cfg,
            ModelBundleSpec(
                profile=market_spec.profile,
                target=market_spec.target,
                bundle_label=bundle_label,
                offsets=market_spec.offsets,
                source_training_run=training_run_label,
            ),
        )
    return train_result, bundle_result


def _quick_screen_training_parallel_workers() -> int:
    raw = str(os.environ.get("PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS", "") or "").strip()
    if raw:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 3
        return max(1, value)
    return 3


def _quick_screen_compaction_paths(
    *,
    cfg: ResearchConfig,
    market_spec,
    train_result: dict[str, object],
    bundle_result: dict[str, object],
) -> list[Path]:
    paths: list[Path] = []
    if train_result.get("run_dir"):
        paths.append(Path(str(train_result["run_dir"])))
    if bundle_result.get("bundle_dir"):
        paths.append(Path(str(bundle_result["bundle_dir"])))

    feature_set = str(getattr(market_spec, "feature_set", cfg.feature_set) or cfg.feature_set)
    label_set = str(getattr(market_spec, "label_set", cfg.label_set) or cfg.label_set)
    target = str(getattr(market_spec, "target", cfg.target) or cfg.target)
    window = getattr(market_spec, "window", None)
    window_label = str(getattr(window, "label", "") or "")
    offsets = tuple(int(value) for value in getattr(market_spec, "offsets", ()) or ())

    if _env_flag("PM15MIN_QUICK_SCREEN_CLEAN_FEATURE_FRAMES", default=True):
        paths.append(cfg.layout.feature_frame_dir(feature_set, source_surface=cfg.source_surface))

    if window_label and offsets and _env_flag("PM15MIN_QUICK_SCREEN_CLEAN_TRAINING_SETS", default=True):
        for offset in offsets:
            paths.append(
                cfg.layout.training_set_dir(
                    feature_set=feature_set,
                    label_set=label_set,
                    target=target,
                    window=window_label,
                    offset=offset,
                )
            )
    return paths


def _remove_compactable_path(path: Path, *, root: Path) -> bool:
    resolved_root = root.resolve()
    if path == resolved_root or not path.is_relative_to(resolved_root):
        raise ValueError(f"Refusing to compact path outside research root: {path}")
    if not path.exists():
        return False
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()
    return True


def _dedupe_compaction_candidates(paths: list[Path]) -> list[Path]:
    out: list[Path] = []
    seen: set[Path] = set()
    for candidate in paths:
        resolved = Path(candidate).resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        out.append(resolved)
    return out


def _is_compactable_existing_path(path: Path, *, root: Path) -> bool:
    resolved_root = root.resolve()
    if path == resolved_root or not path.is_relative_to(resolved_root):
        return False
    return path.exists()


def _quick_screen_artifact_retention_mode(mode: str | None = None) -> str:
    raw = str(mode if mode is not None else os.environ.get("PM15MIN_QUICK_SCREEN_ARTIFACT_RETENTION", "compact_rejects"))
    normalized = raw.strip().lower().replace("-", "_")
    if normalized in {"", "compact_sparse", "compact_reject", "compact_rejects"}:
        return "compact_rejects"
    if normalized in {"off", "none", "keep_all", "retain_all"}:
        return "retain_all"
    if normalized in {"compact_all", "delete_all"}:
        return "compact_all"
    return "compact_rejects"


def _env_flag(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in {"0", "false", "no", "off", "none"}


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


def _positive_int(value: int | None, *, default: int, allow_zero: bool = False) -> int:
    if value is None:
        value = default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    floor = 0 if allow_zero else 1
    return max(floor, parsed)


def _winner_entry_price(frame: pd.DataFrame, *, winner_side: pd.Series) -> pd.Series:
    up_price = pd.to_numeric(frame.get("quote_up_ask"), errors="coerce")
    down_price = pd.to_numeric(frame.get("quote_down_ask"), errors="coerce")
    out = pd.Series(pd.NA, index=frame.index, dtype="Float64")
    up_mask = winner_side.eq("UP")
    down_mask = winner_side.eq("DOWN")
    out.loc[up_mask] = up_price.loc[up_mask]
    out.loc[down_mask] = down_price.loc[down_mask]
    return pd.to_numeric(out, errors="coerce")


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(False, index=frame.index, dtype="boolean")
    return values.astype("boolean").fillna(False).astype(bool)


def _string_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series("", index=frame.index, dtype="string")
    return values.astype("string").fillna("").astype(str)


def _frame_key_set(frame: pd.DataFrame | None) -> set[tuple[object, ...]]:
    if frame is None or frame.empty:
        return set()
    return set(_frame_key_series(frame).tolist())


def _row_key_membership(frame: pd.DataFrame, keys: set[tuple[object, ...]]) -> pd.Series:
    if not keys:
        return pd.Series(False, index=frame.index, dtype=bool)
    return _frame_key_series(frame).isin(keys)


def _frame_key_series(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series([], index=frame.index, dtype=object)
    columns: dict[str, pd.Series] = {}
    for column in _FEATURE_KEY_COLUMNS:
        if column in {"decision_ts", "cycle_start_ts", "cycle_end_ts"}:
            values = pd.to_datetime(frame.get(column), utc=True, errors="coerce")
            columns[column] = values.map(lambda value: None if pd.isna(value) else value.isoformat())
        elif column == "offset":
            values = pd.to_numeric(frame.get(column), errors="coerce")
            columns[column] = values.map(lambda value: None if pd.isna(value) else int(value))
        else:
            values = frame.get(column, pd.Series(pd.NA, index=frame.index))
            columns[column] = values.map(lambda value: None if pd.isna(value) else value)
    return pd.Series(
        [
            tuple(columns[column].iloc[index] for column in _FEATURE_KEY_COLUMNS)
            for index in range(len(frame))
        ],
        index=frame.index,
        dtype=object,
    )


def _apply_final_trade_captures(
    *,
    profitable_pool_frame: pd.DataFrame,
    decisions: pd.DataFrame,
    final_trades: pd.DataFrame | None,
) -> pd.DataFrame:
    out = profitable_pool_frame.copy()
    final_trade_keys = _frame_key_set(final_trades)
    if not final_trade_keys:
        final_trade_membership = pd.Series(False, index=out.index, dtype=bool)
    else:
        final_trade_membership = _row_key_membership(out, final_trade_keys)

    pool_window = _bool_series(out, "profitable_pool_window")
    correct_side = _bool_series(out, "profitable_pool_correct_side")
    capture = pool_window & correct_side & final_trade_membership
    policy_action = _string_series(out, "policy_action").str.lower()
    if "predicted_side" in out.columns:
        predicted_side = _string_series(out, "predicted_side").str.upper()
    else:
        predicted_side = _string_series(decisions, "predicted_side").str.upper()
    winner_side = _string_series(out, "winner_side").str.upper()
    traded_wrong_side = pool_window & final_trade_membership & predicted_side.ne(winner_side)
    correct_side_no_trade = pool_window & correct_side & ~capture
    missed = pool_window & ~(capture | correct_side_no_trade | traded_wrong_side)

    status = pd.Series("not_in_pool", index=out.index, dtype="string")
    status.loc[missed] = "missed"
    status.loc[correct_side_no_trade] = "correct_side_no_trade"
    status.loc[traded_wrong_side] = "traded_wrong_side"
    status.loc[capture] = "captured"
    out["profitable_pool_capture"] = capture.astype(bool)
    out["profitable_pool_status"] = status.astype(str)
    return out


def _apply_cached_profitable_pool(
    *,
    decisions: pd.DataFrame,
    cached_pool: pd.DataFrame,
) -> pd.DataFrame:
    frame = decisions
    out = pd.DataFrame(index=frame.index)
    for column in _FEATURE_KEY_COLUMNS:
        out[column] = frame[column] if column in frame.columns else pd.NA
    cached = cached_pool.rename(
        columns={
            "winner_side": "_pool_winner_side",
            "winner_entry_price": "_pool_winner_entry_price",
        }
    )
    merged = out.merge(cached, on=list(_FEATURE_KEY_COLUMNS), how="left")
    predicted_side = _string_series(frame, "predicted_side").str.upper()
    policy_action = _string_series(frame, "policy_action").str.lower()
    policy_reason = _string_series(frame, "policy_reason")
    quote_status = _string_series(frame, "quote_status")
    winner_side = _string_series(merged, "_pool_winner_side").str.upper()
    winner_entry_price = pd.to_numeric(merged.get("_pool_winner_entry_price"), errors="coerce")

    profitable_pool_window = winner_side.isin(["UP", "DOWN"]) & winner_entry_price.notna()
    profitable_pool_correct_side = profitable_pool_window & predicted_side.eq(winner_side)
    profitable_pool_capture = profitable_pool_correct_side & policy_action.eq("trade")
    traded_wrong_side = profitable_pool_window & policy_action.eq("trade") & predicted_side.ne(winner_side)
    correct_side_no_trade = profitable_pool_correct_side & ~policy_action.eq("trade")
    missed = profitable_pool_window & ~(profitable_pool_capture | correct_side_no_trade | traded_wrong_side)

    profitable_pool_status = pd.Series("not_in_pool", index=frame.index, dtype="string")
    profitable_pool_status.loc[missed] = "missed"
    profitable_pool_status.loc[correct_side_no_trade] = "correct_side_no_trade"
    profitable_pool_status.loc[traded_wrong_side] = "traded_wrong_side"
    profitable_pool_status.loc[profitable_pool_capture] = "captured"

    merged["winner_side"] = winner_side
    merged["predicted_side"] = predicted_side
    merged["policy_action"] = policy_action
    merged["policy_reason"] = policy_reason
    merged["quote_status"] = quote_status
    merged["winner_entry_price"] = winner_entry_price
    merged["profitable_pool_window"] = profitable_pool_window.astype(bool)
    merged["profitable_pool_correct_side"] = profitable_pool_correct_side.astype(bool)
    merged["profitable_pool_capture"] = profitable_pool_capture.astype(bool)
    merged["profitable_pool_status"] = profitable_pool_status.astype(str)
    return merged
