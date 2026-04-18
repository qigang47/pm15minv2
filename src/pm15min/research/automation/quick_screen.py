from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io import write_json_atomic
from pm15min.data.io.parquet import read_parquet_if_exists, write_parquet_atomic
from pm15min.research.bundles.loader import read_bundle_config
from pm15min.research.backtests.decision_engine_parity import (
    apply_decision_engine_parity,
    build_profile_decision_engine_parity_config,
)
from pm15min.research.backtests.engine import _build_bundle_replay, _bundle_offsets, _filter_replay_window
from pm15min.research.backtests.orderbook_surface import attach_canonical_quote_surface
from pm15min.research.backtests.policy import BacktestPolicyConfig, build_policy_decisions, build_policy_reject_frame
from pm15min.research.backtests.regime_parity import resolve_backtest_profile_spec
from pm15min.research.bundles.builder import build_model_bundle
from pm15min.research.bundles.loader import read_bundle_summary, read_training_run_summary
from pm15min.research.config import ResearchConfig
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


def build_profitable_offset_pool_frame(
    decisions: pd.DataFrame,
    *,
    entry_price_min: float | None,
    entry_price_max: float | None,
) -> pd.DataFrame:
    frame = decisions.copy()
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
) -> dict[str, object]:
    frame = decisions.copy()
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
        }

    resolved = _bool_series(frame, "resolved")
    quote_ready = frame.get("quote_status", pd.Series("", index=frame.index, dtype="string")).astype("string").eq("ok")
    winner_side = frame.get("winner_side", pd.Series("", index=frame.index, dtype="string")).astype("string").str.upper()
    predicted_side = frame.get("predicted_side", pd.Series("", index=frame.index, dtype="string")).astype("string").str.upper()
    trade_rows = frame.get("policy_action", pd.Series("", index=frame.index, dtype="string")).astype("string").eq("trade")

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

    rejects = build_policy_reject_frame(frame)
    reject_counts = (
        rejects.get("reason", pd.Series(dtype="string")).astype("string").fillna("").value_counts().sort_index()
    )
    reject_counts = reject_counts[reject_counts.index != ""]

    return {
        "rows": int(len(frame)),
        "resolved_rows": int(resolved.sum()),
        "quote_ready_rows": int(quote_ready.sum()),
        "winner_in_band_rows": int(winner_in_band.sum()),
        "backed_winner_rows": int(backed_winner.sum()),
        "trade_rows": int(trade_rows.sum()),
        "traded_winner_rows": int(traded_winner.sum()),
        "backed_winner_in_band_rows": int((backed_winner & winner_in_band).sum()),
        "traded_winner_in_band_rows": int((traded_winner & winner_in_band).sum()),
        "profitable_pool_rows": profitable_pool_rows,
        "profitable_pool_correct_side_rows": profitable_pool_correct_side_rows,
        "profitable_pool_capture_rows": profitable_pool_capture_rows,
        "profitable_pool_coverage_ratio": profitable_pool_coverage_ratio,
        "profitable_pool_status_counts": profitable_pool_status_counts,
        "reject_reason_counts": {str(index): int(value) for index, value in reject_counts.items()},
    }


def quick_screen_rank_tuple(summary: dict[str, object]) -> tuple[float, int, int, int, int]:
    return (
        float(summary.get("profitable_pool_coverage_ratio") or 0.0),
        int(summary.get("profitable_pool_capture_rows") or 0),
        int(summary.get("profitable_pool_correct_side_rows") or 0),
        int(summary.get("trade_rows") or 0),
        int(summary.get("profitable_pool_rows") or 0),
    )


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
    replay, replay_summary, _available_offsets = _build_bundle_replay(
        bundle_dir=bundle_dir,
        features=features,
        labels=labels,
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
        parity=parity,
    )
    decisions = apply_decision_engine_parity(
        replay,
        config=build_profile_decision_engine_parity_config(
            market=cfg.asset.slug,
            profile_spec=profile_spec,
        ),
        up_price_columns=("quote_up_ask", "quote_prob_up", "p_up"),
        down_price_columns=("quote_down_ask", "quote_prob_down", "p_down"),
    )
    decisions = build_policy_decisions(
        decisions,
        config=BacktestPolicyConfig(prob_floor=0.55, prob_gap_floor=0.0),
        model_source="primary",
    )
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
    summary = build_quick_screen_summary(
        decisions,
        entry_price_min=profile_spec.entry_price_min,
        entry_price_max=profile_spec.entry_price_max,
        profitable_pool_frame=profitable_pool_frame,
    )
    summary.update(
        {
            "replay_rows": int(replay_summary.merged_rows),
            "ready_rows": int(replay_summary.ready_rows),
            "quote_ready_rows_surface": int(quote_summary.quote_ready_rows),
            "quote_missing_rows_surface": int(quote_summary.quote_missing_rows),
            "profitable_pool_cache_status": str(pool_cache.get("cache_status") or ""),
            "profitable_pool_cache_path": str(pool_cache.get("data_path") or ""),
            "profitable_pool_manifest_path": str(pool_cache.get("manifest_path") or ""),
        }
    )
    return summary, decisions


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
    features = load_feature_frame(cfg, feature_set=cfg.feature_set, columns=columns)
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


def _load_quick_screen_label_frame(
    *,
    cfg: ResearchConfig,
    scoped_features: pd.DataFrame,
) -> pd.DataFrame:
    labels = load_label_frame(cfg, label_set=cfg.label_set, columns=_LABEL_REQUIRED_COLUMNS)
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


def _apply_cached_profitable_pool(
    *,
    decisions: pd.DataFrame,
    cached_pool: pd.DataFrame,
) -> pd.DataFrame:
    frame = decisions.copy()
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
