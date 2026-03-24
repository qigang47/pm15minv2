from __future__ import annotations

from dataclasses import dataclass, replace

import numpy as np
import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.live.execution.depth import build_depth_execution_plan, compute_fill_from_depth_record
from pm15min.live.execution.utils import normalize_levels, raw_snapshot_ts_ms
from pm15min.live.execution.policy_helpers import repriced_order_guard, resolve_regime_stake_multiplier
from pm15min.live.profiles.spec import LiveProfileSpec
from pm15min.research.backtests.replay_loader import REPLAY_KEY_COLUMNS
from pm15min.research.backtests.retry_contract import limit_legacy_pre_submit_orderbook_retry_candidates


@dataclass(frozen=True)
class BacktestFillConfig:
    base_stake: float = 1.0
    max_stake: float | None = 3.0
    high_conf_threshold: float = 0.8
    high_conf_multiplier: float = 3.0
    roi_target: float = 0.05
    fee_bps: float = 100.0
    slippage_bps: float = 0.0
    min_edge: float = 0.0
    min_fill_ratio: float = 0.0
    depth_max_slippage_bps: float | None = None
    depth_min_fill_ratio: float | None = None
    raw_depth_time_turnover_gap_ms: int = 30_000
    raw_depth_fak_refresh_enabled: bool = True
    prefer_depth: bool = True
    fill_model: str = "canonical_quote_depth"
    profile_spec: LiveProfileSpec | None = None


@dataclass
class _RawDepthLevelState:
    visible_size: float = 0.0
    credited_size: float = 0.0
    ever_seen: bool = False
    had_attrition: bool = False
    last_credit_ts_ms: int | None = None


def build_fill_plan_frame(
    rows: pd.DataFrame,
    *,
    base_stake: float = 1.0,
    max_stake: float = 3.0,
    min_edge: float = 0.0,
    fee_bps: float = 100.0,
    high_conf_threshold: float = 0.8,
    high_conf_multiplier: float = 3.0,
    roi_target: float = 0.05,
    slippage_bps: float = 0.0,
    fill_model: str = "probability_cap_proxy",
    profile_spec: LiveProfileSpec | None = None,
) -> pd.DataFrame:
    cfg = BacktestFillConfig(
        base_stake=base_stake,
        max_stake=max_stake,
        min_edge=min_edge,
        fee_bps=fee_bps,
        high_conf_threshold=high_conf_threshold,
        high_conf_multiplier=high_conf_multiplier,
        roi_target=roi_target,
        slippage_bps=slippage_bps,
        fill_model=fill_model,
        profile_spec=profile_spec,
    )
    if rows.empty:
        return pd.DataFrame(
            columns=[
                *REPLAY_KEY_COLUMNS,
                "predicted_side",
                "predicted_prob",
                "entry_price",
                "entry_price_source",
                "price_cap",
                "stake_base",
                "stake_multiplier",
                "stake_regime_state",
                "stake",
                "shares",
                "fee_paid",
                "fee_rate",
                "fill_model",
                "fill_valid",
                "fill_reason",
                "depth_status",
                "depth_reason",
                "depth_source_path",
                "depth_fill_ratio",
                "depth_avg_price",
                "depth_best_price",
                "depth_max_price",
                "depth_requested_notional",
                "depth_remaining_notional",
                "depth_levels_available",
                "depth_levels_consumed",
                "depth_partial_fill",
                "depth_stop_reason",
                "depth_price_limit",
                "depth_snapshot_ts_ms",
                "depth_snapshot_age_ms",
                "depth_snapshot_distance_ms",
                "depth_candidate_count",
                "depth_candidate_total_count",
                "depth_candidate_progress_count",
                "depth_chain_mode",
                "depth_queue_turnover_count",
                "depth_time_turnover_count",
                "depth_retry_refresh_count",
                "depth_retry_budget",
                "depth_retry_budget_exhausted",
                "depth_retry_trigger_reason",
                "depth_retry_stage",
                "depth_retry_exit_reason",
                "depth_retry_budget_source",
                "depth_retry_snapshot_unchanged_count",
            ]
        )

    frame = rows.copy()
    frame["p_up"] = pd.to_numeric(frame.get("p_up"), errors="coerce")
    frame["p_down"] = pd.to_numeric(frame.get("p_down"), errors="coerce")
    frame["predicted_side"] = np.where(frame["p_up"].fillna(-1.0) >= frame["p_down"].fillna(-1.0), "UP", "DOWN")
    frame["predicted_prob"] = frame[["p_up", "p_down"]].max(axis=1, skipna=True)
    frame["entry_price"] = _entry_price(frame)
    frame["entry_price_source"] = _entry_price_source(frame)
    frame["price_cap"] = max_price_for_target_roi(
        frame["predicted_prob"],
        roi_target=cfg.roi_target,
        fee_bps=cfg.fee_bps,
        slippage_bps=cfg.slippage_bps,
    )
    frame["probability_edge"] = _resolve_probability_edge(
        frame["predicted_prob"],
        frame["entry_price"],
        frame["entry_price_source"],
    )
    frame["stake_base"] = resolve_stake(frame["predicted_prob"], config=cfg)
    frame["stake_regime_state"] = _resolve_stake_regime_state(frame)
    frame["stake_multiplier"] = _resolve_stake_multiplier(frame, profile_spec=cfg.profile_spec)
    frame["stake"] = _apply_stake_multiplier(
        frame["stake_base"],
        frame["stake_multiplier"],
        max_stake=cfg.max_stake,
    )
    frame["shares"] = np.where(frame["entry_price"].gt(0.0), frame["stake"] / frame["entry_price"], 0.0)
    frame["fee_rate"] = float(cfg.fee_bps) / 10_000.0
    frame["fee_paid"] = frame["stake"] * frame["fee_rate"]
    frame["fill_model"] = str(cfg.fill_model)
    frame["fill_valid"] = True
    frame["fill_reason"] = ""
    frame.loc[frame["predicted_prob"].isna(), "fill_reason"] = "predicted_prob_missing"
    frame.loc[frame["fill_reason"].eq("") & frame["entry_price"].isna(), "fill_reason"] = "quote_missing"
    frame.loc[frame["fill_reason"].eq("") & frame["entry_price"].le(0.0), "fill_reason"] = "quote_invalid"
    frame.loc[frame["fill_reason"].eq("") & frame["probability_edge"].lt(float(cfg.min_edge)), "fill_reason"] = "below_min_edge"
    frame.loc[frame["fill_reason"].ne(""), "fill_valid"] = False
    return frame.reset_index(drop=True)


def build_proxy_fills(
    accepted: pd.DataFrame,
    *,
    config: BacktestFillConfig | None = None,
    profile_spec: LiveProfileSpec | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _build_fills_impl(
        accepted,
        data_cfg=None,
        config=(config if config is not None else BacktestFillConfig(fill_model="probability_cap_proxy", prefer_depth=False, profile_spec=profile_spec)),
        profile_spec=profile_spec,
    )


def build_canonical_fills(
    accepted: pd.DataFrame,
    *,
    data_cfg: DataConfig,
    config: BacktestFillConfig | None = None,
    profile_spec: LiveProfileSpec | None = None,
    depth_replay: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _build_fills_impl(
        accepted,
        data_cfg=data_cfg,
        config=(config if config is not None else BacktestFillConfig(profile_spec=profile_spec)),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )


def _build_fills_impl(
    accepted: pd.DataFrame,
    *,
    data_cfg: DataConfig | None,
    config: BacktestFillConfig,
    profile_spec: LiveProfileSpec | None = None,
    depth_replay: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cfg = config or BacktestFillConfig()
    effective_profile_spec = profile_spec or cfg.profile_spec
    if effective_profile_spec is not cfg.profile_spec:
        cfg = replace(cfg, profile_spec=effective_profile_spec)
    depth_candidates = _build_depth_candidate_lookup(depth_replay)
    planned = build_fill_plan_frame(
        accepted,
        base_stake=cfg.base_stake,
        max_stake=cfg.max_stake,
        min_edge=cfg.min_edge,
        fee_bps=cfg.fee_bps,
        high_conf_threshold=cfg.high_conf_threshold,
        high_conf_multiplier=cfg.high_conf_multiplier,
        roi_target=cfg.roi_target,
        slippage_bps=cfg.slippage_bps,
        fill_model=cfg.fill_model,
        profile_spec=effective_profile_spec,
    )
    if not planned.empty:
        planned = pd.DataFrame(
            [
                _materialize_fill_row(
                    row,
                    data_cfg=data_cfg,
                    config=cfg,
                    raw_depth_candidates=depth_candidates.get(_replay_key_tuple(row)),
                )
                for _, row in planned.iterrows()
            ]
        )
    filled = planned.loc[planned["fill_valid"]].copy().reset_index(drop=True)
    rejected = planned.loc[~planned["fill_valid"], [*REPLAY_KEY_COLUMNS, "fill_reason"]].copy()
    source_rows = planned.loc[~planned["fill_valid"]].copy()
    if "decision_source" in source_rows.columns:
        rejected["decision_source"] = source_rows["decision_source"].astype(str)
    elif "model_source" in source_rows.columns:
        rejected["decision_source"] = source_rows["model_source"].astype(str)
    else:
        rejected["decision_source"] = pd.Series("primary", index=rejected.index, dtype="string").astype(str)
    rejected = rejected.rename(columns={"fill_reason": "reason"}).reset_index(drop=True)
    return filled, rejected


def summarize_fill_reasons(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty or "fill_reason" not in frame.columns:
        return {}
    counts = frame["fill_reason"].astype("string").fillna("").value_counts().sort_index()
    counts = counts[counts.index != ""]
    return {str(reason): int(count) for reason, count in counts.items()}


def max_price_for_target_roi(
    predicted_prob: pd.Series,
    *,
    roi_target: float,
    fee_bps: float,
    slippage_bps: float,
) -> pd.Series:
    p = pd.to_numeric(predicted_prob, errors="coerce").clip(lower=1e-6, upper=1.0)
    slip = max(0.0, float(slippage_bps)) / 10_000.0
    fee_rate = max(0.0, float(fee_bps)) / 10_000.0
    denom = (1.0 + float(roi_target) + fee_rate) * (1.0 + slip)
    return (p / max(denom, 1e-9)).clip(lower=1e-6, upper=1.0)


def resolve_stake(predicted_prob: pd.Series, *, config: BacktestFillConfig) -> pd.Series:
    base = max(0.0, float(config.base_stake))
    cap = max(base, float(config.max_stake)) if config.max_stake is not None else float("inf")
    threshold = float(config.high_conf_threshold)
    multiplier = max(1.0, float(config.high_conf_multiplier))
    prob = pd.to_numeric(predicted_prob, errors="coerce").fillna(0.0)
    scaled = np.where(prob >= threshold, base * multiplier, base)
    return pd.Series(np.minimum(scaled, cap), index=prob.index, dtype=float)


def _resolve_stake_regime_state(frame: pd.DataFrame) -> pd.Series:
    if "regime_state" not in frame.columns:
        return pd.Series("NORMAL", index=frame.index, dtype="string").astype(str)
    states = frame["regime_state"].astype("string").fillna("").astype(str).str.strip().str.upper()
    return states.mask(states.eq(""), "NORMAL").astype(str)


def _resolve_stake_multiplier(
    frame: pd.DataFrame,
    *,
    profile_spec: LiveProfileSpec | None,
) -> pd.Series:
    if profile_spec is None:
        return pd.Series(1.0, index=frame.index, dtype=float)
    regime_states = _resolve_stake_regime_state(frame)
    return pd.Series(
        [
            float(
                resolve_regime_stake_multiplier(
                    spec=profile_spec,
                    regime_state={"state": regime_state},
                )
            )
            for regime_state in regime_states.tolist()
        ],
        index=frame.index,
        dtype=float,
    )


def _apply_stake_multiplier(
    stake_base: pd.Series,
    stake_multiplier: pd.Series,
    *,
    max_stake: float | None,
) -> pd.Series:
    base = pd.to_numeric(stake_base, errors="coerce").fillna(0.0)
    mult = pd.to_numeric(stake_multiplier, errors="coerce").fillna(1.0).clip(lower=0.0)
    scaled = base * mult
    if max_stake is not None:
        scaled = scaled.clip(upper=max(0.0, float(max_stake)))
    return scaled.astype(float)


def _entry_price(frame: pd.DataFrame) -> pd.Series:
    quote_up_ask = _numeric_series(frame, "quote_up_ask")
    quote_down_ask = _numeric_series(frame, "quote_down_ask")
    quote_up = _numeric_series(frame, "quote_prob_up")
    quote_down = _numeric_series(frame, "quote_prob_down")
    p_up = _numeric_series(frame, "p_up")
    p_down = _numeric_series(frame, "p_down")
    predicted_side = _predicted_side_series(frame)
    entry = pd.Series(np.nan, index=frame.index, dtype=float)
    entry.loc[predicted_side == "UP"] = quote_up_ask.loc[predicted_side == "UP"].fillna(quote_up.loc[predicted_side == "UP"]).fillna(p_up.loc[predicted_side == "UP"])
    entry.loc[predicted_side == "DOWN"] = quote_down_ask.loc[predicted_side == "DOWN"].fillna(quote_down.loc[predicted_side == "DOWN"]).fillna(p_down.loc[predicted_side == "DOWN"])
    return entry


def _entry_price_source(frame: pd.DataFrame) -> pd.Series:
    quote_up_ask = _numeric_series(frame, "quote_up_ask")
    quote_down_ask = _numeric_series(frame, "quote_down_ask")
    quote_up = _numeric_series(frame, "quote_prob_up")
    quote_down = _numeric_series(frame, "quote_prob_down")
    predicted_side = _predicted_side_series(frame)
    source = pd.Series("p_up", index=frame.index, dtype="string")
    source.loc[predicted_side == "DOWN"] = "p_down"
    source.loc[(predicted_side == "UP") & quote_up.notna()] = "quote_prob_up"
    source.loc[(predicted_side == "DOWN") & quote_down.notna()] = "quote_prob_down"
    source.loc[(predicted_side == "UP") & quote_up_ask.notna()] = "quote_up_ask"
    source.loc[(predicted_side == "DOWN") & quote_down_ask.notna()] = "quote_down_ask"
    return source.astype(str)


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(values, errors="coerce")


def _resolve_probability_edge(
    predicted_prob: pd.Series,
    entry_price: pd.Series,
    entry_price_source: pd.Series,
) -> pd.Series:
    edge = predicted_prob - entry_price
    fallback_mask = entry_price_source.astype(str).isin({"p_up", "p_down"})
    edge.loc[fallback_mask] = predicted_prob.loc[fallback_mask] - 0.5
    return edge


def _predicted_side_series(frame: pd.DataFrame) -> pd.Series:
    existing = frame.get("predicted_side", pd.Series("", index=frame.index, dtype="string")).astype("string").fillna("")
    normalized = existing.astype(str).str.upper()
    fallback = pd.Series(
        np.where(_numeric_series(frame, "p_up").fillna(-1.0) >= _numeric_series(frame, "p_down").fillna(-1.0), "UP", "DOWN"),
        index=frame.index,
        dtype="string",
    ).astype(str)
    return normalized.where(normalized.isin(["UP", "DOWN"]), fallback)


def _materialize_fill_row(
    row: pd.Series,
    *,
    data_cfg: DataConfig | None,
    config: BacktestFillConfig,
    raw_depth_candidates: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    out = row.to_dict()
    requested_notional = max(0.0, _float_or_none(row.get("stake")) or 0.0)
    entry_price = _float_or_none(row.get("entry_price"))
    price_cap = _float_or_none(row.get("price_cap"))
    entry_price_source = str(row.get("entry_price_source") or "")
    fee_rate = float(config.fee_bps) / 10_000.0

    _apply_depth_diagnostics(out, None, None)
    if not bool(row.get("fill_valid", False)):
        out["fill_model"] = str(config.fill_model)
        return out
    if entry_price is None or entry_price <= 0.0:
        out["fill_valid"] = False
        out["fill_reason"] = "quote_invalid"
        out["fill_model"] = str(config.fill_model)
        return out
    if (
        price_cap is not None
        and price_cap > 0.0
        and entry_price > price_cap
        and entry_price_source not in {"p_up", "p_down"}
    ):
        out["fill_valid"] = False
        out["fill_reason"] = "quote_above_price_cap"
        out["fill_model"] = str(config.fill_model)
        return out

    depth_fill = None
    depth_reason = None
    if data_cfg is not None and bool(config.prefer_depth):
        depth_fill, depth_reason = _resolve_depth_fill(
            row=row,
            data_cfg=data_cfg,
            config=config,
            raw_depth_candidates=raw_depth_candidates,
        )
        _apply_depth_diagnostics(out, depth_fill, depth_reason)
    if isinstance(depth_fill, dict) and str(depth_fill.get("status") or "") == "ok":
        reprice_guard_reason = _resolve_raw_depth_reprice_guard_reason(
            row=row,
            depth_fill=depth_fill,
            config=config,
            raw_depth_candidates=raw_depth_candidates,
        )
        if reprice_guard_reason:
            out["fill_valid"] = False
            out["fill_reason"] = reprice_guard_reason
            out["fill_model"] = "canonical_depth"
            return out
        out.update(
            _build_depth_fill_values(
                depth_fill=depth_fill,
                entry_price=entry_price,
                fee_rate=fee_rate,
                use_conservative_price=bool(raw_depth_candidates and config.raw_depth_fak_refresh_enabled),
            )
        )
        if bool(depth_fill.get("partial_fill")):
            _mark_partial_depth_outcome(out, depth_fill=depth_fill)
            if raw_depth_candidates and bool(config.raw_depth_fak_refresh_enabled):
                return out
            quote_completion = _resolve_quote_fill(
                row=row,
                requested_notional=max(0.0, requested_notional - float(depth_fill["total_cost"])),
                fee_rate=fee_rate,
                config=config,
                min_fill_ratio_override=0.0,
            )
            if bool(quote_completion.get("fill_valid")):
                out.update(
                    _build_depth_quote_fill_values(
                        depth_fill=depth_fill,
                        quote_fill=quote_completion,
                        requested_notional=requested_notional,
                        fee_rate=fee_rate,
                        fallback_entry_price=entry_price,
                    )
                )
        return out

    quote_fill = _resolve_quote_fill(row=row, requested_notional=requested_notional, fee_rate=fee_rate, config=config)
    out.update(quote_fill)
    return out


def _resolve_depth_fill(
    *,
    row: pd.Series,
    data_cfg: DataConfig,
    config: BacktestFillConfig,
    raw_depth_candidates: list[dict[str, object]] | None = None,
) -> tuple[dict[str, object] | None, str | None]:
    quote_row = {
        "decision_ts": row.get("decision_ts"),
        "market_id": row.get("market_id"),
        "token_up": row.get("token_up"),
        "token_down": row.get("token_down"),
        "quote_captured_ts_ms_up": row.get("quote_captured_ts_ms_up"),
        "quote_captured_ts_ms_down": row.get("quote_captured_ts_ms_down"),
    }
    for name in (
        "quote_up_ask",
        "quote_down_ask",
        "quote_up_bid",
        "quote_down_bid",
        "quote_up_ask_size_1",
        "quote_down_ask_size_1",
        "quote_up_bid_size_1",
        "quote_down_bid_size_1",
    ):
        quote_row[name] = row.get(name)
    side = str(row.get("predicted_side") or "").upper()
    requested_notional = max(0.0, _float_or_none(row.get("stake")) or 0.0)
    if side not in {"UP", "DOWN"} or requested_notional <= 0.0:
        return None, "fill_invalid"
    max_slippage_bps, min_fill_ratio = _resolve_depth_constraints(config)
    if raw_depth_candidates:
        return _resolve_raw_depth_fill(
            row=row,
            config=config,
            raw_depth_candidates=raw_depth_candidates,
            side=side,
            requested_notional=requested_notional,
            price_cap=_float_or_none(row.get("price_cap")),
            max_slippage_bps=max_slippage_bps,
            min_fill_ratio=min_fill_ratio,
        )
    return build_depth_execution_plan(
        data_cfg=data_cfg,
        quote_row=quote_row,
        side=side,
        requested_notional=requested_notional,
        price_cap=_float_or_none(row.get("price_cap")),
        max_slippage_bps=max_slippage_bps,
        min_fill_ratio=min_fill_ratio,
    )


def _resolve_quote_fill(
    *,
    row: pd.Series,
    requested_notional: float,
    fee_rate: float,
    config: BacktestFillConfig,
    min_fill_ratio_override: float | None = None,
) -> dict[str, object]:
    entry_price = _float_or_none(row.get("entry_price"))
    price_cap = _float_or_none(row.get("price_cap"))
    entry_price_source = str(row.get("entry_price_source") or "")
    if entry_price is None or entry_price <= 0.0:
        return {
            "fill_valid": False,
            "fill_reason": "quote_invalid",
            "fill_model": "canonical_quote",
        }
    if (
        price_cap is not None
        and price_cap > 0.0
        and entry_price > price_cap
        and entry_price_source not in {"p_up", "p_down"}
    ):
        return {
            "fill_valid": False,
            "fill_reason": "quote_above_price_cap",
            "fill_model": "canonical_quote",
        }
    ask_size = _float_or_none(
        row.get("quote_up_ask_size_1")
        if str(row.get("predicted_side") or "").upper() == "UP"
        else row.get("quote_down_ask_size_1")
    )
    target_shares = requested_notional / entry_price if requested_notional > 0.0 else 0.0
    if ask_size is not None and ask_size > 0.0:
        filled_shares = min(target_shares, ask_size)
    else:
        filled_shares = target_shares
    total_cost = filled_shares * entry_price
    fill_ratio = 0.0 if requested_notional <= 0.0 else total_cost / requested_notional
    if total_cost <= 0.0 or filled_shares <= 0.0:
        return {
            "fill_valid": False,
            "fill_reason": "quote_no_size",
            "fill_model": "canonical_quote",
        }
    min_fill_ratio = float(config.min_fill_ratio if min_fill_ratio_override is None else min_fill_ratio_override)
    if float(fill_ratio) < min_fill_ratio:
        return {
            "fill_valid": False,
            "fill_reason": "quote_fill_ratio_below_threshold",
            "fill_model": "canonical_quote",
        }
    return {
        "stake": float(total_cost),
        "shares": float(filled_shares),
        "fee_rate": float(fee_rate),
        "fee_paid": float(total_cost * fee_rate),
        "fill_ratio": float(fill_ratio),
        "fill_valid": True,
        "fill_reason": "",
        "fill_model": "canonical_quote",
    }


def _resolve_raw_depth_fill(
    *,
    row: pd.Series,
    config: BacktestFillConfig,
    raw_depth_candidates: list[dict[str, object]],
    side: str,
    requested_notional: float,
    price_cap: float | None,
    max_slippage_bps: float,
    min_fill_ratio: float,
) -> tuple[dict[str, object] | None, str | None]:
    if bool(config.raw_depth_fak_refresh_enabled):
        return _resolve_raw_depth_fak_refresh_fill(
            row=row,
            config=config,
            raw_depth_candidates=raw_depth_candidates,
            side=side,
            requested_notional=requested_notional,
            price_cap=price_cap,
            max_slippage_bps=max_slippage_bps,
            min_fill_ratio=min_fill_ratio,
        )
    target_ts_ms = _row_decision_ts_ms(row)
    side_key = "depth_up_record" if side == "UP" else "depth_down_record"
    side_ts_key = "depth_up_snapshot_ts_ms" if side == "UP" else "depth_down_snapshot_ts_ms"
    aggregate_payload: dict[str, object] | None = None
    remaining_notional = float(requested_notional)
    last_payload: dict[str, object] | None = None
    last_reason: str | None = None
    candidate_count = len(raw_depth_candidates)
    candidate_total_count = candidate_count
    progress_count = 0
    queue_turnover_count = 0
    time_turnover_count = 0
    chain_modes: set[str] = set()
    stalled_after_progress = False
    level_states: dict[float, _RawDepthLevelState] = {}
    for candidate in raw_depth_candidates:
        source_path = str(candidate.get("depth_source_path") or "")
        record = candidate.get(side_key)
        snapshot_ts_ms = _int_or_none(candidate.get(side_ts_key))
        if not isinstance(record, dict):
            last_payload = {
                "status": "missing",
                "depth_source_path": source_path,
                **_snapshot_metrics(snapshot_ts_ms=snapshot_ts_ms, target_ts_ms=target_ts_ms),
            }
            last_reason = "depth_snapshot_missing"
            continue
        incremental_record, incremental_meta = _build_incremental_depth_record(
            record=record,
            level_states=level_states,
            snapshot_ts_ms=snapshot_ts_ms,
            max_slippage_bps=max_slippage_bps,
            price_cap=price_cap,
            time_turnover_gap_ms=max(0, int(config.raw_depth_time_turnover_gap_ms)),
        )
        if incremental_record is None:
            last_payload = {
                "status": "blocked",
                "depth_source_path": source_path,
                "price_cap": price_cap,
                **_snapshot_metrics(snapshot_ts_ms=raw_snapshot_ts_ms(record), target_ts_ms=target_ts_ms),
            }
            last_reason = str(incremental_meta.get("reason") or "depth_fill_unavailable")
            stalled_after_progress = stalled_after_progress or (progress_count > 0 and last_reason == "queue_path_stalled")
            continue
        fill = compute_fill_from_depth_record(
            record=incremental_record,
            target_notional=remaining_notional,
            max_slippage_bps=max_slippage_bps,
            price_cap=price_cap,
        )
        if fill is None:
            last_payload = {
                "status": "blocked",
                "depth_source_path": source_path,
                "price_cap": price_cap,
                **_snapshot_metrics(snapshot_ts_ms=raw_snapshot_ts_ms(record), target_ts_ms=target_ts_ms),
            }
            last_reason = "depth_fill_unavailable"
            continue
        if progress_count > 0:
            chain_modes.update(incremental_meta.get("progress_modes") or set())
        if bool(incremental_meta.get("queue_turnover")):
            queue_turnover_count += 1
        if bool(incremental_meta.get("time_turnover")):
            time_turnover_count += 1
        payload = {
            "status": "ok",
            "depth_source_path": source_path,
            "price_cap": price_cap,
            **_snapshot_metrics(snapshot_ts_ms=raw_snapshot_ts_ms(record), target_ts_ms=target_ts_ms),
            **fill,
        }
        aggregate_payload = _accumulate_depth_fill_payload(
            aggregate_payload,
            payload=payload,
            requested_notional=requested_notional,
        )
        progress_count += 1
        stalled_after_progress = False
        remaining_notional = max(0.0, float(aggregate_payload["remaining_notional"]))
        last_payload = aggregate_payload
        last_reason = None
        if remaining_notional <= 1e-10:
            return _finalize_raw_depth_payload(
                aggregate_payload,
                candidate_count=candidate_count,
                candidate_total_count=candidate_total_count,
                progress_count=progress_count,
                chain_modes=chain_modes,
                stalled_after_progress=stalled_after_progress,
                queue_turnover_count=queue_turnover_count,
                time_turnover_count=time_turnover_count,
            ), None
    if aggregate_payload is not None:
        aggregate_payload = _finalize_raw_depth_payload(
            aggregate_payload,
            candidate_count=candidate_count,
            candidate_total_count=candidate_total_count,
            progress_count=progress_count,
            chain_modes=chain_modes,
            stalled_after_progress=stalled_after_progress,
            queue_turnover_count=queue_turnover_count,
            time_turnover_count=time_turnover_count,
        )
        if float(aggregate_payload["fill_ratio"]) < float(min_fill_ratio):
            blocked_payload = dict(aggregate_payload)
            blocked_payload["status"] = "blocked"
            return blocked_payload, "depth_fill_ratio_below_threshold"
        return aggregate_payload, None
    if last_payload is not None:
        last_payload = _finalize_raw_depth_payload(
            last_payload,
            candidate_count=candidate_count,
            candidate_total_count=candidate_total_count,
            progress_count=progress_count,
            chain_modes=chain_modes,
            stalled_after_progress=stalled_after_progress,
            queue_turnover_count=queue_turnover_count,
            time_turnover_count=time_turnover_count,
        )
    return last_payload, last_reason


def _resolve_raw_depth_fak_refresh_fill(
    *,
    row: pd.Series,
    config: BacktestFillConfig,
    raw_depth_candidates: list[dict[str, object]],
    side: str,
    requested_notional: float,
    price_cap: float | None,
    max_slippage_bps: float,
    min_fill_ratio: float,
) -> tuple[dict[str, object] | None, str | None]:
    target_ts_ms = _row_decision_ts_ms(row)
    side_key = "depth_up_record" if side == "UP" else "depth_down_record"
    side_ts_key = "depth_up_snapshot_ts_ms" if side == "UP" else "depth_down_snapshot_ts_ms"
    limited_candidates, retry_budget_meta = limit_legacy_pre_submit_orderbook_retry_candidates(
        raw_depth_candidates,
        spec=config.profile_spec,
    )
    candidate_count = len(limited_candidates)
    candidate_total_count = int(retry_budget_meta.get("candidate_total_count") or candidate_count)
    retry_budget = int(retry_budget_meta.get("retry_budget") or candidate_count)
    retry_budget_exhausted = bool(retry_budget_meta.get("budget_exhausted", False))
    retry_budget_source = str(retry_budget_meta.get("retry_budget_source") or "")
    last_payload: dict[str, object] | None = None
    last_reason: str | None = None
    retry_trigger_reason = ""
    snapshot_unchanged_count = 0
    last_marker: tuple[object, ...] | None = None

    for attempt_index, candidate in enumerate(limited_candidates, start=1):
        source_path = str(candidate.get("depth_source_path") or "")
        record = candidate.get(side_key)
        snapshot_ts_ms = _int_or_none(candidate.get(side_ts_key))
        marker = _retry_snapshot_marker(candidate)
        if attempt_index > 1 and marker is not None and marker == last_marker:
            snapshot_unchanged_count += 1
            if last_payload is None:
                last_payload = {
                    "status": "blocked",
                    "depth_source_path": source_path,
                    "price_cap": price_cap,
                    **_snapshot_metrics(snapshot_ts_ms=snapshot_ts_ms, target_ts_ms=target_ts_ms),
                }
            last_payload = _finalize_fak_refresh_payload(
                last_payload,
                candidate_count=candidate_count,
                candidate_total_count=candidate_total_count,
                attempt_count=max(1, attempt_index - 1),
                retry_budget=retry_budget,
                retry_budget_exhausted=retry_budget_exhausted,
                retry_trigger_reason=retry_trigger_reason,
                retry_stage="pre_submit_orderbook_recheck",
                retry_exit_reason="orderbook_snapshot_unchanged",
                retry_budget_source=retry_budget_source,
                retry_snapshot_unchanged_count=snapshot_unchanged_count,
            )
            return last_payload, "orderbook_snapshot_unchanged"
        if marker is not None:
            last_marker = marker
        snapshot_payload = {
            "depth_source_path": source_path,
            "price_cap": price_cap,
            **_snapshot_metrics(snapshot_ts_ms=snapshot_ts_ms, target_ts_ms=target_ts_ms),
        }
        if not isinstance(record, dict):
            last_payload = _finalize_fak_refresh_payload(
                {
                    "status": "missing",
                    **snapshot_payload,
                },
                candidate_count=candidate_count,
                candidate_total_count=candidate_total_count,
                attempt_count=attempt_index,
                retry_budget=retry_budget,
                retry_budget_exhausted=retry_budget_exhausted,
                retry_trigger_reason=retry_trigger_reason,
                retry_stage="pre_submit_orderbook_recheck",
                retry_exit_reason="depth_snapshot_missing",
                retry_budget_source=retry_budget_source,
                retry_snapshot_unchanged_count=snapshot_unchanged_count,
            )
            last_reason = "depth_snapshot_missing"
            if not retry_trigger_reason:
                retry_trigger_reason = _normalize_retry_trigger_reason(last_reason)
            continue

        fill = compute_fill_from_depth_record(
            record=record,
            target_notional=requested_notional,
            max_slippage_bps=max_slippage_bps,
            price_cap=price_cap,
        )
        if fill is None:
            last_payload = _finalize_fak_refresh_payload(
                {
                    "status": "blocked",
                    **snapshot_payload,
                },
                candidate_count=candidate_count,
                candidate_total_count=candidate_total_count,
                attempt_count=attempt_index,
                retry_budget=retry_budget,
                retry_budget_exhausted=retry_budget_exhausted,
                retry_trigger_reason=retry_trigger_reason,
                retry_stage="pre_submit_orderbook_recheck",
                retry_exit_reason="depth_fill_unavailable",
                retry_budget_source=retry_budget_source,
                retry_snapshot_unchanged_count=snapshot_unchanged_count,
            )
            last_reason = "depth_fill_unavailable"
            if not retry_trigger_reason:
                retry_trigger_reason = _normalize_retry_trigger_reason(last_reason)
            continue

        payload = _accumulate_depth_fill_payload(
            None,
            payload={
                "status": "ok",
                **snapshot_payload,
                **fill,
            },
            requested_notional=requested_notional,
        )
        finalized = _finalize_fak_refresh_payload(
            payload,
            candidate_count=candidate_count,
            candidate_total_count=candidate_total_count,
            attempt_count=attempt_index,
            retry_budget=retry_budget,
            retry_budget_exhausted=retry_budget_exhausted,
            retry_trigger_reason=retry_trigger_reason,
            retry_stage="pre_submit_orderbook_recheck",
            retry_exit_reason="filled_target",
            retry_budget_source=retry_budget_source,
            retry_snapshot_unchanged_count=snapshot_unchanged_count,
        )
        if float(finalized["fill_ratio"]) < float(min_fill_ratio):
            finalized["status"] = "blocked"
            finalized["retry_exit_reason"] = "depth_fill_ratio_below_threshold"
            last_payload = finalized
            last_reason = "depth_fill_ratio_below_threshold"
            if not retry_trigger_reason:
                retry_trigger_reason = _normalize_retry_trigger_reason(last_reason)
            continue
        return finalized, None

    if retry_budget_exhausted and last_payload is not None:
        last_payload["retry_exit_reason"] = "retry_budget_exhausted"
        return last_payload, last_reason
    return last_payload, last_reason


def _build_depth_fill_values(
    *,
    depth_fill: dict[str, object],
    entry_price: float | None,
    fee_rate: float,
    use_conservative_price: bool = False,
) -> dict[str, object]:
    total_cost = float(depth_fill["total_cost"])
    effective_entry_price = float(depth_fill.get("avg_price") or entry_price or 0.0)
    if use_conservative_price:
        conservative_price = float(depth_fill.get("max_price") or effective_entry_price or 0.0)
        if conservative_price > 0.0:
            effective_entry_price = conservative_price
    shares = float(depth_fill["filled_shares"])
    if effective_entry_price > 0.0:
        shares = float(total_cost / effective_entry_price)
    return {
        "entry_price": float(effective_entry_price),
        "stake": total_cost,
        "shares": shares,
        "fee_rate": fee_rate,
        "fee_paid": total_cost * fee_rate,
        "fill_ratio": float(depth_fill["fill_ratio"]),
        "fill_model": "canonical_depth",
        "fill_reason": "",
        "fill_valid": True,
    }


def _resolve_raw_depth_reprice_guard_reason(
    *,
    row: pd.Series,
    depth_fill: dict[str, object],
    config: BacktestFillConfig,
    raw_depth_candidates: list[dict[str, object]] | None,
) -> str | None:
    if not raw_depth_candidates or not bool(config.raw_depth_fak_refresh_enabled):
        return None
    if (_int_or_none(depth_fill.get("retry_refresh_count")) or 0) <= 0:
        return None
    profile_spec = config.profile_spec
    if profile_spec is None:
        return None
    repriced_entry_price = _float_or_none(depth_fill.get("max_price")) or _float_or_none(depth_fill.get("avg_price"))
    if repriced_entry_price is None or repriced_entry_price <= 0.0:
        return None
    _metrics, reasons = repriced_order_guard(
        spec=profile_spec,
        selected_row={
            "recommended_side": str(row.get("predicted_side") or "").upper(),
            "offset": _int_or_none(row.get("offset")) or 0,
            "p_up": _float_or_none(row.get("p_up")),
            "p_down": _float_or_none(row.get("p_down")),
        },
        repriced_entry_price=repriced_entry_price,
    )
    if not reasons:
        return None
    return str(reasons[0] or "").strip() or None


def _build_depth_quote_fill_values(
    *,
    depth_fill: dict[str, object],
    quote_fill: dict[str, object],
    requested_notional: float,
    fee_rate: float,
    fallback_entry_price: float | None,
) -> dict[str, object]:
    depth_cost = float(depth_fill["total_cost"])
    depth_shares = float(depth_fill["filled_shares"])
    quote_cost = float(quote_fill["stake"])
    quote_shares = float(quote_fill["shares"])
    total_cost = depth_cost + quote_cost
    total_shares = depth_shares + quote_shares
    entry_price = fallback_entry_price if fallback_entry_price is not None else 0.0
    if total_shares > 0.0:
        entry_price = total_cost / total_shares
    return {
        "entry_price": float(entry_price),
        "stake": float(total_cost),
        "shares": float(total_shares),
        "fee_rate": float(fee_rate),
        "fee_paid": float(total_cost * fee_rate),
        "fill_ratio": 0.0 if requested_notional <= 0.0 else float(total_cost / requested_notional),
        "fill_model": "canonical_depth_quote",
        "fill_reason": "",
        "fill_valid": True,
    }


def _mark_partial_depth_outcome(out: dict[str, object], *, depth_fill: dict[str, object]) -> None:
    stop_reason = str(depth_fill.get("stop_reason") or "partial_fill")
    out["depth_status"] = "partial"
    out["depth_reason"] = stop_reason


def _accumulate_depth_fill_payload(
    aggregate: dict[str, object] | None,
    *,
    payload: dict[str, object],
    requested_notional: float,
) -> dict[str, object]:
    total_cost = float(payload.get("total_cost") or 0.0)
    total_shares = float(payload.get("filled_shares") or 0.0)
    levels_available = _int_or_none(payload.get("levels_available")) or 0
    levels_consumed = _int_or_none(payload.get("levels_consumed")) or 0
    best_prices = [_float_or_none(payload.get("best_price"))]
    max_prices = [_float_or_none(payload.get("max_price"))]
    if aggregate is not None:
        total_cost += float(aggregate.get("total_cost") or 0.0)
        total_shares += float(aggregate.get("filled_shares") or 0.0)
        levels_available += _int_or_none(aggregate.get("levels_available")) or 0
        levels_consumed += _int_or_none(aggregate.get("levels_consumed")) or 0
        best_prices.append(_float_or_none(aggregate.get("best_price")))
        max_prices.append(_float_or_none(aggregate.get("max_price")))
    remaining_notional = max(0.0, float(requested_notional) - total_cost)
    best_price = min(price for price in best_prices if price is not None) if any(price is not None for price in best_prices) else None
    max_price = max(price for price in max_prices if price is not None) if any(price is not None for price in max_prices) else None
    return {
        "status": "ok",
        "depth_source_path": payload.get("depth_source_path") or (aggregate or {}).get("depth_source_path"),
        "price_cap": payload.get("price_cap") if payload.get("price_cap") is not None else (aggregate or {}).get("price_cap"),
        "snapshot_ts_ms": _first_non_none(
            _int_or_none(payload.get("snapshot_ts_ms")),
            _int_or_none((aggregate or {}).get("snapshot_ts_ms")),
        ),
        "snapshot_age_ms": _first_non_none(
            _int_or_none(payload.get("snapshot_age_ms")),
            _int_or_none((aggregate or {}).get("snapshot_age_ms")),
        ),
        "snapshot_distance_ms": _first_non_none(
            _int_or_none(payload.get("snapshot_distance_ms")),
            _int_or_none((aggregate or {}).get("snapshot_distance_ms")),
        ),
        "filled_shares": float(total_shares),
        "total_cost": float(total_cost),
        "max_price": None if max_price is None else float(max_price),
        "best_price": None if best_price is None else float(best_price),
        "fill_ratio": 0.0 if requested_notional <= 0.0 else float(total_cost / requested_notional),
        "avg_price": None if total_shares <= 0.0 else float(total_cost / total_shares),
        "requested_notional": float(requested_notional),
        "remaining_notional": float(remaining_notional),
        "levels_available": int(levels_available),
        "levels_consumed": int(levels_consumed),
        "partial_fill": bool(remaining_notional > 1e-10),
        "stop_reason": "filled_target" if remaining_notional <= 1e-10 else str(payload.get("stop_reason") or "depth_exhausted"),
        "price_limit": _first_non_none(
            _float_or_none(payload.get("price_limit")),
            _float_or_none((aggregate or {}).get("price_limit")),
        ),
    }


def _build_incremental_depth_record(
    *,
    record: dict[str, object],
    level_states: dict[float, _RawDepthLevelState],
    snapshot_ts_ms: int | None,
    max_slippage_bps: float,
    price_cap: float | None,
    time_turnover_gap_ms: int,
) -> tuple[dict[str, object] | None, dict[str, object]]:
    asks = normalize_levels(record.get("asks"))
    price_limit = _depth_price_limit(asks=asks, max_slippage_bps=max_slippage_bps, price_cap=price_cap)
    if not asks or price_limit is None:
        return None, {
            "reason": "depth_fill_unavailable",
            "progress_modes": set(),
            "visible_level_sizes": {},
            "queue_turnover": False,
            "time_turnover": False,
        }
    delta_levels: list[list[float]] = []
    progress_modes: set[str] = set()
    visible_level_sizes: dict[float, float] = {}
    queue_turnover = False
    time_turnover = False
    for price, size in asks:
        if price > price_limit:
            break
        key = _depth_price_key(price)
        size = float(size)
        visible_level_sizes[key] = max(size, float(visible_level_sizes.get(key, 0.0)))
    for key, state in level_states.items():
        current_visible = float(visible_level_sizes.get(key, 0.0))
        if current_visible < state.visible_size - 1e-10:
            state.credited_size = max(0.0, state.credited_size - (state.visible_size - current_visible))
            state.had_attrition = True
        state.visible_size = current_visible
    for key, size in visible_level_sizes.items():
        state = level_states.setdefault(key, _RawDepthLevelState())
        state.visible_size = float(size)
        delta_size = max(0.0, float(size) - state.credited_size)
        used_time_turnover = False
        if (
            delta_size <= 1e-10
            and state.ever_seen
            and size > 0.0
            and snapshot_ts_ms is not None
            and time_turnover_gap_ms > 0
            and state.last_credit_ts_ms is not None
            and int(snapshot_ts_ms) - int(state.last_credit_ts_ms) >= int(time_turnover_gap_ms)
        ):
            delta_size = float(size)
            used_time_turnover = True
        if delta_size <= 1e-10:
            state.ever_seen = True
            continue
        delta_levels.append([float(key), float(delta_size)])
        if used_time_turnover:
            progress_modes.add("time_turnover")
            time_turnover = True
        elif state.ever_seen:
            progress_modes.add("queue_growth")
            queue_turnover = queue_turnover or bool(state.had_attrition)
        else:
            progress_modes.add("price_path")
        state.credited_size = float(size)
        state.ever_seen = True
        state.had_attrition = False
        state.last_credit_ts_ms = snapshot_ts_ms
    if not delta_levels:
        return None, {
            "reason": "queue_path_stalled",
            "progress_modes": set(),
            "visible_level_sizes": visible_level_sizes,
            "queue_turnover": False,
            "time_turnover": False,
        }
    return {
        **record,
        "asks": delta_levels,
    }, {
        "reason": None,
        "progress_modes": progress_modes,
        "visible_level_sizes": visible_level_sizes,
        "queue_turnover": queue_turnover,
        "time_turnover": time_turnover,
    }


def _depth_price_limit(
    *,
    asks: list[tuple[float, float]],
    max_slippage_bps: float,
    price_cap: float | None,
) -> float | None:
    if not asks:
        return None
    best_price = float(asks[0][0])
    if best_price <= 0.0:
        return None
    if price_cap is not None and price_cap > 0.0 and best_price > price_cap:
        return None
    slip_factor = max(0.0, float(max_slippage_bps)) / 10_000.0
    price_limit = best_price * (1.0 + slip_factor)
    if price_cap is not None and price_cap > 0.0:
        price_limit = min(price_limit, float(price_cap))
    return float(price_limit)


def _finalize_raw_depth_payload(
    payload: dict[str, object],
    *,
    candidate_count: int,
    candidate_total_count: int,
    progress_count: int,
    chain_modes: set[str],
    stalled_after_progress: bool,
    queue_turnover_count: int,
    time_turnover_count: int,
) -> dict[str, object]:
    out = dict(payload)
    out["candidate_count"] = int(candidate_count)
    out["candidate_total_count"] = int(candidate_total_count)
    out["candidate_progress_count"] = int(progress_count)
    out["chain_mode"] = _resolve_depth_chain_mode(progress_count=progress_count, chain_modes=chain_modes)
    out["queue_turnover_count"] = int(queue_turnover_count)
    out["time_turnover_count"] = int(time_turnover_count)
    out["retry_budget"] = None
    out["retry_budget_exhausted"] = False
    out["retry_stage"] = ""
    out["retry_exit_reason"] = ""
    out["retry_budget_source"] = ""
    out["retry_snapshot_unchanged_count"] = 0
    if stalled_after_progress and bool(out.get("partial_fill")):
        out["stop_reason"] = "queue_path_stalled"
    return out


def _finalize_fak_refresh_payload(
    payload: dict[str, object],
    *,
    candidate_count: int,
    candidate_total_count: int,
    attempt_count: int,
    retry_budget: int,
    retry_budget_exhausted: bool,
    retry_trigger_reason: str,
    retry_stage: str,
    retry_exit_reason: str,
    retry_budget_source: str,
    retry_snapshot_unchanged_count: int,
) -> dict[str, object]:
    out = dict(payload)
    out["candidate_count"] = int(candidate_count)
    out["candidate_total_count"] = int(candidate_total_count)
    out["candidate_progress_count"] = int(attempt_count)
    out["chain_mode"] = "single_snapshot" if attempt_count <= 1 else "refresh_retry"
    out["queue_turnover_count"] = 0
    out["time_turnover_count"] = 0
    out["retry_refresh_count"] = max(0, int(attempt_count) - 1)
    out["retry_budget"] = int(retry_budget)
    out["retry_budget_exhausted"] = bool(retry_budget_exhausted)
    out["retry_trigger_reason"] = str(retry_trigger_reason or "")
    out["retry_stage"] = str(retry_stage or "")
    out["retry_exit_reason"] = str(retry_exit_reason or "")
    out["retry_budget_source"] = str(retry_budget_source or "")
    out["retry_snapshot_unchanged_count"] = int(retry_snapshot_unchanged_count)
    return out


def _resolve_depth_chain_mode(*, progress_count: int, chain_modes: set[str]) -> str:
    if progress_count <= 1 or not chain_modes:
        return "single_snapshot"
    if chain_modes == {"time_turnover"}:
        return "time_turnover"
    if chain_modes == {"queue_growth"}:
        return "queue_growth"
    if chain_modes == {"price_path"}:
        return "price_path"
    if chain_modes == {"queue_growth", "time_turnover"}:
        return "queue_time_turnover"
    if chain_modes == {"price_path", "time_turnover"}:
        return "price_time_turnover"
    if chain_modes == {"queue_growth", "price_path", "time_turnover"}:
        return "queue_price_time_turnover"
    return "queue_price_path"


def _depth_price_key(price: float) -> float:
    return round(float(price), 10)


def _float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


def _resolve_depth_constraints(config: BacktestFillConfig) -> tuple[float, float]:
    profile_spec = config.profile_spec
    if config.depth_max_slippage_bps is not None:
        max_slippage_bps = float(config.depth_max_slippage_bps)
    elif profile_spec is not None:
        max_slippage_bps = float(profile_spec.orderbook_max_slippage_bps)
    else:
        max_slippage_bps = float(config.slippage_bps)

    if config.depth_min_fill_ratio is not None:
        min_fill_ratio = float(config.depth_min_fill_ratio)
    elif profile_spec is not None:
        min_fill_ratio = float(profile_spec.orderbook_min_fill_ratio)
    else:
        min_fill_ratio = float(config.min_fill_ratio)
    return max(0.0, max_slippage_bps), max(0.0, min_fill_ratio)


def _build_depth_candidate_lookup(depth_replay: pd.DataFrame | None) -> dict[tuple[object, ...], list[dict[str, object]]]:
    if depth_replay is None or depth_replay.empty:
        return {}
    frame = depth_replay.copy()
    if "depth_snapshot_rank" in frame.columns:
        frame["depth_snapshot_rank"] = pd.to_numeric(frame["depth_snapshot_rank"], errors="coerce")
        frame = frame.sort_values([*REPLAY_KEY_COLUMNS, "depth_snapshot_rank"], kind="stable", na_position="last")
    lookup: dict[tuple[object, ...], list[dict[str, object]]] = {}
    for record in frame.to_dict(orient="records"):
        lookup.setdefault(_replay_key_tuple(record), []).append(record)
    return lookup


def _replay_key_tuple(row: pd.Series | dict[str, object]) -> tuple[object, ...]:
    out: list[object] = []
    for column in REPLAY_KEY_COLUMNS:
        value = row.get(column) if isinstance(row, dict) else row.get(column)
        if column == "offset":
            out.append(_int_or_none(value))
        else:
            ts = pd.to_datetime(value, utc=True, errors="coerce")
            out.append(None if pd.isna(ts) else pd.Timestamp(ts).isoformat())
    return tuple(out)


def _row_decision_ts_ms(row: pd.Series) -> int | None:
    ts = pd.to_datetime(row.get("decision_ts"), utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return int(pd.Timestamp(ts).timestamp() * 1000)


def _snapshot_metrics(*, snapshot_ts_ms: int | None, target_ts_ms: int | None) -> dict[str, int | None]:
    if snapshot_ts_ms is None:
        return {
            "snapshot_ts_ms": None,
            "snapshot_age_ms": None,
            "snapshot_distance_ms": None,
        }
    age_ms = None if target_ts_ms is None else int(target_ts_ms) - int(snapshot_ts_ms)
    distance_ms = None if target_ts_ms is None else abs(int(target_ts_ms) - int(snapshot_ts_ms))
    return {
        "snapshot_ts_ms": int(snapshot_ts_ms),
        "snapshot_age_ms": age_ms,
        "snapshot_distance_ms": distance_ms,
    }


def _apply_depth_diagnostics(
    out: dict[str, object],
    depth_fill: dict[str, object] | None,
    depth_reason: str | None,
) -> None:
    payload = dict(depth_fill or {})
    status = str(payload.get("status") or ("skipped" if depth_reason else ""))
    out["depth_status"] = status
    out["depth_reason"] = str(depth_reason or "")
    out["depth_source_path"] = payload.get("depth_source_path")
    out["depth_fill_ratio"] = _float_or_none(payload.get("fill_ratio"))
    out["depth_avg_price"] = _float_or_none(payload.get("avg_price"))
    out["depth_best_price"] = _float_or_none(payload.get("best_price"))
    out["depth_max_price"] = _float_or_none(payload.get("max_price"))
    out["depth_requested_notional"] = _float_or_none(payload.get("requested_notional"))
    out["depth_remaining_notional"] = _float_or_none(payload.get("remaining_notional"))
    out["depth_levels_available"] = _int_or_none(payload.get("levels_available"))
    out["depth_levels_consumed"] = _int_or_none(payload.get("levels_consumed"))
    out["depth_partial_fill"] = _bool_or_none(payload.get("partial_fill"))
    out["depth_stop_reason"] = str(payload.get("stop_reason") or "")
    out["depth_price_limit"] = _float_or_none(payload.get("price_limit"))
    out["depth_snapshot_ts_ms"] = _int_or_none(payload.get("snapshot_ts_ms"))
    out["depth_snapshot_age_ms"] = _int_or_none(payload.get("snapshot_age_ms"))
    out["depth_snapshot_distance_ms"] = _int_or_none(payload.get("snapshot_distance_ms"))
    out["depth_candidate_count"] = _int_or_none(payload.get("candidate_count"))
    out["depth_candidate_total_count"] = _int_or_none(payload.get("candidate_total_count"))
    out["depth_candidate_progress_count"] = _int_or_none(payload.get("candidate_progress_count"))
    out["depth_chain_mode"] = str(payload.get("chain_mode") or "")
    out["depth_queue_turnover_count"] = _int_or_none(payload.get("queue_turnover_count"))
    out["depth_time_turnover_count"] = _int_or_none(payload.get("time_turnover_count"))
    out["depth_retry_refresh_count"] = _int_or_none(payload.get("retry_refresh_count"))
    out["depth_retry_budget"] = _int_or_none(payload.get("retry_budget"))
    out["depth_retry_budget_exhausted"] = _bool_or_none(payload.get("retry_budget_exhausted"))
    out["depth_retry_trigger_reason"] = str(payload.get("retry_trigger_reason") or "")
    out["depth_retry_stage"] = str(payload.get("retry_stage") or "")
    out["depth_retry_exit_reason"] = str(payload.get("retry_exit_reason") or "")
    out["depth_retry_budget_source"] = str(payload.get("retry_budget_source") or "")
    out["depth_retry_snapshot_unchanged_count"] = _int_or_none(payload.get("retry_snapshot_unchanged_count"))


def _int_or_none(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _bool_or_none(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    token = str(value).strip().lower()
    if token in {"true", "1", "yes", "on"}:
        return True
    if token in {"false", "0", "no", "off"}:
        return False
    return None


def _first_non_none(*values: object) -> object:
    for value in values:
        if value is not None:
            return value
    return None


def _normalize_retry_trigger_reason(reason: str | None) -> str:
    token = str(reason or "").strip()
    if token == "depth_fill_ratio_below_threshold":
        return "depth_fill_unavailable"
    return token


def _retry_snapshot_marker(candidate: dict[str, object]) -> tuple[object, ...] | None:
    up_marker = _first_non_none(
        _int_or_none(candidate.get("depth_up_snapshot_ts_ms")),
        _record_snapshot_marker(candidate.get("depth_up_record")),
    )
    down_marker = _first_non_none(
        _int_or_none(candidate.get("depth_down_snapshot_ts_ms")),
        _record_snapshot_marker(candidate.get("depth_down_record")),
    )
    if up_marker is None and down_marker is None:
        return None
    return (up_marker, down_marker)


def _record_snapshot_marker(record: object) -> object:
    if not isinstance(record, dict):
        return None
    snapshot_ts = raw_snapshot_ts_ms(record)
    if snapshot_ts is not None:
        return int(snapshot_ts)
    asks = normalize_levels(record.get("asks"))
    if not asks:
        return None
    top_levels = tuple((round(float(price), 10), round(float(size), 10)) for price, size in asks[:3])
    return top_levels
