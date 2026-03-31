from __future__ import annotations

import pandas as pd
import time

from pm15min.research.features.base import decision_reference_ts, normalize_feature_frame, prepare_klines
from pm15min.research.features.cross_asset import append_cross_asset_features
from pm15min.research.features.cycle import append_decision_cycle_metadata
from pm15min.research.features.cycle import append_cycle_features
from pm15min.research.features.price import append_price_features
from pm15min.research.features.registry import feature_group, feature_set_columns
from pm15min.research.features.strike import append_strike_features
from pm15min.research.features.volume import append_volume_features


_META_COLUMNS = [
    "open_time",
    "decision_ts",
    "cycle_start_ts",
    "cycle_end_ts",
    "offset",
    "close",
]


def build_feature_frame(
    raw_klines: pd.DataFrame,
    *,
    feature_set: str,
    oracle_prices: pd.DataFrame,
    btc_klines: pd.DataFrame | None = None,
    cycle: str = "15m",
    requested_columns: set[str] | None = None,
) -> pd.DataFrame:
    build_started = time.perf_counter()
    base = prepare_klines(raw_klines)
    base["decision_ts"] = decision_reference_ts(base)
    required_columns = _resolve_required_feature_columns(
        feature_set=feature_set,
        requested_columns=requested_columns,
    )
    required_groups = {feature_group(column) for column in required_columns}
    timings_ms: dict[str, float] = {}
    frame = base.copy()
    if None in required_groups or "price" in required_groups or "calendar" in required_groups:
        price_started = time.perf_counter()
        frame = append_price_features(frame)
        timings_ms["price_stage_ms"] = _elapsed_ms(price_started)
    if "volume" in required_groups:
        volume_started = time.perf_counter()
        frame = append_volume_features(frame)
        timings_ms["volume_stage_ms"] = _elapsed_ms(volume_started)
    metadata_started = time.perf_counter()
    frame = append_decision_cycle_metadata(frame, cycle=cycle)
    timings_ms["decision_cycle_metadata_stage_ms"] = _elapsed_ms(metadata_started)
    if "cycle" in required_groups:
        cycle_started = time.perf_counter()
        frame = append_cycle_features(frame, cycle=cycle)
        timings_ms["cycle_stage_ms"] = _elapsed_ms(cycle_started)
    if "strike" in required_groups:
        strike_started = time.perf_counter()
        frame = append_strike_features(frame, oracle_prices=oracle_prices, cycle=cycle)
        timings_ms["strike_stage_ms"] = _elapsed_ms(strike_started)
    if "cross_asset" in required_groups:
        cross_asset_started = time.perf_counter()
        frame = append_cross_asset_features(frame, btc_klines=btc_klines)
        timings_ms["cross_asset_stage_ms"] = _elapsed_ms(cross_asset_started)

    meta_columns = set(_META_COLUMNS)
    columns = list(_META_COLUMNS) + [
        column
        for column in frame.columns
        if column not in meta_columns and (feature_group(column) in required_groups or column in required_columns)
    ]
    normalize_started = time.perf_counter()
    out = normalize_feature_frame(frame, columns=columns)
    out = out.sort_values("decision_ts").reset_index(drop=True)
    timings_ms["normalize_stage_ms"] = _elapsed_ms(normalize_started)
    timings_ms["builder_total_stage_ms"] = _elapsed_ms(build_started)
    out.attrs["timings_ms"] = timings_ms
    return out


def resolve_live_required_feature_columns(*, feature_set: str) -> set[str]:
    return _resolve_required_feature_columns(
        feature_set=feature_set,
        requested_columns={
            "ret_15m",
            "ret_30m",
            "ret_from_cycle_open",
            "ret_from_strike",
            "move_z",
            "move_z_strike",
        },
    )


def _resolve_required_feature_columns(
    *,
    feature_set: str,
    requested_columns: set[str] | None,
) -> set[str]:
    required = {str(column) for column in feature_set_columns(feature_set)}
    if requested_columns is not None:
        required.update(str(column) for column in requested_columns)
    # Keep upstream dependencies explicit so selective builders can still
    # materialize downstream columns used by live guards and strike features.
    if required.intersection({"move_z"}):
        required.update({"ret_from_cycle_open", "rv_30"})
    if required.intersection({"cycle_range_vs_rv"}):
        required.update({"rv_30"})
    if required.intersection(
        {
            "ret_from_strike",
            "basis_bp",
            "has_oracle_strike",
            "has_cl_strike",
            "move_z_strike",
            "strike_abs_z",
            "strike_flip_count_cycle",
            "q_bs_up_strike",
            "q_bs_up_strike_centered",
        }
    ):
        required.update({"ret_from_cycle_open", "rv_30"})
    return required


def _elapsed_ms(started_at: float) -> float:
    return round(max(0.0, (time.perf_counter() - float(started_at)) * 1000.0), 3)
