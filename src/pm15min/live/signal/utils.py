from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.queries.loaders import load_binance_klines_1m, load_oracle_prices_table
from pm15min.live.oracle import build_live_runtime_oracle_prices
from ..account import summarize_account_state_payload
from ..account import load_latest_open_orders_snapshot, load_latest_positions_snapshot
from ..layout import LiveStateLayout
from ..persistence import write_live_payload_pair
from pm15min.research.features.builders import build_feature_frame as build_feature_frame_df
from pm15min.research.features.registry import feature_set_columns


def supports_feature_set(feature_set: str) -> bool:
    try:
        feature_set_columns(feature_set)
        return True
    except Exception:
        return False


def build_live_feature_frame(cfg, *, feature_set: str) -> pd.DataFrame:
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        surface="live",
        root=cfg.layout.rewrite.root,
    )
    raw_klines = load_binance_klines_1m(data_cfg)
    oracle_prices = build_live_runtime_oracle_prices(
        data_cfg=data_cfg,
        market_slug=cfg.asset.slug,
        raw_klines=raw_klines,
        oracle_prices_table=load_oracle_prices_table(data_cfg),
    )
    btc_klines = None
    if cfg.asset.slug != "btc":
        btc_cfg = DataConfig.build(
            market="btc",
            cycle=f"{int(cfg.cycle_minutes)}m",
            surface="live",
            root=cfg.layout.rewrite.root,
        )
        btc_klines = load_binance_klines_1m(btc_cfg, symbol="BTCUSDT")
    return build_feature_frame_df(
        raw_klines,
        feature_set=feature_set,
        oracle_prices=oracle_prices,
        btc_klines=btc_klines,
        cycle=f"{int(cfg.cycle_minutes)}m",
    )


def load_live_account_context(cfg, *, utc_snapshot_label_fn) -> dict[str, object]:
    open_orders = load_latest_open_orders_snapshot(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
    )
    positions = load_latest_positions_snapshot(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
    )
    payload = {
        "snapshot_ts": utc_snapshot_label_fn(),
        "open_orders": open_orders if isinstance(open_orders, dict) else None,
        "positions": positions if isinstance(positions, dict) else None,
    }
    payload["summary"] = summarize_account_state_payload(payload)
    return {
        **payload,
    }


def feature_coverage(
    *,
    available_columns: set[str],
    required_columns: list[str],
    blacklisted_columns: list[str],
    not_allowed_blacklist_columns: list[str],
    nan_feature_columns: list[str],
) -> dict[str, object]:
    blacklisted = [column for column in required_columns if column in set(blacklisted_columns)]
    present = [column for column in required_columns if column in available_columns]
    missing = [column for column in required_columns if column not in available_columns]
    effective_missing = [column for column in missing if column not in set(blacklisted)]
    nan_columns = sorted({str(column) for column in nan_feature_columns if str(column)})
    return {
        "required_feature_count": len(required_columns),
        "present_feature_count": len(present),
        "missing_feature_count": len(missing),
        "effective_missing_feature_count": len(effective_missing),
        "coverage_ratio": float(len(present) / len(required_columns)) if required_columns else 1.0,
        "present_columns": present,
        "missing_columns": missing,
        "blacklisted_columns": blacklisted,
        "not_allowed_blacklist_columns": list(not_allowed_blacklist_columns),
        "not_allowed_blacklist_count": len(not_allowed_blacklist_columns),
        "effective_missing_columns": effective_missing,
        "nan_feature_columns": nan_columns,
        "nan_feature_count": len(nan_columns),
    }


def resolve_live_blacklist(
    *,
    profile_blacklist: list[str],
    bundle_allowed_blacklist: list[str],
) -> tuple[list[str], list[str]]:
    profile_set = {str(column) for column in profile_blacklist}
    allowed_set = {str(column) for column in bundle_allowed_blacklist}
    effective = sorted(profile_set & allowed_set)
    not_allowed = sorted(profile_set - allowed_set)
    return effective, not_allowed


def apply_live_blacklist(features: pd.DataFrame, *, blacklist_columns: list[str]) -> None:
    if not blacklist_columns:
        return
    masked_cols = [column for column in blacklist_columns if column in features.columns]
    if masked_cols:
        features.loc[:, masked_cols] = 0.0


def latest_nan_feature_columns(
    *,
    features: pd.DataFrame,
    offset: int,
    decision_ts,
    required_columns: list[str],
) -> list[str]:
    rows = features[pd.to_numeric(features.get("offset"), errors="coerce") == int(offset)].copy()
    if rows.empty:
        return []
    if decision_ts is not None:
        target_ts = pd.to_datetime(decision_ts, utc=True, errors="coerce")
        if not pd.isna(target_ts):
            rows = rows[pd.to_datetime(rows["decision_ts"], utc=True, errors="coerce") == target_ts]
    if rows.empty:
        return []
    row = rows.sort_values("decision_ts").tail(1)
    nan_columns: list[str] = []
    for column in required_columns:
        if column not in row.columns:
            continue
        series = pd.to_numeric(row[column], errors="coerce").replace([float("inf"), float("-inf")], pd.NA)
        if bool(series.isna().any()):
            nan_columns.append(str(column))
    return sorted(set(nan_columns))


def persist_live_signal_snapshot(
    cfg,
    *,
    target: str,
    snapshot_ts: str,
    payload: dict[str, object],
) -> dict[str, Path]:
    layout = LiveStateLayout.discover(root=cfg.layout.rewrite.root)
    latest_path = layout.latest_signal_path(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
        target=target,
    )
    snapshot_path = layout.signal_snapshot_path(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
        target=target,
        snapshot_ts=snapshot_ts,
    )
    return write_live_payload_pair(payload=payload, latest_path=latest_path, snapshot_path=snapshot_path)


def extract_feature_snapshot(features: pd.DataFrame, *, offset: int, decision_ts) -> dict[str, object]:
    rows = features[pd.to_numeric(features.get("offset"), errors="coerce") == int(offset)].copy()
    if rows.empty:
        return {}
    if decision_ts is not None:
        target_ts = pd.to_datetime(decision_ts, utc=True, errors="coerce")
        if not pd.isna(target_ts):
            rows = rows[pd.to_datetime(rows["decision_ts"], utc=True, errors="coerce") == target_ts]
    if rows.empty:
        return {}
    row = rows.sort_values("decision_ts").tail(1).iloc[0]
    snapshot: dict[str, object] = {}
    for column in (
        "ret_3m",
        "ret_15m",
        "ret_30m",
        "ret_from_cycle_open",
        "ret_from_strike",
        "move_z",
        "move_z_strike",
        "basis_bp",
        "has_cl_strike",
    ):
        if column in rows.columns:
            value = row.get(column)
            if pd.isna(value):
                snapshot[column] = None
            elif column == "has_cl_strike":
                snapshot[column] = int(value)
            else:
                snapshot[column] = float(value)
    return snapshot


def iso_or_none(value) -> str | None:
    if value is None:
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.isoformat()
