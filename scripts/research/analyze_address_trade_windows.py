#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "pm15min_mpl"))

import joblib
import numpy as np
import pandas as pd
import requests

from pm15min.research.training.calibration import (
    lcb_from_bins,
    load_reliability_bins,
    ucb_from_bins,
)
from pm15min.research.features.builders import build_feature_frame as build_feature_frame_df
from pm15min.data.sources.polymarket_oracle_api import PolymarketOracleApiClient


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = WORKSPACE_ROOT / "research"
FEATURE_FRAMES_ROOT = RESEARCH_ROOT / "feature_frames" / "cycle=15m"
MODEL_BUNDLES_ROOT = RESEARCH_ROOT / "model_bundles" / "cycle=15m"
KLINES_ROOT = WORKSPACE_ROOT / "data" / "live" / "sources" / "binance" / "klines_1m"
ENV_PATH = WORKSPACE_ROOT / ".env"
DATA_API_BASE = "https://data-api.polymarket.com"
EVENT_SLUG_TS_RE = re.compile(r"(\d{10,})$")
DATE_TOKEN_RE = re.compile(r"(20\d{6})")
ASSET_TO_SYMBOL = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "xrp": "XRPUSDT",
}
KRAKEN_PAIR_BY_ASSET = {
    "btc": "XBTUSD",
    "eth": "ETHUSD",
    "sol": "SOLUSD",
    "xrp": "XRPUSD",
}
COINBASE_PRODUCT_BY_ASSET = {
    "btc": "BTC-USD",
    "eth": "ETH-USD",
    "sol": "SOL-USD",
    "xrp": "XRP-USD",
}


@dataclass(frozen=True)
class BundleCandidate:
    bundle_dir: Path
    bundle_label: str
    release_date: date | None


@dataclass
class OffsetRuntime:
    asset: str
    bundle_label: str
    bundle_dir: Path
    offset: int
    feature_set: str
    feature_columns: list[str]
    missing_feature_fill_value: float
    allowed_blacklist_columns: set[str]
    model_lgb: Any
    model_lr: Any
    w_lgb: float
    w_lr: float
    reliability_bins: list[dict[str, float | int]]
    logreg_meta: dict[str, dict[str, Any]]
    lgb_meta: dict[str, dict[str, Any]]
    factor_meta: dict[str, dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze address trade windows against local features and model weights.")
    parser.add_argument("--start", default="2026-03-15T00:00:00Z", help="UTC start timestamp, e.g. 2026-03-15T00:00:00Z")
    parser.add_argument("--today", default="2026-03-30", help="Settlement cutoff date in UTC, e.g. 2026-03-30")
    parser.add_argument("--profile", default="deep_otm_baseline", help="Bundle profile to inspect.")
    parser.add_argument(
        "--out-dir",
        default="research/address_trade_windows/run=20260330_start=20260315_profile=deep_otm_baseline",
        help="Output directory relative to workspace root.",
    )
    parser.add_argument("--user-address", default=None, help="Override Polymarket user address.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start_ts = _parse_utc(args.start)
    settle_day = datetime.strptime(str(args.today), "%Y-%m-%d").date()
    out_dir = WORKSPACE_ROOT / str(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    user_address = str(args.user_address or _load_env_value("POLYMARKET_USER_ADDRESS") or "").strip()
    if not user_address:
        raise SystemExit("Missing POLYMARKET_USER_ADDRESS in env or --user-address.")

    activity = pd.DataFrame(_fetch_all("activity", user=user_address))
    positions = pd.DataFrame(_fetch_all("positions", user=user_address))
    closed_positions = pd.DataFrame(_fetch_all("closed-positions", user=user_address))

    trades = _prepare_trade_activity(activity, start_ts=start_ts)
    redeems = _prepare_redeem_activity(activity, start_ts=start_ts)
    grouped = _group_trade_windows(trades)
    grouped = _attach_status(grouped, redeems=redeems, positions=positions, closed_positions=closed_positions, settle_day=settle_day)

    bundle_registry = _discover_bundle_candidates(profile=str(args.profile))
    runtime_cache: dict[tuple[str, str, int], OffsetRuntime] = {}
    feature_frame_cache: dict[tuple[str, str], pd.DataFrame] = {}
    kline_cache: dict[str, pd.DataFrame] = {}
    kraken_cache: dict[tuple[str, int], pd.DataFrame] = {}
    oracle_cache: dict[tuple[str, int], pd.DataFrame] = {}

    trade_rows: list[dict[str, Any]] = []
    factor_rows: list[dict[str, Any]] = []

    for record in grouped.to_dict(orient="records"):
        cycle_start_ts = record.get("cycle_start_ts")
        if pd.isna(cycle_start_ts):
            trade_rows.append(_base_trade_row(record))
            continue

        asset = str(record["asset"]).lower()
        first_trade_ts = pd.Timestamp(record["first_trade_ts"])
        bundle = _choose_bundle(bundle_registry, asset=asset, trade_ts=first_trade_ts)
        if bundle is None:
            trade_rows.append(_base_trade_row(record) | {"analysis_error": "bundle_missing"})
            continue

        runtime_offsets = _bundle_runtimes(bundle, asset=asset, cache=runtime_cache)
        if not runtime_offsets:
            trade_rows.append(_base_trade_row(record) | {"analysis_error": "offset_runtime_missing"})
            continue

        feature_set = next(iter(runtime_offsets.values())).feature_set
        feature_frame = _load_feature_frame(asset=asset, feature_set=feature_set, cache=feature_frame_cache)
        matched = _match_feature_row(
            feature_frame=feature_frame,
            cycle_start_ts=pd.Timestamp(cycle_start_ts),
            trade_ts=first_trade_ts,
            supported_offsets=tuple(sorted(runtime_offsets)),
        )
        feature_source = "local_feature_frame"
        used_supplemental_feature_row = False
        if matched is None:
            supplemental = _build_supplemental_feature_match(
                trade_record=record,
                runtime_offsets=runtime_offsets,
                trade_ts=first_trade_ts,
                kraken_cache=kraken_cache,
                oracle_cache=oracle_cache,
            )
            if supplemental is not None:
                matched = supplemental
                feature_source = "external_ohlcv_fallback"
                used_supplemental_feature_row = True
            else:
                trade_rows.append(
                    _base_trade_row(record)
                    | {
                        "bundle_label": bundle.bundle_label,
                        "bundle_release_date": None if bundle.release_date is None else bundle.release_date.isoformat(),
                        "analysis_error": "feature_row_missing",
                    }
                )
                continue

        matched_row = matched["row"]
        matched_offset = int(matched["offset"])
        runtime = runtime_offsets[matched_offset]

        score_payload = _score_row(runtime=runtime, row=matched_row)
        kline_payload = _build_kline_context(
            asset=asset,
            cycle_start_ts=pd.Timestamp(cycle_start_ts),
            cycle_end_ts=pd.Timestamp(record["cycle_end_ts"]),
            trade_ts=first_trade_ts,
            cache=kline_cache,
        )
        factor_payload, top_logreg, top_lgb = _build_factor_rows(
            trade_record=record,
            runtime=runtime,
            row=matched_row,
            matched_offset=matched_offset,
        )
        factor_rows.extend(factor_payload)

        trade_rows.append(
            _base_trade_row(record)
            | {
                "bundle_label": bundle.bundle_label,
                "bundle_release_date": None if bundle.release_date is None else bundle.release_date.isoformat(),
                "feature_set": runtime.feature_set,
                "matched_offset": matched_offset,
                "matched_decision_ts": pd.Timestamp(matched_row["decision_ts"]).isoformat(),
                "matched_cycle_start_ts": pd.Timestamp(matched_row["cycle_start_ts"]).isoformat(),
                "matched_cycle_end_ts": pd.Timestamp(matched_row["cycle_end_ts"]).isoformat(),
                "used_future_decision_row": bool(matched["used_future_decision_row"]),
                "trade_minus_decision_seconds": float((first_trade_ts - pd.Timestamp(matched_row["decision_ts"])).total_seconds()),
                "feature_source": feature_source,
                "used_supplemental_feature_row": used_supplemental_feature_row,
                **score_payload,
                **kline_payload,
                "top_logreg_contributors": json.dumps(top_logreg, ensure_ascii=False),
                "top_lgb_contributors": json.dumps(top_lgb, ensure_ascii=False),
            }
        )

    trades_df = pd.DataFrame(trade_rows).sort_values(["first_trade_ts", "asset", "outcome_index"]).reset_index(drop=True)
    factors_df = pd.DataFrame(factor_rows).sort_values(
        ["first_trade_ts", "asset", "outcome_index", "feature"],
        ascending=[True, True, True, True],
    ).reset_index(drop=True)

    trades_path = out_dir / "trade_windows.csv"
    factors_path = out_dir / "trade_factor_weights_long.csv"
    report_path = out_dir / "report.md"

    trades_df.to_csv(trades_path, index=False)
    factors_df.to_csv(factors_path, index=False)
    report_path.write_text(
        _render_report(
            profile=str(args.profile),
            start_ts=start_ts,
            settle_day=settle_day,
            user_address=user_address,
            trades_df=trades_df,
            factors_df=factors_df,
            trades_path=trades_path,
            factors_path=factors_path,
        ),
        encoding="utf-8",
    )

    print(f"trade windows: {trades_path}")
    print(f"factor weights: {factors_path}")
    print(f"report: {report_path}")
    return 0


def _load_env_value(key: str) -> str | None:
    direct = os.getenv(key)
    if direct:
        return direct
    if not ENV_PATH.exists():
        return None
    prefix = f"{key}="
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        if line.startswith(prefix):
            return line.split("=", 1)[1].strip()
    return None


def _parse_utc(raw: str) -> pd.Timestamp:
    token = str(raw).strip()
    if token.endswith("Z"):
        token = token[:-1] + "+00:00"
    return pd.Timestamp(token).tz_convert("UTC") if pd.Timestamp(token).tzinfo else pd.Timestamp(token, tz="UTC")


def _fetch_all(endpoint: str, *, user: str) -> list[dict[str, Any]]:
    session = requests.Session()
    limit = 200
    offset = 0
    out: list[dict[str, Any]] = []
    while True:
        response = session.get(
            f"{DATA_API_BASE.rstrip('/')}/{endpoint}",
            params={"user": str(user), "limit": limit, "offset": offset},
            timeout=30,
        )
        if response.status_code >= 400:
            break
        payload = response.json()
        if not isinstance(payload, list) or not payload:
            break
        out.extend(row for row in payload if isinstance(row, dict))
        if len(payload) < limit:
            break
        offset += limit
    return out


def _prepare_trade_activity(activity: pd.DataFrame, *, start_ts: pd.Timestamp) -> pd.DataFrame:
    if activity.empty:
        return pd.DataFrame(
            columns=[
                "conditionId",
                "outcomeIndex",
                "eventSlug",
                "title",
                "outcome",
                "asset",
                "timestamp",
                "trade_ts",
                "cycle_start_ts",
                "cycle_end_ts",
                "usdcSize",
                "size",
                "price",
            ]
        )
    frame = activity.copy()
    frame["timestamp"] = pd.to_numeric(frame.get("timestamp"), errors="coerce").fillna(0).astype(int)
    frame = frame.loc[frame["type"].astype(str).eq("TRADE") & frame["timestamp"].ge(int(start_ts.timestamp()))].copy()
    frame["trade_ts"] = pd.to_datetime(frame["timestamp"], unit="s", utc=True)
    frame["conditionId"] = frame["conditionId"].astype(str)
    frame["eventSlug"] = frame.get("eventSlug", frame.get("slug", "")).astype(str)
    frame["asset"] = frame["eventSlug"].str.extract(r"^(btc|eth|sol|xrp)", expand=False).fillna("").astype(str)
    frame["outcomeIndex"] = pd.to_numeric(frame.get("outcomeIndex"), errors="coerce").fillna(-1).astype(int)
    frame["usdcSize"] = pd.to_numeric(frame.get("usdcSize"), errors="coerce").fillna(0.0)
    frame["size"] = pd.to_numeric(frame.get("size"), errors="coerce").fillna(0.0)
    frame["price"] = pd.to_numeric(frame.get("price"), errors="coerce").fillna(0.0)
    frame["cycle_start_ts"] = frame["eventSlug"].map(_event_slug_cycle_start)
    frame["cycle_end_ts"] = frame["cycle_start_ts"] + pd.Timedelta(minutes=15)
    return frame.reset_index(drop=True)


def _prepare_redeem_activity(activity: pd.DataFrame, *, start_ts: pd.Timestamp) -> pd.DataFrame:
    if activity.empty:
        return pd.DataFrame(columns=["conditionId", "timestamp", "usdcSize"])
    frame = activity.copy()
    frame["timestamp"] = pd.to_numeric(frame.get("timestamp"), errors="coerce").fillna(0).astype(int)
    frame = frame.loc[frame["type"].astype(str).eq("REDEEM") & frame["timestamp"].ge(int(start_ts.timestamp()))].copy()
    frame["conditionId"] = frame["conditionId"].astype(str)
    frame["usdcSize"] = pd.to_numeric(frame.get("usdcSize"), errors="coerce").fillna(0.0)
    return frame.reset_index(drop=True)


def _group_trade_windows(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    grouped = (
        trades.groupby(["conditionId", "outcomeIndex"], dropna=False)
        .agg(
            asset=("asset", "first"),
            event_slug=("eventSlug", "first"),
            title=("title", "first"),
            outcome=("outcome", "first"),
            fills=("conditionId", "size"),
            cost_usdc=("usdcSize", "sum"),
            shares_bought=("size", "sum"),
            first_trade_ts=("trade_ts", "min"),
            last_trade_ts=("trade_ts", "max"),
            cycle_start_ts=("cycle_start_ts", "first"),
            cycle_end_ts=("cycle_end_ts", "first"),
        )
        .reset_index()
    )
    grouped["avg_entry"] = grouped["cost_usdc"] / grouped["shares_bought"].replace(0.0, np.nan)
    grouped["trade_key"] = grouped.apply(
        lambda row: f"{row['asset']}::{row['event_slug']}::outcome={int(row['outcomeIndex'])}",
        axis=1,
    )
    return grouped


def _attach_status(
    grouped: pd.DataFrame,
    *,
    redeems: pd.DataFrame,
    positions: pd.DataFrame,
    closed_positions: pd.DataFrame,
    settle_day: date,
) -> pd.DataFrame:
    if grouped.empty:
        return grouped

    redeem_by_condition = (
        redeems.groupby("conditionId")["usdcSize"].sum().to_dict() if not redeems.empty else {}
    )
    side_count_by_condition = grouped.groupby("conditionId")["outcomeIndex"].nunique().to_dict()

    closed_map: dict[tuple[str, int], dict[str, Any]] = {}
    if not closed_positions.empty:
        frame = closed_positions.copy()
        frame["conditionId"] = frame["conditionId"].astype(str)
        frame["outcomeIndex"] = pd.to_numeric(frame.get("outcomeIndex"), errors="coerce").fillna(-1).astype(int)
        frame["avgPrice"] = pd.to_numeric(frame.get("avgPrice"), errors="coerce").fillna(0.0)
        frame["totalBought"] = pd.to_numeric(frame.get("totalBought"), errors="coerce").fillna(0.0)
        frame["realizedPnl"] = pd.to_numeric(frame.get("realizedPnl"), errors="coerce").fillna(0.0)
        frame["recovered_usdc"] = frame["totalBought"] * frame["avgPrice"] + frame["realizedPnl"]
        closed_map = (
            frame.groupby(["conditionId", "outcomeIndex"], dropna=False)
            .agg(realized_pnl=("realizedPnl", "sum"), recovered_usdc=("recovered_usdc", "sum"))
            .to_dict("index")
        )

    positions_map: dict[tuple[str, int], dict[str, Any]] = {}
    if not positions.empty:
        frame = positions.copy()
        frame["conditionId"] = frame["conditionId"].astype(str)
        frame["outcomeIndex"] = pd.to_numeric(frame.get("outcomeIndex"), errors="coerce").fillna(-1).astype(int)
        frame["currentValue"] = pd.to_numeric(frame.get("currentValue"), errors="coerce").fillna(0.0)
        frame["cashPnl"] = pd.to_numeric(frame.get("cashPnl"), errors="coerce").fillna(0.0)
        frame["endDate_dt"] = pd.to_datetime(frame.get("endDate"), errors="coerce", utc=True)
        positions_map = (
            frame.groupby(["conditionId", "outcomeIndex"], dropna=False)
            .agg(
                current_value=("currentValue", "sum"),
                cash_pnl=("cashPnl", "sum"),
                end_date_dt=("endDate_dt", "max"),
            )
            .to_dict("index")
        )

    status_rows: list[dict[str, Any]] = []
    for row in grouped.to_dict(orient="records"):
        condition_id = str(row["conditionId"])
        outcome_index = int(row["outcomeIndex"])
        key = (condition_id, outcome_index)
        side_count = int(side_count_by_condition.get(condition_id, 1))
        closed_item = closed_map.get(key)
        position_item = positions_map.get(key)

        status = "unknown"
        settled = False
        recovered_usdc = 0.0
        pnl_usdc = np.nan

        if closed_item is not None:
            status = "redeemed_closed"
            settled = True
            recovered_usdc = float(closed_item["recovered_usdc"])
            pnl_usdc = float(closed_item["realized_pnl"])
        elif condition_id in redeem_by_condition and side_count == 1:
            status = "redeemed_by_condition"
            settled = True
            recovered_usdc = float(redeem_by_condition[condition_id])
            pnl_usdc = recovered_usdc - float(row["cost_usdc"])
        elif position_item is not None:
            end_date_dt = position_item["end_date_dt"]
            end_day = None if pd.isna(end_date_dt) else pd.Timestamp(end_date_dt).date()
            current_value = float(position_item["current_value"] or 0.0)
            cash_pnl = float(position_item["cash_pnl"] or 0.0)
            if end_day is not None and end_day < settle_day:
                settled = True
            if settled:
                if current_value > 0.0:
                    status = "winner_unredeemed"
                    recovered_usdc = current_value
                    pnl_usdc = cash_pnl
                else:
                    status = "no_redeem_loss"
                    recovered_usdc = 0.0
                    pnl_usdc = cash_pnl if cash_pnl != 0.0 else -float(row["cost_usdc"])
            else:
                status = "pending_today_or_later"
                recovered_usdc = current_value
        else:
            status = "missing_state"

        status_rows.append(
            {
                "trade_key": row["trade_key"],
                "status": status,
                "settled": settled,
                "recovered_usdc": recovered_usdc,
                "pnl_usdc": pnl_usdc,
                "hit": status in {"redeemed_closed", "redeemed_by_condition", "winner_unredeemed"},
                "ambiguous_condition_sides": side_count > 1,
            }
        )

    status_df = pd.DataFrame(status_rows)
    return grouped.merge(status_df, on="trade_key", how="left")


def _discover_bundle_candidates(*, profile: str) -> dict[str, list[BundleCandidate]]:
    registry: dict[str, list[BundleCandidate]] = {}
    for asset_dir in sorted(MODEL_BUNDLES_ROOT.glob("asset=*")):
        asset = asset_dir.name.split("=", 1)[1]
        root = asset_dir / f"profile={profile}" / "target=direction"
        candidates: list[BundleCandidate] = []
        for bundle_dir in sorted(root.glob("bundle=*")):
            if not _bundle_has_required_artifacts(bundle_dir):
                continue
            label = bundle_dir.name.split("=", 1)[1]
            release_date = _bundle_release_date(label)
            candidates.append(BundleCandidate(bundle_dir=bundle_dir, bundle_label=label, release_date=release_date))
        registry[asset] = sorted(
            candidates,
            key=lambda item: (item.release_date or date.min, item.bundle_label),
        )
    return registry


def _bundle_has_required_artifacts(bundle_dir: Path) -> bool:
    offset_dirs = sorted((bundle_dir / "offsets").glob("offset=*"))
    if not offset_dirs:
        return False
    for offset_dir in offset_dirs:
        required = [
            offset_dir / "bundle_config.json",
            offset_dir / "models" / "lgbm_sigmoid.joblib",
            offset_dir / "models" / "logreg_sigmoid.joblib",
            offset_dir / "diagnostics" / "logreg_coefficients.json",
            offset_dir / "diagnostics" / "lgb_feature_importance.json",
            offset_dir / "diagnostics" / "factor_direction_summary.json",
        ]
        if not all(path.exists() for path in required):
            return False
    return True


def _bundle_release_date(label: str) -> date | None:
    matches = DATE_TOKEN_RE.findall(str(label))
    if not matches:
        return None
    try:
        return datetime.strptime(matches[-1], "%Y%m%d").date()
    except ValueError:
        return None


def _choose_bundle(
    registry: dict[str, list[BundleCandidate]],
    *,
    asset: str,
    trade_ts: pd.Timestamp,
) -> BundleCandidate | None:
    candidates = list(registry.get(asset, []))
    if not candidates:
        return None
    trade_day = trade_ts.date()
    dated = [item for item in candidates if item.release_date is not None]
    undated = [item for item in candidates if item.release_date is None]
    eligible = [item for item in dated if item.release_date <= trade_day]
    if eligible:
        return eligible[-1]
    if dated:
        return dated[0]
    return undated[-1] if undated else None


def _bundle_runtimes(
    bundle: BundleCandidate,
    *,
    asset: str,
    cache: dict[tuple[str, str, int], OffsetRuntime],
) -> dict[int, OffsetRuntime]:
    out: dict[int, OffsetRuntime] = {}
    for offset_dir in sorted((bundle.bundle_dir / "offsets").glob("offset=*")):
        offset = int(offset_dir.name.split("=", 1)[1])
        key = (asset, bundle.bundle_label, offset)
        runtime = cache.get(key)
        if runtime is None:
            runtime = _load_offset_runtime(asset=asset, bundle=bundle, offset=offset)
            cache[key] = runtime
        out[offset] = runtime
    return out


def _load_offset_runtime(*, asset: str, bundle: BundleCandidate, offset: int) -> OffsetRuntime:
    offset_dir = bundle.bundle_dir / "offsets" / f"offset={int(offset)}"
    bundle_cfg = json.loads((offset_dir / "bundle_config.json").read_text(encoding="utf-8"))
    feature_columns = [str(value) for value in (bundle_cfg.get("feature_columns") or []) if str(value)]
    weights = json.loads((offset_dir / "calibration" / "blend_weights.json").read_text(encoding="utf-8"))
    w_lgb = float(weights.get("w_lgb", 0.5))
    w_lr = float(weights.get("w_lr", 0.5))
    total = w_lgb + w_lr
    if total <= 0.0:
        w_lgb = 0.5
        w_lr = 0.5
    else:
        w_lgb = w_lgb / total
        w_lr = w_lr / total

    reliability_bins = []
    for path in [
        offset_dir / "calibration" / "reliability_bins_blend_weighted.json",
        offset_dir / "calibration" / "reliability_bins_blend.json",
    ]:
        if path.exists():
            reliability_bins = list(load_reliability_bins(path))
            if reliability_bins:
                break

    logreg_payload = json.loads((offset_dir / "diagnostics" / "logreg_coefficients.json").read_text(encoding="utf-8"))
    lgb_payload = json.loads((offset_dir / "diagnostics" / "lgb_feature_importance.json").read_text(encoding="utf-8"))
    factor_payload = json.loads((offset_dir / "diagnostics" / "factor_direction_summary.json").read_text(encoding="utf-8"))

    logreg_meta = {str(row["feature"]): row for row in list(logreg_payload.get("rows") or []) if isinstance(row, dict)}
    lgb_meta = {str(row["feature"]): row for row in list(lgb_payload.get("rows") or []) if isinstance(row, dict)}
    factor_meta = {str(row["feature"]): row for row in list(factor_payload.get("rows") or []) if isinstance(row, dict)}

    return OffsetRuntime(
        asset=asset,
        bundle_label=bundle.bundle_label,
        bundle_dir=bundle.bundle_dir,
        offset=int(offset),
        feature_set=str(bundle_cfg.get("feature_set") or ""),
        feature_columns=feature_columns,
        missing_feature_fill_value=float(bundle_cfg.get("missing_feature_fill_value", 0.0)),
        allowed_blacklist_columns={str(value) for value in (bundle_cfg.get("allowed_blacklist_columns") or []) if str(value)},
        model_lgb=_apply_sklearn_compat_shims(joblib.load(offset_dir / "models" / "lgbm_sigmoid.joblib")),
        model_lr=_apply_sklearn_compat_shims(joblib.load(offset_dir / "models" / "logreg_sigmoid.joblib")),
        w_lgb=float(w_lgb),
        w_lr=float(w_lr),
        reliability_bins=reliability_bins,
        logreg_meta=logreg_meta,
        lgb_meta=lgb_meta,
        factor_meta=factor_meta,
    )


def _apply_sklearn_compat_shims(model: Any) -> Any:
    seen: set[int] = set()

    def _walk(obj: Any) -> None:
        if obj is None:
            return
        ident = id(obj)
        if ident in seen:
            return
        seen.add(ident)

        if obj.__class__.__name__ == "LogisticRegression" and not hasattr(obj, "multi_class"):
            try:
                setattr(obj, "multi_class", "auto")
            except Exception:
                pass

        for child in getattr(obj, "named_steps", {}).values():
            _walk(child)
        for item in list(getattr(obj, "steps", []) or []):
            if isinstance(item, tuple) and len(item) >= 2:
                _walk(item[1])
        for item in list(getattr(obj, "calibrated_classifiers_", []) or []):
            for name in ("estimator", "base_estimator", "classifier", "classifier_"):
                _walk(getattr(item, name, None))
        for name in ("estimator", "base_estimator", "classifier", "classifier_"):
            _walk(getattr(obj, name, None))

    _walk(model)
    return model


def _load_feature_frame(*, asset: str, feature_set: str, cache: dict[tuple[str, str], pd.DataFrame]) -> pd.DataFrame:
    key = (asset, feature_set)
    frame = cache.get(key)
    if frame is not None:
        return frame
    path = FEATURE_FRAMES_ROOT / f"asset={asset}" / f"feature_set={feature_set}" / "source_surface=backtest" / "data.parquet"
    frame = pd.read_parquet(path).copy()
    frame["cycle_start_ts"] = pd.to_datetime(frame["cycle_start_ts"], utc=True, errors="coerce")
    frame["cycle_end_ts"] = pd.to_datetime(frame["cycle_end_ts"], utc=True, errors="coerce")
    frame["decision_ts"] = pd.to_datetime(frame["decision_ts"], utc=True, errors="coerce")
    frame["offset"] = pd.to_numeric(frame["offset"], errors="coerce").astype("Int64")
    cache[key] = frame
    return frame


def _match_feature_row(
    *,
    feature_frame: pd.DataFrame,
    cycle_start_ts: pd.Timestamp,
    trade_ts: pd.Timestamp,
    supported_offsets: tuple[int, ...],
) -> dict[str, Any] | None:
    candidates = feature_frame.loc[
        feature_frame["cycle_start_ts"].eq(pd.Timestamp(cycle_start_ts))
        & feature_frame["offset"].isin(list(supported_offsets))
    ].copy()
    if candidates.empty:
        return None
    candidates = candidates.sort_values(["decision_ts", "offset"]).reset_index(drop=True)
    not_later = candidates.loc[candidates["decision_ts"].le(pd.Timestamp(trade_ts))]
    if not not_later.empty:
        row = not_later.iloc[-1]
        return {"row": row, "offset": int(row["offset"]), "used_future_decision_row": False}
    row = candidates.iloc[0]
    return {"row": row, "offset": int(row["offset"]), "used_future_decision_row": True}


def _score_row(*, runtime: OffsetRuntime, row: pd.Series) -> dict[str, Any]:
    X = pd.DataFrame([row]).copy()
    for column in runtime.feature_columns:
        if column not in X.columns:
            X[column] = 0.0
        X[column] = pd.to_numeric(X[column], errors="coerce")
    X = X[runtime.feature_columns].replace([np.inf, -np.inf], np.nan).fillna(runtime.missing_feature_fill_value)
    for column in runtime.allowed_blacklist_columns:
        if column in X.columns:
            X[column] = runtime.missing_feature_fill_value

    p_lgb = float(runtime.model_lgb.predict_proba(X)[:, 1][0])
    p_lr = float(runtime.model_lr.predict_proba(X)[:, 1][0])
    p_signal = float(runtime.w_lgb * p_lgb + runtime.w_lr * p_lr)
    p_up_raw = float(np.clip(p_signal, 0.0, 1.0))
    p_down_raw = float(1.0 - p_up_raw)
    if runtime.reliability_bins:
        p_up = float(lcb_from_bins(p_up_raw, runtime.reliability_bins))
        p_down = float(1.0 - ucb_from_bins(p_up_raw, runtime.reliability_bins))
        probability_mode = "conservative_reliability_bin"
    else:
        p_up = p_up_raw
        p_down = p_down_raw
        probability_mode = "raw_blend"

    predicted_side = "UP" if p_up >= p_down else "DOWN"
    predicted_prob = max(p_up, p_down)
    return {
        "p_lgb": p_lgb,
        "p_lr": p_lr,
        "p_signal": p_signal,
        "p_up_raw": p_up_raw,
        "p_down_raw": p_down_raw,
        "p_up": p_up,
        "p_down": p_down,
        "predicted_side": predicted_side,
        "predicted_prob": predicted_prob,
        "probability_mode": probability_mode,
    }


def _load_klines(asset: str, cache: dict[str, pd.DataFrame]) -> pd.DataFrame:
    frame = cache.get(asset)
    if frame is not None:
        return frame
    symbol = ASSET_TO_SYMBOL.get(asset)
    if not symbol:
        frame = pd.DataFrame()
    else:
        path = KLINES_ROOT / f"symbol={symbol}" / "data.parquet"
        frame = pd.read_parquet(path).copy()
        frame["open_time"] = pd.to_datetime(frame["open_time"], utc=True, errors="coerce")
        frame["close_time"] = pd.to_datetime(frame["close_time"], utc=True, errors="coerce")
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    cache[asset] = frame
    return frame


def _build_kline_context(
    *,
    asset: str,
    cycle_start_ts: pd.Timestamp,
    cycle_end_ts: pd.Timestamp,
    trade_ts: pd.Timestamp,
    cache: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    klines = _load_klines(asset, cache=cache)
    if klines.empty:
        return {"kline_missing": True}

    cycle_rows = klines.loc[
        klines["open_time"].ge(pd.Timestamp(cycle_start_ts)) & klines["open_time"].lt(pd.Timestamp(cycle_end_ts))
    ].copy()
    trade_rows = klines.loc[klines["open_time"].le(pd.Timestamp(trade_ts))].copy()
    trade_bar = trade_rows.sort_values("open_time").tail(1)

    payload: dict[str, Any] = {"kline_missing": False}
    if not cycle_rows.empty:
        cycle_rows = cycle_rows.sort_values("open_time")
        cycle_open = float(cycle_rows.iloc[0]["open"])
        cycle_close = float(cycle_rows.iloc[-1]["close"])
        payload.update(
            {
                "cycle_spot_open": cycle_open,
                "cycle_spot_high": float(cycle_rows["high"].max()),
                "cycle_spot_low": float(cycle_rows["low"].min()),
                "cycle_spot_close": cycle_close,
                "cycle_spot_volume": float(cycle_rows["volume"].sum()),
                "cycle_spot_ret_pct": ((cycle_close / cycle_open) - 1.0) * 100.0 if cycle_open else np.nan,
            }
        )
    if not trade_bar.empty:
        row = trade_bar.iloc[0]
        trade_close = float(row["close"])
        cycle_open = payload.get("cycle_spot_open")
        payload.update(
            {
                "trade_bar_open_time": pd.Timestamp(row["open_time"]).isoformat(),
                "trade_bar_open": float(row["open"]),
                "trade_bar_high": float(row["high"]),
                "trade_bar_low": float(row["low"]),
                "trade_bar_close": trade_close,
                "trade_bar_volume": float(row["volume"]),
                "trade_vs_cycle_open_ret_pct": ((trade_close / cycle_open) - 1.0) * 100.0 if cycle_open else np.nan,
            }
        )
    return payload


def _build_factor_rows(
    *,
    trade_record: dict[str, Any],
    runtime: OffsetRuntime,
    row: pd.Series,
    matched_offset: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    X = pd.DataFrame([row]).copy()
    for column in runtime.feature_columns:
        if column not in X.columns:
            X[column] = 0.0
        X[column] = pd.to_numeric(X[column], errors="coerce")
    X = X[runtime.feature_columns].replace([np.inf, -np.inf], np.nan).fillna(runtime.missing_feature_fill_value)
    for column in runtime.allowed_blacklist_columns:
        if column in X.columns:
            X[column] = runtime.missing_feature_fill_value

    scaler = runtime.model_lr.named_steps["scaler"]
    classifier = runtime.model_lr.named_steps["clf"]
    scaled_values = scaler.transform(X)[0]
    coefficients = np.asarray(
        getattr(classifier, "coef_", np.zeros((1, len(runtime.feature_columns)), dtype=float)),
        dtype=float,
    ).reshape(-1)
    logreg_contrib = scaled_values * coefficients

    lgb_shap = runtime.model_lgb.predict(X, pred_contrib=True)[0]
    lgb_feature_shap = lgb_shap[:-1]

    rows: list[dict[str, Any]] = []
    for index, feature in enumerate(runtime.feature_columns):
        logreg_row = runtime.logreg_meta.get(feature, {})
        lgb_row = runtime.lgb_meta.get(feature, {})
        factor_row = runtime.factor_meta.get(feature, {})
        rows.append(
            {
                "trade_key": trade_record["trade_key"],
                "asset": trade_record["asset"],
                "title": trade_record["title"],
                "condition_id": trade_record["conditionId"],
                "outcome_index": int(trade_record["outcomeIndex"]),
                "outcome": trade_record["outcome"],
                "status": trade_record["status"],
                "first_trade_ts": pd.Timestamp(trade_record["first_trade_ts"]).isoformat(),
                "cycle_start_ts": pd.Timestamp(trade_record["cycle_start_ts"]).isoformat(),
                "cycle_end_ts": pd.Timestamp(trade_record["cycle_end_ts"]).isoformat(),
                "bundle_label": runtime.bundle_label,
                "matched_offset": int(matched_offset),
                "matched_decision_ts": pd.Timestamp(row["decision_ts"]).isoformat(),
                "feature": feature,
                "raw_feature_value": float(X.iloc[0][feature]),
                "scaled_feature_value": float(scaled_values[index]),
                "logreg_coefficient": float(coefficients[index]),
                "logreg_abs_rank": int(logreg_row.get("rank", 0) or 0),
                "logreg_contribution": float(logreg_contrib[index]),
                "lgb_gain_importance": float(lgb_row.get("gain_importance", 0.0) or 0.0),
                "lgb_gain_share": float(lgb_row.get("gain_share", 0.0) or 0.0),
                "lgb_gain_rank": int(lgb_row.get("rank", 0) or 0),
                "lgb_shap_value": float(lgb_feature_shap[index]),
                "factor_direction_score": float(factor_row.get("direction_score", 0.0) or 0.0),
                "factor_direction": str(factor_row.get("direction", "")),
                "factor_direction_rank": int(factor_row.get("rank", 0) or 0),
                "target_correlation": float(factor_row.get("target_correlation", 0.0) or 0.0),
                "mean_gap": float(factor_row.get("mean_gap", 0.0) or 0.0),
            }
        )

    top_logreg = [
        {"feature": item["feature"], "logreg_contribution": round(float(item["logreg_contribution"]), 6)}
        for item in sorted(rows, key=lambda item: abs(float(item["logreg_contribution"])), reverse=True)[:5]
    ]
    top_lgb = [
        {"feature": item["feature"], "lgb_shap_value": round(float(item["lgb_shap_value"]), 6)}
        for item in sorted(rows, key=lambda item: abs(float(item["lgb_shap_value"])), reverse=True)[:5]
    ]
    return rows, top_logreg, top_lgb


def _base_trade_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "trade_key": record.get("trade_key"),
        "asset": record.get("asset"),
        "condition_id": record.get("conditionId"),
        "outcome_index": record.get("outcomeIndex"),
        "outcome": record.get("outcome"),
        "event_slug": record.get("event_slug"),
        "title": record.get("title"),
        "fills": record.get("fills"),
        "cost_usdc": record.get("cost_usdc"),
        "shares_bought": record.get("shares_bought"),
        "avg_entry": record.get("avg_entry"),
        "first_trade_ts": None if pd.isna(record.get("first_trade_ts")) else pd.Timestamp(record["first_trade_ts"]).isoformat(),
        "last_trade_ts": None if pd.isna(record.get("last_trade_ts")) else pd.Timestamp(record["last_trade_ts"]).isoformat(),
        "cycle_start_ts": None if pd.isna(record.get("cycle_start_ts")) else pd.Timestamp(record["cycle_start_ts"]).isoformat(),
        "cycle_end_ts": None if pd.isna(record.get("cycle_end_ts")) else pd.Timestamp(record["cycle_end_ts"]).isoformat(),
        "status": record.get("status"),
        "settled": record.get("settled"),
        "hit": record.get("hit"),
        "recovered_usdc": record.get("recovered_usdc"),
        "pnl_usdc": record.get("pnl_usdc"),
        "ambiguous_condition_sides": record.get("ambiguous_condition_sides"),
    }


def _event_slug_cycle_start(slug: str) -> pd.Timestamp | pd.NaT:
    match = EVENT_SLUG_TS_RE.search(str(slug))
    if not match:
        return pd.NaT
    try:
        return pd.Timestamp(int(match.group(1)), unit="s", tz="UTC")
    except Exception:
        return pd.NaT


def _render_report(
    *,
    profile: str,
    start_ts: pd.Timestamp,
    settle_day: date,
    user_address: str,
    trades_df: pd.DataFrame,
    factors_df: pd.DataFrame,
    trades_path: Path,
    factors_path: Path,
) -> str:
    settled = trades_df.loc[trades_df["settled"].fillna(False)].copy()
    pending = trades_df.loc[trades_df["status"].astype(str).eq("pending_today_or_later")].copy()

    overview = {
        "trade_windows": int(len(trades_df)),
        "settled_windows": int(len(settled)),
        "pending_windows": int(len(pending)),
        "factor_rows": int(len(factors_df)),
        "profile": profile,
        "start_ts": start_ts.isoformat(),
        "settlement_cutoff_day": settle_day.isoformat(),
    }

    status_table = trades_df["status"].astype(str).value_counts(dropna=False).rename_axis("status").reset_index(name="count")
    bundle_table = (
        trades_df["bundle_label"].astype("string").fillna("missing").value_counts(dropna=False).rename_axis("bundle_label").reset_index(name="trade_windows")
    )
    feature_source_table = (
        trades_df["feature_source"].astype("string").fillna("missing").value_counts(dropna=False).rename_axis("feature_source").reset_index(name="trade_windows")
    )
    coverage_table = (
        trades_df.assign(aligned=trades_df["matched_offset"].notna())
        .groupby("asset", dropna=False)
        .agg(trade_windows=("trade_key", "size"), aligned_windows=("aligned", "sum"))
        .reset_index()
    )
    coverage_table["missing_windows"] = coverage_table["trade_windows"] - coverage_table["aligned_windows"]
    coverage_table["aligned_pct"] = coverage_table["aligned_windows"] / coverage_table["trade_windows"] * 100.0
    loss_table = (
        settled.loc[settled["status"].astype(str).eq("no_redeem_loss"), ["asset", "outcome", "cost_usdc", "title", "bundle_label", "matched_offset"]]
        .sort_values("cost_usdc", ascending=False)
        .head(12)
    )
    win_table = (
        settled.loc[settled["hit"].fillna(False), ["asset", "outcome", "cost_usdc", "recovered_usdc", "pnl_usdc", "title", "bundle_label", "matched_offset"]]
        .sort_values("pnl_usdc", ascending=False)
        .head(12)
    )

    lines = [
        "# Address Trade Window Analysis",
        "",
        f"- user_address: `{user_address[:8]}...{user_address[-4:]}`",
        f"- profile: `{profile}`",
        f"- start_ts: `{start_ts.isoformat()}`",
        f"- settlement_cutoff_day: `{settle_day.isoformat()}`",
        f"- trade_windows_csv: `{trades_path}`",
        f"- factor_weights_csv: `{factors_path}`",
        "",
        "## Overview",
        "",
        pd.DataFrame([overview]).to_markdown(index=False),
        "",
        "## Status Counts",
        "",
        status_table.to_markdown(index=False),
        "",
        "## Bundle Usage",
        "",
        bundle_table.to_markdown(index=False),
        "",
        "## Feature Source Coverage",
        "",
        feature_source_table.to_markdown(index=False),
        "",
        "## Feature Alignment Coverage",
        "",
        coverage_table.to_markdown(index=False),
        "",
        "## Largest Loss Windows",
        "",
        loss_table.to_markdown(index=False) if not loss_table.empty else "_none_",
        "",
        "## Largest Win Windows",
        "",
        win_table.to_markdown(index=False) if not win_table.empty else "_none_",
        "",
        "## Pending Windows",
        "",
        pending[["asset", "outcome", "cost_usdc", "title"]].to_markdown(index=False) if not pending.empty else "_none_",
        "",
        "## Notes",
        "",
        "- `trade_windows.csv` is one row per traded window (`conditionId + outcomeIndex`).",
        "- `trade_factor_weights_long.csv` is one row per traded window x feature.",
        "- `matched_offset` uses the latest supported decision row (`offset 7/8/9`) not later than the first trade timestamp; if none exists, the earliest future row is used and flagged by `used_future_decision_row`.",
        "- Bundle selection is date-aware: it prefers the latest compatible dated bundle not later than the trade date; when none exists locally, it falls back to the earliest compatible dated bundle after the trade date.",
        "- `logreg_contribution` is computed on standardized features (`StandardScaler` output times logistic coefficient).",
        "- `lgb_shap_value` comes from `pred_contrib=True` on the LightGBM model for the matched trade row.",
        "- `feature_row_missing` means neither the local feature frame nor the supplemental Kraken-based rebuild could cover that traded window.",
        "- `feature_source=external_ohlcv_fallback` means the row was rebuilt only for analysis using external 1m OHLCV candles plus Polymarket oracle prices; it does not overwrite canonical research artifacts.",
        "- The external OHLCV fallback preserves price/volume structure, but Binance-specific taker-buy columns are unavailable there and are filled as `0.0`, so flow-style factors should be read more cautiously on fallback rows.",
    ]
    return "\n".join(lines) + "\n"


def _build_supplemental_feature_match(
    *,
    trade_record: dict[str, Any],
    runtime_offsets: dict[int, OffsetRuntime],
    trade_ts: pd.Timestamp,
    kraken_cache: dict[tuple[str, int], pd.DataFrame],
    oracle_cache: dict[tuple[str, int], pd.DataFrame],
) -> dict[str, Any] | None:
    asset = str(trade_record["asset"]).lower()
    cycle_start_ts = pd.Timestamp(trade_record["cycle_start_ts"])
    cycle_end_ts = pd.Timestamp(trade_record["cycle_end_ts"])
    max_offset = max(runtime_offsets)
    history_minutes = 720
    raw_start = cycle_start_ts - pd.Timedelta(minutes=history_minutes)
    raw_end = cycle_end_ts

    raw_klines = _fetch_fallback_klines(asset=asset, start_ts=raw_start, end_ts=raw_end, cache=kraken_cache)
    if raw_klines.empty:
        return None
    oracle_prices = _fetch_oracle_rows(asset=asset, cycle_start_ts=cycle_start_ts, cache=oracle_cache)
    if oracle_prices.empty:
        return None

    btc_klines = None
    if asset != "btc":
        btc_klines = _fetch_fallback_klines(asset="btc", start_ts=raw_start, end_ts=raw_end, cache=kraken_cache)
        if btc_klines.empty:
            return None

    feature_set = next(iter(runtime_offsets.values())).feature_set
    frame = build_feature_frame_df(
        raw_klines,
        feature_set=feature_set,
        oracle_prices=oracle_prices,
        btc_klines=btc_klines,
        cycle="15m",
    )
    frame["cycle_start_ts"] = pd.to_datetime(frame["cycle_start_ts"], utc=True, errors="coerce")
    frame["cycle_end_ts"] = pd.to_datetime(frame["cycle_end_ts"], utc=True, errors="coerce")
    frame["decision_ts"] = pd.to_datetime(frame["decision_ts"], utc=True, errors="coerce")
    frame["offset"] = pd.to_numeric(frame["offset"], errors="coerce").astype("Int64")
    return _match_feature_row(
        feature_frame=frame,
        cycle_start_ts=cycle_start_ts,
        trade_ts=trade_ts,
        supported_offsets=tuple(sorted(runtime_offsets)),
    )


def _fetch_fallback_klines(
    *,
    asset: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    cache: dict[tuple[str, int], pd.DataFrame],
) -> pd.DataFrame:
    key = (asset, int(pd.Timestamp(start_ts).timestamp()))
    cached = cache.get(key)
    if cached is not None:
        return cached.copy()

    frame = _fetch_kraken_klines(asset=asset, start_ts=start_ts, end_ts=end_ts)
    if frame.empty:
        frame = _fetch_coinbase_klines(asset=asset, start_ts=start_ts, end_ts=end_ts)
    cache[key] = frame
    return frame.copy()


def _fetch_kraken_klines(
    *,
    asset: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> pd.DataFrame:
    pair = KRAKEN_PAIR_BY_ASSET.get(asset)
    if not pair:
        return pd.DataFrame()

    response = requests.get(
        "https://api.kraken.com/0/public/OHLC",
        params={"pair": pair, "interval": 1, "since": int(pd.Timestamp(start_ts).timestamp())},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    result = payload.get("result") or {}
    rows = []
    for key_name, value in result.items():
        if key_name == "last" or not isinstance(value, list):
            continue
        rows = value
        break
    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(
        rows,
        columns=["open_time_sec", "open", "high", "low", "close", "vwap", "volume", "number_of_trades"],
    )
    frame["open_time"] = pd.to_datetime(pd.to_numeric(frame["open_time_sec"], errors="coerce"), unit="s", utc=True)
    for column in ("open", "high", "low", "close", "vwap", "volume", "number_of_trades"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["quote_asset_volume"] = frame["vwap"] * frame["volume"]
    frame["taker_buy_quote_volume"] = 0.0
    frame["close_time"] = frame["open_time"] + pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1)
    frame["ignore"] = 0.0
    frame = frame[
        [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_quote_volume",
            "ignore",
        ]
    ].copy()
    frame = frame.loc[
        frame["open_time"].ge(pd.Timestamp(start_ts)) & frame["open_time"].lt(pd.Timestamp(end_ts))
    ].reset_index(drop=True)
    return frame


def _fetch_coinbase_klines(
    *,
    asset: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> pd.DataFrame:
    product = COINBASE_PRODUCT_BY_ASSET.get(asset)
    if not product:
        return pd.DataFrame()

    session = requests.Session()
    cursor = pd.Timestamp(start_ts)
    chunks: list[pd.DataFrame] = []
    while cursor < pd.Timestamp(end_ts):
        chunk_end = min(cursor + pd.Timedelta(minutes=299), pd.Timestamp(end_ts))
        response = session.get(
            f"https://api.exchange.coinbase.com/products/{product}/candles",
            params={
                "granularity": 60,
                "start": cursor.isoformat().replace("+00:00", "Z"),
                "end": chunk_end.isoformat().replace("+00:00", "Z"),
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list) and payload:
            frame = pd.DataFrame(
                payload,
                columns=["open_time_sec", "low", "high", "open", "close", "volume"],
            )
            frame["open_time"] = pd.to_datetime(pd.to_numeric(frame["open_time_sec"], errors="coerce"), unit="s", utc=True)
            for column in ("open", "high", "low", "close", "volume"):
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            frame["quote_asset_volume"] = frame["close"] * frame["volume"]
            frame["number_of_trades"] = 0.0
            frame["taker_buy_quote_volume"] = 0.0
            frame["close_time"] = frame["open_time"] + pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1)
            frame["ignore"] = 0.0
            frame = frame[
                [
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_asset_volume",
                    "number_of_trades",
                    "taker_buy_quote_volume",
                    "ignore",
                ]
            ].copy()
            chunks.append(frame)
        cursor = chunk_end + pd.Timedelta(minutes=1)

    if not chunks:
        return pd.DataFrame()
    frame = pd.concat(chunks, ignore_index=True, sort=False)
    frame = frame.sort_values("open_time").drop_duplicates(subset=["open_time"], keep="last")
    frame = frame.loc[
        frame["open_time"].ge(pd.Timestamp(start_ts)) & frame["open_time"].lt(pd.Timestamp(end_ts))
    ].reset_index(drop=True)
    return frame


def _fetch_oracle_rows(
    *,
    asset: str,
    cycle_start_ts: pd.Timestamp,
    cache: dict[tuple[str, int], pd.DataFrame],
) -> pd.DataFrame:
    key = (asset, int(pd.Timestamp(cycle_start_ts).timestamp()))
    cached = cache.get(key)
    if cached is not None:
        return cached.copy()

    client = PolymarketOracleApiClient(timeout_sec=20.0)
    payload = client.fetch_crypto_price(
        symbol=asset.upper(),
        cycle_start_ts=int(pd.Timestamp(cycle_start_ts).timestamp()),
        cycle_seconds=900,
        sleep_sec=0.02,
        max_retries=4,
    )
    if not payload:
        return pd.DataFrame()
    frame = pd.DataFrame(
        [
            {
                "asset": asset,
                "cycle_start_ts": int(pd.Timestamp(cycle_start_ts).timestamp()),
                "cycle_end_ts": int((pd.Timestamp(cycle_start_ts) + pd.Timedelta(minutes=15)).timestamp()),
                "price_to_beat": payload.get("openPrice"),
                "final_price": payload.get("closePrice"),
                "source_price_to_beat": payload.get("source", "oracle_api"),
                "source_final_price": payload.get("source", "oracle_api"),
                "has_price_to_beat": payload.get("openPrice") is not None,
                "has_final_price": payload.get("closePrice") is not None,
                "has_both": payload.get("openPrice") is not None and payload.get("closePrice") is not None,
            }
        ]
    )
    cache[key] = frame
    return frame.copy()


if __name__ == "__main__":
    raise SystemExit(main())
