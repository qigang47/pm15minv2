from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.pipelines.binance_klines import sync_binance_klines_1m
from pm15min.data.pipelines.market_catalog import sync_market_catalog
from pm15min.data.queries.loaders import load_binance_klines_1m, load_oracle_prices_table
from pm15min.live.oracle import build_live_runtime_oracle_prices
from ..account import summarize_account_state_payload
from ..account import load_latest_open_orders_snapshot, load_latest_positions_snapshot
from ..layout import LiveStateLayout
from ..persistence import write_live_payload_pair
from pm15min.research.features.builders import build_feature_frame as build_feature_frame_df
from pm15min.research.features.registry import feature_set_columns

DEFAULT_LIVE_TRADE_INPUT_REFRESH_RETRY_SECONDS = 30.0
DEFAULT_LIVE_MARKET_CATALOG_MAX_AGE_SECONDS = 300.0
DEFAULT_LIVE_BINANCE_KLINES_MAX_AGE_SECONDS = 120.0
DEFAULT_LIVE_MARKET_CATALOG_LOOKBACK_HOURS = 24
DEFAULT_LIVE_MARKET_CATALOG_LOOKAHEAD_HOURS = 24
DEFAULT_LIVE_BINANCE_LOOKBACK_MINUTES = 2880
DEFAULT_LIVE_BINANCE_BATCH_LIMIT = 1000


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
    ensure_live_trade_inputs_fresh(cfg)
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


def ensure_live_trade_inputs_fresh(cfg) -> dict[str, object]:
    now_utc = datetime.now(timezone.utc)
    cycle = f"{int(cfg.cycle_minutes)}m"
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=cycle,
        surface="live",
        root=cfg.layout.rewrite.root,
    )
    summaries = {
        "market_catalog": _ensure_live_market_catalog_fresh(data_cfg=data_cfg, now_utc=now_utc),
        "binance_primary": _ensure_live_binance_klines_fresh(data_cfg=data_cfg, now_utc=now_utc),
    }
    if cfg.asset.slug != "btc":
        btc_cfg = DataConfig.build(
            market="btc",
            cycle=cycle,
            surface="live",
            root=cfg.layout.rewrite.root,
        )
        summaries["binance_btc"] = _ensure_live_binance_klines_fresh(data_cfg=btc_cfg, now_utc=now_utc)
    return summaries


def _ensure_live_market_catalog_fresh(*, data_cfg: DataConfig, now_utc: datetime) -> dict[str, object]:
    target_path = data_cfg.layout.market_catalog_table_path
    max_age_seconds = _env_float(
        "PM15MIN_LIVE_MARKET_CATALOG_MAX_AGE_SECONDS",
        default=DEFAULT_LIVE_MARKET_CATALOG_MAX_AGE_SECONDS,
    )
    if _path_is_fresh(target_path=target_path, now_utc=now_utc, max_age_seconds=max_age_seconds):
        return {"status": "fresh", "path": str(target_path)}
    _refresh_if_needed(
        data_cfg=data_cfg,
        lock_name=f"market_catalog_{data_cfg.asset.slug}",
        target_path=target_path,
        now_utc=now_utc,
        max_age_seconds=max_age_seconds,
        refresh_fn=lambda: sync_market_catalog(
            data_cfg,
            start_ts=int((now_utc - timedelta(hours=DEFAULT_LIVE_MARKET_CATALOG_LOOKBACK_HOURS)).timestamp()),
            end_ts=int((now_utc + timedelta(hours=DEFAULT_LIVE_MARKET_CATALOG_LOOKAHEAD_HOURS)).timestamp()),
            now=now_utc,
        ),
    )
    _require_fresh_path(
        target_path=target_path,
        now_utc=now_utc,
        max_age_seconds=max_age_seconds,
        dataset_name=f"market_catalog:{data_cfg.asset.slug}",
    )
    return {"status": "refreshed", "path": str(target_path)}


def _ensure_live_binance_klines_fresh(*, data_cfg: DataConfig, now_utc: datetime) -> dict[str, object]:
    target_path = data_cfg.layout.binance_klines_path()
    max_age_seconds = _env_float(
        "PM15MIN_LIVE_BINANCE_MAX_AGE_SECONDS",
        default=DEFAULT_LIVE_BINANCE_KLINES_MAX_AGE_SECONDS,
    )
    if _path_is_fresh(target_path=target_path, now_utc=now_utc, max_age_seconds=max_age_seconds):
        return {"status": "fresh", "path": str(target_path)}
    _refresh_if_needed(
        data_cfg=data_cfg,
        lock_name=f"binance_klines_{data_cfg.asset.binance_symbol}",
        target_path=target_path,
        now_utc=now_utc,
        max_age_seconds=max_age_seconds,
        refresh_fn=lambda: sync_binance_klines_1m(
            data_cfg,
            symbol=data_cfg.asset.binance_symbol,
            now=now_utc,
            lookback_minutes=DEFAULT_LIVE_BINANCE_LOOKBACK_MINUTES,
            batch_limit=DEFAULT_LIVE_BINANCE_BATCH_LIMIT,
        ),
    )
    _require_fresh_path(
        target_path=target_path,
        now_utc=now_utc,
        max_age_seconds=max_age_seconds,
        dataset_name=f"binance_klines:{data_cfg.asset.binance_symbol}",
    )
    return {"status": "refreshed", "path": str(target_path)}


def _refresh_if_needed(
    *,
    data_cfg: DataConfig,
    lock_name: str,
    target_path: Path,
    now_utc: datetime,
    max_age_seconds: float,
    refresh_fn,
) -> None:
    lock_path, state_path = _refresh_coordination_paths(data_cfg=data_cfg, lock_name=lock_name)
    with _exclusive_lock(lock_path):
        if _path_is_fresh(target_path=target_path, now_utc=now_utc, max_age_seconds=max_age_seconds):
            return
        last_attempt_ts = _load_last_refresh_attempt(state_path)
        retry_seconds = _env_float(
            "PM15MIN_LIVE_TRADE_INPUT_REFRESH_RETRY_SECONDS",
            default=DEFAULT_LIVE_TRADE_INPUT_REFRESH_RETRY_SECONDS,
        )
        if (
            last_attempt_ts is not None
            and retry_seconds > 0.0
            and (now_utc - last_attempt_ts).total_seconds() < retry_seconds
        ):
            return
        _store_last_refresh_attempt(state_path=state_path, attempted_at=now_utc)
        refresh_fn()
        if _path_is_fresh(target_path=target_path, now_utc=now_utc, max_age_seconds=max_age_seconds):
            _store_last_refresh_attempt(state_path=state_path, attempted_at=now_utc, success=True)


def _refresh_coordination_paths(*, data_cfg: DataConfig, lock_name: str) -> tuple[Path, Path]:
    lock_dir = data_cfg.layout.surface_var_root / "locks" / "trade_inputs"
    lock_dir.mkdir(parents=True, exist_ok=True)
    return (
        lock_dir / f"{lock_name}.lock",
        lock_dir / f"{lock_name}.state.json",
    )


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _load_last_refresh_attempt(state_path: Path) -> datetime | None:
    if not state_path.exists():
        return None
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    raw = payload.get("last_attempt_ts")
    if raw in (None, ""):
        return None
    try:
        return pd.Timestamp(raw).to_pydatetime()
    except Exception:
        return None


def _store_last_refresh_attempt(*, state_path: Path, attempted_at: datetime, success: bool = False) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_attempt_ts": pd.Timestamp(attempted_at).isoformat(),
        "last_attempt_success": bool(success),
    }
    state_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _path_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


def _path_is_fresh(*, target_path: Path, now_utc: datetime, max_age_seconds: float) -> bool:
    if not _path_exists(target_path):
        return False
    age = max(0.0, now_utc.timestamp() - target_path.stat().st_mtime)
    return age <= max(0.0, float(max_age_seconds))


def _require_fresh_path(*, target_path: Path, now_utc: datetime, max_age_seconds: float, dataset_name: str) -> None:
    if _path_is_fresh(target_path=target_path, now_utc=now_utc, max_age_seconds=max_age_seconds):
        return
    age_seconds = None
    if _path_exists(target_path):
        age_seconds = max(0.0, now_utc.timestamp() - target_path.stat().st_mtime)
    raise RuntimeError(
        f"live_trade_input_stale:{dataset_name}:path={target_path}:age_seconds={age_seconds}"
    )


def _env_float(name: str, *, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


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
    return write_live_payload_pair(
        payload=payload,
        latest_path=latest_path,
        snapshot_path=snapshot_path,
        write_snapshot_history=False,
    )


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
    excluded_columns = {"decision_ts", "cycle_start_ts", "cycle_end_ts", "offset"}
    for column in rows.columns:
        if column in excluded_columns:
            continue
        value = row.get(column)
        if pd.isna(value):
            snapshot[column] = None
            continue
        if isinstance(value, bool):
            snapshot[column] = bool(value)
            continue
        numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(numeric_value):
            continue
        float_value = float(numeric_value)
        snapshot[column] = int(float_value) if float_value.is_integer() else float_value
    return snapshot


def iso_or_none(value) -> str | None:
    if value is None:
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.isoformat()
