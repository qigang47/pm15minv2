from __future__ import annotations

import os
from pathlib import Path
import time

import pandas as pd

from pm15min.data.io.json_files import append_jsonl

from .decision import build_decision_snapshot, persist_decision_snapshot
from ..quotes import build_quote_snapshot
from .scoring import score_live_latest
from .scoring_bundle import resolve_bundle_resolution as _resolve_bundle_resolution


def check_live_latest(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    score_live_latest_fn,
    supports_feature_set_fn,
) -> dict[str, object]:
    signal = score_live_latest_fn(cfg, target=target, feature_set=feature_set, persist=False)
    checks: list[dict[str, object]] = []
    selection = signal.get("active_bundle") or {}
    checks.append(
        {
            "name": "active_bundle_exists",
            "ok": bool(selection),
            "detail": signal.get("active_bundle_selection_path"),
        }
    )
    checks.append(
        {
            "name": "active_bundle_dir_exists",
            "ok": signal.get("bundle_dir") is not None and Path(str(signal.get("bundle_dir"))).exists(),
            "detail": signal.get("bundle_dir"),
        }
    )
    checks.append(
        {
            "name": "bundle_feature_set_supported",
            "ok": supports_feature_set_fn(str(signal.get("builder_feature_set") or "")),
            "detail": signal.get("builder_feature_set"),
        }
    )
    liquidity_state = signal.get("liquidity_state") or {}
    liquidity_blocked = bool(liquidity_state.get("blocked", False))
    checks.append(
        {
            "name": "liquidity_guard_ok",
            "ok": not liquidity_blocked,
            "detail": {
                "snapshot_ts": liquidity_state.get("snapshot_ts"),
                "status": liquidity_state.get("status"),
                "reason": liquidity_state.get("reason"),
                "blocked": liquidity_blocked,
                "reason_codes": list(liquidity_state.get("reason_codes") or []),
            },
        }
    )

    offset_checks = []
    all_offsets_ok = True
    for row in signal.get("offset_signals") or []:
        coverage = row.get("coverage") or {}
        ok = (
            bool(row.get("score_valid", False))
            and int(coverage.get("effective_missing_feature_count") or 0) == 0
            and int(coverage.get("not_allowed_blacklist_count") or 0) == 0
            and int(coverage.get("nan_feature_count") or 0) == 0
        )
        all_offsets_ok = all_offsets_ok and ok
        offset_checks.append(
            {
                "offset": int(row["offset"]),
                "ok": ok,
                "score_valid": bool(row.get("score_valid", False)),
                "effective_missing_feature_count": int(coverage.get("effective_missing_feature_count") or 0),
                "not_allowed_blacklist_count": int(coverage.get("not_allowed_blacklist_count") or 0),
                "nan_feature_count": int(coverage.get("nan_feature_count") or 0),
                "confidence": float(row.get("confidence") or 0.0),
            }
        )
    checks.append(
        {
            "name": "offset_signals_valid",
            "ok": all_offsets_ok,
            "detail": offset_checks,
        }
    )
    overall_ok = all(bool(item.get("ok", False)) for item in checks)
    return {
        "domain": "live",
        "dataset": "live_signal_check",
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": f"{int(cfg.cycle_minutes)}m",
        "target": target,
        "ok": overall_ok,
        "checks": checks,
        "signal_snapshot": signal,
    }


def decide_live_latest(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
    session_state: dict[str, object] | None = None,
    orderbook_provider=None,
    score_live_latest_fn,
    build_quote_snapshot_fn=build_quote_snapshot,
    load_live_account_context_fn,
    build_decision_snapshot_fn=build_decision_snapshot,
    persist_decision_snapshot_fn=persist_decision_snapshot,
) -> dict[str, object]:
    signal_started = time.perf_counter()
    signal, signal_cache_hit = _resolve_live_signal_payload(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=persist,
        session_state=session_state,
        marker_source="decision",
        score_live_latest_fn=score_live_latest_fn,
    )
    signal_elapsed_ms = _elapsed_ms(signal_started)
    quote_started = time.perf_counter()
    quote = build_quote_snapshot_fn(
        cfg=cfg,
        signal_payload=signal,
        persist=persist,
        orderbook_provider=orderbook_provider,
    )
    quote_elapsed_ms = _elapsed_ms(quote_started)
    account_started = time.perf_counter()
    account_state = load_live_account_context_fn(cfg)
    account_elapsed_ms = _elapsed_ms(account_started)
    decision_started = time.perf_counter()
    payload = build_decision_snapshot_fn(
        signal,
        quote,
        account_state,
        session_state=session_state,
        rewrite_root=cfg.layout.rewrite.root,
        orderbook_provider=orderbook_provider,
    )
    decision_elapsed_ms = _elapsed_ms(decision_started)
    payload["timings_ms"] = {
        "signal_stage_ms": signal_elapsed_ms,
        "quote_stage_ms": quote_elapsed_ms,
        "account_context_stage_ms": account_elapsed_ms,
        "decision_build_stage_ms": decision_elapsed_ms,
        "signal_cache_hit": bool(signal_cache_hit),
    }
    if persist:
        paths = persist_decision_snapshot_fn(rewrite_root=cfg.layout.rewrite.root, payload=payload)
        payload["latest_decision_path"] = str(paths["latest"])
        payload["decision_snapshot_path"] = str(paths["snapshot"])
    return payload


def prewarm_live_signal_cache(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = False,
    session_state: dict[str, object] | None = None,
    marker_source: str = "prewarm",
    score_live_latest_fn,
) -> dict[str, object]:
    started = time.perf_counter()
    payload, cache_hit = _resolve_live_signal_payload(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=persist,
        session_state=session_state,
        marker_source=marker_source,
        score_live_latest_fn=score_live_latest_fn,
    )
    elapsed_ms = _elapsed_ms(started)
    now_utc = pd.Timestamp.now(tz="UTC")
    valid_until_ts = _resolve_signal_cache_valid_until(payload=payload, now_utc=now_utc)
    return {
        "status": "ok",
        "marker_source": str(marker_source or "prewarm"),
        "cache_hit": bool(cache_hit),
        "elapsed_ms": elapsed_ms,
        "snapshot_ts": payload.get("snapshot_ts"),
        "latest_feature_decision_ts": payload.get("latest_feature_decision_ts"),
        "valid_until_ts": None if valid_until_ts is None or pd.isna(valid_until_ts) else valid_until_ts.isoformat(),
        "offsets": [
            int(row["offset"])
            for row in list(payload.get("offset_signals") or [])
            if isinstance(row, dict) and row.get("offset") is not None
        ],
    }


def prewarm_live_signal_inputs(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    resolve_live_profile_spec_fn,
    get_active_bundle_selection_fn,
    resolve_model_bundle_dir_fn,
    read_model_bundle_manifest_fn,
    supports_feature_set_fn,
    build_live_feature_frame_fn,
    iso_or_none_fn,
) -> dict[str, object]:
    started = time.perf_counter()
    bundle = _resolve_bundle_resolution(
        cfg,
        target=target,
        feature_set=feature_set,
        resolve_live_profile_spec_fn=resolve_live_profile_spec_fn,
        get_active_bundle_selection_fn=get_active_bundle_selection_fn,
        resolve_model_bundle_dir_fn=resolve_model_bundle_dir_fn,
        read_model_bundle_manifest_fn=read_model_bundle_manifest_fn,
        supports_feature_set_fn=supports_feature_set_fn,
    )
    active_offsets = _bundle_offsets(bundle.bundle_dir)
    features = build_live_feature_frame_fn(
        cfg,
        feature_set=bundle.builder_feature_set,
        retain_offsets=active_offsets,
    )
    latest_feature_decision_ts = None
    if isinstance(features, pd.DataFrame) and "decision_ts" in features.columns and not features.empty:
        latest_feature_decision_ts = iso_or_none_fn(features["decision_ts"].max())
    marker_source = "prewarm_prepare"
    payload = {
        "status": "ok",
        "marker_source": marker_source,
        "elapsed_ms": _elapsed_ms(started),
        "builder_feature_set": bundle.builder_feature_set,
        "bundle_feature_set": bundle.manifest_feature_set,
        "feature_rows": int(len(features)) if isinstance(features, pd.DataFrame) else 0,
        "latest_feature_decision_ts": latest_feature_decision_ts,
        "offsets": [int(offset) for offset in active_offsets],
    }
    _append_signal_refresh_marker(
        cfg,
        target=target,
        feature_set=feature_set,
        cache_key=_signal_cache_key(cfg=cfg, target=target, feature_set=feature_set) + "|prepare",
        source=marker_source,
        elapsed_ms=_elapsed_ms(started),
        payload={
            **payload,
            "offset_signals": [
                {
                    "offset": int(offset),
                    "status": "prepared",
                }
                for offset in active_offsets
            ],
        },
    )
    return payload


def prewarm_live_signal_preview(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    score_live_latest_fn,
) -> dict[str, object]:
    started = time.perf_counter()
    payload = score_live_latest_fn(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=False,
        allow_preview_open_bar=True,
    )
    _append_signal_refresh_marker(
        cfg,
        target=target,
        feature_set=feature_set,
        cache_key=_signal_cache_key(cfg=cfg, target=target, feature_set=feature_set) + "|preview",
        source="prewarm_preview",
        elapsed_ms=_elapsed_ms(started),
        payload=payload,
    )
    return {
        "status": "ok",
        "elapsed_ms": _elapsed_ms(started),
        "snapshot_ts": payload.get("snapshot_ts"),
        "latest_feature_decision_ts": payload.get("latest_feature_decision_ts"),
        "feature_rows": int(payload.get("feature_rows") or 0),
        "offsets": [
            int(row["offset"])
            for row in list(payload.get("offset_signals") or [])
            if isinstance(row, dict) and row.get("offset") is not None
        ],
        "preview_only": True,
    }


def _resolve_live_signal_payload(
    cfg,
    *,
    target: str,
    feature_set: str | None,
    persist: bool,
    session_state: dict[str, object] | None,
    marker_source: str,
    score_live_latest_fn,
) -> tuple[dict[str, object], bool]:
    now_utc = pd.Timestamp.now(tz="UTC")
    cache_enabled = _env_bool("PM15MIN_LIVE_WINDOW_SIGNAL_CACHE", default=True)
    cache_key = _signal_cache_key(cfg=cfg, target=target, feature_set=feature_set)
    if cache_enabled and not _should_bypass_signal_cache_lookup(marker_source=marker_source):
        cached = _load_cached_signal_payload(
            session_state=session_state,
            cache_key=cache_key,
            now_utc=now_utc,
        )
        if cached is not None:
            return cached, True
    refresh_started = time.perf_counter()
    payload = score_live_latest_fn(cfg, target=target, feature_set=feature_set, persist=persist)
    refresh_elapsed_ms = _elapsed_ms(refresh_started)
    if cache_enabled:
        _store_cached_signal_payload(
            session_state=session_state,
            cache_key=cache_key,
            payload=payload,
            now_utc=now_utc,
        )
    _append_signal_refresh_marker(
        cfg,
        target=target,
        feature_set=feature_set,
        cache_key=cache_key,
        source=marker_source,
        elapsed_ms=refresh_elapsed_ms,
        payload=payload,
    )
    return payload, False


def _bundle_offsets(bundle_dir: Path) -> tuple[int, ...]:
    offsets: list[int] = []
    for path in sorted((bundle_dir / "offsets").glob("offset=*")):
        try:
            offsets.append(int(path.name.split("=", 1)[1]))
        except Exception:
            continue
    return tuple(sorted(set(offsets)))


def _signal_cache_key(*, cfg, target: str, feature_set: str | None) -> str:
    feature_token = "" if feature_set is None else str(feature_set).strip().lower()
    return "|".join(
        [
            str(cfg.asset.slug),
            str(cfg.profile),
            str(int(cfg.cycle_minutes)),
            str(target or "direction").strip().lower(),
            feature_token,
        ]
    )


def _should_bypass_signal_cache_lookup(*, marker_source: str) -> bool:
    return str(marker_source or "").strip().lower() == "prewarm_finalize"


def _session_signal_cache(session_state: dict[str, object] | None, *, create: bool = True) -> dict[str, object]:
    if not isinstance(session_state, dict):
        return {}
    raw = session_state.get("live_signal_cache")
    if isinstance(raw, dict):
        return raw
    if not create:
        return {}
    raw = {}
    session_state["live_signal_cache"] = raw
    return raw


def _load_cached_signal_payload(
    *,
    session_state: dict[str, object] | None,
    cache_key: str,
    now_utc: pd.Timestamp,
) -> dict[str, object] | None:
    cache = _session_signal_cache(session_state, create=False)
    raw = cache.get(cache_key)
    if not isinstance(raw, dict):
        return None
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        return None
    valid_until_ts = pd.to_datetime(raw.get("valid_until_ts"), utc=True, errors="coerce")
    if valid_until_ts is None or pd.isna(valid_until_ts):
        return None
    if now_utc >= valid_until_ts:
        cache.pop(cache_key, None)
        return None
    return payload


def _store_cached_signal_payload(
    *,
    session_state: dict[str, object] | None,
    cache_key: str,
    payload: dict[str, object],
    now_utc: pd.Timestamp,
) -> None:
    cache = _session_signal_cache(session_state)
    valid_until_ts = _resolve_signal_cache_valid_until(payload=payload, now_utc=now_utc)
    if valid_until_ts is None or pd.isna(valid_until_ts):
        cache.pop(cache_key, None)
        return
    cache[cache_key] = {
        "valid_until_ts": valid_until_ts.isoformat(),
        "payload": payload,
    }


def _resolve_signal_cache_valid_until(
    *,
    payload: dict[str, object],
    now_utc: pd.Timestamp,
) -> pd.Timestamp | None:
    transitions: list[pd.Timestamp] = []
    for row in list(payload.get("offset_signals") or []):
        if not isinstance(row, dict):
            continue
        for key in ("window_start_ts", "window_end_ts", "cycle_end_ts"):
            ts = pd.to_datetime(row.get(key), utc=True, errors="coerce")
            if ts is None or pd.isna(ts):
                continue
            if ts > now_utc:
                transitions.append(ts)
    if not transitions:
        return None
    return min(transitions)


def _append_signal_refresh_marker(
    cfg,
    *,
    target: str,
    feature_set: str | None,
    cache_key: str,
    source: str,
    elapsed_ms: float,
    payload: dict[str, object],
) -> None:
    path = _signal_refresh_marker_path(cfg=cfg, target=target)
    append_jsonl(
        path,
        {
            "ts": pd.Timestamp.now(tz="UTC").isoformat(),
            "market": str(cfg.asset.slug),
            "profile": str(cfg.profile),
            "cycle": f"{int(cfg.cycle_minutes)}m",
            "target": str(target or "direction"),
            "source": str(source or "unknown"),
            "cache_key": cache_key,
            "cache_hit": False,
            "elapsed_ms": float(elapsed_ms),
            "snapshot_ts": _iso_or_none(payload.get("snapshot_ts")),
            "latest_feature_decision_ts": _iso_or_none(payload.get("latest_feature_decision_ts")),
            "builder_feature_set": _string_or_none(payload.get("builder_feature_set")),
            "requested_feature_set": _string_or_none(feature_set),
            "feature_rows": _int_or_none(payload.get("feature_rows")),
            "offset_rows": _compact_offset_rows(payload.get("offset_signals")),
        },
    )


def _signal_refresh_marker_path(*, cfg, target: str) -> Path:
    root = Path(str(cfg.layout.rewrite.root))
    market = str(cfg.asset.slug)
    profile = str(cfg.profile)
    cycle = f"{int(cfg.cycle_minutes)}m"
    target_token = str(target or "direction").strip().lower() or "direction"
    return (
        root
        / "var"
        / "live"
        / "logs"
        / "markers"
        / f"signal_refresh_cycle={cycle}_asset={market}_profile={profile}_target={target_token}.jsonl"
    )


def _compact_offset_rows(rows: object) -> list[dict[str, object]]:
    compact: list[dict[str, object]] = []
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        compact.append(
            {
                "offset": _int_or_none(row.get("offset")),
                "status": _string_or_none(row.get("status")),
                "decision_ts": _iso_or_none(row.get("decision_ts")),
                "window_start_ts": _iso_or_none(row.get("window_start_ts")),
                "window_end_ts": _iso_or_none(row.get("window_end_ts")),
                "cycle_end_ts": _iso_or_none(row.get("cycle_end_ts")),
            }
        )
    return compact


def _iso_or_none(value: object) -> str | None:
    if value in (None, ""):
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if ts is None or pd.isna(ts):
        return None
    return ts.isoformat()


def _string_or_none(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _int_or_none(value: object) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw in (None, ""):
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _elapsed_ms(started_at: float) -> float:
    return round(max(0.0, (time.perf_counter() - float(started_at)) * 1000.0), 3)


def quote_live_latest(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
    score_live_latest_fn,
    build_quote_snapshot_fn=build_quote_snapshot,
) -> dict[str, object]:
    signal = score_live_latest_fn(cfg, target=target, feature_set=feature_set, persist=persist)
    return build_quote_snapshot_fn(cfg=cfg, signal_payload=signal, persist=persist)
