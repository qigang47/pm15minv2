from __future__ import annotations

from pathlib import Path

from .decision import build_decision_snapshot, persist_decision_snapshot
from ..quotes import build_quote_snapshot
from .scoring import score_live_latest


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
    signal = score_live_latest_fn(cfg, target=target, feature_set=feature_set, persist=persist)
    quote = build_quote_snapshot_fn(
        cfg=cfg,
        signal_payload=signal,
        persist=persist,
        orderbook_provider=orderbook_provider,
    )
    account_state = load_live_account_context_fn(cfg)
    payload = build_decision_snapshot_fn(
        signal,
        quote,
        account_state,
        session_state=session_state,
        rewrite_root=cfg.layout.rewrite.root,
        orderbook_provider=orderbook_provider,
    )
    if persist:
        paths = persist_decision_snapshot_fn(rewrite_root=cfg.layout.rewrite.root, payload=payload)
        payload["latest_decision_path"] = str(paths["latest"])
        payload["decision_snapshot_path"] = str(paths["snapshot"])
    return payload


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
