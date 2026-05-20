from __future__ import annotations

from collections import Counter
from typing import Any


RESEARCH_CANDIDATE_REQUIRED_FIELDS = (
    "primary_lever",
    "feature_width",
    "model_family",
    "feature_set",
    "factor_family_change",
    "expected_trade_count_effect",
    "difference_from_recent_failures",
)


def validate_research_candidate_meta(meta: dict[str, Any] | None) -> dict[str, object]:
    payload = dict(meta or {})
    failures = [
        f"missing_{field}"
        for field in RESEARCH_CANDIDATE_REQUIRED_FIELDS
        if not _present(payload.get(field))
    ]
    return {
        "passed": not failures,
        "failures": failures,
    }


def quick_screen_formal_confirmation_gate(
    quick_screen_run: dict[str, Any] | None,
    formal_run: dict[str, Any] | None,
) -> dict[str, object]:
    quick_payload = dict(quick_screen_run or {})
    formal_payload = dict(formal_run or {})
    quick_top_case = quick_payload.get("top_case") if isinstance(quick_payload.get("top_case"), dict) else {}
    formal_top_case = formal_payload.get("top_case") if isinstance(formal_payload.get("top_case"), dict) else {}
    quick_selected = _truthy(quick_top_case.get("selected_for_formal")) or _truthy(quick_payload.get("selected_for_formal"))
    quick_run_label = str(quick_payload.get("run_label") or "").strip()
    formal_run_label = str(formal_payload.get("run_label") or "").strip()
    if not quick_selected:
        return {
            "passed": True,
            "state": "not_selected",
            "reason": "quick_screen_candidate_not_selected_for_formal",
            "quick_screen_run_label": quick_run_label,
            "formal_run_label": formal_run_label,
        }
    if not formal_payload:
        return {
            "passed": False,
            "state": "formal_required",
            "reason": "quick_screen_selected_candidate_requires_formal_confirmation",
            "quick_screen_run_label": quick_run_label,
            "formal_run_label": "",
        }
    formal_trades = _int_or_none(formal_top_case.get("trades") or formal_top_case.get("trade_rows"))
    if formal_trades is None or formal_trades < 56:
        return {
            "passed": False,
            "state": "formal_sparse",
            "reason": "formal_confirmation_did_not_pass_dense_floor",
            "quick_screen_run_label": quick_run_label,
            "formal_run_label": formal_run_label,
        }
    return {
        "passed": True,
        "state": "formal_confirmed",
        "reason": "formal_confirmation_available",
        "quick_screen_run_label": quick_run_label,
        "formal_run_label": formal_run_label,
    }


def annotate_quick_screen_top_case(top_case: dict[str, Any] | None) -> dict[str, object] | None:
    if top_case is None:
        return None
    annotated = dict(top_case)
    if annotated.get("selected_for_formal") is None:
        annotated.pop("selected_for_formal", None)
        return annotated
    selected = _truthy(annotated.get("selected_for_formal"))
    annotated["promotion_state"] = "formal_required" if selected else "not_selected"
    return annotated


def build_framework_status_matrix(
    report: dict[str, Any],
    *,
    markets: list[str],
    tracks: list[str],
) -> dict[str, object]:
    normalized_markets = [_normalize_token(market) for market in markets if _normalize_token(market)]
    normalized_tracks = [_normalize_token(track) for track in tracks if _normalize_token(track)]
    slots: dict[str, dict[str, dict[str, object]]] = {
        track: {market: _empty_slot() for market in normalized_markets}
        for track in normalized_tracks
    }

    queue = report.get("queue") if isinstance(report.get("queue"), dict) else {}
    for item in queue.get("items") or []:
        if not isinstance(item, dict):
            continue
        track = _normalize_token(item.get("track"))
        market = _normalize_token(item.get("market"))
        if track not in slots or market not in slots[track]:
            continue
        status = _normalize_token(item.get("status")) or "unknown"
        counts = Counter(slots[track][market]["queue_status_counts"])
        counts[status] += 1
        slots[track][market]["queue_status_counts"] = dict(sorted(counts.items()))

    for worker in report.get("formal_workers") or []:
        if not isinstance(worker, dict):
            continue
        track = _normalize_token(worker.get("track"))
        market = _normalize_token(worker.get("market"))
        if track not in slots or market not in slots[track]:
            continue
        slots[track][market]["live_workers"] = int(slots[track][market]["live_workers"]) + 1
        run_label = str(worker.get("run_label") or "").strip()
        if run_label:
            slots[track][market]["live_run_labels"].append(run_label)

    for run in report.get("completed_runs") or []:
        if not isinstance(run, dict):
            continue
        top_case = run.get("top_case") if isinstance(run.get("top_case"), dict) else {}
        if not _truthy(top_case.get("selected_for_formal")):
            continue
        markets_for_run = [_normalize_token(market) for market in run.get("markets") or []]
        target_tracks = _candidate_tracks_for_run(run, report=report, available_tracks=normalized_tracks)
        for track in target_tracks:
            for market in markets_for_run:
                if market not in slots[track]:
                    continue
                slots[track][market]["promotion_state"] = "formal_required"
                slots[track][market]["best_quick_run_label"] = str(run.get("run_label") or "").strip()
                slots[track][market]["best_quick_trades"] = _int_or_none(
                    top_case.get("trades") or top_case.get("trade_rows")
                )
                slots[track][market]["best_quick_capture_rows"] = _int_or_none(
                    top_case.get("profitable_pool_capture_rows")
                )

    totals = {
        "queued": 0,
        "running": 0,
        "live_workers": 0,
        "formal_required": 0,
    }
    for track_slots in slots.values():
        for slot in track_slots.values():
            counts = slot.get("queue_status_counts") if isinstance(slot.get("queue_status_counts"), dict) else {}
            totals["queued"] += int(counts.get("queued", 0)) + int(counts.get("repair", 0))
            totals["running"] += int(counts.get("running", 0))
            totals["live_workers"] += int(slot.get("live_workers") or 0)
            if slot.get("promotion_state") == "formal_required":
                totals["formal_required"] += 1

    return {
        "markets": normalized_markets,
        "tracks": normalized_tracks,
        "slots": slots,
        "totals": totals,
    }


def _candidate_tracks_for_run(
    run: dict[str, Any],
    *,
    report: dict[str, Any],
    available_tracks: list[str],
) -> list[str]:
    explicit_track = _normalize_token(run.get("track"))
    if explicit_track in available_tracks:
        return [explicit_track]

    text = " ".join(
        str(run.get(field) or "").strip().lower()
        for field in ("suite_name", "run_label", "run_dir")
    )
    inferred: list[str] = []
    if "direction" in text and "direction_dense" in available_tracks:
        inferred.append("direction_dense")
    if "reversal" in text and "reversal_dense" in available_tracks:
        inferred.append("reversal_dense")
    if len(inferred) == 1:
        return inferred

    markets_for_run = {_normalize_token(market) for market in run.get("markets") or [] if _normalize_token(market)}
    queue = report.get("queue") if isinstance(report.get("queue"), dict) else {}
    matching_tracks = {
        _normalize_token(item.get("track"))
        for item in queue.get("items") or []
        if isinstance(item, dict)
        and _normalize_token(item.get("track")) in available_tracks
        and _normalize_token(item.get("market")) in markets_for_run
    }
    return sorted(matching_tracks) if len(matching_tracks) == 1 else []


def _empty_slot() -> dict[str, object]:
    return {
        "queue_status_counts": {},
        "live_workers": 0,
        "live_run_labels": [],
        "promotion_state": "",
        "best_quick_run_label": "",
        "best_quick_trades": None,
        "best_quick_capture_rows": None,
    }


def _present(value: Any) -> bool:
    if value in (None, "", [], {}):
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _normalize_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
