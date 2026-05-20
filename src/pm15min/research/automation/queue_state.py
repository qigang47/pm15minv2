from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from pm15min.data.io import write_json_atomic
from pm15min.research.automation.framework_gates import validate_research_candidate_meta
from pm15min.research.automation.search_ledger import build_attempt_record
from pm15min.research.automation.search_policy import validate_candidate_against_policy


QueueInspectRun = Callable[[Path], dict[str, object]]
QueueLauncher = Callable[[dict[str, object]], dict[str, object] | None]
QueueBatchLauncher = Callable[[list[dict[str, object]]], dict[str, object] | None]
DEFAULT_QUEUE_TRACK = "default"
UNKNOWN_QUEUE_TRACK = "unknown"
BLOCKED_QUEUE_ACTION = "blocked"
DEFAULT_MAX_LIVE_RUNS = 10
DEFAULT_MAX_QUEUED_ITEMS = 24
DEFAULT_TRACK_SLOT_CAPS = {
    "direction_dense": 5,
    "reversal_dense": 5,
}
DENSE_QUICK_SCREEN_TRACKS = frozenset({"direction_dense", "reversal_dense"})
SHARED_QUICK_SCREEN_MARKETS = frozenset({"sol", "xrp"})
FIXED_TRACK_SLOT_CAPS_ENV = "PM15MIN_FIXED_TRACK_SLOT_CAPS_JSON"
ALLOWED_QUEUE_MARKETS_ENV = "PM15MIN_ALLOWED_QUEUE_MARKETS"
AUTO_REFILL_REASON_PREFIX = "auto_refill_"


def experiment_queue_path(project_root: Path) -> Path:
    root = Path(project_root).resolve()
    return root / "var" / "research" / "autorun" / "experiment-queue.json"


def build_queue_item(
    *,
    market: str,
    suite_name: str,
    run_label: str,
    action: str,
    status: str = "queued",
    priority: int = 100,
    reason: str = "",
    retry_count: int = 0,
    track: str | None = None,
    session_dir: str | Path | None = None,
    program_path: str | Path | None = None,
) -> dict[str, object]:
    stamp = _utc_now()
    normalized_market = str(market).strip().lower()
    normalized_suite = str(suite_name).strip()
    normalized_run = str(run_label).strip()
    normalized_action = str(action).strip().lower()
    normalized_status = str(status).strip().lower()
    normalized_track = _resolve_item_track(
        {
            "track": track,
            "suite_name": normalized_suite,
            "run_label": normalized_run,
            "session_dir": session_dir,
            "program_path": program_path,
        }
    )
    return {
        "id": _queue_item_id(normalized_market, normalized_track, normalized_suite, normalized_run),
        "market": normalized_market,
        "suite_name": normalized_suite,
        "run_label": normalized_run,
        "action": normalized_action,
        "status": normalized_status,
        "priority": int(priority),
        "reason": str(reason or "").strip(),
        "retry_count": max(0, int(retry_count)),
        "track": normalized_track,
        "session_dir": _stringify_queue_path(session_dir),
        "program_path": _stringify_queue_path(program_path),
        "created_at": stamp,
        "updated_at": stamp,
    }


def load_experiment_queue(
    project_root: Path,
    *,
    apply_fixed_track_slot_caps: bool = True,
) -> dict[str, object]:
    root = Path(project_root).resolve()
    path = experiment_queue_path(project_root)
    if not path.exists():
        return _empty_queue_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _empty_queue_payload()
    if not isinstance(payload, dict):
        return _empty_queue_payload()
    items = payload.get("items")
    payload["items"] = _filter_queue_items_for_allowed_markets(
        [
        _normalize_queue_item(root, entry)
        for entry in items or []
        if isinstance(entry, dict)
        ]
    )
    payload.setdefault("version", 1)
    payload.setdefault("max_live_runs", DEFAULT_MAX_LIVE_RUNS)
    payload.setdefault("max_queued_items", DEFAULT_MAX_QUEUED_ITEMS)
    payload["track_slot_caps"] = _normalize_track_slot_caps(
        payload.get("track_slot_caps"),
        apply_fixed=apply_fixed_track_slot_caps,
    )
    payload.setdefault("updated_at", _utc_now())
    return payload


def save_experiment_queue(project_root: Path, payload: dict[str, object]) -> dict[str, object]:
    root = Path(project_root).resolve()
    normalized = dict(payload)
    normalized["version"] = 1
    normalized["max_live_runs"] = max(1, int(normalized.get("max_live_runs") or DEFAULT_MAX_LIVE_RUNS))
    normalized["max_queued_items"] = max(1, int(normalized.get("max_queued_items") or DEFAULT_MAX_QUEUED_ITEMS))
    normalized["track_slot_caps"] = _normalize_track_slot_caps(normalized.get("track_slot_caps"))
    normalized["updated_at"] = _utc_now()
    items = normalized.get("items")
    normalized["items"] = _filter_queue_items_for_allowed_markets(
        [
        _apply_policy_gate_to_queue_item(root, _normalize_queue_item(root, item))
        for item in (items if isinstance(items, list) else [])
        if isinstance(item, dict)
        ]
    )
    normalized["items"] = _prune_pending_queue_items(
        normalized["items"],
        max_queued_items=int(normalized["max_queued_items"]),
    )
    write_json_atomic(normalized, experiment_queue_path(project_root))
    return normalized


def upsert_queue_item(project_root: Path, item: dict[str, object]) -> dict[str, object]:
    root = Path(project_root).resolve()
    payload = load_experiment_queue(project_root)
    items = [dict(entry) for entry in payload.get("items") or [] if isinstance(entry, dict)]
    target_item = _normalize_queue_item(root, item)
    target_item = _apply_policy_gate_to_queue_item(root, target_item)
    target_id = str(target_item.get("id") or "").strip()

    retained: list[dict[str, object]] = []
    replaced = False
    replaced_created_at: str | None = None
    for entry in items:
        entry_id = str(entry.get("id") or "").strip()

        if entry_id and entry_id == target_id:
            replaced = True
            replaced_created_at = str(entry.get("created_at") or "").strip() or None
            continue
        retained.append(entry)

    if replaced_created_at:
        target_item["created_at"] = replaced_created_at
    target_item["updated_at"] = _utc_now()
    if not _queue_item_market_allowed(target_item):
        payload["items"] = retained
        return save_experiment_queue(project_root, payload)
    retained.append(target_item)
    payload["items"] = retained
    return save_experiment_queue(project_root, payload)


def validate_queue_item_search_policy(
    project_root: Path,
    item: dict[str, object],
) -> dict[str, object]:
    root = Path(project_root).resolve()
    target_item = _normalize_queue_item(root, dict(item))
    track = _policy_track_for_item(root, target_item)
    if track is None:
        return {"allowed": True, "reason": "", "candidate": {}, "attempts": [], "decision": {}}
    candidate = build_attempt_record(root, _queue_item_run_payload(target_item), track=track)
    market = str(target_item.get("market") or "").strip().lower()
    attempts = _recent_attempt_records_for_policy(root, market=market, track=track)
    validation = validate_candidate_against_policy(candidate, attempts)
    validation["candidate"] = candidate
    validation["attempts"] = attempts
    return validation


def validate_experiment_launch_search_policy(
    project_root: Path,
    *,
    suite_name: str,
    run_label: str,
    market: str,
    track: str | None = None,
) -> dict[str, object]:
    root = Path(project_root).resolve()
    normalized_market = str(market or "").strip().lower()
    if not normalized_market:
        normalized_market = _single_suite_market_for_policy(root, suite_name) or ""
    policy_track = _normalize_policy_track(track) or _infer_policy_track_from_suite(root, suite_name)
    if not normalized_market or policy_track is None:
        return {"allowed": True, "reason": "", "candidate": {}, "attempts": [], "decision": {}}
    candidate = build_attempt_record(
        root,
        {
            "suite_name": str(suite_name or "").strip(),
            "run_label": str(run_label or "").strip(),
            "market": normalized_market,
            "top_case": {},
        },
        track=policy_track,
    )
    attempts = _recent_attempt_records_for_policy(root, market=normalized_market, track=policy_track)
    validation = validate_candidate_against_policy(candidate, attempts)
    validation["candidate"] = candidate
    validation["attempts"] = attempts
    return validation


def select_launchable_queue_items(
    payload: dict[str, object],
    *,
    max_live_runs: int,
    live_workers: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    known_items = [dict(item) for item in payload.get("items") or [] if isinstance(item, dict)]
    live_payload = [
        _resolve_live_worker_metadata(dict(item), known_items)
        for item in live_workers or []
        if isinstance(item, dict)
    ]
    live_payload = [
        item
        for item in live_payload
        if _queue_item_market_allowed(item)
    ]
    running_items = [
        dict(item)
        for item in payload.get("items") or []
        if isinstance(item, dict) and str(item.get("status") or "").strip().lower() == "running"
    ]
    occupied_exact: dict[tuple[str, str, str, str], dict[str, object]] = {}
    occupied_fallback: dict[tuple[str, ...], dict[str, object]] = {}

    for item in running_items:
        _register_occupied_item(item, occupied_exact=occupied_exact, occupied_fallback=occupied_fallback)
    for worker in live_payload:
        _register_occupied_item(worker, occupied_exact=occupied_exact, occupied_fallback=occupied_fallback)

    capacity = max(0, int(max_live_runs) - len(occupied_exact) - len(occupied_fallback))
    if capacity <= 0:
        return []

    track_usage: dict[str, int] = {}
    for item in occupied_exact.values():
        track = _item_track(item)
        track_usage[track] = track_usage.get(track, 0) + 1
    unknown_track_usage = len(occupied_fallback)
    track_slot_caps = _normalize_track_slot_caps(payload.get("track_slot_caps"))
    selected: list[dict[str, object]] = []
    selected_track_usage: dict[str, int] = {}
    occupied_shared_quick_screen_keys = _occupied_shared_quick_screen_keys(
        payload,
        live_workers=live_payload,
    )
    occupied_market_fallbacks = {
        key[1]
        for key in occupied_fallback
        if len(key) == 2 and key[0] == "market"
    }
    occupied_suite_run_market_keys = {
        tuple(key[1:])
        for key in occupied_fallback
        if len(key) == 4 and key[0] == "suite_run_market"
    }
    selected_shared_quick_screen_keys: set[tuple[str, str, str, str]] = set()
    queue_items = [
        dict(item)
        for item in payload.get("items") or []
        if isinstance(item, dict)
        and str(item.get("status") or "").strip().lower() in {"queued", "repair"}
    ]
    queue_items.sort(key=_queue_sort_key)
    for item in queue_items:
        market_track = _market_track_key(item)
        if market_track is None:
            continue
        market, track = market_track
        if market in occupied_market_fallbacks:
            continue
        suite_run_market_key = _suite_run_market_key(item)
        if suite_run_market_key is not None and suite_run_market_key in occupied_suite_run_market_keys:
            continue
        shared_quick_screen_key = _shared_quick_screen_key(item)
        if shared_quick_screen_key is not None and (
            shared_quick_screen_key in occupied_shared_quick_screen_keys
            or shared_quick_screen_key in selected_shared_quick_screen_keys
        ):
            continue
        track_cap = track_slot_caps.get(track)
        if track_cap is not None:
            used = track_usage.get(track, 0) + selected_track_usage.get(track, 0)
            if used >= int(track_cap):
                continue
        selected.append(item)
        selected_track_usage[track] = selected_track_usage.get(track, 0) + 1
        if shared_quick_screen_key is not None:
            selected_shared_quick_screen_keys.add(shared_quick_screen_key)
        if len(selected) >= capacity:
            break
    return selected


def summarize_queue_items(items: list[dict[str, object]] | object) -> dict[str, object]:
    normalized_items = [
        dict(item)
        for item in (items if isinstance(items, list) else [])
        if isinstance(item, dict)
    ]
    status_counts = Counter(
        str(item.get("status") or "unknown").strip().lower() or "unknown"
        for item in normalized_items
    )
    pending_count = sum(status_counts.get(status, 0) for status in ("queued", "repair"))
    running_count = status_counts.get("running", 0)
    return {
        "total_items": len(normalized_items),
        "pending_items": pending_count,
        "running_items": running_count,
        "done_items": status_counts.get("done", 0),
        "dead_items": status_counts.get("dead", 0),
        "status_counts": dict(sorted(status_counts.items())),
    }


def reconcile_queue_with_live_workers(
    project_root: Path,
    *,
    live_workers: list[dict[str, object]] | None = None,
    inspect_run: QueueInspectRun | None = None,
    max_repair_attempts: int = 3,
) -> dict[str, object]:
    payload = load_experiment_queue(project_root)
    known_items = [dict(item) for item in payload.get("items") or [] if isinstance(item, dict)]
    running_items = [
        dict(item)
        for item in payload.get("items") or []
        if isinstance(item, dict) and str(item.get("status") or "").strip().lower() == "running"
    ]
    running_fallback_counts = Counter(
        _suite_run_market_key(item)
        for item in running_items
        if _suite_run_market_key(item) is not None
    )
    worker_map: dict[tuple[str, str, str, str], dict[str, object]] = {}
    fallback_alive_keys: set[tuple[str, str, str]] = set()
    shared_quick_screen_alive_keys: set[tuple[str, str, str, str]] = set()
    live_batch_ids: set[str] = set()
    for item in live_workers or []:
        if not isinstance(item, dict):
            continue
        resolved_worker = _resolve_live_worker_metadata(item, known_items)
        batch_id = _item_batch_id(resolved_worker)
        if batch_id:
            live_batch_ids.add(batch_id)
        shared_quick_screen_key = _shared_quick_screen_key(resolved_worker)
        if shared_quick_screen_key is not None:
            shared_quick_screen_alive_keys.add(shared_quick_screen_key)
        identity_key = _resolved_identity_key(resolved_worker)
        if identity_key is not None:
            worker_map[identity_key] = resolved_worker
            continue
        fallback_key = _suite_run_market_key(resolved_worker)
        if fallback_key is not None and running_fallback_counts.get(fallback_key, 0) == 1:
            fallback_alive_keys.add(fallback_key)
    inspector = inspect_run or _default_inspect_run
    updated_items: list[dict[str, object]] = []
    for raw_item in payload.get("items") or []:
        if not isinstance(raw_item, dict):
            continue
        item = dict(raw_item)
        if str(item.get("status") or "").strip().lower() != "running":
            updated_items.append(item)
            continue
        identity_key = _resolved_identity_key(item)
        fallback_key = _suite_run_market_key(item)
        shared_quick_screen_key = _shared_quick_screen_key(item)
        batch_id = _item_batch_id(item)
        if (
            (batch_id and batch_id in live_batch_ids)
            or identity_key in worker_map
            or (fallback_key is not None and fallback_key in fallback_alive_keys)
            or (shared_quick_screen_key is not None and shared_quick_screen_key in shared_quick_screen_alive_keys)
        ):
            item["updated_at"] = _utc_now()
            updated_items.append(item)
            continue

        if fallback_key is not None and running_fallback_counts.get(fallback_key, 0) > 1:
            _advance_running_item_failure(
                item,
                last_error="ambiguous_running_identity",
                max_repair_attempts=max_repair_attempts,
            )
            updated_items.append(item)
            continue

        run_payload = inspector(_queue_run_dir(project_root, item))
        run_state = str(run_payload.get("state") or "").strip().lower()
        if run_state == "completed":
            item["status"] = "done"
            item["updated_at"] = _utc_now()
            updated_items.append(item)
            continue

        _advance_running_item_failure(
            item,
            last_error=str(run_payload.get("last_event") or "worker_missing_nonterminal"),
            max_repair_attempts=max_repair_attempts,
        )
        updated_items.append(item)

    payload["items"] = updated_items
    return save_experiment_queue(project_root, payload)


def ensure_running_queue_items(
    project_root: Path,
    *,
    live_workers: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    payload = load_experiment_queue(project_root)
    items = [dict(item) for item in payload.get("items") or [] if isinstance(item, dict)]
    seen_exact = {
        _resolved_identity_key(item)
        for item in items
        if _resolved_identity_key(item) is not None
    }
    seen_fallback = {
        _occupancy_fallback_key(item)
        for item in items
        if _occupancy_fallback_key(item) is not None
    }
    for worker in live_workers or []:
        if not isinstance(worker, dict):
            continue
        resolved_worker = _resolve_live_worker_metadata(worker, items)
        market = str(resolved_worker.get("market") or "").strip().lower()
        suite_name = str(resolved_worker.get("suite_name") or "").strip()
        run_label = str(resolved_worker.get("run_label") or "").strip()
        if not market or not suite_name or not run_label:
            continue
        if not _queue_item_market_allowed(resolved_worker):
            continue
        identity_key = _resolved_identity_key(resolved_worker)
        fallback_key = _occupancy_fallback_key(resolved_worker)
        if (identity_key is not None and identity_key in seen_exact) or (
            identity_key is None and fallback_key in seen_fallback
        ):
            continue
        items.append(
            build_queue_item(
                market=market,
                suite_name=suite_name,
                run_label=run_label,
                action="resume",
                status="running",
                reason="seeded_from_live_worker",
                track=resolved_worker.get("track"),
                session_dir=resolved_worker.get("session_dir"),
                program_path=resolved_worker.get("program_path"),
            )
        )
        appended_item = items[-1]
        appended_identity = _resolved_identity_key(appended_item)
        appended_fallback = _occupancy_fallback_key(appended_item)
        if appended_identity is not None:
            seen_exact.add(appended_identity)
        if appended_fallback is not None:
            seen_fallback.add(appended_fallback)
    payload["items"] = items
    return save_experiment_queue(project_root, payload)


def reseed_empty_tracks_from_recent_done(
    project_root: Path,
    *,
    live_workers: list[dict[str, object]] | None = None,
    inspect_run: QueueInspectRun | None = None,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    payload = load_experiment_queue(project_root)
    items = [dict(item) for item in payload.get("items") or [] if isinstance(item, dict)]
    inspector = inspect_run or _default_inspect_run

    known_items = [dict(item) for item in items]
    live_payload = [
        _resolve_live_worker_metadata(dict(item), known_items)
        for item in live_workers or []
        if isinstance(item, dict)
    ]

    live_track_counts = Counter(
        _item_track(item)
        for item in live_payload
        if _item_track(item) != UNKNOWN_QUEUE_TRACK
    )
    queued_or_repair_track_counts = Counter(
        _item_track(item)
        for item in items
        if _item_track(item) != UNKNOWN_QUEUE_TRACK
        and str(item.get("status") or "").strip().lower() in {"queued", "repair"}
    )
    active_track_counts = Counter(
        _item_track(item)
        for item in items
        if _item_track(item) != UNKNOWN_QUEUE_TRACK
        and str(item.get("status") or "").strip().lower() == "running"
    )
    occupied_markets_by_track: dict[str, set[str]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        track = _item_track(item)
        if track == UNKNOWN_QUEUE_TRACK:
            continue
        if str(item.get("status") or "").strip().lower() not in {"queued", "repair", "running"}:
            continue
        market = str(item.get("market") or "").strip().lower()
        if not market:
            continue
        occupied_markets_by_track.setdefault(track, set()).add(market)
    for item in live_payload:
        if not isinstance(item, dict):
            continue
        track = _item_track(item)
        if track == UNKNOWN_QUEUE_TRACK:
            continue
        market = str(item.get("market") or "").strip().lower()
        if not market:
            continue
        occupied_markets_by_track.setdefault(track, set()).add(market)
    track_slot_caps = _normalize_track_slot_caps(payload.get("track_slot_caps"))

    selected_ids: set[str] = set()
    selected_reasons: dict[str, str] = {}
    for track, raw_cap in track_slot_caps.items():
        track_cap = max(0, int(raw_cap or 0))
        if track_cap <= 0:
            continue
        if queued_or_repair_track_counts.get(track, 0) > 0:
            continue
        track_usage = max(active_track_counts.get(track, 0), live_track_counts.get(track, 0))
        if track_usage >= track_cap:
            continue
        refill_gap = max(0, track_cap - track_usage)
        if refill_gap <= 0:
            continue
        candidates = _recent_done_reseed_candidates(
            project_root=project_root,
            items=items,
            track=track,
            track_cap=refill_gap,
            inspect_run=inspector,
            occupied_markets=occupied_markets_by_track.get(track, set()),
        )
        for candidate in candidates:
            item_id = str(candidate.get("id") or "").strip()
            if item_id:
                selected_ids.add(item_id)
                selected_reasons[item_id] = "auto_refill_underfilled_track_from_recent_done"

    if not selected_ids:
        return payload, []

    updated_items: list[dict[str, object]] = []
    for item in items:
        item_id = str(item.get("id") or "").strip()
        if item_id in selected_ids:
            item["action"] = "repair"
            item["status"] = "repair"
            item["retry_count"] = 0
            item["reason"] = selected_reasons.get(item_id, "auto_refill_empty_track_from_recent_done")
            item["updated_at"] = _utc_now()
            item.pop("pid", None)
            item.pop("last_error", None)
        updated_items.append(item)

    payload["items"] = updated_items
    saved = save_experiment_queue(project_root, payload)
    reseeded_items = [
        dict(item)
        for item in saved.get("items") or []
        if str(item.get("id") or "").strip() in selected_ids
    ]
    reseeded_items.sort(key=_queue_sort_key)
    return saved, reseeded_items


def launch_ready_queue_items(
    project_root: Path,
    *,
    live_workers: list[dict[str, object]] | None = None,
    launcher: QueueLauncher | None = None,
    max_live_runs: int = 3,
    max_new_launches: int | None = None,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    payload = load_experiment_queue(project_root)
    reserved_live_workers = [dict(item) for item in live_workers or [] if isinstance(item, dict)]
    launched_items: list[dict[str, object]] = []
    launch_limit = int(max_new_launches) if max_new_launches is not None else None

    while True:
        if launch_limit is not None and len(launched_items) >= max(0, launch_limit):
            break
        selected = select_launchable_queue_items(
            payload,
            max_live_runs=max_live_runs,
            live_workers=reserved_live_workers,
        )
        if launch_limit is not None:
            remaining_launches = max(0, launch_limit - len(launched_items))
            selected = selected[:remaining_launches]
        if not selected:
            break

        blocked_by_id: dict[str, str] = {}
        launched_results: dict[str, dict[str, object]] = {}
        launched_ids: set[str] = set()
        launched_shared_results: dict[tuple[str, str, str, str], dict[str, object]] = {}
        for item in selected:
            blocker = _launch_candidate_blocker(item)
            if blocker is not None:
                blocked_by_id[str(item.get("id") or "").strip()] = blocker
                continue
            item_id = str(item.get("id") or "").strip()
            try:
                result = {} if launcher is None else dict(launcher(dict(item)) or {})
            except Exception as exc:
                blocked_by_id[item_id] = f"launch_error: {exc}"
                continue
            launched_results[item_id] = result
            launched_ids.add(item_id)
            shared_quick_screen_key = _shared_quick_screen_key(item)
            if shared_quick_screen_key is not None:
                launched_shared_results[shared_quick_screen_key] = result

        updated_items: list[dict[str, object]] = []
        for raw_item in payload.get("items") or []:
            if not isinstance(raw_item, dict):
                continue
            item = dict(raw_item)
            item_id = str(item.get("id") or "").strip()
            shared_quick_screen_key = _shared_quick_screen_key(item)
            if item_id in blocked_by_id:
                item["action"] = BLOCKED_QUEUE_ACTION
                item["status"] = "dead"
                item["updated_at"] = _utc_now()
                item["last_error"] = blocked_by_id[item_id]
            elif item_id in launched_ids or (
                shared_quick_screen_key is not None and shared_quick_screen_key in launched_shared_results
            ):
                if str(item.get("status") or "").strip().lower() == "repair":
                    item["action"] = "repair"
                item["status"] = "running"
                item["updated_at"] = _utc_now()
                launch_result = launched_results.get(item_id, {})
                if not launch_result and shared_quick_screen_key is not None:
                    launch_result = launched_shared_results.get(shared_quick_screen_key, {})
                if "pid" in launch_result:
                    item["pid"] = launch_result["pid"]
            updated_items.append(item)
        payload["items"] = updated_items

        selected_shared_keys = {
            _shared_quick_screen_key(item)
            for item in selected
            if str(item.get("id") or "").strip() in launched_ids and _shared_quick_screen_key(item) is not None
        }
        round_launched = [
            item
            for item in updated_items
            if str(item.get("id") or "").strip() in launched_ids
            or (_shared_quick_screen_key(item) is not None and _shared_quick_screen_key(item) in selected_shared_keys)
        ]
        if round_launched:
            launched_items.extend(round_launched)
            reserved_live_workers.extend(round_launched)

    saved = save_experiment_queue(project_root, payload)
    return saved, launched_items


def select_launchable_queue_item_batches(
    payload: dict[str, object],
    *,
    max_live_runs: int,
    live_workers: list[dict[str, object]] | None = None,
    quick_screen_batch_size: int = 1,
    max_new_launches: int | None = None,
    single_pool_per_track: bool = False,
) -> list[list[dict[str, object]]]:
    batch_size = max(1, int(quick_screen_batch_size or 1))
    if batch_size <= 1:
        selected = select_launchable_queue_items(
            payload,
            max_live_runs=max_live_runs,
            live_workers=live_workers,
        )
        if max_new_launches is not None:
            selected = selected[: max(0, int(max_new_launches))]
        return [[item] for item in selected]

    occupied_slots = _occupied_launch_slot_keys(payload, live_workers=live_workers)
    process_capacity = max(0, int(max_live_runs) - len(occupied_slots))
    if max_new_launches is not None:
        process_capacity = min(process_capacity, max(0, int(max_new_launches)))
    if process_capacity <= 0:
        return []

    track_slot_caps = _normalize_track_slot_caps(payload.get("track_slot_caps"))
    selected_track_usage: Counter[str] = Counter()
    running_track_usage = Counter(
        _item_track(item)
        for item in payload.get("items") or []
        if isinstance(item, dict)
        and str(item.get("status") or "").strip().lower() == "running"
        and _item_track(item) != UNKNOWN_QUEUE_TRACK
    )
    occupied_shared_quick_screen_keys = _occupied_shared_quick_screen_keys(
        payload,
        live_workers=live_workers,
    )
    occupied_pool_keys = _occupied_quick_screen_pool_keys(
        payload,
        live_workers=live_workers,
    ) if single_pool_per_track else set()
    selected_ids: set[str] = set()
    selected_shared_keys: set[tuple[str, str, str, str]] = set()
    selected_pool_keys: set[tuple[str, str]] = set()
    queue_items = [
        dict(item)
        for item in payload.get("items") or []
        if isinstance(item, dict)
        and str(item.get("status") or "").strip().lower() in {"queued", "repair"}
    ]
    queue_items.sort(key=_queue_sort_key)

    batches: list[list[dict[str, object]]] = []
    while len(batches) < process_capacity:
        seed = _next_batch_seed(
            queue_items,
            selected_ids=selected_ids,
            selected_shared_keys=selected_shared_keys,
            occupied_shared_keys=occupied_shared_quick_screen_keys,
            running_track_usage=running_track_usage,
            selected_track_usage=selected_track_usage,
            track_slot_caps=track_slot_caps,
        )
        if seed is None:
            break
        seed_track = _item_track(seed)
        seed_pool_key = _quick_screen_pool_key(seed)
        if single_pool_per_track and seed_pool_key is None:
            selected_ids.add(str(seed.get("id") or "").strip())
            continue
        if single_pool_per_track and seed_pool_key in occupied_pool_keys:
            selected_ids.add(str(seed.get("id") or "").strip())
            continue
        if single_pool_per_track and seed_pool_key in selected_pool_keys:
            selected_ids.add(str(seed.get("id") or "").strip())
            continue
        batch: list[dict[str, object]] = []
        for item in queue_items:
            if len(batch) >= batch_size:
                break
            if _item_track(item) != seed_track:
                continue
            if single_pool_per_track and _quick_screen_pool_key(item) != seed_pool_key:
                continue
            if not _batch_candidate_available(
                item,
                selected_ids=selected_ids,
                selected_shared_keys=selected_shared_keys,
                occupied_shared_keys=occupied_shared_quick_screen_keys,
                running_track_usage=running_track_usage,
                selected_track_usage=selected_track_usage,
                track_slot_caps=track_slot_caps,
            ):
                continue
            item_id = str(item.get("id") or "").strip()
            shared_key = _shared_quick_screen_key(item)
            selected_ids.add(item_id)
            if shared_key is not None:
                selected_shared_keys.add(shared_key)
            selected_track_usage[seed_track] += 1
            batch.append(dict(item))
        if not batch:
            break
        if single_pool_per_track:
            assert seed_pool_key is not None
            selected_pool_keys.add(seed_pool_key)
        batches.append(batch)
    return batches


def launch_ready_queue_item_batches(
    project_root: Path,
    *,
    live_workers: list[dict[str, object]] | None = None,
    batch_launcher: QueueBatchLauncher | None = None,
    max_live_runs: int = 3,
    max_new_launches: int | None = None,
    quick_screen_batch_size: int = 1,
    single_pool_per_track: bool = False,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    if int(quick_screen_batch_size or 1) <= 1:
        return launch_ready_queue_items(
            project_root,
            live_workers=live_workers,
            launcher=(lambda item: (batch_launcher([item]) if batch_launcher is not None else {})),
            max_live_runs=max_live_runs,
            max_new_launches=max_new_launches,
        )

    payload = load_experiment_queue(project_root)
    reserved_live_workers = [dict(item) for item in live_workers or [] if isinstance(item, dict)]
    launched_items: list[dict[str, object]] = []
    launched_batch_count = 0

    while True:
        if max_new_launches is not None and launched_batch_count >= max(0, int(max_new_launches)):
            break
        remaining_launches = None
        if max_new_launches is not None:
            remaining_launches = max(0, int(max_new_launches) - launched_batch_count)
        batches = select_launchable_queue_item_batches(
            payload,
            max_live_runs=max_live_runs,
            live_workers=reserved_live_workers,
            quick_screen_batch_size=quick_screen_batch_size,
            max_new_launches=remaining_launches,
            single_pool_per_track=single_pool_per_track,
        )
        if not batches:
            break

        for batch in batches:
            blocked_by_id: dict[str, str] = {}
            for item in batch:
                blocker = _launch_candidate_blocker(item)
                if blocker is not None:
                    blocked_by_id[str(item.get("id") or "").strip()] = blocker
            launched_ids: set[str] = set()
            launched_shared_results: dict[tuple[str, str, str, str], dict[str, object]] = {}
            launch_result: dict[str, object] = {}
            if not blocked_by_id:
                try:
                    launch_result = {} if batch_launcher is None else dict(batch_launcher([dict(item) for item in batch]) or {})
                except Exception as exc:
                    for item in batch:
                        blocked_by_id[str(item.get("id") or "").strip()] = f"launch_error: {exc}"
                else:
                    launched_batch_count += 1
                    for item in batch:
                        item_id = str(item.get("id") or "").strip()
                        if item_id:
                            launched_ids.add(item_id)
                        shared_key = _shared_quick_screen_key(item)
                        if shared_key is not None:
                            launched_shared_results[shared_key] = launch_result

            updated_items: list[dict[str, object]] = []
            for raw_item in payload.get("items") or []:
                if not isinstance(raw_item, dict):
                    continue
                item = dict(raw_item)
                item_id = str(item.get("id") or "").strip()
                shared_key = _shared_quick_screen_key(item)
                if item_id in blocked_by_id:
                    item["action"] = BLOCKED_QUEUE_ACTION
                    item["status"] = "dead"
                    item["updated_at"] = _utc_now()
                    item["last_error"] = blocked_by_id[item_id]
                elif item_id in launched_ids or (
                    shared_key is not None and shared_key in launched_shared_results
                ):
                    if str(item.get("status") or "").strip().lower() == "repair":
                        item["action"] = "repair"
                    item["status"] = "running"
                    item["updated_at"] = _utc_now()
                    if "pid" in launch_result:
                        item["pid"] = launch_result["pid"]
                    if "batch_id" in launch_result:
                        item["batch_id"] = launch_result["batch_id"]
                    if "manifest_path" in launch_result:
                        item["batch_manifest_path"] = launch_result["manifest_path"]
                updated_items.append(item)
            payload["items"] = updated_items

            batch_id = str(launch_result.get("batch_id") or "").strip()
            round_launched = [
                item
                for item in updated_items
                if str(item.get("id") or "").strip() in launched_ids
                or (_shared_quick_screen_key(item) is not None and _shared_quick_screen_key(item) in launched_shared_results)
            ]
            if round_launched:
                launched_items.extend(round_launched)
                reserved_live_workers.append(
                    {
                        "batch_id": batch_id,
                        "pid": launch_result.get("pid"),
                    }
                )
                reserved_live_workers.extend(round_launched)
        if max_new_launches is None:
            continue

    saved = save_experiment_queue(project_root, payload)
    return saved, launched_items


def set_queue_item_status(
    project_root: Path,
    *,
    item_id: str | None = None,
    suite_name: str | None = None,
    run_label: str | None = None,
    track: str | None = None,
    status: str,
    reason: str | None = None,
    last_error: str | None = None,
) -> dict[str, object]:
    payload = load_experiment_queue(project_root)
    requested_track = _normalize_track(track)
    matched_indexes: list[int] = []
    items = [dict(item) for item in payload.get("items") or [] if isinstance(item, dict)]
    for index, item in enumerate(items):
        if item_id and str(item.get("id") or "").strip() == str(item_id).strip():
            matched_indexes.append(index)
            continue
        if item_id and _legacy_item_id_matches(item, item_id, requested_track=requested_track):
            matched_indexes.append(index)
            continue
        if suite_name and run_label:
            if (
                str(item.get("suite_name") or "").strip() == str(suite_name).strip()
                and str(item.get("run_label") or "").strip() == str(run_label).strip()
            ):
                if requested_track and _item_track(item) != requested_track:
                    continue
                matched_indexes.append(index)
    if not matched_indexes:
        raise KeyError("queue item not found")
    if len(matched_indexes) > 1:
        raise ValueError("ambiguous queue item match; provide track or item_id")
    for index in matched_indexes:
        items[index]["status"] = str(status).strip().lower()
        items[index]["updated_at"] = _utc_now()
        if reason is not None:
            items[index]["reason"] = str(reason).strip()
        if last_error is not None:
            items[index]["last_error"] = str(last_error).strip()
    payload["items"] = items
    return save_experiment_queue(project_root, payload)


def _occupied_launch_slot_keys(
    payload: dict[str, object],
    *,
    live_workers: list[dict[str, object]] | None = None,
) -> set[tuple[str, ...]]:
    known_items = [dict(item) for item in payload.get("items") or [] if isinstance(item, dict)]
    occupied: set[tuple[str, ...]] = set()
    for item in known_items:
        if str(item.get("status") or "").strip().lower() != "running":
            continue
        if not _queue_item_market_allowed(item):
            continue
        key = _launch_slot_key(item)
        if key is not None:
            occupied.add(key)
    for worker in live_workers or []:
        if not isinstance(worker, dict):
            continue
        resolved = _resolve_live_worker_metadata(dict(worker), known_items)
        if not _queue_item_market_allowed(resolved):
            continue
        key = _launch_slot_key(resolved)
        if key is not None:
            occupied.add(key)
    return occupied


def _occupied_shared_quick_screen_keys(
    payload: dict[str, object],
    *,
    live_workers: list[dict[str, object]] | None = None,
) -> set[tuple[str, str, str, str]]:
    known_items = [dict(item) for item in payload.get("items") or [] if isinstance(item, dict)]
    live_batch_ids: set[str] = set()
    occupied: set[tuple[str, str, str, str]] = set()

    for worker in live_workers or []:
        if not isinstance(worker, dict):
            continue
        resolved = _resolve_live_worker_metadata(dict(worker), known_items)
        if not _queue_item_market_allowed(resolved):
            continue
        batch_id = _item_batch_id(resolved)
        if batch_id:
            live_batch_ids.add(batch_id)
        shared_key = _shared_quick_screen_key(resolved)
        if shared_key is not None:
            occupied.add(shared_key)

    for item in known_items:
        if not _queue_item_market_allowed(item):
            continue
        shared_key = _shared_quick_screen_key(item)
        if shared_key is None:
            continue
        status = str(item.get("status") or "").strip().lower()
        batch_id = _item_batch_id(item)
        if status == "running" or (batch_id and batch_id in live_batch_ids):
            occupied.add(shared_key)

    return occupied


def _occupied_quick_screen_pool_keys(
    payload: dict[str, object],
    *,
    live_workers: list[dict[str, object]] | None = None,
) -> set[tuple[str, str]]:
    known_items = [dict(item) for item in payload.get("items") or [] if isinstance(item, dict)]
    occupied: set[tuple[str, str]] = set()

    for worker in live_workers or []:
        if not isinstance(worker, dict):
            continue
        resolved = _resolve_live_worker_metadata(dict(worker), known_items)
        if not _queue_item_market_allowed(resolved):
            continue
        if not _is_quick_screen_pool_worker(resolved):
            continue
        key = _quick_screen_pool_key(resolved)
        if key is not None:
            occupied.add(key)

    live_batch_ids = {
        _item_batch_id(worker)
        for worker in live_workers or []
        if isinstance(worker, dict) and _item_batch_id(worker)
    }
    for item in known_items:
        if not _queue_item_market_allowed(item):
            continue
        status = str(item.get("status") or "").strip().lower()
        batch_id = _item_batch_id(item)
        if status != "running" and not (batch_id and batch_id in live_batch_ids):
            continue
        if not str(batch_id).startswith("quick_screen_pool_"):
            continue
        key = _quick_screen_pool_key(item)
        if key is not None:
            occupied.add(key)

    return occupied


def _is_quick_screen_pool_worker(item: dict[str, object]) -> bool:
    cmd = str(item.get("cmd") or "").lower()
    batch_id = _item_batch_id(item)
    launcher = str(item.get("launcher") or "").strip().lower()
    return (
        "run_quick_screen_pool.py" in cmd
        or launcher == "quick_screen_pool"
        or batch_id.startswith("quick_screen_pool_")
    )


def _launch_slot_key(item: dict[str, object]) -> tuple[str, ...] | None:
    batch_id = _item_batch_id(item)
    if batch_id:
        return ("batch", batch_id)
    fallback_key = _occupancy_fallback_key(item)
    if fallback_key is not None:
        return fallback_key
    identity_key = _resolved_identity_key(item)
    if identity_key is not None:
        return ("identity", *identity_key)
    return None


def _next_batch_seed(
    queue_items: list[dict[str, object]],
    *,
    selected_ids: set[str],
    selected_shared_keys: set[tuple[str, str, str, str]],
    occupied_shared_keys: set[tuple[str, str, str, str]],
    running_track_usage: Counter[str],
    selected_track_usage: Counter[str],
    track_slot_caps: dict[str, int],
) -> dict[str, object] | None:
    for item in queue_items:
        if _batch_candidate_available(
            item,
            selected_ids=selected_ids,
            selected_shared_keys=selected_shared_keys,
            occupied_shared_keys=occupied_shared_keys,
            running_track_usage=running_track_usage,
            selected_track_usage=selected_track_usage,
            track_slot_caps=track_slot_caps,
        ):
            return dict(item)
    return None


def _batch_candidate_available(
    item: dict[str, object],
    *,
    selected_ids: set[str],
    selected_shared_keys: set[tuple[str, str, str, str]],
    occupied_shared_keys: set[tuple[str, str, str, str]],
    running_track_usage: Counter[str],
    selected_track_usage: Counter[str],
    track_slot_caps: dict[str, int],
) -> bool:
    item_id = str(item.get("id") or "").strip()
    if not item_id or item_id in selected_ids:
        return False
    if _launch_candidate_blocker(item) is not None:
        return False
    track = _item_track(item)
    if track not in DENSE_QUICK_SCREEN_TRACKS:
        return False
    shared_key = _shared_quick_screen_key(item)
    if shared_key is not None and (shared_key in occupied_shared_keys or shared_key in selected_shared_keys):
        return False
    track_cap = track_slot_caps.get(track)
    if track_cap is not None:
        used = int(running_track_usage.get(track, 0)) + int(selected_track_usage.get(track, 0))
        if used >= int(track_cap):
            return False
    return True


def _queue_sort_key(item: dict[str, object]) -> tuple[int, int, str, str]:
    status = str(item.get("status") or "").strip().lower()
    action = str(item.get("action") or "").strip().lower()
    if status == "repair":
        action_rank = 0
    else:
        action_rank = {
            "repair": 0,
            "resume": 1,
            "launch": 2,
        }.get(action, 9)
    priority = -int(item.get("priority") or 0)
    created_at = str(item.get("created_at") or "")
    run_label = str(item.get("run_label") or "")
    return (action_rank, priority, created_at, run_label)


def _recent_done_reseed_candidates(
    *,
    project_root: Path,
    items: list[dict[str, object]],
    track: str,
    track_cap: int,
    inspect_run: QueueInspectRun,
    occupied_markets: set[str] | None = None,
) -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    for item in items:
        if _item_track(item) != track:
            continue
        if str(item.get("status") or "").strip().lower() != "done":
            continue
        if _is_completed_auto_refill_repair(item):
            continue
        if _launch_candidate_blocker(item) is not None:
            continue
        run_payload = inspect_run(_queue_run_dir(project_root, item))
        if str(run_payload.get("state") or "").strip().lower() != "completed":
            continue
        if _is_sparse_failure_reseed(item, run_payload):
            continue
        candidates.append(dict(item))

    candidates.sort(
        key=lambda item: (
            str(item.get("updated_at") or ""),
            int(item.get("priority") or 0),
            str(item.get("run_label") or ""),
        ),
        reverse=True,
    )

    selected: list[dict[str, object]] = []
    selected_ids: set[str] = set()
    covered_markets: set[str] = {
        str(market).strip().lower()
        for market in (occupied_markets or set())
        if str(market).strip()
    }
    for item in candidates:
        market = str(item.get("market") or "").strip().lower()
        item_id = str(item.get("id") or "").strip()
        if not market or market in covered_markets or not item_id:
            continue
        selected.append(item)
        selected_ids.add(item_id)
        covered_markets.add(market)
        if len(selected) >= track_cap:
            return selected

    for item in candidates:
        item_id = str(item.get("id") or "").strip()
        if not item_id or item_id in selected_ids:
            continue
        selected.append(item)
        selected_ids.add(item_id)
        if len(selected) >= track_cap:
            break
    return selected


def _queue_run_dir(project_root: Path, item: dict[str, object]) -> Path:
    root = Path(project_root).resolve()
    suite_name = str(item.get("suite_name") or "").strip()
    run_label = str(item.get("run_label") or "").strip()
    return root / "research" / "experiments" / "runs" / f"suite={suite_name}" / f"run={run_label}"


def _apply_policy_gate_to_queue_item(root: Path, item: dict[str, object]) -> dict[str, object]:
    normalized = dict(item)
    status = str(normalized.get("status") or "").strip().lower()
    action = str(normalized.get("action") or "").strip().lower()
    if status != "queued" or action != "launch":
        return normalized
    if "research_meta" in normalized:
        candidate_gate = validate_research_candidate_meta(
            normalized.get("research_meta") if isinstance(normalized.get("research_meta"), dict) else {}
        )
        normalized["research_candidate_gate"] = candidate_gate
        if not bool(candidate_gate.get("passed")):
            normalized["action"] = BLOCKED_QUEUE_ACTION
            normalized["status"] = "dead"
            normalized["reason"] = "research_candidate_quality_gate_failed"
            normalized["last_error"] = ",".join(str(item) for item in candidate_gate.get("failures") or [])
            normalized["updated_at"] = _utc_now()
            normalized.pop("pid", None)
            return normalized
    try:
        validation = validate_queue_item_search_policy(root, normalized)
    except Exception as exc:
        normalized["research_policy_warning"] = f"policy_validation_failed: {exc}"
        return normalized
    if bool(validation.get("allowed")):
        decision = validation.get("decision")
        if isinstance(decision, dict) and str(decision.get("required_lever") or "none") != "none":
            normalized["research_policy_decision"] = decision
        return normalized
    normalized["action"] = BLOCKED_QUEUE_ACTION
    normalized["status"] = "dead"
    normalized["reason"] = "search_policy_blocked_repeated_sparse_route"
    normalized["last_error"] = str(validation.get("reason") or "search_policy_blocked")
    normalized["research_policy_decision"] = validation.get("decision") or {}
    normalized["updated_at"] = _utc_now()
    normalized.pop("pid", None)
    return normalized


def _queue_item_run_payload(item: dict[str, object]) -> dict[str, object]:
    return {
        "suite_name": str(item.get("suite_name") or "").strip(),
        "run_label": str(item.get("run_label") or "").strip(),
        "market": str(item.get("market") or "").strip().lower(),
        "top_case": {},
    }


def _recent_attempt_records_for_item(root: Path, item: dict[str, object], *, limit: int = 5) -> list[dict[str, object]]:
    market = str(item.get("market") or "").strip().lower()
    track = _policy_track_for_item(root, item) or UNKNOWN_QUEUE_TRACK
    return _recent_attempt_records_for_policy(root, market=market, track=track, limit=limit)


def _recent_attempt_records_for_policy(
    root: Path,
    *,
    market: str,
    track: str,
    limit: int = 5,
) -> list[dict[str, object]]:
    if not market or track == UNKNOWN_QUEUE_TRACK:
        return []
    run_dirs = _recent_completed_run_dirs(root)
    attempts: list[dict[str, object]] = []
    for run_dir in run_dirs:
        try:
            payload = _summarize_completed_run_for_policy(run_dir)
        except Exception:
            continue
        payload_markets = _payload_markets_for_policy(payload)
        if market not in payload_markets:
            continue
        if not _payload_matches_track_for_policy(payload, track):
            continue
        attempts.append(build_attempt_record(root, payload, track=track))
        if len(attempts) >= max(1, int(limit)):
            break
    return attempts


def _recent_completed_run_dirs(root: Path) -> list[Path]:
    runs_root = Path(root).resolve() / "research" / "experiments" / "runs"
    if not runs_root.exists():
        return []
    candidates = [
        path
        for path in runs_root.glob("suite=*/run=*")
        if path.is_dir() and ((path / "summary.json").exists() or (path / "quick_screen_summary.json").exists())
    ]
    return sorted(candidates, key=_completed_run_mtime, reverse=True)


def _completed_run_mtime(run_dir: Path) -> float:
    for name in ("quick_screen_summary.json", "summary.json"):
        path = run_dir / name
        if path.exists():
            return path.stat().st_mtime
    return run_dir.stat().st_mtime


def _summarize_completed_run_for_policy(run_dir: Path) -> dict[str, object]:
    summary_path = run_dir / "quick_screen_summary.json"
    leaderboard_path = run_dir / "quick_screen_leaderboard.csv"
    quick_screen = True
    if not summary_path.exists():
        summary_path = run_dir / "summary.json"
        leaderboard_path = run_dir / "leaderboard.csv"
        quick_screen = False
    summary = _read_json_object(summary_path)
    top_case = _read_top_policy_csv_row(leaderboard_path, quick_screen=quick_screen)
    return {
        "suite_name": str(summary.get("suite_name") or _suite_name_from_run_dir(run_dir) or "").strip(),
        "run_label": str(summary.get("run_label") or _run_label_from_run_dir(run_dir) or "").strip(),
        "run_dir": str(run_dir),
        "markets": _summary_markets_for_policy(summary, top_case),
        "market": str(top_case.get("market") or "").strip().lower(),
        "top_case": top_case,
        "decision_start": str(summary.get("decision_start") or "").strip(),
        "decision_end": str(summary.get("decision_end") or "").strip(),
        "train_end": str(summary.get("train_end") or "").strip(),
    }


def _read_json_object(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_top_policy_csv_row(path: Path, *, quick_screen: bool) -> dict[str, object]:
    if not path.exists():
        return {}
    import csv

    with path.open("r", encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh), None)
    if not isinstance(row, dict):
        return {}
    density = _parse_density_bottleneck_for_policy(row.get("density_bottleneck"))
    return {
        "market": row.get("market"),
        "feature_set": row.get("feature_set"),
        "roi_pct": _float_or_none(row.get("roi_pct")),
        "trades": _int_or_none(row.get("trades") or row.get("trade_rows")),
        "trade_rows": _int_or_none(row.get("trade_rows")),
        "profitable_pool_rows": _int_or_none(row.get("profitable_pool_rows")),
        "profitable_pool_correct_side_rows": _int_or_none(row.get("profitable_pool_correct_side_rows")),
        "profitable_pool_capture_rows": _int_or_none(row.get("profitable_pool_capture_rows")),
        "density_bottleneck": density,
        "quick_screen": bool(quick_screen),
    }


def _parse_density_bottleneck_for_policy(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    token = str(value or "").strip()
    if not token:
        return {}
    try:
        payload = json.loads(token)
    except json.JSONDecodeError:
        return {"primary_bottleneck": token}
    return payload if isinstance(payload, dict) else {}


def _summary_markets_for_policy(summary: dict[str, object], top_case: dict[str, object]) -> list[str]:
    markets: list[str] = []
    seen: set[str] = set()
    for raw in summary.get("markets") or []:
        token = str(raw or "").strip().lower()
        if token and token not in seen:
            seen.add(token)
            markets.append(token)
    top_market = str(top_case.get("market") or "").strip().lower()
    if top_market and top_market not in seen:
        markets.append(top_market)
    return markets


def _payload_markets_for_policy(payload: dict[str, object]) -> set[str]:
    out = {
        str(item or "").strip().lower()
        for item in payload.get("markets") or []
        if str(item or "").strip()
    }
    market = str(payload.get("market") or "").strip().lower()
    if market:
        out.add(market)
    return out


def _payload_matches_track_for_policy(payload: dict[str, object], track: str) -> bool:
    payload_track = _infer_policy_track_from_suite(
        _project_root_from_policy_payload(payload),
        str(payload.get("suite_name") or "").strip(),
    )
    if payload_track is not None:
        return payload_track == track
    suite_name = str(payload.get("suite_name") or "").strip().lower()
    run_label = str(payload.get("run_label") or "").strip().lower()
    text = f"{suite_name} {run_label}"
    if track == "direction_dense":
        return "reversal" not in text
    if track == "reversal_dense":
        return "reversal" in text
    return True


def _policy_track_for_item(root: Path, item: dict[str, object]) -> str | None:
    track = _normalize_policy_track(_item_track(item))
    if track is not None:
        return track
    return _infer_policy_track_from_suite(root, str(item.get("suite_name") or "").strip())


def _normalize_policy_track(value: object) -> str | None:
    token = str(value or "").strip().lower()
    if token in DENSE_QUICK_SCREEN_TRACKS:
        return token
    if token == "direction":
        return "direction_dense"
    if token == "reversal":
        return "reversal_dense"
    return None


def _infer_policy_track_from_suite(root: Path, suite_name: str | None) -> str | None:
    payload = _read_suite_spec_for_policy(root, suite_name)
    if not payload:
        text = str(suite_name or "").lower()
        if "reversal" in text:
            return "reversal_dense"
        if "direction" in text or "midprice" in text:
            return "direction_dense"
        return None
    targets = _suite_targets_for_policy(payload)
    if targets == {"reversal"}:
        return "reversal_dense"
    if targets == {"direction"}:
        return "direction_dense"
    return None


def _single_suite_market_for_policy(root: Path, suite_name: str | None) -> str | None:
    payload = _read_suite_spec_for_policy(root, suite_name)
    if not payload:
        return None
    markets = _suite_markets_for_policy(payload)
    return next(iter(markets)) if len(markets) == 1 else None


def _read_suite_spec_for_policy(root: Path, suite_name: str | None) -> dict[str, object]:
    token = str(suite_name or "").strip()
    if not token:
        return {}
    path = Path(root).resolve() / "research" / "experiments" / "suite_specs" / f"{token}.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _suite_targets_for_policy(payload: dict[str, object]) -> set[str]:
    targets: set[str] = set()

    def visit(value: object) -> None:
        if isinstance(value, dict):
            target = str(value.get("target") or "").strip().lower()
            if target in {"direction", "reversal"}:
                targets.add(target)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    if not targets:
        target = str(payload.get("target") or "").strip().lower()
        if target in {"direction", "reversal"}:
            targets.add(target)
    return targets


def _suite_markets_for_policy(payload: dict[str, object]) -> set[str]:
    markets_payload = payload.get("markets")
    if isinstance(markets_payload, dict):
        return {str(key).strip().lower() for key in markets_payload if str(key).strip()}
    if isinstance(markets_payload, list):
        out: set[str] = set()
        for item in markets_payload:
            if isinstance(item, dict):
                token = str(item.get("market") or item.get("asset") or "").strip().lower()
            else:
                token = str(item or "").strip().lower()
            if token:
                out.add(token)
        return out
    return set()


def _project_root_from_policy_payload(payload: dict[str, object]) -> Path:
    run_dir = str(payload.get("run_dir") or "").strip()
    if run_dir:
        path = Path(run_dir)
        for candidate in [path, *path.parents]:
            if (candidate / "research" / "experiments" / "suite_specs").exists():
                return candidate
    return Path.cwd()


def _suite_name_from_run_dir(path: Path) -> str | None:
    token = path.parent.name
    if token.startswith("suite="):
        return token.split("=", 1)[1] or None
    return token or None


def _run_label_from_run_dir(path: Path) -> str | None:
    token = path.name
    if token.startswith("run="):
        return token.split("=", 1)[1] or None
    return token or None


def _default_inspect_run(run_dir: Path) -> dict[str, object]:
    from pm15min.research.automation.control_plane import inspect_experiment_run

    return inspect_experiment_run(run_dir)


def _empty_queue_payload() -> dict[str, object]:
    return {
        "version": 1,
        "max_live_runs": DEFAULT_MAX_LIVE_RUNS,
        "max_queued_items": DEFAULT_MAX_QUEUED_ITEMS,
        "track_slot_caps": dict(DEFAULT_TRACK_SLOT_CAPS),
        "updated_at": _utc_now(),
        "items": [],
    }


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _item_matches(
    item: dict[str, object],
    *,
    item_id: str | None = None,
    suite_name: str | None = None,
    run_label: str | None = None,
) -> bool:
    if item_id and str(item.get("id") or "").strip() == str(item_id).strip():
        return True
    if suite_name and run_label:
        return (
            str(item.get("suite_name") or "").strip() == str(suite_name).strip()
            and str(item.get("run_label") or "").strip() == str(run_label).strip()
        )
    return False


def _queue_item_id(market: str, track: str, suite_name: str, run_label: str) -> str:
    return f"{market}:{track}:{suite_name}:{run_label}"


def _normalize_queue_item(project_root: Path, item: dict[str, object]) -> dict[str, object]:
    root = Path(project_root).resolve()
    normalized = dict(item)
    market = str(normalized.get("market") or "").strip().lower()
    suite_name = str(normalized.get("suite_name") or "").strip()
    run_label = str(normalized.get("run_label") or "").strip()
    action = str(normalized.get("action") or "").strip().lower()
    status = str(normalized.get("status") or "").strip().lower()
    track = _resolve_item_track(normalized)
    program_path = _resolve_queue_program_path(root, normalized.get("program_path"))
    session_dir = _resolve_queue_session_dir(root, normalized.get("session_dir"))
    created_at = str(normalized.get("created_at") or "").strip() or _utc_now()
    updated_at = str(normalized.get("updated_at") or "").strip() or created_at
    normalized.update(
        {
            "id": _queue_item_id(market, track, suite_name, run_label),
            "market": market,
            "suite_name": suite_name,
            "run_label": run_label,
            "action": action,
            "status": status,
            "priority": int(normalized.get("priority") or 100),
            "reason": str(normalized.get("reason") or "").strip(),
            "retry_count": max(0, int(normalized.get("retry_count") or 0)),
            "track": track,
            "session_dir": session_dir,
            "program_path": program_path,
            "created_at": created_at,
            "updated_at": updated_at,
        }
    )
    return normalized


def _normalize_track(track: object) -> str:
    return str(track or "").strip().lower()


def _normalize_track_slot_caps(raw_caps: object, *, apply_fixed: bool = True) -> dict[str, int]:
    normalized = dict(DEFAULT_TRACK_SLOT_CAPS)
    if isinstance(raw_caps, dict):
        for raw_key, raw_value in raw_caps.items():
            key = str(raw_key or "").strip().lower()
            if not key:
                continue
            normalized[key] = max(0, int(raw_value or 0))
    fixed_caps = _configured_fixed_track_slot_caps() if apply_fixed else {}
    if fixed_caps:
        for key, value in fixed_caps.items():
            normalized[key] = value
    return normalized


def _configured_fixed_track_slot_caps() -> dict[str, int]:
    raw = str(os.environ.get(FIXED_TRACK_SLOT_CAPS_ENV) or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    fixed: dict[str, int] = {}
    for track in DEFAULT_TRACK_SLOT_CAPS:
        if track not in payload:
            continue
        fixed[track] = max(0, int(payload.get(track) or 0))
    return fixed


def _configured_allowed_queue_markets() -> set[str]:
    raw = str(os.environ.get(ALLOWED_QUEUE_MARKETS_ENV) or "").strip()
    if not raw:
        return set()
    return {
        token
        for token in (
            str(part or "").strip().lower()
            for part in raw.replace(";", ",").split(",")
        )
        if token
    }


def _queue_item_market_allowed(item: dict[str, object]) -> bool:
    allowed_markets = _configured_allowed_queue_markets()
    if not allowed_markets:
        return True
    market = str(item.get("market") or "").strip().lower()
    return bool(market) and market in allowed_markets


def _filter_queue_items_for_allowed_markets(items: list[dict[str, object]]) -> list[dict[str, object]]:
    allowed_markets = _configured_allowed_queue_markets()
    if not allowed_markets:
        return [dict(item) for item in items]
    return [
        dict(item)
        for item in items
        if str(item.get("market") or "").strip().lower() in allowed_markets
    ]


def _prune_pending_queue_items(items: list[dict[str, object]], *, max_queued_items: int) -> list[dict[str, object]]:
    pending_items = [
        dict(item)
        for item in items
        if str(item.get("status") or "").strip().lower() in {"queued", "repair"}
    ]
    if len(pending_items) <= max_queued_items:
        return [dict(item) for item in items]

    keep_pending_ids = {
        str(item.get("id") or "").strip()
        for item in sorted(pending_items, key=_queue_sort_key)[:max_queued_items]
    }
    pruned: list[dict[str, object]] = []
    for item in items:
        status = str(item.get("status") or "").strip().lower()
        if status in {"queued", "repair"}:
            item_id = str(item.get("id") or "").strip()
            if item_id not in keep_pending_ids:
                continue
        pruned.append(dict(item))
    return pruned


def _stringify_queue_path(value: str | Path | None) -> str:
    if value is None or not str(value).strip():
        return ""
    return str(Path(value).expanduser())


def _resolve_queue_program_path(project_root: Path, value: object) -> str:
    root = Path(project_root).resolve()
    if value is not None and str(value).strip():
        raw = Path(str(value).strip()).expanduser()
        return str((raw if raw.is_absolute() else root / raw).resolve())
    return ""


def _resolve_queue_session_dir(project_root: Path, value: object) -> str:
    root = Path(project_root).resolve()
    if value is not None and str(value).strip():
        raw = Path(str(value).strip()).expanduser()
        return str((raw if raw.is_absolute() else root / raw).resolve())
    return ""


def _item_track(item: dict[str, object]) -> str:
    track = _normalize_track(item.get("track"))
    if _is_supported_track(track):
        return track
    return UNKNOWN_QUEUE_TRACK


def _market_track_key(item: dict[str, object]) -> tuple[str, str] | None:
    market = str(item.get("market") or "").strip().lower()
    if not market:
        return None
    return (market, _item_track(item))


def _quick_screen_pool_key(item: dict[str, object]) -> tuple[str, str] | None:
    track = _item_track(item)
    if track not in DENSE_QUICK_SCREEN_TRACKS:
        return None
    market = str(item.get("market") or "").strip().lower()
    if market not in SHARED_QUICK_SCREEN_MARKETS:
        return None
    return (track, market)


def _worker_run_key(item: dict[str, object]) -> tuple[str, str] | None:
    suite_name = str(item.get("suite_name") or "").strip()
    run_label = str(item.get("run_label") or "").strip()
    if not suite_name or not run_label:
        return None
    return (suite_name, run_label)


def _resolved_identity_key(item: dict[str, object]) -> tuple[str, str, str, str] | None:
    market = str(item.get("market") or "").strip().lower()
    track = _item_track(item)
    suite_name = str(item.get("suite_name") or "").strip()
    run_label = str(item.get("run_label") or "").strip()
    if not market or not suite_name or not run_label or track == UNKNOWN_QUEUE_TRACK:
        return None
    return (suite_name, run_label, market, track)


def _suite_run_market_key(item: dict[str, object]) -> tuple[str, str, str] | None:
    market = str(item.get("market") or "").strip().lower()
    suite_name = str(item.get("suite_name") or "").strip()
    run_label = str(item.get("run_label") or "").strip()
    if not market or not suite_name or not run_label:
        return None
    return (suite_name, run_label, market)


def _item_batch_id(item: dict[str, object]) -> str:
    return str(item.get("batch_id") or "").strip()


def _shared_quick_screen_key(item: dict[str, object]) -> tuple[str, str, str, str] | None:
    track = _item_track(item)
    if track not in DENSE_QUICK_SCREEN_TRACKS:
        return None
    market = str(item.get("market") or "").strip().lower()
    if market not in SHARED_QUICK_SCREEN_MARKETS:
        return None
    suite_name = str(item.get("suite_name") or "").strip()
    run_label = str(item.get("run_label") or "").strip()
    if not suite_name or not run_label:
        return None
    return (suite_name, run_label, market, track)


def _occupancy_fallback_key(item: dict[str, object]) -> tuple[str, ...] | None:
    shared_quick_screen_key = _shared_quick_screen_key(item)
    if shared_quick_screen_key is not None:
        return ("shared_quick_screen", *shared_quick_screen_key)
    suite_key = _suite_run_market_key(item)
    if suite_key is not None:
        return ("suite_run_market", *suite_key)
    market = str(item.get("market") or "").strip().lower()
    if market:
        return ("market", market)
    return None


def _register_occupied_item(
    item: dict[str, object],
    *,
    occupied_exact: dict[tuple[str, str, str, str], dict[str, object]],
    occupied_fallback: dict[tuple[str, ...], dict[str, object]],
) -> None:
    shared_quick_screen_key = _shared_quick_screen_key(item)
    if shared_quick_screen_key is not None:
        occupied_fallback.setdefault(("shared_quick_screen", *shared_quick_screen_key), item)
        return
    identity_key = _resolved_identity_key(item)
    if identity_key is not None:
        occupied_exact.setdefault(identity_key, item)
        return
    fallback_key = _occupancy_fallback_key(item)
    if fallback_key is not None:
        occupied_fallback.setdefault(fallback_key, item)


def _resolve_live_worker_metadata(
    item: dict[str, object],
    known_items: list[dict[str, object]],
) -> dict[str, object]:
    resolved = dict(item)
    resolved["track"] = _resolve_item_track(resolved, known_items)
    matching_items = _matching_known_items(known_items, resolved)
    if len(matching_items) == 1:
        match = matching_items[0]
        if not str(resolved.get("session_dir") or "").strip():
            resolved["session_dir"] = match.get("session_dir") or ""
        if not str(resolved.get("program_path") or "").strip():
            resolved["program_path"] = match.get("program_path") or ""
    return resolved


def _matching_known_items(
    known_items: list[dict[str, object]],
    item: dict[str, object],
) -> list[dict[str, object]]:
    suite_name = str(item.get("suite_name") or "").strip()
    run_label = str(item.get("run_label") or "").strip()
    market = str(item.get("market") or "").strip().lower()
    track = _item_track(item)
    matches = [
        dict(candidate)
        for candidate in known_items
        if str(candidate.get("suite_name") or "").strip() == suite_name
        and str(candidate.get("run_label") or "").strip() == run_label
        and (not market or str(candidate.get("market") or "").strip().lower() == market)
    ]
    if track != UNKNOWN_QUEUE_TRACK:
        exact_matches = [candidate for candidate in matches if _item_track(candidate) == track]
        if exact_matches:
            return exact_matches
    return matches


def _resolve_item_track(
    item: dict[str, object],
    known_items: list[dict[str, object]] | None = None,
) -> str:
    explicit_track = _normalize_track(item.get("track"))
    if explicit_track:
        if _is_supported_track(explicit_track):
            return explicit_track
        return UNKNOWN_QUEUE_TRACK
    inferred_track = _infer_track_from_signals(
        item.get("session_dir"),
        item.get("program_path"),
        item.get("suite_name"),
        item.get("run_label"),
        item.get("cmd"),
    )
    if inferred_track is not None:
        return inferred_track
    inferred_from_known_items = _infer_track_from_known_items(item, known_items or [])
    if inferred_from_known_items is not None:
        return inferred_from_known_items
    return UNKNOWN_QUEUE_TRACK


def _infer_track_from_signals(*values: object) -> str | None:
    text = " ".join(
        str(value).strip().lower()
        for value in values
        if value is not None and str(value).strip()
    )
    if not text:
        return None
    for track in DEFAULT_TRACK_SLOT_CAPS:
        if track in text:
            return track
    return None


def _infer_track_from_known_items(
    item: dict[str, object],
    known_items: list[dict[str, object]],
) -> str | None:
    matches = _matching_known_items(
        [candidate for candidate in known_items if isinstance(candidate, dict)],
        {
            "suite_name": item.get("suite_name"),
            "run_label": item.get("run_label"),
            "market": item.get("market"),
            "track": "",
        },
    )
    tracks = {
        _item_track(candidate)
        for candidate in matches
        if _item_track(candidate) != UNKNOWN_QUEUE_TRACK
    }
    if len(tracks) == 1:
        return next(iter(tracks))
    return None


def _launch_candidate_blocker(item: dict[str, object]) -> str | None:
    missing: list[str] = []
    if _item_track(item) == UNKNOWN_QUEUE_TRACK:
        missing.append("track")
    if not str(item.get("session_dir") or "").strip():
        missing.append("session_dir")
    if not str(item.get("program_path") or "").strip():
        missing.append("program_path")
    if not missing:
        return None
    action = str(item.get("action") or "").strip().lower()
    status = str(item.get("status") or "").strip().lower()
    label = "repair" if action == "repair" or status == "repair" else "launch"
    return f"unlaunchable_{label}: missing " + ",".join(missing)


def _is_completed_auto_refill_repair(item: dict[str, object]) -> bool:
    action = str(item.get("action") or "").strip().lower()
    reason = str(item.get("reason") or "").strip().lower()
    return action == "repair" and reason.startswith(AUTO_REFILL_REASON_PREFIX)


def _is_sparse_failure_reseed(item: dict[str, object], run_payload: dict[str, object]) -> bool:
    reason = str(item.get("reason") or "").strip().lower()
    if any(
        marker in reason
        for marker in (
            "reject_sparse",
            "completed_sparse",
            "below_56",
            "below dense floor",
            "below_dense_floor",
            "probability_gate",
            "no_sparse_promotion",
        )
    ):
        return True

    top_case = run_payload.get("top_case") if isinstance(run_payload.get("top_case"), dict) else {}
    trades = _int_or_none(
        top_case.get("trades")
        or top_case.get("trade_rows")
        or run_payload.get("trades")
        or run_payload.get("trade_rows")
    )
    if trades is not None and trades < 56:
        return True
    return False


def _int_or_none(value: object) -> int | None:
    try:
        return None if value in {None, ""} else int(value)
    except Exception:
        return None


def _float_or_none(value: object) -> float | None:
    try:
        return None if value in {None, ""} else float(value)
    except Exception:
        return None


def _advance_running_item_failure(
    item: dict[str, object],
    *,
    last_error: str,
    max_repair_attempts: int,
) -> None:
    next_retry_count = max(0, int(item.get("retry_count") or 0)) + 1
    item["retry_count"] = next_retry_count
    item["updated_at"] = _utc_now()
    item["last_error"] = str(last_error).strip() or "worker_missing_nonterminal"
    if next_retry_count >= max(1, int(max_repair_attempts)):
        item["status"] = "dead"
    else:
        item["action"] = "repair"
        item["status"] = "repair"


def _is_supported_track(track: str) -> bool:
    return bool(track) and track in DEFAULT_TRACK_SLOT_CAPS


def _legacy_item_id_matches(
    item: dict[str, object],
    item_id: str,
    *,
    requested_track: str = "",
) -> bool:
    parts = [part.strip() for part in str(item_id).split(":")]
    if len(parts) != 3:
        return False
    market, suite_name, run_label = parts
    if (
        str(item.get("market") or "").strip().lower() != market.lower()
        or str(item.get("suite_name") or "").strip() != suite_name
        or str(item.get("run_label") or "").strip() != run_label
    ):
        return False
    if requested_track and _item_track(item) != requested_track:
        return False
    return True
