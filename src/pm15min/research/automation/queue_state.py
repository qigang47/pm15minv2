from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from pm15min.data.io import write_json_atomic


QueueInspectRun = Callable[[Path], dict[str, object]]
QueueLauncher = Callable[[dict[str, object]], dict[str, object] | None]
DEFAULT_QUEUE_TRACK = "default"
UNKNOWN_QUEUE_TRACK = "unknown"
BLOCKED_QUEUE_ACTION = "blocked"
DEFAULT_TRACK_SLOT_CAPS = {
    "direction_dense": 2,
    "reversal_dense": 2,
}


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


def load_experiment_queue(project_root: Path) -> dict[str, object]:
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
    payload["items"] = [
        _normalize_queue_item(root, entry)
        for entry in items or []
        if isinstance(entry, dict)
    ]
    payload.setdefault("version", 1)
    payload.setdefault("max_live_runs", 4)
    payload["track_slot_caps"] = _normalize_track_slot_caps(payload.get("track_slot_caps"))
    payload.setdefault("updated_at", _utc_now())
    return payload


def save_experiment_queue(project_root: Path, payload: dict[str, object]) -> dict[str, object]:
    root = Path(project_root).resolve()
    normalized = dict(payload)
    normalized["version"] = 1
    normalized["max_live_runs"] = max(1, int(normalized.get("max_live_runs") or 4))
    normalized["track_slot_caps"] = _normalize_track_slot_caps(normalized.get("track_slot_caps"))
    normalized["updated_at"] = _utc_now()
    items = normalized.get("items")
    normalized["items"] = [
        _normalize_queue_item(root, item)
        for item in (items if isinstance(items, list) else [])
        if isinstance(item, dict)
    ]
    write_json_atomic(normalized, experiment_queue_path(project_root))
    return normalized


def upsert_queue_item(project_root: Path, item: dict[str, object]) -> dict[str, object]:
    root = Path(project_root).resolve()
    payload = load_experiment_queue(project_root)
    items = [dict(entry) for entry in payload.get("items") or [] if isinstance(entry, dict)]
    target_item = _normalize_queue_item(root, item)
    target_id = str(target_item.get("id") or "").strip()
    target_market = str(target_item.get("market") or "").strip().lower()
    target_track = _item_track(target_item)
    target_action = str(target_item.get("action") or "").strip().lower()
    target_status = str(target_item.get("status") or "").strip().lower()
    target_is_normal = target_status == "queued" and target_action in {"launch", "resume"}

    retained: list[dict[str, object]] = []
    replaced = False
    replaced_created_at: str | None = None
    for entry in items:
        entry_id = str(entry.get("id") or "").strip()
        entry_market = str(entry.get("market") or "").strip().lower()
        entry_track = _item_track(entry)
        entry_action = str(entry.get("action") or "").strip().lower()
        entry_status = str(entry.get("status") or "").strip().lower()
        entry_is_normal = entry_status == "queued" and entry_action in {"launch", "resume"}

        if entry_id and entry_id == target_id:
            replaced = True
            replaced_created_at = str(entry.get("created_at") or "").strip() or None
            continue
        if (
            target_is_normal
            and entry_market == target_market
            and entry_track == target_track
            and entry_is_normal
        ):
            continue
        retained.append(entry)

    if replaced_created_at:
        target_item["created_at"] = replaced_created_at
    target_item["updated_at"] = _utc_now()
    retained.append(target_item)
    payload["items"] = retained
    return save_experiment_queue(project_root, payload)


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

    occupied_market_tracks = {
        _market_track_key(item)
        for item in occupied_exact.values()
        if _market_track_key(item) is not None
    }
    occupied_unknown_markets = {
        str(item.get("market") or "").strip().lower()
        for item in occupied_fallback.values()
        if str(item.get("market") or "").strip()
    }
    track_usage: dict[str, int] = {}
    for item in occupied_exact.values():
        track = _item_track(item)
        track_usage[track] = track_usage.get(track, 0) + 1
    unknown_track_usage = len(occupied_fallback)
    track_slot_caps = _normalize_track_slot_caps(payload.get("track_slot_caps"))
    selected: list[dict[str, object]] = []
    selected_market_tracks: set[tuple[str, str]] = set()
    selected_track_usage: dict[str, int] = {}
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
        if market in occupied_unknown_markets:
            continue
        if market_track in occupied_market_tracks or market_track in selected_market_tracks:
            continue
        track_cap = track_slot_caps.get(track)
        if track_cap is not None:
            used = track_usage.get(track, 0) + unknown_track_usage + selected_track_usage.get(track, 0)
            if used >= int(track_cap):
                continue
        selected.append(item)
        selected_market_tracks.add(market_track)
        selected_track_usage[track] = selected_track_usage.get(track, 0) + 1
        if len(selected) >= capacity:
            break
    return selected


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
    for item in live_workers or []:
        if not isinstance(item, dict):
            continue
        resolved_worker = _resolve_live_worker_metadata(item, known_items)
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
        if identity_key in worker_map or (fallback_key is not None and fallback_key in fallback_alive_keys):
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


def launch_ready_queue_items(
    project_root: Path,
    *,
    live_workers: list[dict[str, object]] | None = None,
    launcher: QueueLauncher | None = None,
    max_live_runs: int = 3,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    payload = load_experiment_queue(project_root)
    reserved_live_workers = [dict(item) for item in live_workers or [] if isinstance(item, dict)]
    launched_items: list[dict[str, object]] = []

    while True:
        selected = select_launchable_queue_items(
            payload,
            max_live_runs=max_live_runs,
            live_workers=reserved_live_workers,
        )
        if not selected:
            break

        blocked_by_id: dict[str, str] = {}
        launched_results: dict[str, dict[str, object]] = {}
        launched_ids: set[str] = set()
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

        updated_items: list[dict[str, object]] = []
        for raw_item in payload.get("items") or []:
            if not isinstance(raw_item, dict):
                continue
            item = dict(raw_item)
            item_id = str(item.get("id") or "").strip()
            if item_id in blocked_by_id:
                item["action"] = BLOCKED_QUEUE_ACTION
                item["status"] = "dead"
                item["updated_at"] = _utc_now()
                item["last_error"] = blocked_by_id[item_id]
            elif item_id in launched_ids:
                if str(item.get("status") or "").strip().lower() == "repair":
                    item["action"] = "repair"
                item["status"] = "running"
                item["updated_at"] = _utc_now()
                launch_result = launched_results.get(item_id, {})
                if "pid" in launch_result:
                    item["pid"] = launch_result["pid"]
            updated_items.append(item)
        payload["items"] = updated_items

        round_launched = [item for item in selected if str(item.get("id") or "").strip() in launched_ids]
        if round_launched:
            launched_items.extend(round_launched)
            reserved_live_workers.extend(round_launched)

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
    payload["items"] = items
    return save_experiment_queue(project_root, payload)


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


def _queue_run_dir(project_root: Path, item: dict[str, object]) -> Path:
    root = Path(project_root).resolve()
    suite_name = str(item.get("suite_name") or "").strip()
    run_label = str(item.get("run_label") or "").strip()
    return root / "research" / "experiments" / "runs" / f"suite={suite_name}" / f"run={run_label}"


def _default_inspect_run(run_dir: Path) -> dict[str, object]:
    from pm15min.research.automation.control_plane import inspect_experiment_run

    return inspect_experiment_run(run_dir)


def _empty_queue_payload() -> dict[str, object]:
    return {
        "version": 1,
        "max_live_runs": 4,
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


def _normalize_track_slot_caps(raw_caps: object) -> dict[str, int]:
    normalized = dict(DEFAULT_TRACK_SLOT_CAPS)
    if isinstance(raw_caps, dict):
        for raw_key, raw_value in raw_caps.items():
            key = str(raw_key or "").strip().lower()
            if not key:
                continue
            normalized[key] = max(0, int(raw_value or 0))
    return normalized


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


def _occupancy_fallback_key(item: dict[str, object]) -> tuple[str, ...] | None:
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
