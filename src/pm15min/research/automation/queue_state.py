from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from pm15min.data.io import write_json_atomic


QueueInspectRun = Callable[[Path], dict[str, object]]
QueueLauncher = Callable[[dict[str, object]], dict[str, object] | None]


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
) -> dict[str, object]:
    stamp = _utc_now()
    normalized_market = str(market).strip().lower()
    normalized_suite = str(suite_name).strip()
    normalized_run = str(run_label).strip()
    normalized_action = str(action).strip().lower()
    normalized_status = str(status).strip().lower()
    return {
        "id": f"{normalized_market}:{normalized_suite}:{normalized_run}",
        "market": normalized_market,
        "suite_name": normalized_suite,
        "run_label": normalized_run,
        "action": normalized_action,
        "status": normalized_status,
        "priority": int(priority),
        "reason": str(reason or "").strip(),
        "retry_count": max(0, int(retry_count)),
        "created_at": stamp,
        "updated_at": stamp,
    }


def load_experiment_queue(project_root: Path) -> dict[str, object]:
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
    if not isinstance(items, list):
        payload["items"] = []
    payload.setdefault("version", 1)
    payload.setdefault("max_live_runs", 3)
    payload.setdefault("updated_at", _utc_now())
    return payload


def save_experiment_queue(project_root: Path, payload: dict[str, object]) -> dict[str, object]:
    normalized = dict(payload)
    normalized["version"] = 1
    normalized["max_live_runs"] = max(1, int(normalized.get("max_live_runs") or 3))
    normalized["updated_at"] = _utc_now()
    items = normalized.get("items")
    normalized["items"] = list(items) if isinstance(items, list) else []
    write_json_atomic(normalized, experiment_queue_path(project_root))
    return normalized


def upsert_queue_item(project_root: Path, item: dict[str, object]) -> dict[str, object]:
    payload = load_experiment_queue(project_root)
    items = [dict(entry) for entry in payload.get("items") or [] if isinstance(entry, dict)]
    target_id = str(item.get("id") or "").strip()
    target_market = str(item.get("market") or "").strip().lower()
    target_action = str(item.get("action") or "").strip().lower()
    target_status = str(item.get("status") or "").strip().lower()
    target_is_normal = target_status == "queued" and target_action in {"launch", "resume"}

    retained: list[dict[str, object]] = []
    replaced = False
    for entry in items:
        entry_id = str(entry.get("id") or "").strip()
        entry_market = str(entry.get("market") or "").strip().lower()
        entry_action = str(entry.get("action") or "").strip().lower()
        entry_status = str(entry.get("status") or "").strip().lower()
        entry_is_normal = entry_status == "queued" and entry_action in {"launch", "resume"}

        if entry_id and entry_id == target_id:
            replaced = True
            continue
        if (
            target_is_normal
            and entry_market == target_market
            and entry_is_normal
        ):
            continue
        retained.append(entry)

    normalized_item = dict(item)
    if replaced and "created_at" not in normalized_item:
        normalized_item["created_at"] = _utc_now()
    normalized_item["updated_at"] = _utc_now()
    retained.append(normalized_item)
    payload["items"] = retained
    return save_experiment_queue(project_root, payload)


def select_launchable_queue_items(
    payload: dict[str, object],
    *,
    max_live_runs: int,
    live_workers: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    live_payload = list(live_workers or [])
    live_markets = {
        str(item.get("market") or "").strip().lower()
        for item in live_payload
        if str(item.get("market") or "").strip()
    }
    capacity = max(0, int(max_live_runs) - len(live_payload))
    if capacity <= 0:
        return []

    selected: list[dict[str, object]] = []
    selected_markets: set[str] = set()
    queue_items = [
        dict(item)
        for item in payload.get("items") or []
        if isinstance(item, dict) and str(item.get("status") or "").strip().lower() == "queued"
    ]
    queue_items.sort(key=_queue_sort_key)
    for item in queue_items:
        market = str(item.get("market") or "").strip().lower()
        if not market:
            continue
        if market in live_markets or market in selected_markets:
            continue
        selected.append(item)
        selected_markets.add(market)
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
    worker_map = {
        (
            str(item.get("suite_name") or "").strip(),
            str(item.get("run_label") or "").strip(),
        ): item
        for item in (live_workers or [])
        if str(item.get("suite_name") or "").strip() and str(item.get("run_label") or "").strip()
    }
    inspector = inspect_run or _default_inspect_run
    updated_items: list[dict[str, object]] = []
    for raw_item in payload.get("items") or []:
        if not isinstance(raw_item, dict):
            continue
        item = dict(raw_item)
        if str(item.get("status") or "").strip().lower() != "running":
            updated_items.append(item)
            continue
        key = (
            str(item.get("suite_name") or "").strip(),
            str(item.get("run_label") or "").strip(),
        )
        if key in worker_map:
            item["updated_at"] = _utc_now()
            updated_items.append(item)
            continue

        run_payload = inspector(_queue_run_dir(project_root, item))
        run_state = str(run_payload.get("state") or "").strip().lower()
        if run_state == "completed":
            item["status"] = "done"
            item["updated_at"] = _utc_now()
            updated_items.append(item)
            continue

        next_retry_count = max(0, int(item.get("retry_count") or 0)) + 1
        item["retry_count"] = next_retry_count
        item["updated_at"] = _utc_now()
        item["last_error"] = str(run_payload.get("last_event") or "worker_missing_nonterminal")
        if next_retry_count >= max(1, int(max_repair_attempts)):
            item["status"] = "dead"
        else:
            item["status"] = "repair"
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
    seen_ids = {str(item.get("id") or "").strip() for item in items}
    for worker in live_workers or []:
        market = str(worker.get("market") or "").strip().lower()
        suite_name = str(worker.get("suite_name") or "").strip()
        run_label = str(worker.get("run_label") or "").strip()
        if not market or not suite_name or not run_label:
            continue
        item_id = f"{market}:{suite_name}:{run_label}"
        if item_id in seen_ids:
            continue
        items.append(
            build_queue_item(
                market=market,
                suite_name=suite_name,
                run_label=run_label,
                action="resume",
                status="running",
                reason="seeded_from_live_worker",
            )
        )
        seen_ids.add(item_id)
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
    selected = select_launchable_queue_items(
        payload,
        max_live_runs=max_live_runs,
        live_workers=live_workers,
    )
    if not selected:
        return payload, []

    launch_results: dict[str, dict[str, object]] = {}
    launched_ids: set[str] = set()
    for item in selected:
        item_id = str(item.get("id") or "")
        try:
            result = {} if launcher is None else dict(launcher(dict(item)) or {})
        except Exception as exc:
            launch_results[item_id] = {"launch_error": str(exc)}
            continue
        launch_results[item_id] = result
        launched_ids.add(item_id)

    updated_items: list[dict[str, object]] = []
    for raw_item in payload.get("items") or []:
        if not isinstance(raw_item, dict):
            continue
        item = dict(raw_item)
        item_id = str(item.get("id") or "")
        if item_id in launched_ids:
            item["status"] = "running"
            item["updated_at"] = _utc_now()
            launch_result = launch_results.get(item_id, {})
            if "pid" in launch_result:
                item["pid"] = launch_result["pid"]
        elif item_id in launch_results and launch_results[item_id].get("launch_error"):
            item["updated_at"] = _utc_now()
            item["last_error"] = str(launch_results[item_id]["launch_error"])
        updated_items.append(item)
    payload["items"] = updated_items
    saved = save_experiment_queue(project_root, payload)
    launched_items = [item for item in selected if str(item.get("id") or "") in launched_ids]
    return saved, launched_items


def set_queue_item_status(
    project_root: Path,
    *,
    item_id: str | None = None,
    suite_name: str | None = None,
    run_label: str | None = None,
    status: str,
    reason: str | None = None,
) -> dict[str, object]:
    payload = load_experiment_queue(project_root)
    updated = False
    updated_items: list[dict[str, object]] = []
    for raw_item in payload.get("items") or []:
        if not isinstance(raw_item, dict):
            continue
        item = dict(raw_item)
        if _item_matches(item, item_id=item_id, suite_name=suite_name, run_label=run_label):
            item["status"] = str(status).strip().lower()
            item["updated_at"] = _utc_now()
            if reason is not None:
                item["reason"] = str(reason).strip()
            updated = True
        updated_items.append(item)
    if not updated:
        raise KeyError("queue item not found")
    payload["items"] = updated_items
    return save_experiment_queue(project_root, payload)


def _queue_sort_key(item: dict[str, object]) -> tuple[int, int, str, str]:
    action = str(item.get("action") or "").strip().lower()
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
        "max_live_runs": 3,
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
