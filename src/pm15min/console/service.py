from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pm15min.console.action_runner import (
    execute_console_action as _execute_console_action,
)
from pm15min.console.actions import (
    build_console_action_request as _build_console_action_request,
    load_console_action_catalog as _load_console_action_catalog,
)
from pm15min.console.read_models.backtests import (
    describe_console_backtest_run as _describe_console_backtest_run,
    describe_console_backtest_stake_sweep as _describe_console_backtest_stake_sweep,
    list_console_backtest_runs as _list_console_backtest_runs,
)
from pm15min.console.read_models.bundles import (
    list_console_model_bundles as _list_console_model_bundles,
    load_console_model_bundle as _load_console_model_bundle,
)
from pm15min.console.read_models.common import json_ready
from pm15min.console.read_models.data_overview import (
    load_data_overview as _load_data_overview,
)
from pm15min.console.read_models.experiments import (
    describe_console_experiment_matrix as _describe_console_experiment_matrix,
    describe_console_experiment_run as _describe_console_experiment_run,
    list_console_experiment_runs as _list_console_experiment_runs,
)
from pm15min.console.read_models.training_runs import (
    list_console_training_runs as _list_console_training_runs,
    load_console_training_run as _load_console_training_run,
)
from pm15min.console.tasks import (
    list_console_tasks as _list_console_tasks,
    load_console_runtime_history as _load_console_runtime_history,
    load_console_runtime_summary as _load_console_runtime_summary,
    load_console_task as _load_console_task,
    submit_console_action_task as _submit_console_action_task,
)


_SECTION_DEFINITIONS: tuple[dict[str, object], ...] = (
    {
        "id": "data_overview",
        "title": "数据总览",
        "kind": "singleton",
        "list_command": "console show-data-overview --market <market> --cycle <cycle> --surface <surface>",
        "detail_command": None,
        "primary_object_type": "data_surface_summary",
        "notes": "读取标准数据总览，缺失时回退到持久化摘要。",
    },
    {
        "id": "training_runs",
        "title": "训练运行",
        "kind": "list_detail",
        "list_command": "console list-training-runs --market <market> --cycle <cycle>",
        "detail_command": "console show-training-run --market <market> --model-family <family> --target <target> --run-label <run>",
        "primary_object_type": "training_run",
        "notes": "读取标准训练运行及各 offset 诊断产物。",
    },
    {
        "id": "bundles",
        "title": "模型包",
        "kind": "list_detail",
        "list_command": "console list-bundles --market <market> --cycle <cycle>",
        "detail_command": "console show-bundle --market <market> --profile <profile> --target <target> --bundle-label <bundle>",
        "primary_object_type": "model_bundle",
        "notes": "读取标准模型包摘要、诊断和当前激活状态。",
    },
    {
        "id": "backtests",
        "title": "回测结果",
        "kind": "list_detail",
        "list_command": "console list-backtests --market <market> --cycle <cycle>",
        "detail_command": "console show-backtest --market <market> --profile <profile> --spec <spec> --run-label <run>",
        "primary_object_type": "backtest_run",
        "notes": "读取标准回测摘要与产物清单。",
    },
    {
        "id": "experiments",
        "title": "实验对比",
        "kind": "list_detail",
        "list_command": "console list-experiments --suite <suite>",
        "detail_command": "console show-experiment --suite <suite> --run-label <run>",
        "primary_object_type": "experiment_run",
        "notes": "读取标准实验套件输出、排行榜、对比与矩阵产物。",
    },
)

_STATUS_LABELS = {
    "queued": "排队中",
    "running": "运行中",
    "succeeded": "已完成",
    "completed": "已完成",
    "ok": "成功",
    "failed": "失败",
    "error": "错误",
    "unknown": "未知",
}

_STAGE_LABELS = {
    "queued": "排队中",
    "running": "运行中",
    "dispatch": "调度中",
    "finished": "已结束",
    "training_prepare": "训练准备",
    "training_oof": "OOF 训练",
    "training_artifacts": "训练产物写入",
    "training_finalize": "训练收尾",
    "suite_dispatch": "实验调度",
    "suite_group": "实验分组",
    "suite_case": "实验案例",
    "suite_finalize": "实验收尾",
}


def load_console_home(*, root: Path | None = None) -> dict[str, object]:
    sections = list_console_sections()
    runtime_summary = load_console_runtime_state(root=root)
    status_group_counts = runtime_summary.get("status_group_counts") if isinstance(runtime_summary, Mapping) else {}
    return json_ready(
        {
            "domain": "console",
            "dataset": "console_home",
            "root": None if root is None else str(Path(root)),
            "read_only": True,
            "sections": sections,
            "section_count": len(sections),
            "runtime_summary": runtime_summary,
            "runtime_board": runtime_summary.get("runtime_board") if isinstance(runtime_summary, Mapping) else None,
            "runtime_task_count": runtime_summary.get("task_count") if isinstance(runtime_summary, Mapping) else 0,
            "active_task_count": (status_group_counts or {}).get("active", 0) if isinstance(status_group_counts, Mapping) else 0,
            "terminal_task_count": (status_group_counts or {}).get("terminal", 0) if isinstance(status_group_counts, Mapping) else 0,
            "failed_task_count": (status_group_counts or {}).get("failed", 0) if isinstance(status_group_counts, Mapping) else 0,
            "commands": {
                str(section["id"]): {
                    "list_command": section.get("list_command"),
                    "detail_command": section.get("detail_command"),
                }
                for section in sections
            },
            "action_catalog": load_console_action_catalog(),
        }
    )


def list_console_sections() -> list[dict[str, object]]:
    return json_ready([dict(item) for item in _SECTION_DEFINITIONS])


def load_console_runtime_state(*, root: Path | None = None) -> dict[str, object]:
    return json_ready(_runtime_state_payload(dict(_load_console_runtime_summary(root=root))))


def load_console_runtime_history(*, root: Path | None = None) -> dict[str, object]:
    return json_ready(_runtime_history_payload(dict(_load_console_runtime_history(root=root))))


def load_console_query(
    query: Mapping[str, object] | None = None,
    *,
    root: Path | None = None,
) -> dict[str, object] | list[dict[str, object]]:
    payload = {} if query is None else {str(key): value for key, value in query.items()}
    section = _string_value(payload.get("section")) or "home"
    resolved_root = root if root is not None else _optional_path(payload.get("root"))

    if section == "home":
        return load_console_home(root=resolved_root)
    if section == "runtime_state":
        return load_console_runtime_state(root=resolved_root)
    if section == "runtime_history":
        return load_console_runtime_history(root=resolved_root)
    if section == "data_overview":
        return load_console_data_overview(
            market=_required_string(payload, "market", default="sol"),
            cycle=_required_string(payload, "cycle", default="15m"),
            surface=_required_string(payload, "surface", default="backtest"),
            root=resolved_root,
        )
    if section == "training_runs":
        if _string_value(payload.get("run_label")) or _string_value(payload.get("run_dir")):
            return load_console_training_run(
                market=_required_string(payload, "market", default="sol"),
                cycle=_required_string(payload, "cycle", default="15m"),
                model_family=_string_value(payload.get("model_family")),
                target=_string_value(payload.get("target")),
                run_label=_string_value(payload.get("run_label")),
                run_dir=_string_value(payload.get("run_dir")),
                root=resolved_root,
            )
        return list_console_training_runs(
            market=_required_string(payload, "market", default="sol"),
            cycle=_required_string(payload, "cycle", default="15m"),
            model_family=_string_value(payload.get("model_family")),
            target=_string_value(payload.get("target")),
            prefix=_string_value(payload.get("prefix")),
            root=resolved_root,
        )
    if section == "bundles":
        if _string_value(payload.get("bundle_label")) or _string_value(payload.get("bundle_dir")):
            return load_console_bundle(
                market=_required_string(payload, "market", default="sol"),
                cycle=_required_string(payload, "cycle", default="15m"),
                profile=_required_string(payload, "profile", default="deep_otm"),
                target=_required_string(payload, "target", default="direction"),
                bundle_label=_string_value(payload.get("bundle_label")),
                bundle_dir=_string_value(payload.get("bundle_dir")),
                root=resolved_root,
            )
        return list_console_bundles(
            market=_required_string(payload, "market", default="sol"),
            cycle=_required_string(payload, "cycle", default="15m"),
            profile=_string_value(payload.get("profile")),
            target=_string_value(payload.get("target")),
            prefix=_string_value(payload.get("prefix")),
            root=resolved_root,
        )
    if section == "backtest_stake_sweep":
        return load_console_backtest_stake_sweep(
            market=_required_string(payload, "market", default="sol"),
            cycle=_required_string(payload, "cycle", default="15m"),
            profile=_required_string(payload, "profile", default="deep_otm"),
            spec_name=_required_string(payload, "spec", default="baseline_truth"),
            run_label=_required_string(payload, "run_label"),
            root=resolved_root,
        )
    if section == "backtests":
        if _string_value(payload.get("run_label")):
            return load_console_backtest(
                market=_required_string(payload, "market", default="sol"),
                cycle=_required_string(payload, "cycle", default="15m"),
                profile=_required_string(payload, "profile", default="deep_otm"),
                spec_name=_required_string(payload, "spec", default="baseline_truth"),
                run_label=_required_string(payload, "run_label"),
                root=resolved_root,
            )
        return list_console_backtests(
            market=_required_string(payload, "market", default="sol"),
            cycle=_required_string(payload, "cycle", default="15m"),
            profile=_string_value(payload.get("profile")),
            spec_name=_string_value(payload.get("spec")),
            prefix=_string_value(payload.get("prefix")),
            root=resolved_root,
        )
    if section == "experiment_matrix":
        return load_console_experiment_matrix(
            suite_name=_required_string(payload, "suite"),
            run_label=_required_string(payload, "run_label"),
            root=resolved_root,
        )
    if section == "experiments":
        if _string_value(payload.get("run_label")) and _string_value(payload.get("suite")):
            return load_console_experiment(
                suite_name=_required_string(payload, "suite"),
                run_label=_required_string(payload, "run_label"),
                root=resolved_root,
            )
        return list_console_experiments(
            suite_name=_string_value(payload.get("suite")),
            prefix=_string_value(payload.get("prefix")),
            root=resolved_root,
        )
    if section == "actions":
        if _string_value(payload.get("action_id")):
            request_payload = {
                key: value
                for key, value in payload.items()
                if key not in {"section", "action_id", "root", "for_section", "shell_enabled"}
            }
            return build_console_action_request(
                action_id=_required_string(payload, "action_id"),
                request=request_payload,
            )
        return load_console_action_catalog(
            for_section=_string_value(payload.get("for_section")),
            shell_enabled=_optional_bool(payload.get("shell_enabled")),
        )
    if section == "tasks":
        if _string_value(payload.get("task_id")):
            return load_console_task(
                task_id=_required_string(payload, "task_id"),
                root=resolved_root,
            )
        action_ids = _optional_csv_strings(payload.get("action_ids"))
        return list_console_tasks(
            action_id=_string_value(payload.get("action_id")),
            action_ids=action_ids,
            status=_string_value(payload.get("status")),
            status_group=_string_value(payload.get("status_group")),
            marker=_string_value(payload.get("marker")),
            group_by=_string_value(payload.get("group_by")),
            limit=_optional_int(payload.get("limit"), default=20),
            root=resolved_root,
        )
    raise ValueError(f"不支持的 console section: {section}")


def load_console_data_overview(
    *,
    market: str,
    cycle: str | int = "15m",
    surface: str = "backtest",
    root: Path | None = None,
    prefer_persisted: bool = True,
) -> dict[str, object]:
    return _load_data_overview(
        market=market,
        cycle=cycle,
        surface=surface,
        root=root,
        prefer_persisted=prefer_persisted,
    )


def list_console_training_runs(
    *,
    market: str,
    cycle: str | int = "15m",
    model_family: str | None = None,
    target: str | None = None,
    prefix: str | None = None,
    root: Path | None = None,
) -> list[dict[str, object]]:
    return _list_console_training_runs(
        market=market,
        cycle=cycle,
        model_family=model_family,
        target=target,
        prefix=prefix,
        root=root,
    )


def load_console_training_run(
    *,
    market: str,
    cycle: str | int = "15m",
    model_family: str | None = None,
    target: str | None = None,
    run_label: str | None = None,
    run_dir: str | Path | None = None,
    root: Path | None = None,
) -> dict[str, object]:
    return _load_console_training_run(
        market=market,
        cycle=cycle,
        model_family=model_family,
        target=target,
        run_label=run_label,
        run_dir=run_dir,
        root=root,
    )


def list_console_bundles(
    *,
    market: str,
    cycle: str | int = "15m",
    profile: str | None = None,
    target: str | None = None,
    prefix: str | None = None,
    root: Path | None = None,
) -> list[dict[str, object]]:
    return _list_console_model_bundles(
        market=market,
        cycle=cycle,
        profile=profile,
        target=target,
        prefix=prefix,
        root=root,
    )


def load_console_bundle(
    *,
    market: str,
    cycle: str | int = "15m",
    profile: str,
    target: str,
    bundle_label: str | None = None,
    bundle_dir: str | Path | None = None,
    root: Path | None = None,
) -> dict[str, object]:
    return _load_console_model_bundle(
        market=market,
        cycle=cycle,
        profile=profile,
        target=target,
        bundle_label=bundle_label,
        bundle_dir=bundle_dir,
        root=root,
    )


def list_console_backtests(
    *,
    market: str,
    cycle: str | int = "15m",
    profile: str | None = None,
    spec_name: str | None = None,
    prefix: str | None = None,
    root: Path | None = None,
) -> list[dict[str, object]]:
    return _list_console_backtest_runs(
        market=market,
        cycle=cycle,
        profile=profile,
        spec_name=spec_name,
        prefix=prefix,
        root=root,
    )


def load_console_backtest(
    *,
    market: str,
    profile: str,
    spec_name: str,
    run_label: str,
    cycle: str | int = "15m",
    root: Path | None = None,
) -> dict[str, object]:
    return _describe_console_backtest_run(
        market=market,
        profile=profile,
        spec_name=spec_name,
        run_label=run_label,
        cycle=cycle,
        root=root,
    )


def load_console_backtest_stake_sweep(
    *,
    market: str,
    profile: str,
    spec_name: str,
    run_label: str,
    cycle: str | int = "15m",
    root: Path | None = None,
) -> dict[str, object]:
    return _describe_console_backtest_stake_sweep(
        market=market,
        profile=profile,
        spec_name=spec_name,
        run_label=run_label,
        cycle=cycle,
        root=root,
    )


def list_console_experiments(
    *,
    suite_name: str | None = None,
    prefix: str | None = None,
    root: Path | None = None,
) -> list[dict[str, object]]:
    return _list_console_experiment_runs(
        suite_name=suite_name,
        prefix=prefix,
        root=root,
    )


def load_console_experiment(
    *,
    suite_name: str,
    run_label: str,
    root: Path | None = None,
) -> dict[str, object]:
    return _describe_console_experiment_run(
        suite_name=suite_name,
        run_label=run_label,
        root=root,
    )


def load_console_experiment_matrix(
    *,
    suite_name: str,
    run_label: str,
    root: Path | None = None,
) -> dict[str, object]:
    return _describe_console_experiment_matrix(
        suite_name=suite_name,
        run_label=run_label,
        root=root,
    )


def load_console_action_catalog(
    *,
    for_section: str | None = None,
    shell_enabled: bool | None = None,
) -> dict[str, object]:
    return _load_console_action_catalog(
        for_section=for_section,
        shell_enabled=shell_enabled,
    )


def build_console_action_request(
    *,
    action_id: str,
    request: Mapping[str, object] | None = None,
) -> dict[str, object]:
    return _build_console_action_request(action_id, request)


def execute_console_action(
    *,
    action_id: str,
    request: Mapping[str, object] | None = None,
) -> dict[str, object]:
    return _execute_console_action(
        action_id=action_id,
        request=request,
    )


def submit_console_action_task(
    *,
    action_id: str,
    request: Mapping[str, object] | None = None,
    root: Path | None = None,
) -> dict[str, object]:
    return _task_detail_payload(
        dict(
            _submit_console_action_task(
                action_id=action_id,
                request=request,
                root=root,
            )
        )
    )


def load_console_task(
    *,
    task_id: str,
    root: Path | None = None,
) -> dict[str, object]:
    return _task_detail_payload(
        dict(
            _load_console_task(
                task_id=task_id,
                root=root,
            )
        ),
        root=root,
    )


def list_console_tasks(
    *,
    action_id: str | None = None,
    action_ids: tuple[str, ...] | list[str] | None = None,
    status: str | None = None,
    status_group: str | None = None,
    marker: str | None = None,
    group_by: str | None = None,
    limit: int = 20,
    root: Path | None = None,
) -> dict[str, object]:
    normalized_action_ids = _task_action_filters(
        action_id=action_id,
        action_ids=action_ids,
    )
    normalized_marker = _normalized_task_history_marker(marker)
    normalized_group_by = _normalized_task_history_group_by(group_by)
    source = _list_console_tasks(
        action_id=action_id,
        action_ids=normalized_action_ids,
        status=status,
        status_group=status_group,
        limit=limit,
        root=root,
    )
    rows = [_task_row_summary(item) for item in _task_rows_payload(source)]
    task_briefs = _task_briefs(rows)
    history_markers = _task_history_markers(rows)
    history_groups = _task_history_groups(rows)
    runtime_state = _runtime_state_payload(dict(_load_console_runtime_summary(root=root)))
    return {
        "domain": "console",
        "dataset": "console_task_list",
        "object_type": "console_task_list",
        "action_id_filter": normalized_action_ids[0] if len(normalized_action_ids) == 1 else None,
        "action_ids_filter": list(normalized_action_ids),
        "status_filter": status,
        "status_group_filter": status_group,
        "marker_filter": normalized_marker,
        "group_by": normalized_group_by,
        "filters": {
            "action_id": normalized_action_ids[0] if len(normalized_action_ids) == 1 else None,
            "action_ids": list(normalized_action_ids),
            "status": status,
            "status_group": status_group,
            "marker": normalized_marker,
            "group_by": normalized_group_by,
            "limit": int(limit),
        },
        "row_count": len(rows),
        "status_counts": _task_status_counts(rows),
        "status_group_counts": _task_status_group_counts(rows),
        "action_counts": _task_action_counts(rows),
        "marker_options": ["latest", "active", "terminal", "failed"],
        "group_by_options": ["action_id", "status", "status_group"],
        "history_markers": history_markers,
        "selected_marker": history_markers.get(normalized_marker) if normalized_marker is not None else None,
        "history_groups": history_groups,
        "selected_group_rows": history_groups.get(normalized_group_by) if normalized_group_by is not None else None,
        "history_scan": runtime_state.get("history_scan"),
        "summary_recovery": runtime_state.get("summary_recovery"),
        "history_retention": runtime_state.get("history_retention"),
        "operator_summary": runtime_state.get("operator_summary"),
        "runtime_board": runtime_state.get("runtime_board"),
        "task_briefs": task_briefs,
        "latest_task_brief": task_briefs[0] if task_briefs else None,
        "rows": rows,
    }


__all__ = [
    "list_console_backtests",
    "list_console_bundles",
    "list_console_experiments",
    "list_console_sections",
    "list_console_training_runs",
    "load_console_action_catalog",
    "execute_console_action",
    "load_console_backtest",
    "load_console_backtest_stake_sweep",
    "load_console_bundle",
    "load_console_data_overview",
    "load_console_experiment",
    "load_console_experiment_matrix",
    "load_console_home",
    "load_console_runtime_history",
    "load_console_runtime_state",
    "load_console_task",
    "load_console_query",
    "load_console_training_run",
    "list_console_tasks",
    "build_console_action_request",
    "submit_console_action_task",
]


def _string_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_string(payload: Mapping[str, object], key: str, default: str | None = None) -> str:
    value = _string_value(payload.get(key))
    if value is not None:
        return value
    if default is not None:
        return default
    raise ValueError(f"缺少必填 console 查询参数: {key}")


def _optional_path(value: object) -> Path | None:
    text = _string_value(value)
    return None if text is None else Path(text)


def _optional_bool(value: object) -> bool | None:
    text = _string_value(value)
    if text is None:
        return None
    token = text.lower()
    if token in {"1", "true", "yes", "on"}:
        return True
    if token in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"无效的布尔值参数: {value!r}")


def _optional_int_or_none(value: object, default: int | None = None) -> int | None:
    text = _string_value(value)
    if text is None:
        return default
    return int(text)


def _optional_csv_strings(value: object) -> tuple[str, ...]:
    text = _string_value(value)
    if text is None:
        return ()
    return tuple(token.strip() for token in text.split(",") if token.strip())


def _mapping_or_none(value: object) -> Mapping[str, object] | None:
    return value if isinstance(value, Mapping) else None


def _task_action_filters(
    *,
    action_id: str | None = None,
    action_ids: tuple[str, ...] | list[str] | None = None,
) -> tuple[str, ...]:
    values: list[str] = []
    primary = _string_value(action_id)
    if primary is not None:
        values.append(primary)
    if action_ids is not None:
        for value in action_ids:
            token = _string_value(value)
            if token is not None and token not in values:
                values.append(token)
    return tuple(values)


def _normalized_task_history_marker(value: object) -> str | None:
    token = _string_value(value)
    if token is None:
        return None
    if token not in {"latest", "active", "terminal", "failed"}:
        raise ValueError(f"不支持的 console task marker: {value!r}")
    return token


def _normalized_task_history_group_by(value: object) -> str | None:
    token = _string_value(value)
    if token is None:
        return None
    if token not in {"action_id", "status", "status_group"}:
        raise ValueError(f"不支持的 console task group_by: {value!r}")
    return token


def _task_rows_payload(source: object) -> list[dict[str, object]]:
    if isinstance(source, Mapping):
        rows = source.get("rows")
        if isinstance(rows, list):
            return [dict(item) for item in rows if isinstance(item, Mapping)]
        return []
    if isinstance(source, list):
        return [dict(item) for item in source if isinstance(item, Mapping)]
    return []


def _task_detail_payload(
    record: Mapping[str, object],
    *,
    root: Path | None = None,
) -> dict[str, object]:
    payload = dict(record)
    result_paths = _task_result_paths(payload.get("result"))
    primary_output = result_paths[0] if result_paths else {"label": None, "path": None}
    error_detail = _task_error_detail(payload)
    linked_object_details = _task_linked_object_details(payload, root=root)
    detail_payload = {
        "domain": "console",
        "dataset": "console_task",
        "object_type": "console_task",
        **payload,
        "subject_summary": _task_subject_summary(payload),
        "status_label": _task_status_label(payload.get("status")),
        "status_group": _task_status_group(payload.get("status")),
        "progress_summary": _task_progress_summary(payload.get("progress")),
        "result_summary": _task_result_summary(payload.get("result")),
        "error_summary": _task_error_summary(payload.get("error")),
        "error_detail": error_detail,
        "request_summary": _task_request_summary(payload.get("request")),
        "primary_output_label": primary_output.get("label"),
        "primary_output_path": primary_output.get("path"),
        "result_paths": result_paths,
        "linked_objects": _task_linked_objects(payload),
        "linked_object_details": linked_object_details,
        "is_terminal": _task_is_terminal(payload.get("status")),
        "action_context": {
            "task_id": payload.get("task_id"),
            "action_id": payload.get("action_id"),
            "status": payload.get("status"),
        },
    }
    detail_payload["result_path_briefs"] = _result_path_briefs(detail_payload["result_paths"])
    detail_payload["linked_object_detail_briefs"] = _linked_object_detail_briefs(linked_object_details)
    detail_payload["error_brief"] = _task_error_brief(detail_payload)
    detail_payload["task_brief"] = _task_brief(detail_payload)
    return detail_payload


def _task_row_summary(record: Mapping[str, object]) -> dict[str, object]:
    progress = record.get("progress")
    result_paths = _task_result_paths(record.get("result"))
    primary_output = result_paths[0] if result_paths else {"label": None, "path": None}
    row_payload = {
        **{str(key): value for key, value in record.items()},
        "object_type": "console_task",
        "subject_summary": _task_subject_summary(record),
        "status_label": _task_status_label(record.get("status")),
        "status_group": _task_status_group(record.get("status")),
        "progress_summary": _task_progress_summary(progress),
        "progress_pct": _task_progress_field(progress, "progress_pct"),
        "current_stage": _task_progress_field(progress, "current_stage"),
        "result_summary": _task_result_summary(record.get("result")),
        "error_summary": _task_error_summary(record.get("error")),
        "request_summary": _task_request_summary(record.get("request")),
        "primary_output_label": primary_output.get("label"),
        "primary_output_path": primary_output.get("path"),
        "linked_objects": _task_linked_objects(record),
        "is_terminal": _task_is_terminal(record.get("status")),
        "action_context": {
            "task_id": record.get("task_id"),
            "action_id": record.get("action_id"),
            "status": record.get("status"),
        },
    }
    row_payload["task_brief"] = _task_brief(row_payload)
    return row_payload


def _task_progress_field(progress: object, key: str) -> object | None:
    if not isinstance(progress, Mapping):
        return None
    return progress.get(key)


def _task_status_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = _string_value(row.get("status")) or "unknown"
        counts[status] = counts.get(status, 0) + 1
    return counts


def _task_status_group_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts = {"active": 0, "terminal": 0, "failed": 0}
    for row in rows:
        group = _task_status_group(row.get("status"))
        if group in counts:
            counts[group] += 1
    return counts


def _task_action_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        action_id = _string_value(row.get("action_id")) or "unknown"
        counts[action_id] = counts.get(action_id, 0) + 1
    return counts


def _task_status_label(status: object) -> str:
    token = _string_value(status) or "unknown"
    return _STATUS_LABELS.get(token.lower(), token.replace("_", " "))


def _task_status_group(status: object) -> str:
    token = _string_value(status) or ""
    if token in {"queued", "running"}:
        return "active"
    if token in {"failed", "error"}:
        return "failed"
    if token in {"succeeded", "ok"}:
        return "terminal"
    return "unknown"


def _progress_label(value: object) -> str | None:
    text = _string_value(value)
    if text is None:
        return None
    return _STATUS_LABELS.get(text.lower(), text)


def _stage_label(value: object) -> str | None:
    text = _string_value(value)
    if text is None:
        return None
    return _STAGE_LABELS.get(text, text.replace("_", " "))


def _normalized_progress_text(value: object) -> str | None:
    text = _string_value(value)
    if text is None:
        return None
    parts = [_normalized_progress_part(part) for part in text.split(" · ")]
    tokens = [part for part in parts if part]
    return " · ".join(tokens) if tokens else None


def _normalized_progress_part(value: str) -> str:
    token = value.strip()
    if not token:
        return ""
    if token.lower() in _STATUS_LABELS:
        return _STATUS_LABELS[token.lower()]
    if token in _STAGE_LABELS:
        return _STAGE_LABELS[token]
    return token


def _task_progress_summary(progress: object) -> str | None:
    if not isinstance(progress, Mapping):
        return None
    summary = _string_value(progress.get("summary"))
    stage = _string_value(progress.get("current_stage"))
    pct = _optional_int_or_none(progress.get("progress_pct"))
    parts = [part for part in (_progress_label(summary), _stage_label(stage)) if part]
    if pct is not None:
        parts.append(f"{pct}%")
    return " · ".join(parts) if parts else None


def _task_result_summary(result: object) -> str | None:
    if result is None:
        return None
    if not isinstance(result, Mapping):
        return _string_value(result)
    dataset = _string_value(result.get("dataset")) or _string_value(result.get("object_type"))
    label = (
        _string_value(result.get("bundle_label"))
        or _string_value(result.get("run_label"))
        or _string_value(result.get("suite_name"))
        or _string_value(result.get("task_id"))
    )
    if dataset and label:
        return f"{dataset}: {label}"
    if dataset:
        return dataset
    if _string_value(result.get("selection_path")):
        return "已更新激活 bundle 选择"
    if _string_value(result.get("summary_path")):
        return "已写入摘要产物"
    if _string_value(result.get("report_path")):
        return "已写入报告"
    status = _string_value(result.get("status"))
    if status:
        return f"状态: {status}"
    return None


def _task_result_paths(result: object) -> list[dict[str, str]]:
    if not isinstance(result, Mapping):
        return []
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for label in (
        "bundle_dir",
        "selection_path",
        "run_dir",
        "summary_path",
        "report_path",
        "manifest_path",
    ):
        value = _string_value(result.get(label))
        if value is None or value in seen:
            continue
        seen.add(value)
        rows.append({"label": label, "path": value})
    return rows


def _task_linked_objects(record: Mapping[str, object]) -> list[dict[str, str]]:
    action_id = _string_value(record.get("action_id")) or ""
    path_lookup = {
        item["label"]: item["path"]
        for item in _task_result_paths(record.get("result"))
        if isinstance(item, Mapping)
    }
    rows: list[dict[str, str]] = []

    def _append(object_type: str, *, path_label: str, title: str) -> None:
        path = _string_value(path_lookup.get(path_label))
        if path is None:
            return
        row = {
            "object_type": object_type,
            "title": title,
            "path": path,
        }
        for extra_label in ("summary_path", "report_path", "manifest_path"):
            extra_path = _string_value(path_lookup.get(extra_label))
            if extra_path is not None:
                row[extra_label] = extra_path
        rows.append(row)

    if action_id == "research_train_run":
        _append("training_run", path_label="run_dir", title="训练运行")
    elif action_id == "research_bundle_build":
        _append("model_bundle", path_label="bundle_dir", title="模型包")
    elif action_id == "research_activate_bundle":
        _append("active_bundle_selection", path_label="selection_path", title="当前激活模型包")
        _append("model_bundle", path_label="bundle_dir", title="模型包")
    elif action_id == "research_backtest_run":
        _append("backtest_run", path_label="run_dir", title="回测运行")
    elif action_id == "research_experiment_run_suite":
        _append("experiment_run", path_label="run_dir", title="实验运行")

    return rows


def _task_linked_object_details(
    record: Mapping[str, object],
    *,
    root: Path | None = None,
) -> list[dict[str, object]]:
    action_id = _string_value(record.get("action_id")) or ""
    request = _mapping_or_none(record.get("request")) or {}
    result = _mapping_or_none(record.get("result")) or {}
    rows: list[dict[str, object]] = []

    def _append(detail: Mapping[str, object] | None, *, object_type: str, title: str, path: str) -> None:
        if detail is None:
            return
        rows.append(
            {
                "object_type": object_type,
                "title": title,
                "path": path,
                "identity": _linked_object_identity(detail),
                "summary": _linked_object_summary(detail),
            }
        )

    try:
        if action_id == "research_train_run":
            run_dir = _string_value(result.get("run_dir"))
            if run_dir is not None:
                detail = _load_console_training_run(
                    market=_string_value(request.get("market")) or "sol",
                    cycle=_string_value(request.get("cycle")) or "15m",
                    model_family=_string_value(request.get("model_family")),
                    target=_string_value(request.get("target")),
                    run_dir=run_dir,
                    root=root,
                )
                _append(detail, object_type="training_run", title="训练运行", path=run_dir)
        elif action_id == "research_bundle_build":
            bundle_dir = _string_value(result.get("bundle_dir"))
            if bundle_dir is not None:
                detail = _load_console_model_bundle(
                    market=_string_value(request.get("market")) or "sol",
                    cycle=_string_value(request.get("cycle")) or "15m",
                    profile=_string_value(request.get("profile")) or "deep_otm",
                    target=_string_value(request.get("target")) or "direction",
                    bundle_dir=bundle_dir,
                    root=root,
                )
                _append(detail, object_type="model_bundle", title="模型包", path=bundle_dir)
        elif action_id == "research_activate_bundle":
            bundle_dir = _string_value(result.get("bundle_dir"))
            if bundle_dir is not None:
                detail = _load_console_model_bundle(
                    market=_string_value(request.get("market")) or "sol",
                    cycle=_string_value(request.get("cycle")) or "15m",
                    profile=_string_value(request.get("profile")) or "deep_otm",
                    target=_string_value(request.get("target")) or "direction",
                    bundle_dir=bundle_dir,
                    root=root,
                )
                _append(detail, object_type="model_bundle", title="模型包", path=bundle_dir)
            selection_path = _string_value(result.get("selection_path"))
            if selection_path is not None:
                rows.append(
                    {
                        "object_type": "active_bundle_selection",
                        "title": "当前激活模型包",
                        "path": selection_path,
                        "identity": _string_value(request.get("bundle_label")) or _string_value(result.get("bundle_label")),
                        "summary": {},
                    }
                )
        elif action_id == "research_backtest_run":
            run_label = _string_value(request.get("run_label"))
            if run_label is not None:
                detail = _describe_console_backtest_run(
                    market=_string_value(request.get("market")) or "sol",
                    profile=_string_value(request.get("profile")) or "deep_otm",
                    spec_name=_string_value(request.get("spec")) or "baseline_truth",
                    run_label=run_label,
                    cycle=_string_value(request.get("cycle")) or "15m",
                    root=root,
                )
                _append(
                    detail,
                    object_type="backtest_run",
                    title="回测运行",
                    path=_string_value(result.get("run_dir")) or run_label,
                )
        elif action_id == "research_experiment_run_suite":
            suite_name = _string_value(request.get("suite"))
            run_label = _string_value(request.get("run_label"))
            if suite_name is not None and run_label is not None:
                detail = _describe_console_experiment_run(
                    suite_name=suite_name,
                    run_label=run_label,
                    root=root,
                )
                _append(
                    detail,
                    object_type="experiment_run",
                    title="实验运行",
                    path=_string_value(result.get("run_dir")) or run_label,
                )
    except Exception:
        return rows
    return rows


def _task_error_summary(error: object) -> str | None:
    if error is None:
        return None
    if not isinstance(error, Mapping):
        return _string_value(error)
    error_type = _string_value(error.get("type"))
    message = _string_value(error.get("message"))
    if error_type and message:
        return f"{error_type}: {message}"
    return error_type or message


def _task_error_detail(record: Mapping[str, object]) -> dict[str, object]:
    rows: dict[str, object] = {}
    error = record.get("error")
    if isinstance(error, Mapping):
        error_type = _string_value(error.get("type"))
        error_message = _string_value(error.get("message"))
        if error_type is not None:
            rows["type"] = error_type
        if error_message is not None:
            rows["message"] = error_message
    elif error is not None:
        message = _string_value(error)
        if message is not None:
            rows["message"] = message

    result = record.get("result")
    if isinstance(result, Mapping):
        status = _string_value(result.get("status"))
        if status is not None:
            rows["result_status"] = status
        return_code = result.get("return_code")
        if return_code is not None:
            rows["return_code"] = return_code
        stderr_text = _string_value(result.get("stderr"))
        if stderr_text is not None:
            stderr_lines = [line.strip() for line in stderr_text.splitlines() if line.strip()]
            if stderr_lines:
                rows["stderr_excerpt"] = stderr_lines[-3:]
        execution_summary = result.get("execution_summary")
        if isinstance(execution_summary, Mapping):
            last_stderr_line = _string_value(execution_summary.get("last_stderr_line"))
            if last_stderr_line is not None:
                rows["last_stderr_line"] = last_stderr_line
    return rows


def _runtime_state_payload(summary: Mapping[str, object]) -> dict[str, object]:
    payload = dict(summary)
    recent_tasks = _task_rows_payload(payload.get("recent_tasks"))
    recent_active_tasks = _task_rows_payload(payload.get("recent_active_tasks"))
    recent_terminal_tasks = _task_rows_payload(payload.get("recent_terminal_tasks"))
    recent_failed_tasks = _task_rows_payload(payload.get("recent_failed_tasks"))
    latest_markers = _mapping_or_none(payload.get("latest_markers")) or {}
    recent_task_briefs = _task_briefs(recent_tasks)
    recent_active_task_briefs = _task_briefs(recent_active_tasks)
    recent_terminal_task_briefs = _task_briefs(recent_terminal_tasks)
    recent_failed_task_briefs = _task_briefs(recent_failed_tasks)
    latest_task_brief = _task_brief(_mapping_or_none(latest_markers.get("latest"))) or (recent_task_briefs[0] if recent_task_briefs else None)
    latest_active_task_brief = _task_brief(_mapping_or_none(latest_markers.get("active"))) or (recent_active_task_briefs[0] if recent_active_task_briefs else None)
    latest_terminal_task_brief = _task_brief(_mapping_or_none(latest_markers.get("terminal"))) or (recent_terminal_task_briefs[0] if recent_terminal_task_briefs else None)
    latest_failed_task_brief = _task_brief(_mapping_or_none(latest_markers.get("failed"))) or (recent_failed_task_briefs[0] if recent_failed_task_briefs else None)
    payload["recent_task_briefs"] = recent_task_briefs
    payload["recent_active_task_briefs"] = recent_active_task_briefs
    payload["recent_terminal_task_briefs"] = recent_terminal_task_briefs
    payload["recent_failed_task_briefs"] = recent_failed_task_briefs
    payload["latest_task_briefs"] = {
        "latest": latest_task_brief,
        "active": latest_active_task_brief,
        "terminal": latest_terminal_task_brief,
        "failed": latest_failed_task_brief,
    }
    payload["runtime_board"] = _runtime_board_payload(payload)
    payload["operator_summary"] = {
        "has_active_tasks": bool(recent_active_task_briefs),
        "has_failed_tasks": bool(recent_failed_task_briefs),
        "active_task_count": len(recent_active_task_briefs),
        "failed_task_count": len(recent_failed_task_briefs),
        "invalid_task_file_count": _runtime_history_scan_count(payload.get("history_scan"), "invalid_task_file_count"),
        "invalid_task_files": _runtime_invalid_task_files(payload.get("history_scan")),
        "history_retention": _runtime_retention_payload(payload.get("history_retention")),
        "history_truncated": _runtime_retention_flag(payload.get("history_retention"), "is_truncated"),
        "history_limit": _runtime_retention_count(payload.get("history_retention"), "row_limit"),
        "history_group_limit": _runtime_retention_count(payload.get("history_retention"), "group_row_limit"),
        "retained_task_count": _runtime_retention_count(payload.get("history_retention"), "retained_task_count"),
        "dropped_task_count": _runtime_retention_count(payload.get("history_retention"), "dropped_task_count"),
        "summary_source": _string_value(payload.get("summary_source")),
        "recovery_reason": _runtime_recovery_reason(payload.get("summary_recovery")),
        "warnings": _runtime_board_warnings(payload),
        "latest_headline": _brief_field(payload["latest_task_briefs"]["latest"], "headline"),
        "latest_active_headline": _brief_field(payload["latest_task_briefs"]["active"], "headline"),
        "latest_terminal_headline": _brief_field(payload["latest_task_briefs"]["terminal"], "headline"),
        "latest_failed_headline": _brief_field(payload["latest_task_briefs"]["failed"], "headline"),
        "latest_failed_summary": _brief_field(payload["latest_task_briefs"]["failed"], "summary"),
    }
    return payload


def _runtime_history_payload(history: Mapping[str, object]) -> dict[str, object]:
    payload = dict(history)
    rows = _task_rows_payload(payload.get("rows"))
    groups_payload = _mapping_or_none(payload.get("groups")) or {}
    group_briefs = {
        group: _task_briefs(_task_rows_payload((_mapping_or_none(groups_payload.get(group)) or {}).get("rows")))
        for group in ("active", "terminal", "failed")
    }
    payload["task_briefs"] = _task_briefs(rows)
    payload["group_task_briefs"] = group_briefs
    payload["operator_summary"] = {
        "task_count": _optional_int_or_none(payload.get("task_count")) or len(rows),
        "row_count": _optional_int_or_none(payload.get("row_count")) or len(rows),
        "invalid_task_file_count": _runtime_history_scan_count(payload.get("history_scan"), "invalid_task_file_count"),
        "invalid_task_files": _runtime_invalid_task_files(payload.get("history_scan")),
        "retention": _runtime_retention_payload(payload.get("retention")),
        "history_truncated": _runtime_retention_flag(payload.get("retention"), "is_truncated"),
        "history_limit": _runtime_retention_count(payload.get("retention"), "row_limit"),
        "history_group_limit": _runtime_retention_count(payload.get("retention"), "group_row_limit"),
        "retained_task_count": _runtime_retention_count(payload.get("retention"), "retained_task_count"),
        "dropped_task_count": _runtime_retention_count(payload.get("retention"), "dropped_task_count"),
        "history_source": _string_value(payload.get("history_source")),
        "recovery_reason": _runtime_recovery_reason(payload.get("history_recovery")),
        "updated_at": _string_value(payload.get("updated_at")),
        "latest_headline": _brief_field(payload["task_briefs"][0] if payload["task_briefs"] else None, "headline"),
    }
    return payload


def _runtime_board_payload(summary: Mapping[str, object]) -> dict[str, object]:
    latest_task_briefs = _mapping_or_none(summary.get("latest_task_briefs")) or {}
    history_groups = _mapping_or_none(summary.get("history_groups")) or {}
    return {
        "summary_source": _string_value(summary.get("summary_source")),
        "recovery": _mapping_or_none(summary.get("summary_recovery")) or {},
        "history_scan": _mapping_or_none(summary.get("history_scan")) or {},
        "invalid_task_files": _runtime_invalid_task_files(summary.get("history_scan")),
        "retention": _runtime_retention_payload(summary.get("history_retention")),
        "latest": _mapping_or_none(latest_task_briefs.get("latest")),
        "active": _mapping_or_none(latest_task_briefs.get("active")),
        "terminal": _mapping_or_none(latest_task_briefs.get("terminal")),
        "failed": _mapping_or_none(latest_task_briefs.get("failed")),
        "status_groups": list(history_groups.get("status_group") or []),
        "action_groups": list(history_groups.get("action_id") or []),
        "warnings": _runtime_board_warnings(summary),
    }


def _runtime_history_scan_count(scan: object, key: str) -> int:
    if not isinstance(scan, Mapping):
        return 0
    value = scan.get(key)
    try:
        return int(value)
    except Exception:
        return 0


def _runtime_invalid_task_files(scan: object) -> list[dict[str, object]]:
    if not isinstance(scan, Mapping):
        return []
    rows = scan.get("invalid_task_files")
    if not isinstance(rows, list):
        return []
    return [dict(item) for item in rows if isinstance(item, Mapping)]


def _runtime_retention_payload(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {
            "total_task_count": 0,
            "retained_task_count": 0,
            "dropped_task_count": 0,
            "is_truncated": False,
            "row_limit": 0,
            "group_row_limit": 0,
        }
    return {
        "total_task_count": _optional_int_or_none(value.get("total_task_count")) or 0,
        "retained_task_count": _optional_int_or_none(value.get("retained_task_count")) or 0,
        "dropped_task_count": _optional_int_or_none(value.get("dropped_task_count")) or 0,
        "is_truncated": bool(value.get("is_truncated")),
        "row_limit": _optional_int_or_none(value.get("row_limit")) or 0,
        "group_row_limit": _optional_int_or_none(value.get("group_row_limit")) or 0,
    }


def _runtime_retention_count(value: object, key: str) -> int:
    return int(_runtime_retention_payload(value).get(key) or 0)


def _runtime_retention_flag(value: object, key: str) -> bool:
    return bool(_runtime_retention_payload(value).get(key))


def _runtime_board_warnings(summary: Mapping[str, object]) -> list[dict[str, object]]:
    warnings: list[dict[str, object]] = []
    invalid_files = _runtime_invalid_task_files(summary.get("history_scan"))
    if invalid_files:
        warnings.append(
            {
                "code": "invalid_task_files",
                "severity": "warning",
                "message": f"运行态扫描时跳过了 {len(invalid_files)} 个无效任务文件。",
                "items": invalid_files,
            }
        )
    retention = _runtime_retention_payload(summary.get("history_retention"))
    if retention.get("is_truncated"):
        warnings.append(
            {
                "code": "history_truncated",
                "severity": "info",
                "message": (
                    f"持久化运行历史仅保留最近 {retention['retained_task_count']} 个任务，"
                    f"已省略 {retention['dropped_task_count']} 个更早任务。"
                ),
                "retention": retention,
            }
        )
    return warnings


def _runtime_recovery_reason(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    return _string_value(value.get("reason"))


def _task_history_markers(rows: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    latest = _task_marker(rows[0]) if rows else {}
    return {
        "latest": latest,
        "active": _first_task_marker(rows, status_group="active"),
        "terminal": _first_task_marker(rows, status_group="terminal"),
        "failed": _first_task_marker(rows, status_group="failed"),
    }


def _task_history_groups(rows: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    return {
        "action_id": _task_groups_by_action(rows),
        "status": _task_groups_by_status(rows),
        "status_group": _task_groups_by_status_group(rows),
    }


def _first_task_marker(
    rows: list[dict[str, object]],
    *,
    status_group: str,
) -> dict[str, object]:
    for row in rows:
        if _task_status_group(row.get("status")) == status_group:
            return _task_marker(row)
    return {}


def _task_groups_by_action(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        action_id = _string_value(row.get("action_id")) or "unknown"
        grouped.setdefault(action_id, []).append(row)
    result: list[dict[str, object]] = []
    for action_id, members in grouped.items():
        result.append(
            {
                "group": action_id,
                "action_id": action_id,
                "count": len(members),
                "status_counts": _task_status_counts(members),
                "status_group_counts": _task_status_group_counts(members),
                "latest_marker": _task_marker(members[0]),
            }
        )
    result.sort(
        key=lambda item: (
            _marker_updated_at(item.get("latest_marker")),
            _string_value(item.get("action_id")) or "",
        ),
        reverse=True,
    )
    return result


def _task_groups_by_status(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        status = _string_value(row.get("status")) or "unknown"
        grouped.setdefault(status, []).append(row)
    result: list[dict[str, object]] = []
    for status, members in grouped.items():
        result.append(
            {
                "group": status,
                "status": status,
                "count": len(members),
                "latest_marker": _task_marker(members[0]),
            }
        )
    result.sort(
        key=lambda item: (
            _marker_updated_at(item.get("latest_marker")),
            _string_value(item.get("status")) or "",
        ),
        reverse=True,
    )
    return result


def _task_groups_by_status_group(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    counts = _task_status_group_counts(rows)
    markers = _task_history_markers(rows)
    return [
        {
            "group": group,
            "status_group": group,
            "count": int(counts.get(group, 0)),
            "latest_marker": markers.get(group) or {},
        }
        for group in ("active", "terminal", "failed")
    ]


def _task_briefs(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    briefs: list[dict[str, object]] = []
    for row in rows:
        brief = _task_brief(row)
        if brief is not None:
            briefs.append(brief)
    return briefs


def _task_brief(record: Mapping[str, object] | None) -> dict[str, object] | None:
    if not isinstance(record, Mapping):
        return None
    task_id = _string_value(record.get("task_id"))
    action_id = _string_value(record.get("action_id"))
    status = _string_value(record.get("status"))
    status_group = _string_value(record.get("status_group")) or _task_status_group(status)
    status_label = _task_status_label(_string_value(record.get("status_label")) or status)
    subject_summary = _string_value(record.get("subject_summary")) or _task_subject_summary(record)
    progress_summary = _normalized_progress_text(record.get("progress_summary")) or _task_progress_summary(record.get("progress"))
    result_summary = _string_value(record.get("result_summary")) or _task_result_summary(record.get("result"))
    error_summary = _string_value(record.get("error_summary")) or _task_error_summary(record.get("error"))
    primary_output_path = _string_value(record.get("primary_output_path"))
    linked_object = _task_linked_object_label(record)
    if all(
        value is None
        for value in (
            task_id,
            action_id,
            status,
            subject_summary,
            progress_summary,
            result_summary,
            error_summary,
            primary_output_path,
            linked_object,
        )
    ):
        return None
    headline_parts = [part for part in (status_label, subject_summary, task_id) if part]
    summary = error_summary or progress_summary or result_summary
    supporting_parts: list[str] = []
    for value in (progress_summary, result_summary, linked_object, primary_output_path):
        text = _string_value(value)
        if text is None or text == summary or text in supporting_parts:
            continue
        supporting_parts.append(text)
    return {
        "task_id": task_id,
        "action_id": action_id,
        "status": status,
        "status_label": status_label,
        "status_group": status_group,
        "subject_summary": subject_summary,
        "headline": " · ".join(headline_parts) if headline_parts else None,
        "summary": summary,
        "supporting_text": " | ".join(supporting_parts) if supporting_parts else None,
        "progress_summary": progress_summary,
        "result_summary": result_summary,
        "error_summary": error_summary,
        "linked_object": linked_object,
        "primary_output_path": primary_output_path,
        "updated_at": _string_value(record.get("updated_at")),
        "request_summary": _task_request_summary(record.get("request_summary") or record.get("request")),
    }


def _task_marker(record: Mapping[str, object] | None) -> dict[str, object]:
    brief = _task_brief(record)
    if brief is None:
        return {}
    return {
        "task_id": brief.get("task_id"),
        "action_id": brief.get("action_id"),
        "status": brief.get("status"),
        "status_label": brief.get("status_label"),
        "status_group": brief.get("status_group"),
        "subject_summary": brief.get("subject_summary"),
        "headline": brief.get("headline"),
        "summary": brief.get("summary"),
        "supporting_text": brief.get("supporting_text"),
        "primary_output_path": brief.get("primary_output_path"),
        "updated_at": brief.get("updated_at"),
    }


def _marker_updated_at(marker: object) -> str:
    if not isinstance(marker, Mapping):
        return ""
    return _string_value(marker.get("updated_at")) or ""


def _task_linked_object_label(record: Mapping[str, object]) -> str | None:
    detail_rows = record.get("linked_object_detail_briefs")
    if isinstance(detail_rows, list):
        for row in detail_rows:
            if isinstance(row, Mapping):
                headline = _string_value(row.get("headline"))
                if headline is not None:
                    return headline
    linked_object_details = record.get("linked_object_details")
    if isinstance(linked_object_details, list):
        detail_briefs = _linked_object_detail_briefs(linked_object_details)
        if detail_briefs:
            return _string_value(detail_briefs[0].get("headline"))
    linked_objects = record.get("linked_objects")
    if isinstance(linked_objects, list):
        for row in linked_objects:
            if not isinstance(row, Mapping):
                continue
            title = _string_value(row.get("title")) or _string_value(row.get("object_type"))
            path = _string_value(row.get("path"))
            if title and path:
                return f"{title} @ {path}"
            if title or path:
                return title or path
    return None


def _result_path_briefs(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    briefs: list[dict[str, str]] = []
    for row in rows:
        label = _string_value(row.get("label"))
        path = _string_value(row.get("path"))
        if label is None or path is None:
            continue
        briefs.append(
            {
                "label": label,
                "path": path,
                "headline": f"{label} @ {path}",
            }
        )
    return briefs


def _linked_object_detail_briefs(rows: object) -> list[dict[str, object]]:
    if not isinstance(rows, list):
        return []
    briefs: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        title = _string_value(row.get("title")) or _string_value(row.get("object_type"))
        identity = _string_value(row.get("identity"))
        path = _string_value(row.get("path"))
        summary = _mapping_or_none(row.get("summary")) or {}
        summary_items = [
            {"label": key, "value": value}
            for key, value in summary.items()
            if value not in (None, "", [])
        ]
        summary_text = " · ".join(
            f"{item['label']}={_summary_value_text(item['value'])}"
            for item in summary_items
        )
        headline = " · ".join(part for part in (title, identity) if part) or path
        briefs.append(
            {
                "object_type": _string_value(row.get("object_type")),
                "title": title,
                "identity": identity,
                "path": path,
                "headline": headline,
                "summary_items": summary_items,
                "summary_text": summary_text or None,
            }
        )
    return briefs


def _task_error_brief(record: Mapping[str, object]) -> dict[str, object] | None:
    error_detail = _mapping_or_none(record.get("error_detail")) or _task_error_detail(record)
    error_summary = _string_value(record.get("error_summary")) or _task_error_summary(record.get("error"))
    if not error_summary and not error_detail:
        return None
    supporting_text = (
        _string_value(error_detail.get("last_stderr_line"))
        or _string_value(error_detail.get("message"))
    )
    stderr_excerpt = error_detail.get("stderr_excerpt")
    return {
        "headline": error_summary,
        "supporting_text": supporting_text,
        "type": _string_value(error_detail.get("type")),
        "message": _string_value(error_detail.get("message")),
        "result_status": _string_value(error_detail.get("result_status")),
        "return_code": error_detail.get("return_code"),
        "last_stderr_line": _string_value(error_detail.get("last_stderr_line")),
        "stderr_excerpt": list(stderr_excerpt) if isinstance(stderr_excerpt, list) else [],
    }


def _brief_field(payload: Mapping[str, object] | None, key: str) -> object | None:
    if not isinstance(payload, Mapping):
        return None
    return payload.get(key)


def _summary_value_text(value: object) -> str:
    if isinstance(value, list):
        return ", ".join(_summary_value_text(item) for item in value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _linked_object_identity(detail: Mapping[str, object]) -> str | None:
    for key in ("bundle_label", "run_label", "suite_name", "spec_name"):
        value = _string_value(detail.get(key))
        if value is not None:
            return value
    return None


def _linked_object_summary(detail: Mapping[str, object]) -> dict[str, object]:
    summary: dict[str, object] = {}
    for key in (
        "market",
        "cycle",
        "profile",
        "target",
        "model_family",
        "feature_set",
        "label_set",
        "offset_count",
        "is_active",
        "trades",
        "roi_pct",
        "pnl_sum",
        "cases",
        "completed_cases",
        "window",
    ):
        value = detail.get(key)
        if value in (None, "", []):
            continue
        summary[key] = value
    return summary


def _task_request_summary(request: object) -> dict[str, object]:
    if not isinstance(request, Mapping):
        return {}
    summary: dict[str, object] = {}
    for key in (
        "market",
        "cycle",
        "surface",
        "profile",
        "target",
        "model_family",
        "bundle_label",
        "run_label",
        "suite",
        "spec",
        "sync_command",
        "build_command",
        "source_training_run",
        "offsets",
    ):
        value = request.get(key)
        if value in (None, "", []):
            continue
        summary[key] = value
    return summary


def _task_subject_summary(record: Mapping[str, object]) -> str | None:
    request = _task_request_summary(record.get("request"))
    action_id = _string_value(record.get("action_id")) or ""
    market = _string_value(request.get("market"))
    cycle = _string_value(request.get("cycle"))
    market_scope = " / ".join(part for part in (market, cycle) if part)
    if action_id == "data_sync":
        command = _string_value(request.get("sync_command"))
        return _join_task_subject(command, market_scope)
    if action_id == "data_build":
        command = _string_value(request.get("build_command"))
        return _join_task_subject(command, market_scope)
    if action_id == "research_train_run":
        run_label = _string_value(request.get("run_label"))
        return _join_task_subject(run_label, market_scope)
    if action_id == "research_bundle_build":
        bundle_label = _string_value(request.get("bundle_label"))
        source_training_run = _string_value(request.get("source_training_run"))
        bundle_subject = bundle_label if source_training_run is None else f"{bundle_label} 来自 {source_training_run}"
        return _join_task_subject(bundle_subject, market_scope)
    if action_id == "research_activate_bundle":
        bundle_label = _string_value(request.get("bundle_label"))
        return _join_task_subject(bundle_label, market_scope)
    if action_id == "research_backtest_run":
        spec_name = _string_value(request.get("spec"))
        run_label = _string_value(request.get("run_label"))
        return _join_task_subject(" / ".join(part for part in (spec_name, run_label) if part), market_scope)
    if action_id == "research_experiment_run_suite":
        suite_name = _string_value(request.get("suite"))
        run_label = _string_value(request.get("run_label"))
        return _join_task_subject(" / ".join(part for part in (suite_name, run_label) if part), market_scope)
    return market_scope or None


def _join_task_subject(primary: str | None, scope: str | None) -> str | None:
    values = [part for part in (primary, scope) if part]
    if not values:
        return None
    return " | ".join(values)


def _task_is_terminal(status: object) -> bool:
    token = _string_value(status) or ""
    return token in {"succeeded", "failed", "ok", "error"}


def _optional_int(value: object, *, default: int) -> int:
    text = _string_value(value)
    if text is None:
        return int(default)
    return int(text)
