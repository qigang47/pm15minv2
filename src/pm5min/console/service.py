from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from .actions import (
    build_console_action_request as _build_console_action_request,
    load_console_action_catalog as _load_console_action_catalog,
)
from .read_models import (
    describe_console_backtest_run as _describe_console_backtest_run,
    describe_console_experiment_run as _describe_console_experiment_run,
    list_console_backtest_runs as _list_console_backtest_runs,
    list_console_experiment_runs as _list_console_experiment_runs,
    list_console_model_bundles as _list_console_model_bundles,
    list_console_training_runs as _list_console_training_runs,
    load_console_model_bundle as _load_console_model_bundle,
    load_console_training_run as _load_console_training_run,
    load_data_overview as _load_data_overview,
)
from .read_models.common import json_ready
from .runtime_views import (
    build_runtime_history_payload,
    build_runtime_state_payload,
    build_task_detail_payload,
    build_task_list_payload,
)
from .tasks import (
    load_console_runtime_history as _load_console_runtime_history,
    load_console_runtime_summary as _load_console_runtime_summary,
    load_console_task as _load_console_task,
    list_console_tasks as _list_console_tasks,
)


_SECTION_DEFINITIONS: tuple[dict[str, object], ...] = (
    {
        "id": "data_overview",
        "title": "数据总览",
        "kind": "singleton",
        "list_command": "console show-data-overview --market <market> --cycle <cycle> --surface <surface>",
        "detail_command": None,
        "primary_object_type": "data_surface_summary",
    },
    {
        "id": "training_runs",
        "title": "训练运行",
        "kind": "list_detail",
        "list_command": "console list-training-runs --market <market> --cycle <cycle>",
        "detail_command": "console show-training-run --market <market> --model-family <family> --target <target> --run-label <run>",
        "primary_object_type": "training_run",
    },
    {
        "id": "bundles",
        "title": "模型包",
        "kind": "list_detail",
        "list_command": "console list-bundles --market <market> --cycle <cycle>",
        "detail_command": "console show-bundle --market <market> --profile <profile> --target <target> --bundle-label <bundle>",
        "primary_object_type": "model_bundle",
    },
    {
        "id": "backtests",
        "title": "回测结果",
        "kind": "list_detail",
        "list_command": "console list-backtests --market <market> --cycle <cycle>",
        "detail_command": "console show-backtest --market <market> --profile <profile> --spec <spec> --run-label <run>",
        "primary_object_type": "backtest_run",
    },
    {
        "id": "experiments",
        "title": "实验对比",
        "kind": "list_detail",
        "list_command": "console list-experiments --suite <suite>",
        "detail_command": "console show-experiment --suite <suite> --run-label <run>",
        "primary_object_type": "experiment_run",
    },
)
def load_console_home(*, root: Path | None = None) -> dict[str, object]:
    sections = _list_console_sections()
    runtime_summary = load_console_runtime_state(root=root)
    status_group_counts = _mapping_or_none(runtime_summary.get("status_group_counts")) or {}
    return json_ready(
        {
            "domain": "console",
            "dataset": "console_home",
            "root": None if root is None else str(Path(root)),
            "read_only": True,
            "sections": sections,
            "section_count": len(sections),
            "runtime_summary": runtime_summary,
            "runtime_board": runtime_summary.get("runtime_board"),
            "runtime_task_count": runtime_summary.get("task_count", 0),
            "active_task_count": int(status_group_counts.get("active", 0)),
            "terminal_task_count": int(status_group_counts.get("terminal", 0)),
            "failed_task_count": int(status_group_counts.get("failed", 0)),
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


def load_console_runtime_state(*, root: Path | None = None) -> dict[str, object]:
    return json_ready(build_runtime_state_payload(dict(_load_console_runtime_summary(root=root))))


def load_console_runtime_history(*, root: Path | None = None) -> dict[str, object]:
    return json_ready(build_runtime_history_payload(dict(_load_console_runtime_history(root=root))))


def load_console_action_catalog(
    *,
    for_section: str | None = None,
    shell_enabled: bool | None = None,
) -> dict[str, object]:
    return _load_console_action_catalog(for_section=for_section, shell_enabled=shell_enabled)


def build_console_action_request(
    *,
    action_id: str,
    request: Mapping[str, object] | None = None,
) -> dict[str, object]:
    return _build_console_action_request(action_id, request)


def list_console_tasks(
    *,
    action_id: str | None = None,
    action_ids: Sequence[str] | None = None,
    status: str | None = None,
    status_group: str | None = None,
    marker: str | None = None,
    group_by: str | None = None,
    limit: int = 20,
    root: Path | None = None,
) -> dict[str, object]:
    runtime_state = build_runtime_state_payload(dict(_load_console_runtime_summary(root=root)))
    return json_ready(
        build_task_list_payload(
            rows_source=_list_console_tasks(
                root=root,
                action_id=action_id,
                action_ids=action_ids,
                status=status,
                status_group=status_group,
                limit=limit,
            ),
            runtime_state=runtime_state,
            action_id=action_id,
            action_ids=action_ids,
            status=status,
            status_group=status_group,
            marker=marker,
            group_by=group_by,
            limit=limit,
        )
    )


def load_console_task(
    *,
    task_id: str,
    root: Path | None = None,
) -> dict[str, object]:
    return json_ready(build_task_detail_payload(dict(_load_console_task(task_id=task_id, root=root))))


def load_console_data_overview(
    *,
    market: str,
    cycle: str | int = "5m",
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
    cycle: str | int = "5m",
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
    cycle: str | int = "5m",
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
    cycle: str | int = "5m",
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
    cycle: str | int = "5m",
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
    cycle: str | int = "5m",
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
    cycle: str | int = "5m",
    profile: str,
    spec_name: str,
    run_label: str,
    root: Path | None = None,
) -> dict[str, object]:
    return _describe_console_backtest_run(
        market=market,
        cycle=cycle,
        profile=profile,
        spec_name=spec_name,
        run_label=run_label,
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


def _list_console_sections() -> list[dict[str, object]]:
    return [dict(item) for item in _SECTION_DEFINITIONS]

def _mapping_or_none(value: object) -> dict[str, object] | None:
    return {str(key): item for key, item in value.items()} if isinstance(value, Mapping) else None


__all__ = [
    "build_console_action_request",
    "list_console_backtests",
    "list_console_bundles",
    "list_console_experiments",
    "list_console_tasks",
    "list_console_training_runs",
    "load_console_action_catalog",
    "load_console_backtest",
    "load_console_bundle",
    "load_console_data_overview",
    "load_console_experiment",
    "load_console_home",
    "load_console_runtime_history",
    "load_console_runtime_state",
    "load_console_task",
    "load_console_training_run",
]
