from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pm15min.console import service as console_service


JsonMapping = Mapping[str, str]
SectionHandler = Callable[[JsonMapping], object]


@dataclass(frozen=True)
class ConsoleSectionRoute:
    path: str
    section_id: str
    handler: SectionHandler


def build_default_section_handlers() -> dict[str, SectionHandler]:
    return {
        "/api/console/home": _handle_home,
        "/api/console/runtime-state": _handle_runtime_state,
        "/api/console/runtime-history": _handle_runtime_history,
        "/api/console/data-overview": _handle_data_overview,
        "/api/console/training-runs": _handle_training_runs,
        "/api/console/bundles": _handle_bundles,
        "/api/console/backtests": _handle_backtests,
        "/api/console/backtests/stake-sweep": _handle_backtest_stake_sweep,
        "/api/console/experiments": _handle_experiments,
        "/api/console/experiments/matrix": _handle_experiment_matrix,
        "/api/console/actions": _handle_actions,
        "/api/console/tasks": _handle_tasks,
    }


def dispatch_console_section_route(
    path: str,
    query: JsonMapping,
    *,
    section_handlers: Mapping[str, SectionHandler] | None = None,
) -> tuple[ConsoleSectionRoute, dict[str, object]] | None:
    normalized_path = _normalize_path(path)
    handlers = dict(build_default_section_handlers())
    handlers.update({str(key): value for key, value in (section_handlers or {}).items()})
    handler = handlers.get(normalized_path)
    if handler is None:
        return None
    route = ConsoleSectionRoute(
        path=normalized_path,
        section_id=_section_id_for_path(normalized_path),
        handler=handler,
    )
    payload = _as_http_payload(
        result=route.handler(query),
        section_id=route.section_id,
        path=route.path,
        query=query,
    )
    return route, payload


def _handle_home(query: JsonMapping) -> dict[str, object]:
    return console_service.load_console_home(root=_optional_path(query.get("root")))


def _handle_runtime_state(query: JsonMapping) -> dict[str, object]:
    return console_service.load_console_runtime_state(root=_optional_path(query.get("root")))


def _handle_runtime_history(query: JsonMapping) -> dict[str, object]:
    return console_service.load_console_runtime_history(root=_optional_path(query.get("root")))


def _handle_data_overview(query: JsonMapping) -> dict[str, object]:
    return console_service.load_console_data_overview(
        market=_string_value(query.get("market")) or "sol",
        cycle=_string_value(query.get("cycle")) or "15m",
        surface=_string_value(query.get("surface")) or "backtest",
        root=_optional_path(query.get("root")),
    )


def _handle_training_runs(query: JsonMapping) -> object:
    common = {
        "market": _string_value(query.get("market")) or "sol",
        "cycle": _string_value(query.get("cycle")) or "15m",
        "root": _optional_path(query.get("root")),
    }
    if _string_value(query.get("run_label")) or _string_value(query.get("run_dir")):
        return console_service.load_console_training_run(
            **common,
            model_family=_string_value(query.get("model_family")),
            target=_string_value(query.get("target")),
            run_label=_string_value(query.get("run_label")),
            run_dir=_string_value(query.get("run_dir")),
        )
    return console_service.list_console_training_runs(
        **common,
        model_family=_string_value(query.get("model_family")),
        target=_string_value(query.get("target")),
        prefix=_string_value(query.get("prefix")),
    )


def _handle_bundles(query: JsonMapping) -> object:
    common = {
        "market": _string_value(query.get("market")) or "sol",
        "cycle": _string_value(query.get("cycle")) or "15m",
        "root": _optional_path(query.get("root")),
    }
    if _string_value(query.get("bundle_label")) or _string_value(query.get("bundle_dir")):
        return console_service.load_console_bundle(
            **common,
            profile=_string_value(query.get("profile")) or "deep_otm",
            target=_string_value(query.get("target")) or "direction",
            bundle_label=_string_value(query.get("bundle_label")),
            bundle_dir=_string_value(query.get("bundle_dir")),
        )
    return console_service.list_console_bundles(
        **common,
        profile=_string_value(query.get("profile")),
        target=_string_value(query.get("target")),
        prefix=_string_value(query.get("prefix")),
    )


def _handle_backtests(query: JsonMapping) -> object:
    common = {
        "market": _string_value(query.get("market")) or "sol",
        "cycle": _string_value(query.get("cycle")) or "15m",
        "root": _optional_path(query.get("root")),
    }
    if _string_value(query.get("run_label")):
        return console_service.load_console_backtest(
            **common,
            profile=_string_value(query.get("profile")) or "deep_otm",
            spec_name=_string_value(query.get("spec")) or "baseline_truth",
            run_label=_required_string(query, "run_label"),
        )
    return console_service.list_console_backtests(
        **common,
        profile=_string_value(query.get("profile")),
        spec_name=_string_value(query.get("spec")),
        prefix=_string_value(query.get("prefix")),
    )


def _handle_backtest_stake_sweep(query: JsonMapping) -> object:
    return console_service.load_console_backtest_stake_sweep(
        market=_string_value(query.get("market")) or "sol",
        cycle=_string_value(query.get("cycle")) or "15m",
        profile=_string_value(query.get("profile")) or "deep_otm",
        spec_name=_string_value(query.get("spec")) or "baseline_truth",
        run_label=_required_string(query, "run_label"),
        root=_optional_path(query.get("root")),
    )


def _handle_experiments(query: JsonMapping) -> object:
    root = _optional_path(query.get("root"))
    if _string_value(query.get("run_label")):
        return console_service.load_console_experiment(
            suite_name=_required_string(query, "suite"),
            run_label=_required_string(query, "run_label"),
            root=root,
        )
    return console_service.list_console_experiments(
        suite_name=_string_value(query.get("suite")),
        prefix=_string_value(query.get("prefix")),
        root=root,
    )


def _handle_experiment_matrix(query: JsonMapping) -> object:
    return console_service.load_console_experiment_matrix(
        suite_name=_required_string(query, "suite"),
        run_label=_required_string(query, "run_label"),
        root=_optional_path(query.get("root")),
    )


def _handle_actions(query: JsonMapping) -> object:
    if _string_value(query.get("action_id")):
        request = {
            key: value
            for key, value in query.items()
            if key not in {"action_id", "for_section", "shell_enabled"}
        }
        return console_service.build_console_action_request(
            action_id=_required_string(query, "action_id"),
            request=request,
        )
    return console_service.load_console_action_catalog(
        for_section=_string_value(query.get("for_section")),
        shell_enabled=_optional_bool(query.get("shell_enabled")),
    )


def _handle_tasks(query: JsonMapping) -> object:
    if _string_value(query.get("task_id")):
        return console_service.load_console_task(
            task_id=_required_string(query, "task_id"),
            root=_optional_path(query.get("root")),
        )
    return console_service.list_console_tasks(
        action_id=_string_value(query.get("action_id")),
        action_ids=_optional_csv_strings(query.get("action_ids")),
        status=_string_value(query.get("status")),
        status_group=_string_value(query.get("status_group")),
        marker=_string_value(query.get("marker")),
        group_by=_string_value(query.get("group_by")),
        limit=_optional_int(query.get("limit"), default=20),
        root=_optional_path(query.get("root")),
    )


def _as_http_payload(
    *,
    result: object,
    section_id: str,
    path: str,
    query: JsonMapping,
) -> dict[str, object]:
    if isinstance(result, Mapping):
        payload = {str(key): value for key, value in result.items()}
        payload.setdefault("section", section_id)
        payload.setdefault("request_path", path)
        return payload
    rows = list(result if isinstance(result, list) else list(result))
    return {
        "domain": "console_http",
        "dataset": "console_section_rows",
        "section": section_id,
        "request_path": path,
        "query": dict(query),
        "rows": rows,
        "row_count": len(rows),
    }


def _section_id_for_path(path: str) -> str:
    normalized = _normalize_path(path)
    if normalized == "/api/console/backtests/stake-sweep":
        return "backtest_stake_sweep"
    if normalized == "/api/console/experiments/matrix":
        return "experiment_matrix"
    return normalized.rstrip("/").split("/")[-1].replace("-", "_")


def _normalize_path(path: str) -> str:
    token = str(path or "/").strip()
    if not token:
        return "/"
    if token != "/" and token.endswith("/"):
        token = token[:-1]
    return token


def _string_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_string(query: JsonMapping, key: str) -> str:
    value = _string_value(query.get(key))
    if value is None:
        raise ValueError(f"Missing required query parameter: {key}")
    return value


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
    raise ValueError(f"Invalid boolean token: {value!r}")


def _optional_int(value: object, *, default: int) -> int:
    text = _string_value(value)
    if text is None:
        return int(default)
    return int(text)


def _optional_csv_strings(value: object) -> tuple[str, ...]:
    text = _string_value(value)
    if text is None:
        return ()
    return tuple(token.strip() for token in text.split(",") if token.strip())
