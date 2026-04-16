from __future__ import annotations

import csv
import json
from datetime import datetime
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess

from .queue_state import experiment_queue_path, load_experiment_queue

_AUTORESEARCH_DIRNAME = "auto_research"
_LEGACY_AUTORESEARCH_DIR = Path("scripts") / "research"
_AUTORESEARCH_PROGRAM_NAME = "program.md"
_AUTORESEARCH_README_NAME = "README.md"
_PROJECT_AGENTS_CANDIDATES = (
    Path("research") / "AGENTS.md",
    Path("AGENTS.md"),
)

_CODEX_HOME_COPY_FILES = (
    "auth.json",
    "config.toml",
    "AGENTS.md",
    "version.json",
)

_CODEX_PROVIDER_SECTION_RE = re.compile(
    r"(?ms)^\[model_providers\.codex\]\n.*?(?=^\[|\Z)",
)
_MODEL_PROVIDER_LINE_RE = re.compile(r'(?m)^model_provider\s*=\s*"[^"]+"\s*$')
_ACTIVE_SESSION_LINE_RE = re.compile(
    r"(?im)^\s*-\s*(?:active session|new active session)\s*:\s*`?([^`\n]+/(?:session\.md|results\.tsv))`?\s*$"
)
_SESSION_REF_RE = re.compile(r"(sessions/[^\s`]+/(?:session\.md|results\.tsv))")
_PROGRAM_MARKETS_RE = re.compile(r"(?im)^\s*-\s*coins\s*:\s*(.+)$")
_PROGRAM_TARGET_RE = re.compile(r"(?im)^\s*-\s*target(?:\s+fixed\s+to)?\s*[: ]\s*`?(direction|reversal)`?\s*$")
_MARKET_TOKEN_RE = re.compile(r"(?<![a-z0-9])(btc|eth|sol|xrp)(?![a-z0-9])")
_PROGRAM_WIDTH_LADDER_RE = re.compile(r"(?im)^\s*-\s*allowed width ladder\s*:\s*`?([^`\n]+)`?\s*$")
_FOCUS_FEATURE_REF_RE = re.compile(r"\b(focus_[a-z0-9]+_[0-9]+_[a-z0-9_]+)\b")
_TRANSIENT_PROVIDER_FAILURE_MARKERS = (
    "503 service unavailable",
    "429 too many requests",
    "exceeded retry limit",
    "service temporarily unavailable",
    "error sending request for url",
    "stream disconnected before completion",
    "unexpected status 503",
    "error decoding response body",
    "failed to connect to websocket",
    "http error: 500 internal server error",
)
_DENSE_TRACK_TARGETS = {
    "direction_dense": "direction",
    "reversal_dense": "reversal",
}

# Diagnosis-guided family policy distilled from
# docs/DEEP_OTM_BASELINE_UP_DIAGNOSIS.md for bounded factor replacement.
_DIAGNOSIS_PROTECT_CORE = (
    "q_bs_up_strike",
    "ret_from_strike",
    "basis_bp",
    "ret_from_cycle_open",
    "first_half_ret",
    "cycle_range_pos",
    "rv_30",
    "macd_z",
    "volume_z",
    "obv_z",
    "vwap_gap_60",
    "bias_60",
    "regime_high_vol",
)
_DIAGNOSIS_REDUNDANT_FAMILIES = (
    (
        "short_mid_returns",
        (
            "ret_1m",
            "ret_3m",
            "ret_5m",
            "ret_15m",
            "ret_30m",
            "ret_60m",
            "z_ret_30m",
            "z_ret_60m",
            "ret_1m_lag1",
            "ret_1m_lag2",
            "ret_5m_lag1",
            "ret_15m_lag1",
        ),
    ),
    (
        "price_position",
        (
            "ma_gap_5",
            "ma_gap_15",
            "ema_gap_12",
            "ma_15_slope",
            "bb_pos_20",
            "median_gap_20",
            "price_pos_iqr_20",
            "donch_pos_20",
            "vwap_gap_20",
            "vwap_gap_60",
            "bias_60",
        ),
    ),
    (
        "volatility",
        (
            "rv_30",
            "rv_30_lag1",
            "atr_14",
            "gk_vol_30",
            "rs_vol_30",
            "rr_30",
        ),
    ),
    (
        "momentum_oscillator",
        (
            "macd_hist",
            "rsi_14",
            "rsi_14_lag1",
            "delta_rsi",
            "delta_rsi_5",
            "macd_z",
            "macd_extreme",
            "rsi_divergence",
            "momentum_agree",
        ),
    ),
    (
        "flow",
        (
            "taker_buy_ratio",
            "taker_buy_ratio_z",
            "taker_buy_ratio_lag1",
            "trade_intensity",
            "volume_z",
            "volume_z_3",
            "obv_z",
            "vol_price_corr_15",
            "vol_ratio_5_60",
        ),
    ),
    (
        "calendar",
        (
            "hour_sin",
            "hour_cos",
            "dow_sin",
            "dow_cos",
        ),
    ),
)
_DIAGNOSIS_DEFAULT_DROP_ORDER = (
    "short_mid_returns",
    "price_position",
    "momentum_oscillator",
)
_DIAGNOSIS_ADD_THEMES = (
    "timing",
    "persistence",
    "strike_distance",
    "flip_feasibility",
    "market_quality",
    "junk_cheap_filter",
)
_DIAGNOSIS_PROTECT_CORE_SET = set(_DIAGNOSIS_PROTECT_CORE)
_DIAGNOSIS_REDUNDANT_FAMILY_MEMBERS = {
    label: tuple(columns)
    for label, columns in _DIAGNOSIS_REDUNDANT_FAMILIES
}
_DIAGNOSIS_REDUNDANT_MEMBER_SET = {
    column
    for columns in _DIAGNOSIS_REDUNDANT_FAMILY_MEMBERS.values()
    for column in columns
}


def _autoresearch_dir(project_root: Path) -> Path:
    return Path(project_root).resolve() / _AUTORESEARCH_DIRNAME


def _legacy_autoresearch_dir(project_root: Path) -> Path:
    return Path(project_root).resolve() / _LEGACY_AUTORESEARCH_DIR


def _resolve_repo_relative_path(project_root: Path, path: Path | str) -> Path:
    root = Path(project_root).resolve()
    raw = Path(path)
    return raw if raw.is_absolute() else (root / raw)


def resolve_autoresearch_program_path(
    project_root: Path,
    program_path: Path | str | None = None,
) -> Path:
    root = Path(project_root).resolve()
    if program_path is not None and str(program_path).strip():
        return _resolve_repo_relative_path(root, program_path)
    preferred = _autoresearch_dir(root) / _AUTORESEARCH_PROGRAM_NAME
    legacy = root / _AUTORESEARCH_PROGRAM_NAME
    if preferred.exists() or not legacy.exists():
        return preferred
    return legacy


def resolve_autoresearch_script_path(project_root: Path, script_name: str) -> Path:
    root = Path(project_root).resolve()
    preferred = _autoresearch_dir(root) / str(script_name)
    legacy = _legacy_autoresearch_dir(root) / str(script_name)
    if preferred.exists() or not legacy.exists():
        return preferred
    return legacy


def resolve_autoresearch_readme_path(project_root: Path) -> Path:
    root = Path(project_root).resolve()
    preferred = _autoresearch_dir(root) / _AUTORESEARCH_README_NAME
    legacy = _legacy_autoresearch_dir(root) / _AUTORESEARCH_README_NAME
    if preferred.exists() or not legacy.exists():
        return preferred
    return legacy


def resolve_project_agents_path(project_root: Path) -> Path | None:
    root = Path(project_root).resolve()
    for relative_path in _PROJECT_AGENTS_CANDIDATES:
        candidate = root / relative_path
        if candidate.exists():
            return candidate
    return None


def _repo_display_path(project_root: Path, path: Path | str) -> str:
    root = Path(project_root).resolve()
    resolved = _resolve_repo_relative_path(root, path).resolve()
    try:
        return str(resolved.relative_to(root))
    except ValueError:
        return str(resolved)


def summarize_experiment_run(run_dir: Path) -> dict[str, object]:
    path = Path(run_dir)
    summary_path = path / "summary.json"
    if summary_path.exists():
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        leaderboard_path = path / "leaderboard.csv"
        return {
            "suite_name": payload.get("suite_name"),
            "run_label": payload.get("run_label"),
            "cases": _optional_int(payload.get("cases")),
            "completed_cases": _optional_int(payload.get("completed_cases")),
            "failed_cases": _optional_int(payload.get("failed_cases")),
            "leaderboard_rows": _optional_int(payload.get("leaderboard_rows")),
            "top_roi_pct": _optional_float(payload.get("top_roi_pct")),
            "markets": list(payload.get("markets") or []),
            "summary_path": str(summary_path),
            "report_path": str(path / "report.md"),
            "leaderboard_path": str(leaderboard_path) if leaderboard_path.exists() else None,
            "top_case": _read_formal_top_case(leaderboard_path),
            "raw_summary": payload,
        }

    quick_summary_path = path / "quick_screen_summary.json"
    if quick_summary_path.exists():
        payload = json.loads(quick_summary_path.read_text(encoding="utf-8"))
        leaderboard_path = path / "quick_screen_leaderboard.csv"
        rows = _optional_int(payload.get("rows"))
        return {
            "suite_name": payload.get("suite_name"),
            "run_label": payload.get("run_label"),
            "cases": rows,
            "completed_cases": rows,
            "failed_cases": 0,
            "leaderboard_rows": rows,
            "top_roi_pct": None,
            "markets": list(payload.get("markets") or []),
            "summary_path": str(quick_summary_path),
            "report_path": str(path / "quick_screen_report.md"),
            "leaderboard_path": str(leaderboard_path) if leaderboard_path.exists() else None,
            "top_case": _read_quick_screen_top_case(leaderboard_path),
            "raw_summary": payload,
        }

    log_path = path / "logs" / "suite.jsonl"
    if log_path.exists():
        return _summarize_incomplete_formal_run(path)

    raise FileNotFoundError(f"Missing experiment summary: {summary_path}")


def _summary_progress_payload(summary_payload: dict[str, object]) -> tuple[int | None, int, int]:
    cases = _optional_int(summary_payload.get("cases"))
    completed_cases = _optional_int(summary_payload.get("completed_cases")) or 0
    failed_cases = _optional_int(summary_payload.get("failed_cases")) or 0
    return cases, int(completed_cases), int(failed_cases)


def _summary_is_terminal(summary_payload: dict[str, object]) -> bool:
    cases, completed_cases, failed_cases = _summary_progress_payload(summary_payload)
    if cases is None or int(cases) <= 0:
        return True
    return int(completed_cases) + int(failed_cases) >= int(cases)


def prepare_codex_home(home_root: Path, *, source_home: Path | None = None) -> dict[str, object]:
    target_home = Path(home_root).resolve()
    source_root = Path.home() if source_home is None else Path(source_home).resolve()
    source_codex_dir = source_root / ".codex"
    target_codex_dir = target_home / ".codex"
    target_codex_dir.mkdir(parents=True, exist_ok=True)

    copied_files: list[str] = []
    for filename in _CODEX_HOME_COPY_FILES:
        source_path = source_codex_dir / filename
        if not source_path.exists():
            continue
        shutil.copy2(source_path, target_codex_dir / filename)
        copied_files.append(filename)

    # Force sessions into the isolated home so background runs do not depend on
    # access to the operator's primary Codex session store.
    (target_codex_dir / "sessions").mkdir(parents=True, exist_ok=True)

    return {
        "home_root": str(target_home),
        "codex_dir": str(target_codex_dir),
        "copied_files": list(copied_files),
    }


def apply_codex_provider_override(
    home_root: Path,
    *,
    base_url: str,
    api_key: str,
) -> dict[str, object]:
    target_home = Path(home_root).resolve()
    target_codex_dir = target_home / ".codex"
    target_codex_dir.mkdir(parents=True, exist_ok=True)

    normalized_base_url = str(base_url or "").strip().rstrip("/")
    normalized_api_key = str(api_key or "").strip()
    if not normalized_base_url:
        raise ValueError("base_url must not be empty")
    if not normalized_api_key:
        raise ValueError("api_key must not be empty")

    config_path = target_codex_dir / "config.toml"
    config_text = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
    provider_block = "\n".join(
        [
            "[model_providers.codex]",
            'name = "codex"',
            f"base_url = {json.dumps(normalized_base_url)}",
            'wire_api = "responses"',
            "requires_openai_auth = false",
            "",
        ]
    )
    if _CODEX_PROVIDER_SECTION_RE.search(config_text):
        updated_config = _CODEX_PROVIDER_SECTION_RE.sub(provider_block, config_text)
    else:
        separator = "\n" if config_text and not config_text.endswith("\n") else ""
        updated_config = f"{config_text}{separator}{provider_block}"
    config_path.write_text(updated_config, encoding="utf-8")

    auth_path = target_codex_dir / "auth.json"
    payload: dict[str, object] = {}
    if auth_path.exists():
        payload = json.loads(auth_path.read_text(encoding="utf-8"))
    payload["OPENAI_API_KEY"] = normalized_api_key
    auth_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    return {
        "home_root": str(target_home),
        "codex_dir": str(target_codex_dir),
        "config_path": str(config_path),
        "auth_path": str(auth_path),
        "base_url": normalized_base_url,
    }


def apply_codex_auth_override(
    home_root: Path,
    *,
    auth_payload: dict[str, object],
) -> dict[str, object]:
    target_home = Path(home_root).resolve()
    target_codex_dir = target_home / ".codex"
    target_codex_dir.mkdir(parents=True, exist_ok=True)

    config_path = target_codex_dir / "config.toml"
    config_text = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
    updated_config = _CODEX_PROVIDER_SECTION_RE.sub("", config_text).rstrip()
    if _MODEL_PROVIDER_LINE_RE.search(updated_config):
        updated_config = _MODEL_PROVIDER_LINE_RE.sub('model_provider = "openai"', updated_config)
    elif updated_config:
        updated_config = f'{updated_config}\nmodel_provider = "openai"'
    else:
        updated_config = 'model_provider = "openai"'
    if updated_config:
        updated_config = f"{updated_config}\n"
    config_path.write_text(updated_config, encoding="utf-8")

    auth_path = target_codex_dir / "auth.json"
    auth_path.write_text(
        json.dumps(dict(auth_payload), indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    return {
        "home_root": str(target_home),
        "codex_dir": str(target_codex_dir),
        "config_path": str(config_path),
        "auth_path": str(auth_path),
    }


def is_transient_codex_provider_failure(output: str, *, base_url: str | None = None) -> bool:
    text = str(output or "").lower()
    if not text:
        return False
    return any(marker in text for marker in _TRANSIENT_PROVIDER_FAILURE_MARKERS)


def next_autorun_failure_state(
    *,
    previous_failures: int,
    exit_code: int,
    max_consecutive_failures: int,
) -> dict[str, object]:
    if int(exit_code) == 0:
        return {"failure_count": 0, "should_stop": False}
    failure_count = max(0, int(previous_failures)) + 1
    threshold = max(1, int(max_consecutive_failures))
    return {
        "failure_count": failure_count,
        "should_stop": failure_count >= threshold,
    }


def inspect_experiment_run(run_dir: Path) -> dict[str, object]:
    path = Path(run_dir)
    summary_path = path / "summary.json"
    log_path = path / "logs" / "suite.jsonl"
    events = _read_jsonl(log_path)
    summary_payload: dict[str, object] = {}
    if summary_path.exists():
        try:
            summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            summary_payload = {}
    completed_cases = sum(1 for item in events if item.get("event") == "market_completed")
    failed_cases = sum(1 for item in events if item.get("event") == "market_failed")
    cases = None
    if summary_payload:
        cases, summary_completed_cases, summary_failed_cases = _summary_progress_payload(summary_payload)
        completed_cases = max(completed_cases, int(summary_completed_cases))
        failed_cases = max(failed_cases, int(summary_failed_cases))
    last_event_payload = events[-1] if events else {}
    last_event = str(last_event_payload.get("event") or "") or None
    if summary_payload and _summary_is_terminal(summary_payload):
        state = "completed"
    elif completed_cases == 0 and failed_cases == 0 and last_event in {
        "execution_group_seed_case_started",
        "execution_group_warmup_completed",
        "market_cache_resolved",
    }:
        state = "stuck_seed_case"
    elif completed_cases > 0 or failed_cases > 0:
        state = "checkpointed"
    else:
        state = "incomplete"
    return {
        "run_dir": str(path),
        "suite_name": _suite_name_from_run_dir(path),
        "run_label": _run_label_from_run_dir(path),
        "summary_exists": summary_path.exists(),
        "cases": cases,
        "log_path": str(log_path) if log_path.exists() else None,
        "last_event": last_event,
        "last_case_label": (
            last_event_payload.get("case_label")
            or last_event_payload.get("run_name")
            or last_event_payload.get("group_label")
        ),
        "completed_cases": completed_cases,
        "failed_cases": failed_cases,
        "state": state,
    }


def find_incomplete_experiment_runs(project_root: Path, *, limit: int = 10) -> list[dict[str, object]]:
    root = Path(project_root).resolve()
    runs_root = root / "research" / "experiments" / "runs"
    if not runs_root.exists():
        return []
    latest_completed_by_suite: dict[str, float] = {}
    for path in runs_root.glob("suite=*/run=*"):
        if not path.is_dir():
            continue
        summary_path = path / "summary.json"
        if not summary_path.exists():
            continue
        try:
            summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not _summary_is_terminal(summary_payload):
            continue
        suite_name = _suite_name_from_run_dir(path)
        if suite_name is None:
            continue
        latest_completed_by_suite[suite_name] = max(
            latest_completed_by_suite.get(suite_name, 0.0),
            summary_path.stat().st_mtime,
        )
    run_dirs = sorted(
        (
            path
            for path in runs_root.glob("suite=*/run=*")
            if path.is_dir()
            and (path / "logs" / "suite.jsonl").exists()
            and (
                _suite_name_from_run_dir(path) is None
                or (path / "logs" / "suite.jsonl").stat().st_mtime
                >= latest_completed_by_suite.get(_suite_name_from_run_dir(path) or "", 0.0)
            )
            and (
                not (path / "summary.json").exists()
                or not _summary_is_terminal(json.loads((path / "summary.json").read_text(encoding="utf-8")))
            )
        ),
        key=lambda item: (item / "logs" / "suite.jsonl").stat().st_mtime,
        reverse=True,
    )
    return [inspect_experiment_run(path) for path in run_dirs[: max(1, int(limit))]]


def find_recent_completed_experiment_runs(project_root: Path, *, limit: int = 10) -> list[dict[str, object]]:
    root = Path(project_root).resolve()
    runs_root = root / "research" / "experiments" / "runs"
    if not runs_root.exists():
        return []
    run_dirs = sorted(
        (
            path
            for path in runs_root.glob("suite=*/run=*")
            if path.is_dir()
            and (path / "summary.json").exists()
            and _summary_is_terminal(json.loads((path / "summary.json").read_text(encoding="utf-8")))
        ),
        key=lambda item: (item / "summary.json").stat().st_mtime,
        reverse=True,
    )
    payloads: list[dict[str, object]] = []
    for path in run_dirs[: max(1, int(limit))]:
        summary = summarize_experiment_run(path)
        payloads.append(
            {
                **summary,
                "run_dir": str(path),
                "state": "completed",
            }
        )
    return payloads


def find_latest_completed_experiment_runs_by_market(
    project_root: Path,
    *,
    markets: list[str],
    context: dict[str, str | None] | None = None,
    queue_items: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    root = Path(project_root).resolve()
    runs_root = root / "research" / "experiments" / "runs"
    wanted_markets: list[str] = []
    seen_markets: set[str] = set()
    for raw_market in markets:
        market = str(raw_market or "").strip().lower()
        if not market or market in seen_markets:
            continue
        seen_markets.add(market)
        wanted_markets.append(market)
    if not wanted_markets or not runs_root.exists():
        return []

    active_context = dict(context or {})
    known_queue_items = [
        dict(item)
        for item in (queue_items or [])
        if isinstance(item, dict)
    ]
    suite_target_cache: dict[str, list[str]] = {}
    latest_by_market: dict[str, dict[str, object]] = {}
    run_dirs = sorted(
        (
            path
            for path in runs_root.glob("suite=*/run=*")
            if path.is_dir()
            and (path / "summary.json").exists()
            and _summary_is_terminal(json.loads((path / "summary.json").read_text(encoding="utf-8")))
        ),
        key=lambda item: (item / "summary.json").stat().st_mtime,
        reverse=True,
    )
    wanted_market_set = set(wanted_markets)
    for path in run_dirs:
        summary = summarize_experiment_run(path)
        payload = {
            **summary,
            "run_dir": str(path),
            "state": "completed",
        }
        if active_context and not _payload_matches_dense_context(
            root,
            payload,
            active_context,
            queue_items=known_queue_items,
            suite_target_cache=suite_target_cache,
        ):
            continue
        suite_summary = _summarize_suite_variants(root, str(payload.get("suite_name") or ""))
        payload_markets = (
            list(suite_summary.get("markets") or [])
            if isinstance(suite_summary, dict)
            else _infer_markets_from_run_payload(payload)
        )
        for market in payload_markets:
            if market not in wanted_market_set or market in latest_by_market:
                continue
            latest_by_market[market] = dict(payload)
        if len(latest_by_market) >= len(wanted_markets):
            break
    return [dict(latest_by_market[market]) for market in wanted_markets if market in latest_by_market]


def build_autorun_status_report(
    project_root: Path,
    *,
    status_path: Path | None = None,
    log_tail_lines: int = 20,
    max_incomplete_runs: int = 10,
) -> dict[str, object]:
    root = Path(project_root).resolve()
    resolved_status_path = status_path or (root / "var" / "research" / "autorun" / "codex-background.status.json")
    log_path = resolved_status_path.parent / "codex-background.log"
    status_payload: dict[str, object] = {}
    if resolved_status_path.exists():
        status_payload = json.loads(resolved_status_path.read_text(encoding="utf-8"))
        status_payload.setdefault("failure_count", 0)
        if str(status_payload.get("state") or "") == "running" and not _pid_is_live(status_payload.get("pid")):
            status_payload["state"] = "stale"
            status_payload["state_reason"] = "missing_pid"
    queue_payload = load_experiment_queue(root)
    dense_context = _resolve_dense_context(
        status_path=resolved_status_path,
        status_payload=status_payload,
    )
    all_queue_items = [
        dict(item)
        for item in queue_payload.get("items") or []
        if isinstance(item, dict)
    ]
    filtered_queue_items = _filter_payloads_for_dense_context(
        root,
        all_queue_items,
        context=dense_context,
        queue_items=all_queue_items,
    )
    filtered_formal_workers = _filter_payloads_for_dense_context(
        root,
        list(find_live_formal_workers(root)),
        context=dense_context,
        queue_items=all_queue_items,
    )
    filtered_incomplete_runs = _filter_payloads_for_dense_context(
        root,
        find_incomplete_experiment_runs(root, limit=max_incomplete_runs),
        context=dense_context,
        queue_items=all_queue_items,
    )
    filtered_completed_runs = _filter_payloads_for_dense_context(
        root,
        find_recent_completed_experiment_runs(root, limit=max_incomplete_runs),
        context=dense_context,
        queue_items=all_queue_items,
    )
    return {
        "status_path": str(resolved_status_path),
        "log_path": str(log_path),
        "status": status_payload,
        "log_tail": _tail_lines(log_path, limit=max(0, int(log_tail_lines))),
        "queue": {
            "queue_path": str(experiment_queue_path(root)),
            "max_live_runs": int(queue_payload.get("max_live_runs") or 3),
            "track_slot_caps": dict(queue_payload.get("track_slot_caps") or {}),
            "items": filtered_queue_items,
        },
        "formal_workers": filtered_formal_workers,
        "incomplete_runs": filtered_incomplete_runs,
        "completed_runs": filtered_completed_runs,
    }


def resolve_autorun_session_dir(
    project_root: Path,
    *,
    explicit_session_dir: Path | str | None = None,
    program_path: Path | str | None = None,
) -> Path:
    if explicit_session_dir is not None and str(explicit_session_dir).strip():
        return Path(explicit_session_dir).expanduser().resolve()

    root = Path(project_root).resolve()
    program = resolve_autoresearch_program_path(root, program_path)
    if program.exists():
        text = program.read_text(encoding="utf-8")
        match = _ACTIVE_SESSION_LINE_RE.search(text) or _SESSION_REF_RE.search(text)
        if match:
            session_path = Path(match.group(1))
            if not session_path.is_absolute():
                session_path = root / session_path
            return session_path.parent.resolve()

    return (root / "sessions" / "autoresearch").resolve()


def resolve_latest_cycle_eval_file(session_dir: Path) -> Path | None:
    session = Path(session_dir).resolve()
    cycles_dir = session / "cycles"
    if not cycles_dir.exists():
        return None
    candidates = [path for path in cycles_dir.glob("*/eval-results.md") if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.stat().st_mtime)


def resolve_program_markets(program_path: Path) -> list[str]:
    program = Path(program_path).resolve()
    if not program.exists():
        return []
    match = _PROGRAM_MARKETS_RE.search(program.read_text(encoding="utf-8"))
    if not match:
        return []
    seen: set[str] = set()
    markets: list[str] = []
    for token in _MARKET_TOKEN_RE.findall(match.group(1).lower()):
        if token in seen:
            continue
        seen.add(token)
        markets.append(token)
    return markets


def _normalize_dense_track(value: object) -> str | None:
    token = str(value or "").strip().lower()
    return token if token in _DENSE_TRACK_TARGETS else None


def _normalize_target(value: object) -> str | None:
    token = str(value or "").strip().lower()
    return token if token in {"direction", "reversal"} else None


def _infer_dense_track_from_values(*values: object) -> str | None:
    text = " ".join(
        str(value).strip().lower()
        for value in values
        if value is not None and str(value).strip()
    )
    if not text:
        return None
    for track in _DENSE_TRACK_TARGETS:
        if track in text:
            return track
    return None


def _infer_target_from_values(*values: object) -> str | None:
    matches: set[str] = set()
    for value in values:
        text = str(value or "").strip().lower()
        if not text:
            continue
        if "direction_dense" in text or re.search(r"(?<![a-z0-9])direction(?![a-z0-9])", text):
            matches.add("direction")
        if "reversal_dense" in text or re.search(r"(?<![a-z0-9])reversal(?![a-z0-9])", text):
            matches.add("reversal")
    if len(matches) == 1:
        return next(iter(matches))
    return None


def _infer_program_target(program_path: Path | None) -> str | None:
    if program_path is None:
        return None
    program = Path(program_path).resolve()
    if not program.exists():
        return _infer_target_from_values(program)
    text = program.read_text(encoding="utf-8")
    match = _PROGRAM_TARGET_RE.search(text)
    if match:
        return _normalize_target(match.group(1))
    return _infer_target_from_values(program, text)


def _resolve_dense_context(
    *,
    status_path: Path | None = None,
    status_payload: dict[str, object] | None = None,
    session_dir: Path | str | None = None,
    program_path: Path | str | None = None,
) -> dict[str, str | None]:
    payload = dict(status_payload or {})
    track = _infer_dense_track_from_values(
        status_path,
        session_dir,
        program_path,
        payload.get("session_dir"),
        payload.get("program_path"),
    )
    target = _DENSE_TRACK_TARGETS.get(track)
    if target is None:
        resolved_program = Path(program_path).resolve() if program_path is not None and str(program_path).strip() else None
        target = _infer_program_target(resolved_program)
    if target is None:
        target = _infer_target_from_values(
            status_path,
            session_dir,
            program_path,
            payload.get("session_dir"),
            payload.get("program_path"),
        )
    if track is None and target is not None:
        dense_track = f"{target}_dense"
        if dense_track in _DENSE_TRACK_TARGETS:
            track = dense_track
    return {
        "track": track,
        "target": target,
    }


def _suite_targets_for_name(
    project_root: Path,
    suite_name: str | None,
    *,
    cache: dict[str, list[str]],
) -> list[str]:
    normalized_suite = str(suite_name or "").strip()
    if not normalized_suite:
        return []
    cached = cache.get(normalized_suite)
    if cached is not None:
        return list(cached)
    suite_summary = _summarize_suite_variants(project_root, normalized_suite)
    targets = [
        target
        for target in (_normalize_target(item) for item in (suite_summary or {}).get("targets") or [])
        if target is not None
    ]
    cache[normalized_suite] = list(targets)
    return list(targets)


def _infer_payload_track_from_queue_items(
    payload: dict[str, object],
    queue_items: list[dict[str, object]],
) -> str | None:
    suite_name = str(payload.get("suite_name") or "").strip()
    run_label = str(payload.get("run_label") or "").strip()
    market = str(payload.get("market") or "").strip().lower()
    tracks: set[str] = set()
    for item in queue_items:
        if not isinstance(item, dict):
            continue
        if suite_name and str(item.get("suite_name") or "").strip() != suite_name:
            continue
        if run_label and str(item.get("run_label") or "").strip() != run_label:
            continue
        item_market = str(item.get("market") or "").strip().lower()
        if market and item_market and item_market != market:
            continue
        track = _normalize_dense_track(item.get("track"))
        if track is not None:
            tracks.add(track)
    if len(tracks) == 1:
        return next(iter(tracks))
    return None


def _payload_matches_dense_context(
    project_root: Path,
    payload: dict[str, object],
    context: dict[str, str | None],
    *,
    queue_items: list[dict[str, object]],
    suite_target_cache: dict[str, list[str]],
) -> bool:
    current_track = _normalize_dense_track(context.get("track"))
    current_target = _normalize_target(context.get("target"))
    if current_track is None and current_target is None:
        return True

    suite_targets = _suite_targets_for_name(
        project_root,
        str(payload.get("suite_name") or "").strip() or None,
        cache=suite_target_cache,
    )
    if current_target is not None and suite_targets:
        return current_target in suite_targets

    inferred_target = _infer_target_from_values(
        payload.get("suite_name"),
        payload.get("run_label"),
        payload.get("run_dir"),
    )
    if current_target is not None and inferred_target is not None:
        return inferred_target == current_target

    explicit_track = _normalize_dense_track(payload.get("track"))
    if current_track is not None and explicit_track is not None:
        return explicit_track == current_track

    inferred_track = _infer_payload_track_from_queue_items(payload, queue_items)
    if current_track is not None and inferred_track is not None:
        return inferred_track == current_track

    return True


def _filter_payloads_for_dense_context(
    project_root: Path,
    payloads: list[dict[str, object]],
    *,
    context: dict[str, str | None],
    queue_items: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    current_track = _normalize_dense_track(context.get("track"))
    current_target = _normalize_target(context.get("target"))
    if current_track is None and current_target is None:
        return list(payloads)

    known_queue_items = [
        dict(item)
        for item in (queue_items or [])
        if isinstance(item, dict)
    ]
    suite_target_cache: dict[str, list[str]] = {}
    return [
        dict(payload)
        for payload in payloads
        if isinstance(payload, dict)
        and _payload_matches_dense_context(
            project_root,
            payload,
            context,
            queue_items=known_queue_items,
            suite_target_cache=suite_target_cache,
        )
    ]


def _infer_markets_from_run_payload(payload: dict[str, object]) -> list[str]:
    seen: set[str] = set()
    markets: list[str] = []
    for raw in payload.get("markets") or []:
        token = str(raw).strip().lower()
        if token and token not in seen:
            seen.add(token)
            markets.append(token)
    if markets:
        return markets
    for field_name in ("suite_name", "run_label", "run_dir"):
        raw_value = payload.get(field_name)
        if not raw_value:
            continue
        for token in _MARKET_TOKEN_RE.findall(str(raw_value).lower()):
            if token in seen:
                continue
            seen.add(token)
            markets.append(token)
    return markets


def _suite_spec_path(project_root: Path, suite_name: str | None) -> Path | None:
    if not suite_name:
        return None
    path = Path(project_root).resolve() / "research" / "experiments" / "suite_specs" / f"{suite_name}.json"
    return path if path.exists() else None


def _read_suite_spec_by_name(project_root: Path, suite_name: str | None) -> dict[str, object] | None:
    path = _suite_spec_path(project_root, suite_name)
    if path is None:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _summarize_suite_variants(project_root: Path, suite_name: str | None) -> dict[str, object] | None:
    suite_spec = _read_suite_spec_by_name(project_root, suite_name)
    if not suite_spec:
        return None
    feature_sets: list[str] = []
    weight_labels: list[str] = []
    targets: list[str] = []
    seen_feature_sets: set[str] = set()
    seen_weight_labels: set[str] = set()
    seen_targets: set[str] = set()
    markets_payload = suite_spec.get("markets")
    if not isinstance(markets_payload, dict):
        return None
    for market_payload in markets_payload.values():
        if not isinstance(market_payload, dict):
            continue
        groups = market_payload.get("groups")
        if not isinstance(groups, dict):
            continue
        for group_payload in groups.values():
            if not isinstance(group_payload, dict):
                continue
            runs = group_payload.get("runs")
            if not isinstance(runs, list):
                continue
            for run_payload in runs:
                if not isinstance(run_payload, dict):
                    continue
                target = str(run_payload.get("target") or "").strip()
                if target and target not in seen_targets:
                    seen_targets.add(target)
                    targets.append(target)
                feature_variants = run_payload.get("feature_set_variants")
                if isinstance(feature_variants, list):
                    for variant in feature_variants:
                        if not isinstance(variant, dict):
                            continue
                        feature_set = str(variant.get("feature_set") or "").strip()
                        if feature_set and feature_set not in seen_feature_sets:
                            seen_feature_sets.add(feature_set)
                            feature_sets.append(feature_set)
                weight_variants = run_payload.get("weight_variants")
                if isinstance(weight_variants, list):
                    for variant in weight_variants:
                        if not isinstance(variant, dict):
                            continue
                        label = str(variant.get("label") or "").strip()
                        if label and label not in seen_weight_labels:
                            seen_weight_labels.add(label)
                            weight_labels.append(label)
    return {
        "path": str(_suite_spec_path(project_root, suite_name)) if suite_name else None,
        "markets": [str(key) for key in markets_payload.keys()],
        "feature_sets": feature_sets,
        "weight_labels": weight_labels,
        "targets": targets,
        "parallel_case_workers": _optional_int(suite_spec.get("parallel_case_workers")),
    }


def _collect_coin_slot_statuses(
    *,
    project_root: Path,
    markets: list[str],
    incomplete_runs: list[dict[str, object]],
    completed_runs: list[dict[str, object]],
    live_run_labels: set[str],
) -> dict[str, dict[str, object]]:
    incomplete_by_market: dict[str, list[dict[str, object]]] = {market: [] for market in markets}
    completed_by_market: dict[str, list[dict[str, object]]] = {market: [] for market in markets}
    for payload in incomplete_runs:
        suite_summary = _summarize_suite_variants(project_root, str(payload.get("suite_name") or ""))
        payload_markets = list(suite_summary.get("markets") or []) if suite_summary else _infer_markets_from_run_payload(payload)
        for market in payload_markets:
            incomplete_by_market.setdefault(market, []).append(payload)
    for payload in completed_runs:
        suite_summary = _summarize_suite_variants(project_root, str(payload.get("suite_name") or ""))
        payload_markets = list(suite_summary.get("markets") or []) if suite_summary else _infer_markets_from_run_payload(payload)
        for market in payload_markets:
            completed_by_market.setdefault(market, []).append(payload)

    statuses: dict[str, dict[str, object]] = {}
    for market in markets:
        active_payload = next(iter(incomplete_by_market.get(market) or []), None)
        latest_completed = next(iter(completed_by_market.get(market) or []), None)
        if active_payload is not None:
            suite_name = str(active_payload.get("suite_name") or "")
            suite_summary = _summarize_suite_variants(project_root, suite_name)
            run_label = str(active_payload.get("run_label") or "")
            has_live_worker = bool(run_label and run_label in live_run_labels)
            statuses[market] = {
                "slot": "active" if has_live_worker else "checkpointed",
                "suite_name": suite_name or None,
                "run_label": run_label or None,
                "completed_cases": active_payload.get("completed_cases"),
                "cases": active_payload.get("cases"),
                "last_event": (
                    active_payload.get("raw_summary", {}).get("last_event")
                    or active_payload.get("last_event")
                    or "unknown"
                ),
                "live_worker": has_live_worker,
                "suite_summary": suite_summary,
                "top_case": None,
            }
            continue

        if latest_completed is not None:
            suite_name = str(latest_completed.get("suite_name") or "")
            suite_summary = _summarize_suite_variants(project_root, suite_name)
            statuses[market] = {
                "slot": "idle",
                "suite_name": suite_name or None,
                "run_label": latest_completed.get("run_label") or None,
                "completed_cases": latest_completed.get("completed_cases"),
                "cases": latest_completed.get("cases"),
                "last_event": "completed",
                "live_worker": False,
                "suite_summary": suite_summary,
                "top_case": latest_completed.get("top_case") if isinstance(latest_completed.get("top_case"), dict) else None,
            }
            continue

        statuses[market] = {
            "slot": "idle",
            "suite_name": None,
            "run_label": None,
            "completed_cases": None,
            "cases": None,
            "last_event": None,
            "live_worker": False,
            "suite_summary": None,
            "top_case": None,
        }
    return statuses


def _backfill_completed_runs_for_markets(
    *,
    project_root: Path,
    markets: list[str],
    completed_runs: list[dict[str, object]],
    context: dict[str, str | None],
    queue_items: list[dict[str, object]],
) -> list[dict[str, object]]:
    covered_markets: set[str] = set()
    for payload in completed_runs:
        suite_summary = _summarize_suite_variants(project_root, str(payload.get("suite_name") or ""))
        payload_markets = (
            list(suite_summary.get("markets") or [])
            if isinstance(suite_summary, dict)
            else _infer_markets_from_run_payload(payload)
        )
        for market in payload_markets:
            covered_markets.add(str(market).strip().lower())
    missing_markets = [
        market
        for market in markets
        if str(market).strip().lower() and str(market).strip().lower() not in covered_markets
    ]
    if not missing_markets:
        return list(completed_runs)
    fallback_runs = find_latest_completed_experiment_runs_by_market(
        project_root,
        markets=missing_markets,
        context=context,
        queue_items=queue_items,
    )
    if not fallback_runs:
        return list(completed_runs)
    return [*completed_runs, *fallback_runs]


def _format_machine_decision_summary(
    *,
    markets: list[str],
    slot_statuses: dict[str, dict[str, object]],
    allowed_live_runs: int,
    queue_payload: dict[str, object],
) -> list[str]:
    items = [dict(item) for item in queue_payload.get("items") or [] if isinstance(item, dict)]
    queued_count = sum(
        1
        for item in items
        if str(item.get("status") or "").strip().lower() in {"queued", "repair"}
    )
    queued_successor_markets = {
        str(item.get("market") or "").strip().lower()
        for item in items
        if str(item.get("market") or "").strip()
        and str(item.get("status") or "").strip().lower() in {"queued", "repair"}
    }
    live_count = sum(
        1
        for market in markets
        if str((slot_statuses.get(market) or {}).get("slot") or "") == "active"
    )
    actionable_count = sum(
        1
        for market in markets
        if str((slot_statuses.get(market) or {}).get("slot") or "") in {"idle", "checkpointed"}
    )
    successor_needed_count = sum(
        1
        for market in markets
        if str((slot_statuses.get(market) or {}).get("slot") or "") == "active" and market not in queued_successor_markets
    )
    lines = [
        (
            f"- occupancy={live_count}/{allowed_live_runs} / queued={queued_count} / "
            f"refill_required={'yes' if actionable_count > 0 and live_count < allowed_live_runs else 'no'} / "
            f"successors_needed={successor_needed_count}"
        ),
    ]
    for market in markets:
        payload = dict(slot_statuses.get(market) or {})
        slot = str(payload.get("slot") or "idle")
        queued_successor = market in queued_successor_markets
        if slot == "active":
            action = "keep_running" if queued_successor else "prepare_next_now"
        elif slot == "checkpointed":
            action = "resume_or_replace_now"
        else:
            action = "refill_now" if live_count < allowed_live_runs else "wait_for_slot"
        detail_parts = [
            f"{market}: slot={slot}",
            f"action={action}",
        ]
        if slot == "active":
            detail_parts.append(f"queued_successor={'yes' if queued_successor else 'no'}")
        suite_name = str(payload.get("suite_name") or "").strip()
        run_label = str(payload.get("run_label") or "").strip()
        if suite_name:
            detail_parts.append(f"suite={suite_name}")
        if run_label:
            detail_parts.append(f"run={run_label}")
        last_event = str(payload.get("last_event") or "").strip()
        if slot == "checkpointed" and last_event:
            detail_parts.append(f"last_event={last_event}")
        lines.append("- " + " / ".join(detail_parts))
    return lines


def _format_coin_slot_snapshot(
    *,
    project_root: Path,
    markets: list[str],
    incomplete_runs: list[dict[str, object]],
    completed_runs: list[dict[str, object]],
    live_run_labels: set[str],
) -> list[str]:
    slot_statuses = _collect_coin_slot_statuses(
        project_root=project_root,
        markets=markets,
        incomplete_runs=incomplete_runs,
        completed_runs=completed_runs,
        live_run_labels=live_run_labels,
    )
    lines: list[str] = []
    for market in markets:
        payload = dict(slot_statuses.get(market) or {})
        slot = str(payload.get("slot") or "idle")
        suite_name = str(payload.get("suite_name") or "")
        run_label = str(payload.get("run_label") or "")
        suite_summary = payload.get("suite_summary")
        if slot in {"active", "checkpointed"}:
            detail_parts = [
                f"state={slot}",
                f"suite={suite_name or '?'}",
                f"run={run_label or '?'}",
                f"progress={payload.get('completed_cases') or 0}/{payload.get('cases') or '?'}",
                f"last_event={payload.get('last_event') or 'unknown'}",
                "live_worker=yes" if payload.get("live_worker") else "live_worker=no",
            ]
            if isinstance(suite_summary, dict):
                if suite_summary.get("targets"):
                    detail_parts.append(f"targets={','.join(str(item) for item in suite_summary['targets'])}")
                if suite_summary.get("feature_sets"):
                    detail_parts.append(
                        "feature_sets=" + ",".join(str(item) for item in suite_summary["feature_sets"][:4])
                    )
                if suite_summary.get("weight_labels"):
                    detail_parts.append(
                        "weights=" + ",".join(str(item) for item in suite_summary["weight_labels"][:4])
                    )
            lines.append(f"- {market}: " + " / ".join(detail_parts))
            continue

        if suite_name or run_label:
            top_case = payload.get("top_case") or {}
            detail_parts = [
                "state=idle",
                f"latest_completed={suite_name or '?'}",
                f"run={run_label or '?'}",
            ]
            if isinstance(top_case, dict):
                variant_label = str(top_case.get("variant_label") or "").strip()
                if variant_label:
                    detail_parts.append(f"lead_variant={variant_label}")
                if top_case.get("roi_pct") is not None:
                    detail_parts.append(f"roi={top_case['roi_pct']}")
                if top_case.get("trades") is not None:
                    detail_parts.append(f"trades={top_case['trades']}")
            if isinstance(suite_summary, dict):
                if suite_summary.get("feature_sets"):
                    detail_parts.append(
                        "feature_sets=" + ",".join(str(item) for item in suite_summary["feature_sets"][:4])
                    )
                if suite_summary.get("weight_labels"):
                    detail_parts.append(
                        "weights=" + ",".join(str(item) for item in suite_summary["weight_labels"][:4])
                    )
            lines.append(f"- {market}: " + " / ".join(detail_parts))
            continue

        lines.append(f"- {market}: state=idle / no recent formal run summary found")
    return lines


def _extract_referenced_feature_sets(text: str) -> list[str]:
    seen: set[str] = set()
    names: list[str] = []
    for match in _FOCUS_FEATURE_REF_RE.finditer(str(text or "")):
        token = match.group(1).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        names.append(token)
    return names


def _load_custom_feature_registry(project_root: Path) -> dict[str, dict[str, object]]:
    path = Path(project_root).resolve() / "research" / "experiments" / "custom_feature_sets.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, dict[str, object]] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            out[str(key)] = value
    return out


def _collect_relevant_feature_set_names(
    *,
    project_root: Path,
    latest_cycle_eval: Path | None,
    incomplete_runs: list[dict[str, object]],
    completed_runs: list[dict[str, object]],
) -> list[str]:
    seen: set[str] = set()
    names: list[str] = []

    def remember(items: list[str]) -> None:
        for item in items:
            token = str(item).strip()
            if not token or token in seen:
                continue
            seen.add(token)
            names.append(token)

    if latest_cycle_eval is not None and latest_cycle_eval.exists():
        remember(_extract_referenced_feature_sets(latest_cycle_eval.read_text(encoding="utf-8")))

    for payload in [*incomplete_runs, *completed_runs]:
        suite_summary = _summarize_suite_variants(project_root, str(payload.get("suite_name") or ""))
        if not suite_summary:
            continue
        remember(list(suite_summary.get("feature_sets") or []))
        if len(names) >= 8:
            break

    return names[:8]


def _diagnosis_feature_set_summary(columns: list[str]) -> dict[str, object]:
    ordered_columns = [str(item).strip() for item in columns if str(item).strip()]
    column_set = set(ordered_columns)
    core_hits = [name for name in _DIAGNOSIS_PROTECT_CORE if name in column_set]
    group_counts: list[tuple[str, int]] = []
    drop_scores: list[tuple[str, int]] = []
    for label, members in _DIAGNOSIS_REDUNDANT_FAMILIES:
        hits = [name for name in ordered_columns if name in members]
        group_counts.append((label, len(hits)))
        drop_scores.append((label, sum(1 for name in hits if name not in _DIAGNOSIS_PROTECT_CORE_SET)))
    other_count = sum(1 for name in ordered_columns if name not in _DIAGNOSIS_REDUNDANT_MEMBER_SET)
    ranked_drop_groups = [
        label
        for label, score in sorted(
            drop_scores,
            key=lambda item: (
                -int(item[1]),
                _DIAGNOSIS_DEFAULT_DROP_ORDER.index(item[0])
                if item[0] in _DIAGNOSIS_DEFAULT_DROP_ORDER
                else len(_DIAGNOSIS_DEFAULT_DROP_ORDER),
                item[0],
            ),
        )
        if int(score) > 0
    ]
    return {
        "core_hits": core_hits,
        "group_counts": group_counts,
        "other_count": other_count,
        "drop_bias": ranked_drop_groups[:3],
    }


def _format_diagnosis_policy_lines() -> list[str]:
    return [
        "- diagnosis_policy: protect_core=" + ",".join(_DIAGNOSIS_PROTECT_CORE),
        "- diagnosis_policy: drop_from_first=" + ",".join(_DIAGNOSIS_DEFAULT_DROP_ORDER),
        "- diagnosis_policy: add_toward=" + ",".join(_DIAGNOSIS_ADD_THEMES),
    ]


def _format_feature_family_brief(project_root: Path, feature_names: list[str]) -> list[str]:
    registry = _load_custom_feature_registry(project_root)
    lines: list[str] = _format_diagnosis_policy_lines()
    for name in feature_names:
        payload = registry.get(name)
        if not payload:
            continue
        market = str(payload.get("market") or "").strip()
        width = _optional_int(payload.get("width"))
        notes = str(payload.get("notes") or "").strip()
        columns = [str(item) for item in payload.get("columns") or [] if str(item).strip()]
        header_parts = [name]
        meta_parts: list[str] = []
        if market:
            meta_parts.append(f"market={market}")
        if width is not None:
            meta_parts.append(f"width={width}")
        if notes:
            meta_parts.append(f"notes={notes}")
        if meta_parts:
            header_parts.append(": " + " / ".join(meta_parts))
        lines.append("- " + "".join(header_parts))
        if columns:
            diagnosis_summary = _diagnosis_feature_set_summary(columns)
            group_parts = [
                f"{label}={count}"
                for label, count in diagnosis_summary["group_counts"]
                if int(count) > 0
            ]
            group_parts.append(f"other={diagnosis_summary['other_count']}")
            lines.append("  diagnosis_groups: " + " / ".join(group_parts))
            if diagnosis_summary["core_hits"]:
                lines.append("  diagnosis_core_hits: " + ",".join(diagnosis_summary["core_hits"]))
            if diagnosis_summary["drop_bias"]:
                lines.append("  diagnosis_drop_bias: " + ",".join(diagnosis_summary["drop_bias"]))
    return lines


def _format_queue_snapshot(queue_payload: dict[str, object]) -> list[str]:
    items = [
        dict(item)
        for item in queue_payload.get("items") or []
        if isinstance(item, dict)
        and str(item.get("status") or "").strip().lower() in {"queued", "repair", "running"}
    ]
    if not items:
        return ["- no queued formal work"]
    lines = [
        f"- max_live_runs={int(queue_payload.get('max_live_runs') or 3)}",
    ]
    items.sort(
        key=lambda item: (
            str(item.get("status") or ""),
            str(item.get("action") or ""),
            str(item.get("market") or ""),
            str(item.get("run_label") or ""),
        )
    )
    for item in items[:8]:
        lines.append(
            "- "
            + " / ".join(
                [
                    f"track={item.get('track') or '?'}",
                    f"market={item.get('market') or '?'}",
                    f"status={item.get('status') or '?'}",
                    f"action={item.get('action') or '?'}",
                    f"suite={item.get('suite_name') or '?'}",
                    f"run={item.get('run_label') or '?'}",
                    f"retry={item.get('retry_count') or 0}",
                    f"reason={item.get('reason') or ''}".rstrip(),
                ]
            )
        )
    return lines


def _dense_prompt_guidance(program_path: Path) -> list[str]:
    program = Path(program_path).resolve()
    if not program.exists():
        return []
    text = program.read_text(encoding="utf-8").lower()
    if (
        "10-20" not in text
        and "140-280" not in text
        and "direction_dense" not in program.stem.lower()
        and "reversal_dense" not in program.stem.lower()
    ):
        return []
    width_ladder_match = _PROGRAM_WIDTH_LADDER_RE.search(program.read_text(encoding="utf-8"))
    width_ladder = width_ladder_match.group(1).strip() if width_ladder_match else "30 / 34 / 38 / 40 / 44 / 48"
    return [
        "Dense trade target for this session: 10-20 trades per coin per day.",
        "Frozen-window dense target: 140-280 trades per coin over the frozen window.",
        "Do not promote sparse winners; check count before ROI.",
        "A candidate below 56 trades is reject_sparse unless this cycle is only a bounded diagnostic.",
        "Feature-set width is not fixed to 40 for this session.",
        f"Allowed width ladder for this session: {width_ladder}.",
        "Move width by one bucket per bounded cycle only.",
        "Below 56 trades, prefer the next wider bucket before another same-width cosmetic swap.",
        "Inside 140-280 trades, keep width stable and prefer family replacement before changing width again.",
    ]


def find_live_formal_workers(project_root: Path) -> list[dict[str, object]]:
    root = Path(project_root).resolve()
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid=,ppid=,cmd="],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return []
    if result.returncode != 0:
        return []
    deduped_workers: dict[tuple[str, str, str], dict[str, object]] = {}
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if not line or (
            "run_one_experiment.sh" not in line
            and "research experiment run-suite" not in line
        ):
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        cmd = parts[2]
        if str(root) not in cmd:
            continue
        try:
            tokens = shlex.split(cmd)
        except ValueError:
            continue
        run_label = _extract_cli_flag(tokens, "--run-label")
        suite_name = _extract_cli_flag(tokens, "--suite")
        market = _extract_cli_flag(tokens, "--market")
        payload = {
            "pid": pid,
            "ppid": ppid,
            "run_label": run_label,
            "suite_name": suite_name,
            "market": market,
            "cmd": cmd,
        }
        dedupe_key = (
            str(run_label or ""),
            str(suite_name or ""),
            str(market or ""),
        )
        existing = deduped_workers.get(dedupe_key)
        if existing is None or int(pid) < int(existing.get("pid") or pid):
            deduped_workers[dedupe_key] = payload
    return sorted(deduped_workers.values(), key=lambda item: int(item.get("pid") or 0))


def find_live_autorun_processes(
    project_root: Path,
    *,
    output_path: Path | None = None,
) -> list[dict[str, object]]:
    root = Path(project_root).resolve()
    loop_scripts = {
        _autoresearch_dir(root) / "codex_background_loop.sh",
        _legacy_autoresearch_dir(root) / "codex_background_loop.sh",
    }
    resolved_output_path = (
        Path(output_path).resolve()
        if output_path is not None
        else root / "var" / "research" / "autorun" / "codex-last-output.txt"
    )
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid=,ppid=,cmd="],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return []
    if result.returncode != 0:
        return []
    processes: list[dict[str, object]] = []
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        cmd = parts[2]
        kind: str | None = None
        if any(str(loop_script) in cmd for loop_script in loop_scripts) and "__run_loop" in cmd:
            kind = "background_loop"
        elif (
            "codex exec" in cmd
            and f"--cd {root}" in cmd
            and f"--output-last-message {resolved_output_path}" in cmd
        ):
            kind = "codex_exec"
        if kind is None:
            continue
        processes.append(
            {
                "pid": pid,
                "ppid": ppid,
                "kind": kind,
                "cmd": cmd,
            }
        )
    return sorted(processes, key=lambda item: int(item.get("pid") or 0))


def _extract_cli_flag(tokens: list[str], flag: str) -> str | None:
    for index, token in enumerate(tokens):
        if token != flag:
            continue
        next_index = index + 1
        if next_index < len(tokens):
            value = str(tokens[next_index]).strip()
            return value or None
        break
    return None


def build_codex_cycle_prompt(
    *,
    project_root: Path,
    session_dir: Path,
    program_path: Path | None = None,
    status_path: Path | None = None,
) -> str:
    root = Path(project_root).resolve()
    session = Path(session_dir).resolve()
    program = resolve_autoresearch_program_path(root, program_path).resolve()
    queue_script = _repo_display_path(root, resolve_autoresearch_script_path(root, "experiment_queue.py"))
    background_script = _repo_display_path(root, resolve_autoresearch_script_path(root, "run_one_experiment_background.sh"))
    one_shot_script = _repo_display_path(root, resolve_autoresearch_script_path(root, "run_one_experiment.sh"))
    summarize_script = _repo_display_path(root, resolve_autoresearch_script_path(root, "summarize_experiment.py"))
    status_report = build_autorun_status_report(root, log_tail_lines=5, max_incomplete_runs=5, status_path=status_path)
    status_payload = status_report.get("status") or {}
    dense_context = _resolve_dense_context(
        status_path=status_path,
        status_payload=status_payload if isinstance(status_payload, dict) else None,
        session_dir=session,
        program_path=program,
    )
    queue_payload = dict(status_report.get("queue") or {})
    all_queue_items = [
        dict(item)
        for item in queue_payload.get("items") or []
        if isinstance(item, dict)
    ]
    queue_payload["items"] = _filter_payloads_for_dense_context(
        root,
        all_queue_items,
        context=dense_context,
        queue_items=all_queue_items,
    )
    formal_workers = _filter_payloads_for_dense_context(
        root,
        list(status_report.get("formal_workers") or []),
        context=dense_context,
        queue_items=all_queue_items,
    )
    track_slot_caps = {
        str(key).strip().lower(): int(value or 0)
        for key, value in dict(queue_payload.get("track_slot_caps") or {}).items()
        if str(key).strip()
    }
    current_track = _normalize_dense_track(dense_context.get("track"))
    allowed_live_runs = int(track_slot_caps.get(current_track) or queue_payload.get("max_live_runs") or 4)
    live_run_labels = {
        str(item.get("run_label") or "").strip()
        for item in formal_workers
        if str(item.get("run_label") or "").strip()
    }
    incomplete_runs = _filter_payloads_for_dense_context(
        root,
        list(status_report.get("incomplete_runs") or []),
        context=dense_context,
        queue_items=all_queue_items,
    )
    completed_runs = _filter_payloads_for_dense_context(
        root,
        list(status_report.get("completed_runs") or []),
        context=dense_context,
        queue_items=all_queue_items,
    )
    latest_cycle_eval = resolve_latest_cycle_eval_file(session)
    program_markets = resolve_program_markets(program)
    if not program_markets:
        seen_markets: set[str] = set()
        program_markets = []
        for payload in [*incomplete_runs, *completed_runs]:
            for market in _infer_markets_from_run_payload(payload):
                if market in seen_markets:
                    continue
                seen_markets.add(market)
                program_markets.append(market)
    completed_runs = _backfill_completed_runs_for_markets(
        project_root=root,
        markets=program_markets,
        completed_runs=completed_runs,
        context=dense_context,
        queue_items=all_queue_items,
    )
    slot_statuses = _collect_coin_slot_statuses(
        project_root=root,
        markets=program_markets,
        incomplete_runs=incomplete_runs,
        completed_runs=completed_runs,
        live_run_labels=live_run_labels,
    )
    decision_summary_lines = _format_machine_decision_summary(
        markets=program_markets,
        slot_statuses=slot_statuses,
        allowed_live_runs=allowed_live_runs,
        queue_payload=queue_payload,
    )
    coin_slot_lines = _format_coin_slot_snapshot(
        project_root=root,
        markets=program_markets,
        incomplete_runs=incomplete_runs,
        completed_runs=completed_runs,
        live_run_labels=live_run_labels,
    )
    feature_brief_lines = _format_feature_family_brief(
        root,
        _collect_relevant_feature_set_names(
            project_root=root,
            latest_cycle_eval=latest_cycle_eval,
            incomplete_runs=incomplete_runs,
            completed_runs=completed_runs,
        ),
    )
    queue_lines = _format_queue_snapshot(queue_payload)
    snapshot_lines: list[str] = []
    if status_payload:
        snapshot_lines.append(
            f"- autorun state: {status_payload.get('state') or 'unknown'}"
        )
        if status_payload.get("last_started_at"):
            snapshot_lines.append(f"- last_started_at: {status_payload['last_started_at']}")
        if status_payload.get("last_finished_at"):
            snapshot_lines.append(f"- last_finished_at: {status_payload['last_finished_at']}")
    snapshot_lines.append(f"- live formal workers: {len(formal_workers)}")
    if incomplete_runs:
        snapshot_lines.append("- current incomplete runs:")
        for item in incomplete_runs[:3]:
            snapshot_lines.append(
                "  - "
                f"{item.get('suite_name') or item.get('run_dir')} / "
                f"{item.get('run_label') or 'unknown'} / "
                f"state={item.get('state') or 'unknown'} / "
                f"completed={item.get('completed_cases') or 0} / "
                f"failed={item.get('failed_cases') or 0} / "
                f"last_event={item.get('last_event') or 'unknown'}"
            )
    if completed_runs:
        snapshot_lines.append("- recent completed runs:")
        for item in completed_runs[:4]:
            snapshot_lines.append(
                "  - "
                f"{item.get('suite_name') or item.get('run_dir')} / "
                f"{item.get('run_label') or 'unknown'} / "
                f"completed={item.get('completed_cases') or 0} / "
                f"failed={item.get('failed_cases') or 0}"
            )
    initial_files: list[str] = []
    project_agents = resolve_project_agents_path(root)
    if project_agents is not None:
        initial_files.append(f"- {project_agents}")
    initial_files.append(f"- {program}")
    if latest_cycle_eval is not None:
        initial_files.append(f"- {latest_cycle_eval}")
    else:
        initial_files.append(f"- {session / 'session.md'}")
    dense_guidance_lines = _dense_prompt_guidance(program)

    return "\n".join(
        [
            "Read the repository research instructions and complete exactly one autonomous research cycle.",
            "One bounded decision cycle may launch multiple distinct formal runs if that is needed to refill idle coin slots allowed by program.md.",
            "",
            f"Project root: {root}",
            f"Program: {program}",
            f"Session dir: {session}",
            "",
            *([*dense_guidance_lines, ""] if dense_guidance_lines else []),
            "Machine decision summary already collected for you:",
            *(decision_summary_lines or ["- no machine decision summary available"]),
            "",
            "Start with only these files unless they prove insufficient:",
            *initial_files,
            "",
            "Use repository commands sparingly. Do not scan the entire repository or the full experiment history unless the cycle is blocked.",
            "Use the machine decision summary and current autorun snapshot first; open `results.tsv` or historical cycle eval only if you still need extra rationale after accepting the current occupancy in the summary.",
            "Queue or resume formal work for the idle coin slots first when the machine decision summary already shows clear `refill_now` or `resume_or_replace_now` actions; do not delay those launches just to expand historical reading.",
            "Do not open large raw registry files like `research/experiments/custom_feature_sets.json` in the normal decision path; use the pre-extracted coin snapshot and feature-family brief below first.",
            "If you still need exact factor columns, inspect only the exact named feature family that is missing from the brief instead of dumping broad file ranges.",
            "Use the diagnosis-guided family policy in the brief: protect the named core skeleton features, prefer dropping from overloaded repetitive families, and add toward the listed missing-information themes before inventing broader searches.",
            "Only a run with a live `run_one_experiment.sh` worker counts as an active occupied slot; a checkpoint-only incomplete run with `live_worker=no` is resumable but does not currently consume live concurrency.",
            "A bounded cycle refers to the Codex decision pass, not an automatic timeout for every formal worker.",
            "If `rg` is unavailable, use `find`, `grep`, `sed`, and targeted `ls`.",
            "When the session is long, start from results.tsv plus the newest cycle eval and open the full session history only if those are insufficient.",
            "Prefer formal experiment launches over unrelated environment or infrastructure edits.",
            "Only make code changes when they directly unblock the next formal experiment for this session.",
            "When session artifacts disagree with current run directories, trust the current run directories.",
            "Historical cycle eval notes about live workers or CPU health are not authoritative for the current cycle; only the machine-collected snapshot in this prompt plus any fresh verification you perform in this same cycle may decide current occupancy.",
            "A formal run is finished only when `completed_cases + failed_cases` reaches `cases`; if `summary.json` exists but work remains, treat it as a checkpointed resumable run rather than a finished run.",
            "If a healthy live formal worker already fills a coin slot, leave it running; a monitor-only cycle is valid when the active workers are still the right frontier.",
            "If a healthy live formal worker already fills a coin slot but the machine decision summary marks that slot as `action=prepare_next_now`, keep the live worker running and queue exactly one successor behind it in this same cycle.",
            "If the active program explicitly allows multiple simultaneous formal runs and some coin slots are idle, keep launching distinct follow-ups in the same cycle until you fill every allowed idle slot that still has a clear next follow-up.",
            "Do not leave an idle coin slot unfilled solely because the latest result is thin-sample, tied, or still marked `research_only`; when the frontier is unresolved but the slot is free, choose the next bounded follow-up that can strengthen or falsify that edge.",
            "Launching several distinct coin follow-ups in one decision pass still counts as one bounded cycle.",
            "If the current autorun snapshot reports `live formal workers: 0`, you are expected to queue or resume work for every coin slot that is `state=idle` or `state=checkpointed` in the coin snapshot during this same cycle unless you verify a fresh blocking issue from the exact current run directory.",
            "If live formal workers are below the allowed concurrency and some current-line runs are only checkpointed with `live_worker=no`, resume as many checkpointed current-line runs as needed to fill those live slots in the same cycle before opening any new branch.",
            "For active coin slots with `action=prepare_next_now`, queue exactly one queued successor for that same coin and track; the successor should wait in the queue and must not replace or stop the current live frontier during this cycle.",
            "If a feature-set name mentioned by old session artifacts is missing from the current registry, treat that as historical drift rather than a blocker; inspect the exact current suite spec or current run directory for the active coin and continue the decision instead of reconciling the full registry history.",
            f"When you need a long-lived formal worker to keep running after this cycle, launch it detached with `{background_script}` and do not add `--timeout-sec` unless you intentionally want a bounded diagnostic probe.",
            f"Reserve `{one_shot_script}` with `--timeout-sec` for deliberate bounded checkpoints, diagnostics, or stuck-run inspection.",
            "Do not stop or checkpoint a healthy live formal run merely to end the current Codex cycle.",
            f"Queue formal launches and repairs instead of directly filling all slots yourself; the queue supervisor is responsible for keeping up to {allowed_live_runs} live formal runs active.",
            f"Use `{queue_script} enqueue ...` for normal formal work and reserve direct launches for deliberate bounded diagnostics only.",
            "",
            "Current autorun snapshot already collected for you:",
            *(snapshot_lines or ["- no existing autorun status snapshot found"]),
            "",
            "Coin slot snapshot already collected for you:",
            *(coin_slot_lines or ["- no explicit coin slot snapshot available"]),
            "",
            "Queue snapshot already collected for you:",
            *queue_lines,
            "",
            "Relevant feature-family brief already extracted for you:",
            *(feature_brief_lines or ["- no focused feature-family brief available"]),
            "",
            "Required cycle steps:",
            f"1. Read the machine decision summary plus {program.name} before making changes; open results.tsv plus the newest cycle eval only if you still need historical rationale after accepting the current occupancy in the summary.",
            "2. Reconcile the active session artifacts against the current run directories before deciding what is still live, blocked, or already complete.",
            "3. Inspect only the specific active, incomplete, or most recent completed experiment runs needed to avoid duplicates blindly and choose the next step.",
            "4. Decide the next experiment set or code change.",
            "5. If there are idle coin slots that the active program allows you to fill, refill as many distinct idle coin slots as possible in this same cycle instead of stopping after the first new launch.",
            "5a. If there are no idle coin slots but live formal workers are still below the allowed concurrency, resume multiple checkpointed current-line runs in this same cycle until the live slots are filled or no clear resume target remains.",
            "6. If needed, edit code or create/update a suite spec.",
            "7. Queue distinct formal runs or repairs for different idle coins until you have recorded every clear next same-session follow-up needed to refill the allowed slots.",
            f"8. Use `{queue_script} enqueue --market ... --suite ... --run-label ... --action launch|resume|repair --reason ...` for normal formal work; let the queue supervisor consume queued items and keep occupancy near {allowed_live_runs}.",
            f"9. Use `{one_shot_script}` only for intentionally bounded probes, checkpoints, or diagnostics.",
            f"10. Use `{summarize_script}` for completed, failed, or intentionally checkpointed runs; do not interrupt a healthy live worker just to summarize it.",
            f"11. Never have more than {allowed_live_runs} simultaneous formal market runs active unless program.md imposes a stricter limit.",
            "12. Update the session artifacts under the session dir.",
            "13. Stop after this one decision cycle and summarize what changed, what ran, what you queued, and what should happen next.",
            "",
            "Your Codex decision pass must end after this cycle, but any healthy formal experiment workers you started or observed may continue running after you exit.",
        ]
    )


def build_codex_exec_extra_args(extra_args: str | None = None) -> tuple[str, ...]:
    tokens = tuple(shlex.split(str(extra_args or "").strip()))
    if "--skip-git-repo-check" in tokens:
        return tokens
    return (*tokens, "--skip-git-repo-check")


def resolve_codex_exec_binary(
    *,
    explicit_binary: str | None = None,
    home_root: Path | None = None,
    env_path: str | None = None,
) -> str:
    if explicit_binary is not None and str(explicit_binary).strip():
        return str(explicit_binary).strip()

    discovered = shutil.which("codex", path=env_path)
    if discovered:
        return discovered

    home = Path.home() if home_root is None else Path(home_root).expanduser().resolve()
    for candidate in (
        home / ".local" / "bin" / "codex",
        home / "bin" / "codex",
    ):
        if candidate.exists():
            return str(candidate.resolve())

    return "codex"


def resolve_codex_exec_path_prefix(project_root: Path) -> str | None:
    root = Path(project_root).resolve()
    for candidate in (
        root / ".venv_server" / "bin",
        root / ".venv" / "bin",
    ):
        if not candidate.is_dir():
            continue
        if any((candidate / name).exists() for name in ("python", "python3", "codex")):
            return str(candidate)
    return None


def build_codex_exec_command(
    *,
    project_root: Path,
    output_path: Path,
    sandbox_mode: str,
    model: str | None = None,
    extra_args: str | None = None,
    codex_bin: str | None = None,
) -> tuple[str, ...]:
    root = Path(project_root).resolve()
    output = Path(output_path).resolve()
    command: list[str] = [
        resolve_codex_exec_binary(explicit_binary=codex_bin),
        "exec",
        "--cd",
        str(root),
        "--output-last-message",
        str(output),
        "--sandbox",
        str(sandbox_mode),
    ]
    if model:
        command.extend(["--model", str(model)])
    command.extend(build_codex_exec_extra_args(extra_args))
    command.append("-")
    return tuple(command)


def record_session_update(
    *,
    session_dir: Path,
    cycle: str,
    team: str,
    metric: str,
    status: str,
    description: str,
    files_changed: list[str] | None = None,
    timestamp: str | None = None,
    cycle_eval_md: str | None = None,
    cycle_notes: list[str] | None = None,
    tried_lines: list[str] | None = None,
    open_issue_lines: list[str] | None = None,
) -> dict[str, str]:
    base = Path(session_dir)
    base.mkdir(parents=True, exist_ok=True)
    results_path = base / "results.tsv"
    session_path = base / "session.md"
    cycles_dir = base / "cycles" / str(cycle)
    cycles_dir.mkdir(parents=True, exist_ok=True)
    eval_path = cycles_dir / "eval-results.md"
    stamp = timestamp or datetime.now().astimezone().replace(microsecond=0).isoformat()

    if not results_path.exists():
        results_path.write_text(
            "cycle\tteam\tmetric\tstatus\tdescription\tfiles_changed\ttimestamp\n",
            encoding="utf-8",
        )
    with results_path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(
            [
                str(cycle),
                str(team),
                str(metric),
                str(status),
                str(description),
                ",".join(str(item) for item in (files_changed or [])),
                stamp,
            ]
        )

    if cycle_eval_md is not None:
        eval_path.write_text(str(cycle_eval_md), encoding="utf-8")

    if session_path.exists():
        content = session_path.read_text(encoding="utf-8")
    else:
        content = "\n".join(
            [
                "# Research Session",
                "",
                "## Cycles completed",
                "",
                "## What's been tried",
                "",
                "## Open issues",
                "",
            ]
        )
    content = _append_cycle_notes(content, cycle=str(cycle), notes=cycle_notes or [])
    content = _append_section_lines(content, "## What's been tried", [f"- {line}" for line in (tried_lines or [])])
    content = _append_section_lines(content, "## Open issues", [f"- {line}" for line in (open_issue_lines or [])])
    session_path.write_text(content, encoding="utf-8")

    return {
        "results_path": str(results_path),
        "session_path": str(session_path),
        "cycle_eval_path": str(eval_path),
    }


def _append_cycle_notes(content: str, *, cycle: str, notes: list[str]) -> str:
    if not notes:
        return content
    lines = [f"- `{cycle}`", *[f"  - {note}" for note in notes]]
    return _append_section_lines(content, "## Cycles completed", lines)


def _append_section_lines(content: str, heading: str, new_lines: list[str]) -> str:
    if not new_lines:
        return content
    lines = content.splitlines()
    try:
        start = next(index for index, line in enumerate(lines) if line.strip() == heading.strip())
    except StopIteration:
        suffix = [content.rstrip(), "", heading, "", *new_lines, ""]
        return "\n".join(part for part in suffix if part != "")
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index].startswith("## "):
            end = index
            break
    block = lines[start + 1 : end]
    appendable = [line for line in new_lines if line not in block]
    if not appendable:
        return content
    insertion = block[:]
    if insertion and insertion[-1] != "":
        insertion.append("")
    insertion.extend(appendable)
    lines[start + 1 : end] = insertion
    return "\n".join(lines).rstrip() + "\n"


def _optional_int(value: object) -> int | None:
    try:
        return None if value in {None, ""} else int(value)
    except Exception:
        return None


def _optional_float(value: object) -> float | None:
    try:
        return None if value in {None, ""} else float(value)
    except Exception:
        return None


def _summarize_incomplete_formal_run(path: Path) -> dict[str, object]:
    inspection = inspect_experiment_run(path)
    suite_name = inspection.get("suite_name")
    suite_spec = _read_suite_spec(path, suite_name=suite_name if isinstance(suite_name, str) else None)
    leaderboard_path = path / "leaderboard.csv"
    leaderboard_rows = _csv_row_count(leaderboard_path)
    top_case = _read_formal_top_case(leaderboard_path)
    raw_summary = {
        "suite_name": suite_name,
        "run_label": inspection.get("run_label"),
        "cases": _estimate_formal_cases(suite_spec),
        "completed_cases": inspection.get("completed_cases"),
        "failed_cases": inspection.get("failed_cases"),
        "leaderboard_rows": leaderboard_rows,
        "top_roi_pct": top_case.get("roi_pct") if top_case else None,
        "markets": _suite_markets(suite_spec, inspection=inspection),
        "state": inspection.get("state"),
        "last_event": inspection.get("last_event"),
        "last_case_label": inspection.get("last_case_label"),
        "summary_exists": False,
    }
    return {
        "suite_name": suite_name,
        "run_label": inspection.get("run_label"),
        "cases": _optional_int(raw_summary.get("cases")),
        "completed_cases": _optional_int(raw_summary.get("completed_cases")),
        "failed_cases": _optional_int(raw_summary.get("failed_cases")),
        "leaderboard_rows": leaderboard_rows,
        "top_roi_pct": _optional_float(raw_summary.get("top_roi_pct")),
        "markets": list(raw_summary.get("markets") or []),
        "summary_path": str(path / "summary.json"),
        "report_path": str(path / "report.md"),
        "leaderboard_path": str(leaderboard_path) if leaderboard_path.exists() else None,
        "top_case": top_case,
        "raw_summary": raw_summary,
    }


def _read_formal_top_case(leaderboard_path: Path) -> dict[str, object] | None:
    first_row = _read_first_csv_row(leaderboard_path)
    if first_row is None:
        return None
    return {
        "market": first_row.get("market"),
        "group_name": first_row.get("group_name"),
        "run_name": first_row.get("run_name"),
        "target": first_row.get("target"),
        "variant_label": first_row.get("variant_label"),
        "roi_pct": _optional_float(first_row.get("roi_pct")),
        "pnl_sum": _optional_float(first_row.get("pnl_sum")),
        "trades": _optional_int(first_row.get("trades")),
    }


def _read_quick_screen_top_case(leaderboard_path: Path) -> dict[str, object] | None:
    first_row = _read_first_csv_row(leaderboard_path)
    if first_row is None:
        return None
    return {
        "market": first_row.get("market"),
        "group_name": first_row.get("group_name"),
        "run_name": first_row.get("run_name"),
        "target": first_row.get("target"),
        "variant_label": first_row.get("variant_label"),
        "feature_set": first_row.get("feature_set"),
        "roi_pct": _optional_float(first_row.get("roi_pct")),
        "pnl_sum": _optional_float(first_row.get("pnl_sum")),
        "trades": _optional_int(first_row.get("trades") or first_row.get("trade_rows")),
        "trade_rows": _optional_int(first_row.get("trade_rows")),
        "traded_winner_in_band_rows": _optional_int(first_row.get("traded_winner_in_band_rows")),
        "backed_winner_in_band_rows": _optional_int(first_row.get("backed_winner_in_band_rows")),
        "rank": _optional_int(first_row.get("rank")),
    }


def _read_first_csv_row(path: Path) -> dict[str, str] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return next(reader, None)


def _csv_row_count(path: Path) -> int | None:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None:
            return 0
        return sum(1 for _ in reader)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    payloads: list[dict[str, object]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        token = raw_line.strip()
        if not token:
            continue
        try:
            decoded = json.loads(token)
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, dict):
            payloads.append(decoded)
    return payloads


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


def _read_suite_spec(path: Path, *, suite_name: str | None) -> dict[str, object] | None:
    if not suite_name:
        return None
    project_root = _project_root_from_run_dir(path)
    if project_root is None:
        return None
    suite_spec_path = project_root / "research" / "experiments" / "suite_specs" / f"{suite_name}.json"
    if not suite_spec_path.exists():
        return None
    try:
        payload = json.loads(suite_spec_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _project_root_from_run_dir(path: Path) -> Path | None:
    for candidate in [path, *path.parents]:
        if (candidate / "research" / "experiments" / "runs").exists():
            return candidate
    return None


def _estimate_formal_cases(suite_spec: dict[str, object] | None) -> int | None:
    if not suite_spec:
        return None
    markets_payload = suite_spec.get("markets")
    if not isinstance(markets_payload, dict):
        return None
    stakes = suite_spec.get("stakes")
    max_trades = suite_spec.get("max_trades_per_market_values")
    stake_count = max(1, len(stakes)) if isinstance(stakes, list) else 1
    max_trade_count = max(1, len(max_trades)) if isinstance(max_trades, list) else 1
    total = 0
    for market_payload in markets_payload.values():
        if not isinstance(market_payload, dict):
            continue
        groups = market_payload.get("groups")
        if not isinstance(groups, dict):
            continue
        for group_payload in groups.values():
            if not isinstance(group_payload, dict):
                continue
            runs = group_payload.get("runs")
            if not isinstance(runs, list):
                continue
            for run_payload in runs:
                if not isinstance(run_payload, dict):
                    continue
                feature_variants = run_payload.get("feature_set_variants")
                weight_variants = run_payload.get("weight_variants")
                feature_count = max(1, len(feature_variants)) if isinstance(feature_variants, list) else 1
                weight_count = max(1, len(weight_variants)) if isinstance(weight_variants, list) else 1
                total += feature_count * weight_count * stake_count * max_trade_count
    return total or None


def _suite_markets(suite_spec: dict[str, object] | None, *, inspection: dict[str, object]) -> list[str]:
    if suite_spec:
        markets_payload = suite_spec.get("markets")
        if isinstance(markets_payload, dict):
            return [str(key) for key in markets_payload.keys()]
    last_case_label = inspection.get("last_case_label")
    if isinstance(last_case_label, str) and "/" in last_case_label:
        market = last_case_label.split("/", 1)[0].strip()
        if market:
            return [market]
    return []


def _tail_lines(path: Path, *, limit: int) -> list[str]:
    if limit <= 0 or not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return lines[-limit:]


def _pid_is_live(pid_value: object) -> bool:
    try:
        pid = int(pid_value)
    except Exception:
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True
