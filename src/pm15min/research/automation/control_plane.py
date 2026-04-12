from __future__ import annotations

import csv
import json
from datetime import datetime
import os
from pathlib import Path
import re
import shlex
import shutil


_CODEX_HOME_COPY_FILES = (
    "auth.json",
    "config.toml",
    "AGENTS.md",
    "version.json",
)

_CODEX_PROVIDER_SECTION_RE = re.compile(
    r"(?ms)^\[model_providers\.codex\]\n.*?(?=^\[|\Z)",
)
_ACTIVE_SESSION_LINE_RE = re.compile(
    r"(?im)^\s*-\s*(?:active session|new active session)\s*:\s*`?([^`\n]+/(?:session\.md|results\.tsv))`?\s*$"
)
_SESSION_REF_RE = re.compile(r"(sessions/[^\s`]+/(?:session\.md|results\.tsv))")
_TRANSIENT_PROVIDER_FAILURE_MARKERS = (
    "503 service unavailable",
    "service temporarily unavailable",
    "error sending request for url",
    "stream disconnected before completion",
    "unexpected status 503",
    "error decoding response body",
)


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
            "requires_openai_auth = true",
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
    if base_url and str(base_url).strip().lower() in text:
        return True
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
    completed_cases = sum(1 for item in events if item.get("event") == "market_completed")
    failed_cases = sum(1 for item in events if item.get("event") == "market_failed")
    last_event_payload = events[-1] if events else {}
    last_event = str(last_event_payload.get("event") or "") or None
    if summary_path.exists():
        state = "completed"
    elif completed_cases == 0 and failed_cases == 0 and last_event in {
        "execution_group_seed_case_started",
        "execution_group_warmup_completed",
        "market_cache_resolved",
    }:
        state = "stuck_seed_case"
    else:
        state = "incomplete"
    return {
        "run_dir": str(path),
        "suite_name": _suite_name_from_run_dir(path),
        "run_label": _run_label_from_run_dir(path),
        "summary_exists": summary_path.exists(),
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
    run_dirs = sorted(
        (
            path
            for path in runs_root.glob("suite=*/run=*")
            if path.is_dir() and (path / "logs" / "suite.jsonl").exists() and not (path / "summary.json").exists()
        ),
        key=lambda item: (item / "logs" / "suite.jsonl").stat().st_mtime,
        reverse=True,
    )
    return [inspect_experiment_run(path) for path in run_dirs[: max(1, int(limit))]]


def build_autorun_status_report(
    project_root: Path,
    *,
    status_path: Path | None = None,
    log_tail_lines: int = 20,
    max_incomplete_runs: int = 10,
) -> dict[str, object]:
    root = Path(project_root).resolve()
    resolved_status_path = status_path or (root / "var" / "research" / "autorun" / "codex-background.status.json")
    log_path = root / "var" / "research" / "autorun" / "codex-background.log"
    status_payload: dict[str, object] = {}
    if resolved_status_path.exists():
        status_payload = json.loads(resolved_status_path.read_text(encoding="utf-8"))
        status_payload.setdefault("failure_count", 0)
        if str(status_payload.get("state") or "") == "running" and not _pid_is_live(status_payload.get("pid")):
            status_payload["state"] = "stale"
            status_payload["state_reason"] = "missing_pid"
    return {
        "status_path": str(resolved_status_path),
        "log_path": str(log_path),
        "status": status_payload,
        "log_tail": _tail_lines(log_path, limit=max(0, int(log_tail_lines))),
        "incomplete_runs": find_incomplete_experiment_runs(root, limit=max_incomplete_runs),
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
    raw_program = Path(program_path) if program_path is not None else (root / "program.md")
    program = raw_program if raw_program.is_absolute() else (root / raw_program)
    if program.exists():
        text = program.read_text(encoding="utf-8")
        match = _ACTIVE_SESSION_LINE_RE.search(text) or _SESSION_REF_RE.search(text)
        if match:
            session_path = Path(match.group(1))
            if not session_path.is_absolute():
                session_path = root / session_path
            return session_path.parent.resolve()

    return (root / "sessions" / "autoresearch").resolve()


def build_codex_cycle_prompt(
    *,
    project_root: Path,
    session_dir: Path,
    program_path: Path | None = None,
) -> str:
    root = Path(project_root).resolve()
    session = Path(session_dir).resolve()
    program = (program_path or (root / "program.md")).resolve()
    status_report = build_autorun_status_report(root, log_tail_lines=5, max_incomplete_runs=5)
    status_payload = status_report.get("status") or {}
    incomplete_runs = list(status_report.get("incomplete_runs") or [])
    snapshot_lines: list[str] = []
    if status_payload:
        snapshot_lines.append(
            f"- autorun state: {status_payload.get('state') or 'unknown'}"
        )
        if status_payload.get("last_started_at"):
            snapshot_lines.append(f"- last_started_at: {status_payload['last_started_at']}")
        if status_payload.get("last_finished_at"):
            snapshot_lines.append(f"- last_finished_at: {status_payload['last_finished_at']}")
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
    return "\n".join(
        [
            "Read the repository research instructions and complete exactly one autonomous research cycle.",
            "",
            f"Project root: {root}",
            f"Program: {program}",
            f"Session dir: {session}",
            "",
            "Start with only these files unless they prove insufficient:",
            f"- {root / 'AGENTS.md'}",
            f"- {program}",
            f"- {session / 'session.md'}",
            f"- {session / 'results.tsv'}",
            "",
            "Use repository commands sparingly. Do not scan the entire repository or the full experiment history unless the cycle is blocked.",
            "If `rg` is unavailable, use `find`, `grep`, `sed`, and targeted `ls`.",
            "Prefer resuming or launching one formal experiment over unrelated environment or infrastructure edits.",
            "Only make code changes when they directly unblock the next formal experiment for this session.",
            "",
            "Current autorun snapshot already collected for you:",
            *(snapshot_lines or ["- no existing autorun status snapshot found"]),
            "",
            "Required cycle steps:",
            f"1. Read {program.name} and the latest session artifacts before making changes.",
            "2. Inspect only the specific active or incomplete experiment runs needed to avoid duplicates blindly.",
            "3. Decide the next single experiment or code change.",
            "4. If needed, edit code or create/update a suite spec.",
            "5. Use scripts/research/run_one_experiment.sh for any formal experiment run.",
            "6. Use scripts/research/summarize_experiment.py to summarize the experiment run you just completed.",
            "7. Never have more than two simultaneous formal market runs active unless program.md explicitly overrides it.",
            "8. Update the session artifacts under the session dir.",
            "9. Stop after this one cycle and summarize what changed, what ran, and what should happen next.",
            "",
            "Do not run forever in this Codex invocation. One completed cycle only.",
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
    lines = path.read_text(encoding="utf-8").splitlines()
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
