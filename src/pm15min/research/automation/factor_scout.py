from __future__ import annotations

from pathlib import Path
import re
import time
from typing import Any

from pm15min.research.features.registry import feature_registry


FACTOR_SCOUT_BACKLOG_NAME = "DEEP_OTM_BASELINE_FACTOR_SCOUT_BACKLOG.md"

_CANDIDATE_HEADING_RE = re.compile(r"^##\s+Candidate:\s*(?P<title>.+?)\s*$", re.IGNORECASE)
_FIELD_RE = re.compile(r"^-\s*(?P<key>[a-zA-Z0-9_ -]+)\s*:\s*(?P<value>.*?)\s*$")


def factor_scout_backlog_path(project_root: Path | str) -> Path:
    return Path(project_root).resolve() / "docs" / FACTOR_SCOUT_BACKLOG_NAME


def build_factor_scout_prompt(
    *,
    project_root: Path | str,
    target: str = "reversal",
    markets: list[str] | tuple[str, ...] | None = None,
    max_candidates: int = 6,
    backlog_path: Path | str | None = None,
) -> str:
    root = Path(project_root).resolve()
    backlog = Path(backlog_path).resolve() if backlog_path is not None else factor_scout_backlog_path(root)
    market_tokens = _normalize_markets(markets)
    target_token = str(target or "reversal").strip().lower() or "reversal"
    max_count = max(1, int(max_candidates))
    inventory_lines = _format_existing_inventory_lines()

    return "\n".join(
        [
            "You are the dedicated factor scout for this autoresearch system.",
            "",
            f"Project root: {root}",
            f"Target: {target_token}",
            f"Markets: {' / '.join(market_tokens)}",
            f"Backlog path: {backlog}",
            "",
            "Mission:",
            f"- Find up to {max_count} candidate factor ideas from public sources only.",
            "- Focus on short-horizon crypto, prediction-market microstructure, orderbook behavior, flow, regime, and cross-asset leads.",
            "- Prefer ideas that could explain why the current experiment keeps missing trade density or reversal reliability.",
            "",
            "Hard rules:",
            "- Public sources only. Do not use leaked, private, paywalled, or unclear-provenance material.",
            "- Do not modify experiment suite specs.",
            "- do not edit custom_feature_sets.json.",
            "- Do not launch training, backtests, queues, or live workers.",
            f"- Write candidates only to `{_repo_display_path(root, backlog)}`.",
            "- Treat the output as research leads, not approved production factors.",
            "- Every candidate must include a source URL that a reviewer can open.",
            "",
            "For every candidate factor, append this exact Markdown shape:",
            "",
            "## Candidate: <candidate factor name>",
            "- source_url: <public URL>",
            "- source_title: <source title>",
            "- target: <direction|reversal|both>",
            "- markets: <comma-separated markets>",
            "- status: proposed",
            "- idea: <one-sentence explanation>",
            "- repo_overlap: <existing similar repo factor, or none>",
            "- data_needed: <data required to build it>",
            "- test_hypothesis: <what experiment should prove or reject>",
            "- risk: <main reason it may fail>",
            "",
            "Existing repo factor inventory summary:",
            *inventory_lines,
        ]
    )


def summarize_factor_scout_backlog(
    project_root: Path | str,
    *,
    limit: int = 5,
    backlog_path: Path | str | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    backlog = Path(backlog_path).resolve() if backlog_path is not None else factor_scout_backlog_path(root)
    candidates = _parse_backlog_candidates(backlog)
    lines = [_format_candidate_summary(item) for item in candidates[: max(0, int(limit))]]
    return {
        "path": str(backlog),
        "exists": backlog.exists(),
        "candidate_count": len(candidates),
        "lines": lines,
    }


def should_refresh_factor_scout_backlog(
    project_root: Path | str,
    *,
    stamp_path: Path | str | None = None,
    now_epoch: float | None = None,
    min_interval_sec: int = 21600,
    backlog_path: Path | str | None = None,
    markets: list[str] | tuple[str, ...] | None = None,
    target: str | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    backlog = Path(backlog_path).resolve() if backlog_path is not None else factor_scout_backlog_path(root)
    stamp = Path(stamp_path).resolve() if stamp_path is not None else (
        root / "var" / "research" / "autorun" / "factor-scout.last-success"
    )
    now = float(time.time() if now_epoch is None else now_epoch)
    min_interval = max(0, int(min_interval_sec))
    candidates = _parse_backlog_candidates(backlog)
    candidate_count = len(candidates)
    requested_markets = set(_normalize_markets(markets)) if markets else set()
    requested_target = str(target or "").strip().lower()
    matching_candidate_count = len(
        _filter_candidates_for_request(
            candidates,
            markets=requested_markets,
            target=requested_target,
        )
    )
    last_success = _read_epoch_stamp(stamp)
    backlog_age_sec = _path_age_sec(backlog, now)
    age_sec = None if last_success is None else max(0, int(now - last_success))

    if candidate_count <= 0:
        if last_success is not None and age_sec is not None and age_sec < min_interval:
            return {
                "should_refresh": False,
                "reason": "recent_empty_attempt",
                "candidate_count": candidate_count,
                "matching_candidate_count": matching_candidate_count,
                "age_sec": age_sec,
                "stamp_path": str(stamp),
                "backlog_path": str(backlog),
            }
        return {
            "should_refresh": True,
            "reason": "empty_backlog",
            "candidate_count": candidate_count,
            "matching_candidate_count": matching_candidate_count,
            "age_sec": age_sec,
            "stamp_path": str(stamp),
            "backlog_path": str(backlog),
        }

    if requested_markets and matching_candidate_count <= 0:
        if last_success is not None and age_sec is not None and age_sec < min_interval:
            return {
                "should_refresh": False,
                "reason": "recent_market_empty_attempt",
                "candidate_count": candidate_count,
                "matching_candidate_count": matching_candidate_count,
                "age_sec": age_sec,
                "stamp_path": str(stamp),
                "backlog_path": str(backlog),
            }
        return {
            "should_refresh": True,
            "reason": "missing_market_candidates",
            "candidate_count": candidate_count,
            "matching_candidate_count": matching_candidate_count,
            "age_sec": age_sec,
            "stamp_path": str(stamp),
            "backlog_path": str(backlog),
        }

    if last_success is None and backlog_age_sec is not None and backlog_age_sec < min_interval:
        return {
            "should_refresh": False,
            "reason": "fresh_backlog_file",
            "candidate_count": candidate_count,
            "matching_candidate_count": matching_candidate_count,
            "age_sec": backlog_age_sec,
            "stamp_path": str(stamp),
            "backlog_path": str(backlog),
        }

    if last_success is None or age_sec is None or age_sec >= min_interval:
        return {
            "should_refresh": True,
            "reason": "stale_backlog",
            "candidate_count": candidate_count,
            "matching_candidate_count": matching_candidate_count,
            "age_sec": age_sec,
            "stamp_path": str(stamp),
            "backlog_path": str(backlog),
        }

    return {
        "should_refresh": False,
        "reason": "fresh_backlog",
        "candidate_count": candidate_count,
        "matching_candidate_count": matching_candidate_count,
        "age_sec": age_sec,
        "stamp_path": str(stamp),
        "backlog_path": str(backlog),
    }


def _parse_backlog_candidates(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    candidates: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    in_code_block = False
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if line.startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            continue
        heading = _CANDIDATE_HEADING_RE.match(line)
        if heading:
            if current is not None:
                _append_candidate_if_real(candidates, current)
            current = {"title": heading.group("title").strip()}
            continue
        if current is None:
            continue
        field = _FIELD_RE.match(line)
        if not field:
            continue
        key = field.group("key").strip().lower().replace(" ", "_").replace("-", "_")
        current[key] = field.group("value").strip()
    if current is not None:
        _append_candidate_if_real(candidates, current)
    return candidates


def _read_epoch_stamp(path: Path) -> float | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    if not text:
        return None
    try:
        return float(text.splitlines()[0].strip())
    except ValueError:
        return None


def _path_age_sec(path: Path, now_epoch: float) -> int | None:
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return None
    return max(0, int(float(now_epoch) - float(mtime)))


def _append_candidate_if_real(candidates: list[dict[str, str]], candidate: dict[str, str]) -> None:
    title = str(candidate.get("title") or "").strip()
    source = str(candidate.get("source_url") or "").strip()
    if not title or "<" in title or ">" in title:
        return
    if not source or "<" in source or ">" in source:
        return
    candidates.append(candidate)


def _format_candidate_summary(candidate: dict[str, str]) -> str:
    title = candidate.get("title") or "untitled"
    target = candidate.get("target") or "unknown"
    markets = candidate.get("markets") or "unknown"
    status = candidate.get("status") or "unknown"
    source = candidate.get("source_url") or "unknown"
    return f"- {title} / target={target} / markets={markets} / status={status} / source={source}"


def _filter_candidates_for_request(
    candidates: list[dict[str, str]],
    *,
    markets: set[str],
    target: str,
) -> list[dict[str, str]]:
    if not markets and not target:
        return list(candidates)
    out: list[dict[str, str]] = []
    for candidate in candidates:
        if markets:
            candidate_markets = {
                item
                for item in re.findall(
                    r"(?<![a-z0-9])(btc|eth|sol|xrp)(?![a-z0-9])",
                    str(candidate.get("markets") or "").lower(),
                )
            }
            if not candidate_markets.intersection(markets):
                continue
        if target:
            candidate_target = str(candidate.get("target") or "").strip().lower()
            if candidate_target and candidate_target not in {target, "both"}:
                continue
        out.append(candidate)
    return out


def _normalize_markets(markets: list[str] | tuple[str, ...] | None) -> list[str]:
    if not markets:
        return ["btc", "eth", "sol", "xrp"]
    out: list[str] = []
    seen: set[str] = set()
    for raw in markets:
        token = str(raw or "").strip().lower()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out or ["btc", "eth", "sol", "xrp"]


def _format_existing_inventory_lines() -> list[str]:
    groups: dict[str, list[str]] = {}
    for feature in feature_registry().values():
        groups.setdefault(str(feature.group), []).append(str(feature.name))
    lines: list[str] = []
    for group in sorted(groups):
        names = sorted(groups[group])
        shown = names[:12]
        suffix = f",...(+{len(names) - len(shown)})" if len(names) > len(shown) else ""
        lines.append(f"- {group}: {','.join(shown)}{suffix}")
    return lines


def _repo_display_path(root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.resolve())
