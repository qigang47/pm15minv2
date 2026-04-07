from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from pm15min.core.layout import rewrite_root
from pm15min.research.features.registry import feature_group, feature_registry, feature_set_columns
from pm15min.research.layout import slug_token


_EXPLAINABILITY_WEIGHTS = {
    "top_lgb_importance": 1.0,
    "top_logreg_coefficients": 1.0,
    "top_positive_factors": 0.75,
    "top_negative_factors": 0.8,
}

_FAMILY_ORDER = ("strike", "cycle", "volume", "price_vol", "context")
_FAMILY_LIMITS = {
    "strike": (6, 10),
    "cycle": (6, 10),
    "volume": (6, 10),
    "price_vol": (8, 12),
    "context": (0, 4),
}


def rank_focus_features(summary_payloads: list[dict[str, Any]]) -> list[str]:
    return _rank_focus_features(summary_payloads).get("all", [])


def rank_focus_features_by_offset(summary_payloads: list[dict[str, Any]]) -> dict[int, list[str]]:
    ranked = _rank_focus_features(summary_payloads)
    return {
        int(offset): features
        for offset, features in ranked.items()
        if offset != "all"
    }


def _rank_focus_features(summary_payloads: list[dict[str, Any]]) -> dict[int | str, list[str]]:
    known_features = set(feature_registry())
    score_by_key: dict[int | str, Counter[str]] = {"all": Counter()}
    first_seen_by_key: dict[int | str, dict[str, int]] = {"all": {}}
    seen_order_by_key: dict[int | str, int] = {"all": 0}
    for payload in summary_payloads:
        offset_value = payload.get("offset")
        offset_key = int(offset_value) if offset_value is not None else None
        if offset_key is not None and offset_key not in score_by_key:
            score_by_key[offset_key] = Counter()
            first_seen_by_key[offset_key] = {}
            seen_order_by_key[offset_key] = 0
        explainability = dict(payload.get("explainability") or {})
        for key, weight in _EXPLAINABILITY_WEIGHTS.items():
            rows = list(explainability.get(key) or ())
            for index, row in enumerate(rows, start=1):
                feature = str((row or {}).get("feature") or "").strip()
                if not feature or feature not in known_features:
                    continue
                increment = max(0.0, 11.0 - float(index)) * float(weight)
                for score_key in ("all", offset_key):
                    if score_key is None:
                        continue
                    first_seen = first_seen_by_key[score_key]
                    if feature not in first_seen:
                        first_seen[feature] = seen_order_by_key[score_key]
                        seen_order_by_key[score_key] += 1
                    score_by_key[score_key][feature] += increment
    out: dict[int | str, list[str]] = {}
    for score_key, score in score_by_key.items():
        first_seen = first_seen_by_key[score_key]
        out[score_key] = sorted(
            score,
            key=lambda item: (-float(score[item]), int(first_seen[item]), str(item)),
        )
    return out


def build_market_focus_feature_sets(
    *,
    market: str,
    global_ranked_features: list[str],
    market_ranked_features: list[str],
    market_offset_ranked_features: dict[int, list[str]] | None = None,
    global_offset_ranked_features: dict[int, list[str]] | None = None,
    widths: tuple[int, ...] = (12, 18, 24, 32),
    fill_candidates: list[str] | None = None,
    version: str = "v1",
) -> dict[str, dict[str, object]]:
    known_features = set(feature_registry())
    market_ranked = _dedupe_keep_order(market_ranked_features, known_features=known_features)
    fill_ranked = _dedupe_keep_order(fill_candidates or [], known_features=known_features)
    market_offset_ranked = {
        int(offset): _dedupe_keep_order(features, known_features=known_features)
        for offset, features in dict(market_offset_ranked_features or {}).items()
    }
    global_offset_ranked = {
        int(offset): _dedupe_keep_order(features, known_features=known_features)
        for offset, features in dict(global_offset_ranked_features or {}).items()
    }
    widths_norm = sorted({max(1, int(width)) for width in widths})
    bucket_candidates = _build_bucket_candidates(
        market_ranked=market_ranked,
        global_ranked=_dedupe_keep_order(global_ranked_features, known_features=known_features),
        market_offset_ranked=market_offset_ranked,
        global_offset_ranked=global_offset_ranked,
        fill_ranked=fill_ranked,
    )
    out: dict[str, dict[str, object]] = {}
    for width in widths_norm:
        family_targets = _rebalance_family_targets(
            bucket_candidates=bucket_candidates,
            family_targets=_allocate_family_targets(width),
            width=width,
        )
        columns = _select_bucketed_columns(bucket_candidates=bucket_candidates, family_targets=family_targets, width=width)
        name = f"focus_{slug_token(market)}_{int(width)}_{slug_token(version, default='v1')}"
        out[name] = {
            "market": slug_token(market),
            "width": int(width),
            "columns": columns[:width],
            "notes": (
                f"focus search width={int(width)} built from family buckets plus {slug_token(market)} tilt"
                f" ({slug_token(version, default='v1')})"
            ),
        }
    return out


def load_baseline_direction_summary_payloads(market: str) -> list[dict[str, Any]]:
    market_token = slug_token(market)
    bundle_dir = (
        Path(rewrite_root())
        / "research"
        / "model_bundles"
        / "cycle=15m"
        / f"asset={market_token}"
        / "profile=deep_otm_baseline"
        / "target=direction"
        / f"bundle=unified_truth0328_{market_token}_baseline_20260328"
    )
    payloads: list[dict[str, Any]] = []
    for offset in (7, 8, 9):
        summary_path = bundle_dir / f"offsets/offset={offset}/diagnostics/summary.json"
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        payloads.append(payload)
    return payloads


def available_unused_baseline_features() -> list[str]:
    baseline = set(feature_set_columns("bs_q_replace_direction"))
    return sorted(feature for feature in feature_registry() if feature not in baseline)


def _dedupe_keep_order(features: list[str], *, known_features: set[str]) -> list[str]:
    out: list[str] = []
    for feature in features:
        token = str(feature).strip()
        if not token or token not in known_features or token in out:
            continue
        out.append(token)
    return out


def _feature_family(feature_name: str) -> str:
    group = feature_group(feature_name)
    if group == "strike":
        return "strike"
    if group == "cycle":
        return "cycle"
    if group == "volume":
        return "volume"
    if group == "price":
        return "price_vol"
    return "context"


def _round_robin_feature_lists(feature_lists: list[list[str]]) -> list[str]:
    out: list[str] = []
    max_len = max((len(items) for items in feature_lists), default=0)
    for index in range(max_len):
        for items in feature_lists:
            if index >= len(items):
                continue
            feature = items[index]
            if feature not in out:
                out.append(feature)
    return out


def _filter_family(features: list[str], family: str) -> list[str]:
    return [feature for feature in features if _feature_family(feature) == family]


def _build_bucket_candidates(
    *,
    market_ranked: list[str],
    global_ranked: list[str],
    market_offset_ranked: dict[int, list[str]],
    global_offset_ranked: dict[int, list[str]],
    fill_ranked: list[str],
) -> dict[str, list[str]]:
    candidates: dict[str, list[str]] = {}
    for family in _FAMILY_ORDER:
        family_sources: list[list[str]] = []
        market_offset_lists = [_filter_family(market_offset_ranked[offset], family) for offset in sorted(market_offset_ranked)]
        if market_offset_lists:
            family_sources.append(_round_robin_feature_lists(market_offset_lists))
        market_list = _filter_family(market_ranked, family)
        if market_list:
            family_sources.append(market_list)
        global_offset_lists = [_filter_family(global_offset_ranked[offset], family) for offset in sorted(global_offset_ranked)]
        if global_offset_lists:
            family_sources.append(_round_robin_feature_lists(global_offset_lists))
        global_list = _filter_family(global_ranked, family)
        if global_list:
            family_sources.append(global_list)
        fill_list = _filter_family(fill_ranked, family)
        if fill_list:
            family_sources.append(fill_list)
        family_candidates = _round_robin_feature_lists(family_sources)
        candidates[family] = family_candidates
    return candidates


def _allocate_family_targets(width: int) -> dict[str, int]:
    counts = {family: limits[0] for family, limits in _FAMILY_LIMITS.items()}
    remaining = int(width) - sum(counts.values())
    while remaining > 0:
        changed = False
        for family in _FAMILY_ORDER:
            if remaining <= 0:
                break
            _, max_count = _FAMILY_LIMITS[family]
            if counts[family] >= max_count:
                continue
            counts[family] += 1
            remaining -= 1
            changed = True
        if not changed:
            break
    return counts


def _rebalance_family_targets(
    *,
    bucket_candidates: dict[str, list[str]],
    family_targets: dict[str, int],
    width: int,
) -> dict[str, int]:
    counts = {family: int(family_targets.get(family, 0)) for family in _FAMILY_ORDER}
    overflow = 0
    for family in _FAMILY_ORDER:
        available = len(bucket_candidates.get(family, []))
        if counts[family] <= available:
            continue
        overflow += counts[family] - available
        counts[family] = available
    while overflow > 0:
        changed = False
        for family in _FAMILY_ORDER:
            available = len(bucket_candidates.get(family, []))
            _, max_count = _FAMILY_LIMITS[family]
            family_cap = min(int(max_count), int(available))
            if counts[family] >= family_cap:
                continue
            counts[family] += 1
            overflow -= 1
            changed = True
            if overflow <= 0:
                break
        if not changed:
            break
    total = sum(counts.values())
    if total > int(width):
        for family in reversed(_FAMILY_ORDER):
            minimum = min(_FAMILY_LIMITS[family][0], len(bucket_candidates.get(family, [])))
            while counts[family] > minimum and total > int(width):
                counts[family] -= 1
                total -= 1
    return counts


def _select_bucketed_columns(
    *,
    bucket_candidates: dict[str, list[str]],
    family_targets: dict[str, int],
    width: int,
) -> list[str]:
    columns: list[str] = []
    for family in _FAMILY_ORDER:
        target = int(family_targets.get(family, 0))
        for feature in bucket_candidates.get(family, []):
            if feature not in columns:
                columns.append(feature)
            if sum(1 for item in columns if _feature_family(item) == family) >= target:
                break
    if len(columns) < int(width):
        for family in _FAMILY_ORDER:
            for feature in bucket_candidates.get(family, []):
                if feature in columns:
                    continue
                columns.append(feature)
                if len(columns) >= int(width):
                    return columns
    return columns[: int(width)]
