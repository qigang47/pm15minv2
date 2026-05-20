from __future__ import annotations

from typing import Any


REQUIRED_LEVER_NONE = "none"


def choose_required_next_lever(attempts: list[dict[str, Any]]) -> dict[str, Any]:
    extended_recent = [dict(item) for item in attempts[:5] if isinstance(item, dict)]
    extended_sparse = [item for item in extended_recent if str(item.get("outcome") or "") in {"sparse", "no_capture"}]
    if len(extended_sparse) >= 4:
        return _decision(
            "search_space_rework",
            "four recent sparse attempts still failed after ordinary branch changes",
            forbid_same_width=True,
            forbid_same_model=True,
            forbid_same_family=True,
        )

    recent = extended_recent[:3]
    sparse = [item for item in recent if str(item.get("outcome") or "") in {"sparse", "no_capture"}]
    if len(sparse) < 3:
        return _decision(REQUIRED_LEVER_NONE, "not enough repeated sparse attempts")

    widths = {_first_value(item.get("widths")) for item in sparse}
    models = {_first_value(item.get("model_families")) for item in sparse}
    bottlenecks = {str(item.get("primary_bottleneck") or "").strip() for item in sparse}

    if bottlenecks == {"probability_gate"} and len(models) == 1:
        return _decision(
            "model_family",
            "three sparse probability-gate attempts used the same model",
            forbid_same_model=True,
        )
    if len(widths) == 1:
        return _decision(
            "feature_width",
            "three sparse attempts used the same width",
            forbid_same_width=True,
        )
    if len(models) == 1:
        return _decision(
            "factor_family",
            "three sparse attempts used the same model and no width-only move is obvious",
        )
    return _decision("factor_family", "three sparse attempts did not produce a frontier")


def validate_candidate_against_policy(
    candidate: dict[str, Any],
    attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    decision = choose_required_next_lever(attempts)
    required = str(decision.get("required_lever") or REQUIRED_LEVER_NONE).strip()
    if required == REQUIRED_LEVER_NONE:
        return _validation(True, "", decision)

    latest_sparse = [
        item
        for item in attempts[:5]
        if str(item.get("outcome") or "") in {"sparse", "no_capture"}
    ]
    if not latest_sparse:
        return _validation(True, "", decision)

    candidate_widths = _normalized_values(candidate.get("widths"))
    candidate_models = _normalized_values(candidate.get("model_families"))
    candidate_families = _normalized_values(
        candidate.get("factor_family_signature") or candidate.get("factor_families")
    )
    previous_widths = _normalized_values(latest_sparse[0].get("widths"))
    previous_models = _normalized_values(latest_sparse[0].get("model_families"))
    previous_families = _normalized_values(
        latest_sparse[0].get("factor_family_signature") or latest_sparse[0].get("factor_families")
    )

    blockers: list[str] = []
    if decision.get("forbid_same_width") and _same_nonempty(candidate_widths, previous_widths):
        blockers.append("feature_width")
    if decision.get("forbid_same_model") and _same_nonempty(candidate_models, previous_models):
        blockers.append("model_family")
    if decision.get("forbid_same_family") and _same_nonempty(candidate_families, previous_families):
        blockers.append("factor_family")
    if required == "feature_width" and _same_nonempty(candidate_widths, previous_widths):
        blockers.append("feature_width")
    if required == "model_family" and _same_nonempty(candidate_models, previous_models):
        blockers.append("model_family")
    if required == "factor_family" and _same_nonempty(candidate_families, previous_families):
        blockers.append("factor_family")

    unique_blockers = sorted(set(blockers))
    if not unique_blockers:
        return _validation(True, "", decision)
    return _validation(
        False,
        (
            f"required {required} change after repeated sparse attempts; "
            f"candidate repeated {','.join(unique_blockers)}"
        ),
        decision,
    )


def format_policy_decision_line(market: str, decision: dict[str, Any]) -> str:
    return (
        f"{market}: required_next_lever={decision.get('required_lever')} / "
        f"reason={decision.get('reason')} / "
        f"forbid_same_width={int(bool(decision.get('forbid_same_width')))} / "
        f"forbid_same_model={int(bool(decision.get('forbid_same_model')))} / "
        f"forbid_same_family={int(bool(decision.get('forbid_same_family')))}"
    )


def _decision(
    required_lever: str,
    reason: str,
    *,
    forbid_same_width: bool = False,
    forbid_same_model: bool = False,
    forbid_same_family: bool = False,
) -> dict[str, Any]:
    return {
        "required_lever": required_lever,
        "reason": reason,
        "forbid_same_width": bool(forbid_same_width),
        "forbid_same_model": bool(forbid_same_model),
        "forbid_same_family": bool(forbid_same_family),
    }


def _validation(allowed: bool, reason: str, decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "allowed": bool(allowed),
        "reason": str(reason or "").strip(),
        "decision": dict(decision),
    }


def _first_value(value: Any) -> str:
    if isinstance(value, list) and value:
        return str(value[0]).strip()
    return str(value or "").strip()


def _normalized_values(value: Any) -> tuple[str, ...]:
    if isinstance(value, (list, tuple, set)):
        return tuple(sorted(str(item).strip() for item in value if str(item).strip()))
    token = str(value or "").strip()
    return (token,) if token else ()


def _same_nonempty(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    return bool(left) and bool(right) and left == right
