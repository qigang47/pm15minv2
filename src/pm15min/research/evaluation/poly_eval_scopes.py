from __future__ import annotations

POLY_EVAL_ROUTED_SCOPES = frozenset(
    {
        "abm",
        "deep_otm",
        "smc",
        "copula_risk",
        "production_stack",
    }
)

_POLY_EVAL_SCOPE_ALIASES = {
    "abm_demo": "abm",
    "abm-demo": "abm",
    "abm": "abm",
    "deep_otm_demo": "deep_otm",
    "deep-otm-demo": "deep_otm",
    "deep_otm": "deep_otm",
    "smc_demo": "smc",
    "smc-demo": "smc",
    "smc": "smc",
    "copula_risk": "copula_risk",
    "copula-risk": "copula_risk",
    "production_stack": "production_stack",
    "stack": "production_stack",
    "stack_demo": "production_stack",
    "stack-demo": "production_stack",
}


def normalize_poly_eval_scope(scope: object) -> str:
    token = str(scope or "").strip().lower().replace("-", "_")
    return _POLY_EVAL_SCOPE_ALIASES.get(token, token)


def resolve_poly_eval_alias_scope(command: object) -> str | None:
    normalized = normalize_poly_eval_scope(command)
    return normalized if normalized in POLY_EVAL_ROUTED_SCOPES else None
