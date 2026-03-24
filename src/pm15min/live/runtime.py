from __future__ import annotations

from pm15min.core.config import LiveConfig
from .profiles import resolve_live_profile_spec
from pm15min.research.config import ResearchConfig
from pm15min.research.service import get_active_bundle_selection


CANONICAL_LIVE_MARKETS = ("sol", "xrp")
CANONICAL_LIVE_PROFILE = "deep_otm"
CANONICAL_LIVE_TARGET = "direction"
CANONICAL_LIVE_CYCLE = "15m"
CANONICAL_OPERATOR_ENTRY_COMMANDS = (
    "live check-trading-gateway",
    "live show-ready",
    "live show-latest-runner",
)


def describe_live_runtime(cfg: LiveConfig) -> dict[str, object]:
    active = live_active_direction_bundle(cfg)
    profile_spec = resolve_live_profile_spec(cfg.profile)
    canonical_scope = canonical_live_scope(cfg=cfg, target=CANONICAL_LIVE_TARGET)
    profile_resolution = describe_live_profile_resolution(cfg=cfg, profile_spec=profile_spec)
    return {
        "domain": "live",
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle_minutes": cfg.cycle_minutes,
        "loop": cfg.loop,
        "refresh_interval_minutes": cfg.refresh_interval_minutes,
        "decision_poll_interval_sec": cfg.decision_poll_interval_sec,
        "layout": cfg.layout.to_dict(),
        "profile_spec": profile_spec.to_dict(),
        "profile_spec_resolution": profile_resolution,
        "canonical_live_scope": canonical_scope,
        "cli_boundary": describe_live_cli_boundary(
            command="show-layout",
            canonical_scope=canonical_scope,
            profile_resolution=profile_resolution,
        ),
        "active_direction_bundle_selection_path": active["selection_path"],
        "active_direction_bundle": active["selection"],
    }


def describe_live_config(cfg: LiveConfig) -> dict[str, object]:
    payload = cfg.to_dict()
    active = live_active_direction_bundle(cfg)
    profile_spec = resolve_live_profile_spec(cfg.profile)
    canonical_scope = canonical_live_scope(cfg=cfg, target=CANONICAL_LIVE_TARGET)
    profile_resolution = describe_live_profile_resolution(cfg=cfg, profile_spec=profile_spec)
    payload["profile_spec"] = profile_spec.to_dict()
    payload["profile_spec_resolution"] = profile_resolution
    payload["canonical_live_scope"] = canonical_scope
    payload["cli_boundary"] = describe_live_cli_boundary(
        command="show-config",
        canonical_scope=canonical_scope,
        profile_resolution=profile_resolution,
    )
    payload["active_direction_bundle_selection_path"] = active["selection_path"]
    payload["active_direction_bundle"] = active["selection"]
    return payload


def live_active_direction_bundle(cfg: LiveConfig) -> dict[str, object]:
    research_cfg = ResearchConfig.build(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
        target="direction",
        root=cfg.layout.rewrite.root,
    )
    return get_active_bundle_selection(research_cfg, profile=cfg.profile, target="direction")


def canonical_live_scope(*, cfg: LiveConfig, target: str) -> dict[str, object]:
    market = str(cfg.asset.slug)
    profile = str(cfg.profile)
    normalized_target = str(target or CANONICAL_LIVE_TARGET).strip().lower()
    return {
        "market": market,
        "profile": profile,
        "target": normalized_target,
        "market_in_scope": market in CANONICAL_LIVE_MARKETS,
        "profile_in_scope": profile == CANONICAL_LIVE_PROFILE,
        "target_in_scope": normalized_target == CANONICAL_LIVE_TARGET,
        "ok": (
            market in CANONICAL_LIVE_MARKETS
            and profile == CANONICAL_LIVE_PROFILE
            and normalized_target == CANONICAL_LIVE_TARGET
        ),
    }


def describe_live_profile_resolution(*, cfg: LiveConfig, profile_spec) -> dict[str, object]:
    requested_profile = str(cfg.profile or "").strip()
    resolved_profile = str(profile_spec.profile or "").strip()
    exact_match = requested_profile.lower() == resolved_profile.lower()
    if exact_match:
        return {
            "requested_profile": requested_profile,
            "resolved_profile_spec": resolved_profile,
            "exact_match": True,
            "status": "exact_match",
            "reason": None,
        }
    return {
        "requested_profile": requested_profile,
        "resolved_profile_spec": resolved_profile,
        "exact_match": False,
        "status": "compatibility_fallback",
        "reason": (
            f"requested profile={requested_profile or '<missing>'} is outside the live profile registry; "
            f"profile_spec falls back to {resolved_profile or '<missing>'}"
        ),
    }


def canonical_live_contract() -> dict[str, object]:
    return {
        "markets": list(CANONICAL_LIVE_MARKETS),
        "profile": CANONICAL_LIVE_PROFILE,
        "target": CANONICAL_LIVE_TARGET,
        "cycle": CANONICAL_LIVE_CYCLE,
    }


def describe_live_cli_boundary(
    *,
    command: str,
    canonical_scope: dict[str, object],
    profile_resolution: dict[str, object],
) -> dict[str, object]:
    requested_scope_classification = "canonical_live_scope" if bool(canonical_scope.get("ok")) else "non_canonical_scope"
    notes = [
        f"live {command} is a compatibility inspection command, not a canonical operator entry",
        (
            "use live check-trading-gateway, live show-ready, and live show-latest-runner "
            "for canonical operator reads"
        ),
    ]
    if requested_scope_classification == "non_canonical_scope":
        notes.append("requested market/profile falls outside canonical live scope")
    if str(profile_resolution.get("status") or "") == "compatibility_fallback":
        notes.append(
            "requested profile is outside the live profile registry and currently resolves through a fallback profile_spec"
        )
    return {
        "command": command,
        "command_role": "compatibility_inspection",
        "canonical_operator_entry": False,
        "supports_non_canonical_markets": True,
        "supports_non_canonical_profiles": True,
        "requested_scope_classification": requested_scope_classification,
        "canonical_live_contract": canonical_live_contract(),
        "recommended_operator_entries": list(CANONICAL_OPERATOR_ENTRY_COMMANDS),
        "notes": notes,
    }
