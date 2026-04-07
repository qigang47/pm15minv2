import json

from pm15min.core.config import LiveConfig
from pm15min.live.runtime import canonical_live_scope
from pm5min.cli import main, rewrite_pm5min_argv


def test_rewrite_pm5min_argv_injects_5m_defaults() -> None:
    assert rewrite_pm5min_argv(["layout", "--market", "sol"]) == [
        "layout",
        "--market",
        "sol",
        "--cycle",
        "5m",
    ]
    assert rewrite_pm5min_argv(["research", "show-layout", "--market", "sol"]) == [
        "research",
        "show-layout",
        "--market",
        "sol",
        "--cycle",
        "5m",
    ]
    assert rewrite_pm5min_argv(["live", "show-config", "--market", "sol"]) == [
        "live",
        "show-config",
        "--market",
        "sol",
        "--cycle-minutes",
        "5",
        "--profile",
        "deep_otm_5m",
    ]
    assert rewrite_pm5min_argv(
        ["live", "show-config", "--market", "sol", "--profile", "custom_5m"]
    ) == [
        "live",
        "show-config",
        "--market",
        "sol",
        "--profile",
        "custom_5m",
        "--cycle-minutes",
        "5",
    ]


def test_rewrite_pm5min_argv_does_not_inject_cycle_for_console_or_data() -> None:
    assert rewrite_pm5min_argv(["console", "show-home"]) == [
        "console",
        "show-home",
    ]
    assert rewrite_pm5min_argv(
        ["data", "sync", "streams-rpc", "--market", "sol", "--surface", "live"]
    ) == [
        "data",
        "sync",
        "streams-rpc",
        "--market",
        "sol",
        "--surface",
        "live",
    ]


def test_rewrite_pm5min_argv_respects_equals_form_values() -> None:
    assert rewrite_pm5min_argv(["layout", "--market", "sol", "--cycle=15m"]) == [
        "layout",
        "--market",
        "sol",
        "--cycle=15m",
    ]
    assert rewrite_pm5min_argv(
        ["live", "show-config", "--market", "sol", "--profile=custom_5m"]
    ) == [
        "live",
        "show-config",
        "--market",
        "sol",
        "--profile=custom_5m",
        "--cycle-minutes",
        "5",
    ]


def test_rewrite_pm5min_argv_injects_cycle_minutes_for_live_show_layout() -> None:
    assert rewrite_pm5min_argv(["live", "show-layout", "--market", "sol"]) == [
        "live",
        "show-layout",
        "--market",
        "sol",
        "--cycle-minutes",
        "5",
        "--profile",
        "deep_otm_5m",
    ]


def test_pm5min_live_show_layout_uses_5m_profile_and_cycle(capsys) -> None:
    rc = main(["live", "show-layout", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle_minutes"] == 5
    assert payload["profile"] == "deep_otm_5m"
    assert payload["canonical_live_scope"]["cycle"] == "5m"
    assert payload["canonical_live_scope"]["cycle_in_scope"] is False
    assert payload["canonical_live_scope"]["ok"] is False
    assert payload["cli_boundary"]["requested_scope_classification"] == "non_canonical_scope"
    assert payload["cli_boundary"]["canonical_live_contract"]["cycle"] == "15m"
    assert payload["profile_spec_resolution"]["status"] == "exact_match"


def test_canonical_live_scope_rejects_non_canonical_cycle_with_canonical_market_and_profile() -> None:
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=5)

    scope = canonical_live_scope(cfg=cfg, target="direction")

    assert scope["market_in_scope"] is True
    assert scope["profile_in_scope"] is True
    assert scope["target_in_scope"] is True
    assert scope["cycle"] == "5m"
    assert scope["cycle_in_scope"] is False
    assert scope["ok"] is False
