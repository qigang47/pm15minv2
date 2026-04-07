from pm5min.cli import rewrite_pm5min_argv


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


def test_rewrite_pm5min_argv_skips_cycle_minutes_for_live_show_layout() -> None:
    assert rewrite_pm5min_argv(["live", "show-layout", "--market", "sol"]) == [
        "live",
        "show-layout",
        "--market",
        "sol",
        "--profile",
        "deep_otm_5m",
    ]
