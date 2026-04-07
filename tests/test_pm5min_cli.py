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
