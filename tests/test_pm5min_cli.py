import argparse
import json
from pathlib import Path

from pm15min.cli import build_parser as build_pm15min_parser
from pm15min.core.config import LiveConfig
from pm15min.live.runtime import canonical_live_scope
from pm5min.cli import main, rewrite_pm5min_argv


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def _data_cycle_capable_command_paths() -> set[tuple[str, ...]]:
    parser = build_pm15min_parser()
    paths: set[tuple[str, ...]] = set()

    def find_subparsers(target: argparse.ArgumentParser) -> argparse._SubParsersAction | None:
        for action in target._actions:
            if isinstance(action, argparse._SubParsersAction):
                return action
        return None

    root_subparsers = find_subparsers(parser)
    assert root_subparsers is not None
    data_parser = root_subparsers.choices["data"]

    def visit(target: argparse.ArgumentParser, path: tuple[str, ...]) -> None:
        option_strings = {option for action in target._actions for option in action.option_strings}
        if "--cycle" in option_strings and path:
            paths.add(path)
        child_subparsers = find_subparsers(target)
        if child_subparsers is None:
            return
        for token, child in child_subparsers.choices.items():
            visit(child, path + (token,))

    visit(data_parser, ())
    return paths


def test_pm5min_cli_does_not_delegate_to_pm15min_cli() -> None:
    text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "cli.py").read_text(encoding="utf-8")
    assert "from pm15min.cli import main as pm15min_main" not in text
    assert "pm15min." not in text


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
    assert rewrite_pm5min_argv(["data", "show-layout", "--market", "sol"]) == [
        "data",
        "show-layout",
        "--market",
        "sol",
        "--cycle",
        "5m",
    ]
    assert rewrite_pm5min_argv(["data", "run", "live-foundation", "--market", "sol"]) == [
        "data",
        "run",
        "live-foundation",
        "--market",
        "sol",
        "--cycle",
        "5m",
    ]
    assert rewrite_pm5min_argv(["data", "sync", "settlement-truth-rpc", "--market", "sol"]) == [
        "data",
        "sync",
        "settlement-truth-rpc",
        "--market",
        "sol",
        "--cycle",
        "5m",
    ]
    assert rewrite_pm5min_argv(["data", "sync", "legacy-settlement-truth", "--market", "sol"]) == [
        "data",
        "sync",
        "legacy-settlement-truth",
        "--market",
        "sol",
        "--cycle",
        "5m",
    ]
    assert rewrite_pm5min_argv(["data", "build", "truth-15m", "--market", "sol"]) == [
        "data",
        "build",
        "truth-15m",
        "--market",
        "sol",
        "--cycle",
        "5m",
    ]
    assert rewrite_pm5min_argv(["data", "export", "truth-15m", "--market", "sol"]) == [
        "data",
        "export",
        "truth-15m",
        "--market",
        "sol",
        "--cycle",
        "5m",
    ]
    assert rewrite_pm5min_argv(["data", "run", "backfill-direct-oracle", "--market", "sol"]) == [
        "data",
        "run",
        "backfill-direct-oracle",
        "--market",
        "sol",
        "--cycle",
        "5m",
    ]
    assert rewrite_pm5min_argv(["data", "run", "backfill-cycle-labels-gamma", "--markets", "sol"]) == [
        "data",
        "run",
        "backfill-cycle-labels-gamma",
        "--markets",
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


def test_rewrite_pm5min_argv_does_not_inject_cycle_for_console_or_cycleless_data_commands() -> None:
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


def test_rewrite_pm5min_argv_covers_every_cycle_capable_data_command() -> None:
    for path in sorted(_data_cycle_capable_command_paths()):
        rewritten = rewrite_pm5min_argv(["data", *path])
        assert "--cycle" in rewritten, path


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


def test_pm5min_data_show_layout_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")

    rc = main(["data", "show-layout", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_pm5min_data_live_foundation_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")
    monkeypatch.setattr(
        "pm15min.data.cli.run_live_data_foundation",
        lambda cfg, **kwargs: {
            "dataset": "live_foundation",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "status": "ok",
        },
    )

    rc = main(["data", "run", "live-foundation", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_pm5min_settlement_truth_rpc_is_reachable_for_5m(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")
    monkeypatch.setattr(
        "pm15min.data.cli.sync_settlement_truth_from_rpc",
        lambda cfg, **kwargs: {
            "dataset": "settlement_truth_rpc",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "rows_imported": 0,
        },
    )

    rc = main(["data", "sync", "settlement-truth-rpc", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_pm5min_legacy_settlement_truth_is_reachable_for_5m(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")
    monkeypatch.setattr(
        "pm15min.data.cli.import_legacy_settlement_truth",
        lambda cfg, source_path=None: {
            "dataset": "settlement_truth",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "source_file": None if source_path is None else str(source_path),
        },
    )

    rc = main(
        [
            "data",
            "sync",
            "legacy-settlement-truth",
            "--market",
            "sol",
            "--source-path",
            str(tmp_path / "settlement.csv"),
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_canonical_live_scope_rejects_non_canonical_cycle_with_canonical_market_and_profile() -> None:
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=5)

    scope = canonical_live_scope(cfg=cfg, target="direction")

    assert scope["market_in_scope"] is True
    assert scope["profile_in_scope"] is True
    assert scope["target_in_scope"] is True
    assert scope["cycle"] == "5m"
    assert scope["cycle_in_scope"] is False
    assert scope["ok"] is False
