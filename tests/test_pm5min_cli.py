import argparse
import ast
import json
from pathlib import Path

from pm15min.cli import build_parser as build_pm15min_parser
from pm15min.core.config import LiveConfig
from pm15min.live.runtime import canonical_live_scope
from pm5min.cli import build_parser as build_pm5min_parser
from pm5min.cli import main, rewrite_pm5min_argv


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm5min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm5min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm5min.research.layout.rewrite_root", lambda: root)


def _forbid_pm15min_data_handler_delegation(monkeypatch) -> None:
    def _raise_if_called(*args, **kwargs):
        raise AssertionError("pm5min data command delegated to pm15min.data.cli.handlers.run_data_command")

    monkeypatch.setattr("pm15min.data.cli.handlers.run_data_command", _raise_if_called)


def _forbid_pm15min_research_handler_delegation(monkeypatch) -> None:
    def _raise_if_called(*args, **kwargs):
        raise AssertionError(
            "pm5min research command delegated to pm15min.research.cli_handlers.run_research_command"
        )

    monkeypatch.setattr("pm15min.research.cli_handlers.run_research_command", _raise_if_called)


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


def _pm5min_console_module_path(name: str) -> Path:
    return Path(__file__).resolve().parents[1] / "src" / "pm5min" / "console" / name


def _relative_imported_names(module_path: Path, *, module: str) -> set[str]:
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.level == 1 and node.module == module:
            names.update(alias.name for alias in node.names)
    return names


def test_pm5min_cli_does_not_delegate_to_pm15min_cli() -> None:
    text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "cli.py").read_text(encoding="utf-8")
    assert "from pm15min.cli import main as pm15min_main" not in text
    assert "pm15min.data.layout" not in text
    assert "pm15min.data.cli" not in text
    assert "pm15min.live.cli" not in text
    assert "pm15min.research.cli" not in text
    assert "pm15min.console.cli" not in text

    data_cli_text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "data" / "cli.py").read_text(
        encoding="utf-8"
    )
    assert "pm15min.data.cli" not in data_cli_text

    data_handlers_text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "data" / "handlers.py").read_text(
        encoding="utf-8"
    )
    assert "pm15min.data.cli" not in data_handlers_text

    data_compat_text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "data" / "compat.py").read_text(
        encoding="utf-8"
    )
    assert "pm15min.data.cli" not in data_compat_text
    assert "pm15min.data.pipelines.truth" not in data_compat_text
    assert "pm15min.data.pipelines.oracle_prices" not in data_compat_text
    assert "pm15min.data.pipelines.export_tables" not in data_compat_text
    assert "pm15min.data.pipelines.market_catalog" not in data_compat_text
    assert "pm15min.data.pipelines.binance_klines" not in data_compat_text
    assert "pm15min.data.pipelines.source_ingest" not in data_compat_text
    assert "pm15min.data.pipelines.direct_sync" not in data_compat_text
    assert "pm15min.data.pipelines.direct_oracle_prices" not in data_compat_text
    assert "pm15min.data.pipelines.orderbook_recording" not in data_compat_text
    assert "pm15min.data.pipelines.orderbook_runtime" not in data_compat_text
    assert "pm15min.data.pipelines.orderbook_fleet" not in data_compat_text
    assert "pm15min.data.pipelines.backtest_refresh" not in data_compat_text
    assert "pm15min.data.pipelines.foundation_runtime" not in data_compat_text

    live_cli_text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "live" / "cli.py").read_text(
        encoding="utf-8"
    )
    assert "pm15min.live.cli" not in live_cli_text

    live_runtime_text = (
        Path(__file__).resolve().parents[1] / "src" / "pm5min" / "live" / "runtime.py"
    ).read_text(encoding="utf-8")
    assert "pm15min.live.runtime" not in live_runtime_text

    live_compat_text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "live" / "compat.py").read_text(
        encoding="utf-8"
    )
    assert "pm15min.live.runtime" not in live_compat_text

    research_cli_text = (
        Path(__file__).resolve().parents[1] / "src" / "pm5min" / "research" / "cli.py"
    ).read_text(encoding="utf-8")
    assert "pm15min.research.cli" not in research_cli_text
    assert "attach_pm15min_research_subcommands" not in research_cli_text

    research_parser_text = (
        Path(__file__).resolve().parents[1] / "src" / "pm5min" / "research" / "parser.py"
    ).read_text(encoding="utf-8")
    assert "pm15min.research.cli_parser" not in research_parser_text
    assert "pm15min.research.cli_args" not in research_parser_text

    console_cli_text = (
        Path(__file__).resolve().parents[1] / "src" / "pm5min" / "console" / "cli.py"
    ).read_text(encoding="utf-8")
    assert "pm15min.console.cli" not in console_cli_text
    assert "attach_pm15min_console_subcommands" not in console_cli_text

    console_parser_text = (
        Path(__file__).resolve().parents[1] / "src" / "pm5min" / "console" / "parser.py"
    ).read_text(encoding="utf-8")
    assert "pm15min.console.cli" not in console_parser_text

def test_pm5min_data_service_module_does_not_delegate_to_pm15min_service() -> None:
    text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "data" / "service.py").read_text(
        encoding="utf-8"
    )
    assert "from .compat import" not in text
    assert "pm15min.data.service" not in text


def test_pm5min_research_service_module_does_not_delegate_to_pm15min_service() -> None:
    text = (Path(__file__).resolve().parents[1] / "src" / "pm5min" / "research" / "service.py").read_text(
        encoding="utf-8"
    )
    assert "from .compat import" not in text
    assert "pm15min.research.service" not in text


def test_pm5min_console_read_models_do_not_delegate_to_pm15min_modules() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "pm5min" / "console" / "read_models"

    data_text = (root / "data_overview.py").read_text(encoding="utf-8")
    assert "pm15min.data.service" not in data_text
    assert "pm15min.data." not in data_text

    training_text = (root / "training_runs.py").read_text(encoding="utf-8")
    assert "pm15min.research.service" not in training_text
    assert "pm15min.research." not in training_text

    bundles_text = (root / "bundles.py").read_text(encoding="utf-8")
    assert "pm15min.research.service" not in bundles_text
    assert "pm15min.research." not in bundles_text

    for path in root.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "pm15min.console." not in text

    console_root = root.parent
    for name in ("actions.py", "tasks.py"):
        module_path = console_root / name
        assert module_path.exists()
        module_text = module_path.read_text(encoding="utf-8")
        assert "pm15min.console.service" not in module_text


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


def test_pm5min_research_parser_uses_5m_defaults() -> None:
    parser = build_pm5min_parser("research")

    show_bundle_args = parser.parse_args(["research", "show-active-bundle"])
    assert show_bundle_args.cycle == "5m"
    assert show_bundle_args.profile == "deep_otm_5m"

    bundle_build_args = parser.parse_args(["research", "bundle", "build"])
    assert bundle_build_args.cycle == "5m"
    assert bundle_build_args.profile == "deep_otm_5m"
    assert bundle_build_args.offsets == "2,3,4"


def test_pm5min_console_parser_uses_5m_defaults() -> None:
    parser = build_pm5min_parser("console")

    show_data_args = parser.parse_args(["console", "show-data-overview"])
    assert show_data_args.cycle == "5m"

    show_bundle_args = parser.parse_args(["console", "show-bundle", "--bundle-label", "demo"])
    assert show_bundle_args.cycle == "5m"
    assert show_bundle_args.profile == "deep_otm_5m"


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


def test_pm5min_live_show_config_uses_pm5min_layout_root(capsys, monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    monkeypatch.setattr("pm5min.core.layout.rewrite_root", lambda: root)

    rc = main(["live", "show-config", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle_minutes"] == 5
    assert payload["profile"] == "deep_otm_5m"
    assert Path(payload["layout"]["rewrite_root"]) == root


def test_pm5min_research_show_layout_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    monkeypatch.setattr("pm5min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm5min.research.layout.rewrite_root", lambda: root)

    rc = main(["research", "show-layout", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["cycle"] == "5m"
    assert Path(payload["research_root"]).parent == root


def test_pm5min_research_show_config_uses_pm5min_layout_root(capsys, monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    monkeypatch.setattr("pm5min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm5min.research.layout.rewrite_root", lambda: root)

    rc = main(["research", "show-config", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["cycle"] == "5m"
    assert Path(payload["layout"]["rewrite_root"]) == root


def test_pm5min_research_layout_supports_training_runner_signature(tmp_path: Path) -> None:
    from pm5min.research.config import ResearchConfig

    cfg = ResearchConfig.build(market="sol", cycle="5m", profile="deep_otm_5m", root=tmp_path)
    run_dir = cfg.layout.training_run_dir(
        model_family="deep_otm",
        target="direction",
        run_label_text="demo",
    )

    assert str(run_dir).endswith("run=demo")


def test_pm5min_research_defaults_to_5m_when_cycle_omitted(tmp_path: Path) -> None:
    from pm5min.research.config import ResearchConfig
    from pm5min.research.layout import ResearchLayout

    cfg = ResearchConfig.build(market="sol", profile="deep_otm_5m", root=tmp_path)
    layout = ResearchLayout.discover(root=tmp_path).for_market("sol")

    assert cfg.cycle == "5m"
    assert cfg.layout.cycle == "5m"
    assert "cycle=5m" in str(cfg.layout.training_runs_root)
    assert layout.cycle == "5m"
    assert "cycle=5m" in str(layout.training_runs_root)


def test_pm5min_research_layout_supports_bundle_builder_signature(tmp_path: Path) -> None:
    from pm5min.research.config import ResearchConfig

    cfg = ResearchConfig.build(market="sol", cycle="5m", profile="deep_otm_5m", root=tmp_path)
    bundle_dir = cfg.layout.bundle_dir(
        profile="deep_otm_5m",
        target="direction",
        bundle_label_text="demo",
    )

    assert str(bundle_dir).endswith("bundle=demo")


def test_pm5min_research_layout_supports_backtest_runner_signature(tmp_path: Path) -> None:
    from pm5min.research.config import ResearchConfig

    cfg = ResearchConfig.build(
        market="sol",
        cycle="5m",
        profile="deep_otm_5m",
        target="reversal",
        root=tmp_path,
    )
    run_dir = cfg.layout.backtest_run_dir(
        profile="deep_otm_5m",
        spec_name="baseline_truth",
        run_label_text="demo",
    )

    assert "target=reversal" in str(run_dir)
    assert str(run_dir).endswith("run=demo")


def test_pm5min_research_layout_prefers_nonempty_runner_label_alias(tmp_path: Path) -> None:
    from pm5min.research.config import ResearchConfig

    cfg = ResearchConfig.build(
        market="sol",
        cycle="5m",
        profile="deep_otm_5m",
        target="reversal",
        root=tmp_path,
    )
    training_run_dir = cfg.layout.training_run_dir(
        model_family="deep_otm",
        target="direction",
        run_label="",
        run_label_text="demo-training",
    )
    backtest_run_dir = cfg.layout.backtest_run_dir(
        profile="deep_otm_5m",
        spec_name="baseline_truth",
        run_label="",
        run_label_text="demo-backtest",
    )

    assert str(training_run_dir).endswith("run=demo-training")
    assert "target=reversal" in str(backtest_run_dir)
    assert str(backtest_run_dir).endswith("run=demo-backtest")


def test_pm5min_research_list_runs_uses_5m_defaults(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm5min.research.handlers.list_training_runs",
        lambda cfg, model_family=None, target=None, prefix=None: [
            {
                "market": cfg.asset.slug,
                "cycle": cfg.cycle,
                "model_family": model_family,
                "target": target,
                "prefix": prefix,
            }
        ],
    )

    rc = main(["research", "list-runs", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["cycle"] == "5m"


def test_pm5min_research_list_bundles_uses_5m_defaults(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm5min.research.handlers.list_model_bundles",
        lambda cfg, profile=None, target=None, prefix=None: [
            {
                "market": cfg.asset.slug,
                "cycle": cfg.cycle,
                "profile": profile,
                "target": target,
                "prefix": prefix,
            }
        ],
    )

    rc = main(["research", "list-bundles", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["cycle"] == "5m"


def test_pm5min_research_show_active_bundle_uses_5m_profile_and_cycle(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm5min.research.handlers.get_active_bundle_selection",
        lambda cfg, profile=None, target=None: {
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "profile": profile,
            "target": target,
        },
    )

    rc = main(["research", "show-active-bundle", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["profile"] == "deep_otm_5m"


def test_pm5min_research_activate_bundle_uses_5m_profile_and_cycle(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm5min.research.handlers.activate_model_bundle",
        lambda cfg, profile, target, bundle_label=None, notes=None: {
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "profile": profile,
            "target": target,
            "bundle_label": bundle_label,
            "notes": notes,
        },
    )

    rc = main(["research", "activate-bundle", "--market", "sol", "--bundle-label", "demo"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["profile"] == "deep_otm_5m"
    assert payload["bundle_label"] == "demo"


def test_pm5min_research_list_runs_stays_local_and_does_not_delegate_to_pm15min_handlers(
    capsys, monkeypatch
) -> None:
    _forbid_pm15min_research_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm5min.research.handlers.list_training_runs",
        lambda cfg, model_family=None, target=None, prefix=None: [
            {
                "dataset": "training_runs",
                "market": cfg.asset.slug,
                "cycle": cfg.cycle,
                "model_family": model_family,
                "target": target,
                "prefix": prefix,
            }
        ],
    )

    rc = main(["research", "list-runs", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["dataset"] == "training_runs"
    assert payload[0]["cycle"] == "5m"


def test_pm5min_research_list_bundles_stays_local_and_does_not_delegate_to_pm15min_handlers(
    capsys, monkeypatch
) -> None:
    _forbid_pm15min_research_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm5min.research.handlers.list_model_bundles",
        lambda cfg, profile=None, target=None, prefix=None: [
            {
                "dataset": "model_bundles",
                "market": cfg.asset.slug,
                "cycle": cfg.cycle,
                "profile": profile,
                "target": target,
                "prefix": prefix,
            }
        ],
    )

    rc = main(["research", "list-bundles", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["dataset"] == "model_bundles"
    assert payload[0]["cycle"] == "5m"


def test_pm5min_research_show_active_bundle_stays_local_and_does_not_delegate_to_pm15min_handlers(
    capsys, monkeypatch
) -> None:
    _forbid_pm15min_research_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm5min.research.handlers.get_active_bundle_selection",
        lambda cfg, profile=None, target=None: {
            "dataset": "active_bundle_selection",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "profile": profile,
            "target": target,
        },
    )

    rc = main(["research", "show-active-bundle", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "active_bundle_selection"
    assert payload["cycle"] == "5m"
    assert payload["profile"] == "deep_otm_5m"


def test_pm5min_research_activate_bundle_stays_local_and_does_not_delegate_to_pm15min_handlers(
    capsys, monkeypatch
) -> None:
    _forbid_pm15min_research_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm5min.research.handlers.activate_model_bundle",
        lambda cfg, profile, target, bundle_label=None, notes=None: {
            "dataset": "active_bundle_selection",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "profile": profile,
            "target": target,
            "bundle_label": bundle_label,
            "notes": notes,
        },
    )

    rc = main(["research", "activate-bundle", "--market", "sol", "--bundle-label", "demo"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "active_bundle_selection"
    assert payload["cycle"] == "5m"
    assert payload["profile"] == "deep_otm_5m"
    assert payload["bundle_label"] == "demo"


def test_pm5min_research_backtest_run_cli_preserves_reversal_target(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")
    monkeypatch.setattr(
        "pm15min.research.cli.run_research_backtest",
        lambda cfg, spec, dependency_mode="fail_fast": {
            "dataset": "research_backtest",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "target": spec.target,
            "run_dir": str(
                cfg.layout.backtest_run_dir(
                    profile=spec.profile,
                    spec_name=spec.spec_name,
                    run_label_text=spec.run_label,
                )
            ),
        },
    )

    rc = main(
        [
            "research",
            "backtest",
            "run",
            "--market",
            "sol",
            "--profile",
            "deep_otm_5m",
            "--target",
            "reversal",
            "--spec",
            "baseline_truth",
            "--run-label",
            "demo",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["target"] == "reversal"
    assert payload["cycle"] == "5m"
    assert "target=reversal" in payload["run_dir"]


def test_pm5min_console_show_home_still_reachable(capsys) -> None:
    rc = main(["console", "show-home"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert isinstance(payload, dict)


def test_console_read_commands_stay_local(capsys, monkeypatch) -> None:
    handlers_path = _pm5min_console_module_path("handlers.py")
    service_path = _pm5min_console_module_path("service.py")
    compat_path = _pm5min_console_module_path("compat.py")

    service_imports = _relative_imported_names(handlers_path, module="service")
    compat_imports = _relative_imported_names(handlers_path, module="compat")

    assert service_path.exists()
    assert "pm15min.console." not in service_path.read_text(encoding="utf-8")
    assert "pm15min.console.service" not in compat_path.read_text(encoding="utf-8")
    assert {"load_console_data_overview", "load_console_bundle"} <= service_imports
    assert "load_console_data_overview" not in compat_imports
    assert "load_console_bundle" not in compat_imports

    monkeypatch.setattr(
        "pm5min.console.handlers.load_console_data_overview",
        lambda *, market, cycle, surface: {
            "dataset": "console_data_overview",
            "market": market,
            "cycle": cycle,
            "surface": surface,
            "source": "pm5min.console.service",
        },
    )
    monkeypatch.setattr(
        "pm5min.console.handlers.load_console_bundle",
        lambda *, market, cycle, profile, target, bundle_label: {
            "dataset": "console_model_bundle",
            "market": market,
            "cycle": cycle,
            "profile": profile,
            "target": target,
            "bundle_label": bundle_label,
            "source": "pm5min.console.service",
        },
    )
    monkeypatch.setattr(
        "pm15min.console.service.load_console_data_overview",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected pm15min console service call")),
    )
    monkeypatch.setattr(
        "pm15min.console.service.load_console_bundle",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected pm15min console service call")),
    )

    rc = main(["console", "show-data-overview", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_data_overview"
    assert payload["cycle"] == "5m"
    assert payload["source"] == "pm5min.console.service"

    rc = main(["console", "show-bundle", "--market", "sol", "--bundle-label", "demo"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_model_bundle"
    assert payload["cycle"] == "5m"
    assert payload["profile"] == "deep_otm_5m"
    assert payload["source"] == "pm5min.console.service"


def test_show_actions_and_build_action_stay_local(capsys, monkeypatch) -> None:
    handlers_path = _pm5min_console_module_path("handlers.py")
    service_path = _pm5min_console_module_path("service.py")

    service_imports = _relative_imported_names(handlers_path, module="service")
    compat_imports = _relative_imported_names(handlers_path, module="compat")

    assert service_path.exists()
    assert "pm15min.console." not in service_path.read_text(encoding="utf-8")
    assert {"load_console_action_catalog", "build_console_action_request"} <= service_imports
    assert "load_console_action_catalog" not in compat_imports
    assert "build_console_action_request" not in compat_imports

    monkeypatch.setattr(
        "pm5min.console.handlers.load_console_action_catalog",
        lambda: {
            "dataset": "console_action_catalog",
            "action_count": 1,
            "source": "pm5min.console.service",
        },
    )
    monkeypatch.setattr(
        "pm5min.console.handlers.build_console_action_request",
        lambda *, action_id, request: {
            "dataset": "console_action_plan",
            "action_id": action_id,
            "normalized_request": dict(request),
            "source": "pm5min.console.service",
        },
    )
    monkeypatch.setattr(
        "pm15min.console.service.load_console_action_catalog",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected pm15min console service call")),
    )
    monkeypatch.setattr(
        "pm15min.console.service.build_console_action_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected pm15min console service call")),
    )

    rc = main(["console", "show-actions"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_action_catalog"
    assert payload["source"] == "pm5min.console.service"

    rc = main(["console", "build-action", "--action-id", "data_refresh_summary", "--request-json", '{"market":"sol"}'])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_action_plan"
    assert payload["action_id"] == "data_refresh_summary"
    assert payload["normalized_request"]["market"] == "sol"
    assert payload["source"] == "pm5min.console.service"


def test_runtime_and_task_read_commands_stay_local(capsys, monkeypatch) -> None:
    handlers_path = _pm5min_console_module_path("handlers.py")
    service_path = _pm5min_console_module_path("service.py")

    service_imports = _relative_imported_names(handlers_path, module="service")
    compat_imports = _relative_imported_names(handlers_path, module="compat")

    assert service_path.exists()
    assert "pm15min.console." not in service_path.read_text(encoding="utf-8")
    assert {
        "load_console_runtime_state",
        "load_console_runtime_history",
        "list_console_tasks",
        "load_console_task",
    } <= service_imports
    assert "load_console_runtime_state" not in compat_imports
    assert "load_console_runtime_history" not in compat_imports
    assert "list_console_tasks" not in compat_imports
    assert "load_console_task" not in compat_imports

    monkeypatch.setattr(
        "pm5min.console.handlers.load_console_runtime_state",
        lambda: {
            "dataset": "console_runtime_summary",
            "task_count": 1,
            "source": "pm5min.console.service",
        },
    )
    monkeypatch.setattr(
        "pm5min.console.handlers.load_console_runtime_history",
        lambda: {
            "dataset": "console_runtime_history",
            "row_count": 1,
            "source": "pm5min.console.service",
        },
    )
    monkeypatch.setattr(
        "pm5min.console.handlers.list_console_tasks",
        lambda **kwargs: {
            "dataset": "console_task_list",
            "filters": dict(kwargs),
            "source": "pm5min.console.service",
        },
    )
    monkeypatch.setattr(
        "pm5min.console.handlers.load_console_task",
        lambda *, task_id: {
            "dataset": "console_task",
            "task_id": task_id,
            "source": "pm5min.console.service",
        },
    )
    monkeypatch.setattr(
        "pm15min.console.service.load_console_runtime_state",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected pm15min runtime state call")),
    )
    monkeypatch.setattr(
        "pm15min.console.service.load_console_runtime_history",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected pm15min runtime history call")),
    )
    monkeypatch.setattr(
        "pm15min.console.service.list_console_tasks",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected pm15min task list call")),
    )
    monkeypatch.setattr(
        "pm15min.console.service.load_console_task",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected pm15min task detail call")),
    )

    rc = main(["console", "show-runtime-state"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_runtime_summary"
    assert payload["source"] == "pm5min.console.service"

    rc = main(["console", "show-runtime-history"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_runtime_history"
    assert payload["source"] == "pm5min.console.service"

    rc = main(["console", "list-tasks", "--action-id", "research_train_run", "--status-group", "active", "--limit", "5"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_task_list"
    assert payload["source"] == "pm5min.console.service"

    rc = main(["console", "show-task", "--task-id", "task_demo"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_task"
    assert payload["task_id"] == "task_demo"
    assert payload["source"] == "pm5min.console.service"


def test_runtime_and_task_view_helpers_move_out_of_service_module() -> None:
    console_root = Path(__file__).resolve().parents[1] / "src" / "pm5min" / "console"
    service_path = console_root / "service.py"
    runtime_views_path = console_root / "runtime_views.py"

    service_tree = ast.parse(service_path.read_text(encoding="utf-8"))
    service_text = service_path.read_text(encoding="utf-8")
    runtime_views_text = runtime_views_path.read_text(encoding="utf-8")

    assert runtime_views_path.exists()
    assert "def build_runtime_state_payload" in runtime_views_text
    assert "def build_runtime_history_payload" in runtime_views_text
    assert "def build_task_list_payload" in runtime_views_text
    assert "def build_task_detail_payload" in runtime_views_text
    assert "from .runtime_views import" in service_text
    defined_names = {node.name for node in service_tree.body if isinstance(node, ast.FunctionDef)}
    assert "build_runtime_state_payload" not in defined_names
    assert "build_runtime_history_payload" not in defined_names
    assert "build_task_list_payload" not in defined_names
    assert "build_task_detail_payload" not in defined_names


def test_execute_and_serve_keep_explicit_compat_paths(capsys, monkeypatch) -> None:
    handlers_path = _pm5min_console_module_path("handlers.py")
    compat_path = _pm5min_console_module_path("compat.py")

    service_imports = _relative_imported_names(handlers_path, module="service")
    compat_imports = _relative_imported_names(handlers_path, module="compat")
    compat_text = compat_path.read_text(encoding="utf-8")

    assert {"execute_console_action", "submit_console_action_task", "serve_console_http"} <= compat_imports
    assert "execute_console_action" not in service_imports
    assert "submit_console_action_task" not in service_imports
    assert "serve_console_http" not in service_imports
    assert "pm15min.console.service" not in compat_text
    assert "pm15min.console.action_runner" not in compat_text
    assert "pm5min.console.action_runner" in compat_text
    assert "pm15min.console.tasks" in compat_text
    assert "pm15min.console.http" not in compat_text
    assert "pm5min.console.http" in compat_text

    serve_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        "pm5min.console.handlers.execute_console_action",
        lambda *, action_id, request: {
            "dataset": "console_action_execution",
            "action_id": action_id,
            "request": dict(request),
            "source": "pm5min.console.compat",
        },
    )
    monkeypatch.setattr(
        "pm5min.console.handlers.submit_console_action_task",
        lambda *, action_id, request: {
            "dataset": "console_task",
            "task_id": "task_1",
            "action_id": action_id,
            "request": dict(request),
            "source": "pm5min.console.compat",
        },
    )
    monkeypatch.setattr(
        "pm5min.console.handlers.serve_console_http",
        lambda *, host, port, poll_interval: serve_calls.append(
            {
                "host": host,
                "port": port,
                "poll_interval": poll_interval,
                "source": "pm5min.console.compat",
            }
        ),
    )

    rc = main(["console", "execute-action", "--action-id", "research_activate_bundle", "--request-json", '{"bundle_label":"demo"}'])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_action_execution"
    assert payload["source"] == "pm5min.console.compat"

    rc = main(
        [
            "console",
            "execute-action",
            "--action-id",
            "research_train_run",
            "--execution-mode",
            "async",
            "--request-json",
            '{"market":"sol"}',
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_task"
    assert payload["task_id"] == "task_1"
    assert payload["source"] == "pm5min.console.compat"

    rc = main(["console", "serve", "--host", "127.0.0.1", "--port", "9001", "--poll-interval", "0.1"])

    assert rc == 0
    assert serve_calls == [
        {
            "host": "127.0.0.1",
            "port": 9001,
            "poll_interval": 0.1,
            "source": "pm5min.console.compat",
        }
    ]


def test_pm5min_console_http_module_stays_local() -> None:
    http_path = _pm5min_console_module_path("http.py")
    http_text = http_path.read_text(encoding="utf-8")

    assert "pm15min.console.http" not in http_text
    assert "pm15min.console.web" not in http_text


def test_pm5min_console_execute_action_uses_pm5min_planner_and_pm5min_main(monkeypatch) -> None:
    from pm5min.console.action_runner import execute_console_action

    planner_calls: list[tuple[str, dict[str, object]]] = []
    main_calls: list[list[str]] = []

    monkeypatch.setattr(
        "pm5min.console.action_runner.build_console_action_request",
        lambda action_id, request=None: planner_calls.append((action_id, dict(request or {})))
        or {
            "action_id": action_id,
            "normalized_request": dict(request or {}),
            "pm15min_args": ("data", "sync", "legacy-settlement-truth", "--market", "sol"),
            "command_preview": "PYTHONPATH=src python -m pm5min data sync legacy-settlement-truth --market sol",
        },
    )
    monkeypatch.setattr(
        "pm5min.console.action_runner._default_main_fn",
        lambda argv: main_calls.append(list(argv or [])) or 0,
    )

    payload = execute_console_action(
        action_id="data_sync",
        request={"sync_command": "legacy-settlement-truth", "market": "sol"},
    )

    assert planner_calls == [("data_sync", {"sync_command": "legacy-settlement-truth", "market": "sol"})]
    assert main_calls == [["data", "sync", "legacy-settlement-truth", "--market", "sol"]]
    assert payload["status"] == "ok"
    assert payload["action_id"] == "data_sync"
    assert payload["command_preview"].startswith("PYTHONPATH=src python -m pm5min")


def test_pm5min_console_async_task_uses_pm5min_planner(monkeypatch) -> None:
    from types import SimpleNamespace

    from pm5min.console.compat import submit_console_action_task

    def _raise_if_called(*args, **kwargs):
        raise AssertionError("pm5min console async task delegated to pm15min.console.tasks.submit_console_action_task")

    monkeypatch.setattr("pm15min.console.tasks.submit_console_action_task", _raise_if_called)
    monkeypatch.setattr(
        "pm5min.console.compat.execute_console_action",
        lambda *, action_id, request: {
            "dataset": "console_action_execution",
            "action_id": action_id,
            "request": dict(request),
            "source": "pm5min.console.action_runner",
        },
    )

    def _fake_submit_console_task(
        *,
        action_id,
        request=None,
        root=None,
        planner=None,
        executor=None,
        **kwargs,
    ):
        assert planner is not None
        assert executor is not None
        plan = planner(action_id, request)
        executed = executor(SimpleNamespace(action_id=action_id, request=dict(request or {})))
        return {
            "dataset": "console_task",
            "action_id": action_id,
            "request": dict(request or {}),
            "root": root,
            "plan": dict(plan),
            "executor_payload": dict(executed),
        }

    monkeypatch.setattr(
        "pm15min.console.tasks.submit_console_task",
        _fake_submit_console_task,
    )

    payload = submit_console_action_task(
        action_id="research_activate_bundle",
        request={"market": "sol", "bundle_label": "demo"},
        root="/tmp/console-root",
    )

    assert payload["dataset"] == "console_task"
    assert payload["request"]["market"] == "sol"
    assert payload["root"] == "/tmp/console-root"
    assert payload["plan"]["normalized_request"]["market"] == "sol"
    assert payload["plan"]["command_preview"].startswith("PYTHONPATH=src python -m pm5min")
    assert payload["executor_payload"]["source"] == "pm5min.console.action_runner"


def test_pm5min_console_http_handlers_default_to_5m_semantics(monkeypatch) -> None:
    from pm5min.console.http import route_console_http_request

    monkeypatch.setattr(
        "pm5min.console.http.console_service.load_console_data_overview",
        lambda **kwargs: {
            "dataset": "console_data_overview",
            **kwargs,
        },
    )
    monkeypatch.setattr(
        "pm5min.console.http.console_service.load_console_bundle",
        lambda **kwargs: {
            "dataset": "console_model_bundle",
            **kwargs,
        },
    )
    monkeypatch.setattr(
        "pm5min.console.http.console_service.load_console_backtest",
        lambda **kwargs: {
            "dataset": "console_backtest_run_detail",
            **kwargs,
        },
    )
    monkeypatch.setattr(
        "pm15min.console.service.load_console_data_overview",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected pm15min console data overview call")),
    )
    monkeypatch.setattr(
        "pm15min.console.service.load_console_bundle",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected pm15min console bundle call")),
    )
    monkeypatch.setattr(
        "pm15min.console.service.load_console_backtest",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected pm15min console backtest call")),
    )

    data_response = route_console_http_request(
        method="GET",
        target="/api/console/data-overview?market=sol",
    )
    bundle_response = route_console_http_request(
        method="GET",
        target="/api/console/bundles?market=sol&bundle_label=demo",
    )
    backtest_response = route_console_http_request(
        method="GET",
        target="/api/console/backtests?market=sol&run_label=bt1",
    )

    assert data_response.status_code == 200
    assert data_response.payload["cycle"] == "5m"
    assert bundle_response.status_code == 200
    assert bundle_response.payload["cycle"] == "5m"
    assert bundle_response.payload["profile"] == "deep_otm_5m"
    assert backtest_response.status_code == 200
    assert backtest_response.payload["cycle"] == "5m"
    assert backtest_response.payload["profile"] == "deep_otm_5m"
    assert backtest_response.payload["spec_name"] == "baseline_truth"


def test_pm5min_data_show_layout_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")

    rc = main(["data", "show-layout", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_pm5min_data_show_config_uses_pm5min_layout_root(capsys, monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    monkeypatch.setattr("pm5min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm5min.data.layout.rewrite_root", lambda: root)

    rc = main(["data", "show-config", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert Path(payload["layout"]["rewrite_root"]) == root


def test_pm5min_data_show_summary_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")

    def _raise_if_called(*args, **kwargs):
        raise AssertionError("pm5min show-summary delegated to pm15min.data.service.show_data_summary")

    monkeypatch.setattr("pm15min.data.service.show_data_summary", _raise_if_called)

    rc = main(["data", "show-summary", "--market", "sol", "--write-state"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "data_surface_summary"
    assert payload["cycle"] == "5m"
    assert payload["latest_summary_path"].endswith("latest.json")
    assert Path(payload["latest_summary_path"]).exists()


def test_pm5min_data_show_orderbook_coverage_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")

    def _raise_if_called(*args, **kwargs):
        raise AssertionError(
            "pm5min show-orderbook-coverage delegated to pm15min.data.service.orderbook_coverage"
        )

    monkeypatch.setattr("pm15min.data.service.orderbook_coverage.build_orderbook_coverage_report", _raise_if_called)

    rc = main(
        [
            "data",
            "show-orderbook-coverage",
            "--market",
            "sol",
            "--date-from",
            "2026-01-01",
            "--date-to",
            "2026-01-02",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "orderbook_depth_coverage"
    assert payload["cycle"] == "5m"
    assert payload["expected_daily_market_count"] == 288
    assert payload["date_from"] == "2026-01-01"
    assert payload["date_to"] == "2026-01-02"


def test_pm5min_data_live_foundation_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")
    monkeypatch.setattr(
        "pm15min.data.pipelines.foundation_runtime.run_live_data_foundation",
        lambda cfg, **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min live foundation delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.foundation_runtime.run_live_data_foundation",
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
        "pm15min.data.pipelines.direct_sync.sync_settlement_truth_from_rpc",
        lambda cfg, **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min settlement truth rpc delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.direct_sync.sync_settlement_truth_from_rpc",
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
        "pm15min.data.pipelines.source_ingest.import_legacy_settlement_truth",
        lambda cfg, source_path=None: (_ for _ in ()).throw(
            AssertionError("pm5min legacy settlement truth delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.source_ingest.import_legacy_settlement_truth",
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


def test_pm5min_build_truth_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")
    monkeypatch.setattr(
        "pm15min.data.pipelines.truth.build_truth_15m",
        lambda cfg: (_ for _ in ()).throw(AssertionError("pm5min build truth delegated to pm15min pipeline")),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.truth.build_truth_15m",
        lambda cfg: {
            "dataset": "truth_15m",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
        },
    )

    rc = main(["data", "build", "truth-15m", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_pm5min_build_oracle_prices_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")
    monkeypatch.setattr(
        "pm15min.data.pipelines.oracle_prices.build_oracle_prices_15m",
        lambda cfg: (_ for _ in ()).throw(AssertionError("pm5min build oracle delegated to pm15min pipeline")),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.oracle_prices.build_oracle_prices_15m",
        lambda cfg: {
            "dataset": "oracle_prices_15m",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
        },
    )

    rc = main(["data", "build", "oracle-prices-15m", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_pm5min_export_truth_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")
    monkeypatch.setattr(
        "pm15min.data.pipelines.export_tables.export_truth_15m",
        lambda cfg: (_ for _ in ()).throw(AssertionError("pm5min export truth delegated to pm15min pipeline")),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.export_tables.export_truth_15m",
        lambda cfg: {
            "dataset": "truth_export",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
        },
    )

    rc = main(["data", "export", "truth-15m", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_pm5min_backfill_direct_oracle_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    _patch_v2_roots(monkeypatch, tmp_path / "v2")
    monkeypatch.setattr(
        "pm15min.data.pipelines.direct_oracle_prices.backfill_direct_oracle_prices",
        lambda cfg, **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min backfill direct oracle delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.direct_oracle_prices.backfill_direct_oracle_prices",
        lambda cfg, **kwargs: {
            "dataset": "backfill_direct_oracle",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "workers": kwargs.get("workers"),
        },
    )

    rc = main(["data", "run", "backfill-direct-oracle", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["workers"] == 1


def test_pm5min_backfill_cycle_labels_gamma_uses_5m_cycle(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm5min.data.pipelines.backtest_refresh.backfill_cycle_labels_from_gamma",
        lambda **kwargs: {
            "dataset": "backfill_cycle_labels_gamma",
            "markets": kwargs.get("markets"),
            "cycle": kwargs.get("cycle"),
            "surface": kwargs.get("surface"),
        },
    )

    rc = main(["data", "run", "backfill-cycle-labels-gamma", "--markets", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["markets"] == ["sol"]


def test_pm5min_sync_market_catalog_stays_local_and_uses_5m_cycle(capsys, monkeypatch) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.market_catalog.sync_market_catalog",
        lambda cfg, **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min market catalog delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.market_catalog.sync_market_catalog",
        lambda cfg, **kwargs: {
            "dataset": "market_catalog",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
        },
    )

    rc = main(["data", "sync", "market-catalog", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_pm5min_sync_direct_oracle_prices_stays_local_and_uses_5m_cycle(capsys, monkeypatch) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.direct_oracle_prices.sync_polymarket_oracle_prices_direct",
        lambda cfg, **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min direct oracle sync delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.direct_oracle_prices.sync_polymarket_oracle_prices_direct",
        lambda cfg, **kwargs: {
            "dataset": "polymarket_direct_oracle_prices",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
        },
    )

    rc = main(["data", "sync", "direct-oracle-prices", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_pm5min_sync_streams_rpc_stays_local_and_uses_5m_cycle(capsys, monkeypatch) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.direct_sync.sync_streams_from_rpc",
        lambda cfg, **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min streams rpc delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.direct_sync.sync_streams_from_rpc",
        lambda cfg, **kwargs: {
            "dataset": "chainlink_streams_rpc",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "include_block_timestamp": kwargs.get("include_block_timestamp"),
        },
    )

    rc = main(
        [
            "data",
            "sync",
            "streams-rpc",
            "--market",
            "sol",
            "--surface",
            "live",
            "--include-block-timestamp",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["include_block_timestamp"] is True


def test_pm5min_sync_datafeeds_rpc_stays_local_and_uses_5m_cycle(capsys, monkeypatch) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.direct_sync.sync_datafeeds_from_rpc",
        lambda cfg, **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min datafeeds rpc delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.direct_sync.sync_datafeeds_from_rpc",
        lambda cfg, **kwargs: {
            "dataset": "chainlink_datafeeds_rpc",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "chunk_blocks": kwargs.get("chunk_blocks"),
        },
    )

    rc = main(["data", "sync", "datafeeds-rpc", "--market", "sol", "--surface", "live"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["chunk_blocks"] == 5000


def test_pm5min_sync_binance_klines_stays_local_and_uses_5m_cycle(capsys, monkeypatch) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.binance_klines.sync_binance_klines_1m",
        lambda cfg, **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min binance klines delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.binance_klines.sync_binance_klines_1m",
        lambda cfg, **kwargs: {
            "dataset": "binance_klines_1m",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "symbol": kwargs.get("symbol"),
        },
    )

    rc = main(["data", "sync", "binance-klines-1m", "--market", "sol", "--surface", "live"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["symbol"] is None


def test_pm5min_sync_legacy_market_catalog_stays_local_and_uses_5m_cycle(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.source_ingest.import_legacy_market_catalog",
        lambda cfg, source_path=None: (_ for _ in ()).throw(
            AssertionError("pm5min legacy market catalog delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.source_ingest.import_legacy_market_catalog",
        lambda cfg, source_path=None: {
            "dataset": "market_catalog",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "source_file": None if source_path is None else str(source_path),
        },
    )

    rc = main(
        [
            "data",
            "sync",
            "legacy-market-catalog",
            "--market",
            "sol",
            "--source-path",
            str(tmp_path / "markets.csv"),
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["source_file"] == str(tmp_path / "markets.csv")


def test_pm5min_sync_legacy_orderbook_depth_stays_local_and_uses_5m_cycle(capsys, monkeypatch) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.source_ingest.import_legacy_orderbook_depth",
        lambda cfg, **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min legacy orderbook depth delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.source_ingest.import_legacy_orderbook_depth",
        lambda cfg, **kwargs: {
            "dataset": "orderbook_depth",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "overwrite": kwargs.get("overwrite"),
        },
    )

    rc = main(["data", "sync", "legacy-orderbook-depth", "--market", "sol", "--overwrite"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["overwrite"] is True


def test_pm5min_sync_legacy_streams_stays_local_and_uses_5m_cycle(capsys, monkeypatch, tmp_path: Path) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.source_ingest.import_legacy_streams",
        lambda cfg, source_path=None: (_ for _ in ()).throw(
            AssertionError("pm5min legacy streams delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.source_ingest.import_legacy_streams",
        lambda cfg, source_path=None: {
            "dataset": "chainlink_streams",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "source_file": None if source_path is None else str(source_path),
        },
    )

    rc = main(
        [
            "data",
            "sync",
            "legacy-streams",
            "--market",
            "sol",
            "--source-path",
            str(tmp_path / "streams.csv"),
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["source_file"] == str(tmp_path / "streams.csv")


def test_pm5min_build_orderbook_index_stays_local_and_uses_5m_cycle(capsys, monkeypatch) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.orderbook_recording.build_orderbook_index_from_depth",
        lambda cfg, **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min orderbook index build delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.orderbook_recording.build_orderbook_index_from_depth",
        lambda cfg, **kwargs: {
            "dataset": "orderbook_index",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "date": kwargs.get("date_str"),
        },
    )

    rc = main(["data", "build", "orderbook-index", "--market", "sol", "--date", "2026-01-02"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["date"] == "2026-01-02"


def test_pm5min_export_oracle_prices_stays_local_and_uses_5m_cycle(capsys, monkeypatch) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.export_tables.export_oracle_prices_15m",
        lambda cfg: (_ for _ in ()).throw(AssertionError("pm5min export oracle delegated to pm15min pipeline")),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.export_tables.export_oracle_prices_15m",
        lambda cfg: {
            "dataset": "oracle_prices_export",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
        },
    )

    rc = main(["data", "export", "oracle-prices-15m", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"


def test_pm5min_record_orderbooks_stays_local_and_uses_5m_cycle(capsys, monkeypatch) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.orderbook_runtime.run_orderbook_recorder",
        lambda cfg, **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min orderbook recorder delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.orderbook_runtime.run_orderbook_recorder",
        lambda cfg, **kwargs: {
            "dataset": "orderbook_recorder",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "iterations": kwargs.get("iterations"),
        },
    )

    rc = main(["data", "record", "orderbooks", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["iterations"] == 1


def test_pm5min_run_orderbook_fleet_stays_local_and_defaults_to_5m(capsys, monkeypatch) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.orderbook_fleet.run_orderbook_recorder_fleet",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min orderbook fleet delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.orderbook_fleet.run_orderbook_recorder_fleet",
        lambda **kwargs: {
            "dataset": "orderbook_recorder_fleet",
            "markets": kwargs.get("markets"),
            "cycle": kwargs.get("cycle"),
            "surface": kwargs.get("surface"),
        },
    )

    rc = main(["data", "run", "orderbook-fleet", "--markets", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["markets"] == "sol"
    assert payload["surface"] == "live"


def test_pm5min_run_backtest_refresh_stays_local_and_uses_5m_semantics(capsys, monkeypatch) -> None:
    _forbid_pm15min_data_handler_delegation(monkeypatch)
    monkeypatch.setattr(
        "pm15min.data.pipelines.backtest_refresh.run_backtest_data_refresh",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("pm5min backtest refresh delegated to pm15min pipeline")
        ),
    )
    monkeypatch.setattr(
        "pm5min.data.pipelines.backtest_refresh.run_backtest_data_refresh",
        lambda **kwargs: {
            "dataset": "backtest_data_refresh",
            "markets": kwargs.get("markets"),
            "cycle": None if kwargs.get("options") is None else kwargs["options"].cycle,
            "surface": "backtest",
        },
    )

    rc = main(["data", "run", "backtest-refresh", "--markets", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle"] == "5m"
    assert payload["markets"] == ["sol"]
    assert payload["surface"] == "backtest"


def test_canonical_live_scope_rejects_non_canonical_cycle_with_canonical_market_and_profile() -> None:
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=5)

    scope = canonical_live_scope(cfg=cfg, target="direction")

    assert scope["market_in_scope"] is True
    assert scope["profile_in_scope"] is True
    assert scope["target_in_scope"] is True
    assert scope["cycle"] == "5m"
    assert scope["cycle_in_scope"] is False
    assert scope["ok"] is False
