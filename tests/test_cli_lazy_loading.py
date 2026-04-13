from __future__ import annotations

import importlib
import json
import sys
import types


def test_data_command_does_not_load_live_domain(monkeypatch, capsys) -> None:
    data_cli = types.ModuleType("pm15min.data.cli")

    def attach_data_subcommands(subparsers) -> None:
        parser = subparsers.add_parser("data")
        data_sub = parser.add_subparsers(dest="data_command")
        show_summary = data_sub.add_parser("show-summary")
        show_summary.add_argument("--market", default="btc")

    def run_data_command(args) -> int:
        print(json.dumps({"domain": args.domain, "market": args.market}))
        return 0

    data_cli.attach_data_subcommands = attach_data_subcommands
    data_cli.run_data_command = run_data_command

    def _boom(*_args, **_kwargs):
        raise AssertionError("unexpected optional domain load")

    live_cli = types.ModuleType("pm15min.live.cli")
    live_cli.attach_live_subcommands = _boom
    live_cli.run_live_command = _boom

    research_cli = types.ModuleType("pm15min.research.cli")
    research_cli.attach_research_subcommands = _boom
    research_cli.run_research_command = _boom

    console_cli = types.ModuleType("pm15min.console.cli")
    console_cli.attach_console_subcommands = _boom
    console_cli.run_console_command = _boom

    monkeypatch.setitem(sys.modules, "pm15min.data.cli", data_cli)
    monkeypatch.setitem(sys.modules, "pm15min.live.cli", live_cli)
    monkeypatch.setitem(sys.modules, "pm15min.research.cli", research_cli)
    monkeypatch.setitem(sys.modules, "pm15min.console.cli", console_cli)

    import pm15min.cli as cli_module

    cli_module = importlib.reload(cli_module)
    rc = cli_module.main(["data", "show-summary", "--market", "sol"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"domain": "data", "market": "sol"}
