from __future__ import annotations

import json
from pathlib import Path

from pm5min.research.config import ResearchConfig


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def test_pm5min_console_read_models_package_exports_local_models_only() -> None:
    from pm5min.console import read_models

    assert sorted(read_models.__all__) == [
        "describe_console_backtest_run",
        "describe_console_backtest_stake_sweep",
        "describe_console_experiment_matrix",
        "describe_console_experiment_run",
        "list_console_backtest_runs",
        "list_console_experiment_runs",
        "list_console_model_bundles",
        "list_console_training_runs",
        "load_console_model_bundle",
        "load_console_training_run",
        "load_data_overview",
    ]


def test_pm5min_console_backtest_read_models_use_5m_layout_and_profile_defaults(tmp_path: Path) -> None:
    from pm5min.console.read_models import backtests

    assert backtests.ResearchConfig.__module__ == "pm5min.research.config"
    assert backtests.read_manifest.__module__ == "pm5min.research.manifests"

    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="5m",
        profile="deep_otm_5m",
        target="direction",
        model_family="deep_otm",
        root=root,
    )
    run_dir = cfg.layout.backtest_run_dir(
        profile="deep_otm_5m",
        target="direction",
        spec_name="baseline_truth",
        run_label="bt_console",
    )
    spread_run_dir = (
        cfg.layout.backtests_root
        / "profile=deep_otm_5m"
        / "target=spread"
        / "spec=baseline_truth"
        / "run=bt_spread"
    )
    other_profile_run_dir = (
        cfg.layout.backtests_root
        / "profile=other_profile"
        / "target=direction"
        / "spec=baseline_truth"
        / "run=bt_other"
    )
    _write_json(
        run_dir / "summary.json",
        {
            "market": "sol",
            "cycle": "5m",
            "profile": "deep_otm_5m",
            "target": "direction",
            "spec_name": "baseline_truth",
            "run_label": "bt_console",
            "trades": 3,
            "wins": 2,
            "losses": 1,
            "rejects": 0,
            "pnl_sum": 1.5,
            "stake_sum": 6.0,
            "avg_roi_pct": 25.0,
            "roi_pct": 25.0,
        },
    )
    _write_json(
        spread_run_dir / "summary.json",
        {
            "market": "sol",
            "cycle": "5m",
            "profile": "deep_otm_5m",
            "target": "spread",
            "spec_name": "baseline_truth",
            "run_label": "bt_spread",
            "trades": 1,
            "wins": 1,
            "losses": 0,
            "rejects": 0,
            "pnl_sum": 0.3,
            "stake_sum": 1.0,
            "avg_roi_pct": 30.0,
            "roi_pct": 30.0,
        },
    )
    _write_json(
        other_profile_run_dir / "summary.json",
        {
            "market": "sol",
            "cycle": "5m",
            "profile": "other_profile",
            "target": "direction",
            "spec_name": "baseline_truth",
            "run_label": "bt_other",
            "trades": 2,
            "wins": 1,
            "losses": 1,
            "rejects": 0,
            "pnl_sum": 0.2,
            "stake_sum": 2.0,
            "avg_roi_pct": 10.0,
            "roi_pct": 10.0,
        },
    )
    (run_dir / "report.md").write_text("# Backtest\n", encoding="utf-8")

    rows = backtests.list_console_backtest_runs(market="sol", root=root)
    assert len(rows) == 1
    assert [row["run_label"] for row in rows] == ["bt_console"]
    assert rows[0]["profile"] == "deep_otm_5m"
    assert rows[0]["target"] == "direction"
    assert rows[0]["spec_name"] == "baseline_truth"
    assert rows[0]["run_label"] == "bt_console"

    detail = backtests.describe_console_backtest_run(
        market="sol",
        profile="deep_otm_5m",
        spec_name="baseline_truth",
        run_label="bt_console",
        root=root,
    )
    assert detail["profile"] == "deep_otm_5m"
    assert detail["target"] == "direction"
    assert detail["spec_name"] == "baseline_truth"
    assert detail["run_label"] == "bt_console"


def test_pm5min_console_experiment_read_models_use_pm5min_research_layout(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from pm5min.console.read_models import experiments

    assert experiments.ResearchLayout.__module__ == "pm5min.research.layout"
    assert experiments.slug_token.__module__ == "pm5min.research.layout_helpers"
    assert experiments.read_manifest.__module__ == "pm5min.research.manifests"

    root = tmp_path / "v2"
    seen: dict[str, object] = {}
    resolved_run_dir = root / "research" / "experiments" / "runs" / "suite=console_suite" / "run=exp_console"

    class _FakeLayout:
        experiment_runs_root = root / "research" / "experiments" / "runs"

        def experiment_run_dir(self, suite_name: str, run_label_text: str) -> Path:
            seen["suite_name"] = suite_name
            seen["run_label"] = run_label_text
            return resolved_run_dir

    def _discover(cls, *, root=None):
        seen["root"] = root
        return _FakeLayout()

    monkeypatch.setattr(experiments.ResearchLayout, "discover", classmethod(_discover))
    monkeypatch.setattr(experiments, "_build_experiment_run_detail", lambda path: {"path": str(path)})
    monkeypatch.setattr(experiments, "_build_experiment_matrix_detail", lambda path: {"path": str(path), "dataset": "matrix"})

    rows = experiments.list_console_experiment_runs(root=root)
    detail = experiments.describe_console_experiment_run(
        suite_name="console_suite",
        run_label="exp_console",
        root=root,
    )
    matrix = experiments.describe_console_experiment_matrix(
        suite_name="console_suite",
        run_label="exp_console",
        root=root,
    )

    assert rows == []
    assert detail == {"path": str(resolved_run_dir)}
    assert matrix == {"path": str(resolved_run_dir), "dataset": "matrix"}
    assert seen == {
        "root": root,
        "suite_name": "console_suite",
        "run_label": "exp_console",
    }


def test_pm5min_console_data_overview_uses_pm5min_data_service(monkeypatch, tmp_path: Path) -> None:
    from pm5min.console.read_models import data_overview
    from pm5min.data import service as data_service

    assert data_overview.show_data_summary.__module__ == "pm5min.data.service"
    assert data_overview.describe_data_runtime.__module__ == "pm5min.data.service"

    seen: dict[str, object] = {}
    audit_now = object()

    def _show_data_summary(cfg, *, persist: bool = False, now=None) -> dict[str, object]:
        seen["market"] = cfg.asset.slug
        seen["cycle"] = cfg.cycle
        seen["surface"] = cfg.surface
        seen["now"] = now
        return {
            "domain": "data",
            "dataset": "data_surface_summary",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "surface": cfg.surface,
            "generated_at": "2026-04-13T00-00-00Z",
            "generated_at_iso": "2026-04-13T00:00:00+00:00",
            "summary": {"dataset_count": 1},
            "audit": {"status": "ok"},
            "completeness": {"status": "ok"},
            "issues": [],
            "datasets": {
                "truth_table": {
                    "kind": "single_parquet",
                    "status": "ok",
                    "exists": True,
                    "path": str(tmp_path / "truth.parquet"),
                    "row_count": 12,
                }
            },
        }

    monkeypatch.setattr(data_service, "_show_data_summary", _show_data_summary)
    monkeypatch.setattr(
        data_service,
        "_describe_data_runtime",
        lambda cfg: {
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "surface": cfg.surface,
        },
    )

    payload = data_overview.load_data_overview(market="sol", root=tmp_path / "v2", now=audit_now)

    assert seen == {"market": "sol", "cycle": "5m", "surface": "backtest", "now": audit_now}
    assert payload["cycle"] == "5m"
    assert payload["summary_source"] == data_overview.SUMMARY_SOURCE_COMPUTED
    assert payload["runtime"]["cycle"] == "5m"
    assert payload["dataset_rows"][0]["dataset_name"] == "truth_table"


def test_pm5min_console_training_runs_uses_pm5min_research_service(monkeypatch, tmp_path: Path) -> None:
    from pm5min.console.read_models import training_runs

    assert training_runs._list_training_runs.__module__ == "pm5min.research.service"
    assert training_runs.normalize_label_set.__module__ == "pm5min.research.config"
    assert training_runs.read_manifest.__module__ == "pm5min.research.manifests"

    run_dir = tmp_path / "training" / "run=demo"
    _write_json(
        run_dir / "summary.json",
        {
            "market": "sol",
            "cycle": "5m",
            "model_family": "deep_otm",
            "target": "direction",
            "run_label": "demo",
            "feature_set": "deep_otm_v1",
            "label_set": "truth",
            "offsets": [2, 3],
        },
    )

    seen: dict[str, object] = {}

    def _list_training_runs(cfg, *, model_family=None, target=None, prefix=None) -> list[dict[str, object]]:
        seen["market"] = cfg.asset.slug
        seen["cycle"] = cfg.cycle
        seen["profile"] = cfg.profile
        seen["model_family"] = model_family
        seen["target"] = target
        seen["prefix"] = prefix
        return [{"path": str(run_dir)}]

    monkeypatch.setattr(training_runs, "_list_training_runs", _list_training_runs)

    rows = training_runs.list_console_training_runs(
        market="sol",
        model_family="deep_otm",
        target="direction",
        root=tmp_path / "v2",
    )
    detail = training_runs.load_console_training_run(
        market="sol",
        model_family="deep_otm",
        target="direction",
        run_label="demo",
        root=tmp_path / "v2",
    )

    assert seen == {
        "market": "sol",
        "cycle": "5m",
        "profile": "deep_otm_5m",
        "model_family": "deep_otm",
        "target": "direction",
        "prefix": "demo",
    }
    assert rows[0]["cycle"] == "5m"
    assert rows[0]["run_label"] == "demo"
    assert detail["cycle"] == "5m"
    assert detail["run_label"] == "demo"
    assert detail["offsets"] == [2, 3]


def test_pm5min_console_bundle_detail_uses_pm5min_bundle_registry(tmp_path: Path) -> None:
    from pm5min.console.read_models import bundles

    assert bundles.resolve_model_bundle_dir.__module__ == "pm5min.research.bundles.loader"
    assert bundles.read_model_bundle_manifest.__module__ == "pm5min.research.bundles.loader"
    assert bundles._get_active_bundle_selection.__module__ == "pm5min.research.service"

    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="5m",
        profile="deep_otm_5m",
        target="direction",
        model_family="deep_otm",
        root=root,
    )
    bundle_dir = cfg.layout.model_bundle_dir(profile="deep_otm_5m", target="direction", bundle_label="demo")
    _write_json(
        bundle_dir / "summary.json",
        {
            "market": "sol",
            "cycle": "5m",
            "profile": "deep_otm_5m",
            "target": "direction",
            "bundle_label": "demo",
            "feature_set": "deep_otm_v1",
            "label_set": "truth",
            "model_family": "deep_otm",
            "source_training_run_dir": str(root / "research" / "training_runs" / "cycle=5m" / "asset=sol" / "demo"),
            "offsets": [2],
        },
    )
    selection_path = cfg.layout.active_bundle_selection_path(profile="deep_otm_5m", target="direction")
    _write_json(
        selection_path,
        {
            "market": "sol",
            "cycle": "5m",
            "profile": "deep_otm_5m",
            "target": "direction",
            "bundle_label": "demo",
            "bundle_dir": str(bundle_dir),
        },
    )

    detail = bundles.load_console_model_bundle(
        market="sol",
        profile="deep_otm_5m",
        target="direction",
        bundle_label="demo",
        root=root,
    )

    assert detail["cycle"] == "5m"
    assert detail["bundle_label"] == "demo"
    assert detail["bundle_dir"] == str(bundle_dir)
    assert detail["is_active"] is True
    assert detail["active_selection"]["selection"]["bundle_dir"] == str(bundle_dir)


def test_pm5min_console_bundle_list_uses_5m_default_profile(monkeypatch, tmp_path: Path) -> None:
    from pm5min.console.read_models import bundles

    seen: dict[str, object] = {}

    def _list_model_bundles(cfg, *, profile=None, target=None, prefix=None) -> list[dict[str, object]]:
        seen["cfg_profile"] = cfg.profile
        seen["profile_arg"] = profile
        seen["target"] = target
        seen["prefix"] = prefix
        return []

    monkeypatch.setattr(bundles, "_list_model_bundles", _list_model_bundles)

    rows = bundles.list_console_model_bundles(
        market="sol",
        profile=None,
        target="direction",
        root=tmp_path / "v2",
    )

    assert rows == []
    assert seen == {
        "cfg_profile": "deep_otm_5m",
        "profile_arg": None,
        "target": "direction",
        "prefix": None,
    }
