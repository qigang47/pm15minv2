from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
import threading
import time

import pandas as pd

from pm15min.research._contracts_frames import DateWindow
from pm15min.research._contracts_runs import BacktestParitySpec
from pm15min.research.config import ResearchConfig
from pm15min.research.experiments.orchestration import build_execution_groups
from pm15min.research.experiments.runner import (
    _backtest_run_label,
    _case_key,
    _seed_bundle_cache,
    _seed_training_cache,
    run_experiment_suite,
)


@dataclass(frozen=True)
class _MarketSpec:
    market: str
    profile: str = "deep_otm"
    model_family: str = "deep_otm"
    feature_set: str = "deep_otm_v1"
    label_set: str = "truth"
    target: str = "direction"
    offsets: tuple[int, ...] = (7, 8)
    window: DateWindow = field(default_factory=lambda: DateWindow.from_bounds("2026-03-01", "2026-03-01"))
    backtest_spec: str = "baseline_truth"
    variant_label: str = "default"
    variant_notes: str = ""
    stake_usd: float | None = None
    max_notional_usd: float | None = None
    weight_variant_label: str = "default"
    balance_classes: bool | None = None
    weight_by_vol: bool | None = None
    inverse_vol: bool | None = None
    contrarian_weight: float | None = None
    contrarian_quantile: float | None = None
    contrarian_return_col: str | None = None
    offset_weight_overrides: dict[int, dict[str, object]] = field(default_factory=dict)
    matrix_parent_run_name: str = ""
    matrix_stake_label: str = ""
    hybrid_secondary_target: str | None = None
    hybrid_secondary_offsets: tuple[int, ...] | None = None
    hybrid_fallback_reasons: tuple[str, ...] = ()
    parity: BacktestParitySpec = field(default_factory=BacktestParitySpec)
    group_name: str = ""
    run_name: str = ""
    tags: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "profile": self.profile,
            "model_family": self.model_family,
            "feature_set": self.feature_set,
            "label_set": self.label_set,
            "target": self.target,
            "offsets": list(self.offsets),
            "window": self.window.to_dict(),
            "backtest_spec": self.backtest_spec,
            "variant_label": self.variant_label,
            "variant_notes": self.variant_notes,
            "stake_usd": self.stake_usd,
            "max_notional_usd": self.max_notional_usd,
            "weight_variant_label": self.weight_variant_label,
            "balance_classes": self.balance_classes,
            "weight_by_vol": self.weight_by_vol,
            "inverse_vol": self.inverse_vol,
            "contrarian_weight": self.contrarian_weight,
            "contrarian_quantile": self.contrarian_quantile,
            "contrarian_return_col": self.contrarian_return_col,
            "offset_weight_overrides": {
                str(int(offset)): dict(overrides)
                for offset, overrides in sorted(self.offset_weight_overrides.items())
            },
            "matrix_parent_run_name": self.matrix_parent_run_name,
            "matrix_stake_label": self.matrix_stake_label,
            "hybrid_secondary_target": self.hybrid_secondary_target,
            "hybrid_secondary_offsets": None if self.hybrid_secondary_offsets is None else list(self.hybrid_secondary_offsets),
            "hybrid_fallback_reasons": list(self.hybrid_fallback_reasons),
            "parity": self.parity.to_dict(),
            "group_name": self.group_name,
            "run_name": self.run_name,
            "tags": list(self.tags),
        }


@dataclass(frozen=True)
class _Suite:
    suite_name: str
    cycle: str
    markets: tuple[_MarketSpec, ...]
    runtime_policy: object | None = None


def _build_cfg(root: Path) -> ResearchConfig:
    return ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        model_family="deep_otm",
        root=root,
    )


def _install_runtime_fakes(
    monkeypatch,
    *,
    root: Path,
    counters: dict[str, int],
    backtest_trace: list[dict[str, object]],
    reporter_trace: dict[str, list[bool]] | None = None,
) -> None:
    monkeypatch.setattr("pm15min.research.experiments.runner.build_feature_frame_dataset", lambda cfg: counters.__setitem__("feature", counters["feature"] + 1) or {"ok": True})
    monkeypatch.setattr("pm15min.research.experiments.runner.build_label_frame_dataset", lambda cfg: counters.__setitem__("label", counters["label"] + 1) or {"ok": True})

    def _fake_train(cfg, spec, reporter=None):
        counters["train"] += 1
        if reporter_trace is not None:
            reporter_trace.setdefault("train", []).append(callable(reporter))
        if callable(reporter):
            reporter(
                "Training started",
                current=0,
                total=1,
                current_stage="training_offsets",
                progress_pct=0,
                heartbeat="2026-03-23T00:00:00Z",
            )
            reporter(
                "Training finished",
                current=1,
                total=1,
                current_stage="training_offsets",
                progress_pct=100,
                heartbeat="2026-03-23T00:00:01Z",
            )
        run_dir = root / "research" / "training_runs" / spec.run_label
        run_dir.mkdir(parents=True, exist_ok=True)
        return {"run_dir": str(run_dir), "run_label": spec.run_label}

    def _fake_bundle(cfg, spec, reporter=None):
        del reporter
        counters["bundle"] += 1
        bundle_dir = root / "research" / "model_bundles" / spec.bundle_label
        bundle_dir.mkdir(parents=True, exist_ok=True)
        return {"bundle_dir": str(bundle_dir), "bundle_label": spec.bundle_label}

    def _fake_backtest(cfg, spec, reporter=None):
        counters["backtest"] += 1
        if reporter_trace is not None:
            reporter_trace.setdefault("backtest", []).append(callable(reporter))
        if callable(reporter):
            reporter(
                "Loading backtest inputs",
                current=1,
                total=8,
                current_stage="load_inputs",
                progress_pct=10,
                heartbeat="2026-03-23T00:00:02Z",
            )
            reporter(
                "Backtest finished",
                current=1,
                total=1,
                current_stage="finished",
                progress_pct=100,
                heartbeat="2026-03-23T00:00:00Z",
            )
        backtest_trace.append(
            {
                "run_label": spec.run_label,
                "bundle_label": spec.bundle_label,
                "variant_label": spec.variant_label,
                "stake_usd": getattr(spec, "stake_usd", None),
                "max_notional_usd": getattr(spec, "max_notional_usd", None),
            }
        )
        run_dir = cfg.layout.backtest_run_dir(
            profile=spec.profile,
            spec_name=spec.spec_name,
            run_label_text=spec.run_label,
        )
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "trades": 1,
                    "rejects": 0,
                    "wins": 1,
                    "losses": 0,
                    "pnl_sum": 1.0,
                    "stake_sum": 1.0,
                    "roi_pct": 100.0,
                    "bundle_dir": str(root / "research" / "model_bundles" / spec.bundle_label),
                    "secondary_bundle_dir": None,
                }
            ),
            encoding="utf-8",
        )
        return {"run_dir": str(run_dir), "summary_path": str(summary_path)}

    monkeypatch.setattr("pm15min.research.experiments.runner.train_research_run", _fake_train)
    monkeypatch.setattr("pm15min.research.experiments.runner.build_model_bundle", _fake_bundle)
    monkeypatch.setattr("pm15min.research.experiments.runner.run_research_backtest", _fake_backtest)
    monkeypatch.setattr("pm15min.research.experiments.runner.build_leaderboard", lambda frame: frame[["market", "roi_pct"]].copy())


def test_case_identity_changes_when_weight_settings_change() -> None:
    base = _MarketSpec(
        market="btc",
        group_name="core",
        run_name="baseline",
        variant_label="default",
    )
    no_vol = replace(base, weight_variant_label="no_vol_weight", weight_by_vol=False)
    contrarian = replace(
        base,
        weight_variant_label="offset_reversal_mild",
        offset_weight_overrides={
            7: {"weight_by_vol": "false", "contrarian_weight": 1.5},
            8: {"inverse_vol": "true", "contrarian_weight": 1.75},
        },
    )

    case_keys = {
        _case_key(base),
        _case_key(no_vol),
        _case_key(contrarian),
    }
    backtest_labels = {
        _backtest_run_label(run_label="resume-exp", market_spec=base, case_key=_case_key(base)),
        _backtest_run_label(run_label="resume-exp", market_spec=no_vol, case_key=_case_key(no_vol)),
        _backtest_run_label(run_label="resume-exp", market_spec=contrarian, case_key=_case_key(contrarian)),
    }

    assert len(case_keys) == 3
    assert len(backtest_labels) == 3


def test_run_experiment_suite_reports_progress(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="progress_suite",
        cycle="15m",
        markets=(
            _MarketSpec(market="sol", group_name="core", run_name="run-a", variant_label="baseline"),
            _MarketSpec(market="eth", group_name="core", run_name="run-b", variant_label="alt"),
        ),
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")
    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace: list[dict[str, object]] = []
    reporter_trace = {"train": [], "backtest": []}
    _install_runtime_fakes(
        monkeypatch,
        root=root,
        counters=counters,
        backtest_trace=backtest_trace,
        reporter_trace=reporter_trace,
    )

    events: list[dict[str, object]] = []
    summary = run_experiment_suite(
        cfg=cfg,
        suite_name="progress_suite",
        run_label="progress-exp",
        reporter=lambda **payload: events.append(payload),
    )

    assert summary["dataset"] == "experiment_run"
    assert events[0]["current_stage"] == "experiment_suite"
    assert any(event["current_stage"] == "experiment_group" for event in events)
    assert any(event["current_stage"] == "experiment_group_warmup" for event in events)
    assert any(event["current_stage"] == "experiment_cases" for event in events)
    assert any(event["current_stage"] == "training_offsets" for event in events)
    assert any(event["current_stage"] == "load_inputs" for event in events)
    assert any("Built prepared datasets for" in str(event["summary"]) for event in events)
    assert all(reporter_trace["train"])
    assert all(reporter_trace["backtest"])
    assert len(reporter_trace["train"]) == 2
    assert len(reporter_trace["backtest"]) == 2
    training_events = [event for event in events if event["current_stage"] == "training_offsets"]
    assert all(event["total"] == 2 for event in training_events)
    assert all(event["current"] in {1, 2} for event in training_events)
    assert any(event["heartbeat"] == "2026-03-23T00:00:02Z" for event in events)
    progress_values = [int(event["progress_pct"]) for event in events if event.get("progress_pct") is not None]
    assert progress_values == sorted(progress_values)
    assert events[-1]["current_stage"] == "finished"
    assert events[-1]["progress_pct"] == 100


def test_run_experiment_suite_releases_backtest_memory_after_each_group(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="memory_release_suite",
        cycle="15m",
        markets=(
            _MarketSpec(market="sol", group_name="core", run_name="run-a", variant_label="baseline"),
            _MarketSpec(market="eth", group_name="core", run_name="run-b", variant_label="alt"),
        ),
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace: list[dict[str, object]] = []
    _install_runtime_fakes(
        monkeypatch,
        root=root,
        counters=counters,
        backtest_trace=backtest_trace,
    )

    release_trace: list[str] = []
    monkeypatch.setattr(
        "pm15min.research.experiments.runner.clear_process_backtest_runtime_cache",
        lambda: release_trace.append("clear"),
    )
    monkeypatch.setattr(
        "pm15min.research.experiments.runner.gc.collect",
        lambda: release_trace.append("gc") or 0,
    )

    run_experiment_suite(cfg=cfg, suite_name="memory_release_suite", run_label="memory-release-exp")

    assert release_trace == ["clear", "gc", "clear", "gc"]


def test_build_execution_groups_groups_stake_matrix_cases_by_parent_run_name_and_stake() -> None:
    groups = build_execution_groups(
        (
            _MarketSpec(
                market="sol",
                group_name="core",
                run_name="run-a__stake_5usd",
                matrix_parent_run_name="run-a",
                matrix_stake_label="stake_5usd",
                stake_usd=5.0,
            ),
            _MarketSpec(
                market="sol",
                group_name="core",
                run_name="run-a__stake_1usd",
                matrix_parent_run_name="run-a",
                matrix_stake_label="stake_1usd",
                stake_usd=1.0,
            ),
            _MarketSpec(
                market="sol",
                group_name="core",
                run_name="run-b",
                variant_label="alt",
            ),
        )
    )

    assert len(groups) == 2
    assert groups[0].group_label == "sol/core/run-a"
    assert [spec.run_name for spec in groups[0].market_specs] == ["run-a__stake_1usd", "run-a__stake_5usd"]
    assert [spec.stake_usd for spec in groups[0].market_specs] == [1.0, 5.0]
    assert groups[1].group_label == "sol/core/run-b/alt"


def test_seed_reuse_caches_strip_partition_prefixes_from_training_and_bundle_dirs() -> None:
    rows = [
        {
            "market": "sol",
            "profile": "deep_otm",
            "model_family": "deep_otm",
            "feature_set": "deep_otm_v1",
            "label_set": "truth",
            "target": "direction",
            "window": "2026-03-01_2026-03-01",
            "offsets": [7, 8],
            "training_run_dir": "/tmp/v2/research/training_runs/cycle=15m/asset=sol/model_family=deep_otm/target=direction/run=sol-train-a",
            "bundle_dir": "/tmp/v2/research/model_bundles/cycle=15m/asset=sol/profile=deep_otm/target=direction/bundle=sol-bundle-a",
        }
    ]

    training_cache = _seed_training_cache(rows)
    bundle_cache = _seed_bundle_cache(rows)

    assert list(training_cache.values())[0]["run_label"] == "sol-train-a"
    assert list(bundle_cache.values())[0]["bundle_label"] == "sol-bundle-a"


def test_run_experiment_suite_does_not_reuse_training_or_bundle_across_weight_variants(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="weight_variant_suite",
        cycle="15m",
        markets=(
            _MarketSpec(
                market="sol",
                group_name="core",
                run_name="run-a__w_current_default",
                weight_variant_label="current_default",
            ),
            _MarketSpec(
                market="sol",
                group_name="core",
                run_name="run-a__w_no_vol_weight",
                weight_variant_label="no_vol_weight",
                weight_by_vol=False,
            ),
        ),
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace: list[dict[str, object]] = []
    _install_runtime_fakes(monkeypatch, root=root, counters=counters, backtest_trace=backtest_trace)

    summary = run_experiment_suite(cfg=cfg, suite_name="weight_variant_suite", run_label="weight-variant-exp")
    run_dir = Path(summary["run_dir"])
    training_runs = pd.read_parquet(run_dir / "training_runs.parquet").sort_values("run_name").reset_index(drop=True)

    assert counters == {"feature": 1, "label": 1, "train": 2, "bundle": 2, "backtest": 2}
    assert training_runs["training_reused"].tolist() == [False, False]
    assert training_runs["bundle_reused"].tolist() == [False, False]


def test_run_experiment_suite_reuses_training_and_bundle_across_variants(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="matrix_suite",
        cycle="15m",
        markets=(
            _MarketSpec(market="sol", group_name="core", run_name="run-a", variant_label="baseline", tags=("suite", "core")),
            _MarketSpec(market="sol", group_name="core", run_name="run-b", variant_label="alt", tags=("suite", "core")),
        ),
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace: list[dict[str, object]] = []
    _install_runtime_fakes(monkeypatch, root=root, counters=counters, backtest_trace=backtest_trace)

    summary = run_experiment_suite(cfg=cfg, suite_name="matrix_suite", run_label="matrix-exp")
    run_dir = Path(summary["run_dir"])
    training_runs = pd.read_parquet(run_dir / "training_runs.parquet").sort_values("variant_label").reset_index(drop=True)
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet").sort_values("variant_label").reset_index(drop=True)

    assert counters == {"feature": 1, "label": 1, "train": 1, "bundle": 1, "backtest": 2}
    assert backtest_trace[0]["bundle_label"] == backtest_trace[1]["bundle_label"]
    assert training_runs["training_reused"].tolist() == [True, False]
    assert training_runs["bundle_reused"].tolist() == [True, False]
    assert training_runs["group_name"].tolist() == ["core", "core"]
    assert training_runs["run_name"].tolist() == ["run-b", "run-a"]
    assert backtest_runs["bundle_dir"].nunique() == 1
    assert backtest_runs["resumed_from_existing"].tolist() == [False, False]


def test_run_experiment_suite_reuses_training_and_bundle_across_stake_matrix_cases(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="stake_matrix_suite",
        cycle="15m",
        markets=(
            _MarketSpec(
                market="sol",
                group_name="core",
                run_name="run-a__stake_1usd",
                matrix_parent_run_name="run-a",
                matrix_stake_label="stake_1usd",
                stake_usd=1.0,
                max_notional_usd=8.0,
            ),
            _MarketSpec(
                market="sol",
                group_name="core",
                run_name="run-a__stake_5usd",
                matrix_parent_run_name="run-a",
                matrix_stake_label="stake_5usd",
                stake_usd=5.0,
                max_notional_usd=8.0,
            ),
        ),
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace: list[dict[str, object]] = []
    _install_runtime_fakes(monkeypatch, root=root, counters=counters, backtest_trace=backtest_trace)

    summary = run_experiment_suite(cfg=cfg, suite_name="stake_matrix_suite", run_label="stake-matrix-exp")
    run_dir = Path(summary["run_dir"])
    training_runs = pd.read_parquet(run_dir / "training_runs.parquet").sort_values("stake_usd").reset_index(drop=True)
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet").sort_values("stake_usd").reset_index(drop=True)
    suite_log = (run_dir / "logs" / "suite.jsonl").read_text(encoding="utf-8")

    assert counters == {"feature": 1, "label": 1, "train": 1, "bundle": 1, "backtest": 2}
    assert [row["stake_usd"] for row in backtest_trace] == [1.0, 5.0]
    assert [row["max_notional_usd"] for row in backtest_trace] == [8.0, 8.0]
    assert training_runs["training_reused"].tolist() == [False, True]
    assert training_runs["bundle_reused"].tolist() == [False, True]
    assert training_runs["matrix_parent_run_name"].tolist() == ["run-a", "run-a"]
    assert training_runs["run_name"].tolist() == ["run-a__stake_1usd", "run-a__stake_5usd"]
    assert backtest_runs["stake_usd"].tolist() == [1.0, 5.0]
    assert backtest_runs["max_notional_usd"].tolist() == [8.0, 8.0]
    assert '"event": "execution_group_started"' in suite_log
    assert '"event": "execution_group_warmup_started"' in suite_log
    assert '"event": "market_cache_resolved"' in suite_log
    assert '"event": "execution_group_completed"' in suite_log


def test_run_experiment_suite_resumes_only_cases_with_existing_summary(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="resume_suite",
        cycle="15m",
        markets=(
            _MarketSpec(market="sol", group_name="core", run_name="run-a", variant_label="baseline"),
            _MarketSpec(market="sol", group_name="core", run_name="run-b", variant_label="alt"),
        ),
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace: list[dict[str, object]] = []
    _install_runtime_fakes(monkeypatch, root=root, counters=counters, backtest_trace=backtest_trace)

    summary = run_experiment_suite(cfg=cfg, suite_name="resume_suite", run_label="resume-exp")
    run_dir = Path(summary["run_dir"])
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet").sort_values("variant_label").reset_index(drop=True)
    alt_summary_path = Path(str(backtest_runs.loc[0, "summary_path"]))
    alt_summary_path.unlink()

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace = []
    _install_runtime_fakes(monkeypatch, root=root, counters=counters, backtest_trace=backtest_trace)

    summary = run_experiment_suite(cfg=cfg, suite_name="resume_suite", run_label="resume-exp")
    run_dir = Path(summary["run_dir"])
    training_runs = pd.read_parquet(run_dir / "training_runs.parquet").sort_values("variant_label").reset_index(drop=True)
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet").sort_values("variant_label").reset_index(drop=True)

    assert counters == {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 1}
    assert len(backtest_trace) == 1
    assert backtest_trace[0]["variant_label"] == "alt"
    assert training_runs["resumed_from_existing"].tolist() == [False, True]
    assert backtest_runs["resumed_from_existing"].tolist() == [False, True]


def test_run_experiment_suite_recovers_missing_case_rows_from_existing_backtest_summary(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="recover_summary_suite",
        cycle="15m",
        markets=(
            _MarketSpec(
                market="sol",
                group_name="core",
                run_name="run-a__stake_1usd",
                matrix_parent_run_name="run-a",
                matrix_stake_label="stake_1usd",
                stake_usd=1.0,
            ),
            _MarketSpec(
                market="sol",
                group_name="core",
                run_name="run-a__stake_5usd",
                matrix_parent_run_name="run-a",
                matrix_stake_label="stake_5usd",
                stake_usd=5.0,
            ),
        ),
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace: list[dict[str, object]] = []
    _install_runtime_fakes(monkeypatch, root=root, counters=counters, backtest_trace=backtest_trace)

    summary = run_experiment_suite(cfg=cfg, suite_name="recover_summary_suite", run_label="recover-summary-exp")
    run_dir = Path(summary["run_dir"])
    training_runs = pd.read_parquet(run_dir / "training_runs.parquet")
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet")

    missing_case_key = str(backtest_runs.loc[backtest_runs["stake_usd"].eq(5.0), "case_key"].iloc[0])
    training_runs = training_runs.loc[training_runs["case_key"].ne(missing_case_key)].reset_index(drop=True)
    backtest_runs = backtest_runs.loc[backtest_runs["case_key"].ne(missing_case_key)].reset_index(drop=True)
    training_runs.to_parquet(run_dir / "training_runs.parquet", index=False)
    backtest_runs.to_parquet(run_dir / "backtest_runs.parquet", index=False)

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace = []
    _install_runtime_fakes(monkeypatch, root=root, counters=counters, backtest_trace=backtest_trace)

    summary = run_experiment_suite(cfg=cfg, suite_name="recover_summary_suite", run_label="recover-summary-exp")
    run_dir = Path(summary["run_dir"])
    training_runs = pd.read_parquet(run_dir / "training_runs.parquet").sort_values("stake_usd").reset_index(drop=True)
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet").sort_values("stake_usd").reset_index(drop=True)

    assert counters == {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    assert backtest_trace == []
    assert training_runs["stake_usd"].tolist() == [1.0, 5.0]
    assert backtest_runs["stake_usd"].tolist() == [1.0, 5.0]
    assert training_runs["resumed_from_existing"].tolist() == [True, True]
    assert backtest_runs["resumed_from_existing"].tolist() == [True, True]


def test_run_experiment_suite_reruns_completed_cases_when_runtime_policy_requests_it(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="rerun_completed_suite",
        cycle="15m",
        markets=(
            _MarketSpec(market="sol", group_name="core", run_name="run-a", variant_label="baseline"),
            _MarketSpec(market="sol", group_name="core", run_name="run-b", variant_label="alt"),
        ),
        runtime_policy={"completed_cases": "rerun"},
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace: list[dict[str, object]] = []
    _install_runtime_fakes(monkeypatch, root=root, counters=counters, backtest_trace=backtest_trace)
    run_experiment_suite(cfg=cfg, suite_name="rerun_completed_suite", run_label="rerun-completed-exp")

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace = []
    _install_runtime_fakes(monkeypatch, root=root, counters=counters, backtest_trace=backtest_trace)

    summary = run_experiment_suite(cfg=cfg, suite_name="rerun_completed_suite", run_label="rerun-completed-exp")
    run_dir = Path(summary["run_dir"])
    training_runs = pd.read_parquet(run_dir / "training_runs.parquet").sort_values("variant_label").reset_index(drop=True)
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet").sort_values("variant_label").reset_index(drop=True)

    assert summary["runtime_policy"] == {
        "completed_cases": "rerun",
        "failed_cases": "rerun",
        "parallel_case_workers": 1,
    }
    assert counters == {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 2}
    assert len(backtest_trace) == 2
    assert training_runs["training_reused"].tolist() == [True, True]
    assert training_runs["bundle_reused"].tolist() == [True, True]
    assert training_runs["resumed_from_existing"].tolist() == [False, False]
    assert backtest_runs["resumed_from_existing"].tolist() == [False, False]


def test_run_experiment_suite_captures_failed_cases_and_reruns_only_failures(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="failed_suite",
        cycle="15m",
        markets=(
            _MarketSpec(market="sol", group_name="core", run_name="run-a", variant_label="baseline"),
            _MarketSpec(market="sol", group_name="core", run_name="run-b", variant_label="alt"),
        ),
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")
    monkeypatch.setattr("pm15min.research.experiments.runner.build_feature_frame_dataset", lambda cfg: {"ok": True})
    monkeypatch.setattr("pm15min.research.experiments.runner.build_label_frame_dataset", lambda cfg: {"ok": True})

    counters = {"train": 0, "bundle": 0, "backtest": 0}

    def _fake_train(cfg, spec):
        counters["train"] += 1
        run_dir = root / "research" / "training_runs" / spec.run_label
        run_dir.mkdir(parents=True, exist_ok=True)
        return {"run_dir": str(run_dir), "run_label": spec.run_label}

    def _fake_bundle(cfg, spec):
        counters["bundle"] += 1
        bundle_dir = root / "research" / "model_bundles" / spec.bundle_label
        bundle_dir.mkdir(parents=True, exist_ok=True)
        return {"bundle_dir": str(bundle_dir), "bundle_label": spec.bundle_label}

    state = {"fail_alt": True}

    def _fake_backtest(cfg, spec):
        counters["backtest"] += 1
        if spec.variant_label == "alt" and state["fail_alt"]:
            raise RuntimeError("alt backtest failed")
        run_dir = root / "research" / "backtests" / spec.run_label
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps({"trades": 1, "rejects": 0, "wins": 1, "losses": 0, "pnl_sum": 1.0, "stake_sum": 1.0, "roi_pct": 100.0}),
            encoding="utf-8",
        )
        return {"run_dir": str(run_dir), "summary_path": str(summary_path)}

    monkeypatch.setattr("pm15min.research.experiments.runner.train_research_run", _fake_train)
    monkeypatch.setattr("pm15min.research.experiments.runner.build_model_bundle", _fake_bundle)
    monkeypatch.setattr("pm15min.research.experiments.runner.run_research_backtest", _fake_backtest)

    summary = run_experiment_suite(cfg=cfg, suite_name="failed_suite", run_label="failed-exp")
    run_dir = Path(summary["run_dir"])
    failed_cases = pd.read_parquet(run_dir / "failed_cases.parquet").sort_values("variant_label").reset_index(drop=True)
    compare_df = pd.read_parquet(run_dir / "compare.parquet").sort_values("variant_label").reset_index(drop=True)
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet").sort_values("variant_label").reset_index(drop=True)

    assert (run_dir / "failed_cases.csv").exists()
    assert counters == {"train": 1, "bundle": 1, "backtest": 2}
    assert len(backtest_runs) == 1
    assert backtest_runs.loc[0, "variant_label"] == "baseline"
    assert len(failed_cases) == 1
    assert failed_cases.loc[0, "variant_label"] == "alt"
    assert failed_cases.loc[0, "failure_stage"] == "backtest"
    assert compare_df["variant_label"].tolist() == ["alt", "baseline"]
    assert compare_df["status"].tolist() == ["failed", "completed"]

    state["fail_alt"] = False
    counters = {"train": 0, "bundle": 0, "backtest": 0}
    monkeypatch.setattr("pm15min.research.experiments.runner.train_research_run", _fake_train)
    monkeypatch.setattr("pm15min.research.experiments.runner.build_model_bundle", _fake_bundle)
    monkeypatch.setattr("pm15min.research.experiments.runner.run_research_backtest", _fake_backtest)

    summary = run_experiment_suite(cfg=cfg, suite_name="failed_suite", run_label="failed-exp")
    run_dir = Path(summary["run_dir"])
    failed_cases = pd.read_parquet(run_dir / "failed_cases.parquet")
    compare_df = pd.read_parquet(run_dir / "compare.parquet").sort_values("variant_label").reset_index(drop=True)
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet").sort_values("variant_label").reset_index(drop=True)
    training_runs = pd.read_parquet(run_dir / "training_runs.parquet").sort_values("variant_label").reset_index(drop=True)

    assert counters == {"train": 0, "bundle": 0, "backtest": 1}
    assert failed_cases.empty
    assert compare_df["status"].tolist() == ["completed", "completed"]
    assert backtest_runs["variant_label"].tolist() == ["alt", "baseline"]
    assert training_runs["resumed_from_existing"].tolist() == [False, True]


def test_run_experiment_suite_retains_failed_cases_when_runtime_policy_skips_reruns(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="retain_failed_suite",
        cycle="15m",
        markets=(
            _MarketSpec(market="sol", group_name="core", run_name="run-a", variant_label="baseline"),
            _MarketSpec(market="sol", group_name="core", run_name="run-b", variant_label="alt"),
        ),
        runtime_policy={"failed_cases": "skip"},
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")
    monkeypatch.setattr("pm15min.research.experiments.runner.build_feature_frame_dataset", lambda cfg: {"ok": True})
    monkeypatch.setattr("pm15min.research.experiments.runner.build_label_frame_dataset", lambda cfg: {"ok": True})

    counters = {"train": 0, "bundle": 0, "backtest": 0}

    def _fake_train(cfg, spec):
        counters["train"] += 1
        run_dir = root / "research" / "training_runs" / spec.run_label
        run_dir.mkdir(parents=True, exist_ok=True)
        return {"run_dir": str(run_dir), "run_label": spec.run_label}

    def _fake_bundle(cfg, spec):
        counters["bundle"] += 1
        bundle_dir = root / "research" / "model_bundles" / spec.bundle_label
        bundle_dir.mkdir(parents=True, exist_ok=True)
        return {"bundle_dir": str(bundle_dir), "bundle_label": spec.bundle_label}

    state = {"fail_alt": True}

    def _fake_backtest(cfg, spec):
        counters["backtest"] += 1
        if spec.variant_label == "alt" and state["fail_alt"]:
            raise RuntimeError("alt backtest failed")
        run_dir = root / "research" / "backtests" / spec.run_label
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps({"trades": 1, "rejects": 0, "wins": 1, "losses": 0, "pnl_sum": 1.0, "stake_sum": 1.0, "roi_pct": 100.0}),
            encoding="utf-8",
        )
        return {"run_dir": str(run_dir), "summary_path": str(summary_path)}

    monkeypatch.setattr("pm15min.research.experiments.runner.train_research_run", _fake_train)
    monkeypatch.setattr("pm15min.research.experiments.runner.build_model_bundle", _fake_bundle)
    monkeypatch.setattr("pm15min.research.experiments.runner.run_research_backtest", _fake_backtest)

    run_experiment_suite(cfg=cfg, suite_name="retain_failed_suite", run_label="retain-failed-exp")

    state["fail_alt"] = False
    counters = {"train": 0, "bundle": 0, "backtest": 0}
    monkeypatch.setattr("pm15min.research.experiments.runner.train_research_run", _fake_train)
    monkeypatch.setattr("pm15min.research.experiments.runner.build_model_bundle", _fake_bundle)
    monkeypatch.setattr("pm15min.research.experiments.runner.run_research_backtest", _fake_backtest)

    summary = run_experiment_suite(cfg=cfg, suite_name="retain_failed_suite", run_label="retain-failed-exp")
    run_dir = Path(summary["run_dir"])
    failed_cases = pd.read_parquet(run_dir / "failed_cases.parquet").sort_values("variant_label").reset_index(drop=True)
    compare_df = pd.read_parquet(run_dir / "compare.parquet").sort_values("variant_label").reset_index(drop=True)
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet").sort_values("variant_label").reset_index(drop=True)
    suite_log = (run_dir / "logs" / "suite.jsonl").read_text(encoding="utf-8")

    assert summary["runtime_policy"] == {
        "completed_cases": "resume",
        "failed_cases": "skip",
        "parallel_case_workers": 1,
    }
    assert counters == {"train": 0, "bundle": 0, "backtest": 0}
    assert failed_cases["variant_label"].tolist() == ["alt"]
    assert compare_df["status"].tolist() == ["failed", "completed"]
    assert backtest_runs["variant_label"].tolist() == ["baseline"]
    assert "market_failed_retained" in suite_log


def test_run_experiment_suite_parallelizes_remaining_group_cases_when_enabled(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="parallel_cases_suite",
        cycle="15m",
        markets=(
            _MarketSpec(market="sol", group_name="core", run_name="run-a", stake_usd=1.0, max_notional_usd=8.0),
            _MarketSpec(market="sol", group_name="core", run_name="run-a", stake_usd=5.0, max_notional_usd=8.0),
            _MarketSpec(market="sol", group_name="core", run_name="run-a", stake_usd=10.0, max_notional_usd=8.0),
        ),
        runtime_policy={"parallel_case_workers": 2},
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")
    monkeypatch.setattr("pm15min.research.experiments.runner.build_feature_frame_dataset", lambda cfg: {"ok": True})
    monkeypatch.setattr("pm15min.research.experiments.runner.build_label_frame_dataset", lambda cfg: {"ok": True})

    counters = {"train": 0, "bundle": 0, "backtest": 0}
    backtest_trace: list[dict[str, object]] = []
    active = {"count": 0, "max": 0}
    barrier = threading.Barrier(2)
    lock = threading.Lock()

    def _fake_train(cfg, spec, reporter=None):
        counters["train"] += 1
        run_dir = root / "research" / "training_runs" / spec.run_label
        run_dir.mkdir(parents=True, exist_ok=True)
        return {"run_dir": str(run_dir), "run_label": spec.run_label}

    def _fake_bundle(cfg, spec, reporter=None):
        counters["bundle"] += 1
        bundle_dir = root / "research" / "model_bundles" / spec.bundle_label
        bundle_dir.mkdir(parents=True, exist_ok=True)
        return {"bundle_dir": str(bundle_dir), "bundle_label": spec.bundle_label}

    def _fake_backtest(cfg, spec, reporter=None):
        del reporter
        counters["backtest"] += 1
        with lock:
            active["count"] += 1
            active["max"] = max(active["max"], active["count"])
        try:
            if float(spec.stake_usd or 0.0) > 1.0:
                barrier.wait(timeout=2)
                time.sleep(0.05)
            backtest_trace.append({"stake_usd": spec.stake_usd})
            run_dir = root / "research" / "backtests" / spec.run_label
            run_dir.mkdir(parents=True, exist_ok=True)
            summary_path = run_dir / "summary.json"
            summary_path.write_text(
                json.dumps({"trades": 1, "rejects": 0, "wins": 1, "losses": 0, "pnl_sum": 1.0, "stake_sum": 1.0, "roi_pct": 100.0}),
                encoding="utf-8",
            )
            return {"run_dir": str(run_dir), "summary_path": str(summary_path)}
        finally:
            with lock:
                active["count"] -= 1

    monkeypatch.setattr("pm15min.research.experiments.runner.train_research_run", _fake_train)
    monkeypatch.setattr("pm15min.research.experiments.runner.build_model_bundle", _fake_bundle)
    monkeypatch.setattr("pm15min.research.experiments.runner.run_research_backtest", _fake_backtest)

    events: list[dict[str, object]] = []
    summary = run_experiment_suite(
        cfg=cfg,
        suite_name="parallel_cases_suite",
        run_label="parallel-exp",
        reporter=lambda **payload: events.append(payload),
    )
    suite_log = (Path(summary["run_dir"]) / "logs" / "suite.jsonl").read_text(encoding="utf-8")

    assert summary["runtime_policy"] == {
        "completed_cases": "resume",
        "failed_cases": "rerun",
        "parallel_case_workers": 2,
    }
    assert counters == {"train": 1, "bundle": 1, "backtest": 3}
    assert active["max"] >= 2
    assert sorted(row["stake_usd"] for row in backtest_trace) == [1.0, 5.0, 10.0]
    assert any(event["current_stage"] == "experiment_group_warmup" for event in events)
    assert any(event["current_stage"] == "experiment_group_parallel" for event in events)
    assert any("Queued case" in str(event["summary"]) for event in events)
    assert any("Launching 2 parallel cases" in str(event["summary"]) for event in events)
    assert any("Collected parallel case" in str(event["summary"]) for event in events)
    assert '"event": "execution_group_seed_case_started"' in suite_log
    assert '"event": "execution_group_parallel_started"' in suite_log
    assert '"event": "execution_group_parallel_completed"' in suite_log


def test_run_experiment_suite_surfaces_pre_training_failures_in_compare_and_report(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="prepare_failed_suite",
        cycle="15m",
        markets=(
            _MarketSpec(market="sol", group_name="core", run_name="run-a", variant_label="baseline"),
            _MarketSpec(
                market="sol",
                group_name="core",
                run_name="run-b",
                variant_label="broken",
                feature_set="broken_v1",
            ),
        ),
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")

    def _fake_features(cfg):
        if cfg.feature_set == "broken_v1":
            raise RuntimeError("feature build failed")
        return {"ok": True}

    monkeypatch.setattr("pm15min.research.experiments.runner.build_feature_frame_dataset", _fake_features)
    monkeypatch.setattr("pm15min.research.experiments.runner.build_label_frame_dataset", lambda cfg: {"ok": True})

    def _fake_train(cfg, spec):
        run_dir = root / "research" / "training_runs" / spec.run_label
        run_dir.mkdir(parents=True, exist_ok=True)
        return {"run_dir": str(run_dir), "run_label": spec.run_label}

    def _fake_bundle(cfg, spec):
        bundle_dir = root / "research" / "model_bundles" / spec.bundle_label
        bundle_dir.mkdir(parents=True, exist_ok=True)
        return {"bundle_dir": str(bundle_dir), "bundle_label": spec.bundle_label}

    def _fake_backtest(cfg, spec):
        run_dir = root / "research" / "backtests" / spec.run_label
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps({"trades": 1, "rejects": 0, "wins": 1, "losses": 0, "pnl_sum": 1.0, "stake_sum": 1.0, "roi_pct": 100.0}),
            encoding="utf-8",
        )
        return {"run_dir": str(run_dir), "summary_path": str(summary_path)}

    monkeypatch.setattr("pm15min.research.experiments.runner.train_research_run", _fake_train)
    monkeypatch.setattr("pm15min.research.experiments.runner.build_model_bundle", _fake_bundle)
    monkeypatch.setattr("pm15min.research.experiments.runner.run_research_backtest", _fake_backtest)

    summary = run_experiment_suite(cfg=cfg, suite_name="prepare_failed_suite", run_label="prepare-failed-exp")
    run_dir = Path(summary["run_dir"])
    compare_df = pd.read_parquet(run_dir / "compare.parquet").sort_values("run_name").reset_index(drop=True)
    failed_cases = pd.read_parquet(run_dir / "failed_cases.parquet").sort_values("run_name").reset_index(drop=True)
    summary_payload = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    report_text = (run_dir / "report.md").read_text(encoding="utf-8")

    assert compare_df["status"].tolist() == ["completed", "failed"]
    assert compare_df["failure_stage"].fillna("").tolist() == ["", "prepare_datasets"]
    assert len(failed_cases) == 1
    assert failed_cases.loc[0, "run_name"] == "run-b"
    assert summary_payload["cases"] == 2
    assert summary_payload["completed_cases"] == 1
    assert summary_payload["failed_cases"] == 1
    assert "## Failures" in report_text
    assert "feature build failed" in report_text


def test_run_experiment_suite_reuses_cross_run_shared_cache(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = _build_cfg(root)
    suite = _Suite(
        suite_name="shared_cache_suite",
        cycle="15m",
        markets=(
            _MarketSpec(market="sol", group_name="core", run_name="run-a", variant_label="baseline"),
            _MarketSpec(market="sol", group_name="core", run_name="run-b", variant_label="alt"),
        ),
    )
    monkeypatch.setattr("pm15min.research.experiments.runner.load_suite_definition", lambda path: suite)
    monkeypatch.setattr("pm15min.research.experiments.runner._resolve_suite_spec_path", lambda cfg, suite_name: root / "suite.json")

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace: list[dict[str, object]] = []
    _install_runtime_fakes(monkeypatch, root=root, counters=counters, backtest_trace=backtest_trace)

    run_experiment_suite(cfg=cfg, suite_name="shared_cache_suite", run_label="cache-exp-a")
    assert counters == {"feature": 1, "label": 1, "train": 1, "bundle": 1, "backtest": 2}

    counters = {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 0}
    backtest_trace = []
    _install_runtime_fakes(monkeypatch, root=root, counters=counters, backtest_trace=backtest_trace)

    summary = run_experiment_suite(cfg=cfg, suite_name="shared_cache_suite", run_label="cache-exp-b")
    run_dir = Path(summary["run_dir"])
    training_runs = pd.read_parquet(run_dir / "training_runs.parquet").sort_values("variant_label").reset_index(drop=True)

    assert counters == {"feature": 0, "label": 0, "train": 0, "bundle": 0, "backtest": 2}
    assert len(backtest_trace) == 2
    assert training_runs["training_reused"].tolist() == [True, True]
    assert training_runs["bundle_reused"].tolist() == [True, True]
