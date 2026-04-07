from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

import joblib
import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.queries.loaders import load_binance_klines_1m
from pm15min.research.backtests.build_signature import backtest_build_signature
from pm15min.research.backtests.decision_quote_surface import (
    InitialSnapshotDecisionSummary,
    attach_initial_snapshot_decision_surface,
    apply_initial_snapshot_decision_parity,
    build_empty_initial_snapshot_decision_summary,
)
from pm15min.research.backtests.decision_engine_parity import (
    DECISION_ENGINE_PARITY_COLUMNS,
    DecisionEngineParityConfig,
    apply_decision_engine_parity,
    build_profile_decision_engine_parity_config,
)
from pm15min.research.backtests.depth_replay import DepthReplaySummary, build_raw_depth_replay_frame
from pm15min.research.backtests.data_surface_fallback import preflight_orderbook_index_dates
from pm15min.research.backtests.fills import BacktestFillConfig, build_canonical_fills, build_depth_candidate_lookup
from pm15min.research.backtests.guard_parity import GuardParitySummary
from pm15min.research.backtests.hybrid import apply_hybrid_fallback
from pm15min.research.backtests.live_state_parity import attach_live_state_parity
from pm15min.research.backtests.orderbook_surface import attach_canonical_quote_surface
from pm15min.research.backtests.policy import BacktestPolicyConfig, build_policy_decisions, build_policy_reject_frame
from pm15min.research.backtests.regime_parity import resolve_backtest_profile_spec
from pm15min.research.backtests.replay_loader import build_replay_frame
from pm15min.research.backtests.retry_contract import (
    attach_pre_submit_orderbook_retry_contract,
    build_backtest_retry_contract,
)
from pm15min.research.backtests.runtime_cache import (
    BacktestPreparedRuntime,
    BacktestSharedRuntimeKey,
    process_backtest_runtime_cache,
    snapshot_source_mtimes,
)
from pm15min.research.backtests.reports import (
    build_backtest_summary,
    build_factor_pnl_frame,
    build_offset_summary_frame,
    build_policy_breakdown_frame,
    build_reject_summary_frame,
    build_stake_sweep_frame,
    render_backtest_report,
)
from pm15min.research.backtests.settlement import build_equity_curve, build_market_summary, settle_trade_fills
from pm15min.research.bundles.loader import (
    read_model_bundle_manifest,
    resolve_model_bundle_dir,
)
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import BacktestRunSpec
from pm15min.research.datasets.loaders import load_feature_frame, load_label_frame
from pm15min.research.freshness import prepare_research_artifacts
from pm15min.research.inference.scorer import score_bundle_offset
from pm15min.research.labels.alignment import merge_feature_and_label_frames
from pm15min.research.labels.sources import normalize_label_set
from pm15min.research.labels.runtime import build_truth_runtime_summary
from pm15min.research.manifests import build_manifest, read_manifest, write_manifest


_BACKTEST_FEATURE_KEY_COLUMNS = ("decision_ts", "cycle_start_ts", "cycle_end_ts", "offset")
_BACKTEST_LABEL_REQUIRED_COLUMNS = (
    "asset",
    "cycle_start_ts",
    "cycle_end_ts",
    "market_id",
    "condition_id",
    "label_set",
    "settlement_source",
    "label_source",
    "resolved",
    "price_to_beat",
    "final_price",
    "winner_side",
    "direction_up",
    "full_truth",
)
_REVERSAL_ANCHOR_COLUMNS = ("ret_from_strike", "ret_from_cycle_open")


class BacktestReporter(Protocol):
    def __call__(
        self,
        *,
        summary: str,
        current: int | None = None,
        total: int | None = None,
        current_stage: str | None = None,
        progress_pct: int | None = None,
        heartbeat: str | None = None,
    ) -> None: ...


class _BacktestProgressReporter:
    # Keep the reporter shape compatible with console task progress without importing console into research.
    def __init__(self, reporter: BacktestReporter | None, *, total_stages: int) -> None:
        self._reporter = reporter
        self._total_stages = max(1, int(total_stages))

    def stage(self, *, stage_index: int, stage_name: str, summary: str) -> None:
        self._emit(
            summary=summary,
            stage_index=stage_index,
            stage_name=stage_name,
            progress_pct=_stage_progress_pct(stage_index=stage_index, total_stages=self._total_stages),
        )

    def heartbeat(self, *, stage_index: int, stage_name: str) -> Callable[[str], None] | None:
        if self._reporter is None:
            return None

        def _heartbeat(summary: str) -> None:
            self._emit(
                summary=summary,
                stage_index=stage_index,
                stage_name=stage_name,
                progress_pct=_stage_progress_pct(stage_index=stage_index, total_stages=self._total_stages),
            )

        return _heartbeat

    def complete(self, *, summary: str) -> None:
        if self._reporter is None:
            return
        self._reporter(
            summary=summary,
            current=self._total_stages,
            total=self._total_stages,
            current_stage="finished",
            progress_pct=100,
            heartbeat=_utc_timestamp(),
        )

    def _emit(
        self,
        *,
        summary: str,
        stage_index: int,
        stage_name: str,
        progress_pct: int,
    ) -> None:
        if self._reporter is None:
            return
        current = min(max(int(stage_index), 1), self._total_stages)
        self._reporter(
            summary=summary,
            current=current,
            total=self._total_stages,
            current_stage=stage_name,
            progress_pct=progress_pct,
            heartbeat=_utc_timestamp(),
        )


def run_research_backtest(
    cfg: ResearchConfig,
    spec: BacktestRunSpec,
    *,
    reporter: BacktestReporter | None = None,
    dependency_mode: str = "auto_repair",
) -> dict[str, object]:
    progress = _BacktestProgressReporter(reporter, total_stages=_backtest_stage_total(spec=spec))
    stage_index = 0

    def _start_stage(stage_name: str, summary: str) -> tuple[int, str]:
        nonlocal stage_index
        stage_index += 1
        progress.stage(stage_index=stage_index, stage_name=stage_name, summary=summary)
        return stage_index, stage_name

    _start_stage("load_inputs", "Loading backtest inputs")
    bundle_dir = resolve_model_bundle_dir(
        cfg,
        profile=spec.profile,
        target=spec.target,
        bundle_label=spec.bundle_label,
    )
    bundle_manifest = read_model_bundle_manifest(bundle_dir)
    feature_set = str(bundle_manifest.spec.get("feature_set") or cfg.feature_set)
    label_set = normalize_label_set(str(bundle_manifest.spec.get("label_set") or cfg.label_set))
    prepare_research_artifacts(
        cfg,
        feature_set=feature_set,
        label_set=label_set,
        mode=dependency_mode,
    )

    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        surface=cfg.source_surface,
        root=cfg.layout.storage.rewrite_root,
    )
    profile_spec = resolve_backtest_profile_spec(
        market=cfg.asset.slug,
        profile=spec.profile,
        parity=spec.parity,
    )
    liquidity_proxy_mode = str(spec.parity.liquidity_proxy_mode or "spot_kline_mirror")
    secondary_bundle_dir, secondary_target = _resolve_secondary_bundle_context(
        cfg=cfg,
        spec=spec,
    )
    required_bundle_dirs = tuple(
        bundle for bundle in (bundle_dir, secondary_bundle_dir) if bundle is not None
    )
    required_targets = tuple(
        target_text
        for target_text in (spec.target, secondary_target if secondary_bundle_dir is not None else None)
        if target_text
    )
    required_offsets = sorted(
        {
            *(_bundle_offsets(bundle_dir)),
            *(_bundle_offsets(secondary_bundle_dir) if secondary_bundle_dir is not None else []),
        }
    )
    retry_contract_summary = build_backtest_retry_contract(profile_spec)
    fill_config = _build_backtest_fill_config(spec=spec, profile_spec=profile_spec)
    orderbook_preflight_summary: dict[str, object] | None = None
    prepared_runtime = _load_cached_primary_runtime(
        cfg=cfg,
        spec=spec,
        bundle_dir=bundle_dir,
        feature_set=feature_set,
        label_set=label_set,
        profile_spec=profile_spec,
        liquidity_proxy_mode=liquidity_proxy_mode,
    )
    runtime_cache_status = "reused" if prepared_runtime is not None else "built"
    if prepared_runtime is None:
        features = _load_scoped_backtest_feature_frame(
            cfg=cfg,
            feature_set=feature_set,
            bundle_dirs=required_bundle_dirs,
            targets=required_targets,
            available_offsets=required_offsets,
            decision_start=spec.decision_start,
            decision_end=spec.decision_end,
        )
        labels = _load_scoped_backtest_label_frame(
            cfg=cfg,
            label_set=label_set,
            scoped_features=features,
        )
        raw_klines = _scope_backtest_klines(
            load_binance_klines_1m(data_cfg),
            decision_start=spec.decision_start,
            decision_end=spec.decision_end,
            required_lookback_minutes=_required_backtest_klines_lookback_minutes(profile_spec),
        )
        _start_stage("bundle_replay", "Scoring bundle replay")
        replay, replay_summary, available_offsets = _build_bundle_replay(
            bundle_dir=bundle_dir,
            features=features,
            labels=labels,
        )
        replay = _filter_replay_window(
            replay,
            decision_start=spec.decision_start,
            decision_end=spec.decision_end,
        )
        preflight_stage_index, preflight_stage_name = _start_stage("orderbook_preflight", "Preflighting orderbook coverage")
        orderbook_preflight_summary = _build_orderbook_preflight_summary(
            data_cfg=data_cfg,
            replay=replay,
            decision_start=spec.decision_start,
            decision_end=spec.decision_end,
            heartbeat=progress.heartbeat(stage_index=preflight_stage_index, stage_name=preflight_stage_name),
        )
        depth_stage_index, depth_stage_name = _start_stage("depth_replay", "Replaying raw depth snapshots")
        depth_replay, depth_replay_summary, depth_candidate_lookup = _build_decision_depth_runtime(
            replay=replay,
            data_cfg=data_cfg,
            fill_config=fill_config,
            heartbeat=progress.heartbeat(stage_index=depth_stage_index, stage_name=depth_stage_name),
        )
        quote_stage_index, quote_stage_name = _start_stage("quote_surface", "Attaching quote surface")
        live_state_stage_index, live_state_stage_name = _start_stage("live_state_surface", "Attaching live state parity")
        runtime_replay, quote_summary, state_summary = _attach_replay_runtime_surface(
            replay=replay,
            data_cfg=data_cfg,
            market=cfg.asset.slug,
            raw_klines=raw_klines,
            profile_spec=profile_spec,
            liquidity_proxy_mode=liquidity_proxy_mode,
            quote_heartbeat=progress.heartbeat(stage_index=quote_stage_index, stage_name=quote_stage_name),
            live_state_heartbeat=progress.heartbeat(stage_index=live_state_stage_index, stage_name=live_state_stage_name),
        )
        prepared_runtime = _store_primary_runtime(
            cfg=cfg,
            spec=spec,
            bundle_dir=bundle_dir,
            feature_set=feature_set,
            label_set=label_set,
            profile_spec=profile_spec,
            liquidity_proxy_mode=liquidity_proxy_mode,
            prepared=BacktestPreparedRuntime(
                bundle_dir=str(bundle_dir),
                feature_set=feature_set,
                label_set=label_set,
                features=features,
                labels=labels,
                raw_klines=raw_klines,
                available_offsets=tuple(int(value) for value in available_offsets),
                replay=replay,
                replay_summary=replay_summary,
                depth_replay=depth_replay,
                depth_replay_summary=depth_replay_summary,
                depth_candidate_lookup=depth_candidate_lookup,
                runtime_replay=runtime_replay,
                quote_summary=quote_summary,
                state_summary=state_summary,
                source_mtimes=snapshot_source_mtimes(
                    _prepared_runtime_source_paths(
                        cfg=cfg,
                        data_cfg=data_cfg,
                        bundle_dir=bundle_dir,
                        feature_set=feature_set,
                        label_set=label_set,
                        depth_replay=depth_replay,
                        runtime_replay=runtime_replay,
                    )
                ),
            ),
        )
    else:
        _start_stage("bundle_replay", "Reusing cached bundle replay")
        _start_stage("depth_replay", "Reusing cached depth replay")
        _start_stage("quote_surface", "Reusing cached quote surface")
        _start_stage("live_state_surface", "Reusing cached live state parity")
    features = prepared_runtime.features
    labels = prepared_runtime.labels
    raw_klines = prepared_runtime.raw_klines
    replay = prepared_runtime.runtime_replay
    replay_summary = prepared_runtime.replay_summary
    available_offsets = list(prepared_runtime.available_offsets)
    depth_replay = prepared_runtime.depth_replay
    depth_replay_summary = prepared_runtime.depth_replay_summary
    depth_candidate_lookup = prepared_runtime.depth_candidate_lookup
    quote_summary = prepared_runtime.quote_summary
    state_summary = prepared_runtime.state_summary

    replay = _filter_replay_window(
        replay.copy(deep=False),
        decision_start=spec.decision_start,
        decision_end=spec.decision_end,
    )
    _ensure_replay_labels_resolved(replay)
    if orderbook_preflight_summary is None:
        preflight_stage_index, preflight_stage_name = _start_stage("orderbook_preflight", "Preflighting orderbook coverage")
        orderbook_preflight_summary = _build_orderbook_preflight_summary(
            data_cfg=data_cfg,
            replay=replay,
            decision_start=spec.decision_start,
            decision_end=spec.decision_end,
            heartbeat=progress.heartbeat(stage_index=preflight_stage_index, stage_name=preflight_stage_name),
        )
    label_runtime_summary = _load_label_runtime_summary(cfg=cfg, label_set=label_set)
    truth_runtime_summary = build_truth_runtime_summary(data_cfg)
    build_signature = backtest_build_signature()
    policy_stage_index, policy_stage_name = _start_stage("policy_decisions", "Building policy decisions")
    decisions, guard_summary, decision_quote_summary = _build_guarded_policy_decisions(
        replay=replay,
        market=cfg.asset.slug,
        profile=spec.profile,
        profile_spec=profile_spec,
        model_source="primary",
        depth_replay=depth_replay,
        fill_config=fill_config,
        heartbeat=progress.heartbeat(stage_index=policy_stage_index, stage_name=policy_stage_name),
    )
    if spec.secondary_bundle_label:
        _start_stage("secondary_decisions", "Building hybrid fallback decisions")
        secondary_replay = _build_secondary_replay(
            cfg=cfg,
            spec=spec,
            features=features,
            labels=labels,
        )
        secondary_replay = _filter_replay_window(
            secondary_replay,
            decision_start=spec.decision_start,
            decision_end=spec.decision_end,
        )
        secondary_replay, _secondary_quote_summary, _secondary_state_summary = _attach_replay_runtime_surface(
            replay=secondary_replay,
            data_cfg=data_cfg,
            market=cfg.asset.slug,
            raw_klines=raw_klines,
            profile_spec=profile_spec,
            liquidity_proxy_mode=liquidity_proxy_mode,
        )
        secondary_decisions, _, _secondary_decision_quote_summary = _build_guarded_policy_decisions(
            replay=secondary_replay,
            market=cfg.asset.slug,
            profile=spec.profile,
            profile_spec=profile_spec,
            model_source="secondary",
            depth_replay=depth_replay,
            fill_config=fill_config,
        )
        decisions = apply_hybrid_fallback(
            decisions,
            secondary_decisions,
            fallback_reasons=spec.fallback_reasons or ("direction_prob", "policy_low_confidence"),
        )
    fills_stage_index, fills_stage_name = _start_stage("fills_materialization", "Materializing canonical fills")
    policy_rejects = build_policy_reject_frame(decisions)
    accepted = decisions.loc[decisions["policy_action"].eq("trade")].copy()
    fill_depth_replay = depth_replay
    fill_depth_candidate_lookup = depth_candidate_lookup
    if bool(fill_config.raw_depth_fak_refresh_enabled) or depth_replay.empty:
        fill_depth_replay, _fill_depth_replay_summary, fill_depth_candidate_lookup = _build_fill_depth_runtime(
            accepted=accepted,
            data_cfg=data_cfg,
            heartbeat=progress.heartbeat(stage_index=fills_stage_index, stage_name=fills_stage_name),
        )
    fills, fill_rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=fill_config,
        profile_spec=profile_spec,
        depth_replay=fill_depth_replay,
        depth_candidate_lookup=fill_depth_candidate_lookup,
        heartbeat=progress.heartbeat(stage_index=fills_stage_index, stage_name=fills_stage_name),
    )
    _start_stage("settlement_summary", "Settling fills and building summaries")
    trades = settle_trade_fills(fills)
    rejects = pd.concat([policy_rejects, fill_rejects], ignore_index=True, sort=False).reset_index(drop=True)
    markets = build_market_summary(trades)
    equity_curve = build_equity_curve(trades)

    _start_stage("write_outputs", "Writing backtest artifacts")
    run_dir = cfg.layout.backtest_run_dir(
        profile=spec.profile,
        spec_name=spec.spec_name,
        run_label_text=spec.run_label,
    )
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    trades_path = run_dir / "trades.parquet"
    rejects_path = run_dir / "rejects.parquet"
    decisions_path = run_dir / "decisions.parquet"
    markets_path = run_dir / "markets.parquet"
    equity_curve_path = run_dir / "equity_curve.parquet"
    stake_sweep_path = run_dir / "stake_sweep.parquet"
    offset_summary_path = run_dir / "offset_summary.parquet"
    factor_pnl_path = run_dir / "factor_pnl.parquet"
    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"
    log_path = logs_dir / "backtest.jsonl"

    write_parquet_atomic(trades, trades_path)
    write_parquet_atomic(rejects, rejects_path)
    write_parquet_atomic(_serialize_decision_frame(decisions), decisions_path)
    write_parquet_atomic(markets, markets_path)
    write_parquet_atomic(equity_curve, equity_curve_path)

    summary = build_backtest_summary(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        profile=spec.profile,
        spec_name=spec.spec_name,
        target=spec.target,
        bundle_dir=str(bundle_dir),
        feature_set=feature_set,
        label_set=label_set,
        available_offsets=available_offsets,
        replay_summary=replay_summary.to_dict(),
        depth_replay_summary=depth_replay_summary.to_dict(),
        decision_quote_summary=decision_quote_summary.to_dict(),
        retry_contract_summary=retry_contract_summary.to_dict(),
        label_runtime_summary=label_runtime_summary,
        truth_runtime_summary=truth_runtime_summary,
        quote_summary=quote_summary.to_dict(),
        guard_summary=guard_summary.to_dict(),
        regime_summary=state_summary.to_dict(),
        scored=decisions,
        trades=trades,
        rejects=rejects,
        variant_label=spec.variant_label,
        secondary_bundle_label=spec.secondary_bundle_label,
        stake_usd=spec.stake_usd,
        max_notional_usd=spec.max_notional_usd,
        fallback_reasons=spec.fallback_reasons,
        parity=spec.parity.to_dict(),
        orderbook_preflight_summary=orderbook_preflight_summary,
    )
    summary["run_label"] = spec.run_label
    summary["backtest_build_signature"] = build_signature
    summary["shared_runtime_cache_status"] = runtime_cache_status
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    stake_sweep = build_stake_sweep_frame(summary=summary)
    offset_summary = build_offset_summary_frame(
        decisions=decisions,
        trades=trades,
        rejects=rejects,
        available_offsets=available_offsets,
    )
    factor_pnl = build_factor_pnl_frame(
        decisions=decisions,
        trades=trades,
        factor_columns=_bundle_feature_columns(bundle_dir),
    )
    write_parquet_atomic(stake_sweep, stake_sweep_path)
    write_parquet_atomic(offset_summary, offset_summary_path)
    write_parquet_atomic(factor_pnl, factor_pnl_path)
    report_path.write_text(
        render_backtest_report(
            summary,
            reject_summary=build_reject_summary_frame(rejects),
            policy_breakdown=build_policy_breakdown_frame(decisions),
            market_summary=markets,
        ),
        encoding="utf-8",
    )

    _append_backtest_log(
        log_path,
        {
            "event": "backtest_runtime_resolved",
            "shared_runtime_cache_status": runtime_cache_status,
            "bundle_dir": str(bundle_dir),
            "available_offsets": available_offsets,
            "decision_start": spec.decision_start,
            "decision_end": spec.decision_end,
        },
    )
    _append_backtest_log(
        log_path,
        {
            "event": "backtest_completed",
            "rows_scored": int(summary.get("score_covered_rows", 0)),
            "trades": int(len(trades)),
            "rejects": int(len(rejects)),
            "bundle_dir": str(bundle_dir),
        },
    )

    manifest = build_manifest(
        object_type="backtest_run",
        object_id=spec.object_id,
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        path=run_dir,
        spec={
            **spec.to_dict(),
            "feature_set": feature_set,
            "label_set": label_set,
            "bundle_dir": str(bundle_dir),
        },
        inputs=[
            {"kind": "model_bundle", "path": str(bundle_dir)},
            {"kind": "feature_frame", "path": str(cfg.layout.feature_frame_path(feature_set, source_surface=cfg.source_surface))},
            {"kind": "label_frame", "path": str(cfg.layout.label_frame_path(label_set))},
        ],
        outputs=[
            {"kind": "summary_json", "path": str(summary_path)},
            {"kind": "trades_parquet", "path": str(trades_path)},
            {"kind": "rejects_parquet", "path": str(rejects_path)},
            {"kind": "decisions_parquet", "path": str(decisions_path)},
            {"kind": "markets_parquet", "path": str(markets_path)},
            {"kind": "equity_curve_parquet", "path": str(equity_curve_path)},
            {"kind": "stake_sweep_parquet", "path": str(stake_sweep_path)},
            {"kind": "offset_summary_parquet", "path": str(offset_summary_path)},
            {"kind": "factor_pnl_parquet", "path": str(factor_pnl_path)},
            {"kind": "report_md", "path": str(report_path)},
            {"kind": "backtest_log", "path": str(log_path)},
        ],
        metadata={
            "rows_scored": int(summary.get("score_covered_rows", 0)),
            "trades": int(len(trades)),
            "rejects": int(len(rejects)),
            "ready_rows": int(summary.get("ready_rows", 0)),
            "backtest_build_signature": build_signature,
            "shared_runtime_cache_status": runtime_cache_status,
        },
    )
    write_manifest(run_dir / "manifest.json", manifest)
    progress.complete(summary="Backtest completed")

    return {
        "dataset": "backtest_run",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "profile": spec.profile,
        "spec_name": spec.spec_name,
        "target": spec.target,
        "bundle_label": spec.bundle_label,
        "secondary_bundle_label": spec.secondary_bundle_label,
        "stake_usd": spec.stake_usd,
        "max_notional_usd": spec.max_notional_usd,
        "fallback_reasons": list(spec.fallback_reasons),
        "parity": spec.parity.to_dict(),
        "variant_label": spec.variant_label,
        "feature_set": feature_set,
        "label_set": label_set,
        "run_label": spec.run_label,
        "shared_runtime_cache_status": runtime_cache_status,
        "trades": int(len(trades)),
        "rejects": int(len(rejects)),
        "orderbook_preflight_summary": dict(orderbook_preflight_summary or {}),
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(run_dir / "manifest.json"),
    }


def _backtest_stage_total(*, spec: BacktestRunSpec) -> int:
    return 11 if spec.secondary_bundle_label else 10


def _build_empty_depth_runtime() -> tuple[pd.DataFrame, DepthReplaySummary, object]:
    depth_replay = pd.DataFrame()
    summary = DepthReplaySummary(
        market_rows_loaded=0,
        replay_rows=0,
        source_files_scanned=0,
        raw_records_scanned=0,
        raw_record_matches=0,
        snapshot_rows=0,
        complete_snapshot_rows=0,
        partial_snapshot_rows=0,
        decision_key_snapshot_rows=0,
        token_window_snapshot_rows=0,
        mixed_strategy_snapshot_rows=0,
        replay_rows_with_snapshots=0,
        replay_rows_without_snapshots=0,
    )
    return depth_replay, summary, build_depth_candidate_lookup(depth_replay)


def _build_decision_depth_runtime(
    *,
    replay: pd.DataFrame,
    data_cfg: DataConfig,
    fill_config: BacktestFillConfig,
    heartbeat: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, DepthReplaySummary, object]:
    if not bool(fill_config.raw_depth_fak_refresh_enabled):
        return _build_empty_depth_runtime()
    depth_replay, summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
        max_snapshots_per_replay_row=1,
        heartbeat=heartbeat,
    )
    return depth_replay, summary, build_depth_candidate_lookup(depth_replay)


def _build_fill_depth_runtime(
    *,
    accepted: pd.DataFrame,
    data_cfg: DataConfig,
    heartbeat: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, DepthReplaySummary, object]:
    if accepted.empty:
        return _build_empty_depth_runtime()
    depth_replay, summary = build_raw_depth_replay_frame(
        replay=accepted,
        data_cfg=data_cfg,
        heartbeat=heartbeat,
    )
    return depth_replay, summary, build_depth_candidate_lookup(depth_replay)


def _build_orderbook_preflight_summary(
    *,
    data_cfg: DataConfig,
    replay: pd.DataFrame,
    decision_start: str | None,
    decision_end: str | None,
    heartbeat: Callable[[str], None] | None = None,
) -> dict[str, object]:
    date_strings = _resolve_orderbook_preflight_dates(
        replay=replay,
        decision_start=decision_start,
        decision_end=decision_end,
    )
    if heartbeat is not None and date_strings:
        heartbeat(f"Checking orderbook coverage for {len(date_strings)} date(s)")
    return preflight_orderbook_index_dates(
        data_cfg,
        date_strings=date_strings,
        expected_market_ids_by_date=_replay_market_ids_by_date(replay),
    )


def _assert_orderbook_preflight_is_usable(summary: dict[str, object] | None) -> None:
    payload = dict(summary or {})
    empty_dates = [str(value) for value in payload.get("empty_depth_source_dates") or [] if str(value)]
    missing_dates = [str(value) for value in payload.get("missing_depth_dates") or [] if str(value)]
    partial_dates = [str(value) for value in payload.get("partial_market_coverage_dates") or [] if str(value)]
    if not empty_dates and not missing_dates and not partial_dates:
        return
    raise RuntimeError(
        "backtest_orderbook_coverage_incomplete:"
        f"empty_depth_source_dates={empty_dates}:"
        f"missing_depth_dates={missing_dates}:"
        f"partial_market_coverage_dates={partial_dates}"
    )


def _resolve_orderbook_preflight_dates(
    *,
    replay: pd.DataFrame,
    decision_start: str | None,
    decision_end: str | None,
) -> list[str]:
    replay_dates = _replay_decision_dates(replay)
    if decision_start is None and decision_end is None:
        return replay_dates

    start_day = None
    end_day = None
    start_bound = _parse_window_bound(decision_start, is_end=False)
    if start_bound is not None:
        start_day = start_bound.normalize()

    end_bound = _parse_window_bound(decision_end, is_end=True)
    if end_bound is not None:
        end_day = (end_bound - pd.Timedelta(days=1)).normalize() if _looks_like_date_only(decision_end) else end_bound.normalize()

    if start_day is None and replay_dates:
        start_day = _date_start_bound(replay_dates[0]).normalize()
    if end_day is None and replay_dates:
        end_day = _date_start_bound(replay_dates[-1]).normalize()
    if start_day is None and end_day is not None:
        start_day = end_day
    if end_day is None and start_day is not None:
        end_day = start_day
    if start_day is None or end_day is None:
        return replay_dates
    if end_day < start_day:
        return []
    return [ts.strftime("%Y-%m-%d") for ts in pd.date_range(start_day, end_day, freq="D", tz="UTC")]


def _replay_decision_dates(replay: pd.DataFrame) -> list[str]:
    if replay.empty:
        return []
    decision_ts = pd.to_datetime(replay.get("decision_ts"), utc=True, errors="coerce")
    if decision_ts.empty:
        return []
    normalized = decision_ts.dropna().dt.strftime("%Y-%m-%d")
    return sorted({str(value) for value in normalized.tolist() if str(value)})


def _replay_market_ids_by_date(replay: pd.DataFrame) -> dict[str, set[str]]:
    if replay.empty or "market_id" not in replay.columns:
        return {}
    decision_ts = pd.to_datetime(replay.get("decision_ts"), utc=True, errors="coerce")
    market_ids = replay.get("market_id", pd.Series(index=replay.index, dtype="string")).astype("string").fillna("")
    frame = pd.DataFrame({"decision_ts": decision_ts, "market_id": market_ids})
    frame = frame.loc[frame["decision_ts"].notna() & frame["market_id"].ne("")].copy()
    if frame.empty:
        return {}
    dates = frame["decision_ts"].dt.strftime("%Y-%m-%d")
    grouped = frame.groupby(dates, dropna=False, sort=False)["market_id"]
    return {
        str(date_str): {str(value) for value in series.astype("string").tolist() if str(value)}
        for date_str, series in grouped
    }


def _date_start_bound(date_text: str) -> pd.Timestamp:
    return pd.Timestamp(str(date_text)).tz_localize("UTC")


def _filter_replay_window(
    replay: pd.DataFrame,
    *,
    decision_start: str | None,
    decision_end: str | None,
) -> pd.DataFrame:
    if replay.empty or (decision_start is None and decision_end is None):
        return replay

    decision_ts = pd.to_datetime(replay.get("decision_ts"), utc=True, errors="coerce")
    mask = decision_ts.notna()

    start_bound = _parse_window_bound(decision_start, is_end=False)
    if start_bound is not None:
        mask &= decision_ts.ge(start_bound)

    end_bound = _parse_window_bound(decision_end, is_end=True)
    if end_bound is not None:
        if _looks_like_date_only(decision_end):
            mask &= decision_ts.lt(end_bound)
        else:
            mask &= decision_ts.le(end_bound)

    return replay.loc[mask].reset_index(drop=True)


def _parse_window_bound(value: str | None, *, is_end: bool) -> pd.Timestamp | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = pd.Timestamp(text)
    parsed = parsed.tz_localize("UTC") if parsed.tzinfo is None else parsed.tz_convert("UTC")
    if is_end and _looks_like_date_only(text):
        return parsed + pd.Timedelta(days=1)
    return parsed


def _looks_like_date_only(value: str | None) -> bool:
    text = str(value or "").strip()
    return len(text) == 10 and text[4:5] == "-" and text[7:8] == "-"


def _stage_progress_pct(*, stage_index: int, total_stages: int) -> int:
    if total_stages <= 0:
        return 0
    completed_stages = min(max(int(stage_index) - 1, 0), total_stages)
    return int(round((completed_stages / total_stages) * 100))


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _bundle_offsets(bundle_dir: Path) -> list[int]:
    values: list[int] = []
    for path in (bundle_dir / "offsets").glob("offset=*"):
        try:
            values.append(int(path.name.split("=", 1)[1]))
        except Exception:
            continue
    return sorted(values)


def _bundle_feature_columns(bundle_dir: Path) -> tuple[str, ...]:
    columns: list[str] = []
    seen: set[str] = set()
    for feature_path in sorted((bundle_dir / "offsets").glob("offset=*/feature_cols.joblib")):
        try:
            loaded = joblib.load(feature_path)
        except Exception:
            continue
        for value in loaded if isinstance(loaded, (list, tuple)) else ():
            column = str(value)
            if not column or column in seen:
                continue
            seen.add(column)
            columns.append(column)
    return tuple(columns)


def _resolve_secondary_bundle_context(
    *,
    cfg: ResearchConfig,
    spec: BacktestRunSpec,
) -> tuple[Path | None, str]:
    secondary_target = str(spec.secondary_target or spec.target)
    if not spec.secondary_bundle_label:
        return None, secondary_target
    secondary_cfg = cfg if secondary_target == cfg.target else ResearchConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        profile=cfg.profile,
        source_surface=cfg.source_surface,
        feature_set=cfg.feature_set,
        label_set=cfg.label_set,
        target=secondary_target,
        model_family=cfg.model_family,
        root=cfg.layout.storage.rewrite_root,
    )
    bundle_dir = resolve_model_bundle_dir(
        secondary_cfg,
        profile=spec.profile,
        target=secondary_target,
        bundle_label=spec.secondary_bundle_label,
    )
    return bundle_dir, secondary_target


def _required_backtest_feature_columns(
    *,
    bundle_dirs: tuple[Path, ...],
    targets: tuple[str, ...],
) -> list[str]:
    columns: list[str] = list(_BACKTEST_FEATURE_KEY_COLUMNS)
    seen = set(columns)
    for bundle_dir in bundle_dirs:
        for column in _bundle_feature_columns(bundle_dir):
            if not column or column in seen:
                continue
            seen.add(column)
            columns.append(column)
    for target in targets:
        if str(target).strip().lower() != "reversal":
            continue
        for column in _REVERSAL_ANCHOR_COLUMNS:
            if column in seen:
                continue
            seen.add(column)
            columns.append(column)
    return columns


def _load_scoped_backtest_feature_frame(
    *,
    cfg: ResearchConfig,
    feature_set: str,
    bundle_dirs: tuple[Path, ...],
    targets: tuple[str, ...],
    available_offsets: list[int],
    decision_start: str | None,
    decision_end: str | None,
) -> pd.DataFrame:
    features = load_feature_frame(
        cfg,
        feature_set=feature_set,
        columns=_required_backtest_feature_columns(bundle_dirs=bundle_dirs, targets=targets),
    )
    return _scope_backtest_feature_frame(
        features,
        available_offsets=available_offsets,
        decision_start=decision_start,
        decision_end=decision_end,
    )


def _scope_backtest_feature_frame(
    features: pd.DataFrame,
    *,
    available_offsets: list[int],
    decision_start: str | None,
    decision_end: str | None,
) -> pd.DataFrame:
    if features.empty:
        return features
    decision_ts = pd.to_datetime(features.get("decision_ts"), utc=True, errors="coerce")
    offset_values = pd.to_numeric(features.get("offset"), errors="coerce")
    mask = decision_ts.notna()
    if available_offsets:
        mask &= offset_values.isin([int(offset) for offset in available_offsets])

    start_bound = _parse_window_bound(decision_start, is_end=False)
    if start_bound is not None:
        mask &= decision_ts.ge(start_bound)

    end_bound = _parse_window_bound(decision_end, is_end=True)
    if end_bound is not None:
        if _looks_like_date_only(decision_end):
            mask &= decision_ts.lt(end_bound)
        else:
            mask &= decision_ts.le(end_bound)

    return features.loc[mask].reset_index(drop=True)


def _load_scoped_backtest_label_frame(
    *,
    cfg: ResearchConfig,
    label_set: str,
    scoped_features: pd.DataFrame,
) -> pd.DataFrame:
    labels = load_label_frame(
        cfg,
        label_set=label_set,
        columns=_BACKTEST_LABEL_REQUIRED_COLUMNS,
    )
    return _scope_backtest_label_frame(labels, scoped_features=scoped_features)


def _scope_backtest_label_frame(
    labels: pd.DataFrame,
    *,
    scoped_features: pd.DataFrame,
) -> pd.DataFrame:
    if labels.empty or scoped_features.empty:
        return labels.iloc[0:0].copy()

    feature_pairs = scoped_features.loc[:, ["cycle_start_ts", "cycle_end_ts"]].copy()
    feature_pairs["cycle_start_ts"] = pd.to_datetime(feature_pairs["cycle_start_ts"], utc=True, errors="coerce")
    feature_pairs["cycle_end_ts"] = pd.to_datetime(feature_pairs["cycle_end_ts"], utc=True, errors="coerce")
    feature_pairs = feature_pairs.dropna().drop_duplicates().reset_index(drop=True)
    if feature_pairs.empty:
        return labels.iloc[0:0].copy()

    label_start = pd.to_numeric(labels.get("cycle_start_ts"), errors="coerce")
    label_end = pd.to_numeric(labels.get("cycle_end_ts"), errors="coerce")
    valid = label_start.notna() & label_end.notna()
    if not bool(valid.any()):
        return labels.iloc[0:0].copy()

    label_pairs = pd.DataFrame(
        {
            "cycle_start_ts": pd.to_datetime(label_start.loc[valid].astype("int64"), unit="s", utc=True),
            "cycle_end_ts": pd.to_datetime(label_end.loc[valid].astype("int64"), unit="s", utc=True),
        }
    )
    allowed_pairs = pd.MultiIndex.from_frame(feature_pairs)
    keep_mask = pd.Series(False, index=labels.index, dtype=bool)
    keep_mask.loc[label_pairs.index] = pd.MultiIndex.from_frame(label_pairs).isin(allowed_pairs)
    return labels.loc[keep_mask].reset_index(drop=True)


def _required_backtest_klines_lookback_minutes(profile_spec) -> int:
    lookback = int(getattr(profile_spec, "liquidity_guard_lookback_minutes", 0) or 0)
    baseline = int(getattr(profile_spec, "liquidity_guard_baseline_minutes", 0) or 0)
    return max(30, lookback + baseline)


def _scope_backtest_klines(
    raw_klines: pd.DataFrame,
    *,
    decision_start: str | None,
    decision_end: str | None,
    required_lookback_minutes: int,
) -> pd.DataFrame:
    if raw_klines is None or raw_klines.empty or "open_time" not in raw_klines.columns:
        return raw_klines
    start_bound = _parse_window_bound(decision_start, is_end=False)
    end_bound = _parse_window_bound(decision_end, is_end=True)
    if start_bound is None and end_bound is None:
        return raw_klines

    open_time = pd.to_datetime(raw_klines.get("open_time"), utc=True, errors="coerce")
    mask = open_time.notna()

    if start_bound is not None:
        lower_bound = start_bound - pd.Timedelta(minutes=max(0, int(required_lookback_minutes)))
        mask &= open_time.ge(lower_bound)

    if end_bound is not None:
        if _looks_like_date_only(decision_end):
            mask &= open_time.lt(end_bound)
        else:
            mask &= open_time.le(end_bound)

    return raw_klines.loc[mask].reset_index(drop=True)


def _load_cached_primary_runtime(
    *,
    cfg: ResearchConfig,
    spec: BacktestRunSpec,
    bundle_dir: Path,
    feature_set: str,
    label_set: str,
    profile_spec,
    liquidity_proxy_mode: str,
) -> BacktestPreparedRuntime | None:
    cache_key = _backtest_runtime_cache_key(
        cfg=cfg,
        spec=spec,
        bundle_dir=bundle_dir,
        feature_set=feature_set,
        label_set=label_set,
        profile_spec=profile_spec,
        liquidity_proxy_mode=liquidity_proxy_mode,
    )
    return process_backtest_runtime_cache().get(cache_key)


def _store_primary_runtime(
    *,
    cfg: ResearchConfig,
    spec: BacktestRunSpec,
    bundle_dir: Path,
    feature_set: str,
    label_set: str,
    profile_spec,
    liquidity_proxy_mode: str,
    prepared: BacktestPreparedRuntime,
) -> BacktestPreparedRuntime:
    cache_key = _backtest_runtime_cache_key(
        cfg=cfg,
        spec=spec,
        bundle_dir=bundle_dir,
        feature_set=feature_set,
        label_set=label_set,
        profile_spec=profile_spec,
        liquidity_proxy_mode=liquidity_proxy_mode,
    )
    return process_backtest_runtime_cache().put(cache_key, prepared)


def _backtest_runtime_cache_key(
    *,
    cfg: ResearchConfig,
    spec: BacktestRunSpec,
    bundle_dir: Path,
    feature_set: str,
    label_set: str,
    profile_spec,
    liquidity_proxy_mode: str,
) -> BacktestSharedRuntimeKey:
    return BacktestSharedRuntimeKey(
        rewrite_root=str(cfg.layout.storage.rewrite_root),
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        source_surface=cfg.source_surface,
        bundle_dir=str(bundle_dir),
        feature_set=feature_set,
        label_set=label_set,
        profile_spec_key=_json_text(profile_spec.to_dict() if hasattr(profile_spec, "to_dict") else profile_spec),
        liquidity_proxy_mode=str(liquidity_proxy_mode or ""),
        decision_start=str(spec.decision_start or ""),
        decision_end=str(spec.decision_end or ""),
    )


def _prepared_runtime_source_paths(
    *,
    cfg: ResearchConfig,
    data_cfg: DataConfig,
    bundle_dir: Path,
    feature_set: str,
    label_set: str,
    depth_replay: pd.DataFrame,
    runtime_replay: pd.DataFrame,
) -> list[Path]:
    paths = [
        bundle_dir / "manifest.json",
        cfg.layout.feature_frame_path(feature_set, source_surface=cfg.source_surface),
        cfg.layout.label_frame_path(label_set),
        data_cfg.layout.binance_klines_path(),
    ]
    for column in ("depth_source_path", "quote_source_path"):
        if column not in depth_replay.columns and column not in runtime_replay.columns:
            continue
        frame = depth_replay if column in depth_replay.columns else runtime_replay
        values = frame[column].dropna().astype(str).tolist()
        paths.extend(Path(value) for value in values if value)
    return paths


def _build_bundle_replay(
    *,
    bundle_dir: Path,
    features: pd.DataFrame,
    labels: pd.DataFrame,
) -> tuple[pd.DataFrame, object, list[int]]:
    available_offsets = _bundle_offsets(bundle_dir)
    aligned_features, _alignment_metadata = merge_feature_and_label_frames(features, labels)
    score_frames = [score_bundle_offset(bundle_dir, aligned_features, offset=offset) for offset in available_offsets]
    scores = (
        pd.concat(score_frames, ignore_index=True, sort=False)
        if score_frames
        else pd.DataFrame(
            columns=["decision_ts", "offset", "p_lgb", "p_lr", "p_signal", "p_up", "p_down", "score_valid", "score_reason"]
        )
    )
    replay, replay_summary = build_replay_frame(
        features=features,
        labels=labels,
        score_frames=[scores] if not scores.empty else [],
        available_offsets=available_offsets,
        scoped_offsets=available_offsets,
    )
    return replay, replay_summary, available_offsets


def _attach_replay_runtime_surface(
    *,
    replay: pd.DataFrame,
    data_cfg: DataConfig,
    market: str,
    raw_klines: pd.DataFrame,
    profile_spec,
    liquidity_proxy_mode: str,
    quote_heartbeat: Callable[[str], None] | None = None,
    live_state_heartbeat: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, object, object]:
    replay, quote_summary = attach_canonical_quote_surface(
        replay=replay,
        data_cfg=data_cfg,
        heartbeat=quote_heartbeat,
    )
    replay, state_summary = attach_live_state_parity(
        market=market,
        profile=profile_spec,
        replay=replay,
        raw_klines=raw_klines,
        liquidity_proxy_mode=liquidity_proxy_mode,
        heartbeat=live_state_heartbeat,
    )
    return replay, quote_summary, state_summary


def _build_guarded_policy_decisions(
    *,
    replay: pd.DataFrame,
    market: str,
    profile: str,
    profile_spec,
    model_source: str,
    depth_replay: pd.DataFrame | None,
    fill_config: BacktestFillConfig,
    heartbeat: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, object, InitialSnapshotDecisionSummary]:
    if heartbeat is not None:
        heartbeat("Attaching decision engine surface")
    replay, decision_quote_summary = _attach_decision_engine_surface(
        replay,
        market=market,
        profile_spec=profile_spec,
        depth_replay=depth_replay,
        fill_config=fill_config,
    )
    if heartbeat is not None:
        heartbeat("Applying policy thresholds")
    decisions = build_policy_decisions(
        replay,
        config=BacktestPolicyConfig(prob_floor=0.55, prob_gap_floor=0.0),
        model_source=model_source,
    )
    decisions["guard_reasons"] = [[] for _ in range(len(decisions))]
    decisions["guard_primary_reason"] = ""
    decisions["guard_blocked"] = False
    decisions["quote_metrics"] = [{} for _ in range(len(decisions))]
    decisions["account_context"] = [{} for _ in range(len(decisions))]
    guard_summary = GuardParitySummary(evaluated_rows=0, blocked_rows=0)
    if heartbeat is not None:
        heartbeat(f"Built guarded decisions: {len(decisions):,} rows")
    return decisions, guard_summary, decision_quote_summary


def _attach_decision_engine_surface(
    replay: pd.DataFrame,
    *,
    market: str,
    profile_spec,
    depth_replay: pd.DataFrame | None,
    fill_config: BacktestFillConfig,
) -> tuple[pd.DataFrame, InitialSnapshotDecisionSummary]:
    frame = replay.copy()
    frame["decision_engine_min_dir_prob_boost"] = frame.get(
        "regime_guard_hints",
        pd.Series(index=frame.index, dtype="object"),
    ).apply(_decision_engine_min_dir_prob_boost)
    if bool(fill_config.raw_depth_fak_refresh_enabled):
        decision_config = build_profile_decision_engine_parity_config(
            market=market,
            profile_spec=profile_spec,
        )
        if depth_replay is None or depth_replay.empty:
            if _frame_has_decision_quote_fallback_prices(frame):
                frame = attach_initial_snapshot_decision_surface(
                    frame,
                    depth_replay=None,
                    profile_spec=profile_spec,
                    fill_config=fill_config,
                )
                out = apply_decision_engine_parity(
                    frame,
                    config=decision_config,
                    up_price_columns=("quote_up_ask", "quote_prob_up", "p_up"),
                    down_price_columns=("quote_down_ask", "quote_prob_down", "p_down"),
                    min_dir_prob_boost_column="decision_engine_min_dir_prob_boost",
                )
                out = attach_pre_submit_orderbook_retry_contract(
                    out,
                    spec=profile_spec,
                )
                return out, build_empty_initial_snapshot_decision_summary()
            out = attach_pre_submit_orderbook_retry_contract(
                frame,
                spec=profile_spec,
            )
            return out, build_empty_initial_snapshot_decision_summary()
        out, summary = apply_initial_snapshot_decision_parity(
            frame,
            depth_replay=depth_replay,
            profile_spec=profile_spec,
            fill_config=fill_config,
            decision_config=decision_config,
            min_dir_prob_boost_column="decision_engine_min_dir_prob_boost",
        )
        out = attach_pre_submit_orderbook_retry_contract(
            out,
            spec=profile_spec,
        )
        missing_depth_mask = _decision_quote_missing_depth_mask(out)
        if missing_depth_mask.any():
            fallback = apply_decision_engine_parity(
                frame.loc[missing_depth_mask].copy(),
                config=decision_config,
                up_price_columns=("quote_up_ask", "quote_prob_up", "p_up"),
                down_price_columns=("quote_down_ask", "quote_prob_down", "p_down"),
                min_dir_prob_boost_column="decision_engine_min_dir_prob_boost",
            )
            for column in DECISION_ENGINE_PARITY_COLUMNS:
                out.loc[missing_depth_mask, column] = fallback[column].to_numpy()
            fallback_retry = attach_pre_submit_orderbook_retry_contract(
                out.loc[missing_depth_mask].copy(),
                spec=profile_spec,
            )
            for column in (
                "pre_submit_orderbook_retry_armed",
                "pre_submit_orderbook_retry_reason",
                "pre_submit_orderbook_retry_interval_sec",
                "pre_submit_orderbook_retry_max",
                "pre_submit_orderbook_retry_state_key",
            ):
                out.loc[missing_depth_mask, column] = fallback_retry[column].to_numpy()
        return out, summary
    out = apply_decision_engine_parity(
        frame,
        config=DecisionEngineParityConfig(min_dir_prob_default=0.55),
        min_dir_prob_boost_column="decision_engine_min_dir_prob_boost",
    )
    out = attach_pre_submit_orderbook_retry_contract(
        out,
        spec=profile_spec,
    )
    return out, build_empty_initial_snapshot_decision_summary()


def _build_secondary_replay(
    *,
    cfg,
    spec: BacktestRunSpec,
    features: pd.DataFrame,
    labels: pd.DataFrame,
) -> pd.DataFrame:
    secondary_target = str(spec.secondary_target or spec.target)
    secondary_cfg = cfg if secondary_target == cfg.target else ResearchConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        profile=cfg.profile,
        source_surface=cfg.source_surface,
        feature_set=cfg.feature_set,
        label_set=cfg.label_set,
        target=secondary_target,
        model_family=cfg.model_family,
        root=cfg.layout.storage.rewrite_root,
    )
    bundle_dir = resolve_model_bundle_dir(
        secondary_cfg,
        profile=spec.profile,
        target=secondary_target,
        bundle_label=spec.secondary_bundle_label,
    )
    secondary_replay, _, _available_offsets = _build_bundle_replay(
        bundle_dir=bundle_dir,
        features=features,
        labels=labels,
    )
    return secondary_replay


def _append_backtest_log(path: Path, event: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False, sort_keys=True))
        fh.write("\n")


def _serialize_decision_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in (
        "guard_reasons",
        "quote_metrics",
        "account_context",
        "liquidity_reason_codes",
        "liquidity_metrics",
        "regime_reason_codes",
        "regime_guard_hints",
        "regime_source_of_truth",
    ):
        if column not in out.columns:
            continue
        out[column] = out[column].apply(_json_text)
    return out


def _json_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _decision_engine_min_dir_prob_boost(value: object) -> float:
    if not isinstance(value, dict):
        return 0.0
    try:
        return float(value.get("min_dir_prob_boost") or 0.0)
    except Exception:
        return 0.0


def _decision_quote_missing_depth_mask(frame: pd.DataFrame) -> pd.Series:
    values = frame.get("decision_quote_has_raw_depth", pd.Series(False, index=frame.index, dtype="boolean"))
    return ~values.astype("boolean").fillna(False).astype(bool)


def _frame_has_decision_quote_fallback_prices(frame: pd.DataFrame) -> bool:
    for column in ("quote_up_ask", "quote_down_ask", "quote_prob_up", "quote_prob_down"):
        if column not in frame.columns:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.notna().any():
            return True
    return False


def _ensure_replay_labels_resolved(replay: pd.DataFrame) -> None:
    if replay.empty:
        return
    resolved = pd.to_numeric(replay.get("resolved"), errors="coerce").fillna(0).astype(int).astype(bool)
    winner_side = replay.get("winner_side", pd.Series("", index=replay.index, dtype="string")).astype("string").fillna("").astype(str).str.upper()
    unresolved_mask = ~resolved | ~winner_side.isin(["UP", "DOWN"])
    if not unresolved_mask.any():
        return
    sample = replay.loc[unresolved_mask, ["cycle_start_ts", "cycle_end_ts", "offset"]].head(5).copy()
    raise RuntimeError(
        "backtest_label_coverage_incomplete:"
        f"rows={int(unresolved_mask.sum())}:"
        f"samples={sample.to_dict(orient='records')}"
    )


def _build_backtest_fill_config(*, spec: BacktestRunSpec, profile_spec) -> BacktestFillConfig:
    fill_base_stake = 1.0 if spec.stake_usd is None else float(spec.stake_usd)
    if spec.max_notional_usd is not None:
        fill_max_stake = float(spec.max_notional_usd)
    elif spec.stake_usd is not None:
        fill_max_stake = float(spec.stake_usd)
    else:
        fill_max_stake = 3.0
    return BacktestFillConfig(
        base_stake=fill_base_stake,
        max_stake=fill_max_stake,
        fee_bps=50.0,
        raw_depth_fak_refresh_enabled=(
            True if spec.parity.raw_depth_fak_refresh_enabled is None else bool(spec.parity.raw_depth_fak_refresh_enabled)
        ),
        fill_model="canonical_quote_depth",
        profile_spec=profile_spec,
    )


def _load_label_runtime_summary(*, cfg: ResearchConfig, label_set: str) -> dict[str, object]:
    manifest_path = cfg.layout.label_frame_manifest_path(label_set)
    if not manifest_path.exists():
        return {}
    metadata = read_manifest(manifest_path).metadata
    keys = (
        "status",
        "truth_table_rows",
        "truth_source_counts",
        "truth_table_updated_at",
        "oracle_table_rows",
        "oracle_has_both_rows",
        "oracle_source_counts",
        "oracle_table_updated_at",
        "truth_runtime_foundation_status",
        "truth_runtime_foundation_reason",
        "truth_runtime_foundation_issue_codes",
        "truth_runtime_direct_oracle_fail_open",
        "truth_runtime_truth_table_status",
        "truth_runtime_truth_table_rows",
        "truth_runtime_oracle_prices_table_status",
        "truth_runtime_oracle_prices_table_rows",
        "truth_runtime_direct_oracle_source_status",
        "truth_runtime_direct_oracle_source_rows",
        "truth_runtime_settlement_truth_source_status",
        "truth_runtime_settlement_truth_source_rows",
        "truth_runtime_streams_source_status",
        "truth_runtime_streams_source_rows",
        "truth_runtime_datafeeds_source_status",
        "truth_runtime_datafeeds_source_rows",
    )
    return {
        str(key): metadata.get(key)
        for key in keys
        if key in metadata
    }
