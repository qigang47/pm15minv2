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
from pm15min.research.backtests.decision_quote_surface import (
    InitialSnapshotDecisionSummary,
    apply_initial_snapshot_decision_parity,
    build_empty_initial_snapshot_decision_summary,
)
from pm15min.research.backtests.decision_engine_parity import (
    DECISION_ENGINE_PARITY_COLUMNS,
    DecisionEngineParityConfig,
    apply_decision_engine_parity,
    build_profile_decision_engine_parity_config,
)
from pm15min.research.backtests.depth_replay import build_raw_depth_replay_frame
from pm15min.research.backtests.fills import BacktestFillConfig, build_canonical_fills
from pm15min.research.backtests.guard_parity import apply_live_guard_parity
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
from pm15min.research.inference.scorer import score_bundle_offset
from pm15min.research.labels.runtime import build_truth_runtime_summary
from pm15min.research.manifests import build_manifest, read_manifest, write_manifest


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
    label_set = str(bundle_manifest.spec.get("label_set") or cfg.label_set)

    features = load_feature_frame(cfg, feature_set=feature_set)
    labels = load_label_frame(cfg, label_set=label_set)
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        surface=cfg.source_surface,
        root=cfg.layout.storage.rewrite_root,
    )
    raw_klines = load_binance_klines_1m(data_cfg)
    profile_spec = resolve_backtest_profile_spec(
        market=cfg.asset.slug,
        profile=spec.profile,
        parity=spec.parity,
    )
    liquidity_proxy_mode = str(spec.parity.liquidity_proxy_mode or "spot_kline_mirror")

    _start_stage("bundle_replay", "Scoring bundle replay")
    replay, replay_summary, available_offsets = _build_bundle_replay(
        bundle_dir=bundle_dir,
        features=features,
        labels=labels,
    )
    depth_stage_index, depth_stage_name = _start_stage("depth_replay", "Replaying raw depth snapshots")
    depth_replay, depth_replay_summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
        heartbeat=progress.heartbeat(stage_index=depth_stage_index, stage_name=depth_stage_name),
    )
    _start_stage("runtime_surface", "Attaching runtime surfaces")
    replay, quote_summary, state_summary = _attach_replay_runtime_surface(
        replay=replay,
        data_cfg=data_cfg,
        market=cfg.asset.slug,
        raw_klines=raw_klines,
        profile_spec=profile_spec,
        liquidity_proxy_mode=liquidity_proxy_mode,
    )
    fill_config = _build_backtest_fill_config(spec=spec, profile_spec=profile_spec)
    retry_contract_summary = build_backtest_retry_contract(profile_spec)
    label_runtime_summary = _load_label_runtime_summary(cfg=cfg, label_set=label_set)
    truth_runtime_summary = build_truth_runtime_summary(data_cfg)
    _start_stage("policy_decisions", "Building policy decisions")
    decisions, guard_summary, decision_quote_summary = _build_guarded_policy_decisions(
        replay=replay,
        market=cfg.asset.slug,
        profile=spec.profile,
        profile_spec=profile_spec,
        model_source="primary",
        depth_replay=depth_replay,
        fill_config=fill_config,
    )
    if spec.secondary_bundle_label:
        _start_stage("secondary_decisions", "Building hybrid fallback decisions")
        secondary_replay = _build_secondary_replay(
            cfg=cfg,
            spec=spec,
            features=features,
            labels=labels,
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
    _start_stage("fills_materialization", "Materializing canonical fills")
    policy_rejects = build_policy_reject_frame(decisions)
    accepted = decisions.loc[decisions["policy_action"].eq("trade")].copy()
    fills, fill_rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=fill_config,
        profile_spec=profile_spec,
        depth_replay=depth_replay,
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
    )
    summary["run_label"] = spec.run_label
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
        "trades": int(len(trades)),
        "rejects": int(len(rejects)),
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(run_dir / "manifest.json"),
    }


def _backtest_stage_total(*, spec: BacktestRunSpec) -> int:
    return 9 if spec.secondary_bundle_label else 8


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


def _build_bundle_replay(
    *,
    bundle_dir: Path,
    features: pd.DataFrame,
    labels: pd.DataFrame,
) -> tuple[pd.DataFrame, object, list[int]]:
    available_offsets = _bundle_offsets(bundle_dir)
    score_frames = [score_bundle_offset(bundle_dir, features, offset=offset) for offset in available_offsets]
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
) -> tuple[pd.DataFrame, object, object]:
    replay, quote_summary = attach_canonical_quote_surface(replay=replay, data_cfg=data_cfg)
    replay, state_summary = attach_live_state_parity(
        market=market,
        profile=profile_spec,
        replay=replay,
        raw_klines=raw_klines,
        liquidity_proxy_mode=liquidity_proxy_mode,
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
) -> tuple[pd.DataFrame, object, InitialSnapshotDecisionSummary]:
    replay, decision_quote_summary = _attach_decision_engine_surface(
        replay,
        market=market,
        profile_spec=profile_spec,
        depth_replay=depth_replay,
        fill_config=fill_config,
    )
    decisions = build_policy_decisions(
        replay,
        config=BacktestPolicyConfig(prob_floor=0.55, prob_gap_floor=0.0),
        model_source=model_source,
    )
    decisions, guard_summary = apply_live_guard_parity(
        market=market,
        profile=profile,
        decisions=decisions,
        profile_spec=profile_spec,
    )
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
                config=DecisionEngineParityConfig(min_dir_prob_default=0.55),
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
