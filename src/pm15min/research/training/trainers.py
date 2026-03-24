from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Protocol

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from pm15min.research.features.pruning import FeaturePruningPlan, build_feature_pruning_plan
from pm15min.research.training.splits import build_purged_time_series_splits
from pm15min.research.training.weights import compute_sample_weights


class TrainingProgressReporter(Protocol):
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


def report_training_progress(
    reporter: TrainingProgressReporter | None,
    *,
    summary: str,
    current: int | None = None,
    total: int | None = None,
    current_stage: str | None = None,
    progress_pct: int | None = None,
    heartbeat: str | None = None,
) -> None:
    if reporter is None:
        return
    reporter(
        summary=summary,
        current=current,
        total=total,
        current_stage=current_stage,
        progress_pct=progress_pct,
        heartbeat=heartbeat,
    )


@dataclass(frozen=True)
class TrainerConfig:
    n_splits: int = 3
    lgb_num_leaves: int = 15
    lgb_learning_rate: float = 0.05
    lgb_n_estimators: int = 200
    random_seed: int = 42
    purge_minutes: int = 15
    embargo_minutes: int = 0
    balance_classes: bool = True
    weight_by_vol: bool = True
    inverse_vol: bool = False
    contrarian_weight: float = 1.0
    contrarian_quantile: float = 0.8
    contrarian_return_col: str = "ret_from_strike"
    apply_shared_blacklist: bool = False
    extra_drop_columns: tuple[str, ...] = ()
    parallel_workers: int = 1


def training_features(df: pd.DataFrame) -> list[str]:
    exclude = {
        "decision_ts",
        "cycle_start_ts",
        "cycle_end_ts",
        "offset",
        "target",
        "y",
        "y_direction",
        "current_ret",
        "current_ret_col",
        "current_up",
        "winner_side",
        "resolved",
        "settlement_source",
        "label_source",
        "label_alignment_mode",
        "label_alignment_status",
        "label_alignment_gap_seconds",
        "price_to_beat",
        "final_price",
        "full_truth",
    }
    return [column for column in df.columns if column not in exclude]


def prepare_training_matrix(
    df: pd.DataFrame,
    *,
    market: str = "",
    feature_set: str = "",
    cfg: TrainerConfig | None = None,
) -> tuple[pd.DataFrame, pd.Series, FeaturePruningPlan]:
    feature_cols = training_features(df)
    pruning_plan = build_feature_pruning_plan(
        feature_cols,
        feature_set=feature_set,
        market=market,
        extra_drop_columns=tuple(cfg.extra_drop_columns) if cfg is not None else (),
        apply_shared_blacklist=bool(cfg.apply_shared_blacklist) if cfg is not None else False,
    )
    X = df[list(pruning_plan.kept_columns)].copy()
    for column in X.columns:
        X[column] = pd.to_numeric(X[column], errors="coerce")
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    y = pd.to_numeric(df["y"], errors="coerce").astype(int)
    return X, y, pruning_plan


def fit_logreg(X: pd.DataFrame, y: pd.Series, *, cfg: TrainerConfig, sample_weight: pd.Series | None = None) -> Pipeline:
    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    penalty="l2",
                    C=1.0,
                    solver="lbfgs",
                    max_iter=1000,
                    random_state=int(cfg.random_seed),
                ),
            ),
        ]
    )
    if sample_weight is None:
        model.fit(X, y)
    else:
        model.fit(X, y, clf__sample_weight=np.asarray(sample_weight, dtype=float))
    return model


def fit_lgbm(
    X: pd.DataFrame,
    y: pd.Series,
    *,
    cfg: TrainerConfig,
    sample_weight: pd.Series | None = None,
) -> lgb.LGBMClassifier:
    model = lgb.LGBMClassifier(
        n_estimators=int(cfg.lgb_n_estimators),
        learning_rate=float(cfg.lgb_learning_rate),
        num_leaves=int(cfg.lgb_num_leaves),
        min_data_in_leaf=max(2, int(len(X) * 0.05)),
        objective="binary",
        n_jobs=_resolve_lgb_n_jobs(cfg),
        random_state=int(cfg.random_seed),
        verbosity=-1,
    )
    if sample_weight is None:
        model.fit(X, y)
    else:
        model.fit(X, y, sample_weight=np.asarray(sample_weight, dtype=float))
    return model


def generate_oof_predictions(
    X: pd.DataFrame,
    y: pd.Series,
    *,
    decision_ts: pd.Series,
    raw_frame: pd.DataFrame,
    cfg: TrainerConfig,
    reporter: TrainingProgressReporter | None = None,
) -> pd.DataFrame:
    n_rows = len(X)
    if n_rows < 8 or len(np.unique(y)) < 2:
        return pd.DataFrame(columns=["row_number", "fold", "y", "p_lgb", "p_lr"])

    splits = build_purged_time_series_splits(
        decision_ts,
        n_splits=min(int(cfg.n_splits), max(2, n_rows // 4)),
        purge_minutes=cfg.purge_minutes,
        embargo_minutes=cfg.embargo_minutes,
    )
    rows: list[pd.DataFrame] = []
    for fold, (train_idx, test_idx) in enumerate(splits):
        report_training_progress(
            reporter,
            summary=f"Generating OOF predictions (fold {fold + 1}/{len(splits)})",
            current_stage="training_oof",
        )
        X_train = X.iloc[train_idx]
        y_train = y.iloc[train_idx]
        X_test = X.iloc[test_idx]
        y_test = y.iloc[test_idx]
        if len(np.unique(y_train)) < 2 or len(X_test) == 0:
            continue
        sample_weight = compute_sample_weights(
            raw_frame.iloc[train_idx],
            y_train,
            balance_classes=cfg.balance_classes,
            weight_by_vol=cfg.weight_by_vol,
            inverse_vol=cfg.inverse_vol,
            contrarian_weight=cfg.contrarian_weight,
            contrarian_quantile=cfg.contrarian_quantile,
            contrarian_return_col=cfg.contrarian_return_col,
        )
        logreg = fit_logreg(X_train, y_train, cfg=cfg, sample_weight=sample_weight)
        lgbm = fit_lgbm(X_train, y_train, cfg=cfg, sample_weight=sample_weight)
        rows.append(
            pd.DataFrame(
                {
                    "row_number": X_test.index.astype(int),
                    "fold": fold,
                    "y": y_test.astype(int).values,
                    "p_lgb": lgbm.predict_proba(X_test)[:, 1].astype(float),
                    "p_lr": logreg.predict_proba(X_test)[:, 1].astype(float),
                }
            )
        )
    if not rows:
        return pd.DataFrame(columns=["row_number", "fold", "y", "p_lgb", "p_lr"])
    return pd.concat(rows, ignore_index=True).sort_values(["row_number", "fold"]).reset_index(drop=True)


def _resolve_lgb_n_jobs(cfg: TrainerConfig) -> int:
    cpu_count = max(1, int(os.cpu_count() or 1))
    parallel_workers = max(1, int(cfg.parallel_workers))
    return max(1, cpu_count // parallel_workers)
