from __future__ import annotations

from contextlib import contextmanager
import fcntl
from pathlib import Path
import sys

from pm15min.data.config import DataConfig
from pm15min.data.pipelines.oracle_prices import build_oracle_prices_15m
from pm15min.data.pipelines.truth import build_truth_15m
from pm15min.research.config import ResearchConfig
from pm15min.research.features.builders import build_feature_frame as build_feature_frame_df
from pm15min.research.labels.frames import build_label_frame as build_label_frame_df
from pm15min.research.labels.sources import normalize_label_set


def ensure_research_feature_dependencies_aligned(cfg: ResearchConfig) -> dict[str, object]:
    data_cfg = _data_cfg(cfg)
    return {
        "oracle_prices_table": _ensure_oracle_prices_table_aligned(
            data_cfg,
            lock_root=_lock_root(cfg),
        )
    }


def ensure_research_label_dependencies_aligned(cfg: ResearchConfig) -> dict[str, object]:
    data_cfg = _data_cfg(cfg)
    summaries = ensure_research_feature_dependencies_aligned(cfg)
    summaries["truth_table"] = _ensure_truth_table_aligned(
        data_cfg,
        lock_root=_lock_root(cfg),
    )
    return summaries


def ensure_research_artifacts_aligned(
    cfg: ResearchConfig,
    *,
    feature_set: str | None = None,
    label_set: str | None = None,
) -> dict[str, object]:
    resolved_feature_set = str(feature_set or "").strip() or None
    resolved_label_set = normalize_label_set(label_set) if label_set is not None else None
    data_cfg = _data_cfg(cfg)
    lock_root = _lock_root(cfg)
    summaries: dict[str, object] = {}

    if resolved_feature_set is not None or resolved_label_set is not None:
        summaries["oracle_prices_table"] = _ensure_oracle_prices_table_aligned(
            data_cfg,
            lock_root=lock_root,
        )
    if resolved_label_set is not None:
        summaries["truth_table"] = _ensure_truth_table_aligned(
            data_cfg,
            lock_root=lock_root,
        )
    if resolved_feature_set is not None:
        summaries["feature_frame"] = _ensure_feature_frame_aligned(
            _cfg_with_feature_set(cfg, feature_set=resolved_feature_set),
            lock_root=lock_root,
        )
    if resolved_label_set is not None:
        summaries["label_frame"] = _ensure_label_frame_aligned(
            _cfg_with_label_set(cfg, label_set=resolved_label_set),
            lock_root=lock_root,
        )
    return summaries


def _ensure_oracle_prices_table_aligned(
    data_cfg: DataConfig,
    *,
    lock_root: Path,
) -> dict[str, object]:
    dependencies = [
        data_cfg.layout.market_catalog_table_path,
        data_cfg.layout.direct_oracle_source_path,
        *sorted(data_cfg.layout.streams_source_root.glob("year=*/month=*/data.parquet")),
        Path(build_oracle_prices_15m.__code__.co_filename).resolve(),
    ]
    return _ensure_artifact_aligned(
        dataset="oracle_prices_table",
        lock_root=lock_root,
        lock_name=f"oracle_prices_{data_cfg.asset.slug}_{data_cfg.cycle}_{data_cfg.surface}",
        target_path=data_cfg.layout.oracle_prices_table_path,
        dependencies=dependencies,
        build_fn=lambda: build_oracle_prices_15m(data_cfg),
    )


def _ensure_truth_table_aligned(
    data_cfg: DataConfig,
    *,
    lock_root: Path,
) -> dict[str, object]:
    dependencies = [
        data_cfg.layout.market_catalog_table_path,
        data_cfg.layout.oracle_prices_table_path,
        data_cfg.layout.settlement_truth_source_path,
        Path(build_truth_15m.__code__.co_filename).resolve(),
    ]
    return _ensure_artifact_aligned(
        dataset="truth_table",
        lock_root=lock_root,
        lock_name=f"truth_{data_cfg.asset.slug}_{data_cfg.cycle}_{data_cfg.surface}",
        target_path=data_cfg.layout.truth_table_path,
        dependencies=dependencies,
        build_fn=lambda: build_truth_15m(data_cfg),
    )


def _ensure_feature_frame_aligned(
    cfg: ResearchConfig,
    *,
    lock_root: Path,
) -> dict[str, object]:
    data_cfg = _data_cfg(cfg)
    dependencies = [
        data_cfg.layout.binance_klines_path(),
        data_cfg.layout.oracle_prices_table_path,
        *_feature_frame_code_dependencies(),
    ]
    if cfg.asset.slug != "btc":
        btc_cfg = DataConfig.build(
            market="btc",
            cycle=cfg.cycle,
            surface=cfg.source_surface,
            root=cfg.layout.storage.rewrite_root,
        )
        dependencies.append(btc_cfg.layout.binance_klines_path(symbol="BTCUSDT"))
    return _ensure_artifact_aligned(
        dataset="feature_frame",
        lock_root=lock_root,
        lock_name=f"feature_frame_{cfg.asset.slug}_{cfg.cycle}_{cfg.source_surface}_{cfg.feature_set}",
        target_path=cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface),
        dependencies=dependencies,
        build_fn=lambda: _rebuild_feature_frame(cfg),
    )


def _ensure_label_frame_aligned(
    cfg: ResearchConfig,
    *,
    lock_root: Path,
) -> dict[str, object]:
    data_cfg = _data_cfg(cfg)
    dependencies = [
        data_cfg.layout.truth_table_path,
        data_cfg.layout.oracle_prices_table_path,
        Path(build_label_frame_df.__code__.co_filename).resolve(),
    ]
    return _ensure_artifact_aligned(
        dataset="label_frame",
        lock_root=lock_root,
        lock_name=f"label_frame_{cfg.asset.slug}_{cfg.cycle}_{cfg.label_set}",
        target_path=cfg.layout.label_frame_path(cfg.label_set),
        dependencies=dependencies,
        build_fn=lambda: _rebuild_label_frame(cfg),
    )


def _ensure_artifact_aligned(
    *,
    dataset: str,
    lock_root: Path,
    lock_name: str,
    target_path: Path,
    dependencies: list[Path],
    build_fn,
) -> dict[str, object]:
    reasons = _staleness_reasons(target_path=target_path, dependencies=dependencies)
    if not reasons:
        return {
            "dataset": dataset,
            "status": "fresh",
            "path": str(target_path),
            "reasons": [],
        }
    lock_path = lock_root / "artifacts_alignment" / f"{lock_name}.lock"
    with _exclusive_lock(lock_path):
        reasons = _staleness_reasons(target_path=target_path, dependencies=dependencies)
        if not reasons:
            return {
                "dataset": dataset,
                "status": "fresh",
                "path": str(target_path),
                "reasons": [],
            }
        build_fn()
        post_reasons = _staleness_reasons(target_path=target_path, dependencies=dependencies)
        if post_reasons:
            raise RuntimeError(
                f"research_artifact_still_stale:{dataset}:path={target_path}:reasons={','.join(post_reasons)}"
            )
        return {
            "dataset": dataset,
            "status": "rebuilt",
            "path": str(target_path),
            "reasons": reasons,
        }


def _staleness_reasons(*, target_path: Path, dependencies: list[Path]) -> list[str]:
    reasons: list[str] = []
    target_mtime = _latest_path_mtime(target_path)
    if target_mtime is None:
        return ["missing"]
    for dependency in dependencies:
        dep_mtime = _latest_path_mtime(dependency)
        if dep_mtime is None:
            continue
        if dep_mtime > target_mtime:
            reasons.append(f"dependency_newer:{dependency.name}")
    return reasons


def _feature_frame_code_dependencies() -> list[Path]:
    return _module_family_dependency_paths(
        package_prefix="pm15min.research.features",
        fallback_paths=[Path(build_feature_frame_df.__code__.co_filename).resolve()],
    )


def _module_family_dependency_paths(
    *,
    package_prefix: str,
    fallback_paths: list[Path] | None = None,
) -> list[Path]:
    paths: dict[str, Path] = {}
    for module_name, module in sys.modules.items():
        if module_name != package_prefix and not module_name.startswith(f"{package_prefix}."):
            continue
        file_name = getattr(module, "__file__", None)
        if not file_name:
            continue
        try:
            path = Path(file_name).resolve()
        except Exception:
            continue
        if path.suffix not in {".py", ".pyi"}:
            continue
        paths[str(path)] = path
    for fallback_path in fallback_paths or []:
        try:
            path = Path(fallback_path).resolve()
        except Exception:
            continue
        if path.suffix not in {".py", ".pyi"}:
            continue
        paths[str(path)] = path
    return [paths[key] for key in sorted(paths)]


def _latest_path_mtime(path: Path) -> float | None:
    try:
        if not path.exists():
            return None
        latest = float(path.stat().st_mtime)
        if path.is_dir():
            for child in path.rglob("*"):
                try:
                    latest = max(latest, float(child.stat().st_mtime))
                except Exception:
                    continue
        return latest
    except Exception:
        return None


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _cfg_with_feature_set(cfg: ResearchConfig, *, feature_set: str) -> ResearchConfig:
    if cfg.feature_set == feature_set:
        return cfg
    return ResearchConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        profile=cfg.profile,
        source_surface=cfg.source_surface,
        feature_set=feature_set,
        label_set=cfg.label_set,
        target=cfg.target,
        model_family=cfg.model_family,
        run_prefix=cfg.run_prefix,
        root=cfg.layout.storage.rewrite_root,
    )


def _cfg_with_label_set(cfg: ResearchConfig, *, label_set: str) -> ResearchConfig:
    normalized = normalize_label_set(label_set)
    if cfg.label_set == normalized:
        return cfg
    return ResearchConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        profile=cfg.profile,
        source_surface=cfg.source_surface,
        feature_set=cfg.feature_set,
        label_set=normalized,
        target=cfg.target,
        model_family=cfg.model_family,
        run_prefix=cfg.run_prefix,
        root=cfg.layout.storage.rewrite_root,
    )


def _data_cfg(cfg: ResearchConfig) -> DataConfig:
    return DataConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        surface=cfg.source_surface,
        root=cfg.layout.storage.rewrite_root,
    )


def _lock_root(cfg: ResearchConfig) -> Path:
    root = cfg.layout.storage.locks_root
    root.mkdir(parents=True, exist_ok=True)
    return root


def _rebuild_feature_frame(cfg: ResearchConfig) -> dict[str, object]:
    from pm15min.research.datasets.feature_frames import build_feature_frame_dataset

    return build_feature_frame_dataset(cfg, skip_freshness=True)


def _rebuild_label_frame(cfg: ResearchConfig) -> dict[str, object]:
    from pm15min.research.labels.datasets import build_label_frame_dataset

    return build_label_frame_dataset(cfg, skip_freshness=True)
