from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.research.config import ResearchConfig


def _read_required_parquet(path: Path, *, kind: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {kind}: {path}")
    return pd.read_parquet(path)


def load_feature_frame(cfg: ResearchConfig, *, feature_set: str | None = None) -> pd.DataFrame:
    selected = feature_set or cfg.feature_set
    return _read_required_parquet(
        cfg.layout.feature_frame_path(selected, source_surface=cfg.source_surface),
        kind="feature_frame parquet",
    )


def load_label_frame(cfg: ResearchConfig, *, label_set: str | None = None) -> pd.DataFrame:
    selected = label_set or cfg.label_set
    return _read_required_parquet(
        cfg.layout.label_frame_path(selected),
        kind="label_frame parquet",
    )
