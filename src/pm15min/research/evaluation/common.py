from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.research.config import ResearchConfig


def resolve_backtest_run_dir(
    cfg: ResearchConfig,
    *,
    profile: str,
    spec_name: str,
    run_label: str | None = None,
) -> Path:
    root = cfg.layout.backtests_root / f"profile={profile}" / f"spec={spec_name}"
    if run_label:
        direct = root / f"run={run_label}"
        if direct.exists():
            return direct
        candidate = Path(run_label)
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"Backtest run not found: {run_label}")
    candidates = sorted(
        [path for path in root.glob("run=*") if path.is_dir()],
        key=lambda path: (path.stat().st_mtime_ns, path.name),
    )
    if not candidates:
        raise FileNotFoundError(f"No backtest runs available under {root}")
    return candidates[-1]


def load_backtest_trades(run_dir: Path) -> pd.DataFrame:
    path = run_dir / "trades.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing backtest trades: {path}")
    return pd.read_parquet(path)


def load_backtest_summary(run_dir: Path) -> dict[str, object]:
    path = run_dir / "summary.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing backtest summary: {path}")
    return json.loads(path.read_text(encoding="utf-8"))
