from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(".")
FAILED = ROOT / "research/experiments/runs/suite=baseline_focus_feature_search_btc_reversal_40plus_2usd_5max_20260409/run=srv_rev40_fixlabel_20260410/failed_cases.csv"


def _check_parquet(path: Path) -> str | None:
    try:
        pd.read_parquet(path)
    except Exception as exc:  # pragma: no cover - operational helper
        return str(exc)
    return None


def main() -> None:
    df = pd.read_csv(FAILED)
    targets: set[Path] = set()
    for col in ["training_run_dir", "bundle_dir", "backtest_run_dir"]:
        if col not in df.columns:
            continue
        for raw in df[col].dropna().astype(str):
            token = raw.strip()
            if token:
                targets.add(Path(token))

    report: dict[str, object] = {"targets": [str(path) for path in sorted(targets)], "bad_files": []}
    for target in sorted(targets):
        if not target.exists():
            report["bad_files"].append({"path": str(target), "error": "missing"})
            continue
        if target.is_file() and target.suffix == ".parquet":
            err = _check_parquet(target)
            if err:
                report["bad_files"].append({"path": str(target), "error": err})
            continue
        for path in sorted(target.rglob("*.parquet")):
            err = _check_parquet(path)
            if err:
                report["bad_files"].append({"path": str(path), "error": err})
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
