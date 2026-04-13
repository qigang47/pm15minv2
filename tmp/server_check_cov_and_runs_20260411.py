from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(".")
ASSETS = ["btc", "eth", "sol", "xrp"]
RUN_ROOTS = {
    "btc": ROOT / "research/experiments/runs/suite=baseline_focus_feature_search_btc_reversal_40plus_2usd_5max_20260409/run=srv_rev40_fixlabel_20260410",
    "eth": ROOT / "research/experiments/runs/suite=baseline_focus_feature_search_eth_reversal_40plus_2usd_5max_20260409/run=srv_rev40_fixlabel_20260410",
    "sol": ROOT / "research/experiments/runs/suite=baseline_focus_feature_search_sol_reversal_38band_2usd_5max_20260410/run=srv_rev40_fixlabel_20260410",
    "xrp": ROOT / "research/experiments/runs/suite=baseline_focus_feature_search_xrp_reversal_38band_2usd_5max_20260410/run=srv_rev40_fixlabel_20260410",
}


def _stats(df: pd.DataFrame, cols: list[str]) -> dict[str, object]:
    ts = pd.to_datetime(pd.to_numeric(df["cycle_start_ts"], errors="coerce"), unit="s", utc=True, errors="coerce").dropna()
    out: dict[str, object] = {
        "rows": int(len(df)),
        "min": str(ts.min()) if not ts.empty else None,
        "max": str(ts.max()) if not ts.empty else None,
    }
    for col in cols:
        out[col] = int(pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int).sum())
    return out


def main() -> None:
    report: dict[str, object] = {"coverage": {}, "runs": {}}
    for asset in ASSETS:
        oracle = pd.read_parquet(
            ROOT / f"data/backtest/tables/oracle_prices/cycle=15m/asset={asset}/data.parquet",
            columns=["cycle_start_ts", "has_both"],
        )
        truth = pd.read_parquet(
            ROOT / f"data/backtest/tables/truth/cycle=15m/asset={asset}/data.parquet",
            columns=["cycle_start_ts", "resolved", "full_truth"],
        )
        labels = pd.read_parquet(
            ROOT / f"research/label_frames/cycle=15m/asset={asset}/label_set=truth/data.parquet",
            columns=["cycle_start_ts", "resolved", "full_truth"],
        )
        report["coverage"][asset] = {
            "oracle": _stats(oracle, ["has_both"]),
            "truth": _stats(truth, ["resolved", "full_truth"]),
            "labels": _stats(labels, ["resolved", "full_truth"]),
        }

    for asset, run_root in RUN_ROOTS.items():
        payload: dict[str, object] = {}
        failed = run_root / "failed_cases.csv"
        if failed.exists():
            df = pd.read_csv(failed)
            payload["failed_rows"] = int(len(df))
            for col in ["failure_stage", "error_type", "error_message", "backtest_run_dir", "training_run_dir"]:
                if col in df.columns:
                    payload[col] = df[col].dropna().astype(str).head(3).tolist()
        summary = run_root / "summary.json"
        if summary.exists():
            try:
                payload["summary"] = json.loads(summary.read_text())
            except Exception as exc:
                payload["summary_error"] = str(exc)
        report["runs"][asset] = payload
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
