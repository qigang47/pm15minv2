from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pm15min.research.config import ResearchConfig
from pm15min.research.experiments.runner import run_experiment_suite


RUN_LABEL = "srv_rev40_apr9_rerun_20260411"
SUITES = [
    "baseline_focus_feature_search_eth_reversal_40plus_2usd_5max_20260409",
    "baseline_focus_feature_search_sol_reversal_38band_2usd_5max_20260410",
    "baseline_focus_feature_search_xrp_reversal_38band_2usd_5max_20260410",
    "baseline_focus_feature_search_btc_reversal_40plus_2usd_5max_20260409",
]


def main() -> None:
    root = Path(".")
    out_path = root / "var" / "research" / "logs" / "rerun_failed_suites_20260411.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "run_label": RUN_LABEL,
        "results": [],
    }
    for suite in SUITES:
        cfg = ResearchConfig.build(market="btc", cycle="15m", source_surface="backtest", root=root)
        started = datetime.now(timezone.utc).isoformat()
        try:
            summary = run_experiment_suite(cfg=cfg, suite_name=suite, run_label=RUN_LABEL)
            payload["results"].append(
                {
                    "suite": suite,
                    "started_at": started,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "ok": True,
                    "summary": summary,
                }
            )
        except Exception as exc:  # pragma: no cover - operational launcher
            payload["results"].append(
                {
                    "suite": suite,
                    "started_at": started,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "ok": False,
                    "error": str(exc),
                }
            )
            out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            raise
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["finished_at"] = datetime.now(timezone.utc).isoformat()
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
