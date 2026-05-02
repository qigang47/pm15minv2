#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


RUNS = {
    "btc": {
        "run_label": "auto_btc_direction_entry45_50_prob60_formal_20260424",
        "suite": "baseline_midprice_direction_btc_2usd_5max_20260424",
    },
    "eth": {
        "run_label": "auto_eth_direction_entry45_50_prob60_formal_20260424",
        "suite": "baseline_midprice_direction_eth_2usd_5max_20260424",
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_dir(root: Path, info: dict[str, str]) -> Path:
    return root / "research/experiments/runs" / f"suite={info['suite']}" / f"run={info['run_label']}"


def latest_stage(root: Path, info: dict[str, str]) -> tuple[str, str]:
    path = run_dir(root, info) / "logs/suite.jsonl"
    if not path.exists():
        return "starting", ""
    last = ""
    try:
        with path.open("rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - 8192))
            lines = fh.read().decode("utf-8", errors="ignore").strip().splitlines()
            last = lines[-1] if lines else ""
        payload = json.loads(last) if last else {}
    except Exception:
        return "unknown", last[-240:]
    text = " ".join(
        str(payload.get(key) or "")
        for key in ("event", "stage", "current_stage", "summary", "case_label")
    )
    low = text.lower()
    if "backtest" in low:
        bucket = "backtest"
    elif "train" in low or "bundle" in low or "warm" in low:
        bucket = "training_or_bundle"
    else:
        bucket = str(payload.get("event") or payload.get("stage") or "unknown")
    return bucket, json.dumps(payload, ensure_ascii=False)[:500]


def ps_rows() -> list[dict[str, object]]:
    out = subprocess.check_output(["ps", "-eo", "pid,ppid,stat,pcpu,pmem,rss,args"], text=True)
    rows = []
    for line in out.splitlines()[1:]:
        parts = line.split(None, 6)
        if len(parts) < 7:
            continue
        pid, ppid, stat, pcpu, pmem, rss, args = parts
        try:
            rows.append(
                {
                    "pid": int(pid),
                    "ppid": int(ppid),
                    "stat": stat,
                    "pcpu": float(pcpu),
                    "pmem": float(pmem),
                    "rss_kb": int(rss),
                    "args": args,
                }
            )
        except Exception:
            continue
    return rows


def sample_once(root: Path) -> list[dict[str, object]]:
    all_rows = ps_rows()
    samples = []
    now = utc_now()
    for market, info in RUNS.items():
        run_label = info["run_label"]
        matched = [row for row in all_rows if run_label in str(row["args"])]
        python_rows = [
            row
            for row in matched
            if "python" in str(row["args"]) and "pm15min research experiment run-suite" in str(row["args"])
        ]
        main = max(python_rows, key=lambda row: int(row["rss_kb"]), default=None)
        stage, stage_detail = latest_stage(root, info)
        samples.append(
            {
                "ts": now,
                "market": market,
                "stage": stage,
                "pid": "" if main is None else int(main["pid"]),
                "process_count": len(matched),
                "python_rss_mb": round((int(main["rss_kb"]) if main else 0) / 1024, 1),
                "total_rss_mb": round(sum(int(row["rss_kb"]) for row in matched) / 1024, 1),
                "python_cpu_pct": round(float(main["pcpu"]) if main else 0.0, 1),
                "total_cpu_pct": round(sum(float(row["pcpu"]) for row in matched), 1),
                "stage_detail": stage_detail,
            }
        )
    return samples


def update_summary(csv_path: Path, summary_path: Path) -> None:
    if not csv_path.exists():
        return
    with csv_path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    summary: dict[str, object] = {
        "updated_at": utc_now(),
        "csv_path": str(csv_path),
        "by_market": {},
    }
    for market in RUNS:
        sub = [row for row in rows if row.get("market") == market]
        buckets: dict[str, dict[str, object]] = {}
        for row in sub:
            stage = row.get("stage") or "unknown"
            bucket = buckets.setdefault(
                stage,
                {"samples": 0, "max_python_rss_mb": 0.0, "max_total_rss_mb": 0.0, "max_cpu_pct": 0.0},
            )
            bucket["samples"] = int(bucket["samples"]) + 1
            bucket["max_python_rss_mb"] = max(float(bucket["max_python_rss_mb"]), float(row.get("python_rss_mb") or 0))
            bucket["max_total_rss_mb"] = max(float(bucket["max_total_rss_mb"]), float(row.get("total_rss_mb") or 0))
            bucket["max_cpu_pct"] = max(float(bucket["max_cpu_pct"]), float(row.get("total_cpu_pct") or 0))
        summary["by_market"][market] = {
            "samples": len(sub),
            "latest": sub[-1] if sub else None,
            "by_stage": buckets,
        }
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--out-dir", default="var/research/autorun/manual_midprice")
    parser.add_argument("--interval-sec", type=int, default=15)
    parser.add_argument("--duration-sec", type=int, default=3600)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    out_dir = (root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "memory_watch.csv"
    summary_path = out_dir / "memory_watch_summary.json"
    pid_path = out_dir / "memory_watch.pid"
    pid_path.write_text(str(os.getpid()), encoding="utf-8")

    header = [
        "ts",
        "market",
        "stage",
        "pid",
        "process_count",
        "python_rss_mb",
        "total_rss_mb",
        "python_cpu_pct",
        "total_cpu_pct",
        "stage_detail",
    ]
    write_header = not csv_path.exists()
    end_at = time.time() + max(1, int(args.duration_sec))
    with csv_path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        if write_header:
            writer.writeheader()
        while time.time() < end_at:
            for row in sample_once(root):
                writer.writerow(row)
            fh.flush()
            update_summary(csv_path, summary_path)
            time.sleep(max(1, int(args.interval_sec)))
    update_summary(csv_path, summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
