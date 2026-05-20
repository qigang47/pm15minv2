#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ProcessMemory:
    pid: int
    kind: str
    rss_kb: int | None
    pss_kb: int | None
    shared_kb: int | None
    private_kb: int | None
    pmem: float | None
    pcpu: float | None
    elapsed: str
    cmd: str

    @property
    def rss_mib(self) -> float | None:
        return None if self.rss_kb is None else round(float(self.rss_kb) / 1024.0, 1)

    @property
    def pss_mib(self) -> float | None:
        return None if self.pss_kb is None else round(float(self.pss_kb) / 1024.0, 1)

    @property
    def private_mib(self) -> float | None:
        return None if self.private_kb is None else round(float(self.private_kb) / 1024.0, 1)

    @property
    def shared_mib(self) -> float | None:
        return None if self.shared_kb is None else round(float(self.shared_kb) / 1024.0, 1)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.update(
            {
                "rss_mib": self.rss_mib,
                "pss_mib": self.pss_mib,
                "private_mib": self.private_mib,
                "shared_mib": self.shared_mib,
            }
        )
        return payload


def parse_smaps_rollup(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if ":" not in line:
            continue
        name, raw_value = line.split(":", 1)
        parts = raw_value.strip().split()
        if not parts:
            continue
        try:
            value = int(parts[0])
        except ValueError:
            continue
        key = name.strip().lower()
        values[f"{key}_kb"] = value
    shared_kb = int(values.get("shared_clean_kb", 0)) + int(values.get("shared_dirty_kb", 0))
    private_kb = int(values.get("private_clean_kb", 0)) + int(values.get("private_dirty_kb", 0))
    return {
        "rss_kb": int(values.get("rss_kb", 0)),
        "pss_kb": int(values.get("pss_kb", 0)),
        "shared_kb": shared_kb,
        "private_kb": private_kb,
    }


def classify_process(cmd: str) -> str | None:
    text = str(cmd)
    lowered = text.lower()
    if "run_quick_screen_queue_batch.py" in text or "run_quick_screen_pool.py" in text:
        return "quick_screen"
    if "research experiment run-suite" in lowered:
        return "formal"
    if "orderbook" in lowered and (
        "recorder" in lowered
        or "record" in lowered
        or "orderbook-depth" in lowered
        or "start_v2_orderbook_fleet" in lowered
    ):
        return "orderbook"
    return None


def collect_process_memory(*, proc_root: Path = Path("/proc")) -> list[ProcessMemory]:
    rows: list[ProcessMemory] = []
    for record in _ps_records():
        kind = classify_process(record["cmd"])
        if kind is None:
            continue
        pid = int(record["pid"])
        smaps = _safe_parse_smaps(proc_root / str(pid) / "smaps_rollup")
        rss_kb = _int_or_none(record.get("rss"))
        rows.append(
            ProcessMemory(
                pid=pid,
                kind=kind,
                rss_kb=_nonzero_or_none(smaps.get("rss_kb")) or rss_kb,
                pss_kb=_nonzero_or_none(smaps.get("pss_kb")),
                shared_kb=_nonzero_or_none(smaps.get("shared_kb")),
                private_kb=_nonzero_or_none(smaps.get("private_kb")),
                pmem=_float_or_none(record.get("pmem")),
                pcpu=_float_or_none(record.get("pcpu")),
                elapsed=str(record.get("etime") or ""),
                cmd=str(record.get("cmd") or ""),
            )
        )
    return sorted(rows, key=lambda item: (item.kind, item.pid))


def build_report(*, proc_root: Path = Path("/proc")) -> dict[str, Any]:
    rows = collect_process_memory(proc_root=proc_root)
    totals: dict[str, dict[str, int | None]] = {}
    for kind in sorted({row.kind for row in rows}):
        members = [row for row in rows if row.kind == kind]
        totals[kind] = {
            "processes": len(members),
            "rss_kb": _sum_known(row.rss_kb for row in members),
            "pss_kb": _sum_known(row.pss_kb for row in members),
            "shared_kb": _sum_known(row.shared_kb for row in members),
            "private_kb": _sum_known(row.private_kb for row in members),
        }
    return {
        "processes": [row.to_dict() for row in rows],
        "totals": totals,
    }


def render_report(report: dict[str, Any]) -> str:
    rows = list(report.get("processes") or [])
    if not rows:
        return "no quick_screen/formal/orderbook processes found"
    lines = [
        "kind          pid     rss_mib   pss_mib private_mib shared_mib  cpu%  mem%  elapsed  cmd",
        "------------ ------- --------- --------- ----------- ---------- ----- ----- -------- ----------------",
    ]
    for row in rows:
        lines.append(
            f"{str(row.get('kind') or ''):<12} "
            f"{int(row.get('pid') or 0):>7} "
            f"{_fmt_mib(row.get('rss_mib')):>9} "
            f"{_fmt_mib(row.get('pss_mib')):>9} "
            f"{_fmt_mib(row.get('private_mib')):>11} "
            f"{_fmt_mib(row.get('shared_mib')):>10} "
            f"{_fmt_float(row.get('pcpu')):>5} "
            f"{_fmt_float(row.get('pmem')):>5} "
            f"{str(row.get('elapsed') or ''):>8} "
            f"{_short_cmd(str(row.get('cmd') or ''))}"
        )
    totals = report.get("totals") if isinstance(report.get("totals"), dict) else {}
    if totals:
        lines.append("")
        lines.append("totals:")
        for kind, total in sorted(totals.items()):
            if not isinstance(total, dict):
                continue
            lines.append(
                f"  {kind}: processes={total.get('processes')} "
                f"rss={_kb_to_mib(total.get('rss_kb'))}MiB "
                f"pss={_kb_to_mib(total.get('pss_kb'))}MiB "
                f"private={_kb_to_mib(total.get('private_kb'))}MiB "
                f"shared={_kb_to_mib(total.get('shared_kb'))}MiB"
            )
    return "\n".join(lines)


def _ps_records() -> list[dict[str, str]]:
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid=,etime=,rss=,pmem=,pcpu=,args="],
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError:
        return []
    rows: list[dict[str, str]] = []
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 5)
        if len(parts) < 6:
            continue
        pid, etime, rss, pmem, pcpu, cmd = parts
        rows.append({"pid": pid, "etime": etime, "rss": rss, "pmem": pmem, "pcpu": pcpu, "cmd": cmd})
    return rows


def _safe_parse_smaps(path: Path) -> dict[str, int]:
    try:
        return parse_smaps_rollup(path)
    except Exception:
        return {}


def _int_or_none(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(str(value).strip())
    except Exception:
        return None


def _float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        return float(str(value).strip())
    except Exception:
        return None


def _nonzero_or_none(value: object) -> int | None:
    parsed = _int_or_none(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _sum_known(values) -> int | None:
    cleaned = [int(value) for value in values if value is not None]
    return sum(cleaned) if cleaned else None


def _kb_to_mib(value: object) -> str:
    parsed = _int_or_none(value)
    if parsed is None:
        return "?"
    return str(round(float(parsed) / 1024.0, 1))


def _fmt_mib(value: object) -> str:
    if value is None:
        return "?"
    try:
        return f"{float(value):.1f}"
    except Exception:
        return "?"


def _fmt_float(value: object) -> str:
    if value is None:
        return "?"
    try:
        return f"{float(value):.1f}"
    except Exception:
        return "?"


def _short_cmd(cmd: str, *, max_len: int = 96) -> str:
    text = " ".join(cmd.split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def main() -> int:
    parser = argparse.ArgumentParser(description="Report memory for quick-screen, formal, and orderbook processes.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--proc-root", type=Path, default=Path("/proc"))
    args = parser.parse_args()

    report = build_report(proc_root=args.proc_root)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(render_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
