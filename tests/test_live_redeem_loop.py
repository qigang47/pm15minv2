from __future__ import annotations

import json
from pathlib import Path

from pm15min.core.config import LiveConfig
from pm15min.data.io.json_files import append_jsonl
from pm15min.live.persistence import write_live_payload_pair
from pm15min.live.redeem.runtime import _resolve_iteration_limit, run_live_redeem_loop


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def test_run_live_redeem_loop_writes_summary_and_log(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    labels = iter(
        [
            "2026-03-20T00-00-00Z",
            "2026-03-20T00-00-01Z",
            "2026-03-20T00-00-02Z",
            "2026-03-20T00-00-03Z",
            "2026-03-20T00-00-04Z",
        ]
    )

    def _fake_apply_redeem_policy(*args, **kwargs):
        return {
            "snapshot_ts": next(labels),
            "status": "ok",
            "reason": "redeem_policy_applied",
            "summary": {
                "candidate_conditions": 1,
                "submitted_conditions": 1,
                "redeemed_conditions": 1,
                "error_conditions": 0,
            },
            "trading_gateway": {"adapter": "fake"},
        }

    summary = run_live_redeem_loop(
        cfg,
        iterations=2,
        loop=True,
        sleep_sec=0.0,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        max_conditions=1,
        gateway=None,
        apply_redeem_policy_fn=_fake_apply_redeem_policy,
        append_jsonl_fn=append_jsonl,
        write_live_payload_pair_fn=write_live_payload_pair,
        utc_now_iso_fn=lambda: next(labels),
    )

    assert summary["status"] == "ok"
    assert summary["completed_iterations"] == 2
    assert summary["last_iteration"]["redeemed_conditions"] == 1
    latest_path = Path(summary["latest_redeem_runner_path"])
    log_path = Path(summary["redeem_runner_log_path"])
    assert latest_path.exists()
    assert log_path.exists()

    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    assert latest["status"] == "ok"
    assert latest["completed_iterations"] == 2

    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any('"event": "redeem_iteration"' in line for line in lines)


def test_resolve_iteration_limit_supports_daemon_mode() -> None:
    assert _resolve_iteration_limit(iterations=0, loop=True) is None
    assert _resolve_iteration_limit(iterations=-1, loop=True) is None
    assert _resolve_iteration_limit(iterations=0, loop=False) == 1
    assert _resolve_iteration_limit(iterations=2, loop=True) == 2
