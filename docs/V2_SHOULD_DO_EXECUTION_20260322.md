# v2 本轮 should-do 执行单（2026-03-22）

这份文档只记录这一轮明确要收口的三件事，不再混入更大的 roadmap。

目标：

- 把 `orderbook-fleet --loop` 变成正式可启动的后台入口
- 把 `show-ready` / `show-latest-runner` 的热缓存 stale/missing 诊断补成 operator 可直接读
- 把 v2 的 `auto-redeem` 补成正式 daemon loop 和启动入口

---

## 1. `orderbook-fleet --loop` 正式启动脚本

验收口径：

- 有独立 entrypoint，不再借 legacy recorder 脚本兜底
- 固定走 `PYTHONPATH=v2/src python -m pm15min data run orderbook-fleet`
- 默认四币 `btc,eth,sol,xrp`
- 默认后台 loop，不重复启动同类进程
- stdout/stderr 有固定落点

状态：已完成

落点：

- `scripts/entrypoints/start_v2_orderbook_fleet.sh`
- `scripts/entrypoints/README.md`

---

## 2. `show-ready` / `show-latest-runner` 热缓存诊断

验收口径：

- operator 主视图直接暴露 recent orderbook hot cache 的 `status` / `reason`
- 至少区分 `missing` / `empty` / `stale` / `ok`
- `next_actions` 能直接把 operator 指到 hot cache，而不是只让人翻原始 parquet
- 测试覆盖 `missing` / `stale`

状态：已完成

落点：

- `v2/src/pm15min/live/orderbook_hot_cache.py`
- `v2/src/pm15min/live/readiness_state.py`
- `v2/src/pm15min/live/operator_summary.py`
- `v2/src/pm15min/live/operator_smoke.py`
- `v2/src/pm15min/live/operator_action_followups_blockers.py`
- `v2/tests/test_live_service.py`

---

## 3. v2 native `auto-redeem` daemon loop

验收口径：

- `live redeem-loop` 成为正式 CLI
- loop 语义支持 daemon 运行，不是只跑一轮
- redeem runner 自己写 summary / snapshot / jsonl log
- 有独立 entrypoint 启动脚本

状态：已完成

落点：

- `v2/src/pm15min/live/redeem_api.py`
- `v2/src/pm15min/live/redeem_runtime.py`
- `v2/src/pm15min/live/cli_parser.py`
- `v2/src/pm15min/live/cli.py`
- `scripts/entrypoints/start_v2_auto_redeem.sh`
- `v2/tests/test_live_redeem_loop.py`
- `v2/tests/test_cli.py`

---

## 4. 本轮验证

应做验证：

- 相关单测通过
- v2 全量测试再跑一轮

状态：待本轮代码改完后执行
