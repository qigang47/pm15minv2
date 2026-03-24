# 当前剩余工作 Checklist（2026-03-22）

这份文档只跟踪这轮还需要收口的三件事：

1. 给 `pm15min data run orderbook-fleet --loop` 配正式启动脚本
2. 把 `show-ready` / `show-latest-runner` 的热缓存 stale/missing 诊断收进 operator 主视图
3. 把 v2 原生 `auto-redeem` daemon loop 收口成正式可运行入口

原则不变：

- `v2` 是唯一长期主线
- 运维入口优先看简单、稳定、可重复执行
- 代码和脚本都按 `data / live / operator` 分层，不把兼容层和主线搅在一起

---

## 一句话状态

- 这轮三件事已经全部收口
- `show-ready` / `show-latest-runner` 的热缓存诊断已经落地
- `live redeem-loop` 已收口成可长期运行的 v2 daemon loop 入口
- `orderbook-fleet` / `auto-redeem` 都已经有官方 v2 entrypoint 脚本和验证记录

---

## 本轮事项

### 1. Orderbook Fleet 正式启动脚本

状态：已完成

- [x] `data run orderbook-fleet` 已支持四币并行录制：`btc,eth,sol,xrp`
- [x] 默认轮询频率已收紧到 `0.35s`
- [x] `recent.parquet` 热窗口已支持 `--recent-window-minutes`
- [x] 增加官方脚本：`scripts/entrypoints/start_v2_orderbook_fleet.sh`
- [x] 在 `scripts/entrypoints/README.md` 明确它是 v2 canonical 入口

收口标准：

- 值班时不再手敲长命令
- 启动脚本只负责环境、日志、防重复启动
- 真实 recorder state / recorder log 仍以 v2 canonical 路径为准

### 2. Operator 热缓存诊断

状态：已完成

- [x] `show-ready` / `show-latest-runner` 已能返回：
  - `operator_summary.orderbook_hot_cache_status`
  - `operator_summary.orderbook_hot_cache_reason`
  - `operator_summary.orderbook_hot_cache_summary`
- [x] `operator_smoke_summary` 已带上：
  - `orderbook_hot_cache_status`
  - `orderbook_hot_cache_reason`
- [x] `next_actions` 已明确提示去看热缓存是 `missing / empty / stale`

当前目的很简单：

- 当 `quote_inputs_missing` 发生时，operator 不用再猜是盘口没录到、热缓存太旧，还是只是策略拒单

### 3. v2 Native Auto Redeem Daemon Loop

状态：已完成

- [x] `live apply-redeem-policy` 已在 v2 主线
- [x] `live redeem-loop` 已成为 v2 原生命令
- [x] `live redeem-loop --loop --iterations 0` 已支持真正 daemon 语义
- [x] redeem runner summary / snapshot / jsonl log 已进入 v2 layout
- [x] 增加官方脚本：`scripts/entrypoints/start_v2_auto_redeem.sh`
- [x] 在 operator / entrypoint 文档里把它写成正式入口

收口标准：

- 运维层面不再依赖旧的 `live_trading/auto_redeem.py`
- redeem loop 的运行状态以 v2 state/log 路径为准
- legacy redeem 兼容层只作为过渡，不再作为默认启动面

---

## 本轮执行顺序

1. 先补 `start_v2_orderbook_fleet.sh`
2. 再补 `start_v2_auto_redeem.sh`
3. 同步 `LIVE_OPERATOR_RUNBOOK.md` 和 `scripts/entrypoints/README.md`
4. 跑 v2 测试
5. 如环境允许，再补一次真实 smoke

---

## 验证清单

- [x] `PYTHONPATH=v2/src pytest -q v2/tests`
- [x] `PYTHONPATH=v2/src python -m pm15min data run orderbook-fleet --markets btc,eth,sol,xrp --iterations 1 --poll-interval-sec 0.35`
- [x] `PYTHONPATH=v2/src python -m pm15min live redeem-loop --market sol --profile deep_otm --adapter direct --iterations 1 --dry-run`

说明：

- 第一条是本地结构回归验证
- 后两条是运行面 smoke
- 本轮实际结果：
  - `pytest`: `188 passed, 1 warning`
  - `orderbook-fleet`: `btc/eth/sol/xrp` 全部 `status=ok`，最近一次抓取时间 `2026-03-21T18:03:40.713Z`
  - `redeem-loop --dry-run`: `status=ok`，当前因为 `positions_snapshot_unavailable` 跳过赎回，这符合 dry-run 现状
