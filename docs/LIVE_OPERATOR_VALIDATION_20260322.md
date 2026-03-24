# Live Operator Validation Record (2026-03-22 HKT / 2026-03-21 UTC)

这份记录只做一件事：

- 把一轮真实 `operator` 主路径验证固定成可回溯证据

本轮记录范围：

- canonical live scope
- `profile=deep_otm`
- `target=direction`
- `cycle=15m`
- `adapter=direct`
- `markets=sol,xrp`

本轮命令顺序：

1. `check-trading-gateway --probe-open-orders --probe-positions`
2. `runner-once --dry-run-side-effects`
3. `show-ready`

口径说明：

- 这里记录的是“真实命令在当时环境下的语义结果”，不是把某次 `row_count` 写成长期 contract
- `row_count` 只代表这次观测值，后续可以变化
- 更应该关注的是：
  - gateway / probe 是否打通
  - runner dry-run 能否走通
  - `show-ready` 把当前 blocker 归类成哪种语义

## 环境备注

- 本轮命令均返回退出码 `0`
- 每条命令都打印了 `requests` 的版本警告
- `runner-once` 额外打印了：
  - LightGBM 参数 warning
  - `sklearn` `InconsistentVersionWarning`
- 本轮把这些 warning 视为环境 / 模型序列化兼容性噪音，不视为 operator 路径失败

## SOL

### 1. Gateway Probe

执行时间（UTC）：

- `2026-03-21T16:56:09Z`

命令：

```bash
PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct --probe-open-orders --probe-positions
```

关键结果：

- `snapshot_ts=2026-03-21T16:56:34Z`
- `ok=true`
- `trading_gateway.adapter=direct`
- `trading_gateway.gateway_class=DirectLiveTradingGateway`
- `probes.open_orders.status=ok`
- `probes.open_orders.row_count=0`
- `probes.positions.status=ok`
- `probes.positions.row_count=2008`

判断：

- `sol + direct` 的 gateway build、只读 probe、positions 读路本轮已真实打通

### 2. Runner Dry-Run

执行时间（UTC）：

- `2026-03-21T16:56:40Z`

命令：

```bash
PYTHONPATH=v2/src python -m pm15min live runner-once --market sol --profile deep_otm --target direction --adapter direct --dry-run-side-effects
```

关键结果：

- `run_started_at=2026-03-21T16:56:59Z`
- `status=ok`
- `completed_iterations=1`
- `errors=0`
- `last_iteration.snapshot_ts=2026-03-21T16-58-12Z`
- `last_iteration.decision.status=reject`
- `last_iteration.execution.status=no_action`
- `last_iteration.order_action.status=skipped`
- `last_iteration.order_action.reason=execution_not_plan:no_action`
- `last_iteration.cancel_action.status=ok`
- `last_iteration.redeem_action.status=ok`
- `last_iteration.runner_health.overall_status=warning`
- `last_iteration.runner_health.primary_blocker=decision_not_accept`
- `last_iteration.runner_health.blocker_stage=decision`
- `risk_summary.decision.top_reject_reasons=[confidence_below_threshold, quote_missing_inputs, quote_up_quote_missing, quote_down_quote_missing]`
- `risk_summary.foundation.status=ok_with_errors`
- `risk_summary.foundation.issue_codes=[oracle_direct_rate_limited]`

判断：

- `sol + direct` 的 runner dry-run 这次已经能完整跑完一轮
- 本轮不是 side-effect 接入失败，而是：
  - decision 因 `quote_inputs_missing` 等原因拒单
  - foundation 同时挂着 `oracle_direct_rate_limited` warning

### 3. Show Ready

执行时间（UTC）：

- `2026-03-21T16:58:18Z`

命令：

```bash
PYTHONPATH=v2/src python -m pm15min live show-ready --market sol --profile deep_otm --adapter direct
```

关键结果：

- `status=not_ready`
- `primary_blocker=decision_not_accept`
- `ready_for_side_effects=false`
- `gateway_ok=true`
- `gateway_failed_probes=[]`
- `operator_smoke_summary.status=blocked`
- `operator_smoke_summary.reason=runner_data_blocked`
- `operator_smoke_summary.runner_smoke_status=data_blocked`
- `operator_smoke_summary.runner_decision_reject_category=quote_inputs_missing`
- `foundation_status=ok_with_errors`
- `foundation_issue_codes=[oracle_direct_rate_limited]`

判断：

- `show-ready` 当前把 `sol` 这条线归类为：
  - gateway / probe 已通
  - 但 runner 当前卡在数据层
  - 当前更准确是 `runner_data_blocked`，不是 `gateway_failed`

## XRP

### 1. Gateway Probe

执行时间（UTC）：

- `2026-03-21T16:58:47Z`

命令：

```bash
PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market xrp --profile deep_otm --adapter direct --probe-open-orders --probe-positions
```

关键结果：

- `snapshot_ts=2026-03-21T16:59:09Z`
- `ok=true`
- `trading_gateway.adapter=direct`
- `trading_gateway.gateway_class=DirectLiveTradingGateway`
- `probes.open_orders.status=ok`
- `probes.open_orders.row_count=0`
- `probes.positions.status=ok`
- `probes.positions.row_count=2008`

判断：

- `xrp + direct` 的 gateway build、只读 probe、positions 读路本轮已真实打通

### 2. Runner Dry-Run

执行时间（UTC）：

- `2026-03-21T16:59:17Z`

命令：

```bash
PYTHONPATH=v2/src python -m pm15min live runner-once --market xrp --profile deep_otm --target direction --adapter direct --dry-run-side-effects
```

关键结果：

- `run_started_at=2026-03-21T16:59:35Z`
- `status=ok`
- `completed_iterations=1`
- `errors=0`
- `last_iteration.snapshot_ts=2026-03-21T17-00-46Z`
- `last_iteration.decision.status=reject`
- `last_iteration.execution.status=no_action`
- `last_iteration.order_action.status=skipped`
- `last_iteration.order_action.reason=execution_not_plan:no_action`
- `last_iteration.cancel_action.status=ok`
- `last_iteration.redeem_action.status=ok`
- `last_iteration.runner_health.overall_status=warning`
- `last_iteration.runner_health.primary_blocker=decision_not_accept`
- `last_iteration.runner_health.blocker_stage=decision`
- `risk_summary.decision.top_reject_reasons=[entry_price_max]`
- `risk_summary.foundation.status=ok_with_errors`
- `risk_summary.foundation.issue_codes=[oracle_direct_rate_limited]`

判断：

- `xrp + direct` 的 runner dry-run 这次也能完整跑完一轮
- 本轮 blocker 不是 quote 输入缺失，而是策略层：
  - `entry_price_max`

### 3. Show Ready

执行时间（UTC）：

- `2026-03-21T17:00:55Z`

命令：

```bash
PYTHONPATH=v2/src python -m pm15min live show-ready --market xrp --profile deep_otm --adapter direct
```

关键结果：

- `status=not_ready`
- `primary_blocker=decision_not_accept`
- `ready_for_side_effects=false`
- `gateway_ok=true`
- `gateway_failed_probes=[]`
- `operator_smoke_summary.status=operational`
- `operator_smoke_summary.reason=strategy_reject_only`
- `operator_smoke_summary.runner_smoke_status=strategy_only_blocked`
- `operator_smoke_summary.runner_decision_reject_category=entry_or_quote_threshold`
- `operator_smoke_summary.runner_decision_reject_interpretation=entry_price_above_live_cap`
- `foundation_status=ok_with_errors`
- `foundation_issue_codes=[oracle_direct_rate_limited]`

判断：

- `xrp` 当前已经不是 infra / data blocker
- `show-ready` 对这条线的更准确语义是：
  - 主路径 operational
  - 但本轮应 `strategy_reject_only`
  - 同时挂着 foundation warning

## 本轮结论

- `direct` adapter 的 gateway + probe 路径，这次对 `sol` / `xrp` 都已真实打通
- `direct` adapter 的 `runner-once --dry-run-side-effects`，这次对 `sol` / `xrp` 都能完整跑完一轮并返回 `status=ok`
- 两个市场当前都不是 gateway 接入故障
- 两个市场当前都仍挂着同一个 foundation warning：
  - `oracle_direct_rate_limited`
- 当前分歧点在 runner 语义：
  - `sol`
    - `runner_data_blocked`
    - 主 reject 类别偏 `quote_inputs_missing`
  - `xrp`
    - `strategy_reject_only`
    - 主 reject 类别偏 `entry_price_above_live_cap`

## 后续如何读这份记录

- 如果以后 probe `row_count` 变化，不代表这份记录失效
- 这份记录真正固定下来的，是当前 operator 语义分类：
  - gateway / probe 可通
  - `sol` 当前更像 `runner_data_blocked`
  - `xrp` 当前更像 `strategy_reject_only`
  - foundation warning 当前仍会把 `show-ready` 保持在 `not_ready`
