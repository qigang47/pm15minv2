# Live Operator Runbook

这份文档只回答运维视角的问题：

1. 当前 canonical live 到底是什么
2. 上线前应该按什么顺序检查
3. 运行中应该看什么命令、什么路径、什么状态

如果技术边界有疑问，以：

- `v2/docs/LIVE_TECHNICAL_PLAN.md`
- `v2/src/pm15min/live/`

为准。

本轮这份 runbook 已和 `CURRENT_REMAINING_CHECKLIST.md`、`REWRITE_STATUS_AND_ROADMAP.md`、`PHASE_A_TRADING_INFRA_TECHNICAL_PLAN.md` 对齐到同一套 operator blocker 语义。

最近一轮独立验证记录见：

- `v2/docs/LIVE_OPERATOR_VALIDATION_20260322.md`

---

## 1. Canonical Live Scope

当前 canonical live 主线固定为：

- `profile=deep_otm`
- `target=direction`
- `cycle=15m`
- `markets=sol,xrp`

当前真实 side-effect 主线命令：

- `live execute-latest`
- `live runner-once`
- `live runner-loop`

这些命令当前只允许：

- `target=direction`

不再允许把 `target=reversal` 混入真实 side-effect 主线。

当前 canonical operator 只读入口固定为：

- `live check-trading-gateway`
- `live show-ready`
- `live show-latest-runner`

当前 canonical 后台守护入口固定为：

- `scripts/entrypoints/start_v2_orderbook_fleet.sh`
- `scripts/entrypoints/start_v2_auto_redeem.sh`

这两层只负责：

- 激活 conda / 设置 `PYTHONPATH=v2/src`
- 防止重复启动
- 留一份很薄的 wrapper stdout/stderr 日志

真正运行状态仍以 v2 canonical 路径为准：

- orderbook fleet
  - state: `v2/var/live/state/orderbooks/.../state.json`
  - log: `v2/var/live/logs/data/recorders/.../recorder.jsonl`
- auto redeem
  - state: `v2/var/live/state/redeem_runner/.../latest.json`
  - log: `v2/var/live/logs/redeem_runner/.../redeem_runner.jsonl`

其中：

- `show-ready` / `show-latest-runner` 的 CLI 已固定 `target=direction`
- `check-trading-gateway` 不带 `target` 维度，只负责 adapter / probe / readiness

不是 operator 入口、只是查看型命令的有：

- `live show-config`
- `live show-layout`

这两个命令当前保留 compatibility inspection 能力：

- 可以查看非 canonical market/profile
- 但输出会显式写出：
  - `canonical_live_scope`
  - `cli_boundary`
  - `profile_spec_resolution`

值班时不要用它们代替：

- `live check-trading-gateway`
- `live show-ready`
- `live show-latest-runner`

---

## 2. 上线前检查顺序

### 2.1 先做 adapter health check

direct:

```bash
PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct
```

legacy:

```bash
PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter legacy
```

应该重点看：

- `status`
- `checks`
- `capabilities`
- `blocked_by`

如果 `blocked_by` 非空，先修配置或依赖，不要继续做后面的 smoke run。

这里的 `checks` 和后面的 `probes` 不是一回事：

- `checks`
  - 更接近 adapter build / auth / dependency / capability readiness
- `probes`
  - 更接近 open-orders / positions 真实读路有没有打通

所以 `checks` 通过不等于 `probes` 也通过。

### 2.2 再做只读 probe

open orders:

```bash
PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct --probe-open-orders
```

positions:

```bash
PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct --probe-positions
```

当前本机已验证过一轮：

- `direct --probe-open-orders`
  - `row_count = 0`
- `direct --probe-positions`
  - `row_count = 2001 ~ 2002`
- `legacy --probe-open-orders`
  - `row_count = 0`
- `legacy --probe-positions`
  - `row_count = 2001 ~ 2002`

这些数字不是 contract，只是最近一次本机环境结果。

人工处理时也要分开看：

- `probe-open-orders`
  - 重点验证 open-order 读路、订单映射、账户 open-order 读取是否通
- `probe-positions`
  - 重点验证 positions API、持仓快照规范化、redeem 侧上游读路是否通

### 2.3 再看运行态是否能读出来

账户状态：

```bash
PYTHONPATH=v2/src python -m pm15min live sync-account-state --market sol --profile deep_otm --adapter direct
```

流动性状态：

```bash
PYTHONPATH=v2/src python -m pm15min live sync-liquidity-state --market sol --profile deep_otm
```

### 2.4 最后才做 dry-run

执行 dry-run:

```bash
PYTHONPATH=v2/src python -m pm15min live execute-latest --market sol --profile deep_otm --target direction --adapter direct --dry-run
```

runner dry-run:

```bash
PYTHONPATH=v2/src python -m pm15min live runner-once --market sol --profile deep_otm --target direction --adapter direct --dry-run-side-effects
```

当前最近一次 `sol + direct` operator 观测（`2026-03-20 14:28 UTC` 左右）：

- gateway readiness 已通过
- `show-ready` 当前返回：
  - `status=not_ready`
  - `primary_blocker=decision_not_accept`
  - `operator_smoke_summary.status=operational`
  - `foundation_status=ok_with_errors`
  - `foundation_issue_codes=["oracle_direct_rate_limited"]`
- `show-latest-runner --risk-only` 当前返回：
  - `decision_reject_category=confidence_threshold`
  - `decision_reject_interpretation=market_priced_through_signal`
  - `decision_reject_diagnostics.best_rejected_offset.offset=9`
  - `entry_price=0.63`
  - `entry_price_max=0.30`
  - `edge_vs_quote < 0`
  - `roi_net_vs_quote < 0`
- `show-ready.next_actions` 当前还会直接提示：
  - 等待 direct oracle rate-limit window
  - 重跑 `data run live-foundation` 或 `runner-once --dry-run-side-effects`
  - 把 `oracle_prices_table` 视为临时 fail-open fallback

这代表当前主线问题已经从“交易接入/账户接口不通”前移到了“两件并存的事”：

- 当前市场价格已经把信号打穿，本轮应 `no-trade`
- live foundation 的 direct oracle 仍有 rate-limit 降级 warning

这两件事都不等于交易接入故障。

当前最近一次 `xrp + direct` operator 观测（`2026-03-20 15:24-15:25 UTC` 左右）：

- `check-trading-gateway --probe-open-orders --probe-positions`
  - `ok=true`
  - `open_orders row_count=0`
  - `positions row_count=2003`
- `runner-once --dry-run-side-effects`
  - `decision.status=accept`
  - `execution.status=plan`
  - `order_action.status=ok`
  - `order_action.reason=dry_run`
  - `runner_health.blocking_issue_count=0`
- `show-ready`
  - `status=not_ready`
  - `primary_blocker=foundation_ok_with_errors`
  - `operator_smoke_summary.status=operational`
  - `operator_smoke_summary.reason=foundation_warning_only`
  - `foundation_issue_codes=["oracle_direct_rate_limited"]`
- `show-ready.next_actions`
  - 会直接提示：
    - 等待 direct oracle rate-limit window
    - 重跑 `data run live-foundation` 或 `runner-once --dry-run-side-effects`
    - 把 `oracle_prices_table` 视为临时 fail-open fallback

这说明当前 `xrp` 这条线的主路径已经能跑到 dry-run side effect，只是 operator 入口仍因为 foundation warning 保守保持 not-ready。

因此当前 runbook 里已经有的真实记录，至少覆盖了四类不同语义：

- `runner_missing`
  - 说明 gateway 已 ready，但还缺 canonical runner dry-run
- `strategy_reject_only`
  - 说明主路径 smoke 已通，但这一轮应 `no-trade`
- `foundation_warning_only`
  - 说明主路径 smoke 已通，但 operator 入口仍保守挂着 foundation warning
- `path_operational`
  - 说明当前没有策略拒单，主路径也已通，只是 readiness 仍会受 foundation warning 影响

---

## 3. 运行中看什么

### 3.1 看最新 runner 概览

如果你只想先知道“现在能不能开 side effects”，先跑：

```bash
PYTHONPATH=v2/src python -m pm15min live show-ready --market sol --profile deep_otm --adapter direct
```

这条命令当前会把两层信息合起来：

- gateway readiness
- runner/operator summary
- operator smoke summary

重点看：

- `status`
  - `ready` / `not_ready`
- `primary_blocker`
- `gateway_failed_probes`
- `operator_smoke_summary`
- `next_actions`
- `ready_for_side_effects`

如果 `status=not_ready`，优先按 `next_actions` 给出的顺序继续排查。

这里有一个很重要的区分：

- `ready_for_side_effects=false`
  - 只说明“这一轮现在不该直接开真实 side effects”

- `operator_smoke_summary.status=operational`
  - 说明 read-only probe + latest runner dry-run 已经证明主路径是通的
  - 即使当前 `primary_blocker=decision_not_accept`，也可能只是策略层 `no-trade`
  - 不应该再把这种情况误判成交易接入故障
- 如果同时 `foundation_status=ok_with_errors`
  - 说明当前还有 foundation warning
  - 但它已经被显式产品化成 operator 字段，不需要回到 raw log 猜问题

当前 `operator_smoke_summary` 的分流口径可以直接按下面读：

- `blocked`
  - 典型原因：
    - `gateway_checks_failed`
    - `gateway_probes_failed`
    - `runner_missing`
    - `runner_infra_blocked`
    - `runner_data_blocked`
- `operational`
  - 典型原因：
    - `strategy_reject_only`
    - `foundation_warning_only`
    - `path_operational`
  - 这类结果的共同点是：
    - 主路径 smoke 已通
    - `not_ready` 不再默认等于“系统坏了”

值班时建议固定按下面顺序读：

1. 先看 `canonical_live_scope.ok`
   - 先排除自己根本跑错 scope
2. 再看 `gateway_failed_checks` / `gateway_failed_probes`
   - 只要这里非空，先修 gateway / probe
3. 再看 `operator_smoke_summary`
   - `blocked`：
     - 继续按 infra / data 故障排
   - `operational`：
     - 主路径 smoke 已经通了
     - 不要再把当前 blocker 默认理解成系统坏了
4. 最后才看 `operator_summary.primary_blocker`
   - 这里决定当前到底是：
     - 能开真实 side effects
     - 还是本轮应 `no-trade`

把这些字段合在一起看时，建议按下面的“组合 blocker”口径理解：

- `operator_smoke_summary.status=blocked`
  - 优先把它当成主路径还没验证通过
  - 这时先修 `gateway_*` / `runner_*` / probe 问题
- `operator_smoke_summary.status=operational` 且 `primary_blocker=decision_not_accept`
  - 优先把它当成策略层或报价层的本轮 `no-trade`
  - 如果同时还有 `foundation_status=ok_with_errors`
    - 说明是“本轮不该做”与“foundation warning 仍可见”并存
    - 不是 gateway 故障
- `operator_smoke_summary.status=operational` 且 `primary_blocker=foundation_ok_with_errors` 或 `null`
  - 说明主路径 smoke 已通
  - 当前 not-ready 更接近保守的 foundation warning，而不是 side-effect 路径不通
- `operator_smoke_summary.status=operational` 且 `primary_blocker=order_action_error` / `account_state_sync_error`
  - 说明问题更接近 side-effect 后段或账户读路 follow-up
  - 不要先退回 signal / decision / gateway 假设

只看风险视角：

```bash
PYTHONPATH=v2/src python -m pm15min live show-latest-runner --market sol --profile deep_otm --risk-only
```

看完整最新 runner:

```bash
PYTHONPATH=v2/src python -m pm15min live show-latest-runner --market sol --profile deep_otm
```

这条命令重点看：

- `status`
- `canonical_live_scope`
- `latest_state_summary`
- `operator_summary`
- `next_actions`
- `risk_summary`
- `runner_health`
- `risk_alerts`
- `risk_alert_summary`
- `decision`
- `execution`

如果 `latest runner` 还不存在，这条命令也不会只返回一个 “missing”。

当前会继续给出：

- `operator_summary`
  - 当前最主要的 blocker
  - 当前能不能进入 side-effect 主线

- `next_actions`
  - 当前建议 operator 下一步先跑什么

所以在 runner 还没跑起来时，这条命令也仍然有意义。

### 3.2 `risk_summary` 怎么读

`risk_summary` 当前会显式汇总：

- `foundation`
  - 当前 live foundation 刷新状态

- `liquidity`
  - 当前流动性状态是否 blocked

- `regime`
  - 当前 regime state / pressure / reason_codes

- `decision`
  - 当前 decision 是 accept 还是 reject
  - top reject reasons 是什么

- `execution`
  - 当前 execution 是 `plan` / `blocked` / `no_action`
  - 当前 `stake_multiplier`
  - 当前 `requested_notional_usd`

- `side_effects`
  - 当前 order / account_state / cancel / redeem 这四段的状态
  - 能看出问题到底卡在“前置交易判断”还是“后段 side-effect 编排”

### 3.3 `runner_health` 怎么读

`runner_health` 是 runner 内部更结构化的一层 health/blocker 摘要。

当前至少包括：

- `overall_status`
  - `ok` / `warning` / `error`

- `pre_side_effect_status`
  - runner 到真正 side-effect 之前这段是不是已经 ready
  - 重点覆盖：
    - foundation
    - liquidity
    - decision
    - execution

- `post_side_effect_status`
  - side-effect 编排之后这段有没有错误或 warning
  - 重点覆盖：
    - order
    - account
    - cancel
    - redeem

- `primary_blocker`
  - 当前 runner 最主要的阻塞 code

- `blocker_stage`
  - 阻塞发生在哪个 stage

- `checks`
  - 每个 stage 的结构化检查结果

### 3.4 `operator_summary` 怎么读

`operator_summary` 是更适合值班/运维的一层压缩结论。

当前至少包括：

- `canonical_live_scope_ok`
  - 当前 market/profile/target 是否仍在 canonical live 范围内

- `can_run_side_effects`
  - 当前是否已经满足“可以进入真实 side-effect 主线”的最小条件

- `primary_blocker`
  - 当前最主要的阻塞原因
  - 典型值包括：
    - `latest_runner_missing`
    - `foundation_not_ok`
    - `liquidity_state_error`
    - `liquidity_blocked`
    - `decision_not_accept`
    - `execution_not_plan`
    - `order_action_error`
    - `account_state_sync_error`

- `blocker_stage`
- `runner_health_status`
- `pre_side_effect_status`
- `post_side_effect_status`
- `decision_status`
- `decision_top_reject_reasons`
- `decision_reject_category`
- `decision_reject_interpretation`
- `decision_reject_diagnostics`
- `capital_usage_summary`
- `operator_smoke_summary`
- `foundation_status`
- `foundation_reason`
- `foundation_issue_codes`
- `foundation_degraded_tasks`
- `execution_status`
- `execution_reason`
- `execution_reasons`
- `execution_block_category`
- `order_action_status`
- `order_action_reason`
- `account_state_status`
- `liquidity_blocked`
- `regime_state`
- `cancel_action_status`
- `cancel_action_reason`
- `redeem_action_status`
- `redeem_action_reason`
- `orderbook_hot_cache_status`
- `orderbook_hot_cache_reason`
- `orderbook_hot_cache_summary`
- `risk_alert_summary`

其中：

- `decision_reject_interpretation`
  - 是给 operator 的一层更直接结论
  - 例如：
    - `market_priced_through_signal`
      - 表示盘口价格已经明显高于 live entry cap，而且高于模型当前 fair value
      - 这时更接近“本轮不该做”，而不是“系统出错”

- `decision_reject_diagnostics`
  - 会压缩输出：
    - `shared_guard_reasons`
    - `dominant_guard_reasons`
    - `best_rejected_offset`
    - `rejected_offsets`
  - 重点先看：
    - `best_rejected_offset`
      - 当前最值得人工核对的一条 rejected offset 摘要
      - 里面会直接给：
        - `offset`
        - `side`
        - `confidence`
        - `entry_price`
        - `p_side`
        - `edge_vs_quote`
        - `roi_net_vs_quote`

- `capital_usage_summary`
  - 是给 operator 的一阶“可见资金占用”摘要
  - 当前会显式写出：
    - `account_overview`
      - 当前账户级可见总览：
        - `total_open_orders`
    - `total_positions`

- `orderbook_hot_cache_*`
  - 是给 quote / readiness 的热盘口状态摘要
  - 优先回答三件事：
    - 最近热缓存文件在不在
    - 里面有没有有效行
    - 最新一笔盘口是不是已经 stale
  - 重点先看：
    - `orderbook_hot_cache_status`
      - 典型值：`ok / missing / empty / stale`
    - `orderbook_hot_cache_reason`
      - 典型值：`recent_cache_missing / recent_cache_empty / recent_cache_stale`
    - `orderbook_hot_cache_summary`
      - 会带：
        - `latest_captured_ts`
        - `age_ms`
        - `row_count`
        - `market_count`
        - `token_count`
        - `provider`
        - `recent_window_minutes`
        - `redeemable_positions`
        - `visible_capital_usage_usd`
    - `portfolio`
      - 最新账户快照里可见的：
        - `visible_open_order_notional_usd`
        - `visible_position_mark_usd`
        - `visible_position_cash_pnl_usd`
        - `visible_capital_usage_usd`
    - `focus_market`
      - 当前 selected market 或 best rejected market 对应的：
        - `open_orders_count`
        - `open_orders_notional_usd`
        - `positions_count`
        - `positions_current_value_usd`
        - `active_trade_count`
    - `execution_budget`
      - 当前这轮 execution 打算申请的 notional：
        - `stake_base_usd`
        - `stake_multiplier`
        - `requested_notional_usd`
        - `requested_vs_max_notional_ratio`
    - `regime_context`
      - 当前 regime 对这轮资金和 trade slot 的约束：
        - `state`
        - `pressure`
        - `defense_max_trades_per_market`
        - `current_market_trade_slots_remaining`

  - 这层摘要故意只叫“visible capital usage”
    - 它只使用：
      - open-order notional
      - positions current value
      - positions cash pnl
    - 它不代表：
      - 完整 cash balance
      - 完整账户 equity
      - 完整跨 venue 资金视图
  - 所以当前文档里说“更完整账户总览”时，指的是：
    - 如果后续确有需要，再补 cash/equity 级别 account-wide view
    - 而不是把现在的 `visible_*` 直接改写成完整账户权益
  - 当前值班如果只是为了判断 blocker / regime / focus market
    - 现有 `account_overview + portfolio + focus_market + execution_budget + regime_context` 已经是主要入口

- `operator_smoke_summary`
  - 是给 operator 的更窄 smoke 结论
  - 当前会显式写出：
    - `status`
      - `operational` / `blocked`
    - `reason`
      - 例如：
        - `strategy_reject_only`
        - `gateway_checks_failed`
        - `gateway_probes_failed`
        - `runner_infra_blocked`
    - `gateway_check_failures`
    - `gateway_probe_failures`
    - `runner_smoke_status`
    - `runner_primary_blocker`

  - 其中最关键的解释是：
    - 如果：
      - `ready_for_side_effects=false`
      - 但 `operator_smoke_summary.status=operational`
    - 那当前更接近：
      - “系统通了，但本轮不该做”
    - 而不是：
      - “side-effect 主路径还没打通”

- `foundation_reason` / `foundation_issue_codes` / `foundation_degraded_tasks`
  - 是给 operator 的 foundation 降级原因链
  - 当前如果 `live foundation` 某个子任务是 `fail_open degraded`
    - 不需要再翻 raw foundation log
    - 直接在 `show-latest-runner` / `show-ready` 里就能看到：
      - 哪个 task 降级了
      - issue code 是什么
      - 原始错误文本是什么
  - 当前真实主线已经验证过一类：
    - `foundation_issue_codes=["oracle_direct_rate_limited"]`
    - `foundation_reason` 会直接带出 `/api/crypto/crypto-price ... Too Many Requests`
  - 实际值班时可以直接把它当成分流器：
    - `blocked`
      - 回到 gateway / data / runner infra
    - `operational`
      - 直接进入 strategy / execution / regime 解释层

- `order_action_reason` / `account_state_status` / `cancel_action_reason` / `redeem_action_reason`
  - 是给 side-effect 后段分流用的
  - 现在不需要再只盯 `risk_alerts`
  - 直接在 `operator_summary` 就能看到：
    - 下单失败是不是 request / auth / place-order 问题
    - account refresh 是 open-orders 读路问题还是 positions 读路问题
    - cancel / redeem 是 candidate 选择问题还是 gateway / relay submit 问题

- `execution_block_category`
  - 是给 `execution_not_plan` 进一步压缩的一层结论
  - 当前至少会区分：
    - `orderbook_depth`
      - 例如：
        - `depth_fill_ratio_below_threshold`
        - `depth_fill_unavailable`
        - `depth_snapshot_missing`
    - `repriced_quote_threshold`
      - 例如：
        - `repriced_entry_price_max`
        - `repriced_net_edge_below_threshold`
        - `repriced_roi_below_threshold`
    - `execution_inputs_missing`
    - `regime_budget_blocked`
    - `decision_reject`

### 3.5 `next_actions` 怎么读

`next_actions` 是 operator 的下一步建议动作列表。

当前已经产品化到 operator 输出里的主要分流包括：

- `gateway_checks_failed` / `gateway_probes_failed`
- `foundation_not_ok`
- `foundation_ok_with_errors + oracle_direct_rate_limited`
- `decision_not_accept + quote_inputs_missing`
- `decision_not_accept + confidence_threshold`
- `decision_not_accept + entry_or_quote_threshold`
- `decision_not_accept + regime_trade_count_cap`
- `execution_not_plan + depth_*`
- `execution_not_plan + repriced_*`
- `order_action_error`
- `account_state_sync_error`
- `cancel_action_error` / `cancel_action ok_with_errors`
- `redeem_action_error` / `redeem_action ok_with_errors`

当前逻辑会按 blocker 给出不同建议：

- 如果是 `latest_runner_missing`
  - 先跑 `check-trading-gateway`
  - 再跑 `runner-once --dry-run-side-effects`

- 如果是 `foundation_not_ok`
  - 先看最新 foundation summary
  - 再补跑 `data run live-foundation`

- 如果不是 `foundation_not_ok`，只是 `foundation_ok_with_errors`
  - 先看 `risk_summary.foundation`
  - 再看 `data show-summary --surface live` 的 `completeness / issues`
  - 如果 `foundation_issue_codes` 里有 `oracle_direct_rate_limited`
    - 先等 direct oracle rate-limit window
    - 再跑 `data run live-foundation` 或 `runner-once --dry-run-side-effects`
    - 同时把 `oracle_prices_table` 视为临时 fail-open fallback，而不是整条 live path 故障
  - 如果同时 `operator_smoke_summary.status=operational`
    - 优先把它视为“数据面有 warning，但主路径 smoke 已通”
    - 不要直接把它升级成整条 live path 故障

- 如果是 `liquidity_state_error`
  - 先看最新 liquidity state payload
  - 再跑 `sync-liquidity-state`

- 如果是 `liquidity_blocked`
  - 先跑 `sync-liquidity-state`
  - 再看最新 liquidity snapshot

- 如果是 `decision_not_accept`
  - 先看最新 decision snapshot 和 reject reasons
  - 如果 `decision_reject_category=quote_inputs_missing`
    - 先看最新 quote snapshot
    - 再看 `operator_summary.orderbook_hot_cache_status / reason / summary`
    - 再看 `orderbook_index` / orderbook source 是否覆盖到被拒绝市场
    - 再补跑 `data run live-foundation` 或 `data record orderbooks`
  - 如果 `decision_reject_category=confidence_threshold`
    - 先看 `operator_summary.decision_reject_diagnostics.best_rejected_offset`
    - 如果 `decision_reject_interpretation=market_priced_through_signal`
      - 优先把它视为本轮 `no-trade`
      - 不要把它当成 gateway / quote 缺失问题去排查
  - 如果 `decision_reject_category=entry_or_quote_threshold`
    - 先看 `operator_summary.decision_reject_diagnostics.best_rejected_offset`
    - 如果 `decision_reject_interpretation=market_priced_through_signal`
      - 优先把它视为本轮 `no-trade`
      - 不要把它当成 gateway / quote 缺失问题去排查
      - 只有在你明确要改变 live 风险约束时，才去改 `entry_price_max` / edge / ROI 阈值
  - 如果 `decision_reject_category=regime_guard`
    - 先看 `capital_usage_summary.focus_market`
    - 再看 `capital_usage_summary.regime_context`
    - 如果 focus market 已有挂单/持仓
      - 优先把它视为 regime 风控在生效
      - 不要先怀疑 signal/quote 本身

- 如果是 `execution_not_plan`
  - 如果 `execution_block_category=orderbook_depth`
    - 先看最新 execution `depth_plan`
    - 再看 orderbook recorder / latest depth coverage
  - 如果 `execution_block_category=repriced_quote_threshold`
    - 先看最新 execution `repriced_metrics`
    - 再看 repriced entry / edge / ROI 为什么越过 live profile 阈值
  - 如果 `execution_block_category=execution_inputs_missing`
    - 先看 selected decision row / quote row / execution snapshot 缺了哪个输入
  - 如果 `execution_block_category=regime_budget_blocked`
    - 先看 `capital_usage_summary.execution_budget` 和 `regime_context`
  - 否则
    - 先看最新 execution snapshot 的 blocked/no-action 原因

值班最容易误判的 3 种情况：

1. `status=not_ready` 就等于系统坏了
   - 不对
   - 如果 `operator_smoke_summary.status=operational`，更可能只是本轮不该做

2. `decision_not_accept` 就先回头查 gateway
   - 不对
   - 先看 `decision_reject_category`
   - 只有 `quote_inputs_missing` 才优先回到 data / quote 覆盖

3. `execution_not_plan` 就直接改 profile 阈值
   - 不对
   - 先看 `execution_block_category`
   - 区分：
     - `orderbook_depth`
     - `repriced_quote_threshold`
     - `execution_inputs_missing`
     - `decision_reject`

4. `account_state_sync_error` / `cancel_action_error` / `redeem_action_error` 就先怀疑 decision 语义
   - 不对
   - 这三类更接近 side-effect 后段或账户读写路径问题
   - 应先回到对应 payload 和最新 state snapshot

5. `gateway_checks_failed` 和 `gateway_probes_failed` 反正都算 gateway 问题
   - 不对
   - `gateway_checks_failed`
     - 更接近 auth / dependency / capability / adapter build 问题
   - `gateway_probes_failed`
     - 更接近 open-orders / positions 真实读路问题
   - 处理顺序也不同：
     - 前者先修配置和依赖
     - 后者先回到 `--probe-open-orders` / `--probe-positions`

- 如果是 `order_action_error`
  - 先看最新 runner `order_action` payload
  - 再看 gateway / account / order request 细节
  - 如果同时 `account_state_status=error`
    - 说明 submit 失败之外，post-submit account refresh 也有问题
    - `next_actions` 现在会继续追加 account read-path 的 follow-up，而不是只停在下单失败
  - 如果同时 `cancel_action_status` / `redeem_action_status` 还是 warning
    - `next_actions` 也会继续追加对应 reconciliation 动作

- 如果是 `account_state_sync_error`
  - 先看最新 runner `account_state` payload
  - 再单独跑 `sync-account-state`
  - 如果 `account_open_orders_status=error`
    - 再跑 `check-trading-gateway --probe-open-orders`
  - 如果 `account_positions_status=error`
    - 再跑 `check-trading-gateway --probe-positions`
  - 如果只是 post-submit account refresh 失败
    - 不要先怀疑 signal / decision / execution
    - 先把它当成 account read-path 问题处理

- 如果是 `cancel_action_error`
  - 先看最新 `cancel_action` payload
  - 再看最新 `open_orders` snapshot
  - 再看 `operator_summary.cancel_action_reason`
  - 如果 `cancel_action_status=ok_with_errors`
    - 优先把它视为 follow-up reconciliation
    - 先确认还有哪些 `order_id` 仍处于 open 状态
  - 最后再看是不是 gateway cancel contract 或 open-order 映射有问题

- 如果是 `redeem_action_error`
  - 先看最新 `redeem_action` payload
  - 再看最新 `positions` snapshot 和 `redeemable_conditions`
  - 再看 `operator_summary.redeem_action_reason`
  - 如果 `redeem_action_status=ok_with_errors`
    - 优先把它视为 follow-up reconciliation
    - 先确认还剩哪些 condition 仍可赎回
  - 最后再看 redeem relay / builder / RPC 配置

- 如果 `primary_blocker=null`，但 `cancel_action_status` / `redeem_action_status` 仍是 warning
  - `next_actions` 现在也会继续给出 follow-up 动作
  - 这类情况不要把它误判成“当前 cycle 可以完全不用看 side-effect 后段”

### 3.6 `risk_alerts` 怎么读

`risk_alerts` 是给 operator 看的压缩告警层，当前至少会产出：

- `foundation_not_ok`
  - 严重级别：`critical`

- `liquidity_state_error`
  - 严重级别：`critical`

- `liquidity_blocked`
  - 严重级别：`critical`

- `regime_defense`
  - 警告级别：`warning`

- `decision_reject`
  - 警告级别：`warning`

- `execution_blocked` / `execution_no_action`
  - 警告级别：`warning`

- `order_action_error`
  - 严重级别：`critical`

- `account_state_sync_error`
  - 严重级别：`critical`

- `cancel_action_error` / `redeem_action_error`
  - 警告级别：`warning`

- `side_effects_dry_run`
  - 信息级别：`info`

配套的 `risk_alert_summary` 会给：

- 各严重级别数量
- `highest_severity`
- `has_critical`

---

## 4. 关键路径

### 4.1 runner

最新 runner summary:

```text
v2/var/live/state/runner/cycle=15m/asset=<asset>/profile=<profile>/target=<target>/latest.json
```

runner history snapshot:

```text
v2/var/live/state/runner/cycle=15m/asset=<asset>/profile=<profile>/target=<target>/snapshots/snapshot_ts=<ts>/run.json
```

runner log:

```text
v2/var/live/logs/runner/cycle=15m/asset=<asset>/profile=<profile>/target=<target>/runner.jsonl
```

### 4.2 其他最新状态

decision:

```text
v2/var/live/state/decisions/cycle=15m/asset=<asset>/profile=<profile>/target=<target>/latest.json
```

execution:

```text
v2/var/live/state/execution/cycle=15m/asset=<asset>/profile=<profile>/target=<target>/latest.json
```

orders:

```text
v2/var/live/state/orders/cycle=15m/asset=<asset>/profile=<profile>/target=<target>/latest.json
```

account snapshots:

```text
v2/var/live/state/open_orders/asset=<asset>/latest.json
v2/var/live/state/positions/asset=<asset>/latest.json
```

liquidity:

```text
v2/var/live/state/liquidity/cycle=15m/asset=<asset>/profile=<profile>/latest.json
```

regime:

```text
v2/var/live/state/regime/cycle=15m/asset=<asset>/profile=<profile>/latest.json
```

---

## 5. 当前结论

截至当前实现：

- Phase A 的交易 adapter 主路径已经可用
- direct / legacy 都已做过本机只读 smoke validation
- direct 还已有 `sol` / `xrp` 的真实 `runner-once --dry-run-side-effects` 记录
- 当前 runbook 已和 checklist / roadmap / Phase A 状态说明对齐
- Phase B 已经开始落地：
  - `DEFENSE trade-count cap`
  - `regime stake scale`
  - runner `risk_summary`
  - runner `risk_alerts`
  - `show-latest-runner`

但这还不等于 live 收口完全完成。

当前还应继续推进：

- 把已实现的 blocker 分流继续压成更短的 runbook 小节
- 有条件时继续补更多真实 dry-run / adapter validation 记录
- 如确有需要，再补 cash/equity 级别的 account-wide capital view

当前不需要优先回去做的事：

- 再做一轮 live CLI 边界大改
- 再把 `show-config` / `show-layout` 重新当成 operator 入口
