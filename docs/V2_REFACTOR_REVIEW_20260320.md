# V2 重构评审与修复记录（2026-03-22 更新）

## 范围

这份文档同时承担两件事：

1. 对照旧结构和 `v2/` 当前结构，判断这次重构到底重构到了什么程度。
2. 记录已经确认的风险、已经落地的修复，以及下一轮继续拆分的优先级。

本次观察的主线范围：

- 旧版：
  - `src/`
  - `live_trading/`
  - `apps/`
- 重构版：
  - `v2/src/pm15min/`
  - `v2/tests/`
  - `v2/docs/`

---

## 当前验证结果

本地在 `2026-03-22` 的验证结果：

```bash
PYTHONPATH=v2/src pytest -q v2/tests
```

结果：

- `172 passed, 1 warning`

额外回归（本轮）：

- `PYTHONPATH=v2/src pytest -q v2/tests/test_live_service.py v2/tests/test_cli.py`

结论很明确：

- `v2` 不只是“目录整理过了”
- 当前主线是可运行、可验证、可继续演进的
- 最近几轮 live 域拆分没有把测试面打坏

---

## 一句话判断

这次重构整体是成功的。

更准确地说：

- canonical 主线已经建立起来了
- research / data / layout 的语义已经明显比旧版干净
- live 域的复杂度收口已经进入后半段
- 但还没有到“完全轻薄”的程度，剩余工作主要是继续削大文件，而不是推翻结构

如果只给当前状态一个工程判断：

- 架构方向：`8.5/10`
- 代码清晰度：`8/10`
- live 域完成度：`主线已稳定，复杂度仍需继续下沉`

补一句更现实的判断：

- 现在最大的未收口复杂度已经不只在 `live`
- 从全仓看，`data` 和 `layout` 也开始进入下一轮值得收口的范围

---

## 当前全仓热点

如果把视野放到整个 `v2/src/pm15min/`，并且只看这轮更关心的“结构性热点”，当前更值得注意的是：

| 文件 | 当前行数 | 判断 |
| --- | --- | --- |
| `live/layout.py` | `438` | layout contract 仍然稳定，method factory 已继续减重，但 public path surface 仍偏大 |
| `research/layout.py` | `360` | research path builder 已继续减重，helper 已下沉，主类仍相对稳定 |
| `live/operator_actions.py` | `334` | operator follow-up 已拆层，但 facade/orchestrator 仍承载较多组合逻辑 |
| `data/cli_handlers.py` | `333` | data CLI 主分发已独立，后续如有需要可继续按 command family 压薄 |
| `live/service.py` | `322` | facade 已明显变薄，但 public surface 仍然大，后续仍可继续收口 |
| `data/layout.py` | `306` | data path builder 已明显减重，当前剩余复杂度主要在 path surface 本身 |
| `live/service_facade_helpers.py` | `282` | facade helper / alias / wiring bridge 已独立，但仍偏密 |
| `live/action_builders.py` | `230` | action payload builders 已独立，后续如有需要还可继续压薄 |
| `live/quote_row_builder.py` | `141` | quote 行级组装已独立，但细节判断仍然密集 |

这意味着：

- `live` 这轮已经明显收口
- `live/signal_scoring.py` 已经从主热点退出，主文件现在只剩 `126` 行 facade
- `data/service.py`、`data/cli.py`、`research/cli.py` 这轮已经真正收口，不再是最先要救的热点
- 如果后面继续优化“整个仓库”的优雅度，当前更值得继续看的会是 `live/layout.py`、`live/operator_actions.py`、`live/service.py`

---

## 已落地修复

| 项目 | 状态 | 处理 |
| --- | --- | --- |
| live state 非原子写 | 已修复 | 新增 `live/persistence.py`，统一改成 snapshot 优先 + latest 原子写 |
| `live/service.py` 过重 | 已继续收口 | 已拆到 `runtime.py`、`readiness.py`、`gateway_checks.py`、`runner_api.py`、`signal_service.py`、`signal_utils.py`、`operation_service.py`、`service_wiring.py`、`service_facade_helpers.py`，`service.py` 更接近纯 facade |
| `service` / `runner` 入口缠绕 | 已修复 | CLI 通过 `live/runner_api.py` 进入 runner，`service.py` 不再拥有 runner 启动入口 |
| trading adapter 重复逻辑 | 已修复 | 新增 `live/trading/normalize.py`、`live/trading/positions_api.py`，direct/legacy 共用归一化和 positions 拉取 |
| research phase1 残留 helper | 已修复 | 清理 `research/service.py` 和 `research/cli.py` 中已废弃 skeleton / import |
| `live/actions.py` 过重 | 已修复 | 先拆到 `action_service.py`、`action_utils.py`，本轮继续下沉到 `cancel_service.py`、`redeem_service.py`、`order_submit_service.py`、`action_builders.py`、`action_gate.py`、`action_persistence.py` |
| `live/execution.py` 过重 | 已修复 | 拆到 `execution_utils.py`、`execution_depth.py`、`execution_policy.py`、`execution_service.py`，`execution.py` 保留 facade |
| `live/runner.py` 过重 | 已修复 | 拆到 `runner_service.py`、`runner_diagnostics.py`、`runner_utils.py`，`runner.py` 保留 monkeypatch 友好 facade |
| runner 循环异常时可能无限重试 | 已修复 | `runner_service.py` 改成按尝试次数收敛，避免 `loop=True` 下异常卡死 |
| readiness 聚合逻辑过于集中 | 已显著收口 | 新增 `readiness_state.py` 和 `operator_summary.py`，`readiness.py` 保留展示入口 |
| `live/liquidity.py` 过重 | 已修复 | 拆到 `liquidity_fetch.py`、`liquidity_policy.py`、`liquidity_state.py`，`liquidity.py` 保留 facade 和测试 patch 点 |
| `live/liquidity_policy.py` 规则层过重 | 已修复 | 拆到 `liquidity_policy_thresholds.py`、`liquidity_policy_raw.py`、`liquidity_policy_temporal.py`，`liquidity_policy.py` 保留 facade |
| `live/cli.py` 分发过重 | 已修复 | 先改成参数 helper + handler registry + command dispatch，本轮继续拆到 `cli_common.py`、`cli_parser.py`，`cli.py` 保留 runtime facade |
| `live/profiles.py` 配置聚合过重 | 已修复 | 拆到 `profile_spec.py`、`profile_catalog.py`，`profiles.py` 保留 resolver/public facade |
| `live/account.py` 状态聚合过重 | 已修复 | 拆到 `account_state.py`、`account_summary.py`、`account_persistence.py`，`account.py` 保留 facade |
| `live/operator_followups.py` 规则聚合过重 | 已修复 | 拆到 `operator_actions.py`、`operator_categories.py`、`operator_rejects.py`，`operator_followups.py` 保留 facade |
| `live/operator_actions.py` follow-up 规则过重 | 已修复 | 拆到 `operator_action_followups_blockers.py`、`operator_action_followups_side_effects.py`，`operator_actions.py` 保留 facade/orchestrator |
| `live/gateway_checks.py` 探测聚合过重 | 已修复 | 拆到 `gateway_service.py`、`gateway_capabilities.py`、`gateway_probes.py`，`gateway_checks.py` 保留 facade |
| `live/capital_usage.py` 解释聚合过重 | 已修复 | 拆到 `capital_usage_service.py`、`capital_usage_context.py`、`capital_usage_overview.py`，`capital_usage.py` 保留 facade |
| `live/regime.py` 控制聚合过重 | 已修复 | 拆到 `regime_controller.py`、`regime_state.py`、`regime_persistence.py`，`regime.py` 保留 facade |
| `live/execution_policy.py` 策略聚合过重 | 已修复 | 拆到 `execution_policy_helpers.py`、`execution_retry_policy.py`、`execution_order_policy.py`，`execution_policy.py` 保留 facade |
| `live/guards.py` 规则聚合过重 | 已修复 | 拆到 `guard_quote.py`、`guard_regime.py`、`guard_features.py`，`guards.py` 保留统一入口 |
| `live/quotes.py` 桥接聚合过重 | 已修复 | 拆到 `quote_market.py`、`quote_orderbook.py`、`quote_service.py`，`quotes.py` 保留 facade |
| `live/quote_service.py` snapshot / row / persistence 混合 | 已修复 | 拆到 `quote_snapshot_builder.py`、`quote_row_builder.py`、`quote_snapshot_persistence.py`，`quote_service.py` 保留 facade |
| `live/signal_service.py` 编排过重 | 已修复 | 拆出 `signal_scoring.py`，并继续收口到 `signal_scoring_bundle.py` / `signal_scoring_offsets.py`，`signal_service.py` 保留 check/decide/quote 编排入口 |
| `live/signal_scoring.py` 单段流程过长 | 已修复 | 拆到 `signal_scoring_bundle.py`、`signal_scoring_offsets.py`，`signal_scoring.py` 保留 `score_live_latest` facade 和 payload 组装 |
| `live/runner_service.py` 编排过重 | 已修复 | 拆到 `runner_iteration.py`、`runner_runtime.py`，`runner_service.py` 保留兼容出口 |
| `data/service.py` 过重 | 已修复 | 拆到 `summary_datasets.py`、`summary_audit.py`、`summary_reporting.py`、`summary_shared.py`，`service.py` 保留 facade/orchestration |
| `data/cli.py` 过重 | 已修复 | 拆到 `cli_args.py`、`cli_parser.py`、`cli_handlers.py`，`cli.py` 保留 facade 和 monkeypatch 兼容 deps 组装 |
| `data/summary_audit.py` 规则聚合过重 | 已修复 | 继续拆到 `summary_audit_rules.py`、`summary_audit_dataset_checks.py`、`summary_audit_alignment.py`，`summary_audit.py` 保留 orchestrator 和兼容导出 |
| `data/layout.py` helper 与 path builder 混合 | 已继续收口 | 先拆出 `layout_helpers.py`，本轮继续拆出 `layout_paths.py`，`layout.py` 保留 `DataLayout` / `MarketDataLayout` 和 public re-export |
| `research/layout.py` helper 与 path builder 混合 | 已修复 | 拆出 `layout_helpers.py`，`layout.py` 保留 `ResearchLayout` / `MarketResearchLayout` 和 public re-export |
| `research/cli.py` 过重 | 已修复 | 拆到 `cli_args.py`、`cli_parser.py`、`cli_handlers.py`，`cli.py` 保留 facade 和兼容导出 |
| `research/contracts.py` 对象族聚合过重 | 已修复 | 拆到 `_contracts_frames.py`、`_contracts_training.py`、`_contracts_runs.py`，`contracts.py` 保留 facade |
| `live/layout.py` 路径拼接重复 | 已继续收口 | 先拆出 `layout_paths.py`，本轮继续引入 `layout_state_specs.py` 和 scoped private helpers，`layout.py` 保留 `LiveStateLayout` public methods 和路径 contract |
| `live/runner_diagnostics.py` 解释层聚合过重 | 已修复 | 拆到 `runner_diagnostics_risk.py`、`runner_diagnostics_health.py`，`runner_diagnostics.py` 保留 facade |
| `live/service.py` facade helper 仍混杂 | 已继续收口 | facade helper 已继续下沉到 `service_facade_helpers.py`，公共 patch 点和导入路径保留；但 public surface 仍然较大 |
| `live/operator_actions.py` 组合逻辑仍偏密 | 已继续收口 | 已引入 `_OperatorActionContext` 和 primary/secondary helper，外部函数面保持不变 |
| docs 状态文档口径不完全一致 | 已修复 | `README`、checklist、roadmap、Phase A 方案、operator runbook 已统一到当前结构、测试状态和 blocker 语义 |

---

## 这次重构里做对的地方

### 1. canonical 入口已经明确

- `apps/` 退化成 shim，而不是继续和新主线并列
- 新主线收敛到 `PYTHONPATH=v2/src python -m pm15min ...`
- 这件事有测试守着，不只是口头约定

### 2. legacy 边界是显式的

- 新代码没有继续无边界地回流到 `live_trading`
- legacy 依赖收敛到明确适配层，而不是渗透到各域逻辑里

### 3. research 域已经明显比旧版清楚

- labels / datasets / training / inference 的职责比旧版更稳定
- 这部分已经接近“以后继续做功能也不会马上烂掉”的状态

### 4. layout 语义是对的

- rewrite runtime path 和 legacy reference path 已经分开
- 这不只是改目录，而是在改路径职责

### 5. live 域已经从“大一坨”变成“兼容 facade + focused modules”

最近几轮拆分后的结果很清楚：

- `service.py` 约 `370` 行
- `actions.py` 约 `73` 行
- `action_service.py` 约 `73` 行
- `action_utils.py` 约 `34` 行
- `account.py` 约 `18` 行
- `capital_usage.py` 约 `3` 行
- `cli.py` 约 `224` 行
- `execution.py` 约 `54` 行
- `execution_policy.py` 约 `51` 行
- `gateway_checks.py` 约 `49` 行
- `guards.py` 约 `99` 行
- `operator_followups.py` 约 `21` 行
- `profiles.py` 约 `9` 行
- `quotes.py` 约 `30` 行
- `regime.py` 约 `33` 行
- `runner.py` 约 `136` 行
- `readiness.py` 约 `181` 行
- `liquidity.py` 约 `181` 行
- `signal_service.py` 约 `134` 行

也就是说：

- 老问题不是完全消失了
- 但主流程协调器已经不再集中在单一超大文件里

---

## 本轮关键修复记录

### R1. runner 拆分已稳定

本轮继续拆分并稳定了 runner 这条线：

- `v2/src/pm15min/live/runner.py`
- `v2/src/pm15min/live/runner_service.py`
- `v2/src/pm15min/live/runner_diagnostics.py`
- `v2/src/pm15min/live/runner_utils.py`

处理要点：

- 保留 `runner.py` 作为兼容 facade
- 继续保留 tests 依赖的 monkeypatch 路径
- 把纯编排、风险汇总、错误 payload 组装拆开

顺手修掉了一个真实缺陷：

- `runner_diagnostics.py` 里局部变量遮蔽了 `account_state_status(...)`
- 在 `loop=True` 场景下，这个异常会被 runner 吞掉并不断重试
- 现在 diagnostics 已修正，runner 循环也改成按尝试次数终止，不再存在这种卡死模式

### R2. readiness 已经从“聚合大文件”继续收口

这轮新增：

- `v2/src/pm15min/live/readiness_state.py`
- `v2/src/pm15min/live/operator_summary.py`

现在职责更清楚：

- `readiness.py`：展示入口，负责 `show_live_latest_runner` / `show_live_ready`
- `readiness_state.py`：latest state 路径和 state summary
- `operator_summary.py`：operator summary 聚合和补充上下文动作

这类拆分是值得继续复制的模式：

- 保留稳定入口
- 下沉聚合细节
- 不破坏 CLI 和已有测试合同

### R3. liquidity 已经从“单文件混合实现”拆成四层

这轮新增：

- `v2/src/pm15min/live/liquidity_fetch.py`
- `v2/src/pm15min/live/liquidity_policy.py`
- `v2/src/pm15min/live/liquidity_state.py`

现在职责更清楚：

- `liquidity.py`：兼容 facade，继续保留 `utc_snapshot_label` 和 `_fetch_*` patch 点
- `liquidity_fetch.py`：行情抓取、HTTP fallback、基础数值工具
- `liquidity_policy.py`：阈值、原始评估、时间滤波
- `liquidity_state.py`：snapshot 构建、load / persist / summarize

这次拆分最关键的点不是单纯“文件变小”，而是：

- 抓取层和规则层终于分开了
- state 持久化不再和评估细节混在一起
- 测试合同没有变化，`test_live_liquidity.py` 和上层 runner / service / cli 都继续通过

### R4. action side-effect 层已经按职责拆开

这轮新增：

- `v2/src/pm15min/live/cancel_service.py`
- `v2/src/pm15min/live/redeem_service.py`
- `v2/src/pm15min/live/order_submit_service.py`
- `v2/src/pm15min/live/action_builders.py`
- `v2/src/pm15min/live/action_gate.py`
- `v2/src/pm15min/live/action_persistence.py`

同时收口：

- `action_service.py` 现在是兼容 facade
- `action_utils.py` 现在是兼容 re-export 层

现在职责更清楚：

- `cancel_service.py`：cancel policy 主流程
- `redeem_service.py`：redeem policy 主流程
- `order_submit_service.py`：execution submit 主流程
- `action_builders.py`：signature / request / candidate / type convert helper
- `action_gate.py`：retry gate、attempt context、节流判断
- `action_persistence.py`：order / cancel / redeem 写盘

这次拆分的收益很直接：

- submit / cancel / redeem 三条 side effect 线终于不再混在一个文件
- gate 与 persistence 从业务动作里抽出来了
- `actions.py` 和 `test_live_actions.py` 的合同没有变化，上层 `runner / service / cli` 也继续通过

### R5. CLI 已从长分支分发改成 registry 结构

这轮处理的是：

- `v2/src/pm15min/live/cli.py`

现在结构更清楚：

- 参数添加逻辑收成了一组小 helper
- 各子命令变成独立 handler
- `run_live_command(...)` 只负责通过 handler registry 分发并打印 payload

这次调整的重点不是“继续拆文件”，而是先把命令入口的控制流理顺：

- 去掉了大段 `if/elif`
- 保留了 `pm15min.live.cli.*` 这些测试 monkeypatch 点
- `test_cli.py` 和全量测试都继续通过

### R6. profiles 已拆成 spec + catalog + facade

这轮新增：

- `v2/src/pm15min/live/profile_spec.py`
- `v2/src/pm15min/live/profile_catalog.py`

同时收口：

- `profiles.py` 现在只保留 `LiveProfileSpec` re-export 和 `resolve_live_profile_spec(...)`

现在职责更清楚：

- `profile_spec.py`：profile 数据结构和 accessor method
- `profile_catalog.py`：预定义 live profile catalog
- `profiles.py`：public facade / resolver

这次拆分的价值在于：

- profile 结构和 profile 数据终于分开了
- 外部 import 合同几乎没变
- `test_live_execution.py`、`test_live_guards.py`、`test_cli.py` 和全量测试都继续通过

### R7. account 已拆成 state + summary + persistence + facade

这轮新增：

- `v2/src/pm15min/live/account_state.py`
- `v2/src/pm15min/live/account_summary.py`
- `v2/src/pm15min/live/account_persistence.py`

同时收口：

- `account.py` 现在只保留 public facade

现在职责更清楚：

- `account_state.py`：open orders / positions snapshot 构建
- `account_summary.py`：account、orders、positions 的汇总逻辑和 redeem plan
- `account_persistence.py`：latest/snapshot 的读写
- `account.py`：public facade

这次拆分的价值在于：

- snapshot 构建和汇总终于分开了
- 未使用的 normalize helper 被清掉了
- `test_live_account.py`、`test_live_actions.py`、`test_live_runner.py` 和全量测试都继续通过

### R8. operator_followups 已拆成 actions + categories + rejects

这轮新增：

- `v2/src/pm15min/live/operator_actions.py`
- `v2/src/pm15min/live/operator_categories.py`
- `v2/src/pm15min/live/operator_rejects.py`

同时收口：

- `operator_followups.py` 现在只保留 facade 和 public export

现在职责更清楚：

- `operator_actions.py`：follow-up action 生成
- `operator_categories.py`：decision / execution block 分类
- `operator_rejects.py`：decision reject diagnostics 和 interpretation
- `operator_followups.py`：public facade

这次拆分的价值在于：

- 解释层规则不再全堆在一个文件
- operator_summary / readiness 依赖的 public API 没有变化
- `test_live_service.py`、`test_cli.py` 和全量测试都继续通过

### R9. gateway_checks 已拆成 service + capabilities + probes + facade

这轮新增：

- `v2/src/pm15min/live/gateway_service.py`
- `v2/src/pm15min/live/gateway_capabilities.py`
- `v2/src/pm15min/live/gateway_probes.py`

同时收口：

- `gateway_checks.py` 现在只保留 public facade 和依赖注入

现在职责更清楚：

- `gateway_service.py`：check payload 主编排
- `gateway_capabilities.py`：capability requirement 计算
- `gateway_probes.py`：probe 执行、smoke command、module availability
- `gateway_checks.py`：public facade

这次拆分的价值在于：

- capability / probe / summary 不再混在一个文件
- `service.py` 原有的依赖注入方式保持不变
- `test_live_service.py`、`test_cli.py` 和全量测试都继续通过

### R10. capital_usage 已拆成 service + context + overview + facade

这轮新增：

- `v2/src/pm15min/live/capital_usage_service.py`
- `v2/src/pm15min/live/capital_usage_context.py`
- `v2/src/pm15min/live/capital_usage_overview.py`

同时收口：

- `capital_usage.py` 现在只保留 public facade

现在职责更清楚：

- `capital_usage_service.py`：capital usage summary 主组装
- `capital_usage_context.py`：execution / regime / focus-market context 解析
- `capital_usage_overview.py`：account overview 和 fallback summary
- `capital_usage.py`：public facade

这次拆分的价值在于：

- 解释层的上下文解析和 overview 组装终于分开了
- `operator_summary.py` 继续只依赖稳定入口
- `test_live_service.py`、`test_cli.py` 和全量测试都继续通过

### R11. regime 已拆成 controller + state + persistence + facade

这轮新增：

- `v2/src/pm15min/live/regime_controller.py`
- `v2/src/pm15min/live/regime_state.py`
- `v2/src/pm15min/live/regime_persistence.py`

同时收口：

- `regime.py` 现在只保留 facade 和 public export

现在职责更清楚：

- `regime_controller.py`：状态机/controller 本体和基础 helper
- `regime_state.py`：regime snapshot 构建
- `regime_persistence.py`：load / persist / summarize
- `regime.py`：public facade

这次拆分的价值在于：

- control layer 的状态机和持久化终于分开了
- `utc_snapshot_label` 的测试 patch 点仍保留在 facade
- `test_live_regime.py`、`test_live_service.py`、`test_cli.py` 和全量测试都继续通过

### R12. execution_policy 已拆成 helpers + retry + order-policy + facade

这轮新增：

- `v2/src/pm15min/live/execution_policy_helpers.py`
- `v2/src/pm15min/live/execution_retry_policy.py`
- `v2/src/pm15min/live/execution_order_policy.py`

同时收口：

- `execution_policy.py` 现在只保留 execution record 组装和 public facade

现在职责更清楚：

- `execution_policy_helpers.py`：policy context、policy state、repriced guard、stake multiplier
- `execution_retry_policy.py`：retry policy 相关规则
- `execution_order_policy.py`：cancel / redeem policy
- `execution_policy.py`：public facade

这次拆分的价值在于：

- retry/cancel/redeem 三类 execution policy 不再混在一个文件
- execution 主流程仍然只依赖稳定入口
- `test_live_execution.py`、`test_live_service.py` 和全量测试都继续通过

### R13. CLI 已进一步拆成 common + parser + runtime facade

这轮新增：

- `v2/src/pm15min/live/cli_common.py`
- `v2/src/pm15min/live/cli_parser.py`

同时收口：

- `cli.py` 现在主要保留 handler runtime facade 和 public export

现在职责更清楚：

- `cli_common.py`：canonical target/helper/config builder
- `cli_parser.py`：subcommand parser 构建
- `cli.py`：runtime handler 和 public facade

这次拆分的价值在于：

- parser 构建和命令执行终于分开了
- `pm15min.live.cli.*` 的 monkeypatch 路径没有变化
- `test_cli.py` 和全量测试都继续通过

### R14. guards 已拆成 quote + regime + features + facade

这轮新增：

- `v2/src/pm15min/live/guard_quote.py`
- `v2/src/pm15min/live/guard_regime.py`
- `v2/src/pm15min/live/guard_features.py`

同时收口：

- `guards.py` 现在只保留统一编排入口

现在职责更清楚：

- `guard_quote.py`：quote / edge / roi guard
- `guard_regime.py`：liquidity / regime / trade-count-cap guard
- `guard_features.py`：directional return / tail-space guard
- `guards.py`：public orchestration

这次拆分的价值在于：

- decision guard 终于按主题拆开了
- `decision.py` 仍然只依赖一个稳定入口
- `test_live_guards.py`、`test_live_service.py` 和全量测试都继续通过

### R15. quotes 已拆成 market + orderbook + service + facade

这轮新增：

- `v2/src/pm15min/live/quote_market.py`
- `v2/src/pm15min/live/quote_orderbook.py`
- `v2/src/pm15min/live/quote_service.py`

同时收口：

- `quotes.py` 现在只保留 public facade 和常量出口

现在职责更清楚：

- `quote_market.py`：market row 读取和 market 解析
- `quote_orderbook.py`：orderbook index 校验和 quote 选行
- `quote_service.py`：quote snapshot 主组装
- `quotes.py`：public facade

这次拆分的价值在于：

- signal -> decision -> execution 之间的 quote 桥接层终于拆开了
- 关键 market 选择和 orderbook 选择逻辑不再混在一个文件
- `test_live_quotes.py`、`test_live_service.py` 和全量测试都继续通过

### R16. signal_service 已拆成 scoring + orchestration

这轮新增：

- `v2/src/pm15min/live/signal_scoring.py`
- `v2/src/pm15min/live/signal_scoring_bundle.py`
- `v2/src/pm15min/live/signal_scoring_offsets.py`

同时收口：

- `signal_service.py` 现在主要保留 `check/decide/quote` 这层编排入口
- `signal_scoring.py` 现在只保留 facade 和最终 payload 组装

现在职责更清楚：

- `signal_scoring.py`：`score_live_latest` public facade 和 payload 组装
- `signal_scoring_bundle.py`：bundle 选择、feature set 解析、live feature/liquidity/regime state 准备
- `signal_scoring_offsets.py`：offset 扫描、blacklist 应用、coverage / NaN 判定、score row 组装
- `signal_service.py`：signal check、decision orchestration、quote orchestration

这次拆分的价值在于：

- signal scoring 不再堆在一个文件里
- `service.py` 仍然只依赖一个稳定模块入口
- `test_live_service.py`、`test_cli.py` 和全量测试都继续通过

### R17. runner_service 已拆成 iteration + runtime + facade

这轮新增：

- `v2/src/pm15min/live/runner_iteration.py`
- `v2/src/pm15min/live/runner_runtime.py`

同时收口：

- `runner_service.py` 现在只保留兼容导出

现在职责更清楚：

- `runner_iteration.py`：单次 runner iteration 构建
- `runner_runtime.py`：loop、日志、summary 落盘
- `runner_service.py`：public facade

这次拆分的价值在于：

- iteration 构建和 loop/runtime 终于分开了
- `runner.py` 的 monkeypatch 兼容路径没有变化
- `test_live_runner.py` 和全量测试都继续通过

---

## 当前逐项 Check

| 检查项 | 结论 | 说明 |
| --- | --- | --- |
| canonical 入口是否单一 | 通过 | `pm15min` 已是主入口，`apps/` 是 shim |
| legacy 依赖是否被收口 | 通过 | 新旧边界已显式化 |
| rewrite path / legacy path 是否语义分离 | 通过 | layout 方向正确 |
| research 是否比旧版更清晰 | 通过 | labels / datasets / training / inference 已分层 |
| live 主流程是否比旧版清晰 | 通过 | 已从 god module 过渡到 facade + focused modules |
| action side-effect 边界是否清楚 | 通过 | facade / builders / gate / persistence / submit-cancel-redeem service 已分层 |
| runner 边界是否清楚 | 通过 | 入口、编排、诊断、工具已拆开 |
| readiness 边界是否清楚 | 通过 | 展示入口与状态/汇总聚合已分离 |
| liquidity 边界是否清楚 | 通过 | facade / fetch / policy / state 已分层 |
| CLI 分发结构是否清楚 | 通过 | helper / handler / registry 已分离，命令合同未变 |
| profile 配置层是否清楚 | 通过 | spec / catalog / resolver facade 已分离 |
| account 状态层是否清楚 | 通过 | state / summary / persistence / facade 已分离 |
| operator follow-up 解释层是否清楚 | 通过 | actions / categories / rejects / facade 已分离 |
| gateway 检查层是否清楚 | 通过 | service / capabilities / probes / facade 已分离 |
| capital usage 解释层是否清楚 | 通过 | service / context / overview / facade 已分离 |
| regime 控制层是否清楚 | 通过 | controller / state / persistence / facade 已分离 |
| execution policy 层是否清楚 | 通过 | helpers / retry / order-policy / facade 已分离 |
| decision guard 层是否清楚 | 通过 | quote / regime / features / facade 已分离 |
| quote 桥接层是否清楚 | 通过 | market / orderbook / service / facade 已分离 |
| signal 编排层是否清楚 | 通过 | scoring / check / decide / quote 已开始分层 |
| runner 编排层是否清楚 | 通过 | iteration / runtime / facade 已分离 |
| adapter 边界是否干净 | 通过 | direct / legacy 重复逻辑已共享 |
| 持久化层是否足够稳 | 通过 | latest/snapshot 已改原子写路径 |
| CLI 合同是否足够一致 | 通过 | canonical target 限定已前置到 parser |
| 测试是否覆盖主线 | 通过 | 当前全量 `172 passed` |

---

## 当前仍值得继续拆的热点

先说明范围：

- 下面这张表只看 `live` 域
- 因为这份文档的修复主线一直集中在 `live`
- 全仓级热点已经在上面的“当前全仓热点”里单独列出

下面这些不是“结构错误”，而是 `live` 域里下一轮最值得继续减重的地方：

| 文件 | 当前行数 | 判断 |
| --- | --- | --- |
| `live/layout.py` | `438` | 路径 contract 已稳定，method factory 已减重一轮，但 public path surface 仍偏大 |
| `live/operator_actions.py` | `334` | follow-up 规则已拆层，但 facade/orchestrator 仍承载较多组合逻辑 |
| `live/service.py` | `322` | facade 已明显变薄，但 public surface 仍然大 |
| `live/service_facade_helpers.py` | `282` | facade helper / alias / wiring bridge 已独立，但仍偏密 |
| `live/action_builders.py` | `230` | action payload builders 已独立，但单文件里的 payload 细节仍偏多 |
| `live/runner_iteration.py` | `200` | runner 单次迭代已独立，后续如有需要可再把 foundation/side-effect 编排分开 |

重点说明：

- 这些文件现在“偏大”，但不等于“设计错误”
- 当前 live 域已经进入第二阶段
- 第二阶段的目标不是救火，而是继续做减法，让模块变化原因更单一
- `live/signal_scoring.py` 这轮已经完成主要收口，不再是最优先目标

---

## 推荐的下一轮拆分顺序

如果继续只优化 `live`，我会按下面这个顺序走。

### 1. `live/layout.py`

优先级最高。

原因：

- 现在最重的结构性热点已经回到 path contract
- method factory 虽然减掉了重复实现，但 public path surface 仍集中在一个类里

### 2. `live/operator_actions.py` 或 `live/service.py`

中期可做。

原因：

- 一个是 operator 动作建议规则层，一个是 live 总门面的 public surface
- 都相对稳定，但仍有继续做减法的空间

### 3. `live/service_facade_helpers.py` 或 `live/action_builders.py`

这两块相对稳定，但如果继续做结构减法，可以考虑它们。

如果把范围放大到全仓，我会额外考虑这一条线：

1. `live/layout.py`
2. `live/operator_actions.py` / `live/service.py`
3. `data/layout.py` / `research/layout.py`

理由：

- `data/service.py` / `data/cli.py` / `research/cli.py` 这轮已经完成系统性收口
- 现在剩下的热点更偏 live path surface、operator 解释层和 facade surface

---

## 最终判断

如果只回答“重构得怎么样”：

- 比旧版明显更好
- 已经不是散装工程
- `v2` 已经具备 canonical path、可验证边界和持续演进能力

如果回答“现在是不是已经足够简洁清晰优雅”：

- research 基本接近
- data / layout 方向正确
- live 主线已经稳定
- 但 live 还有几块体量偏厚，值得继续拆

所以当前最准确的结论是：

- 大结构已经对了
- 现在不该再推翻
- 应该继续沿着现有边界，把剩余大文件一块块削薄

---

## 当前建议

如果继续沿着 `live` 主线走，下一轮就按这个顺序继续：

1. 先看 `live/layout.py`
2. 再看 `live/operator_actions.py` 或 `live/service.py`
3. 然后再处理 `live/service_facade_helpers.py` 或 `live/action_builders.py`

这条路线是合理的，因为它在继续提升结构优雅度的同时，不会破坏已经稳定下来的 public contract 和测试面。

如果改成看全仓，那么我会把优先级调整成：

1. `live/layout.py`
2. `live/operator_actions.py` / `live/service.py`
3. `data/layout.py` / `research/layout.py`
