# Phase A 技术方案：去 legacy 化交易基础设施

这份文档是 `v2` 下一阶段的详细技术方案。

它解决的问题只有一个：

- 让 `v2/live` 的真实交易 side effect 不再散落在 `account.py` / `actions.py` 里直接 import `live_trading`
- 把真实账户读取、真实下单、真实撤单、真实 redeem 收敛成一个清晰、可替换、可测试的交易接入层

这不是新的大重构。

这一步的目标非常克制：

- 不改 `deep_otm direction` 的业务语义
- 不推翻当前 `signal -> quote -> decision -> execution -> actions -> runner` 主链路
- 不动 `rust_fullset_engine/`
- 只收口“真实交易基础设施接入方式”

如果后续实现和本文档冲突，以最终 `v2/src/pm15min/live/` 实现为准；但实现应尽快改回本文档的边界，而不是继续发散。

---

## 0. 当前进度（2026-03-21）

当前已经完成 Phase A 的第一批收口：

- 已新增 `live/trading/`
  - `auth.py`
  - `contracts.py`
  - `gateway.py`
  - `service.py`
  - `legacy_adapter.py`
- `account.py` 已不再直接 import `live_trading`
- `actions.py` 已不再直接 import `live_trading`
- `live_trading` 依赖当前已集中收敛到 `live/trading/legacy_adapter.py`
- `build_account_state_snapshot` / `build_open_orders_snapshot` / `build_positions_snapshot`
  - 已支持显式 `gateway` 注入
- `submit_execution_payload` / `apply_cancel_policy` / `apply_redeem_policy`
  - 已支持显式 `gateway` 注入
- `service.py` / `runner.py`
  - 已支持将 `gateway` 向上层 orchestration 继续透传
- `account` / `order` / `cancel` / `redeem` payload
  - 已显式写入 `trading_gateway`
  - 当前可以稳定区分 `legacy` / `direct` / injected fake gateway
- `test_live_account.py` / `test_live_actions.py`
  - 已切到 fake gateway 测试路径，不再依赖 patch 私有 legacy helper
- 已新增 `test_live_trading_legacy_adapter.py`
  - 覆盖：
    - legacy auth/trader 构造
    - order request 转换
    - cancel / redeem result 映射
    - positions API 规范化
- `trading/service.py`
  - 已支持 adapter 选择逻辑
  - 已集中 live trading env prerequisite 检查与 gateway 构造 helper
  - 当前支持：
    - `legacy`
    - `direct`
- `account.py` / `actions.py`
  - 已改为复用 `trading/service.py` 的统一 helper
  - 不再各自散落解析 trading env prerequisite
- `service.py`
  - `check-trading-gateway` 已改为复用 `trading/service.py` 的统一 env config loader
- `submit_execution_payload`
  - 下单请求的 dict -> `PlaceOrderRequest` 转换已收拢到 `live/trading/service.py`
- `direct_adapter.py`
  - 已接入：
    - `list_open_orders`
    - `list_positions`
    - `place_order`
    - `cancel_order`
    - `redeem_positions`
- `PM15MIN_LIVE_TRADING_ADAPTER`
  - 已成为 adapter 选择入口
- `live check-trading-gateway`
  - 已建立零 side effect 的 adapter health/validation 命令
  - 支持：
    - `--adapter legacy|direct` 覆盖检查
    - config readiness 检查
    - dependency 检查
    - 可选只读 probe：`open_orders` / `positions`
  - 当前输出还会显式给出：
    - 每个 capability 的 readiness / blocked_by
    - 推荐 smoke run 顺序
- `sync-account-state` / `execute-latest` / `apply-cancel-policy` / `apply-redeem-policy` / `runner-once` / `runner-loop`
  - 已支持 `--adapter legacy|direct`
  - 可以在不切环境变量的情况下做目标 adapter smoke run
- 已新增：
  - `redeem_relayer.py`
  - direct redeem helper 测试
- 已新增 auth 解析测试
- direct adapter 异常路径测试已补到：
  - 缺失 private key
  - 缺失 user address
  - positions API 非 list payload
  - unsupported action
  - limit sell 下单失败映射
  - cancel 失败映射
- legacy adapter 异常路径测试已补到：
  - 缺失 private key
  - 缺失 user address
  - positions API 非 list payload
  - unsuccessful place_order response 映射
  - cancel 失败映射
  - empty redeem payload
- 已完成一轮本机真实 smoke validation：
  - `direct` adapter:
    - `check-trading-gateway` build 成功
    - `--probe-open-orders` 成功，当前返回 `0` 行
    - `--probe-positions` 成功，最近观测约 `2000+` 行
  - `legacy` adapter:
    - `check-trading-gateway` build 成功
    - `--probe-open-orders` 成功，当前返回 `0` 行
    - `--probe-positions` 成功，最近观测约 `2000+` 行
- 已更新 architecture guard，明确只允许 adapter 边界依赖 legacy
- `docs/` 下的状态文档与 operator runbook 已对齐当前 adapter / blocker / smoke 语义
- 当前本地回归：
  - `PYTHONPATH=v2/src pytest -q v2/tests`
  - `172 passed, 1 warning`

当前还没有完成的部分：

- trading adapter 仍缺更多异常路径测试
- 部分 CLI 仍只暴露生产入口，不暴露测试态 gateway 注入
- `show-ready`
  - 现在会主动带：
    - `open_orders` probe
    - `positions` probe
    - `operator_smoke_summary`
  - 已能区分：
    - `gateway/probe` 没打通
    - `runner` 真正 infra-blocked
    - 只是策略层 `no-trade`
  - 本轮已经把这三层组合读法同步到：
    - `LIVE_OPERATOR_RUNBOOK.md`
    - `CURRENT_REMAINING_CHECKLIST.md`
    - `REWRITE_STATUS_AND_ROADMAP.md`
  - 后续只需随 operator 字段变化增量同步文档

本轮新增验证：

- `PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct --probe-open-orders --probe-positions`
  - 在 `2026-03-20 14:28 UTC` 左右返回 `ok=true`
  - `open_orders row_count=0`
  - `positions row_count=2002`
- `PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market xrp --profile deep_otm --adapter direct --probe-open-orders --probe-positions`
  - 在 `2026-03-20 15:23 UTC` 左右返回 `ok=true`
  - `open_orders row_count=0`
  - `positions row_count=2003`
- `PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter legacy --probe-open-orders --probe-positions`
  - 在 `2026-03-20 15:33 UTC` 左右返回 `ok=true`
  - `open_orders row_count=0`
  - `positions row_count=2003`
- `PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market xrp --profile deep_otm --adapter legacy --probe-open-orders --probe-positions`
  - 在 `2026-03-20 15:33 UTC` 左右返回 `ok=true`
  - `open_orders row_count=0`
  - `positions row_count=2003`
- `PYTHONPATH=v2/src python -m pm15min live show-ready --market sol --profile deep_otm --adapter direct`
  - 在 `2026-03-20 14:09 UTC` 左右，当前返回：
    - `status=not_ready`
    - `primary_blocker=decision_not_accept`
    - `operator_smoke_summary.status=operational`
    - `operator_smoke_summary.reason=strategy_reject_only`
  - 这说明当时 `sol` 的 direct 主路径已经打通，但本轮仍是 strategy no-trade，不是 gateway / probe 故障
- `PYTHONPATH=v2/src python -m pm15min live show-ready --market xrp --profile deep_otm --adapter direct`
  - 在 `2026-03-20 14:09 UTC` 左右，当前返回：
    - `status=not_ready`
    - `primary_blocker=latest_runner_missing`
    - `operator_smoke_summary.status=blocked`
    - `operator_smoke_summary.reason=runner_missing`
  - 这说明当时 `xrp` 的 direct gateway 已 ready，但还缺一次 canonical runner dry-run
- `PYTHONPATH=v2/src python -m pm15min live runner-once --market sol --profile deep_otm --target direction --adapter direct --dry-run-side-effects`
  - 在 `2026-03-20 14:09 -> 14:10 UTC` 完成
  - 当前最新结果已前移到：
    - `decision.status=accept`
    - `execution.status=plan`
    - `order_action.status=ok` 且 `reason=dry_run`
    - `runner_health.blocking_issue_count=0`
  - 但 `foundation.status` 仍是 `ok_with_errors`
  - foundation log 已明确记录：
    - `task=oracle`
    - direct `/api/crypto/crypto-price` 返回 `Too Many Requests`
    - 当前按 `fail_open` 退回 `oracle_prices_table` fallback
  - 当前代码已经继续把这条信息链产品化到：
    - `risk_summary.foundation.reason`
    - `risk_summary.foundation.issue_codes`
    - `operator_summary.foundation_reason`
    - `operator_summary.foundation_issue_codes`
    - `operator_summary.foundation_degraded_tasks`
- `PYTHONPATH=v2/src python -m pm15min live runner-once --market xrp --profile deep_otm --target direction --adapter direct --dry-run-side-effects`
  - 在 `2026-03-20 15:24 -> 15:25 UTC` 再次完成
  - 当前最新结果同样已前移到：
    - `decision.status=accept`
    - `execution.status=plan`
    - `order_action.status=ok` 且 `reason=dry_run`
    - `runner_health.blocking_issue_count=0`
  - 同样仍有 `foundation.status=ok_with_errors`
  - foundation log 也明确记录：
    - `task=oracle`
    - direct `/api/crypto/crypto-price` 返回 `Too Many Requests`
    - 当前按 `fail_open` 退回 `oracle_prices_table` fallback
- `PYTHONPATH=v2/src python -m pm15min live show-ready --market sol --profile deep_otm --adapter direct`
  - 在 `2026-03-20 14:10 UTC` 左右的历史记录里再次执行，返回：
    - `status=not_ready`
    - `primary_blocker=null`
    - `operator_smoke_summary.status=operational`
    - `operator_smoke_summary.reason=path_operational`
    - `ready_for_side_effects=false`
  - 这说明当时不再是策略拒单，也不是 gateway / runner 主路径不通，而是 `foundation_ok_with_errors` 仍让 operator 入口保持保守
- `PYTHONPATH=v2/src python -m pm15min live show-ready --market xrp --profile deep_otm --adapter direct`
  - 在 `2026-03-20 15:25 UTC` 左右再次执行，返回：
    - `status=not_ready`
    - `primary_blocker=foundation_ok_with_errors`
    - `operator_smoke_summary.status=operational`
    - `operator_smoke_summary.reason=foundation_warning_only`
    - `ready_for_side_effects=false`
  - 当前语义更清楚：
    - gateway / probe / runner dry-run 主路径已通
    - operator 入口保守 not-ready 的原因就是 foundation oracle degrade
- `PYTHONPATH=v2/src python -m pm15min live show-ready --market sol --profile deep_otm --adapter direct`
  - 在 `2026-03-20 14:28 UTC` 左右再次执行，当前返回：
    - `status=not_ready`
    - `primary_blocker=decision_not_accept`
    - `operator_smoke_summary.status=operational`
    - `operator_smoke_summary.reason=strategy_reject_only`
    - `foundation_status=ok_with_errors`
    - `foundation_issue_codes=["oracle_direct_rate_limited"]`
  - 当前 `next_actions` 还会直接提示：
    - 等待 direct oracle rate-limit window
    - 重跑 `data run live-foundation` 或 `runner-once --dry-run-side-effects`
    - 把 `oracle_prices_table` 视为临时 fail-open fallback
  - 这说明实时市场变化下，`primary_blocker` 可能重新回到策略拒单，但 foundation 降级链仍然明确可见，operator 不会再把它误判成 gateway 故障
- `PYTHONPATH=v2/src python -m pm15min live runner-once --market sol --profile deep_otm --target direction --adapter legacy --dry-run-side-effects`
  - 在 `2026-03-20 15:33 -> 15:35 UTC` 左右完成
  - 随后再看 `show-latest-runner --risk-only` / `show-ready --adapter legacy`
    - `decision_status=reject`
    - `execution_status=no_action`
    - `order_action_status=skipped`
    - `primary_blocker=decision_not_accept`
    - `operator_smoke_summary.reason=strategy_reject_only`
    - `foundation_status=ok_with_errors`
    - `foundation_issue_codes=["oracle_direct_rate_limited"]`
  - 这说明：
    - `legacy` adapter 本身不是当前瓶颈
    - 当前 real blocker 仍可能回到策略拒单
    - 同时 foundation oracle degrade warning 仍然并存

这些真实记录当前已经覆盖的语义可以更明确地归成：

- adapter / probe 读路已通
  - `check-trading-gateway`
  - `--probe-open-orders`
  - `--probe-positions`
- runner dry-run 已通到 side-effect 计划层
  - `decision.status=accept`
  - `execution.status=plan`
  - `order_action.status=ok`
  - `order_action.reason=dry_run`
- operator 视角已经覆盖四种不同 blocker 语义
  - `runner_missing`
  - `strategy_reject_only`
  - `foundation_warning_only`
  - `path_operational`
- 这也说明当前 `not_ready` 已经不是单一语义
  - 可能是主路径未通
  - 也可能只是策略拒单
  - 也可能是 foundation warning 仍保守挂着

所以当前状态更准确地说是：

- Phase A 已经开始实施
- Phase A 的边界已经立起来
- direct gateway / account / decision / execution / dry-run order path 已验证可运行
- 当前残留的主要不足已经收敛到：
  - live foundation 的 oracle 子任务会被 Polymarket `/api/crypto/crypto-price` rate limit
  - 当前靠 fallback table fail-open 继续跑
  - 当前已经不需要再翻 foundation log 才能知道这个原因
  - `show-ready.next_actions` 已能直接给出恢复动作
- 实时市场变化下，`primary_blocker` 可能在 `decision_not_accept` 与 `null/path_operational` 之间切换
- 但当前剩余问题已经不再是交易接入层打不通，而是策略拒单与 foundation 降级 warning 的组合
- 这也解释了当前 should 顺序为什么是：
  - 先把 operator 组合 blocker 分流写清楚
  - 再继续补更多真实 dry-run / adapter validation 记录
  - 最后才考虑 cash/equity 级别账户总览

### 0.1 当前推荐 smoke run 顺序

如果要验证某个 adapter 是否已经达到“可继续往前推进”的状态，当前推荐按下面顺序跑：

1. 基础 health check

```bash
PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct
```

2. 只读 probe open orders

```bash
PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct --probe-open-orders
```

3. 只读 probe positions

```bash
PYTHONPATH=v2/src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct --probe-positions
```

4. 账户同步

```bash
PYTHONPATH=v2/src python -m pm15min live sync-account-state --market sol --profile deep_otm --adapter direct
```

5. 干跑 execute

```bash
PYTHONPATH=v2/src python -m pm15min live execute-latest --market sol --profile deep_otm --target direction --adapter direct --dry-run
```

6. 干跑 runner

```bash
PYTHONPATH=v2/src python -m pm15min live runner-once --market sol --profile deep_otm --target direction --adapter direct --dry-run-side-effects
```

原则：

- 先 health check
- 再 probe
- 再 sync
- 最后才是 execute / runner dry-run

如果 `check-trading-gateway` 的 capability readiness 已经显示某项 `blocked_by`，先修配置或依赖，不要跳过直接跑后面的命令。

### 0.2 当前结论

截至 2026-03-21，这个阶段可以更准确地描述成：

- Phase A 的 adapter 边界已经立稳
- direct / legacy 两条主读路径已经做过真实只读 smoke validation
- 真实 side-effect 主路径现在也已经有更窄的 operator smoke 收口
- operator runbook / checklist / roadmap 已对齐当前 blocker 与 smoke 顺序
- Phase A 剩余工作主要是异常路径继续补齐与 adapter 边界继续收口，而不是主路径不可用
- 下一阶段已经可以开始进入 Phase B 的 live 收口

---

## 1. 当前问题定义

### 1.1 当前已经完成的部分

当前 `v2/live` 已经完成了：

- `signal / quote / decision / execution` 主链
- `runner-once / runner-loop`
- `sync-account-state`
- `execute-latest`
- `apply-cancel-policy`
- `apply-redeem-policy`

也就是说，`v2` 现在已经能编排真实交易相关 side effect。

### 1.2 当前真正没有完成的部分

当前真实交易接入虽然已经完成第一步收口，但仍然没有彻底摆脱 legacy。

当前剩余的 legacy 耦合已经集中在：

- `live/trading/legacy_adapter.py`
  - 负责 legacy `PolymarketAuth` / `PolymarketTrader` 转接
  - 负责 legacy `OrderRequest` 转换
  - 负责 legacy `auto_redeem.redeem_positions` 转接

这意味着当前 `v2` 的问题不是“不能跑”，而是：

- orchestration 已经在 `v2`
- 但交易接入层仍不是 `v2` 自己的边界

### 1.3 当前问题为什么严重

这种结构会带来 5 个长期问题：

1. 虽然 legacy import 已收口，但 direct client 仍未替换
2. 上层仍有一部分过渡期 helper 需要继续清理
3. future direct client 替换还没有真正发生
4. fake gateway 测试模式还没有成为默认主方式
5. `v2/live` 还不能称为真正 clean-room runtime

---

## 2. Phase A 的明确目标

### 2.1 总目标

在 `v2/src/pm15min/live/` 下建立唯一 canonical 交易接入层。

这个接入层负责：

- 认证配置解析
- 账户读取
- 下单
- 撤单
- redeem
- 与 legacy 的过渡桥接

而 `account.py` / `actions.py` / `service.py` / `runner.py` 只负责 orchestration。

### 2.2 Definition of Done

Phase A 完成的标志必须同时满足：

1. `v2/src/pm15min/live/account.py` 不再直接 import `live_trading`
2. `v2/src/pm15min/live/actions.py` 不再直接 import `live_trading`
3. `v2/src/pm15min/live/` 中如果还存在 legacy 依赖，只允许集中在一个显式 adapter 模块
4. `execute-latest` / `apply-cancel-policy` / `apply-redeem-policy` / `sync-account-state` 行为不发生业务回归
5. 交易接入层可以被 fake gateway 替换，单元测试不需要真的 import legacy client
6. 文档能明确回答：
   - 真实交易入口在哪里
   - legacy 过渡层在哪里
   - 将来 direct client 替换时需要改哪里

### 2.3 非目标

Phase A 不负责：

- 重写 Polymarket 底层协议 client
- 改交易策略逻辑
- 改 score / decision / execution 语义
- 改 Data / Research 域
- 一步到位去掉所有 legacy 代码

Phase A 只负责：

- 收口接入边界
- 缩小 legacy 依赖面
- 为下一步 direct client 替换打地基

---

## 3. 目标架构

### 3.1 新的目录结构

Phase A 完成后，`live` 交易基础设施应收敛到：

```text
v2/src/pm15min/live/
  trading/
    __init__.py
    auth.py
    contracts.py
    gateway.py
    service.py
    legacy_adapter.py
```

其中职责固定：

- `trading/auth.py`
  - 只做环境变量解析与认证配置标准化

- `trading/contracts.py`
  - 定义稳定的 v2 交易数据契约

- `trading/gateway.py`
  - 定义 gateway protocol / interface

- `trading/service.py`
  - 提供统一入口：
    - `build_live_gateway_from_env()`
    - `build_account_reader_from_env()`
    - 或统一 `build_trading_gateway_from_env()`

- `trading/legacy_adapter.py`
  - 当前阶段唯一允许直接 import `live_trading` 的位置

### 3.2 现有模块重定位

Phase A 后这些文件职责要更清楚：

- `live/account.py`
  - 只负责“账户快照编排 + 规范化输出”
  - 不再知道 legacy trader 是谁

- `live/actions.py`
  - 只负责：
    - action key
    - 幂等 gate
    - retry gate
    - 请求前后状态编排
  - 不再直接构造 legacy request / trader / redeem client

- `live/service.py`
  - 只负责 CLI 对应的 orchestration

- `live/runner.py`
  - 只负责调用上述 orchestration
  - 不参与交易 client 细节

---

## 4. 核心接口设计

### 4.1 统一 gateway 接口

需要定义一个稳定 protocol，例如：

```python
class LiveTradingGateway(Protocol):
    def list_open_orders(self) -> list[OpenOrderRecord]: ...
    def list_positions(self) -> list[PositionRecord]: ...
    def place_order(self, request: PlaceOrderRequest) -> PlaceOrderResult: ...
    def cancel_order(self, order_id: str) -> CancelOrderResult: ...
    def redeem_positions(self, condition_id: str, index_sets: list[int]) -> RedeemResult: ...
```

核心原则：

- `v2/live` 上层永远依赖这个接口
- 不依赖 legacy client 的类名、返回结构、字段命名

### 4.2 contracts 层必须定义的对象

至少需要这些稳定 contract：

- `TradingAuthConfig`
- `DataApiConfig`
- `PlaceOrderRequest`
- `PlaceOrderResult`
- `CancelOrderResult`
- `RedeemRequest`
- `RedeemResult`
- `OpenOrderRecord`
- `PositionRecord`

这些 contract 的字段命名必须使用 `v2` 自己的语义，而不是 legacy 字段名原样透传。

### 4.3 auth 解析 contract

`trading/auth.py` 需要统一解析：

- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_SIGNATURE_TYPE`
- `POLYMARKET_FUNDER`
- `POLYMARKET_USER_ADDRESS`
- `POLYMARKET_DATA_API_BASE`

并产出两个独立对象：

- `TradingAuthConfig`
  - 用于 CLOB 读写

- `DataApiConfig`
  - 用于 positions API

原则：

- 环境变量解析只能放在这一层
- 上层不应该反复自己读 env

---

## 5. 数据契约详细约束

### 5.1 `OpenOrderRecord`

应稳定包含：

- `order_id`
- `market_id`
- `token_id`
- `side`
- `status`
- `price`
- `size`
- `created_at`
- `raw`

这应与当前 `account.py` 产出的规范化结构保持兼容，不允许 Phase A 改动下游依赖字段。

### 5.2 `PositionRecord`

应稳定包含：

- `market_id`
- `condition_id`
- `token_id`
- `outcome_index`
- `index_set`
- `redeemable`
- `current_value`
- `cash_pnl`
- `size`
- `raw`

原则：

- `redeemable` 和 `index_set` 仍保持当前语义
- `condition_id` 必须保持为 redeem policy 的主键

### 5.3 `PlaceOrderRequest`

需要明确包含：

- `market_id`
- `token_id`
- `side`
- `order_type`
- `order_kind`
- `action`
- `price`
- `size`
- `decision_ts`
- `metadata`

这层是 v2 canonical request，不是 legacy request。

legacy 的 `OrderRequest` 只能由 adapter 负责转换。

### 5.4 `PlaceOrderResult`

至少包含：

- `success`
- `status`
- `order_id`
- `message`
- `raw`

上层 action log 只能依赖这套返回结构。

---

## 6. adapter 设计

### 6.1 为什么必须有 `legacy_adapter.py`

因为当前阶段还不能一步重写 Polymarket client。

所以最合理的过渡方案不是继续散点直连，而是：

- 建一个显式 `legacy_adapter.py`
- 让它成为唯一 legacy import 汇聚点

### 6.2 `legacy_adapter.py` 负责什么

它负责：

- 把 `TradingAuthConfig` 转成 legacy `PolymarketAuth`
- 创建 legacy `PolymarketTrader`
- 把 v2 `PlaceOrderRequest` 转成 legacy `OrderRequest`
- 调用 legacy `redeem_positions`
- 把 legacy 返回值再规范化成 v2 `*Result`

### 6.3 `legacy_adapter.py` 明确不负责什么

它不负责：

- action gate
- idempotency
- retry policy
- policy decision
- account snapshot persistence

这些全部继续由 `v2/live` 上层掌控。

### 6.4 adapter 的边界规则

必须满足：

- `legacy_adapter.py` 可以 import `live_trading`
- `account.py` / `actions.py` 不可以 import `live_trading`
- 如果未来 direct client 成熟，应新增 `direct_adapter.py`
- 上层只切换 `service.py` 的 gateway provider，不改 orchestration

---

## 7. 分阶段落地步骤

### Step 1：引入 contracts 与 gateway interface

新增：

- `live/trading/contracts.py`
- `live/trading/gateway.py`

完成内容：

- 定义稳定 dataclass / protocol
- 把当前 `account.py` / `actions.py` 里隐含的 request/result 结构显式化

验收标准：

- 上层不再依赖“某个 dict 恰好长这样”
- request / response 结构可以单独被测试

### Step 2：引入 auth 解析层

新增：

- `live/trading/auth.py`

完成内容：

- 收口所有 Polymarket 相关 env 解析
- 输出规范化 auth config

验收标准：

- `account.py` / `actions.py` 不再自己读 env
- env 缺失时的报错语义集中统一

### Step 3：实现 `legacy_adapter.py`

新增：

- `live/trading/legacy_adapter.py`

完成内容：

- 封装：
  - `list_open_orders`
  - `list_positions`
  - `place_order`
  - `cancel_order`
  - `redeem_positions`

验收标准：

- 所有 legacy import 只出现在这一个模块
- adapter 的输入输出完全使用 v2 contract

### Step 4：实现 `trading/service.py`

新增：

- `live/trading/service.py`

完成内容：

- 对外暴露统一 provider builder
- 当前默认返回 direct adapter gateway

建议入口：

- `build_live_trading_gateway_from_env()`
- `build_live_account_gateway_from_env()`

或者统一成：

- `build_live_gateway_from_env()`

验收标准：

- `account.py` / `actions.py` 只依赖 service 层

### Step 5：重构 `account.py`

完成内容：

- 用 gateway 取代 `_fetch_open_orders_from_live_client`
- 用 gateway 取代直接 positions API 读取逻辑，或者把 positions API 也视为 gateway 一部分

注意：

- 输出 payload 结构不能变
- `latest_open_orders_path`
- `open_orders_snapshot_path`
- `latest_positions_path`
- `positions_snapshot_path`
  都必须保持兼容

验收标准：

- `account.py` 不再 import `live_trading`
- `account.py` 支持显式 `gateway` 注入
- account snapshot 测试仍通过

### Step 6：重构 `actions.py`

完成内容：

- 用 gateway 取代：
  - `_cancel_order_with_live_client`
  - `_redeem_condition_with_live_client`
  - `_place_order_with_live_client`

- 保持现有：
  - `action_key`
  - `gate`
  - `attempt`
  - `retry_interval`
  - `summary`
  语义不变

验收标准：

- `actions.py` 不再 import `live_trading`
- `actions.py` 支持显式 `gateway` 注入
- `execute/cancel/redeem` 行为对上层保持兼容

### Step 7：runner 与 service 只做透传复核

完成内容：

- `service.py` / `runner.py` 一般不需要大改
- 只需要确认 side effect 仍走同一套 orchestration

验收标准：

- `runner-once`
- `runner-loop`
- `execute-latest`
- `apply-cancel-policy`
- `apply-redeem-policy`
  都不需要知道 adapter 细节

---

## 8. 测试方案

### 8.1 必须新增的单元测试

新增测试应至少覆盖：

- auth env parsing
- gateway request/response contract
- legacy adapter request conversion
- legacy adapter result normalization
- account snapshot 在 fake gateway 下的输出
- place/cancel/redeem action 在 fake gateway 下的输出

### 8.2 必须保留的行为回归测试

需要确认这些现有能力不回归：

- `sync-account-state`
- `execute-latest --dry-run`
- `apply-cancel-policy --dry-run`
- `apply-redeem-policy --dry-run`
- `runner-once --dry-run-side-effects`

### 8.3 建议的 fake gateway 测试方式

不要在单元测试里真的起 legacy trader。

建议：

- 用内存 fake gateway 返回固定 open orders / positions / order results
- 用 fake 返回值验证：
  - payload shape
  - gate behavior
  - action_key stability
  - error mapping

### 8.4 验收测试

Phase A 收尾时至少跑：

```bash
PYTHONPATH=v2/src pytest -q v2/tests
```

如果后续补了 trading adapter tests，还应单独跑：

```bash
PYTHONPATH=v2/src pytest -q v2/tests/test_live_account.py
PYTHONPATH=v2/src pytest -q v2/tests/test_live_actions.py
PYTHONPATH=v2/src pytest -q v2/tests/test_live_runner.py
```

---

## 9. 路径与边界约束

### 9.1 允许的依赖方向

允许：

- `live/account.py` -> `live/trading/service.py`
- `live/actions.py` -> `live/trading/service.py`
- `live/trading/service.py` -> `live/trading/legacy_adapter.py`
- `live/trading/legacy_adapter.py` -> `live_trading.*`

### 9.2 禁止的依赖方向

禁止：

- `live/account.py` -> `live_trading.*`
- `live/actions.py` -> `live_trading.*`
- `live/runner.py` -> `live_trading.*`
- `live/service.py` -> `live_trading.*`

### 9.3 文档规则

Phase A 完成后，需要同步更新：

- `v2/docs/LIVE_TECHNICAL_PLAN.md`
- `v2/docs/REWRITE_STATUS_AND_ROADMAP.md`

必须把“legacy 依赖还在哪里”写清楚，不允许文档继续模糊表述。

---

## 10. 风险与规避

### 风险 1：重构时误改业务语义

风险：

- 下单请求字段变了
- `order_kind` / `action` 语义变了
- `redeem` 判定被顺手改掉

规避：

- Phase A 只抽边界，不改策略语义
- 先冻结现有 payload shape
- 加 fake gateway 回归测试

### 风险 2：env 解析收口后行为变化

风险：

- 原来某些 env 缺失还能跑
- 收口后可能直接失败

规避：

- 在 auth 层保留当前默认值与缺失容忍规则
- 先做兼容，不先做“严格化”

### 风险 3：legacy adapter 变成新的大泥球

风险：

- 把所有逻辑都塞进 adapter

规避：

- adapter 只做转换和调用
- gate / retry / orchestration 一律留在上层

---

## 11. 完成后的状态应该长什么样

Phase A 完成后，应该能明确说：

- `v2/live` 真实 side effect 仍可通过 legacy 运行
- 但 legacy 已经被压缩到一个显式 adapter 层
- 上层 orchestration 不再知道 legacy client 细节
- 将来替换成 direct client 时，不需要重写 `account.py` / `actions.py` / `runner.py`

也就是说，Phase A 完成后：

- 还不是“完全摆脱 legacy”
- 但已经完成“结构上摆脱散点 legacy 依赖”

这一步完成后，下一阶段才适合继续做：

- Phase B：Live 风控收口
- 或 direct Polymarket client 替换

---

## 12. 建议执行顺序

实际实施时按这个顺序最稳：

1. `trading/contracts.py`
2. `trading/gateway.py`
3. `trading/auth.py`
4. `trading/legacy_adapter.py`
5. `trading/service.py`
6. 重构 `account.py`
7. 重构 `actions.py`
8. 回归 `service.py / runner.py`
9. 补测试
10. 更新文档

不要一开始就直接改 `runner.py`。

`runner.py` 现在已经是上层 orchestration；它不是当前阶段最脏的地方。

---

## 13. 当前推荐结论

当前下一阶段最值得做、也最不该跳过的事，就是：

- 先完成 Phase A
- 把 `v2/live` 的交易接入层做成清晰边界

因为只有做完这一步，后续这些工作才不会继续带着 legacy 包袱：

- regime-aware execution
- runner 监控收口
- 真实 live 托管
- direct client 替换

如果要用一句话概括这份方案：

- 不是重写交易逻辑
- 是把交易接入边界做对
