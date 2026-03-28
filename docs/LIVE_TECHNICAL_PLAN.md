# Live Domain Specification

这份文档描述当前仓库里长期有效的 `live` 域 contract。

目标：

- 说明 live 主链路负责什么。
- 说明当前 CLI 面、状态面和交易接入边界。
- 说明 operator 应该依赖哪些产物。

如果代码和文档冲突，以 `src/pm15min/live/`、`src/pm15min/live/layout/` 为准。

## 1. 边界

`live` 只负责运行时行为：

- 读取 active bundle
- 构造最新特征与打分
- 生成 quote / decision / execution plan
- 执行 runtime guard
- 写入 runner、account、redeem、operator 相关状态

`live` 不负责：

- 原始数据录制
- 训练和回测
- 实验编排

这些分别属于：

- `data`
- `research`

## 2. 当前模块结构

当前 `src/pm15min/live/` 已形成较稳定的子域：

- `account/`
- `actions/`
- `capital_usage/`
- `cli/`
- `execution/`
- `gateway/`
- `guards/`
- `layout/`
- `liquidity/`
- `operator/`
- `oracle/`
- `profiles/`
- `quotes/`
- `readiness/`
- `redeem/`
- `regime/`
- `runner/`
- `service/`
- `signal/`
- `trading/`

含义：

- `trading/` 和 `gateway/` 负责交易接入与规范化。
- `signal/`、`quotes/`、`execution/` 负责主计算链。
- `runner/`、`operator/`、`readiness/`、`redeem/` 负责运行编排和 operator 视图。

## 3. CLI 面

当前公开的 `live` 顶层命令为：

```text
show-config
show-layout
check-trading-gateway
show-latest-runner
show-ready
score-latest
quote-latest
check-latest
decide-latest
runner-once
runner-loop
execution-simulate
execute-latest
sync-account-state
sync-liquidity-state
apply-cancel-policy
apply-redeem-policy
redeem-loop
```

最常用命令：

```bash
PYTHONPATH=src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct
PYTHONPATH=src python -m pm15min live show-ready --market sol --profile deep_otm --adapter direct
PYTHONPATH=src python -m pm15min live runner-once --market sol --profile deep_otm --target direction --dry-run-side-effects
```

## 4. canonical live 入口

当前文档层面只认两类入口：

### 4.1 Python CLI

- `python -m pm15min live ...`

在当前仓库根目录下默认应写成：

```bash
PYTHONPATH=src python -m pm15min live ...
```

### 4.2 shell entrypoints

当前仓库内的 canonical shell wrappers 在：

```text
scripts/entrypoints/
```

主要入口：

- `start_v2_live_foundation.sh`
- `start_v2_live_trading.sh`
- `start_v2_orderbook_fleet.sh`
- `start_v2_auto_redeem.sh`

这些脚本只做很薄的 runtime 包装：

- 加载环境
- 防重复启动
- 记录 wrapper 层 stdout / stderr

真正状态仍以 `var/live/` 为准。

## 5. 状态与日志

`live/layout/paths.py` 定义的 state/log scope 是长期 contract。

常见状态根目录：

```text
var/live/state/
var/live/logs/
```

常见对象：

- runner summary / snapshots
- account state
- liquidity state
- readiness state
- redeem runner state
- orderbook hot cache state

路径风格固定为：

```text
var/live/state/<group>/cycle=<cycle>/asset=<asset>/profile=<profile>/...
var/live/state/<group>/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/...
var/live/logs/<group>/cycle=<cycle>/asset=<asset>/profile=<profile>/...
```

规则：

- “当前最新状态”放 `latest.json`。
- 历史快照放 `snapshots/snapshot_ts=<ts>/...`。
- 连续运行日志放 `jsonl`。

## 6. bundle 与 profile 规则

live 不直接扫描 training run。

当前主规则：

- 先读 `research/active_bundles/.../selection.json`
- 再加载对应 `research/model_bundles/...`

`profile` 的语义由 `live/profiles/` 和 active bundle 一起定义。

文档层面不再维护“某个日期的开发阶段路线图”；只维护长期规则：

- 同一 `asset + profile + target` 只应有一个 active bundle。
- live 侧默认依赖 active selection，而不是目录猜测。

## 7. 交易接入规则

当前 live 交易接入已收敛到 gateway / trading 抽象：

- `live/trading/`
- `live/gateway/`

应长期遵守：

- 上层 action / service 通过 gateway 能力做只读 probe、下单、撤单、redeem。
- 不允许在新代码里重新散落旧的交易直连逻辑。
- adapter 选择由统一配置和 CLI 参数控制。

## 8. operator 视图规则

operator 层当前主入口为：

- `show-ready`
- `show-latest-runner`
- `check-trading-gateway`

这三个命令的目的不同：

- `check-trading-gateway` 只看接入、依赖和只读 probe。
- `show-ready` 看是否允许进入 side-effect 主线。
- `show-latest-runner` 看最近一次 runner 落盘状态。

如果需要值班手册，使用 `docs/LIVE_OPERATOR_RUNBOOK.md`，不要把 operator 步骤继续写进本技术规范。

## 9. 维护规则

- 本文件只保留长期边界，不记录某一轮“已经完成三件事”的 checklist。
- 单次验证记录和 dated audit 不放在 `docs/` 里作为长期规范。
- 新增 live 对象时，优先补 state/log contract、summary、tests，再补文档。
