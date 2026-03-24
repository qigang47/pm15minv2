# V2 重构完成度与后续路线图（2026-03-21）

这份文档回答 3 个问题：

1. `v2` 现在到底已经完成了什么
2. 还有哪些关键部分没有完成
3. 什么时候才能说“这次重构真的完成了”

结论先写在最前面：

- `v2` 的架构重构已经完成了主骨架
- `v2` 的业务迁移还没有完全完成
- 当前阶段最重要的工作，不是再改大结构，而是继续去 legacy 化并把关键业务语义收口

---

## 1. 当前状态一句话结论

截至 2026-03-21，`v2` 已经具备：

- 唯一 canonical Python CLI 主入口
- 清晰的四域结构
- Data / Research / Live 的主路径实现
- active bundle registry
- live runtime state / runner / execution 主链路
- 一组覆盖主线的测试

但 `v2` 还没有具备：

- 完全脱离 legacy 的真实交易基础设施
- 所有 live 风控/监控收口
- 所有 data 数据面补齐
- 全量关键业务语义 golden parity 验证

所以现在更准确的说法是：

- “重构容器已完成”
- “业务迁移进行中”

如果进一步压缩成阶段判断，当前更接近：

- `必须做` 已完成本轮收口
- 当前正式进入 `应该做` 阶段
- operator 文档 / 状态文档本轮已完成基线对齐

---

## 2. 已经完成的内容

### 2.1 主线收敛

现在仓库已经明确收敛成单主线：

- 唯一重构实现：`v2/src/pm15min/`
- 唯一重构文档：`v2/docs/`
- 唯一 canonical Python 入口：`PYTHONPATH=v2/src python -m pm15min ...`

历史上的 `apps/` 路径不再是 canonical runtime，只保留为 deprecated shim。

这不代表仓库里已经没有其他 legacy shell / script 入口，而是说：

- 它们不应该再被当成重构主线
- 当前状态判断、后续开发和文档都应以 `pm15min` CLI 为准

### 2.2 结构重构

`v2` 的包结构已经形成稳定边界：

```text
v2/src/pm15min/
  core/
  data/
  research/
  live/
```

当前这个结构是对的，后续不应该再推翻重来。

### 2.3 Data 域

按当前实现与文档盘点，Data 域已落地的主能力包括：

- `surface=live|backtest` 分面
- Binance `klines_1m` direct sync
- `market_catalog` snapshot + canonical table
- direct Polymarket oracle sync
- streams RPC sync
- datafeeds RPC sync
- settlement truth RPC sync
- Polymarket orderbook source + orderbook index
- orderbook recorder runtime
- live foundation runtime
- `oracle_prices_15m` canonical builder
- `truth_15m` canonical builder
- truth / oracle exports
- data summary / audit state
  - 已能输出 freshness / dataset-level checks / alignment checks
- legacy 数据导入到 canonical 路径

这意味着 Data 域已经不是“只搭了目录”，而是能承接 live / research 输入的真实域。

### 2.4 Research 域

Research 域已经具备一条可运行的 canonical 主链：

- feature frame
- label frame
- training set
- training run
- model bundle
- active bundle selection
- offline inference
- backtest
- experiment suite
- evaluation

这条链已经足够支撑“训练 -> bundle -> 激活 -> live 使用”的主闭环。

但更严谨地说，Research 还不能表述成“已经完全完成”：

- 一部分规划/说明型接口仍保留在 `research/service.py`
- 语义一致性仍然缺更强的 golden parity 验证
- 回测 / 评估虽然已经能跑，但还不适合被写成“所有研究语义都已完全收口”

### 2.5 Live 域

Live 域当前已经形成主线：

- 读取 `active_bundles`
- 读取 `v2/data/live`
- 构造 live feature frame
- 按 bundle 做 score
- 产出 `signal / quote / decision / execution` state
- 同步 account state / liquidity state / regime state
- 支持 `runner-once` / `runner-loop`
- 支持真实 `execute / cancel / redeem` side effect 编排

当前已落地的主要 guard / simulate 能力包括：

- blacklist compatibility
- NaN feature guard
- liquidity guard
- regime decision guard
- probability threshold
- ret_30m direction guard
- tail-space guard
- quote-aware price / edge / ROI guard
- L1 / full-depth fill simulate
- retry / cancel / redeem 的只读 contract
- `show-latest-runner` / `show-ready` 的 operator 视图
  - 已能区分“基础设施未就绪”与“本轮应 no-trade”
  - 当前会显式写出：
    - `decision_reject_interpretation`
    - `decision_reject_diagnostics`
    - `capital_usage_summary`
    - `operator_smoke_summary`
    - `foundation_reason`
    - `foundation_issue_codes`
    - `foundation_degraded_tasks`

当前 `show-ready` / `show-latest-runner` 还有一个更关键的新事实：

- `show-ready.operator_smoke_summary` 已把 operator 分流显式收口成两层
  - `blocked`
    - `gateway_checks_failed`
    - `gateway_probes_failed`
    - `runner_missing`
    - `runner_infra_blocked`
    - `runner_data_blocked`
  - `operational`
    - `strategy_reject_only`
    - `foundation_warning_only`
    - `path_operational`
- `foundation_ok_with_errors` 不再只是 raw log 里的信息
- 对 `oracle_direct_rate_limited`
  - 当前已经会直接暴露降级 task / issue code / 原始错误
  - `show-ready.next_actions` 也会直接提示：
    - 等待 rate-limit window
    - 重跑 `data run live-foundation` 或 `runner-once --dry-run-side-effects`
    - 把 `oracle_prices_table` 视为临时 fail-open fallback

当前真实 `sol + direct` dry-run 还有一个很重要的状态变化：

- 之前的真实 blocker 曾经是 `quote_inputs_missing`
- 现在最新真实 blocker 已经前移到策略阈值层：
  - `entry_price_max`
  - `net_edge_below_quote_threshold`
  - `roi_net_below_threshold`

这说明当前 live 主线已经走过：

- gateway build
- account sync
- quote market 选择

现在卡住的是“市场价格是否仍值得做”，不是“基础设施是否打不通”。

在 `2026-03-20 14:28 UTC` 左右再次执行 `show-ready --market sol --profile deep_otm --adapter direct` 时，返回同时表现为：

- `status=not_ready`
- `primary_blocker=decision_not_accept`
- `operator_smoke_summary.status=operational`
- `foundation_status=ok_with_errors`
- `foundation_issue_codes=["oracle_direct_rate_limited"]`

这说明当前 live 主线已经能把“策略 no-trade”和“foundation 降级 warning”同时清楚地暴露出来，而不是再把所有 `not_ready` 都混成交易接入故障。

### 2.6 路径与文档收敛

这轮收口还额外完成了 3 件重要的“架构清理”：

- 根文档已经明确 `v2` 是唯一重构主线
- `apps/` 已经从“坏掉的伪主线”降级成明确的废弃入口
- `v2` 的 layout 已经把 runtime 路径和 legacy reference 路径拆开，不再混淆

### 2.7 测试状态

截至 2026-03-21，本地执行：

```bash
PYTHONPATH=v2/src pytest -q v2/tests
```

结果为：

- `172 passed, 1 warning`

这说明 `v2` 当前主线是可运行、可验证的，不是纸上架构。

---

## 3. 还没有完成的内容

### 3.1 真实交易基础设施仍然依赖 legacy

这是当前最重要的未完成项。

`v2` 里的真实账户读取、真实下单、真实 redeem 相关逻辑，仍然依赖旧 `live_trading` 的基础设施层。

但当前已经完成第一步收口：

- `account.py` / `actions.py` 不再直接 import `live_trading`
- legacy 依赖已集中到显式过渡层 `live/trading/legacy_adapter.py`

也就是说：

- orchestration 已经在 `v2`
- 真实交易 client / auth / order submit 仍有 legacy 依赖
- 只是 legacy 依赖面已经明显缩小

这代表 `v2` 还没有做到“真正 clean-room runtime”。

### 3.2 Live 收口还差最后一层

按现有技术文档，Live 主线剩余缺口主要是：

- runner 监控 / 风控 / 告警收口
- 更深的 cash/equity 级别 portfolio accounting
- operator 文档 / 验证记录继续贴近真实值班

也就是说，主链路有了，但最后一层运营级收口还不完整。

更具体地说，当前 `Live` 剩余项已经不再是“主路径能不能跑”，也不再是“operator 文档还没跟上”，而是：

- 要不要继续累积更多真实验证记录
- 如确有需要，要不要再补更完整的 cash/equity 账户视图
- 后续只在 operator 输出字段变化时，继续做增量文档同步

### 3.3 Data 域还有缺口

Data 域目前还差：

- 5m canonical oracle / truth tables
- 更细粒度 completeness 规则可继续按需要扩展

这些属于“让数据域更完整、更可审计”的工作。

如果暂时明确排除 `5m`，那 Data 域当前最主要剩余项就只剩：

- 按实际运行反馈继续细化 completeness 规则
  - 当前已经补了一轮：
    - backtest completeness 改成“主线消费优先”
    - `market_catalog_table / chainlink_streams_source` 缺失不再默认阻断当前 backtest 主线

### 3.4 Research 语义一致性还需要更强验证

Research 主链路虽然已经有了，但还应该继续补齐 golden parity 验证，尤其是：

- `ret_from_strike`
- backtest fill / settlement 行为

这里的口径已经进一步收窄：

- `reversal` 不再作为当前主线或后续计划项
- 因此剩余 parity 工作不再包含 `reversal` 语义和 mapping

否则结构虽然新了，但关键业务语义是否完全继承，还缺更强的证据。

### 3.5 Live 边界已基本收口

当前 canonical live 主线应当是：

- `profile=deep_otm`
- `target=direction`
- `cycle=15m`
- `markets=sol,xrp`

当前已经完成第一批边界收紧：

- `execute-latest`
- `runner-once`
- `runner-loop`
- `score-latest`
- `quote-latest`
- `check-latest`
- `decide-latest`
- `execution-simulate`

这些命令当前都会拒绝非 canonical `target`，不再允许 `target=reversal` 混入真实主线或其核心只读判读入口。

这轮又继续收紧了一层：

- `show-config` / `show-layout`
  - 不再笼统自称 canonical 入口
  - 当前明确是 `compatibility inspection` 命令
  - 输出里会显式带：
    - `canonical_live_scope`
    - `cli_boundary`
    - `profile_spec_resolution`
- `show-ready` / `show-latest-runner`
  - CLI 已固定 `target=direction`
  - 不再给值班入口留下 target 歧义

当前口径已经更清楚地拆成两类：

- canonical operator 只读入口：
  - `check-trading-gateway`
  - `show-ready`
  - `show-latest-runner`
- compatibility inspection 入口：
  - `show-config`
  - `show-layout`

这意味着“当前支持的主线”和“对外暴露的能力表面”已经基本对齐，不再靠隐含约定判断。

所以当前这层剩下的已经不是再做一轮边界重设计，也不是再补一轮基础 runbook，而只是：

- 继续在文档里随字段变化保持这套分流口径一致
- 如果未来明确要删除 compatibility inspection，再回来做最后一轮收口

如果只看当前 should 阶段，建议按下面顺序推进：

1. 先补更多真实 dry-run / side-effect 验证记录
   - 价值很高
   - 现在已经比继续细修文档更值得优先积累

2. 有条件时再补更完整的账户总览
   - 重要
   - 但不影响当前 operator / runner 主线收口

3. 文档保持增量同步
   - 基线已对齐
   - 只在 operator 字段或边界变化时更新

### 3.6 如果先不做 5m，当前剩余工作的优先级

如果先明确不做 `5m`，当前剩余工作建议按下面顺序推进：

1. 先补更多真实 dry-run / adapter validation 记录
   - direct / legacy 只读 probe 已通
   - 但真实验证记录仍值得继续积累

2. 如确有需要，再补 cash/equity 级别账户总览
   - 当前 `capital_usage_summary` 已够值班分流
   - 但还不是完整账户视图

3. 文档按字段变化增量同步
   - 当前 runbook / checklist / roadmap 已经对齐
   - 不再单独作为最近第一优先级

4. 最后再进入 `5m` completeness 和 research parity

---

## 4. 现在最合理的后续路线

下面是建议的 4 个阶段。不要并行乱做，按顺序推进。

### Phase A：去 legacy 化交易基础设施

详细技术方案见：

- `v2/docs/PHASE_A_TRADING_INFRA_TECHNICAL_PLAN.md`

目标：

- 让 `v2/live` 的真实 side effect 不再直接 import `live_trading`

当前进度：

- Phase A 已开始实施
- `live/trading/` 基础包已建立
- `account.py` / `actions.py` 已完成第一步去 direct legacy import
- `account.py` / `actions.py` 已支持显式 gateway 注入
- `service.py` / `runner.py` 已支持 gateway 透传
- 关键 live state/action payload 已显式记录 `trading_gateway`
- 相关测试已切到 fake gateway 路径
- `legacy_adapter` 已有独立 request/result 转换测试
- adapter 选择逻辑已建立，`direct_adapter.py` 已接入 open orders / positions / place / cancel / redeem 主路径
- trading env prerequisite 检查与 gateway 构造已进一步集中回 `live/trading/service.py`
- `account.py` / `actions.py` 已改为复用统一 helper，不再各自散落解析 trading env prerequisite
- `live check-trading-gateway` 已建立 adapter health/validation 命令，并支持 `--adapter` 覆盖检查
- `live check-trading-gateway` 当前还能显式输出 capability readiness / blocked_by / 推荐 smoke run 顺序
- 关键交易命令已支持 `--adapter legacy|direct` 覆盖
- direct adapter 关键异常路径测试已补齐第一批
- direct / legacy adapter 已完成一轮真实只读 smoke validation
- Phase B 已开始第一批 live 收口：
  - `decision` 已接入 defense trade-count cap
  - `execution` 已接入 regime stake scale
  - `runner` iteration 已输出显式 `risk_summary`
  - `runner` iteration 已输出显式 `runner_health`
  - `runner` iteration 已输出显式 `risk_alerts`
  - `runner` iteration 已能把 order/account/cancel/redeem 后段异常归并进 alert / blocker 视图
  - `execute-latest` / `runner-once` / `runner-loop` 已拒绝非 canonical `target`
- Phase C 已开始第一批 data completeness 收口：
  - `data show-summary` 已建立
  - 当前可输出核心 canonical source/table 的存在性、行数、主键质量、时间范围、freshness 和 source-table lag
  - 当前 audit 已能显式给出 `stale_issue_datasets` / `dataset_audits` / `alignment_checks`
  - `--write-state` 已额外写出 `latest.manifest.json` / snapshot `manifest.json`
  - 当前已建立第一版 `completeness` / `issues` / `dataset_inventory`
- 当前剩余任务是把这个 adapter 边界继续做实，而不是重新设计大结构

应该做的事：

- 在 `v2/src/pm15min/live/` 下补一个明确的交易接入层
- 把真实 Polymarket 账户、下单、撤单、持仓、redeem 接口都收敛到这层
- 让 `actions.py` / `account.py` 只做 orchestration，不直接知道 legacy client 细节
- 把环境变量解析、认证对象转换、请求对象转换都放到接入层里

完成标志：

- `v2/src/pm15min/` 里不再直接 import `live_trading`，或者只允许一个显式 legacy adapter 过渡层
- 真实 live run 可以只通过 `pm15min` CLI 走完

### Phase B：补齐 Live 收口

目标：

- 让 `v2/live` 从“能跑”变成“可长期托管”

应该做的事：

- 继续扩展 runner 的监控 / 汇总 / 风控状态输出
- 继续收紧 live CLI 的对外边界
- 明确哪些 target/profile 是 live canonical，哪些只是研究/兼容能力
- 保持 operator 文档和 blocker 语义随实现增量同步

完成标志：

- runner 风险状态可读
- 关键拒绝原因有稳定输出
- 非 canonical live 路径不会再和主线混淆

### Phase C：补齐 Data 完整性

目标：

- 让 `v2/data` 成为更完整、更可信的 canonical source

应该做的事：

- 补 5m canonical builder
- 继续扩展更细的数据 completeness 规则
- 按 operator 使用反馈继续细化 manifest / summary / 审计输出

完成标志：

- 关键数据集都有来源、质量、时间范围、新鲜度说明
- 5m / 15m 两个周期的 canonical 输出都齐全

### Phase D：补齐 Research / Backtest 语义验证

目标：

- 证明 `v2` 不只是“结构像”，而是关键业务语义也对

应该做的事：

- 增加 golden parity tests
- 验证 strike anchor 语义
- 验证 backtest fill / settlement 的关键行为

说明：

- `reversal` 已不再作为当前主线或后续计划项
- 因此这里不再继续把 reversal label / mapping 列为 Phase D 目标

完成标志：

- 能明确回答“哪些地方已经和 legacy 行为一致”
- 回归时不靠人工感觉判断策略语义有没有漂移

---

## 5. 什么叫“这次重构真正完成”

满足下面条件，才可以说 `v2` 重构完成：

1. `v2` 是唯一实际运行入口，旧入口只保留历史兼容或彻底归档
2. `v2/live` 不再直接依赖 legacy 交易基础设施
3. `v2/data` 能完整产出 live / research 所需 canonical 数据
4. `v2/research` 的训练、bundle、回测、评估链路稳定
5. `v2/live` 的 runner / guard / execution 收口完整
6. 关键业务语义有 golden tests，而不是只靠“看起来差不多”
7. 文档、CLI、目录、测试都指向同一套事实

在那之前，更准确的说法应当是：

- “v2 主体已建成”
- “剩余工作是迁移收口，不是再次推翻结构”

---

## 6. 当前建议的最近执行顺序

如果只看最近几周，建议按这个顺序做：

1. 先补更多真实 dry-run / adapter validation 记录
2. 如确有需要，再补 cash/equity 级别账户总览
3. 文档随 operator 字段变化增量同步
4. 最后再进入 `5m` data completeness 和 research parity

原因很简单：

- 最近真正还能继续增加确定性的，是更多真实验证记录
- operator 分流文档本轮已经完成基线对齐
- live CLI 边界已经基本对齐 canonical 主线，不再是最近主任务
- `5m` / research parity 仍重要，但不该抢在当前 should 项前面

---

## 7. 不应该再做的事

后续开发里，下面这些事不应该再发生：

- 再引入第二套顶层架构
- 再把 `apps/` 重新扶正
- 再让 `v2` 混回旧 `data/markets` 运行态
- 再把 live / research / data 的边界揉回一个大脚本
- 再靠目录名或 mtime 猜当前 active 模型

如果未来有人对结构有疑问，默认答案应该是：

- 看 `v2/src/pm15min/`
- 看 `v2/docs/`
- 看这份路线图
