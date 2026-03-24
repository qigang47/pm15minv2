# V2 全量代码对比审计（2026-03-23）

## 1. 审计范围

本次对比的“旧代码”范围：

- `src/`
- `live_trading/`
- `poly_eval/`
- `apps/`
- `scripts/data/`

本次对比的“重构代码”范围：

- `v2/src/pm15min/`
- `v2/tests/`

本次**不把运行产物**当成源码对比对象：

- `v2/data/`
- `v2/var/`
- `v2/research/active_bundles/`
- 各类 `joblib/parquet/json` 产物

## 2. 本次实际验证

我在本地重新执行了两套测试，而不是沿用旧文档里的历史结果：

```bash
PYTHONPATH=v2/src pytest -q v2/tests
pytest -q tests
```

结果：

- `PYTHONPATH=v2/src pytest -q v2/tests` -> `331 passed, 1 warning`
- `pytest -q tests` -> `46 passed`

结论：

- `v2` 当前主线是可运行、可验证的
- 旧主线当前测试也仍然是绿的
- 但“测试全绿”不等于“100% 无语义差异”，下面会把本质差异单独点出来

## 3. 总结论

一句话判断：

- `v2` 已经不是“只改目录”的重构，而是**可用的 canonical 主线**
- 但它**不是**旧代码的“逐位无差别复制”
- 更准确地说，它是：
  - `data/research/eval` 上大体成功的结构化迁移
  - `live` 上主流程可用、但风险包络没有完全等价迁完

如果你的验收标准是：

- `pm15min` 这条新主线能不能作为现在的主入口继续用？
  - 结论：**可以**

- `v2` 是不是已经和旧 `live_trading` 在所有逻辑、风控、概率保守化、资金管理上完全一模一样？
  - 结论：**不是**

## 4. 最重要的本质差异

下面这些不是“拆文件导致的表面差异”，而是真正会影响行为和流程的点。

### 4.1 Live 概率语义变了：旧版是保守概率，v2 当前是原始 blend 概率

旧版：

- `live_trading/core/signal_engine.py`
- 通过 `src.inference.effective_probability_bounds(...)`
- 把模型输出变成：
  - `p_mean`
  - `p_eff_up = LCB(P(UP))`
  - `p_eff_down = 1 - UCB(P(UP))`

v2 当前：

- `v2/src/pm15min/research/inference/scorer.py`
- `v2/src/pm15min/live/signal/scoring_offsets.py`
- 直接使用 `lgb + lr` 的 blend 概率
- live 决策层读取的是 `p_up / p_down / confidence`
- **没有把 reliability bins 的 LCB/UCB 再带回 live 决策**

这意味着：

- `v2` live 当前比旧版**更接近原始模型概率**
- 如果你旧版依赖“保守概率”来降风险，`v2` 当前不是严格 parity

这是我认为最需要你明确知道的 live 语义差异。

### 4.2 Live 方向 guard 缩窄了：旧版 `ret_3m/ret_15m/ret_30m`，v2 只保留 `ret_30m`

旧版：

- `live_trading/core/decision_guards.py`
- 方向过滤会看：
  - `ret_3m_up_floor / ret_3m_down_ceiling`
  - `ret_15m_up_floor / ret_15m_down_ceiling`
  - `ret_30m_up_floor / ret_30m_down_ceiling`

v2 当前：

- `v2/src/pm15min/live/guards/features.py`
- 只保留：
  - `ret30m_up_floor`
  - `ret30m_down_ceiling`

这意味着：

- 如果你旧 runner 曾依赖 `3m` 或 `15m` 的短期动量过滤，`v2` 当前并没有等价搬过来
- `v2/tests/test_live_guards.py` 也证明现在 canonical guard 就只测 `ret_30m`

### 4.3 Live 全局资金/组合级风控没有完整迁过去

旧版明确存在但 v2 canonical live 中未发现等价实现的项：

- `max_daily_loss`
- `max_open_markets`
- `stop_trading_below_cash_usd`
- `stake_cash_pct`
- `stake_balance_step_threshold_usd`
- `stake_balance_step_usd`
- `stake_balance_base_usd`
- `stake_balance_increment_usd`

旧版这些逻辑主要散在：

- `live_trading/configs/shared.py`
- `live_trading/configs/live.py`
- `live_trading/core/runner.py`
- `live_trading/core/execution_flow.py`

v2 当前 live 侧：

- 主要保留了单笔 stake、regime stake scale、repeat-same-decision、cancel/redeem/retry contract
- **没有旧 runner 那套组合级/现金级停机与动态 stake 管理**

这不影响 `v2` 主线“能跑”，但影响它和旧 live 的**风险包络是否等价**。

### 4.4 旧版全局价格边界 `price_floor/price_cap` 没有完整迁到 v2 live decision

旧版：

- `live_trading/core/trade_logic.py`
- 有 `enforce_price_bounds`
- 同时存在：
  - `price_floor`
  - `price_cap`
  - `entry_price_min`
  - `entry_price_max`

v2 当前：

- `v2/src/pm15min/live/guards/quote.py`
- 保留了：
  - `entry_price_min`
  - `entry_price_max`
  - net edge / roi against quote
- **没有旧版那层独立的全局 `price_floor/price_cap` 过滤**

### 4.5 旧版 `poly_eval_adapter` 影子栈没有进入 v2 canonical live runtime

旧版存在：

- `live_trading/poly_eval_adapter/*`
- `live_trading/core/runner.py` 中有 `build_poly_eval_service(...)`

v2 当前：

- research 里保留了 `poly_eval` 的评估能力
- 但 canonical live runtime 中**没有**旧版那套 `poly_eval_adapter` 实盘影子服务

因此：

- 如果你旧 live 运行依赖那套 shadow gate / shadow log / async service，`v2` 当前不等价

### 4.6 Research 训练链被“收窄”了，不再是旧训练管线的全集

旧版 `src/models/training_pipeline.py` 支持的东西更多：

- `lgb + lr + xgb + catboost`
- 多种 calibration / 温度缩放路径
- 更宽的训练参数面

v2 当前 canonical training：

- `v2/src/pm15min/research/training/*`
- 训练主线只围绕：
  - `LightGBM`
  - `LogisticRegression`
  - OOF
  - inverse-brier blend
  - reliability bins

这意味着：

- 对当前 `deep_otm direction` 主线，v2 是够用的
- 但它不是旧训练管线的“全能力镜像”

### 4.7 Label 语义不是简单复制，而是升级为“结算真值驱动”

旧版 `src/labeling.py`：

- 核心是 `close[t+h] vs close[t]`
- 或 fixed-cycle 的 `cycle_end_close vs cycle_start_close`

v2 `research/labels/*`：

- 核心变成：
  - `truth_15m`
  - `oracle_prices_15m`
  - `cycle boundary join`
  - `label_source / settlement_source / full_truth`

这不是 bug，而是**刻意升级**：

- 对 Polymarket 固定周期市场来说，v2 的标签更接近真实 settlement 语义
- 但它已经不是旧 `labeling.py` 的同一套监督定义

## 5. 逐域对比结论

### 5.1 入口 / Core / 路径层

| v2 模块 | 旧模块 | 结论 | 说明 |
| --- | --- | --- | --- |
| `v2/src/pm15min/cli.py`, `__main__.py` | `apps/*` | 有意升级 | `pm15min` 成为唯一 canonical CLI，`apps/*` 只保留 deprecated shim |
| `v2/src/pm15min/core/layout.py` | `src/data/polymarket_paths.py`, `live_trading/configs/shared.resolve_market_root` 等 | 有意升级 | 明确拆出 `rewrite runtime path` 与 `legacy reference path`，这是正确变化，不是 bug |
| `v2/src/pm15min/core/config.py` | `live_trading/configs/*`, 旧脚本参数拼装 | 有意升级 | `LiveConfig/ResearchConfig/DataConfig` 统一了配置入口，减少了旧代码里配置散落的问题 |
| `apps/_deprecated.py`, `apps/live/cli.py`, `apps/research/cli.py`, `apps/data/cli.py` | 旧 `apps/*` 入口 | 流程改变 | `apps/*` 现在不会继续执行旧逻辑，只负责把人引导到 `pm15min` |

判断：

- 入口层不是做到了“继续兼容执行旧流程”
- 而是明确做成“只保留兼容提示，不再把 `apps/*` 当 runtime”
- 这个变化是对的，但你要把它视为**流程切换**，不是纯 refactor

### 5.2 Data 域

| v2 模块 | 旧模块 | 结论 | 说明 |
| --- | --- | --- | --- |
| `data/cli/*` | `scripts/data/*`, `apps/data/*` | 有意升级 | 原来分散脚本被收口到单 CLI |
| `data/layout/*` | `src/data/polymarket_paths.py` | 有意升级 | 老布局只覆盖局部 Polymarket 路径；v2 是完整 canonical data layout |
| `data/sources/binance_spot.py`, `data/pipelines/binance_klines.py` | `src/data/binance_downloader.py`, `scripts/data/download_recent_klines.py` | 基本等价 + 收口 | 功能一致，写入目标改到 v2 canonical source storage |
| `data/sources/polymarket_gamma.py`, `data/pipelines/market_catalog.py` | `scripts/data/fetch_updown_15m_last_years.py`, `fetch_updown_5m_last_years.py` | 基本等价 + 结构升级 | 市场目录抓取被规范成 canonical market table |
| `data/sources/chainlink_rpc.py`, `data/pipelines/direct_sync.py` | `scripts/data/fetch_chainlink_streams_reports*.py`, `fetch_chainlink_datafeeds_history.py`, `fetch_polymarket_15m_oracle_prices_past_results.py` | 有意升级 | 从“脚本抓取”变成“可重复的 source ingest pipeline” |
| `data/pipelines/oracle_prices.py`, `data/pipelines/truth.py` | `scripts/data/build_polymarket_15m_oracle_prices.py`, `build_polymarket_15m_settlement_truth.py` | 基本等价 | 同样是 build canonical table，但 v2 contract 更清楚 |
| `data/sources/orderbook_provider.py`, `data/pipelines/orderbook_recording.py`, `orderbook_runtime.py`, `orderbook_fleet.py`, `orderbook_recent.py` | `live_trading/infra/orderbook_provider.py`, `scripts/data/record_orderbook_snapshots.py`, `record_orderbook_snapshots_multi.py`, `orderbook_hub.py` | 基本等价 + 收口 | orderbook 录制/读取/索引从“脚本 + runtime 混合”变成 canonical 数据面 |
| `data/pipelines/source_ingest.py` | 旧 `data/markets/*`、CSV/ndjson 直接复用方式 | 新增能力 | 明确把 legacy 数据导入 v2，而不是新代码继续直接吃旧路径 |
| `data/service/*` | 旧零散脚本审计逻辑 | 有意升级 | 数据 freshness / alignment / completeness 现在是显式 audit，旧版没有同样清晰的 service 层 |
| `data/pipelines/foundation_runtime.py` | 无单一等价旧模块 | 新增 canonical runtime | 它把 live 所需的数据刷新链条显式化，是 v2 新增能力 |

判断：

- Data 域整体不是“改名”，而是**从脚本集合升级为 canonical data subsystem**
- 我没有看到会导致主数据逻辑反转的本质问题
- 这里的变化主要是结构升级，不是错误性差异

### 5.3 Research 特征 / 标签 / 数据集

| v2 模块 | 旧模块 | 结论 | 说明 |
| --- | --- | --- | --- |
| `research/features/base.py`, `price.py`, `volume.py`, `cycle.py`, `strike.py`, `cross_asset.py`, `builders.py` | `src/features/feature_engineering.py`, `src/features/strike_moneyness.py` | 大体等价 + 模块化 | 重叠特征公式基本保持，拆成 price/volume/cycle/strike/cross-asset 模块 |
| `research/features/registry.py` | 旧版隐式特征列选择 | 有意升级 | 现在 feature set / schema 是显式 contract |
| `research/labels/frames.py`, `direction.py`, `reversal.py`, `alignment.py`, `sources.py`, `datasets.py` | `src/labeling.py` + 旧 truth/oracle 拼装脚本 | 语义升级 | v2 不再用单纯 future-close label，而是 settlement-aware label frame |
| `research/datasets/feature_frames.py` | 旧 `build_features` + 外部脚本 | 有意升级 | feature frame 现在有 manifest 和输入输出 contract |
| `research/datasets/training_sets.py` | 旧特征/标签临时拼表逻辑 | 有意升级 | alignment、label source、window、offset 全部落成可追踪 dataset |

判断：

- 特征工程层：我没发现重叠指标被故意改坏
- 标签层：v2 是**升级语义**，不是“保留旧监督定义”
- 对 Polymarket 固定周期任务，这个升级方向是对的

### 5.4 Research 训练 / 推理 / bundle

| v2 模块 | 旧模块 | 结论 | 说明 |
| --- | --- | --- | --- |
| `research/training/trainers.py`, `runner.py`, `splits.py`, `weights.py`, `metrics.py`, `calibration.py`, `probes.py`, `reports.py` | `src/models/training_pipeline.py`, `src/metrics/reliability.py`, `src/utils/calibration.py` | 主线可用，但能力收窄 | 保留了 LGB/LR、OOF、权重、可靠性分箱；没完整保留 XGB/CatBoost/全部训练开关 |
| `research/inference/scorer.py` | `src/inference.py` | 部分等价 | 仍然做 bundle offset 打分，但 live 不再用旧版 `effective_probability_bounds` 那套保守概率 |
| `research/bundles/*` | `live_trading/configs/profiles.py` 里的 artifact 解析 + 老 artifacts_runs 约定 | 有意升级 | bundle/active selection 现在是显式资源，不再靠 profile 动态猜目录 |

判断：

- 训练链主线没坏
- 但它是“更窄更明确”的 canonical 训练器，不是旧训练器全集
- 如果你要继续复用旧 XGB/CatBoost 路线，v2 当前不是严格 parity

### 5.5 Research evaluation / poly_eval

| v2 模块 | 旧模块 | 结论 | 说明 |
| --- | --- | --- | --- |
| `research/evaluation/methods/control_variate.py` | `poly_eval/control_variate.py` | 高度等价 | 主要是 namespace 迁移 |
| `research/evaluation/methods/copula_risk.py` | `poly_eval/copula_risk.py` | 高度等价 | 主要是 namespace 迁移 |
| `research/evaluation/methods/copulas.py` | `poly_eval/copulas.py` | 高度等价 | 主要是 namespace 迁移 |
| `research/evaluation/methods/decision.py` | `poly_eval/decision.py`, `poly_eval/types.py` | 基本等价 | `TakerDecision` 被内聚回模块，算法本身未见本质改动 |
| `research/evaluation/methods/pipeline.py` | `poly_eval/pipeline.py` | 基本等价 | import 路径改到 v2 methods，主计算链一致 |
| `research/evaluation/methods/production_stack.py` | `poly_eval/production_stack.py` | 基本等价 | 以 namespace/typing 调整为主 |
| `research/evaluation/methods/time_slices.py` | `poly_eval/time_slices.py` | 基本等价 | v2 版本更偏 method 层，绘图边界更干净 |
| `research/evaluation/methods/binary_metrics.py` | `poly_eval/brier_score.py` | 轻微重组 | 把 brier / calibration bins 收口到统一二元指标模块 |
| `research/evaluation/methods/probability/*` | `poly_eval/importance_sampling.py`, `mc_convergence.py`, `mc_estimators.py`, `path_models.py`, `types.py` | 拆分迁移 | 主要是文件拆分，不是算法换写 |
| `research/evaluation/methods/smc/particle_filter.py` | `poly_eval/smc.py` | 拆分迁移 | 逻辑保持，路径改变 |
| `research/evaluation/methods/abm/simulation.py` | `poly_eval/abm.py` | 拆分迁移 | 逻辑保持，路径改变 |
| `research/evaluation/poly_eval.py`, `abm_eval.py`, `calibration.py`, `drift.py` | `poly_eval/cli.py` + 旧分析脚本 | 有意升级 | v2 增加了报告层和 run-dir 组织，不是旧 CLI 直接复制 |

判断：

- `poly_eval` 相关算法是 v2 里 parity 最高的一块
- 这里的变化主要是 namespace、模块边界、报告封装，不是核心数值逻辑被改写

### 5.6 Research backtests / experiments

| v2 模块 | 旧模块 | 结论 | 说明 |
| --- | --- | --- | --- |
| `research/backtests/decision_engine_parity.py`, `guard_parity.py`, `live_state_parity.py`, `regime_parity.py`, `retry_contract.py` | `live_trading/core/trade_logic.py`, `decision_guards.py`, `execution_flow.py`, `infra/regime_controller.py` | 有意升级 | 这批代码不是旧版就存在的独立模块，而是把旧 live 语义抽出来做回放 parity |
| `research/backtests/depth_replay.py`, `orderbook_surface.py`, `fills.py`, `settlement.py`, `reports.py` | `scripts/analysis/backtest_deep_otm_orderbook.py`, `backtest_deep_otm_hybrid_orderbook.py` 等 | 有意升级 | 从分析脚本提升成可复用 backtest engine |
| `research/experiments/*` | `scripts/analysis/alpha_experiment_framework.py` 及相关脚本 | 有意升级 | experiment orchestration/report/cache 现在是显式子系统 |

判断：

- 这块不是“从旧代码照搬”
- 而是把旧分析/回放经验结构化后重新落成
- 从工程质量看是正向变化

### 5.7 Live 信号 / 决策 / quote / execution

| v2 模块 | 旧模块 | 结论 | 说明 |
| --- | --- | --- | --- |
| `live/signal/scoring.py`, `scoring_bundle.py`, `scoring_offsets.py` | `live_trading/core/signal_engine.py`, `src/inference.py` | 主链可用，但非严格 parity | bundle 解析和 offset 打分流程成立，但没有旧 live 的保守概率 `p_eff` 语义 |
| `live/signal/decision.py` | `live_trading/core/trade_logic.py`, `decision_guards.py` | 主链可用，但 guard 集合收窄 | 保留了阈值/quote/roi/tail-space/regime/liquidity 主路径，去掉了部分旧全局风险层 |
| `live/quotes/*` | `live_trading/infra/polymarket_client.py`, `infra/orderbook_provider.py` | 基本等价 + 明确化 | quote row/snapshot/orderbook 组装逻辑被拆清楚，contract 更稳定 |
| `live/execution/*` | `live_trading/core/execution_flow.py` | 基本等价 + 显式化 | depth plan、repriced guard、retry/cancel/redeem contract 都被显式建模 |

判断：

- live 的 signal -> quote -> decision -> execution 主顺序没丢
- 但 live 决策输入概率和 guard 范围都与旧版不完全一致

### 5.8 Live runner / state / operator

| v2 模块 | 旧模块 | 结论 | 说明 |
| --- | --- | --- | --- |
| `live/runner/*`, `live/service/*` | `live_trading/core/runner.py` | 主流程可用，但不是 monolith parity clone | 旧 runner 的单体状态机被拆成 snapshot pipeline：`foundation -> liquidity -> decision -> execution -> side effects` |
| `live/account/*` | 旧 runner 内嵌 open-orders / positions 逻辑 | 有意升级 | 抽成显式 state + summary + persistence |
| `live/actions/*` | 旧 runner 下单 / cancel / redeem side-effect 逻辑 | 基本等价 + 显式化 | 真正 side effect 入口被 contract 化 |
| `live/readiness/*`, `live/operator/*`, `live/capital_usage/*`, `live/gateway/*` | 旧 `runtime_logging.py` + runner 里的杂糅 operator 输出 | 新增能力 | v2 的 operator/readiness/gateway 检查比旧版清楚得多 |

判断：

- runner 这块的主要变化是“可观测性增强 + 状态显式化”
- 不是坏事
- 但旧 runner 内的一些组合级风控没有一起搬过来

### 5.9 Live liquidity / regime / oracle / trading infra

| v2 模块 | 旧模块 | 结论 | 说明 |
| --- | --- | --- | --- |
| `live/liquidity/*` | `live_trading/infra/liquidity_guard.py` | 基本等价 + 状态显式化 | guard 目的保持一致，v2 有更明确的 snapshot / persistence / policy 分层 |
| `live/regime/*` | `live_trading/infra/regime_controller.py` | 高度等价 | 状态机思想保持，v2 多了持久化与 backtest parity 对齐 |
| `live/oracle/strike_runtime.py`, `strike_cache.py` | `live_trading/oracle/strike_provider.py`, `strike_cache.py` | 部分等价 | 第一层 fallback 语义保留，但旧 RTDS/synthetic/exchange feed 整套没有完整进入 canonical live runtime |
| `live/trading/legacy_adapter.py` | `live_trading.infra.polymarket_client`, `live_trading.runners.auto_redeem` | 明确桥接 | legacy 依赖面被压缩到显式 adapter |
| `live/trading/direct_adapter.py`, `redeem_relayer.py`, `auth.py`, `service.py` | 旧 `PolymarketTrader` + `auto_redeem.py` | 有意升级 | 默认适配器已经转向 `direct`，这不是简单重命名 |

判断：

- 交易基础设施层比旧版更清楚
- 但“默认适配器 = direct”本身就是流程变化
- legacy 仍可用，但被隔离成过渡层

## 6. 哪些部分我认为已经足够放心

下面这些我认为可以判成“重构后主语义成立，可以继续用”：

- `poly_eval -> v2/research/evaluation/methods` 迁移
- data canonical layout / source ingest / build / export
- research feature frame / label frame / training set / bundle / active registry 主链
- live quote / execution / cancel / redeem contract 化
- live runner 的状态落盘、operator/readiness/gateway 检查
- `apps/*` 到 `pm15min` 的主线收敛

## 7. 哪些部分我不建议你直接当成“已经和旧版完全等价”

下面这些如果你要追求“旧 live 行为完全复制”，还需要继续补：

1. 把 live 的 `reliability bins -> conservative p_eff` 语义补回去，或明确接受“v2 改成 raw blend 概率”的风险偏好变化。
2. 把 `ret_3m / ret_15m` 方向 guard 是否恢复，做一个明确决策。
3. 决定是否恢复旧版 `price_floor / price_cap`。
4. 决定是否恢复组合级风控：
   - `max_daily_loss`
   - `max_open_markets`
   - `stop_trading_below_cash_usd`
   - cash-based dynamic stake
5. 决定旧 `poly_eval_adapter` 是否彻底废弃，还是要迁入 canonical live。

## 8. 最终判断

最终判断分两层：

### 8.1 作为当前 canonical v2 主线

结论：

- **可以用**

理由：

- `v2` 全量测试通过
- old tests 也通过
- data / research / evaluation / live 主流程都已经成体系
- 没看到“重构后明显跑不通”或“主流程断裂”的证据

### 8.2 作为旧 live 全功能等价替身

结论：

- **还不能说 100% 等价**

主要差异集中在：

- live 概率保守化
- live guard 集合收窄
- 组合级资金风控缺口
- old poly_eval shadow runtime 缺口

所以更准确的验收结论是：

- `v2` 已经是**可用的新主线**
- 但它目前是“**主链可用 + 风险包络尚未完全对齐旧 live**”

