# Poly Eval 与 Legacy Scripts 迁移专项技术方案

这份文档是对 `v2/docs/RESEARCH_TECHNICAL_PLAN.md` 的专项细化。

它只回答一个问题：

- `poly_eval/` 和 `scripts/` 里真正有业务价值的能力，应该如何系统迁进 `v2/`

这里说的“迁移”不是：

- 把旧脚本原样复制到 `v2/scripts/`

而是：

- 把业务语义拆出来
- 放进 `v2/src/pm15min/{research,data,live}/`
- 用统一的 manifest / layout / CLI 接起来
- 最后把 legacy 脚本降级成 thin wrapper、一次性 importer，或明确废弃

如果本文件和 `v2/docs/RESEARCH_TECHNICAL_PLAN.md` 冲突，以本文件作为 `poly_eval + scripts` 迁移执行口径。

---

## 1. 当前判断

### 1.1 当前已经有的 v2 基座

`v2` 并不是从零开始。当前已经有这些正式对象：

- `research feature-frame / label-frame / training-set`
- `research training-run / model-bundle / backtest-run / experiment-run`
- `research evaluation calibration / drift / poly_eval`
- 统一路径：
  - `v2/research/...`
  - `v2/var/research/...`
- 统一命令入口：
  - `PYTHONPATH=v2/src python -m pm15min research ...`

对应实现已经在：

- `v2/src/pm15min/research/cli_parser.py`
- `v2/src/pm15min/research/layout.py`
- `v2/src/pm15min/research/training/runner.py`
- `v2/src/pm15min/research/backtests/engine.py`
- `v2/src/pm15min/research/experiments/runner.py`
- `v2/src/pm15min/research/evaluation/poly_eval.py`

### 1.2 当前真正缺的不是“有没有目录”，而是“语义迁移深度不够”

当前 v2 research 更像是：

- 已经有 package-first 的 canonical shape
- 已经跑通了 training / backtest / experiments 主链
- Phase B 的 backtest / experiment parity 已经进入可用状态
- 但 legacy 的最深业务语义还没有完整迁入

更具体地说，当前最明显的 4 个缺口是：

1. `training`
   - 结构已经有了
   - 但 dataset / target / pruning / calibration / bundle 语义仍明显偏浅

2. `backtest`
   - 当前已经不是最小 scorer
   - 已经接进 raw depth multi-snapshot replay、legacy 初次 snapshot repriced quote surface、partial fill 第一刀与 DecisionEngine 级选边第一刀
   - 但还不是 legacy 最后一段 full replay parity

3. `experiments`
   - 当前已经有 group/run/variant/matrix、cross-run shared cache、resume、compare/report/focus、variant compare policy、runtime policy、stake/cap matrix execution，以及 matrix-focused summary / focus surface
   - 这条线已经不再卡在 stake/cap matrix 本身，剩余更多是更细的 legacy compare grammar / suite richness 收尾

4. `poly_eval`
   - 当前 v2 已有 report 入口、第一批 reusable methods package、以及 `deep-otm-demo / smc-demo / copula-risk / stack-demo` 这批正式 evaluate 命令
   - probability / SMC / decision / copula / pipeline / stack / ABM 第一批已经迁入
   - `poly-eval --scope` 现在已经开始收成 canonical router，`abm / deep_otm / smc / copula_risk / production_stack` 已能走同一条入口
   - 剩余 CLI 尾项主要是 alias 进一步弱化成纯兼容层，以及其余更长尾 scope 收尾

也就是说，当前问题不是：

- 没有 `v2/research`

而是：

- `scripts/training/`
- `scripts/analysis/`
- `poly_eval/`

里面很多真正有用的逻辑，还停留在 legacy 形态。

### 1.3 基于当前代码树的真实迁移进度基线

为了避免把“已经有文件”误判成“已经完成迁移”，这里按当前 `v2/src/pm15min/` 的真实实现给一个基线：

| 区域 | 当前 v2 对象 | 当前状态 | 还缺什么 |
| --- | --- | --- | --- |
| training | `research/training/runner.py`、`trainers.py`、`metrics.py` | 已经能跑最小训练链路，包含 OOF、双模型、blend、manifest | 缺 dataset cache / lock、label-source parity、pruning、walk-forward split、sample weight、calibration artifact、bundle-grade metadata |
| backtest | `research/backtests/*` | 已有 replay、canonical quote/orderbook surface、liquidity/regime live-state parity、hybrid variant、raw depth multi-snapshot replay、legacy 初次 snapshot repriced quote surface、DecisionEngine side-selection parity、raw depth partial fill + quote completion 第一刀、profile-driven canonical depth -> quote fallback、depth diagnostics/report | 还缺按 legacy 收口成 `snapshot taker + FAK immediate-retry` 的 execution parity 收尾、Chainlink truth/runtime parity、最后一段 legacy full replay 归位 |
| experiments | `research/experiments/specs.py`、`runner.py`、`orchestration.py`、`cache.py`、`compare_policy.py`、`leaderboard.py`、`reports.py` | 已支持 hierarchy/defaults inheritance、matrix planner、stake/cap matrix execution、in-run feature/label reuse、cross-run training/bundle shared cache、conservative resume、failed_cases 持久化、group/run/focus compare/report builders、matrix summary / best-by-matrix focus、variant compare policy、runtime policy / per-case append-rerun orchestration | 还缺更细的 legacy compare grammar / suite richness 收尾 |
| evaluation / poly_eval | `research/evaluation/calibration.py`、`drift.py`、`poly_eval.py`、`evaluation/methods/*`、`evaluation/poly_eval_scopes.py`、`evaluation/abm_eval.py`、`cli_handlers.py` | calibration / drift / poly_eval report 已开始共用正式 methods package，probability / SMC / decision / copula / pipeline / stack / ABM 第一批已入主线，且 `poly-eval --scope {abm,deep_otm,smc,copula_risk,production_stack}` 与兼容 alias 已接进 `research evaluate` | 还缺 alias 收口与其余长尾 scope |
| live/data 诊断归位 | `data/pipelines/*`、`data/sources/*`、`live/*.py` | data/live 主域已经成形，但诊断入口还没收口 | 缺 `live report/audit/monitor` 与 `data audit/validate` 的正式命令和 package 归位 |

这意味着：

- 本文后面的迁移 phase，大多数不是“优化现有实现”
- 而是把当前只有 minimal skeleton 的对象补成真正可替代 legacy 的正式对象

所以后面所有“完成标准”都必须按 parity 来验，不按“v2 里已经有个同名文件”来验。

---

## 2. 什么叫“有用的脚本”

这次迁移不按“文件存在就迁”。

判断一个 legacy 脚本或模块是否值得迁，必须同时满足下面至少两条：

1. 它承载了明确的业务语义，而不只是环境 glue。
2. 它现在还在服务 `15m + deep_otm + direction/reversal + sol/xrp/btc/eth` 的研究或实盘闭环。
3. 它能被 package-first 形式表达成稳定 API，而不是只能靠一段 shell 临时串起来。
4. 它的输入输出能落进 `v2/data` / `v2/research` / `v2/live` 的 canonical layout。
5. 它值得被测试、被复现、被值班或研究人员长期重复调用。

因此，下面这些默认不属于“应该迁进 v2 的核心能力”：

- GDrive / rclone / 服务器 bootstrap
- 部署脚本
- 一次性清理脚本
- 目录修复脚本
- 只服务历史 layout 的数据搬运脚本
- 单次实验 shell runner，且内部没有独立业务逻辑

这些东西可以保留，但不应该变成 `v2/src/pm15min/...` 的正式域模型。

---

## 3. 迁移总原则

### 3.1 先迁“语义”，不迁“脚本形状”

例如：

- `train_fixed_cycle_models.py`
  - 不应该在 v2 里继续变成一个 1500 行大脚本
  - 应该拆成：
    - dataset builder
    - label builder
    - trainer
    - calibration
    - bundle builder

- `backtest_deep_otm_orderbook.py`
  - 不应该在 v2 里继续扮演“什么都做”的神脚本
  - 应该拆成：
    - replay loader
    - scoring
    - policy
    - fill simulation
    - settlement
    - report

### 3.2 不允许新 research 逻辑继续依赖顶层 legacy 包

新代码不允许继续长期 import：

- `scripts.*`
- 顶层 `poly_eval.*`
- `src.*`
- `live_trading.*`

允许的方式只有两种：

1. 过渡期 importer / adapter，在边界内引用一次 legacy。
2. 把语义 clean-room 写进 `v2/src/pm15min/...` 后，再由 CLI 调用。

### 3.3 一切输入输出都要落回 canonical layout

迁进 v2 后，产物必须进入：

- `v2/research/feature_frames`
- `v2/research/label_frames`
- `v2/research/training_sets`
- `v2/research/training_runs`
- `v2/research/model_bundles`
- `v2/research/backtests`
- `v2/research/experiments`
- `v2/research/evaluations`

以及：

- `v2/var/research/logs`
- `v2/var/research/cache`

不再继续散落到：

- `reports/`
- `data/markets/<asset>/artifacts_runs/...`
- 各种无 manifest 的 csv/md/json 目录

### 3.4 live 只拿最小可证明稳定的接口

`poly_eval` 里某些能力最终可能给 live 用，但 live 只能接：

- 小接口
- fail-open
- 可缓存
- 不阻塞热路径

所以这轮迁移必须先把 `poly_eval` 收编进 research/evaluation 体系，再决定哪一层暴露给 `v2/live`。

---

## 4. Legacy 资产分层清单

### 4.1 `poly_eval/` 的迁移清单

先明确一个现实状态：

- `v2/src/pm15min/research/evaluation/poly_eval.py`
  - 现在还只是 trade metrics report writer
  - 不是 legacy `poly_eval/` 方法包的正式迁移版

所以如果只看“v2 里已经有 `poly_eval.py`”，会高估实际进度。

### A. 第一优先级：必须迁

这些模块有明确研究或 live-sidecar 价值，应该优先纳入 v2：

- `poly_eval/brier_score.py`
- `poly_eval/time_slices.py`
- `poly_eval/decision.py`
- `poly_eval/smc.py`
- `poly_eval/copulas.py`
- `poly_eval/copula_risk.py`

原因：

- `brier_score` / `time_slices`
  - 是 calibration / drift 的正式评估能力
- `decision`
  - 是概率到 EV / ROI / taker-make 辅助判断的桥接层
- `smc`
  - 是最有机会进入 live sidecar 的概率状态层
- `copulas` / `copula_risk`
  - 是 regime / cross-asset / tail 风险层的正式候选

### B. 第二优先级：应迁，但不需要先进入主链

- `poly_eval/importance_sampling.py`
- `poly_eval/control_variate.py`
- `poly_eval/mc_estimators.py`
- `poly_eval/path_models.py`
- `poly_eval/events.py`
- `poly_eval/types.py`

原因：

- 它们是 `SMC / MC / decision / risk` 的方法层基础件
- 应该进 v2，但可以先作为 research-only primitive
- 第一轮不要求 live 能消费它们

补充说明：

- `poly_eval/cli.py`
  - 应该被当成“旧交互面参考”
  - 不是应该整包平移的主目标
  - 正确做法是：
    - 先迁方法层
    - 再把常用入口重写进 `pm15min research evaluate ...`

### C. 第三优先级：迁为 demo / sandbox，而不是 core runtime

- `poly_eval/abm.py`
- `poly_eval/pipeline.py`
- `poly_eval/production_stack.py`
- `poly_eval/mc_convergence.py`

这些不该消失，但也不该假装自己是生产核心：

- 它们更像：
  - 研究 demo
  - 教学工具
  - 端到端原理实验

迁入方式应该是：

- `v2/src/pm15min/research/evaluation/poly_eval/demos/*`

而不是直接塞进 live / backtest 主路径。

### D. `poly_eval` 的目标目录

最终不再保留顶层 `poly_eval/` 作为主线。

推荐结构：

```text
v2/src/pm15min/research/evaluation/poly_eval/
  __init__.py
  cli.py
  reports.py

  calibration.py
  time_slices.py
  decision.py

  smc.py
  mc_estimators.py
  importance_sampling.py
  control_variate.py

  copulas.py
  copula_risk.py

  path_models.py
  events.py
  types.py

  demos/
    abm.py
    production_stack.py
    pipeline.py
    mc_convergence.py
```

当前已有的单文件：

- `v2/src/pm15min/research/evaluation/poly_eval.py`

应在迁移过程中重构成 package：

- 保留它的 report 语义
- 但把真正的 `poly_eval` 方法层拆进子包

### E. `poly_eval -> live` 的正确边界

结合当前代码和 legacy 行为复核后的结论很明确：

- 未来 `v2/live` 不应该直接吃整套 `poly_eval`
- 它只应该消费一个很窄的 sidecar 接口
- 当前唯一已经被 legacy 实盘证明过有价值的，是 `SMC-first` 的概率状态层

因此，live 第一阶段只应该先接：

- `posterior_mean_up`
- `posterior_q05_up`
- `posterior_q95_up`
- `posterior_width`
- `ess_ratio`
- `obs_gap_abs`
- `stake_multiplier`
- `block_reason`

而不应该直接接：

- `ABM`
- `production_stack`
- generic MC demo
- plotting / report code
- CSV-oriented CLI helper

另外，legacy sidecar 还暴露了几个必须显式写进 v2 方案的工程原则：

- 必须异步
- 必须 fail-open
- 第一阶段只允许 soft scale，不允许 hard block
- 必须有结果 age gate
  - 不能读取无限久之前的旧缓存
- state key 不能只按 `market_id`
  - 后续至少要带：
    - `market_id`
    - `offset`
    - `profile`
    - `target`
    - `decision slot`

建议的 live sidecar 输入 contract 也应该提前固定：

- `decision_key`
- `state_key`
- `market_slug`
- `market_id`
- `offset`
- `decision_ts`
- `p_lgb`
- `p_lr`
- `p_mean`
- `p_eff_up`
- `p_eff_down`
- `quote_prob_up`
- `quote_prob_down`
- `spread_sum`
- `up_liquidity`
- `down_liquidity`
- `obs_prob_up`
- `obs_source`
- `obs_marker`

---

### 4.2 `scripts/training/` 的迁移清单

### A0. 当前最关键的 training 语义缺口

这部分结合当前代码复核后最明确：当前 v2 training 结构对了，但语义还明显偏浅。

下面这些 legacy 训练语义，当前都还没有在 v2 被正式表示完整：

- dataset cache + file lock
- label source parity
  - `settlement_truth`
  - `streams`
  - `datafeeds`
  - `chainlink_mixed`
  - `oracle_prices`
- strike-anchor / basis feature parity
- reversal target filter parity
  - `reversal_abs_ret_bp`
  - `reversal_side`
  - related sample filtering
- feature pruning parity
  - shared blacklist
  - feature-set-specific drop policy
- richer trainer semantics
  - purge/embargo walk-forward
  - sample weighting
  - class balance / contrarian weight
  - optional XGB/CatBoost
  - calibration
  - reliability bins
  - OOF blend optimization
- OOT / holdout reporting
- bundle parity
  - 不只是模型文件，还要带 calibration / reliability / final probe metadata

### A. 必须迁入 package 的核心脚本

- `scripts/training/train_fixed_cycle_models.py`
- `scripts/training/run_alpha_experiment_train_suite.py`

这是整个 legacy research 的高价值核心：

- 一个承载训练真实语义
- 一个承载多实验编排真实语义

迁移归宿：

- `train_fixed_cycle_models.py`
  - `v2/src/pm15min/research/datasets/*`
  - `v2/src/pm15min/research/labels/*`
  - `v2/src/pm15min/research/training/*`
- `run_alpha_experiment_train_suite.py`
  - `v2/src/pm15min/research/experiments/specs.py`
  - `v2/src/pm15min/research/experiments/runner.py`

建议进一步拆得更明确：

- dataset cache / locking
  - `v2/src/pm15min/research/datasets/cache.py`
- label causal alignment
  - `v2/src/pm15min/research/labels/alignment.py`
- feature pruning
  - `v2/src/pm15min/research/features/pruning.py`
- training splits / weights / calibration / probes
  - `v2/src/pm15min/research/training/splits.py`
  - `v2/src/pm15min/research/training/weights.py`
  - `v2/src/pm15min/research/training/calibration.py`
  - `v2/src/pm15min/research/training/probes.py`
  - `v2/src/pm15min/research/training/reports.py`

### B. 不做一等公民，只保留 thin wrapper 的 shell

- `scripts/training/run_v5_training.sh`
- `scripts/training/run_v5.1_training.sh`
- `scripts/training/run_v6_training.sh`
- `scripts/training/run_tf9_12_training.sh`
- `scripts/training/run_deep_otm_v6.sh`
- `scripts/training/run_sol_stage*_train_backtest.sh`
- `scripts/training/run_xrp_stage*_train_backtest.sh`
- `scripts/training/launch_alpha_search_no_shared_blacklist_retrain.sh`

这些脚本的问题不是“没价值”，而是：

- 价值主要在参数组合和批处理顺序
- 不是底层业务逻辑本身

迁移方式应该是：

1. 先把底层逻辑进 package。
2. 再把这些 shell 改成：
   - 调 `pm15min research train run`
   - 或调 `pm15min research experiment run-suite`
3. 长期把“阶段/矩阵/搜索”的信息收口进 suite spec，而不是继续写 shell matrix。

---

### 4.3 `scripts/analysis/` 的迁移清单

### A. 第一优先级：研究闭环主干

- `scripts/analysis/backtest_deep_otm_orderbook.py`
- `scripts/analysis/backtest_deep_otm_hybrid_orderbook.py`
- `scripts/analysis/alpha_experiment_framework.py`
- `scripts/analysis/run_alpha_experiment_backtests.py`
- `scripts/analysis/run_alpha_experiment_case.py`
- `scripts/analysis/run_alpha_experiment_case_matrix.py`
- `scripts/analysis/run_full_backtest_matrix.py`
- `scripts/analysis/generate_alpha_sparse_suite.py`
- `scripts/analysis/build_alpha_search_tables.py`
- `scripts/analysis/build_alpha_focus_compare.py`
- `scripts/analysis/export_feature_weight_leaderboard.py`

这些脚本共同构成了：

- 回测
- 实验编排
- leaderboard
- alpha 搜索结果整形

这是 research 域必须收口的一组能力。

迁移归宿：

- `backtest_*`
  - `v2/src/pm15min/research/backtests/*`
- `alpha_experiment_framework.py`
  - `v2/src/pm15min/research/experiments/specs.py`
  - `v2/src/pm15min/research/experiments/runner.py`
  - `v2/src/pm15min/research/experiments/leaderboard.py`
- `generate_alpha_sparse_suite.py`
  - `v2/src/pm15min/research/experiments/spec_builders.py`
- `build_alpha_search_tables.py`
  - `v2/src/pm15min/research/experiments/reports.py`
- `build_alpha_focus_compare.py`
  - `v2/src/pm15min/research/experiments/compare.py`
- `export_feature_weight_leaderboard.py`
  - `v2/src/pm15min/research/training/reports.py`

### A0. 当前 backtest 真正缺的不是“命令”，而是“语义”

当前 v2 backtest 仍然明显弱于 legacy，缺的主要是：

- raw orderbook replay
- fee-aware `p_cap`
- fill curve / partial fill
- live-like `DecisionEngine` parity
- post-decision guard parity
- regime override parity
- liquidity proxy parity
- Chainlink truth refresh / load parity
- richer reject taxonomy
- hybrid primary/secondary model fallback

换句话说，当前 v2 backtest 还更像：

- `score bundle`
- `merge labels`
- `argmax(p_up, p_down)`
- `stake = 1.0`
- `pnl = +/-1.0`

因此这一组脚本不该直接被“报告层”吞掉，而应该先拆成：

- `research/backtests/replay_loader.py`
- `research/backtests/fills.py`
- `research/backtests/policy.py`
- `research/backtests/guard_parity.py`
- `research/backtests/settlement.py`
- `research/backtests/hybrid.py`
- `research/backtests/reports.py`

### A1. 当前 experiment 真正缺的是 suite richness 和 matrix runtime

当前 v2 experiments 也还是最小版，缺的不是“能不能跑 suite”，而是：

- market/group/run hierarchy
- defaults inheritance
- backtest variants
- tags / notes
- oracle CSV mapping
- sparse suite generation
- cap/stake matrix execution
- shared-cache bundle reuse
- resume / rerun-errors
- per-case result JSON / summary append

因此这一组脚本不能只迁成一个更大的 `runner.py`，而应该拆成：

- `research/experiments/specs.py`
- `research/experiments/planner.py`
- `research/experiments/case_runner.py`
- `research/experiments/cache.py`
- `research/experiments/spec_builders.py`
- `research/experiments/runner.py`

### A2. report migration 不能抢在 backtest parity 前面

这也是结合当前代码复核后最关键的提醒：

- 如果先迁 report
- 但 backtest 还是当前 minimal scorer
- 那么你只是把错误语义更漂亮地输出了

所以：

- `build_alpha_search_tables.py`
- `build_alpha_focus_compare.py`
- `export_feature_weight_leaderboard.py`

必须排在：

- backtest parity
- experiment matrix/runtime parity

之后。

### B. 第二优先级：诊断脚本，需要分流到 research 或 live

- `scripts/analysis/analyze_daily_loss.py`
- `scripts/analysis/analyze_factor_pnl.py`
- `scripts/analysis/analyze_risk_rejection.py`
- `scripts/analysis/summarize_live_trades.py`
- `scripts/analysis/run_poly_eval_trade_drift.py`

这些脚本不应该再混在一个 `analysis/` 目录里。

应该按对象分流：

- 研究产物诊断
  - `analyze_factor_pnl.py`
  - `run_poly_eval_trade_drift.py`
  - 归到：
    - `v2/src/pm15min/research/evaluation/*`

- 实盘结果诊断
  - `analyze_daily_loss.py`
  - `analyze_risk_rejection.py`
  - `summarize_live_trades.py`
  - 归到：
    - `v2/src/pm15min/live/reports/*`
    - 或 `v2/src/pm15min/live/analysis/*`

### C. 第三优先级：更像 data audit，不放 research 主线

- `scripts/analysis/align_market_streams_datafeeds.py`
- `scripts/analysis/compare_chainlink_vs_binance.py`
- `scripts/analysis/compare_streams_datafeeds_binance.py`
- `scripts/analysis/build_polymarket_5m_reference_dataset.py`
- `scripts/analysis/classify_artifact_runs.py`
- `scripts/analysis/organize_artifact_runs.py`
- `scripts/analysis/organize_market_training_inventory.py`

这些文件虽然有用，但语义上更接近：

- 数据质量审计
- 历史资产整理
- reference dataset importer

应优先迁去：

- `v2/src/pm15min/data/audits/*`
- `v2/src/pm15min/research/importers/*`
- `v2/src/pm15min/research/inventory/*`

而不是继续当 research 主闭环的一部分。

---

### 4.4 `scripts/tools/`、`scripts/oracle/`、`scripts/monitor/`、`scripts/data/` 的处理

### A. 不做 1:1 平移

以下目录不应整体复制进 v2：

- `scripts/tools/`
- `scripts/oracle/`
- `scripts/monitor/`
- `scripts/data/`

原因很简单：

- 这里面混合了：
  - 数据导入
  - layout 修复
  - 一次性清理
  - 运维监控
  - 历史格式转换

它们不是一个 domain。

### B. 真正值得迁的少数能力

应该迁的只有这些：

- `scripts/tools/check_trades_vs_orderbook.py`
  - 迁去 `v2/src/pm15min/live/audits/orderbook_trade_alignment.py`
- `scripts/tools/calculate_settled_performance.py`
  - 迁去 `v2/src/pm15min/live/reports/settled_performance.py`
- `scripts/tools/run_alignment_report.py`
  - 迁去 `v2/src/pm15min/live/reports/alignment_report.py`
- `scripts/oracle/validate_synthetic_oracle.py`
  - 迁去 `v2/src/pm15min/data/validators/oracle_validation.py`
- `scripts/monitor/pnl_monitor.py`
  - 迁去 `v2/src/pm15min/live/monitor/pnl_monitor.py`
- `scripts/monitor/live_stop_signal_monitor.py`
  - 迁去 `v2/src/pm15min/live/monitor/stop_signal_monitor.py`
- `scripts/data/orderbook_hub.py`
  - 优先并入现有：
    - `v2/src/pm15min/data/pipelines/orderbook_fleet.py`
    - `v2/src/pm15min/data/sources/orderbook_provider.py`
  - 只有当职责明确独立时，才新增 `v2/src/pm15min/data/runtime/orderbook_hub.py`

### C. 保留为 admin / importer 的脚本

这些可以继续留在 `scripts/`，但不作为 v2 主线：

- `scripts/tools/cleanup_*`
- `scripts/tools/convert_predexon_orderbooks_to_daily_depth.py`
- `scripts/tools/merge_orderbook_depth_files.py`
- `scripts/tools/fix_signal_debug_header.py`
- 大多数 `scripts/data/fetch_*`
- 大多数 `scripts/data/build_*`

它们的定位应该是：

- admin
- importer
- one-shot repair

而不是 canonical runtime。

更具体地说，下面这些目前更适合保留为 importer / reconciliation，而不是 package-first 主线：

- `scripts/data/fetch_chainlink_datafeeds_history.py`
- `scripts/data/fetch_chainlink_streams_reports.py`
- `scripts/data/fetch_chainlink_streams_reports_from_registry.py`
- `scripts/data/fetch_polymarket_15m_oracle_prices_past_results.py`
- `scripts/data/fetch_polymarket_15m_oracle_prices_web.py`
- `scripts/data/build_polymarket_15m_oracle_prices.py`
- `scripts/data/build_polymarket_15m_settlement_truth.py`
- `scripts/data/fetch_polymarket_stream_15m_events.py`
- `scripts/data/fetch_polymarket_stream_5m_events.py`
- `scripts/data/fetch_updown_15m_last_years.py`
- `scripts/data/fetch_updown_5m_last_years.py`
- `scripts/tools/convert_predexon_orderbooks_to_daily_depth.py`
- `scripts/tools/merge_orderbook_depth_files.py`

### 4.5 按终态分类的 legacy 处置矩阵

从执行角度看，legacy 资产最后只允许落到 4 种终态之一：

1. `core package`
2. `thin wrapper`
3. `importer/admin`
4. `deprecated`

如果一个 legacy 文件最后不能明确归到这 4 类之一，就说明迁移边界还没想清楚，不应该开工。

| legacy 资产 | 代表文件 | 目标归宿 | 终态 | 说明 |
| --- | --- | --- | --- | --- |
| `poly_eval` 方法层 | `brier_score.py`、`time_slices.py`、`decision.py`、`smc.py`、`copulas.py`、`copula_risk.py` | `v2/src/pm15min/research/evaluation/poly_eval/*` | `core package` | 正式 research 方法层 |
| `poly_eval` demo 层 | `abm.py`、`pipeline.py`、`production_stack.py`、`mc_convergence.py` | `v2/src/pm15min/research/evaluation/poly_eval/demos/*` | `core package` | 不是 live/runtime，但仍属于可维护 demo |
| training 主语义 | `train_fixed_cycle_models.py` | `v2/src/pm15min/research/datasets/*`、`labels/*`、`training/*` | `core package` | 这是 training 迁移主战场 |
| training shell 批处理 | `run_v5*.sh`、`run_v6*.sh`、`run_tf9_12_training.sh`、`run_*stage*_train_backtest.sh` | `scripts/training/*.sh -> pm15min research ...` | `thin wrapper` | 只保留参数矩阵与批处理顺序 |
| backtest 主语义 | `backtest_deep_otm_orderbook.py`、`backtest_deep_otm_hybrid_orderbook.py` | `v2/src/pm15min/research/backtests/*` | `core package` | 必须先补 replay / fills / guards |
| experiment 编排 | `alpha_experiment_framework.py`、`run_alpha_experiment_*.py`、`run_full_backtest_matrix.py`、`generate_alpha_sparse_suite.py` | `v2/src/pm15min/research/experiments/*` | `core package` | suite schema、planner、runner、leaderboard 都在这里收口 |
| research 结果整形 / 对比 | `build_alpha_search_tables.py`、`build_alpha_focus_compare.py`、`export_feature_weight_leaderboard.py` | `v2/src/pm15min/research/experiments/*`、`training/reports.py` | `core package` | 但必须晚于 backtest / experiment parity |
| research 诊断 | `analyze_factor_pnl.py`、`run_poly_eval_trade_drift.py` | `v2/src/pm15min/research/evaluation/*` | `core package` | 统一变成 evaluate/report surface |
| live 诊断 | `analyze_daily_loss.py`、`analyze_risk_rejection.py`、`summarize_live_trades.py`、`pnl_monitor.py` | `v2/src/pm15min/live/*` | `core package` | 统一走 `pm15min live report/audit/monitor ...` |
| data 对账 / importer | 大多数 `scripts/data/fetch_*`、`build_*`，以及 `validate_synthetic_oracle.py` | `v2/src/pm15min/data/*` 或继续留在 `scripts/` | `importer/admin` | 是否 package 化，要看是否进入 canonical runtime |
| 运维 / 部署 / 清理 | `scripts/entrypoints/*`、`scripts/setup/*`、`scripts/tools/cleanup_*` | 继续留在 `scripts/` | `deprecated` 或 `importer/admin` | 不是 v2 主域能力，不做 package-first 主线 |

---

## 5. v2 目标结构

迁完后，建议结构如下：

```text
v2/src/pm15min/
  data/
    audits/
    validators/
    importers/

  research/
    datasets/
    labels/
    training/
    bundles/
    inference/
    backtests/
    experiments/
    evaluation/
      calibration.py
      drift.py
      poly_eval/

  live/
    reports/
    audits/
    monitor/
    poly_eval/
```

其中：

- `research/evaluation/poly_eval/`
  - 放离线评估与方法层
- `live/poly_eval/`
  - 只放被 live sidecar 需要的最小适配层
  - 不直接复制整个 `poly_eval/`

换句话说：

- `poly_eval` 先收编进 research
- 然后再从 research 向 live 暴露一个很窄的接口

而不是直接把整包塞进 `v2/live`

这里要补一个当前代码树相关的约束：

- 上面的结构图表达的是“语义归位”，不是要求机械地新增深层目录
- 对 `live/` 和 `data/`，如果当前 flat facade + helper implementation 的布局更稳定，可以继续沿用
- 关键不是目录深不深，而是：
  - 主逻辑归到正确 domain
  - CLI 收口正确
  - manifest / path / ownership 清楚

也就是说：

- 可以不为了“看起来整齐”去强行重排 `v2/src/pm15min/live/*.py`
- 但不能继续让 `scripts/analysis/*.py` 同时承担 live/data/research 三种语义

---

## 6. 详细执行顺序

### 6.1 Phase A：先收 `train_fixed_cycle_models.py`

目标：

- 先把 legacy 训练主程序拆成 package-first 结构

本阶段必须落地：

- feature cache / lock
- label-source parity
- causal alignment
- strike-anchor parity
- reversal filter parity
- pruning parity
- feature build 语义补齐
- label build 语义补齐
- training set schema 固定
- offset 训练语义固定
- calibration / blend / feature schema / metrics 固定

完成标准：

- `pm15min research train run` 能覆盖当前 `train_fixed_cycle_models.py` 的主线能力
- shell 训练脚本只剩参数 wrapper

### 6.2 Phase B：再收 `backtest_deep_otm_orderbook.py`

目标：

- 把 full orderbook replay / fill / settlement 主语义收进 `research/backtests`

本阶段必须落地：

- raw orderbook replay
- fill curve / partial fill
- fee-aware entry / `p_cap`
- live decision parity
- post-decision guard parity
- regime / liquidity proxy parity
- replay loader
- offset replay
- fill policy
- reject reason taxonomy
- settlement / pnl summary
- backtest manifests

完成标准：

- `pm15min research backtest run` 不再只是 minimal scorer
- 能承担当前 Deep OTM 主回测闭环

### 6.3 Phase C：收 alpha experiment framework

目标：

- 把 `train -> bundle -> backtest -> leaderboard` 编排彻底从脚本群迁入 suite runner

本阶段必须落地：

- suite/group/run hierarchy
- variant/default inheritance
- cap/stake matrix runner
- shared-cache reuse
- resume / rerun-errors
- suite spec schema
- matrix planner
- sparse suite generator
- leaderboard / compare / summary tables

当前已经落地：

- suite/group/run hierarchy
- variant/default inheritance
- in-run shared-cache reuse
- 保守 resume
- same `run_label` 重跑时对 missing/failed case 的自然 rerun-errors 语义
- per-case append/update 式 artifacts 持久化
- `failed_cases.parquet/csv` + `compare/summary/report` 输出

当前仍待补：

- cap/stake matrix runner 的正式实验化
- sparse suite generator
- 剩余 legacy experiment orchestration
- 更细的 compare policy / rerun policy 收尾

完成标准：

- 不再依赖：
  - `run_alpha_experiment_case.py`
  - `run_alpha_experiment_case_matrix.py`
  - `run_alpha_experiment_backtests.py`
  - `build_alpha_search_tables.py`

而统一走：

- `pm15min research experiment run-suite ...`

### 6.4 Phase D：收 `poly_eval`

目标：

- 把顶层 `poly_eval/` 变成 `research/evaluation/poly_eval/`

本阶段必须落地：

- calibration / time_slices
- decision
- smc
- copula risk
- reports
- poly_eval method CLI

同时必须遵守：

- 先 research，后 live
- 先方法层，后 sidecar 接口
- live 第一阶段只接 SMC-oriented narrow core

建议按下面子阶段推进：

1. 先迁：
   - `brier_score`
   - `time_slices`
2. 再迁：
   - `smc`
   - `types`
3. 再迁：
   - `decision`
   - `copulas`
   - `copula_risk`
4. 最后才做：
   - v2 live sidecar adapter
   - age gate / state-key / soft-scale contract
   - research evaluate CLI 收口

完成标准：

- 不再需要 `python -m poly_eval ...` 作为主入口
- 统一走：
  - `pm15min research evaluate calibration ...`
  - `pm15min research evaluate drift ...`
  - `pm15min research evaluate poly-eval ...`
- 不会把 legacy `poly_eval/cli.py` 原样复制进 v2

### 6.5 Phase E：分流 live/data 诊断脚本

目标：

- 把 legacy `analysis/monitor/tools` 里的常用诊断脚本，按 domain 归位

本阶段必须落地：

- live reports
- live audits
- live monitors
- data audits
- orderbook_hub package 化

完成标准：

- `scripts/analysis/*.py` 不再混合 research/live/data 语义
- 值班脚本和研究脚本完全分开

### 6.6 Phase F：legacy 退场

目标：

- 让 legacy 入口变成可控的兼容层，而不是默认真入口

最终状态：

- 旧脚本要么：
  - 变 thin wrapper
  - 变 importer
  - 变 deprecated

不能继续保持：

- “真正逻辑还在 legacy，v2 只是壳”

### 6.7 依赖关系与可并行边界

这轮迁移不是所有 phase 都能并行推进，依赖关系必须写清楚：

- `Phase A -> Phase B`
  - backtest 要消费训练产物、bundle metadata、offset 语义
  - 如果 training parity 没补齐，backtest parity 一定会反复返工
- `Phase B -> Phase C`
  - experiment framework 的价值建立在 backtest 是真 replay engine 上
  - 不能拿当前 minimal scorer 去承载 matrix runtime
- `Phase D(method layer)` 可以部分并行
  - `brier_score` / `time_slices` 迁移可以和 `A/B` 并行
  - 因为它们主要依赖评估输入 contract，不依赖 full backtest runtime
- `Phase D(live sidecar adapter)` 不能过早开工
  - 它依赖：
    - `poly_eval` 方法层已经稳定
    - live 决策 contract 稳定
    - state-key / age gate / fail-open 规则已经固定
- `Phase E` 可以在 `A/B/C/D` 的主接口稳定后穿插推进
  - 但不应该在 live/data CLI surface 还没定下来前，先大量搬脚本
- `Phase F` 必须最后执行
  - 只有当 wrapper、importer、canonical CLI 都已经可用时，legacy 才能真正退场

简化成一句话就是：

- 训练和回测决定主闭环
- 实验编排建立在主闭环之上
- `poly_eval` 方法层可以部分并行
- live/data 诊断归位和 legacy 退场都不能抢跑

### 6.8 推荐并行工作包

如果后续真的要用多子 agent 并行实现，建议只按不交叉 write scope 拆。

推荐的第一批并行包：

- `A1 datasets/labels parity`
  - 负责：
    - `research/datasets/cache.py`
    - `research/labels/alignment.py`
    - `research/labels/loaders.py`
    - `research/labels/frames.py`
  - 目标：
    - 补 label-source parity
    - 补 causal alignment
    - 固定 dataset cache / lock contract

- `A2 features/pruning parity`
  - 负责：
    - `research/features/pruning.py`
    - `research/features/strike.py`
    - `research/datasets/feature_frames.py`
  - 目标：
    - 补 strike-anchor parity
    - 补 feature pruning / shared blacklist 语义

- `A3 trainer internals parity`
  - 负责：
    - `research/training/splits.py`
    - `research/training/weights.py`
    - `research/training/calibration.py`
    - `research/training/probes.py`
    - `research/training/trainers.py`
  - 目标：
    - 补 purge/embargo
    - 补 weighting / calibration / reliability bins

- `A4 bundle/report parity`
  - 负责：
    - `research/training/reports.py`
    - `research/bundles/builder.py`
    - `research/bundles/loader.py`
  - 目标：
    - 补 bundle metadata
    - 补 final-model probe / calibration artifacts

推荐的第二批并行包：

- `B1 backtest replay/fills`
  - 负责：
    - `research/backtests/replay_loader.py`
    - `research/backtests/fills.py`
    - `research/backtests/settlement.py`

- `B2 backtest guards/policy`
  - 负责：
    - `research/backtests/policy.py`
    - `research/backtests/guard_parity.py`
    - `research/backtests/hybrid.py`

- `C1 experiment specs/planner`
  - 负责：
    - `research/experiments/specs.py`
    - `research/experiments/planner.py`
    - `research/experiments/spec_builders.py`

- `C2 experiment runtime/cache/reports`
  - 负责：
    - `research/experiments/case_runner.py`
    - `research/experiments/cache.py`
    - `research/experiments/runner.py`
    - `research/experiments/reports.py`
    - `research/experiments/compare.py`

- `D1 poly_eval research methods`
  - 负责：
    - `research/evaluation/poly_eval/*`
  - 目标：
    - 先迁方法层，不碰 live

- `E1 live/data diagnostics`
  - 负责：
    - `live/reports/*`
    - `live/audits/*`
    - `live/monitor/*`
    - `data/runtime/orderbook_hub.py`
    - `data/validators/*`

明确禁止的并行方式：

- 一个 agent 改 `training/trainers.py`，另一个同时改 `training/runner.py` 的同一段契约
- 一个 agent 改 `backtests/engine.py`，另一个同时在里面继续塞 replay/fill/guard 全部逻辑
- 一个 agent先做 live sidecar，另一个还在改 `poly_eval` 方法 contract

原则只有一个：

- 并行可以加速
- 但分层和 write scope 必须先稳定
- 否则只是更快地产生耦合

---

## 7. CLI 收口方案

最终命令应该统一成下面三类：

### 7.1 research 主命令

```bash
PYTHONPATH=v2/src python -m pm15min research build ...
PYTHONPATH=v2/src python -m pm15min research train ...
PYTHONPATH=v2/src python -m pm15min research bundle ...
PYTHONPATH=v2/src python -m pm15min research backtest ...
PYTHONPATH=v2/src python -m pm15min research experiment ...
PYTHONPATH=v2/src python -m pm15min research evaluate ...
```

### 7.2 live 报告/监控命令

```bash
PYTHONPATH=v2/src python -m pm15min live report ...
PYTHONPATH=v2/src python -m pm15min live audit ...
PYTHONPATH=v2/src python -m pm15min live monitor ...
```

### 7.3 data 审计/校验命令

```bash
PYTHONPATH=v2/src python -m pm15min data audit ...
PYTHONPATH=v2/src python -m pm15min data validate ...
```

### 7.4 典型 legacy -> v2 映射

| legacy | target v2 |
| --- | --- |
| `python scripts/training/train_fixed_cycle_models.py ...` | `python -m pm15min research train run ...` |
| `python scripts/analysis/backtest_deep_otm_orderbook.py ...` | `python -m pm15min research backtest run ...` |
| `python scripts/analysis/run_alpha_experiment_backtests.py ...` | `python -m pm15min research experiment run-suite ...` |
| `python scripts/analysis/analyze_factor_pnl.py ...` | `python -m pm15min research evaluate factor-pnl ...` |
| `python scripts/analysis/analyze_risk_rejection.py ...` | `python -m pm15min live report rejection-analysis ...` |
| `python scripts/analysis/summarize_live_trades.py ...` | `python -m pm15min live report summarize-trades ...` |
| `python -m poly_eval eval ...` | `python -m pm15min research evaluate calibration ...` |
| `python -m poly_eval eval-timeslices ...` | `python -m pm15min research evaluate drift ...` |
| `python -m poly_eval copula-risk ...` | `python -m pm15min research evaluate poly-eval --scope copula_risk ...` |
| `python -m poly_eval smc-demo ...` | `python -m pm15min research evaluate poly-eval --scope smc ...` |

---

## 8. 测试与验收标准

### 8.1 迁移验收不是只看“能跑”

每个阶段都必须同时过四类检查：

1. 结构检查
   - 代码已经在 `v2/src/pm15min/...`
   - 不再靠 legacy script 承担主逻辑

2. 路径检查
   - 产物进入 `v2/research/...` 或 `v2/var/...`
   - 不再散落到 legacy `artifacts_runs` / `reports`

3. 语义检查
   - legacy vs v2 在主要 summary / row count / reject taxonomy / metric 上保持可解释一致

4. 测试检查
   - 新增单测
   - 新增 golden sample
   - 新增 CLI smoke

### 8.2 必须补的测试面

- `training` parity tests
- `backtest` parity tests
- `experiment suite` manifest tests
- `poly_eval` numerical property tests
- `live sidecar` fail-open tests
- `report schema` tests

额外必须显式补的两类风险测试：

- `poly_eval live sidecar` state-key 粒度测试
  - 防止只按 `market_id` 缓存导致跨 offset / target 污染
- `backtest minimal scorer` 防误验收测试
  - 防止在没有 raw replay / guard parity / fill parity 的情况下，误把当前 v2 backtest 当成 legacy 等价物

### 8.3 每个 Phase 的硬完成定义

下面这些门槛没有过，就不能对外声称某个 phase “完成”：

| Phase | 不能缺的完成条件 |
| --- | --- |
| A training parity | `pm15min research train run` 能覆盖正式支持的 label source、offset、target、pruning、calibration；training run 产物已经足够直接喂给 bundle builder；legacy shell 不再承载核心训练逻辑 |
| B backtest parity | `pm15min research backtest run` 已经是 raw replay + fill + settlement 语义；reject taxonomy 可解释；输出全部写进 `v2/research/backtests/...`；不会再把 fixed-stake minimal scorer 当成正式回测 |
| C experiment parity | `pm15min research experiment run-suite` 已支持 suite defaults、market/group/run hierarchy、matrix execution、in-run shared cache、resume / rerun-errors、per-case artifact persistence、failed-case capture、leaderboard/compare/report；并且 stake/cap matrix 结果也能进入正式 compare/report surface；不再依赖 legacy experiment 脚本群 |
| D poly_eval parity | `pm15min research evaluate ...` 已覆盖 calibration / drift / poly-eval 主能力，并把已迁入的 deep-otm / SMC / copula-risk / production-stack 能力收成统一的 scope-driven surface；顶层 `python -m poly_eval ...` 不再是主入口；live 只接窄 SMC sidecar，而不是整包引用 |
| E live/data diagnostics parity | 研究诊断、实盘诊断、数据审计都已通过各自 domain CLI 暴露；值班和研究不再共用同一个 legacy `analysis/` 目录 |
| F legacy retirement | 旧脚本要么是 wrapper、要么是 importer/admin、要么明确 deprecated；正式产物不再写入 legacy path；任何新逻辑都不再落到 `scripts/` / 顶层 `poly_eval/` |

---

## 9. 这轮不该做的事

这份迁移方案明确不建议做下面这些坏动作：

### 9.1 不要把整个 `scripts/` 平移到 `v2/scripts/`

这样只会复制混乱，不会减少混乱。

### 9.2 不要把整个 `poly_eval/` 直接 import 进 live

这会把 research 复杂度直接带进热路径。

### 9.3 不要用 shell matrix 继续承载主业务语义

参数矩阵可以保留，但业务语义必须先下沉到 package。

### 9.4 不要为了“迁完”而保留 legacy path 作为新主写路径

所有正式产物都必须写入 v2 canonical layout。

---

## 10. 建议的最近执行顺序

如果按价值和风险排序，我建议接下来按这个顺序做：

1. 先补 `train_fixed_cycle_models.py` 剩余语义到 `v2/research/training`
2. 再补 `backtest_deep_otm_orderbook.py` 的 full replay / fill / settlement
3. 然后把 alpha experiment framework 完整收进 suite runner
4. 再把 `poly_eval` 从单文件 report 扩成正式 package
5. 最后再分流 live/data 的诊断脚本

原因：

- 训练、回测、实验编排是 research 主闭环
- `poly_eval` 很重要，但不能抢在主闭环之前扩大范围
- live/data 诊断值得做，但应该在主闭环收稳后再分流

---

## 11. 2026-03-22 第一批已落地结果

这一轮没有继续停留在文档层，而是已经把 `Phase A` 的第一批并行包落进代码了。

落地范围对应：

- `A1 datasets/labels parity`
- `A2 features/pruning parity`
- `A3 trainer internals parity`
- `A4 bundle/report parity`

### 11.1 A1：datasets / labels parity 第一批已落地

已经落地的对象：

- `v2/src/pm15min/research/labels/sources.py`
- `v2/src/pm15min/research/labels/frames.py`
- `v2/src/pm15min/research/labels/datasets.py`
- `v2/src/pm15min/research/labels/alignment.py`
- `v2/src/pm15min/research/datasets/training_sets.py`
- `v2/src/pm15min/research/_contracts_training.py`

已经补上的语义：

- `label_set -> base_label_set / label_source` build plan
- `truth / settlement_truth / streams / datafeeds / chainlink_mixed / oracle_prices` 的 label-set 归一化入口
- label frame 的 `label_source` 持久化
- feature/label merge 的显式 alignment summary
- training set manifest 中的：
  - `label_source_counts`
  - `aligned_rows`
  - `missing_label_rows`
  - `label_alignment_gap_seconds_*`

这意味着：

- training set 不再只是“拼好一张表”
- 而是开始显式记录“标签来自哪里、对齐是否正确”

但还没完成的部分仍然包括：

- 真正独立的 `streams/datafeeds/chainlink_mixed` canonical label surface
- dataset cache / file lock 的正式 package 化
- 更细的 causal alignment policy

### 11.2 A2：features / pruning parity 第一批已落地

已经落地的对象：

- `v2/src/pm15min/research/features/pruning.py`
- `v2/src/pm15min/research/features/registry.py`
- `v2/src/pm15min/research/features/__init__.py`

已经补上的语义：

- feature-set drop policy
- not-in-feature-set 的显式剔除
- pruning report / dropped reason 持久化
- bundle 可消费的 `allowed_blacklist_columns`

当前达到的效果是：

- training 不再默认把“所有非 meta 列”都当成特征
- 而是开始按 feature-set contract 和 pruning policy 收口

但还没完成的部分仍然包括：

- 更贴近 legacy 的 market-specific shared blacklist 调优
- pruning 和 alpha-search 的更深层联动

### 11.3 A3：trainer internals parity 第一批已落地

已经落地的对象：

- `v2/src/pm15min/research/training/splits.py`
- `v2/src/pm15min/research/training/weights.py`
- `v2/src/pm15min/research/training/calibration.py`
- `v2/src/pm15min/research/training/probes.py`
- `v2/src/pm15min/research/training/reports.py`
- `v2/src/pm15min/research/training/trainers.py`
- `v2/src/pm15min/research/training/runner.py`

已经补上的语义：

- purge / embargo aware split helper
- sample weighting
  - class balance
  - volatility weighting
  - contrarian weighting
- reliability bins 产物
- per-offset `summary / metrics / feature_pruning / probe / report`
- run-level `report.md`

这意味着当前 training run 已经不只是：

- 模型文件 + 一个 `summary.json`

而是开始具备：

- 可解释的 pruning 结果
- calibration diagnostics
- offset-level report surface

但还没完成的部分仍然包括：

- learned calibrator 对象本身
- richer OOT / holdout probes
- XGB / CatBoost / richer blend optimization

### 11.4 A4：bundle / report parity 第一批已落地

已经落地的对象：

- `v2/src/pm15min/research/bundles/builder.py`
- `v2/src/pm15min/research/bundles/loader.py`

已经补上的语义：

- bundle copy optional diagnostics artifacts
- bundle-level `summary.json`
- bundle-level `report.md`
- `allowed_blacklist_columns` 向 bundle config 传播
- bundle manifest metadata 补充 optional artifacts 与 summary path

这意味着 bundle 不再只是：

- 拷几份模型和 `feature_cols.joblib`

而是开始承载：

- training diagnostics 的 deploy-time 子集
- 可读的 bundle summary/report

但还没完成的部分仍然包括：

- backtest / live 对这些新 diagnostics 的正式消费
- 更完整的 calibration / probe contract

### 11.5 本轮验证结果

本轮已经实际补上的测试面：

- `v2/tests/test_research_feature_pruning.py`
- `v2/tests/test_research_training_datasets_parity.py`
- `v2/tests/test_research_training_parity.py`
- `v2/tests/test_research_bundle_parity.py`

并且已经通过：

- `pytest -q v2/tests/test_research_feature_pruning.py v2/tests/test_research_training_datasets_parity.py v2/tests/test_research_training_parity.py v2/tests/test_research_bundle_parity.py`
- `pytest -q v2/tests/test_research_builders.py`
- `pytest -q v2/tests/test_cli.py -k 'research_training_set_build or research_train_run or research_bundle_build or research_backtest_run or research_evaluate_poly_eval'`

所以截至这次更新，最准确的状态不是：

- `Phase A` 还没开始

而是：

- `Phase A` 已经完成第一批高价值迁移
- 但离 full legacy parity 还有明显剩余语义

如果继续往前推，最合理的下一步已经不是继续补文档，而是：

- 收尾 `Phase A` 剩余 training parity
- 然后直接转入 `Phase B backtest parity`

---

## 12. 2026-03-22 第二批已落地结果

这一轮已经开始进入 `Phase B backtest parity`，但口径必须明确：

- 这次落的是“基于当前 canonical 数据面可落地的第一批 backtest parity”
- 不是“已经完成 raw orderbook replay parity”

换句话说，这次的正确表述是：

- `backtest engine` 已经从单文件 minimal scorer，升级成有 replay / policy / fills / settlement / reports 分层的 package-first 结构
- 但还没有进入 legacy `backtest_deep_otm_orderbook.py` 那种 full orderbook + live-guard parity

### 12.1 Phase B 第一批已落地模块

已经落地的对象：

- `v2/src/pm15min/research/backtests/replay_loader.py`
- `v2/src/pm15min/research/backtests/policy.py`
- `v2/src/pm15min/research/backtests/hybrid.py`
- `v2/src/pm15min/research/backtests/fills.py`
- `v2/src/pm15min/research/backtests/settlement.py`
- `v2/src/pm15min/research/backtests/taxonomy.py`
- `v2/src/pm15min/research/backtests/reports.py`
- `v2/src/pm15min/research/backtests/engine.py`

### 12.2 这次真正补上的 backtest 语义

已经补上的能力：

- replay loading / alignment summary
  - score coverage
  - unresolved labels
  - bundle offset availability
  - ready rows
- decision policy layer
  - probability floor
  - probability-gap floor
  - policy reject taxonomy
- hybrid fallback helper
  - primary reject reason 触发 secondary 接管
- canonical fill layer
  - profile-driven canonical depth execution
  - `depth -> quote fallback`
  - target ROI 对应的 `price_cap`
  - regime-driven stake scaling
- settlement / equity / market summary
  - trade-level pnl / roi
  - equity curve
  - market summary
- backtest report surface
  - `summary.json`
  - `report.md`
  - `decisions.parquet`
  - `trades.parquet`
  - `rejects.parquet`

这意味着当前 v2 backtest 已经不再只是：

- `merge labels`
- `argmax(p_up, p_down)`
- `stake = 1.0`
- `pnl = +/-1.0`

而是已经开始具备：

- replay coverage 解释能力
- policy 层 reject taxonomy
- profile-driven canonical depth / quote fill
- bundle/evaluation 可以消费的更完整 backtest 产物

### 12.3 这次明确还没做的部分

这轮故意没有假装完成下面这些更重的 parity：

- raw depth candidate chain 之上的更完整 partial fill engine
- Chainlink truth refresh/runtime parity

原因不是这些不重要，而是：

- 它们已经越过“research backtest 模块内重构”
- 会直接牵扯 canonical orderbook / quote / live contract 的进一步收编

所以当前更准确的状态是：

- `Phase B` 已经完成第一批可落地 package 化
- 但距离 legacy full replay parity 还有明显第二批工作

### 12.4 本轮新增验证

本轮新增并通过的 focused tests：

- `v2/tests/test_research_backtest_replay.py`
- `v2/tests/test_research_backtest_fills.py`
- `v2/tests/test_research_backtest_policy.py`
- `v2/tests/test_research_backtest_parity.py`

并且已经通过：

- `pytest -q v2/tests/test_research_backtest_replay.py v2/tests/test_research_backtest_fills.py v2/tests/test_research_backtest_policy.py v2/tests/test_research_backtest_parity.py`
- `pytest -q v2/tests/test_research_builders.py -k 'run_research_backtest_from_bundle or run_experiment_suite or run_evaluations_from_backtest'`
- `pytest -q v2/tests/test_cli.py -k 'research_backtest_run or research_evaluate_poly_eval'`

### 12.5 最合理的下一步

如果继续往前推，最合理的下一步不再是回头补 Phase A 文档，而是继续把 `Phase B` 往真正 legacy parity 推：

1. 把 current proxy fill layer 接到更真实的 quote/orderbook canonical surface
2. 把 `liquidity proxy -> regime state -> live guard` 这条链补成 research 可复用纯接口
3. 再把 hybrid primary/secondary runtime 扩成 experiment 可消费的正式 backtest variant

### 12.6 继续推进后的当前状态

上面这 3 件事，这一轮已经各自往前推进了一步。

#### 已落地的新模块

- `v2/src/pm15min/research/backtests/orderbook_surface.py`
- `v2/src/pm15min/research/backtests/guard_parity.py`
- `v2/src/pm15min/research/backtests/liquidity_proxy.py`
- `v2/src/pm15min/research/backtests/regime_parity.py`
- `v2/src/pm15min/research/backtests/live_state_parity.py`

并且下面这些现有模块已经继续收口：

- `v2/src/pm15min/research/backtests/fills.py`
- `v2/src/pm15min/research/backtests/engine.py`
- `v2/src/pm15min/research/backtests/reports.py`
- `v2/src/pm15min/research/_contracts_runs.py`
- `v2/src/pm15min/research/experiments/specs.py`
- `v2/src/pm15min/research/experiments/runner.py`

#### 这轮真正补上的语义

1. `quote/orderbook canonical surface`

- backtest replay 现在会先挂上一层 canonical quote surface
- 它会从：
  - canonical `market_catalog`
  - canonical `orderbook_index`
  读取：
  - `token_up / token_down`
  - `quote_up_ask / quote_down_ask`
  - `quote_*_size_1`
  - `quote_captured_ts_ms_*`
  - `quote_age_ms_*`

这意味着当前 fill planner 已经不再只依赖：

- `p_up / p_down`

而是会优先吃：

- canonical quote/orderbook 字段

2. `live-like guard parity`

- research backtest 现在已经有一层独立的 guard parity adapter
- 它复用了 v2 live 现有的 pure guard contract
- 目前先接入：
  - quote guards
  - liquidity guard state
  - regime guard state
  - trade-count-cap context

并且保持一个重要边界：

- 对“整条 canonical quote surface 尚不可用”的情况仍然 fail-open
- 不会因为没有完整 quote surface 就把现有 backtest 全部打成 reject

3. `hybrid primary/secondary -> experiment variant`

- `BacktestRunSpec` 现在已经支持：
  - `secondary_target`
  - `secondary_bundle_label`
  - `fallback_reasons`
  - `variant_label`
  - `variant_notes`
- experiment suite spec 现在也能声明：
  - `variant_label`
  - `variant_notes`
  - `hybrid_secondary_target`
  - `hybrid_secondary_offsets`
  - `hybrid_fallback_reasons`
- experiment runner 会在需要时自动构建 secondary training run / bundle，然后把 hybrid variant 一路传到 backtest runtime

4. `liquidity proxy module`

- research backtest 现在已经有独立的 `liquidity_proxy.py`
- 它先用 `spot_kline_mirror` 这条最小 pure adapter，对 raw klines 生成：
  - quote-volume ratio/window
  - trades ratio/window
  - soft-fail reason codes
- 这一层已经是 package-first 能复用的正式对象

5. `regime/liquidity parity wiring`

- backtest engine 现在会把：
  - raw klines
  - parity-resolved `LiveProfileSpec`
  - liquidity proxy mode
  - liquidity proxy
  - regime controller
  串成一条独立的 research-side parity 链
- 这条链会把 row-level 的：
  - `liquidity_status/degraded/reason_codes`
  - `regime_state/pressure/reason_codes`
  先挂回 replay / decision frame，再进入 guard parity
- `summary.json` / `report.md` 也开始输出：
  - `liquidity_proxy_enabled`
  - `liquidity_proxy_mode`
  - liquidity available/missing/degraded rows
  - regime state / pressure counts

6. `typed parity contract propagation`

- `BacktestRunSpec` / experiment suite spec 现在已经能 typed 承载：
  - liquidity proxy mode / lookback / baseline
  - regime stake / dir-prob / trade-cap overrides
- 这层现在不只是 metadata 传播
- engine 已经会先 resolve 一次 parity-resolved `LiveProfileSpec`
- 然后把已解析结果下沉进：
  - `live_state_parity`
  - `guard_parity`
  - `fills/runtime`

7. `regime-driven stake scale`

- fills 现在正式支持吃 parity-resolved `profile_spec`
- fill plan 会直接输出：
  - `stake_base`
  - `stake_multiplier`
  - `stake_regime_state`
- 最终 `stake` 会按 live 同语义的 regime multiplier 缩放
- 对应字段也会继续保留到 trade artifacts 里，便于审计

8. `profile-driven depth parameters + depth diagnostics`

- canonical fills 现在不再只吃通用 fill config
- depth path 会优先对齐 parity-resolved `LiveProfileSpec` 里的：
  - `orderbook_max_slippage_bps`
  - `orderbook_min_fill_ratio`
- 同时保留现有的 `depth -> quote fallback` 分层，不在 backtest 里再发明第二套执行路径
- fill artifacts 现在也会直接带出：
  - `depth_status`
  - `depth_reason`
  - `depth_source_path`
  - `depth_fill_ratio`
  - `depth_avg_price / best_price / max_price`
- 这意味着当前 backtest 已经能解释：
  - depth 为什么命中
  - 为什么 fallback 到 quote
  - 以及被 depth block 的直接原因是什么

9. `experiment matrix/runtime parity 第一刀`

- experiment suite spec 现在已经不再只支持最小 `markets: [...]`
- 它已经开始支持：
  - `markets: {...}` mapping 形式
  - `groups -> runs` 层级
  - `backtest_variants` 在 suite / market / group / run 各层展开
  - suite -> market -> group -> run -> variant 的 defaults / parity 继承
- runner 这边也已经不再是“每个 case 全量重跑一遍”：
  - 会给每个展开后的 case 生成稳定 `case_key`
  - 会在同一 experiment run 内复用 feature/label 准备
  - 会按 training key / bundle key 复用 training 和 bundle
  - 会对已有且 `summary.json` 仍存在的 case 做保守 resume
  - 会在每个 case 完成或失败后立刻刷新 artifacts，保证中途崩溃后仍保留部分进度
  - 同一个 `run_label` 重跑时，会自然只补 missing/failed case，而不是把已完成 case 全量重做
- experiment artifacts 现在也开始直接带出：
  - `group_name`
  - `run_name`
  - `tags_json`
  - `training_reused / bundle_reused`
  - `secondary_training_reused / secondary_bundle_reused`
  - `resumed_from_existing`
  - `status / failure_stage / error_type / error_message`

10. `experiment compare/report builders 第一刀`

- experiment run 现在不再只落：
  - `training_runs.parquet`
  - `backtest_runs.parquet`
  - `leaderboard.parquet/csv`
- 它还会继续生成：
  - `failed_cases.parquet`
  - `failed_cases.csv`
  - `compare.parquet`
  - `compare.csv`
  - `summary.json`
  - `report.md`
- 这层 report/compare 直接建立在现有：
  - training rows
  - backtest rows
  - failed case rows
  - leaderboard
  之上，不再回头走 legacy 脚本拼表
- 当前 report 已经能直接回答：
  - 哪些 case 属于哪个 `group/run/variant`
  - 哪些 case 命中了 training/bundle reuse
  - 哪些 case 是从已有结果 resume 回来的
  - 哪些 case 失败了，失败发生在哪个 stage，异常类型/消息是什么
  - 当前 top leaderboard 和 compare 面板分别是什么

11. `raw depth multi-snapshot replay 第一刀`

- backtest 侧现在已经有独立的 `depth_replay.py`
- 它会把 raw depth 按：
  - `decision_ts + offset` 的 legacy key
  - 或 token/time-window fallback
  配对成多 snapshot bucket
- bucket 会保留：
  - `depth_up_record`
  - `depth_down_record`
  - `depth_snapshot_rank`
  - `depth_snapshot_ts_ms`
  - `depth_snapshot_status / reason`
- engine/fills 主线现在会优先消费这批 raw depth candidate
- 只有 raw candidate 不可用时，才继续退回现有 canonical depth -> quote fallback

12. `raw depth partial fill + quote completion 第一刀`

- fills 现在已经不再把 raw depth candidate 只当单 snapshot hit/miss
- 它会沿 candidate chain 累积 partial notional
- 如果 depth 只完成了部分 notional，当前主线会继续允许 quote completion
- 最终 fill model 现在会明确区分：
  - `canonical_depth`
  - `canonical_depth_quote`
  - `canonical_quote`
- summary/report 也会开始单独统计：
  - `depth_quote_completion_rows`

13. `DecisionEngine side-selection parity 第一刀`

- backtest 侧现在已经有独立的 `decision_engine_parity.py`
- 这层会把 live `DecisionEngine` 的窄语义下沉成纯 adapter：
  - side selection
  - rationale
  - reject taxonomy
  - roi-net ranking tie-break
- policy/fill 主线现在已经开始消费：
  - `decision_engine_side`
  - `decision_engine_prob`
  - `decision_engine_probability_gap`
  - `decision_engine_reason`
- 这意味着 backtest 选边已经不再只靠 `argmax(p_up, p_down)`

14. `experiment cross-run shared cache + richer reports 第一刀`

- experiment 现在已经有独立的 `cache.py`
- shared cache 会持久化：
  - prepared datasets
  - training reuse
  - bundle reuse
- runner 现在会先读 shared cache，再用当前 run 的状态覆盖
- 每个 case 刷新 artifacts 时，也会同步刷新 shared cache
- report 这边则已经补上：
  - `groups` / `runs`
  - group summary
  - run summary
  - focus cuts

15. `experiment runtime policy / orchestration 第一刀`

- suite spec 现在已经能 typed 支持：
  - `runtime_policy.completed_cases = resume | rerun`
  - `runtime_policy.failed_cases = rerun | skip`
- runner 现在不再直接把 existing rows / failed rows 逻辑散落在主函数里
- 已经下沉到独立的 `orchestration.py`
- 当前 formal runtime 已能表达：
  - completed case resume
  - completed case forced rerun
  - failed case rerun
  - failed case retained
- 这意味着 legacy `--resume/--rerun-errors` 的核心策略已经开始进入正式 suite/runtime contract

16. `evaluation methods package 第一刀`

- `research/evaluation/methods/*` 已经落地
- calibration / drift / poly_eval report 现在开始共用这套正式 methods package
- 当前第一批已正式迁入：
  - binary metrics / time slices / trade metrics
  - probability estimators
  - SMC particle filter
  - decision helpers
  - copula / copula risk
  - event builders / control variates
  - deep-otm pipeline
  - production stack demo
  - ABM market simulation
- 这意味着 v2 evaluation 不再完全依赖 legacy 顶层 `poly_eval/` 的散模块

17. `experiment matrix summary / focus surface`

- experiment compare frame 现在已经不会在 report 层丢掉：
  - `matrix_parent_run_name`
  - `matrix_stake_label`
  - `stake_usd`
  - `max_notional_usd`
- experiment run artifacts 现在会继续正式输出：
  - `matrix_summary.parquet`
  - `matrix_summary.csv`
- report / summary 现在已经能直接回答：
  - 一个 matrix parent run 下有哪些 `stake_usd_values`
  - 哪个 `matrix_stake_label` 是 best row
  - `Best Variant Per Matrix` 分别是什么
- 这意味着 stake/cap matrix 已经不再只是“能跑”，而是已经成为 compare/report 的正式一等视图

18. `poly-eval --scope canonical router 第一刀`

- `research evaluate poly-eval --scope ...` 现在已经不再只是 backtest report label
- 当前已正式收口的 routed scope：
  - `abm`
  - `deep_otm`
  - `smc`
  - `copula_risk`
  - `production_stack`
- 兼容命令：
  - `deep-otm-demo`
  - `smc-demo`
  - `copula-risk`
  - `stack-demo`
  现在只是 thin alias，底层会走同一条 scope dispatch
- 这意味着 v2 evaluate CLI 已经开始从“多命令并存”收口到“`poly-eval --scope` 统一入口”

19. `raw depth time_turnover 第一刀`

- raw depth partial-fill path 现在已经不再只认识：
  - `price_path`
  - `queue_growth`
- 对“同价位、同可见量、但已经隔了足够长时间”的 raw snapshot，当前会保守记成：
  - `time_turnover`
- 这层不会把同价位静态队列直接无限重放
- 但会允许一个更接近真实市场时间流逝的保守再进度事件
- 对应 diagnostics 已经正式带出：
  - `depth_time_turnover_count`
  - `depth_time_turnover_rows`

21. `legacy FAK refresh parity 开关第一刀`

- 这一步没有把现有 raw depth 主线直接整条翻掉
- 而是先补了一条正式 parity contract：
  - `BacktestParitySpec.raw_depth_fak_refresh_enabled`
- 当前 v2 默认口径已经切回 legacy 风格：
  - parity 未显式配置时，按 `snapshot taker + FAK immediate-retry` 解释
- 如果研究侧确实还要保留旧的“多 snapshot 累积 / quote completion”实验语义：
  - 可以显式把 `raw_depth_fak_refresh_enabled=false`
- 打开后，raw depth candidate chain 会按更接近 legacy 的方式解释：
  - `当前 snapshot` 先算一次可成交量
  - 如果是 `no fill / fill_ratio 不达标 / orderbook_limit_reject` 语义，才继续吃后面的 snapshot，当作一次立即 refresh retry
  - 一旦已经有 partial fill，不再跨 snapshot 累积，也不再对 raw depth 结果做 quote completion
  - 记账口径也回到 legacy 的保守写法：
    - depth 侧 `entry_price` 按 `max_price`
    - 不按跨层平均成交价去美化 fill
- 对应 diagnostics 现在会继续带出：
  - `depth_retry_refresh_count`
  - `depth_retry_refresh_rows`
  - `depth_chain_mode = refresh_retry`

22. `FAK refresh reprice guard 第一刀`

- 这一步已经把 legacy `FAK` refresh 之后最关键的二次校验接回 v2 backtest：
  - `repriced_entry_price_min`
  - `repriced_entry_price_max`
  - `repriced_net_edge_below_threshold`
  - `repriced_roi_below_threshold`
- 当前接法保持一个很窄的边界：
  - 只在 `refresh_retry` 真的发生时触发
  - 不把这层 guard 扩散到所有普通 depth path
- 这意味着 v2 现在已经不再只是：
  - retry 到能成交就放行
- 而是更接近 legacy：
  - retry 到能成交之后，还要再过一次 price-band / net-edge / roi-net 检查

这一步的意义是：

- 先把 v2 拉回 legacy 的主口径：
  - `snapshot taker + FAK immediate-retry`
- 而不是继续默认往“多 snapshot 累积成交”那个方向发散

补充口径必须明确：

- 这不是要把 v2 推成重的 queue simulation
- 回看 legacy `backtest_deep_otm_orderbook.py` 与 live `execution_flow.py` 之后，更准确的目标应该是：
  - `snapshot taker`
  - `respect p_cap / max_slippage / min_fill_ratio`
  - `FAK no-match / orderbook_limit_reject` 时的即时 refresh retry
- 也就是说，`raw depth` 后续该补的是：
  - 轻量 `FAK` execution parity
- 而不是：
  - maker-style queue waiting
  - 多时刻慢速成交仿真

20. `backtest Source Of Truth diagnostics 第一刀`

- backtest summary / report 现在已经会正式输出：
  - `label_sources`
  - `label_source_counts`
  - `settlement_source_counts`
  - `price_to_beat_rows`
  - `final_price_rows`
- 这意味着 research backtest 现在能直接解释：
  - 这轮回测主要吃的是 `settlement_truth`
  - 还是 `streams / datafeeds / chainlink_mixed`
- 这是 Chainlink truth/runtime parity 的第一刀：
  - 先把 source-of-truth 诊断打进 canonical summary/report
  - 再继续推更深的 refresh/runtime contract

23. `legacy 初次 snapshot 决策前 repriced quote parity`

- 这一步把 legacy `backtest_deep_otm_orderbook.py` 最关键的决策前语义接回了 v2 主链：
  - 先吃 `raw depth` 的首个 candidate snapshot
  - 先按 `首档 ask fee + p_cap / roi_threshold / orderbook_max_slippage_bps` 重算 `UP/DOWN` 可成交 price
  - 再把这组 repriced `quote surface` 喂给 `DecisionEngine` 做选边/拒绝
- 这一层现在明确保持 legacy 约束：
  - 只看首个 snapshot，不跨多个 snapshot 混 side
  - 如果首个 snapshot 缺一侧盘口，按 `orderbook_missing` 拒绝
  - 如果首个 snapshot 两侧都过不了 price cap，则按 `orderbook_limit_reject` 拒绝
  - 只有“一侧可成交、另一侧只是 limit reject”时，才允许保留那一侧进入决策
  - 如果该行根本没有 raw depth candidate，就继续保留原来的 research canonical quote 决策口径，不顺手改掉旧实验语义
- 对应 diagnostics 已正式进 backtest summary/report：
  - `decision_quote_raw_depth_rows`
  - `decision_quote_repriced_rows`
  - `decision_quote_limit_reject_rows`
  - `decision_quote_orderbook_missing_rows`

#### 这轮仍然没假装完成的部分

这轮虽然已经不再停留在 pure proxy/minimal scorer，但还没有完成下面这些更重的 parity：

- raw depth 的 `snapshot taker + immediate recheck` parity 目前按这条更简的主线收口：
  - `新快照就是新机会`
  - 当前 candidate chain 已经重新按 `pre_submit orderbook recheck` 解释
  - budget 现在按 `orderbook_fast_retry_max`，不是 `fak_immediate_retry_max`
  - 也已经正式带出 `retry stage / retry exit reason / retry budget / retry trigger reason / snapshot unchanged` 诊断
  - 本轮 retry 不成功就直接放弃，不再把这条 research/backtest 线继续往跨轮 `retry_state / scheduling endgame` 推
- legacy 初次 snapshot 上的 repriced quote / side-selection 已接回主链：
  - 当前缺的已经不再是“决策前有没有先 repriced”
  - 当前剩余更多是 live 侧更重的 runtime contract，而不是要把 research/backtest 继续推成跨轮重试状态机
- `liquidity proxy -> regime controller -> guard parity` 的更深层 offline parity
- Chainlink truth refresh/runtime parity 也只完成了中间一段：
  - label/backtest 现在已经会正式带 `truth_runtime_*` / label runtime 元数据
  - live foundation 也已经把 `streams-rpc -> oracle_prices_table` 这段 recent runtime 刷新接回来了
  - live runtime strike 现在已经有 `openPrice -> streams exact boundary -> local strike cache` 第一刀
  - 当前如果 `Polymarket openPrice` 被视为稳定可用主源，那么 `RTDS boundary fallback` 不是近期主优先级
  - 当前更值得补的是更完整 `window-scoped truth refresh meta`
    - 这轮 recent refresh 有没有跑
    - refresh 成功还是失败
    - 当前 truth/oracle table 是否 stale / fail-open
    - operator / backtest report 能不能直接看懂这批 truth/runtime 数据靠不靠谱
- `poly-eval` alias 进一步退场与其余长尾 scope 收尾

所以现在最准确的状态不是：

- `Phase B` 已完成

而是：

- `Phase B` 已进入第二批实装
- canonical quote surface、guard parity、hybrid variant、liquidity proxy、regime parity wiring 都已经入主线
- 但离 legacy full replay parity 还有最后一段重工作业

#### 本轮新增验证

这轮新增并通过的 focused tests：

- `v2/tests/test_research_backtest_liquidity_proxy.py`
- `v2/tests/test_research_backtest_regime_parity.py`
- `v2/tests/test_research_backtest_live_state_parity.py`
- `v2/tests/test_research_backtest_depth_parity.py`
- `v2/tests/test_research_backtest_decision_engine_parity.py`
- `v2/tests/test_research_backtest_decision_quote_surface.py`
- `v2/tests/test_research_backtest_phase_b.py`
- `v2/tests/test_research_backtest_fills.py`
- `v2/tests/test_research_backtest_runtime_parity.py`
- `v2/tests/test_research_experiment_parity_specs.py`
- `v2/tests/test_research_experiment_matrix_parity.py`
- `v2/tests/test_research_experiment_runtime_resume.py`
- `v2/tests/test_research_experiment_reports.py`
- `v2/tests/test_data_foundation_runtime.py`
- `v2/tests/test_live_execution.py`
- `v2/tests/test_live_strike_runtime.py`

并且已经通过：

- `pytest -q v2/tests/test_research_backtest_liquidity_proxy.py v2/tests/test_research_backtest_regime_parity.py v2/tests/test_research_backtest_policy.py v2/tests/test_research_experiment_parity_specs.py`
- `pytest -q v2/tests/test_research_backtest_phase_b.py v2/tests/test_research_backtest_parity.py v2/tests/test_research_builders.py`
- `pytest -q v2/tests/test_cli.py v2/tests/test_research_*`
- `pytest -q v2/tests`
- `PYTHONPATH=v2/src pytest -q v2/tests/test_research_experiment_reports.py`
- `PYTHONPATH=v2/src pytest -q v2/tests/test_cli.py -k 'research_evaluate_commands or research_evaluate_poly_eval_demo_commands or research_evaluate_poly_eval_scope_router_commands'`
- `PYTHONPATH=v2/src pytest -q v2/tests/test_research_backtest_fills.py v2/tests/test_research_backtest_policy.py`
- `PYTHONPATH=v2/src pytest -q v2/tests/test_research_backtest_decision_engine_parity.py v2/tests/test_research_backtest_policy.py v2/tests/test_research_backtest_fills.py v2/tests/test_research_experiment_parity_specs.py`
- `PYTHONPATH=v2/src pytest -q v2/tests/test_research_backtest_decision_quote_surface.py v2/tests/test_research_backtest_decision_engine_parity.py v2/tests/test_research_backtest_fills.py`
- `PYTHONPATH=v2/src pytest -q v2/tests/test_live_execution.py v2/tests/test_data_foundation_runtime.py v2/tests/test_research_backtest_runtime_parity.py`
- `PYTHONPATH=v2/src pytest -q v2/tests/test_live_strike_runtime.py v2/tests/test_live_service.py`
- `PYTHONPATH=v2/src pytest -q v2/tests/test_research_training_datasets_parity.py`
- `PYTHONPATH=v2/src pytest -q v2/tests/test_research_experiment_parity_specs.py`
- `PYTHONPATH=v2/src pytest -q v2/tests`

当前本机结果：

- `v2/tests`: `331 passed, 1 warning`

#### 继续往前推的最合理下一步

现在再往前推，最合理的下一步已经收窄成：

1. 先把 Chainlink truth runtime 的 `window refresh meta` 收完整
2. 如需继续补 raw depth，也只沿 `新快照就是新机会；retry fail 就 drop` 这条轻量主线收口
3. `RTDS boundary fallback` 只作为更后面的 live 兜底层再补
4. 继续收尾 `poly-eval` alias 与其余长尾 scope

#### 当前要落地的 `window refresh meta` contract

这一步不再谈抽象“runtime parity”，只落一个简单、可读、可值班的 contract。

要回答的就是 4 个问题：

1. 这轮 foundation refresh 有没有真的跑
2. 最近一次 refresh 是成功、降级 fail-open，还是直接失败
3. 现在拿来做 truth/oracle read 的表，是 fresh、stale，还是干脆 missing
4. operator / backtest report 能不能不用翻 log 就直接看懂

这轮正式口径：

- foundation 级字段直接看最近一次 run：
  - `run_started_at`
  - `last_completed_at`
  - `finished_at`
  - `completed_iterations`
  - `status / reason / issue_codes`
- truth runtime 级字段给一个总解释：
  - `truth_runtime_recent_refresh_status`
  - `truth_runtime_recent_refresh_interpretation`
- 每个关键 dataset 都给统一 refresh 面：
  - `*_rows`
  - `*_freshness_max`
  - `*_freshness_age_seconds`
  - `*_freshness_state`
  - `*_recent_refresh_status`

这里的状态解释保持极简：

- foundation 总状态只看：
  - `fresh`
  - `fail_open`
  - `degraded`
  - `error`
  - `running`
  - `unknown`
- dataset 状态只看：
  - `fresh`
  - `stale`
  - `missing`
  - `empty`
  - `unknown`

这里特别强调两点：

- `Polymarket openPrice` 当前被视为 live strike 的稳定主源，所以这一步不补 `RTDS boundary fallback`
- 这一步也不把 raw depth 回测线继续推成跨轮 retry state machine；仍然保持 `新快照就是新机会，retry fail 就 drop`

落地位置固定为两层：

1. producer
   - `data/pipelines/foundation_runtime.py`
   - `research/labels/runtime.py`
2. consumer
   - `research/backtests/reports.py`
   - `live/runner/diagnostics_risk.py`
   - `live/operator/summary.py`
   - `live/readiness/*`

完成标准也很明确：

- backtest report 能直接看到最近 refresh 时间和 truth/oracle freshness state
- `show-ready / show-latest-runner` 能直接看到 foundation refresh 是 fresh、fail-open 还是 stale-like degraded
- 不需要再去翻 foundation json/log，操作面就能判断这批 truth/runtime 数据能不能信

#### 当前并行推进口径

为了保持 package-first 和低耦合，当前继续推进时不应该让多个 worker 同时去改同一个 orchestration 文件，而应该按下面 3 条主线拆：

1. `raw depth partial fill engine`
   - 只负责 `research/backtests/` 下的 depth replay candidate consumption、pre-submit orderbook retry semantics、以及 `retry fail -> drop` 的轻量 diagnostics 收口
   - 主集成点仍由 `engine.py` / `fills.py` 收口

2. `truth runtime parity`
   - 只负责 `data/pipelines/foundation_runtime.py`、truth runtime metadata、live/readiness/backtest 可见面继续收口
   - research label/backtest 继续吃 canonical tables + runtime metadata
   - live strike 先以 `openPrice` 为稳定主源；`RTDS boundary fallback` 后补，不抢当前优先级

3. `poly_eval methods migration`
   - 只负责 `research/evaluation/methods/*` 与 evaluate 入口继续收口到 CLI-adjacent helpers
   - `calibration.py` / `drift.py` / `poly_eval.py` 继续保持薄 orchestration

这一步的判断标准不是“又多几个 helper 文件”，而是：

- raw depth path 能解释 `当前 snapshot 可成交多少`
- backtest report 能解释首个 snapshot 是 `repriced`、`orderbook_missing`，还是 `orderbook_limit_reject`
- `FAK no-match / orderbook_limit_reject` 时能否像 legacy 一样做即时 refresh retry
- evaluation 不再需要直接依赖 legacy 顶层 `poly_eval/*` 的核心方法层
- experiment 侧 stake/cap matrix 不只是能跑，而是已经能被正式 compare/report surface 消费

#### 当前这一刀已经完成的口径

这一步没有回头扩写 training，也没有回头重做文档结构。

这一步实际完成的是：

1. 把 offline `liquidity_proxy` 接进 `research/backtests/engine.py`
2. 把 live `RegimeController` 下沉成 backtest 可复用的纯 `regime_parity` adapter
3. 把 parity-resolved `LiveProfileSpec` 串进 `live_state_parity -> guard_parity -> fills`
4. 把 `summary.json` / `report.md` 补上 regime/liquidity 诊断字段
5. 把 regime-driven stake scale 正式接进 fills/runtime
6. 把 profile-driven depth parameters 和 depth diagnostics 正式接进 canonical fills
7. 把 experiment suite 的 hierarchy/variant planner 与 in-run reuse/resume 正式接进 runner
8. 把 experiment compare/report outputs 正式接进 experiment run artifacts
9. 把 raw depth multi-snapshot replay candidate chain 接进 backtest 主线
10. 把 DecisionEngine side-selection parity 接进 policy/fill 主线
11. 把 experiment cross-run shared cache 与 group/run/focus reports 接进 runner/report 主线
12. 把 `research/evaluation/methods/*` 第一批正式 package 接进 calibration / drift / poly_eval report
13. 把 raw depth partial fill + quote completion 第一刀接进 fills/report 主线
14. 把 experiment variant compare policy 接进 suite spec / runner artifacts
15. 把 probability / SMC / decision / copula 第一批 methods 正式接进 v2 evaluation package

这一步的完成标准不是：

- “多了一个新 helper 文件”

而是：

- backtest 决策行在进入 guard parity 之前，已经带上可解释的 `liquidity_*` / `regime_*` 状态
- 这些状态来自 canonical raw kline + live pure contracts，而不是研究侧再发明一套私有逻辑
- report 能直接回答：
  - 这次回测有没有进入 `CAUTION` / `DEFENSE`
  - liquidity proxy 有没有持续软失败
  - guard reject 到底是 quote、policy，还是 regime/liquidity 造成的

如果这一步完成，`Phase B` 才算真正把 `quote/orderbook canonical surface -> liquidity proxy -> regime state -> live-like guards` 这条链接上。

下一步如果继续往前推，就不该再回头补这一刀的文档，而应该优先收尾真正剩下的三件事：

1. 继续往 raw depth queue/price-path partial fill engine 推
2. 继续推 Chainlink truth refresh/runtime parity
3. 继续把长尾 migrated poly_eval methods 收口到 `poly-eval --scope` canonical surface

---

## 13. 一句话结论

这轮迁移的正确方向不是：

- “把 `poly_eval/` 和 `scripts/` 搬进 v2”

而是：

- “把真正的研究、评估、诊断语义，从 `poly_eval/` 和 `scripts/` 里抽出来，重新收编进 `v2/src/pm15min/{research,data,live}`，然后让 legacy 退成边界层。”
