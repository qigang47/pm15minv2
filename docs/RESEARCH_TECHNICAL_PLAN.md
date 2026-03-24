# v2 Research Domain Specification

这份文档是 `v2` Research 域的正式技术方案，覆盖：

- 特征工程
- 标签构建
- 训练数据集
- 模型训练
- 模型 bundle
- 离线推理
- 回测
- 实验编排
- 评估与报告

目标只有一个：以后任何人看到 `v2/research/`，都能立刻知道：

- 这个目录存的到底是什么
- 它依赖 `v2/data` 的哪个 canonical 数据集
- 它是 feature frame、label frame、training set、training run、model bundle，还是 backtest / experiment / evaluation
- 它由哪个命令生成
- live / backtest / evaluation 各自该读哪里，不该读哪里

如果后续代码实现与本文档冲突，以最终 `v2/src/pm15min/research/` 实现为准；但实现应尽快改回和本文一致，而不是继续沿用 legacy 混乱结构。

关于 `poly_eval/` 与 `scripts/` 的专项迁移执行细则，见：

- `v2/docs/POLY_EVAL_AND_SCRIPTS_MIGRATION_TECHNICAL_PLAN.md`

---

## 1. 背景与结论

### 1.1 本次盘点过的 legacy 代码

这次方案不是凭空设计，是在看过以下旧代码之后给出的 clean-room 方案：

- 文档
  - `docs/ALPHA_EXPERIMENT_FRAMEWORK.md`
  - `docs/DEEP_OTM_LIVE_FACTORS_AND_TRAINING.md`
  - `docs/MODELS.md`
  - `docs/CANONICAL_ARCHITECTURE.md`
  - `docs/CODEBASE_INVENTORY.md`
  - `scripts/training/README.md`
  - `scripts/analysis/README.md`
  - `poly_eval/README.md`

- 研究 / 训练 / 推理核心
  - `src/features/feature_engineering.py`
  - `src/features/strike_moneyness.py`
  - `src/models/training_pipeline.py`
  - `src/inference.py`

- 训练 / 回测 / 实验脚本
  - `scripts/training/train_fixed_cycle_models.py`
  - `scripts/training/run_alpha_experiment_train_suite.py`
  - `scripts/analysis/backtest_deep_otm_orderbook.py`
  - `scripts/analysis/alpha_experiment_framework.py`
  - `scripts/analysis/run_alpha_experiment_backtests.py`

- 实盘耦合点
  - `live_trading/core/signal_engine.py`
  - `live_trading/core/trade_logic.py`
  - `live_trading/core/decision_guards.py`
  - `live_trading/configs/backtest.py`
  - `live_trading/configs/shared.py`
  - `live_trading/configs/profiles.py`

- 评估工具
  - `poly_eval/cli.py`
  - `poly_eval/pipeline.py`
  - `poly_eval/production_stack.py`

- 过渡层
  - `apps/research/cli.py`
  - `apps/research/bridge.py`
  - `v2/src/pm15min/research/cli.py`
  - `v2/src/pm15min/research/service.py`

### 1.2 Legacy 的核心问题

盘点后的结论很清楚，旧研究域主要有 7 个问题：

1. `script-first`，不是 `package-first`
   - 真正的业务逻辑散在 `src/`、`scripts/training/`、`scripts/analysis/`、`poly_eval/`。
   - CLI 只是桥接，目录边界不是真边界。

2. 单脚本职责过载
   - `train_fixed_cycle_models.py` 同时负责：
     - kline 加载
     - dataset cache
     - feature build
     - strike anchor 修正
     - label 构建
     - reversal target 构造
     - holdout / OOT
     - 训练
     - bundle 输出
   - `backtest_deep_otm_orderbook.py` 同时负责：
     - 扫描 orderbook 文件
     - 生成特征
     - 调 signal engine
     - 套 live guards
     - 做 fill 模拟
     - 算 settlement
     - 写 report

3. 训练产物和推理产物没有严格分层
   - 现在一个 `artifacts_runs/<run>/artifacts_v6_offset*/` 目录里，既有训练调试需要的东西，也有 live 推理需要的东西。
   - `training run` 和 `deployable model bundle` 不是两个对象。

4. Research 对 Live 耦合过重
   - 回测直接 import `live_trading` 的 config / signal / guard / decision 逻辑。
   - 这虽然短期省事，但长期会导致：
     - 回测规则隐含
     - 研究代码不能独立演进
     - live 侧小修改会影响 offline 语义

5. 路径语义不稳定
   - `data/markets/<asset>/artifacts_runs/<run_name>/...`
   - `alpha_search_stage5_converge_*`
   - `summary.csv`、`meta.json`、`oracle_prices_merged.csv`
   - 这些路径能用，但目录含义依赖人脑记忆，不是依赖固定目录结构。

6. 特征体系没有 registry
   - `feature_engineering.py` 里硬编码了大批 feature。
   - 哪些是基础因子、哪些是 strike 因子、哪些是 cross-asset 因子，没有正式注册表。
   - 很难做版本化、灰度删除、稳定复现。

7. `poly_eval` 虽然独立，但没有纳入统一 research 产物流
   - 评估工具在仓库里是 standalone 包。
   - 它的输入输出和训练 run / backtest run 没有统一 manifest。

### 1.3 重构结论

`v2/research` 必须是 clean-room rewrite，不再继续修 legacy 结构。

原则：

- 新 research 代码不 import 旧的 `src/` / `scripts/` / `poly_eval/` / `live_trading/`
- 旧代码只作为业务语义参考
- research 只从 `v2/data` 读 canonical 数据
- training run、model bundle、backtest run、experiment run 必须是 4 个不同对象

---

## 2. 设计原则

### 2.1 Domain 边界

`research` 只负责：

- 从 `v2/data` canonical 数据集构建研究输入
- 定义 feature / label / dataset / split
- 训练模型
- 产出可部署的 model bundle
- 做离线回测 / 实验 / 评估

`research` 不负责：

- 原始数据抓取
- live 实盘执行
- Polymarket API 下单
- recorder runtime
- live runtime state

这些都属于：

- `v2/data`
- `v2/live`

### 2.2 Package-first

任何能力都必须先有包内实现，再有 CLI。

禁止继续新增这种模式：

- 先写一个 `scripts/foo.py`
- 里面塞 1200 行逻辑
- 最后再靠 shell 批量调用

正确顺序必须是：

1. `pm15min.research.*` 包内模块
2. 明确的 dataclass / manifest / contract
3. `python -m pm15min research ...` CLI
4. 如有需要，再加 shell wrapper

### 2.3 四类核心持久化对象

Research 域固定有 4 类核心持久化对象：

1. `training_set`
   - 已经对齐好的训练表
   - 只是样本，不是模型

2. `training_run`
   - 一次训练执行的完整产物
   - 包括 metrics / logs / OOF / feature schema / offset outputs

3. `model_bundle`
   - 只保留推理所需内容
   - 给 `live` 和 `backtest` 读取

4. `backtest_run`
   - 一次离线回放的完整结果
   - 包括 trades / rejects / summary / manifest

必须显式分开，绝对禁止混在一个目录里。

### 2.4 Path-driven，不靠 run name 猜语义

以后语义必须主要由目录层级表达，不由文件名或 run name 暗示。

允许：

- `cycle=15m`
- `asset=sol`
- `target=reversal`
- `feature_set=deep_otm_v1`
- `offset=7`
- `run=2026-03-19T12-05-01Z`

不允许新增：

- `train_v6_rev_sol_20260306_004403`
- `stage5_converge_fixed_truth_final2`
- `tmp_compare_best`

这些名字可以保留在 manifest 的 `alias` / `notes` 里，但不能成为唯一语义来源。

### 2.5 Manifest-first

每一个 persisted object 都必须有 manifest：

- `manifest.json`
- `input_manifest.json`
- `bundle_manifest.json`
- `summary.json`

最少要写清楚：

- 输入数据路径
- 输入数据版本 / hash / row count
- 参数
- 产物路径
- 时间范围
- 目标语义
- 生成时间

### 2.6 Live 语义要“共享契约”，不要“直接引用 live 代码”

Legacy 的回测直接复用 `live_trading/core/signal_engine.py` 和 guards，这会让 research 和 live 无限纠缠。

`v2` 的正确方式：

- 把 live/research 共同需要的纯契约下沉到 `pm15min.core` 或 `pm15min.research.contracts`
- research 自己实现 offline scorer / guard evaluator
- live 自己实现 runtime adapter

也就是说：

- 共享“规则定义”
- 不共享“legacy 模块依赖”

---

## 3. 顶层目录方案

Research 域落地后，`v2` 目录应该长这样：

```text
v2/
  src/
    pm15min/
      research/
        __init__.py
        cli.py
        layout.py
        config.py
        contracts.py
        manifests.py
        features/
        labels/
        datasets/
        training/
        bundles/
        inference/
        backtests/
        experiments/
        evaluation/

  research/
    feature_frames/
    label_frames/
    training_sets/
    training_runs/
    model_bundles/
    backtests/
    experiments/
    evaluations/

  var/
    research/
      cache/
      locks/
      logs/
      tmp/
```

注意：

- `v2/src/pm15min/research/` 存代码
- `v2/research/` 存持久化研究产物
- `v2/var/research/` 存临时态、缓存、锁、运行日志

---

## 4. 标准目录树

下面是 Research 域标准树。后续新增能力必须在这棵树里找位置，不能重新发明目录。

```text
v2/research/
  feature_frames/
    cycle=15m/
      asset=sol/
        feature_set=deep_otm_v1/
          source_surface=backtest/
            data.parquet
            manifest.json

  label_frames/
    cycle=15m/
      asset=sol/
        label_set=truth/
          data.parquet
          manifest.json
        label_set=oracle_prices/
          data.parquet
          manifest.json

  training_sets/
    cycle=15m/
      asset=sol/
        feature_set=deep_otm_v1/
          label_set=truth/
            target=reversal/
              window=2025-10-27_2026-03-05/
                offset=7/
                  data.parquet
                  manifest.json
                offset=8/
                  data.parquet
                  manifest.json
                offset=9/
                  data.parquet
                  manifest.json

  training_runs/
    cycle=15m/
      asset=sol/
        model_family=deep_otm/
          target=reversal/
            run=2026-03-19T12-05-01Z/
              manifest.json
              summary.json
              logs/
                train.jsonl
              offsets/
                offset=7/
                  metrics.json
                  oof_predictions.parquet
                  feature_schema.json
                  models/
                    lgbm_sigmoid.joblib
                    logreg_sigmoid.joblib
                    catboost.joblib
                  calibration/
                    blend_weights.json
                    cat_temperature.json
                    reliability_bins_lgb.json
                    reliability_bins_lr.json
                    reliability_bins_blend.json
                    reliability_bins_blend_weighted.json
                offset=8/
                offset=9/

  model_bundles/
    cycle=15m/
      asset=sol/
        profile=deep_otm/
          target=reversal/
            bundle=2026-03-19T12-30-00Z/
              manifest.json
              offsets/
                offset=7/
                  feature_schema.json
                  bundle_config.json
                  models/
                    lgbm_sigmoid.joblib
                    logreg_sigmoid.joblib
                    catboost.joblib
                  calibration/
                    blend_weights.json
                    cat_temperature.json
                    reliability_bins_blend_weighted.json
                offset=8/
                offset=9/

  active_bundles/
    cycle=15m/
      asset=sol/
        profile=deep_otm/
          target=direction/
            selection.json
        profile=deep_otm_baseline/
          target=direction/
            selection.json

  backtests/
    cycle=15m/
      asset=sol/
        profile=deep_otm/
          spec=baseline_truth/
            run=2026-03-19T13-00-00Z/
              manifest.json
              summary.json
              trades.parquet
              rejects.parquet
              markets.parquet
              equity_curve.parquet
              logs/
                backtest.jsonl

  experiments/
    suite_specs/
      alpha_search_sol_stage5.json
      alpha_search_xrp_stage5.json
    runs/
      suite=alpha_search_sol_stage5/
        run=2026-03-19T14-00-00Z/
          manifest.json
          training_runs.parquet
          backtest_runs.parquet
          leaderboard.parquet
          leaderboard.csv
          logs/
            suite.jsonl

  evaluations/
    calibration/
      asset=sol/
        bundle=2026-03-19T12-30-00Z/
          run=2026-03-19T15-00-00Z/
            manifest.json
            summary.json
            reliability.parquet
            plots/
              reliability.png
              brier_over_time.png
    drift/
      asset=sol/
        bundle=2026-03-19T12-30-00Z/
          run=2026-03-19T15-10-00Z/
            manifest.json
            summary.json
            slices.parquet
    poly_eval/
      asset=sol/
        run=2026-03-19T15-20-00Z/
          manifest.json
          report.md
          outputs/
```

---

## 5. 路径命名规则

### 5.1 固定分区名

统一使用显式分区名：

- `cycle=15m`
- `asset=btc|eth|sol|xrp`
- `feature_set=...`
- `label_set=truth|oracle_prices`
- `target=direction|reversal`
- `window=YYYY-MM-DD_YYYY-MM-DD`
- `offset=7`
- `model_family=deep_otm|trend_follow|...`
- `profile=deep_otm|custom_band|...`
- `spec=baseline_truth|guard_sweep|...`
- `suite=<suite_slug>`
- `run=<UTC timestamp>`
- `bundle=<UTC timestamp>`

### 5.2 固定文件名

目录决定语义，文件名尽量固定：

- `data.parquet`
- `manifest.json`
- `summary.json`
- `metrics.json`
- `feature_schema.json`
- `bundle_config.json`
- `trades.parquet`
- `rejects.parquet`
- `equity_curve.parquet`
- `leaderboard.parquet`

### 5.3 文件格式规则

- 样本表 / 结果表：`parquet`
- manifest / summary / config：`json`
- 运行日志：`jsonl`
- 给人看的排行榜 / 汇总：`csv`
- sklearn / joblib 兼容模型：`joblib`

### 5.4 不允许的命名

不允许新增以下风格：

- `tmp_train_final_v2`
- `train_v6_rev_sol_20260306_004403`
- `summary_fixed.csv`
- `best_weights_latest.json`
- `deepotm_backtest_new2`

原因：

- 看路径不知道 object type
- 看路径不知道 target / label_set / profile
- 看路径不知道是不是 canonical persisted object

---

## 6. Research 与 Data 的边界

### 6.1 Research 只读 `v2/data` 的 canonical 数据

Research 域只能从下面这些 canonical 数据入口读取：

- `v2/data/backtest/tables/markets/...`
- `v2/data/backtest/tables/orderbook_index/...`
- `v2/data/backtest/tables/oracle_prices/...`
- `v2/data/backtest/tables/truth/...`
- `v2/data/backtest/sources/binance/klines_1m/...`
- `v2/data/backtest/sources/polymarket/orderbooks/...`

允许在极少数情况下读取 `v2/data/backtest/sources/...` 的 raw source，但必须通过 `pm15min.data.queries` 访问，不能在 research 模块里直接手搓文件扫描。

### 6.2 Research 不直接读 legacy `data/markets`

禁止：

- 直接读 `data/markets/<asset>/artifacts_runs/...`
- 直接读 `data/markets/_shared/logs/...`
- 直接读 legacy `data/raw` 当正式训练输入
- 在 v2 代码里继续假设 legacy symlink 结构

如需迁移旧产物，只允许做一次性 importer，把旧产物导入 `v2/research/...`。

### 6.3 Live / Backtest surface 关系

Research 本身是 offline domain，但它依赖的数据要明确区分 surface：

- 训练和回测默认读 `v2/data/backtest/...`
- live smoke / deployment verification 如需做最新 bundle 检查，可只读 `v2/data/live/tables/...`
- 任何研究逻辑都不能把 `live source` 当成可复现实验基准

---

## 7. Canonical 输入对象定义

Research 先固定 3 个 canonical 输入对象。

### 7.1 `feature_frame`

路径：

```text
v2/research/feature_frames/cycle=<cycle>/asset=<asset>/feature_set=<feature_set>/source_surface=backtest/data.parquet
```

职责：

- 在 1m 决策时钟上形成可训练 / 可推理的特征矩阵
- 这是 research 域唯一允许长期复用的特征事实表

主键：

- `decision_ts`

核心列：

- `decision_ts`
- `cycle_start_ts`
- `cycle_end_ts`
- `offset`
- `ret_from_cycle_open`
- `ret_from_strike`
- `basis_bp`
- `has_oracle_strike`
- 所有 feature 列

输入来源：

- Binance 1m backtest source
- `v2/data/backtest/tables/oracle_prices/...`

规则：

- feature frame 只做特征事实，不做 target 构造
- `target=reversal` 不能提前写死在 feature frame 里

### 7.2 `label_frame`

路径：

```text
v2/research/label_frames/cycle=<cycle>/asset=<asset>/label_set=<label_set>/data.parquet
```

`label_set` 先固定两类：

- `truth`
- `oracle_prices`

主键：

- `cycle_start_ts`

核心列：

- `cycle_start_ts`
- `cycle_end_ts`
- `price_to_beat`
- `final_price`
- `direction_up`
- `settlement_source`
- `label_set`

说明：

- `truth` 代表最终 resolved truth 口径
- `oracle_prices` 代表 `price_to_beat/final_price` 口径
- `reversal` 不是 label frame 级别对象，因为 reversal 要结合 `decision_ts` 当时的 moneyness

### 7.3 `training_set`

路径：

```text
v2/research/training_sets/cycle=<cycle>/asset=<asset>/feature_set=<feature_set>/label_set=<label_set>/target=<target>/window=<window>/offset=<offset>/data.parquet
```

职责：

- feature frame 和 label frame 对齐后的最终训练表
- 已经能直接喂给 trainer

主键：

- `decision_ts`

核心列：

- `decision_ts`
- `cycle_start_ts`
- `cycle_end_ts`
- `offset`
- `y`
- `target`
- `split`
- `fwd_return`
- 所有训练特征

规则：

- `target=direction`
  - `y` 只依赖最终方向

- `target=reversal`
  - `y` 必须显式由
    - 当前 `ret_from_strike` 正负
    - 最终方向
    联合构造

- training set manifest 必须记录：
  - source feature frame
  - source label frame
  - window
  - offset
  - target
  - holdout_months
  - feature drop list

---

## 8. 训练产物定义

### 8.1 `training_run`

路径：

```text
v2/research/training_runs/cycle=<cycle>/asset=<asset>/model_family=<family>/target=<target>/run=<run_ts>/
```

`training_run` 是完整训练执行记录，不是部署目录。

根目录固定包含：

- `manifest.json`
- `summary.json`
- `logs/train.jsonl`
- `offsets/`

`manifest.json` 至少包含：

- `run_id`
- `asset`
- `cycle_minutes`
- `model_family`
- `target`
- `feature_set`
- `label_set`
- `window`
- `offsets`
- `input_training_set_paths`
- `trainer_config`
- `random_seed`
- `created_at`

### 8.2 offset 子目录

路径：

```text
.../offsets/offset=<offset>/
```

固定包含：

- `metrics.json`
- `feature_schema.json`
- `oof_predictions.parquet`
- `models/`
- `calibration/`

`models/` 固定放：

- `lgbm_sigmoid.joblib`
- `logreg_sigmoid.joblib`
- `xgb.joblib` 可选
- `catboost.joblib` 可选

`calibration/` 固定放：

- `blend_weights.json`
- `cat_temperature.json`
- `reliability_bins_lgb.json`
- `reliability_bins_lr.json`
- `reliability_bins_blend.json`
- `reliability_bins_blend_weighted.json`

### 8.3 `training_run` 和 `model_bundle` 的区别

`training_run` 给研究复现用，包含：

- OOF
- 训练日志
- 评估输出
- 完整参数

`model_bundle` 给推理用，只保留：

- feature schema
- 模型文件
- calibration 文件
- bundle config

禁止让 live / backtest 直接读取 `training_run`。

---

## 9. Model Bundle 定义

### 9.1 `model_bundle`

路径：

```text
v2/research/model_bundles/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/bundle=<bundle_ts>/
```

bundle 是 Research 域对 Live / Backtest 暴露的唯一推理对象。

但这里要额外强调：

- `model_bundles/` 可以有很多 bundle
- 同一个 `asset + profile + target` 在实盘上只能有一个 active bundle
- active 选择必须单独落在 `active_bundles/`，不能靠“最新目录”暗示

根目录固定包含：

- `manifest.json`
- `offsets/`

`manifest.json` 至少包含：

- `bundle_id`
- `source_training_run`
- `asset`
- `cycle_minutes`
- `profile`
- `target`
- `offsets`
- `required_history_minutes`
- `feature_blacklist`
- `created_at`

### 9.2 offset bundle 子目录

固定包含：

- `feature_schema.json`
- `bundle_config.json`
- `models/*`
- `calibration/*`

`bundle_config.json` 必须显式记录：

- `signal_target=direction|reversal`
- `offset`
- `feature_columns`
- `missing_feature_fill_value`
- `required_feature_columns`
- `allowed_blacklist_columns`

### 9.3 为什么要单独做 bundle

因为 legacy 最大的问题之一，就是“训练产物”和“上线产物”混在一起。

`bundle` 的作用是把部署语义定死：

- live 只读 bundle
- backtest 只读 bundle
- evaluation 可以读 bundle + backtest outputs
- 训练调试细节不能泄漏到推理接口

### 9.4 `active_bundles`

路径：

```text
v2/research/active_bundles/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/selection.json
```

这个对象不是 bundle 本体，而是 active registry。

职责非常明确：

- `model_bundles/` 负责保存所有可复现、可回测、可部署的 bundle
- `active_bundles/` 负责声明“当前 live / baseline 默认到底用哪一个”

`selection.json` 至少包含：

- `market`
- `cycle`
- `profile`
- `target`
- `bundle_label`
- `bundle_dir`
- `source_run_dir`
- `usage`
- `activated_at`
- `notes`

### 9.5 解析规则

默认 bundle 解析顺序必须固定：

1. 先读 `active_bundles/.../selection.json`
2. 如果 active registry 存在，就直接使用其中的 `bundle_dir`
3. 只有当 active registry 缺失时，才允许回退到 `model_bundles/` 下的默认搜索逻辑

原因：

- 实验阶段会产生很多 bundle
- 实盘不能靠“最新生成那个目录”自动切换权重
- baseline 也不能和 live profile 混在一起靠命名猜

---

## 10. 离线推理接口

`v2/research/inference/` 固定负责 3 件事：

1. 加载 bundle
2. 根据 feature frame 打分
3. 输出统一 signal contract

统一 signal contract 必须包含：

- `p_mean`
- `p_eff_up`
- `p_eff_down`
- `signal_target`
- `offset`
- `decision_ts`

对于 `target=reversal`，离线推理必须在 research 内自己显式完成：

- `P(reversal)` -> `P(UP)` / `P(DOWN)` 映射
- 当前 moneyness 缺失时的 fail-closed 语义

不能继续把这层语义隐含在 legacy `live_trading/core/signal_engine.py` 里。

---

## 11. Backtest 定义

### 11.1 `backtest_run`

路径：

```text
v2/research/backtests/cycle=<cycle>/asset=<asset>/profile=<profile>/spec=<spec>/run=<run_ts>/
```

职责：

- 用 canonical bundle + canonical backtest data 进行 replay
- 输出可审计、可比较、可聚合的结果

固定文件：

- `manifest.json`
- `summary.json`
- `trades.parquet`
- `rejects.parquet`
- `markets.parquet`
- `equity_curve.parquet`
- `logs/backtest.jsonl`

### 11.2 Backtest 输入

Backtest 只允许读：

- `model_bundle`
- `v2/data/backtest/tables/orderbook_index/...`
- `v2/data/backtest/sources/polymarket/orderbooks/...`
- `v2/research/feature_frames/...`
- `v2/research/label_frames/...`

禁止：

- 直接 glob legacy `data/markets/<asset>/data/polymarket/raw/orderbooks_full`
- 直接 import legacy `live_trading.configs.backtest`

### 11.3 Backtest 规则分层

Backtest 内部固定拆成 5 层：

1. `replay_loader`
   - 负责扫描 orderbook index 和加载深度快照

2. `scorer`
   - 负责用 bundle 对 feature frame 打分

3. `policy`
   - 负责把 `p_eff_up/p_eff_down` 和 quotes 转为候选动作

4. `fills`
   - 负责盘口成交模拟

5. `settlement`
   - 负责用 label frame / truth frame 判定结果

### 11.4 Backtest summary 固定字段

`summary.json` 至少要有：

- `asset`
- `profile`
- `spec`
- `bundle_id`
- `start_ts`
- `end_ts`
- `records_scanned`
- `trades`
- `wins`
- `losses`
- `pnl_sum`
- `stake_sum`
- `roi_pct`
- `reject_reason_counts`
- `truth_source_counts`

---

## 12. Experiment Suite 定义

### 12.1 `suite_spec`

路径：

```text
v2/research/experiments/suite_specs/<suite_name>.json
```

它定义的是“实验矩阵”，不是执行结果。

最少字段：

- `suite_name`
- `cycle_minutes`
- `markets`
- `feature_set`
- `label_set`
- `target`
- `train_windows`
- `offsets`
- `bundle_profile`
- `backtest_specs`
- `evaluation_specs`

### 12.2 `experiment_run`

路径：

```text
v2/research/experiments/runs/suite=<suite_name>/run=<run_ts>/
```

固定包含：

- `manifest.json`
- `training_runs.parquet`
- `backtest_runs.parquet`
- `leaderboard.parquet`
- `leaderboard.csv`
- `logs/suite.jsonl`

### 12.3 Experiment 的职责

Experiment 域只负责“编排和汇总”，不负责“重新实现训练和回测逻辑”。

也就是说：

- suite runner 调 `training.run`
- suite runner 调 `bundle.build`
- suite runner 调 `backtest.run`
- suite runner 调 `evaluation.*`

它自己不应该再出现一份独立的 feature / label / backtest 实现。

---

## 13. Evaluation 定义

### 13.1 Evaluation 必须纳入统一目录

Legacy 的 `poly_eval` 是独立包，v2 里要保留能力，但要纳入 `research/evaluations/` 体系。

Evaluation 分 3 类：

1. `calibration`
   - reliability
   - brier
   - time slice drift

2. `drift`
   - 概率漂移
   - feature drift
   - segment drift

3. `poly_eval`
   - SMC
   - copula risk
   - ABM
   - production stack

### 13.2 Evaluation 目录

固定在：

- `v2/research/evaluations/calibration/...`
- `v2/research/evaluations/drift/...`
- `v2/research/evaluations/poly_eval/...`

### 13.3 `poly_eval` 在 v2 的处理原则

不再保留顶层 `poly_eval/` 作为最终架构目标。

推荐迁移到：

- `v2/src/pm15min/research/evaluation/poly_eval/`

这样可以保证：

- CLI 入口统一到 `python -m pm15min research ...`
- 输入输出路径统一写到 `v2/research/evaluations/...`
- 不再出现“一个独立包 + 一套独立 reports 目录”的并行体系

---

## 14. `src/pm15min/research/` 代码结构

下面是推荐的包结构。

```text
v2/src/pm15min/research/
  layout.py
  config.py
  contracts.py
  manifests.py
  cli.py

  features/
    registry.py
    base.py
    price.py
    volume.py
    cycle.py
    strike.py
    cross_asset.py
    builders.py

  labels/
    frames.py
    direction.py
    reversal.py
    loaders.py

  datasets/
    feature_frames.py
    training_sets.py
    splits.py
    manifests.py
    loaders.py

  training/
    trainers.py
    blend.py
    calibration.py
    reports.py
    runner.py

  bundles/
    builder.py
    loader.py
    validators.py

  inference/
    scorer.py
    signal_mapping.py
    contracts.py

  backtests/
    replay_loader.py
    fills.py
    settlement.py
    policy.py
    engine.py
    reports.py

  experiments/
    specs.py
    planner.py
    runner.py
    leaderboard.py

  evaluation/
    calibration.py
    drift.py
    poly_eval/
```

### 14.1 每层职责

`features/`

- 只定义 feature
- 不做训练
- 不做回测

`labels/`

- 只定义 label frame 和 target builder

`datasets/`

- feature frame + label frame 对齐
- split 生成
- manifest 写出

`training/`

- fit / calibration / OOF / report

`bundles/`

- 从 training run 提炼 bundle

`inference/`

- 只做 bundle 读取与打分

`backtests/`

- 只做 replay

`experiments/`

- 只做 suite orchestration

`evaluation/`

- 只做评估与报告

---

## 15. Legacy -> v2 映射

### 15.1 特征

- `src/features/feature_engineering.py`
  - 拆到：
    - `v2/src/pm15min/research/features/price.py`
    - `v2/src/pm15min/research/features/volume.py`
    - `v2/src/pm15min/research/features/cycle.py`
    - `v2/src/pm15min/research/features/cross_asset.py`
    - `v2/src/pm15min/research/features/registry.py`

- `src/features/strike_moneyness.py`
  - 拆到：
    - `v2/src/pm15min/research/features/strike.py`

### 15.2 训练

- `src/models/training_pipeline.py`
  - 拆到：
    - `v2/src/pm15min/research/training/trainers.py`
    - `v2/src/pm15min/research/training/blend.py`
    - `v2/src/pm15min/research/training/calibration.py`
    - `v2/src/pm15min/research/training/reports.py`

- `scripts/training/train_fixed_cycle_models.py`
  - 拆到：
    - `v2/src/pm15min/research/datasets/feature_frames.py`
    - `v2/src/pm15min/research/datasets/training_sets.py`
    - `v2/src/pm15min/research/labels/*`
    - `v2/src/pm15min/research/training/runner.py`

### 15.3 推理

- `src/inference.py`
  - 拆到：
    - `v2/src/pm15min/research/inference/scorer.py`
    - `v2/src/pm15min/research/inference/signal_mapping.py`
    - `v2/src/pm15min/research/bundles/loader.py`

### 15.4 回测

- `scripts/analysis/backtest_deep_otm_orderbook.py`
  - 拆到：
    - `v2/src/pm15min/research/backtests/replay_loader.py`
    - `v2/src/pm15min/research/backtests/fills.py`
    - `v2/src/pm15min/research/backtests/policy.py`
    - `v2/src/pm15min/research/backtests/settlement.py`
    - `v2/src/pm15min/research/backtests/engine.py`
    - `v2/src/pm15min/research/backtests/reports.py`

### 15.5 实验编排

- `scripts/analysis/alpha_experiment_framework.py`
- `scripts/training/run_alpha_experiment_train_suite.py`
- `scripts/analysis/run_alpha_experiment_backtests.py`
  - 合并到：
    - `v2/src/pm15min/research/experiments/specs.py`
    - `v2/src/pm15min/research/experiments/planner.py`
    - `v2/src/pm15min/research/experiments/runner.py`
    - `v2/src/pm15min/research/experiments/leaderboard.py`

### 15.6 评估

- `poly_eval/*`
  - 迁入：
    - `v2/src/pm15min/research/evaluation/poly_eval/*`

### 15.7 过渡层

- `apps/research/*`
  - 不再作为最终架构目标
  - 只保留过渡期兼容意义

---

## 16. CLI 方案

Research 域统一走：

```bash
PYTHONPATH=v2/src python -m pm15min research ...
```

### 16.1 Feature / Label / Dataset

```bash
PYTHONPATH=v2/src python -m pm15min research build feature-frame --market sol --feature-set deep_otm_v1
PYTHONPATH=v2/src python -m pm15min research build label-frame --market sol --label-set truth
PYTHONPATH=v2/src python -m pm15min research build training-set --market sol --target reversal --offset 7
```

### 16.2 Training / Bundle

```bash
PYTHONPATH=v2/src python -m pm15min research train run --market sol --model-family deep_otm --target reversal
PYTHONPATH=v2/src python -m pm15min research bundle build --market sol --training-run <run_id> --profile deep_otm
```

### 16.3 Backtest

```bash
PYTHONPATH=v2/src python -m pm15min research backtest run --market sol --profile deep_otm --spec baseline_truth
```

### 16.4 Experiment

```bash
PYTHONPATH=v2/src python -m pm15min research experiment run-suite --spec v2/research/experiments/suite_specs/alpha_search_sol_stage5.json
```

### 16.5 Evaluation

```bash
PYTHONPATH=v2/src python -m pm15min research evaluate calibration --market sol --bundle <bundle_id>
PYTHONPATH=v2/src python -m pm15min research evaluate drift --market sol --bundle <bundle_id>
PYTHONPATH=v2/src python -m pm15min research evaluate poly-eval --market sol --input <backtest_run>
```

### 16.6 命令与产物映射

- `build feature-frame`
  - 产出 `v2/research/feature_frames/...`

- `build label-frame`
  - 产出 `v2/research/label_frames/...`

- `build training-set`
  - 产出 `v2/research/training_sets/...`

- `train run`
  - 产出 `v2/research/training_runs/...`

- `bundle build`
  - 产出 `v2/research/model_bundles/...`

- `backtest run`
  - 产出 `v2/research/backtests/...`

- `experiment run-suite`
  - 产出 `v2/research/experiments/runs/...`

- `evaluate *`
  - 产出 `v2/research/evaluations/...`

---

## 17. 需要显式保留的业务语义

这部分是从 legacy 代码里提炼出来、v2 不能丢的业务语义。

### 17.1 Strike anchor 语义必须保留

Legacy 训练和回测里都有这条关键逻辑：

- Binance 的 `ret_from_cycle_open`
- 需要修正成 Polymarket / Chainlink strike 锚点
- 派生出：
  - `ret_from_strike`
  - `basis_bp`
  - `move_z_strike`

这条语义在 v2 里必须成为正式 feature family：

- `features/strike.py`

不能继续作为脚本里的临时补丁函数存在。

### 17.2 `target=reversal` 语义必须显式化

Legacy 中 `reversal` 的定义非常关键：

- 当前在 strike 上方但最后 DOWN
- 或当前在 strike 下方但最后 UP

这必须在 v2 里成为明确的 target builder，而不是 `train_fixed_cycle_models.py` 里的脚本分支。

### 17.3 Bundle 必须显式写 `signal_target`

Legacy live 里已经证明：

- 同一套模型文件，如果不知道它输出的是 `P(UP)` 还是 `P(reversal)`，就会被错误解释

因此 `bundle_config.json` 必须强制写：

- `signal_target`

不能再依赖环境变量猜测。

### 17.4 Backtest 必须保留“盘口 replay”而不是退化成 bar backtest

Legacy 的一个真正有价值点是：

- 用完整 orderbook depth 做 replay
- fill 基于 orderbook levels
- guard 基于 decision-time quotes

这个能力要保留，但实现要重写成清晰模块化结构。

---

## 18. 测试与验收

### 18.1 单元测试

必须覆盖：

- feature registry
- strike anchor feature 计算
- reversal target builder
- dataset manifest 写出 / 读取
- bundle manifest 校验
- path locator

### 18.2 集成测试

至少要覆盖：

1. `feature_frame -> training_set`
2. `training_set -> training_run`
3. `training_run -> model_bundle`
4. `model_bundle -> offline inference`
5. `model_bundle + replay data -> backtest_run`

### 18.3 Golden 行为测试

v2 不要求字节级复刻 legacy 目录，但必须验证关键业务语义不丢：

- `ret_from_strike` 方向一致
- `reversal` label 一致
- bundle 输出字段齐全
- `P(reversal)` -> `P(UP/DOWN)` 映射一致
- backtest fill / settlement 基本一致

---

## 19. 实施顺序

Research 域建议按下面顺序落地，不要乱序。

### Phase 1

- `research/layout.py`
- `research/config.py`
- `research/contracts.py`
- `research/manifests.py`
- CLI skeleton

### Phase 2

- feature registry
- strike feature family
- label frame builders
- feature frame builders

### Phase 3

- training set builder
- split / holdout / oot 逻辑
- dataset manifest

### Phase 4

- trainer
- calibration
- OOF report
- training run writer

### Phase 5

- bundle builder
- bundle loader
- offline scorer

### Phase 6

- replay loader
- fill engine
- settlement engine
- backtest run writer

### Phase 7

- suite spec
- experiment runner
- leaderboard

### Phase 8

- calibration evaluation
- drift evaluation
- poly_eval migration

---

## 20. 最终结论

对现在这个仓库来说，`research` 不能继续做 incremental cleanup。

正确方向只有一个：

- 在 `v2/` 下新建清晰的 research domain
- 把路径、对象、manifest、命令全部定死
- 用 `feature_frame -> training_set -> training_run -> model_bundle -> backtest_run -> experiment_run` 这条主线替代旧的脚本网状结构

这样后面你再看仓库时，才会是“一眼就懂”，而不是继续靠记忆去猜：

- 哪个目录是训练输入
- 哪个目录是上线模型
- 哪个目录是回测结果
- 哪个目录只是临时脚本输出
