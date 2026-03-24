# v2 训练 / 回测可视化工作台现状对齐草案

这份文档是按当前 `v2` 代码重新整理后的版本。

目标不是继续写一份“理想产品愿景”，而是把下面 3 件事说清楚：

1. 现在代码里已经有什么。
2. 你的可视化需求里，哪些已经有后端对象可以直接接。
3. 哪些还没做，需要补新的产物或任务层。

这份文档当前默认的产品前提也一并明确下来：

- 这是本地单用户控制台，不以登录 / 多用户 / 权限系统为当前目标。
- 当前最重要的是回测结果展示和实验对比，而不是训练过程监控。
- 训练页当前优先展示 run summary、explainability artifact 和必要状态，不优先做复杂实时监控。

---

## 1. 当前结论

按现在的 `v2` 代码看，训练 / bundle / 回测后端已经比之前完整很多：

- `data` 域已有 canonical summary、freshness、orderbook 覆盖和 foundation runtime 状态。
- `research.training` 已有 richer diagnostics，不再只是最小版训练。
- `research.bundles` 已会复制诊断产物，并生成 bundle summary / report。
- `research.backtests` 已不是简单 label replay，而是带 depth replay、decision quote parity、regime/liquidity parity、hybrid fallback、canonical fills 的统一回放管线。
- `research.experiments` 已支持 suite、stake matrix、hybrid variant、resume/rerun、leaderboard、compare、matrix summary。

但同时，当前 `v2` 也已经不再停留在“只有只读 summary”的阶段：

- persisted runtime state / history 已经存在，并且有 operator-facing runtime board。
- explainability 产物已经正式落盘到 training / bundle artifact 体系。
- advanced backtest 参数已经暴露到基础 CLI / console action。
- 单 run backtest 的 stake / offset / factor sweep object 已经落成 canonical parquet。
- 训练和 experiment 都已经具备受控并发能力。

当前真正还没完成的，更多是：

- 更完整的 section 页面渲染和图表化 operator dashboard。
- 更大范围的动作覆盖和更顺手的 row-linked workflow。
- 比现在更成熟的回测 / experiment 可视化页面，而不只是 artifact inventory。
- training 页的必要展示仍要继续收口，但训练过程监控不是当前第一优先级。

所以这份文档的结论是：

- 现在应该做的是“把已有 canonical outputs 做成可视化控制台”。
- 不是重新设计一套新的 research/backtest 内核。

再往前推进时，当前更合理的评估是：

- 方向仍然是对的：`console + canonical outputs + thin action/task layer` 仍然是主线。
- `Phase A/B` 的基础层已经成型，`Phase C` 的底层 contract 也已经补齐，不需要再把这些当成“待设计愿景”。
- 下一阶段更应该集中在 `Console 产品化最后收口`，把已经有的对象和任务真正做成清晰、稳定、可操作的页面。
- 不需要为了继续推进去重写 research/backtest 内核；现在主要是把已有能力完整暴露出来。

---

## 2. 当前代码真实状态

## 2.1 UI / 服务层现状

当前 `v2` 已经有一个最小但正式的 `console/` 层。

现在已经存在：

- `console/read_models/`
- `console/service.py`
- `console/http/`
- `console/web/`
- `python -m pm15min console ...`

也就是说，当前已经不再是“完全没有 UI/API/server”。

当前已有的能力是：

- console CLI
- JSON HTTP server
- sectioned API 路由
- 极简 HTML/CSS/JS 页面壳
- action catalog / command preview facade
- thin action execution / action result
- 首页已接通少量真实动作按钮
- 部分 section 已接通局部动作条和最近动作结果面板
- action catalog 已能表达 section 归属和 shell-enabled 动作
- `training runs`、`bundles`、`backtests`、`experiments` 已有局部表单
- read-model 已补 `action_context`，页面可用选中行作为默认参数
- 页面 action form 已由 action catalog metadata 驱动渲染
- 已有最小后台 task contract、task API 和 async action submit/polling
- 已有 selected-row quick actions
- task contract 已补 `current_stage / progress_pct / heartbeat / started_at / finished_at`
- `console task` 已对 `training/backtest/experiment` 直连 runner，开始吃到真实 runtime progress
- `console task` 已对 `bundle build / activate bundle` 直连 Python entrypoint
- `data sync` / `data build` 已进入统一 metadata-driven + async task 流
- `/api/console/tasks` list/detail payload 已开始补 status/progress/result/error/request 摘要
- `/api/console/tasks` 已支持按 `action_ids` 过滤，CLI `console list-tasks` 也已补 action filter parity
- `/api/console/tasks` 已支持 `status_group=active|terminal|failed` 粗过滤，CLI 也已补对应 parity
- task 写盘时已同步更新 persisted `console_runtime_summary`
- persisted `console_runtime_summary` 已补 `recent_active_tasks / recent_terminal_tasks / recent_failed_tasks` 和 latest task markers
- task 写盘时也已同步更新 persisted `console_runtime_history`
- 已新增 `console show-runtime-state` 和 `/api/console/runtime-state`，可直接读取 runtime summary
- 已新增 `console show-runtime-history` 和 `/api/console/runtime-history`
- `console_home` payload 已开始吃 runtime summary，并补 runtime counts 给首页直接展示
- `runtime_state` / `runtime_history` payload 已补 `task_brief`、`latest_task_briefs`、`operator_summary`
- home 页已补 `Operator Board`、runtime warnings、invalid task files / truncated history 提示
- home 页已补 `Recent Activity`，开始承担 console-wide recent task activity 总览
- non-home section 已补 section-local recent tasks/history 面板
- recent task 行已可直接 drill down 到完整 task detail，并复用现有 action result / json 区展示 error / output path
- task detail / task row 已补 `subject_summary / primary_output_path / result_paths / linked_objects / error_detail` 摘要
- `training/runner.py` 已开始补更细一层阶段：`training_prepare / training_oof / training_artifacts / training_finalize`
- bundles / experiments 页面壳已补更明确的 row-linked workflow 和 task/result 提示

但当前还没有的是：

- 完整的交互式页面应用
- 完整的动作页面和更大范围的动作覆盖
- 更定制化的 section 页面和图表，而不只是通用卡片 / 表格 / JSON
- 登录、权限、多用户、持久会话这类完整 web app 能力

所以更准确的表述应是：

- `v2` 已经有了 console 基础层，包括只读视图和薄动作层
- 后续是在这个基础层上继续做页面和动作，不是从零新建一套系统

## 2.2 Data 域当前已具备的能力

当前 `pm15min.data` 已经有：

- canonical layout
- data summary service
- dataset audit
- source/table/export pipelines
- orderbook recording/runtime/fleet
- live foundation runtime

当前 `show-summary` 已能输出这些数据集的状态：

- Binance 1m source
- market catalog table
- direct oracle source
- settlement truth source
- oracle prices table
- truth table
- chainlink streams source
- chainlink datafeeds source
- orderbook index table
- orderbook depth source

其中已覆盖的核心信息：

- `status`
- `row_count`
- `partition_count`
- `time_range`
- `freshness_range`
- `date_range`
- `duplicate_count`
- `null_key_count`

这意味着你提的“每个币的数据要显示时间范围”，data 侧已经有很大一部分基础。

## 2.3 Label / Truth Runtime 当前已具备的能力

当前 label 侧已经不只是一个简单 `truth`。

代码里已有：

- `truth`
- `settlement_truth`
- `oracle_prices`
- `streams`
- `datafeeds`
- `chainlink_mixed`

对应能力包括：

- label frame build plan
- label source 过滤
- label source counts
- truth runtime summary
- truth runtime visibility
- foundation refresh 状态解释

这意味着：

- Data Overview 页面不应该只显示“truth 有/没有”
- 应该显式显示 label source / truth source / runtime freshness / fail-open 状态

## 2.4 Training 当前已具备的能力

当前训练不再只是“跑模型 + 写 joblib”。

训练 run 级产物现在至少包括：

- `summary.json`
- `report.md`
- `manifest.json`
- `logs/train.jsonl`

每个 offset 级产物现在至少包括：

- `summary.json`
- `metrics.json`
- `report.md`
- `feature_schema.json`
- `feature_cols.joblib`
- `feature_pruning.json`
- `probe.json`
- `oof_predictions.parquet`
- `models/logreg_sigmoid.joblib`
- `models/lgbm_sigmoid.joblib`
- `calibration/blend_weights.json`
- `calibration/reliability_bins.json`
- `calibration/reliability_bins_lgbm.json`
- `calibration/reliability_bins_logreg.json`
- `calibration/reliability_bins_blend.json`
- `reports/offset_report.md`

训练逻辑当前还已经包含：

- purged time-series split
- embargo / purge 控制
- sample weights
- class balance
- vol-based weighting
- contrarian weighting
- feature pruning
- final model probe

所以当前训练页应展示的内容，不能再停留在“rows + auc”这个级别。

## 2.5 Bundle 当前已具备的能力

当前 bundle 也不只是复制模型文件。

bundle 级产物现在包括：

- `summary.json`
- `report.md`
- `manifest.json`

每个 offset 下会复制：

- 核心推理文件
  - `feature_schema.json`
  - `feature_cols.joblib`
  - `models/*.joblib`
  - `calibration/blend_weights.json`

- 可选诊断文件
  - `diagnostics/metrics.json`
  - `diagnostics/feature_pruning.json`
  - `diagnostics/probe.json`
  - `diagnostics/oof_predictions.parquet`
  - `diagnostics/summary.json`
  - `diagnostics/report.md`
  - `reports/offset_report.md`
  - 以及额外 calibration 文件

bundle config 里现在还有：

- `allowed_blacklist_columns`
- `feature_columns`
- `required_feature_columns`
- `missing_feature_fill_value`

这意味着 Bundle Center 页面不应该只展示 bundle label 和 offsets，还应展示：

- 来源 training run
- copied diagnostics
- allowed blacklist columns
- offset summaries

## 2.6 Backtest 当前已具备的能力

当前 backtest 已经是一套统一的 replay pipeline，不是之前那种极简回放。

现在的 backtest 引擎包含：

- bundle replay
- raw depth replay
- canonical quote surface
- decision quote parity
- live state parity
- regime parity
- guard parity
- canonical fills
- retry contract
- hybrid fallback

当前 backtest run 级产物包括：

- `summary.json`
- `report.md`
- `manifest.json`
- `decisions.parquet`
- `trades.parquet`
- `rejects.parquet`
- `markets.parquet`
- `equity_curve.parquet`
- `logs/backtest.jsonl`

当前 backtest summary 已经包含的不只是：

- trades
- pnl
- roi
- 胜率

还包括：

- replay coverage
- raw depth usage
- decision quote stats
- retry contract stats
- truth runtime / label runtime 状态
- liquidity / regime 统计
- decision source counts
- hybrid secondary bundle 信息
- reject reason counts

所以回测页应该以当前 summary 为主，不需要再自己拼一套 coverage 统计。

## 2.7 Backtest 配置对象当前已具备的能力

`BacktestRunSpec` 当前已经支持：

- `profile`
- `spec_name`
- `run_label`
- `target`
- `bundle_label`
- `secondary_target`
- `secondary_bundle_label`
- `fallback_reasons`
- `variant_label`
- `variant_notes`
- `stake_usd`
- `max_notional_usd`
- `parity`

这里需要区分两个事实：

1. 后端对象层已经支持 stake / hybrid / parity。
2. 这些能力现在也已经暴露到基础 CLI / console action / console form。

也就是说：

- 能力已经在 engine/spec 层存在
- operator-facing CLI 已经补齐，接下来重点是把这些参数和 sweep 结果做成更清晰的页面化操作体验

## 2.8 Experiments 当前已具备的能力

这一块是上次文档明显低估的地方。

当前 `research.experiments` 已经支持：

- suite spec
- `stakes_usd` 扩展成 matrix cases
- `max_notional_usd`
- `hybrid_secondary_target`
- `hybrid_secondary_offsets`
- `hybrid_fallback_reasons`
- parity spec 透传
- compare policy
- runtime policy
  - `resume`
  - `rerun`
  - `skip`
- shared cache / reuse
- leaderboard
- compare frame
- matrix summary
- variant compare
- failed case tracking

experiment run 当前会落这些产物：

- `training_runs.parquet`
- `backtest_runs.parquet`
- `failed_cases.parquet`
- `leaderboard.parquet`
- `leaderboard.csv`
- `compare.parquet`
- `compare.csv`
- `matrix_summary.parquet`
- `matrix_summary.csv`
- `variant_compare.parquet`
- `variant_compare.csv`
- `summary.json`
- `report.md`
- `manifest.json`
- `logs/suite.jsonl`

所以“Experiments Matrix 页面”现在不应该只算可选扩展，而应该是现有代码已经可以直接接 UI 的一等页面。

---

## 3. 你的需求与当前代码的映射

## 3.1 数据：每个币、每种数据、显示时间范围

这个需求当前已基本有后端支撑。

现状：

- 已有 per-market / per-surface / per-cycle summary
- 已有 `time_range` / `freshness_range`
- orderbook source 已有 `date_range`
- truth runtime / foundation refresh 也已有摘要能力

还建议在 UI 上补出来的字段：

- label source counts
- truth runtime overall status
- recent refresh status
- fail-open / degraded 解释

## 3.2 数据：支持一键拉最新数据

这个需求当前可以直接复用现有 data CLI：

- `data sync ...`
- `data build ...`
- `data show-summary --write-state`

适合做成按钮的动作包括：

- 拉 Binance 最新 1m
- 拉 market catalog
- 拉 direct oracle prices
- 拉 settlement truth
- build oracle prices
- build truth
- refresh summary

orderbook 这里仍要保持你的原始要求：

- 原始 orderbook 还是手动去服务器补
- UI 只负责看覆盖范围、看缺口、看 index 是否完成

## 3.3 模型训练：每个币、每个 offset、因子、权重、训练进度、多并发

这里现在更准确地说，已经从“基础诊断”推进到了“解释层 + 受控并发 + progress”。

当前已经有的：

- 任意 offsets 训练
- per-offset summary
- pruning diagnostics
- sample weight summary
- split summary
- reliability bins
- probe
- run-level report
- 训练任务 progress state
- `logreg_coefficients.json`
- `lgb_feature_importance.json`
- `factor_correlations.parquet`
- `factor_direction_summary.json`
- `parallel_workers` 驱动的 offset 级受控并发

所以训练页面现在应该这样设计：

- 第一版就应该直接把 explainability artifact、progress、parallel worker 配置都展示出来
- 后续再补更图表化的 factor / weight operator 视图，而不是再去补底层产物

## 3.4 回测：offset 不限、金额不限、输出 pnl/roi/胜率

这个需求当前也要拆开看。

当前已经有的：

- bundle offsets 本身没有硬编码上限
- `BacktestRunSpec` 已支持 `stake_usd`、`max_notional_usd`
- experiments suite 已支持 `stakes_usd` matrix 展开
- backtest summary 已输出 pnl / roi / wins / losses / trades / rejects
- raw depth replay 和 hybrid fallback 已存在

当前已经补齐的：

- base CLI 直接暴露 stake / hybrid / parity 全量参数
- 单个 backtest run 自动产出 `stake_sweep.parquet`
- 单个 backtest run 自动产出 `offset_summary.parquet`
- 单个 backtest run 自动产出 `factor_pnl.parquet`
- console 已新增专门 stake sweep 结果页 API：`GET /api/console/backtests/stake-sweep`
- stake sweep 专门 payload 已补 `summary`、`stake_sweep_preview`、`surface_summary`、`highlights`、`chart_rows`、`rows_by_theme`

当前还没有的：

- operator-facing 可视化图表和矩阵对比

所以“金额 1 到不限制”目前更准确的现状是：

- 单值 `stake_usd` 已支持
- 多 stake matrix 已通过 experiments suite 支持
- stake sweep 已有正式只读结果页 contract，但前端专门页面仍待继续消费这条 API

## 3.5 因子的相关、正负向输出

当前这块已经落成正式产物。

现在已经有：

- factor vs label / score 的 correlation frame
- logreg coef
- lgb importance
- 因子正负向 summary

所以这块后续不再是“补产物”，而是“把这些对象更好地可视化出来”。

---

## 4. 建议的页面结构

按当前代码现状，建议页面从原来的 4 页改成 6 页。

## 4.1 Data Overview

读取：

- `v2/var/<surface>/state/summary/.../latest.json`
- `v2/var/<surface>/state/summary/.../latest.manifest.json`

展示：

- 数据覆盖范围
- freshness
- orderbook 日期覆盖
- truth runtime / foundation 状态
- label source counts

操作：

- 一键 sync/build/refresh summary

## 4.2 Training Runs

读取：

- `training_runs/.../run=<...>/summary.json`
- `training_runs/.../run=<...>/report.md`
- `training_runs/.../run=<...>/offsets/offset=<n>/summary.json`

展示：

- run 列表
- per-offset rows / positive_rate / metrics
- dropped features
- split summary
- weight summary
- reliability bins
- probe
- logreg coef
- lgb importance
- factor correlations
- factor direction summary
- progress / parallel workers

## 4.3 Bundle Center

读取：

- `model_bundles/.../bundle=<...>/summary.json`
- `model_bundles/.../bundle=<...>/report.md`
- `active_bundles/.../selection.json`

展示：

- bundle 列表
- source training run
- offsets
- allowed blacklist columns
- optional diagnostics copied
- 当前 active 状态

操作：

- build bundle
- activate bundle

## 4.4 Backtests

读取：

- `backtests/.../run=<...>/summary.json`
- `backtests/.../run=<...>/report.md`
- `backtests/.../run=<...>/decisions.parquet`
- `backtests/.../run=<...>/trades.parquet`
- `backtests/.../run=<...>/rejects.parquet`
- `backtests/.../run=<...>/equity_curve.parquet`
- `backtests/.../run=<...>/stake_sweep.parquet`
- `backtests/.../run=<...>/offset_summary.parquet`
- `backtests/.../run=<...>/factor_pnl.parquet`

展示：

- pnl / roi / win rate
- decisions / rejects / fills
- stake / offset / factor summary
- replay coverage
- raw depth usage
- decision quote stats
- retry stats
- truth runtime visibility
- liquidity / regime stats
- hybrid source counts

注意：

- 产品上可以分成 “Summary / Depth / Decisions / Runtime” 几个 tab
- 但底层应继续复用统一 backtest engine，不要拆成两套回测系统

## 4.5 Experiments Matrix

这一页建议升级为一等页面。

读取：

- `experiments/runs/.../training_runs.parquet`
- `experiments/runs/.../backtest_runs.parquet`
- `experiments/runs/.../leaderboard.parquet`
- `experiments/runs/.../compare.parquet`
- `experiments/runs/.../matrix_summary.parquet`
- `experiments/runs/.../variant_compare.parquet`
- `experiments/runs/.../summary.json`
- `experiments/runs/.../report.md`

展示：

- suite 运行结果
- stake matrix
- variant 对比
- hybrid variant 对比
- reused / resumed 情况
- leaderboard
- failed cases

如果你后面想做“不同金额、不同 offset、不同 target 的矩阵回测”，最适合落在这一页，而不是塞进单 run backtest 页面。

## 4.6 Evaluations

当前 `evaluate` 侧已经有：

- calibration
- drift
- poly-eval
- deep-otm-demo
- smc-demo
- copula-risk
- stack-demo

这页不是你当前最核心的需求，但后续可以直接接现有 outputs。

## 4.7 当前页面优先级

按“本地单用户、先看回测结果、先选出更好的权重/参数”这个目标，当前页面优先级建议明确为：

1. `Backtests`
   - 最先做成真正可用的结果页。
   - 重点展示 `pnl / roi / win rate / trades / rejects / equity_curve / stake_sweep / offset_summary / factor_pnl`。

2. `Experiments Matrix`
   - 第二优先级。
   - 重点回答“哪个 stake / offset / variant / bundle / fallback / parity 组合更好”。

3. `Training Runs`
   - 第三优先级。
   - 当前先展示 explainability artifact、offset 诊断、run summary、必要 progress。
   - 不优先做复杂训练过程监控 UI。

状态同步（`2026-03-24`）：

- `Backtests`、`Experiments`、`Training Runs` 三个高优先级 section 已经具备 selected-row detail workflow 和对应 read-model 预览字段。
- 页面壳、section copy、动作文案、任务 / 结果 / drilldown 提示已经统一中文化。
- `Backtests` 已经是结果优先 detail 页，`Experiments` 已经是比较优先 detail 页，`Training Runs` 维持轻量必要展示页。
- `Backtests` 结果页已经开始直接展示“结果摘要卡 + 扫参领先者 + 金额 / offset / 因子预览表”，用于更快挑出更好的金额和配置。
- `Experiments` 比较页已经开始直接展示“比较摘要卡 + 领先者摘要 + 按 market/group/run 的领先者预览 + 失败概览”，用于更快挑出更好的组合。
- `Training Runs` 页面已经补到“轻量闭环”层级：除了 explainability 之外，也开始直接回答 bundle readiness 和基础 metric summary。
- `Backtests` / `Experiments` 现在已经补上第一批轻量图表：资金曲线、金额 ROI、Offset PnL、因子 PnL、排行榜 ROI、矩阵最佳 ROI、变体增量。
- `Experiments` 现在已经开始支持更正式的“不同 feature_set 实验”入口：console 动作层支持 `existing | inline` 两种模式；`inline` 可直接生成 canonical suite spec，并用 `feature_set_variants` 展开多组因子集合实验；compare / leaderboard 输出也开始显式带 `feature_set`。
- 已新增专门结果页 API：`GET /api/console/backtests/stake-sweep` 与 `GET /api/console/experiments/matrix`，用于让前端直接消费 stake sweep / matrix 的正式结果对象，而不是再从通用 detail payload 中二次拆装。
- 以当前这版代码为准，文档里定义的高优先级已经完成：
  - `Backtests` 可用结果页
  - `Experiments Matrix` 可用比较页
  - `Training Runs` 轻量必要展示页
- 后续更合理的优先级，已经不再是“补高优先级缺口”，而是：
  - 图表质量继续精修
  - 更强的交互与筛选
  - 中低优先级页面和动作扩展

---

## 5. 当前适合直接做成 UI 的 canonical 路径

建议 UI 只依赖下面这些 canonical 对象：

### Data

- `v2/var/<surface>/state/summary/cycle=<cycle>/asset=<asset>/latest.json`
- `v2/var/<surface>/state/summary/cycle=<cycle>/asset=<asset>/latest.manifest.json`

### Research datasets

- `v2/research/feature_frames/cycle=<cycle>/asset=<asset>/feature_set=<feature_set>/source_surface=<surface>/data.parquet`
- `v2/research/label_frames/cycle=<cycle>/asset=<asset>/label_set=<label_set>/data.parquet`

### Training

- `v2/research/training_runs/cycle=<cycle>/asset=<asset>/model_family=<family>/target=<target>/run=<run_label>/`

### Bundles

- `v2/research/model_bundles/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/bundle=<bundle_label>/`
- `v2/research/active_bundles/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/selection.json`

### Backtests

- `v2/research/backtests/cycle=<cycle>/asset=<asset>/profile=<profile>/spec=<spec>/run=<run_label>/`

### Experiments

- `v2/research/experiments/runs/suite=<suite>/run=<run_label>/`

UI 不应该去依赖：

- legacy `data/markets/...`
- `var/quarantine/`
- 临时脚本输出

---

## 6. 当前主要剩余的能力

按当前代码，真正还缺的主要集中在产品层可视化收口。

## 6.1 还没有完整交互式 UI / dashboard 层

当前已经有：

- `console` CLI
- `console.service`
- stdlib JSON HTTP server
- sectioned API 路由
- 极简页面壳
- action catalog / action request facade
- thin action execution / action result
- section-local action bars / latest action result panels
- minimal async task contract / task API / async action polling
- runtime-level reporter hooks for training/backtest/experiment

但还没有：

- 更完整的 section 页面渲染
- 更成熟的图表 / tab / 对比视图
- 更完整的 row-linked action workflow
- 比当前 generic shell 更成型的 operator-facing dashboard

## 6.2 runtime 体系已经稳定，但还可以继续细化

当前已经有正式的 console task contract，包括：

- `task_id`
- `status`
- `current_stage`
- `progress_pct`
- `heartbeat`
- `started_at`
- `finished_at`

同时：

- `training/backtest/experiment` 已能透出基础 runtime progress
- 页面已能做“后台提交 + task 轮询 + 基础阶段展示”
- `console_runtime_summary` / `console_runtime_history` 已持久化
- runtime retention / invalid task files / truncated history warnings 已进入 operator summary
- task detail / recent activity / section-local history / drilldown 都已经接通

当前还值得继续补的主要是：

- 更细粒度的 experiment/training/backtest 内部阶段监控
- 更成熟的任务结果页排版和 section-specific operator copy
- 把 runtime history / warning / retention 做成更清晰的页面化入口，而不只是 JSON + 通用卡片

## 6.3 模型权重 / 因子相关性正式产物已经补齐

这一块已经补齐。训练 offset 目录现在会正式输出 `logreg_coefficients.json`、`lgb_feature_importance.json`、`factor_correlations.parquet`、`factor_direction_summary.json`，bundle 也会把这些 explainability 诊断文件一起复制到 `diagnostics/`，console training read-model 也已经能直接看到这些 artifact。

## 6.4 受控并发训练已经落地

这一块也已经补齐为“受控并发”而不是粗暴放开。`TrainingRunSpec` 现在支持 `parallel_workers`，`research train run --parallel-workers N` 和 console training action 都能直接驱动 offset 级并发；LightGBM 的 `n_jobs` 也不再写死为 `1`，而是按当前 parallel worker 预算自动分配 CPU。

`ExperimentRuntimePolicy` 现在支持 `parallel_case_workers`（兼容 `parallel_workers` / `max_parallel_cases` 别名），experiment suite 会先顺序预热共享 training / bundle cache，再对同一 execution group 中剩余 case 做并发 backtest，从而把并发加在最稳定的一层，而不让 console/task contract 继续漂移。

## 6.5 advanced backtest 参数已经暴露到基础 CLI

这一块也已经补齐。基础 `research backtest run` CLI、console action catalog、console backtest action form 现在都能直接透传 `stake_usd`、`max_notional_usd`、`secondary_bundle_label`、`fallback_reasons`、`parity`（通过 `parity_json`）到 `BacktestRunSpec`。

## 6.6 operator-facing sweep 结果对象已经补齐

单个 backtest run 现在已经会正式输出 `stake_sweep.parquet`、`offset_summary.parquet`、`factor_pnl.parquet`。这些对象已经进入 manifest outputs 和 console backtest read-model artifact map，后续 UI 要做单 run stake / offset / factor operator summary 时不需要再临时拼表。

---

## 7. 对这份可视化工作的推荐落地顺序

按当前代码，建议把后续工作收束成 3 个大阶段，而不是继续拆成很多细 phase。

## Phase A：Console 产品化收口

目标：

- 把现有分路由 API、页面壳、section 页面和 row-linked actions 收口成稳定 operator workflow
- 继续扩大现有动作覆盖，但坚持复用现有 CLI / package，不复制逻辑

这一阶段重点包括：

- `data_overview`、`training_runs`、`bundles`、`backtests`、`experiments` 的 section 页面继续做实
- action result 页面结构化
- row-linked quick actions 和 form defaults 继续收口
- task list/detail 继续从最小 contract 收口成 operator-facing 结果页

这一阶段已经完成的部分：

- `bundle build / activate` 已补更直接的 task/executor 接法
- task list/detail 已开始暴露 status/progress/result/error/request summaries
- `/api/console/tasks` 已支持按 `action_ids` 过滤，CLI `console list-tasks` 已补 action filter parity
- non-home section 已补 section-local recent tasks/history 面板
- bundles / experiments 页面壳已补一层更明确的 row-linked workflow 文案和 task polling 提示

当前这阶段真正还剩的，是把这些对象做成更定制化的 section 页面和图表，而不是继续补底层 contract。

如果按当前本地单用户需求继续排优先级，这一阶段内部也建议按下面顺序推进：

1. `Backtests` 结果页
2. `Experiments Matrix` 比较页
3. `Training Runs` 必要展示页

## Phase B：Task / Progress Runtime 稳定化

这一阶段已经完成：`console_runtime_summary` 与 `console_runtime_history` 已持久化，runtime retention / invalid task files / truncated history warnings 已进入 operator summary，并通过 `console show-runtime-state`、`console show-runtime-history`、`/api/console/runtime-state`、`/api/console/runtime-history` 统一暴露。

当前 home runtime board、console-wide recent activity、section-local recent tasks/history、recent task -> full task detail / error / output drilldown 都已落到同一套单 shell workflow 里，没有额外拆出新的 task 页面体系；training / backtest / experiment 也已经把 `current_stage / progress_pct / heartbeat` 以及更细一层的 stage telemetry 接到 runtime contract。

## Phase C：Explainability / Sweep / Concurrency

这一阶段已经把 explainability 底层产物补齐：training run / bundle / console read-model 现在都有 logreg coefficient、LightGBM importance、factor correlation、factor direction summary 这些稳定对象。

operator-facing stake / offset / factor sweep object 也已经落成 canonical parquet；并发能力则收口成 `TrainingRunSpec.parallel_workers` 与 `ExperimentRuntimePolicy.parallel_case_workers` 这两层稳定契约，训练和 experiment 都已经具备受控并发能力。

这一阶段最终保持了原先的约束：没有额外新造 explainability 页面，没有在 canonical object 之外临时拼 sweep 图表，也没有为了并发去破坏当前已经稳定下来的 console/task contract。

---

## 8. 多 Agent 并行开发方案

如果这件事要支持多 agent 并行开发，最重要的原则不是“按页面切”，而是“按当前稳定包边界切”。

当前 `v2` 最天然的并行 seams 是：

- `data`
- `research.training`
- `research.bundles`
- `research.backtests`
- `research.experiments`

真正高冲突的反而是：

- 顶层 CLI
- layout
- contracts
- 共享 config

所以并行方案应该按下面 7 个 workstream 拆。

### 8.0 统一执行基线

这部分是多 agent 开发的硬约束，不是建议项。

#### Agent 模型配置

所有并行 agent 统一使用：

- `gpt-5.4`
- `reasoning_effort=xhigh`

原因很简单：

- 当前任务横跨 `data / research / backtests / experiments`
- 共享 contract 多
- 很多地方不是“能跑就行”，而是要严格保持结构和语义一致

所以这里不追求便宜和快，优先追求：

- 边界清晰
- 改动可解释
- 少返工
- 少冲突

#### 代码质量要求

所有 agent 都要遵守下面这些要求：

1. 代码结构要优雅，优先沿用现有 `v2` 包边界，不新造横切大文件。
2. 代码要清晰，优先稳定门面 + 子模块实现，不把逻辑重新糊回单文件。
3. 不要创造屎山代码。
4. 不要为了赶进度绕开 canonical object 和现有 layout/manifest 体系。
5. 能放在 read-model / service / reports 这种已有分层里的，不要塞进 CLI handler。
6. UI/API 壳层只做装配，不复制训练、bundle、backtest 核心逻辑。
7. 新增字段和产物时，优先补 manifest / summary / report，而不是只在 UI 里临时拼。

#### 完成项更新要求

每个 agent 完成一个明确里程碑后，必须及时更新状态。

最少要更新 3 类信息：

1. 当前任务状态
   - `todo / doing / done / blocked`
2. 已完成的产物
   - 新增了什么文件
   - 改了什么 contract
   - 是否补了测试
3. 下一步
   - 还剩什么
   - 是否依赖其他 agent

推荐更新节奏：

- 每完成一个子任务就更新一次
- 每次改动共享 contract 后立刻更新一次
- 每天收尾至少更新一次

如果没有及时更新，多 agent 并行很容易出现：

- 重复开发
- contract 漂移
- 集成时才发现冲突
- 无法判断谁已经完成了什么

所以“及时更新”在这里不是协作文档装饰，而是并行开发能不能成立的前提。

统一更新位置：

- `v2/docs/EXECUTION_BOARD.md`

不要把 agent 完成情况分散记在多个文档里。

### 8.1 Agent 0：Console Shell / API Scaffold

职责：

- 新建控制台壳层
- 路由
- read-model 组装
- 页面/API 骨架

建议独占目录：

- 新包 `v2/src/pm15min/console/`

要求：

- 不要把 UI/API 逻辑塞进 `data/` 或 `research/`
- 不要改训练 / 回测内核

依赖：

- 各域 agent 先冻结 summary / artifact contract

### 8.2 Agent 1：Data Overview

职责：

- 数据总览
- freshness / coverage
- orderbook 覆盖
- foundation / truth runtime 可视化
- 一键 sync/build/refresh summary 所需 read-model

建议独占目录：

- `v2/src/pm15min/data/service/`
- `v2/src/pm15min/data/pipelines/`
- `v2/src/pm15min/data/cli/`

适合直接消费的现有对象：

- `data_surface_summary`
- `latest.json`
- `latest.manifest.json`

依赖：

- 无，可最先开工

### 8.3 Agent 2：Training Diagnostics

职责：

- 训练页 read-model
- per-offset 诊断展示
- explainability artifact 暴露
- training progress / parallel worker 可视化

建议独占目录：

- `v2/src/pm15min/research/training/`
- `v2/src/pm15min/research/datasets/`
- `v2/src/pm15min/research/labels/`

当前直接可用对象：

- `training_runs/.../summary.json`
- `offsets/.../summary.json`
- `feature_pruning.json`
- `probe.json`
- `reliability_bins*.json`
- `logreg_coefficients.json`
- `lgb_feature_importance.json`
- `factor_correlations.parquet`
- `factor_direction_summary.json`

依赖：

- Data 只作为前置数据存在性条件
- 代码层可并行

### 8.4 Agent 3：Bundle Center

职责：

- bundle 列表与详情
- active bundle
- source training run 追溯
- bundle compare

建议独占目录：

- `v2/src/pm15min/research/bundles/`
- `v2/src/pm15min/research/service.py`

当前直接可用对象：

- `bundle summary/report/manifest`
- `active bundle selection`

依赖：

- 与 Training Diagnostics 共享 contract
- 但大部分只读能力可并行开发

### 8.5 Agent 4：Backtest Console

职责：

- backtest 总览
- depth usage
- decisions / rejects / trades
- runtime parity / truth freshness 可视化
- stake / offset / factor sweep 可视化

建议独占目录：

- `v2/src/pm15min/research/backtests/`

当前直接可用对象：

- `backtest summary/report/manifest`
- `decisions.parquet`
- `trades.parquet`
- `rejects.parquet`
- `equity_curve.parquet`
- `stake_sweep.parquet`
- `offset_summary.parquet`
- `factor_pnl.parquet`

依赖：

- Bundle contract
- Data 的 orderbook coverage 只需只读接入

### 8.6 Agent 5：Experiments Matrix

职责：

- suite matrix
- stake matrix
- hybrid variant compare
- leaderboard
- failed / resumed / reused cases

建议独占目录：

- `v2/src/pm15min/research/experiments/`

当前直接可用对象：

- `training_runs.parquet`
- `backtest_runs.parquet`
- `leaderboard.parquet`
- `compare.parquet`
- `matrix_summary.parquet`
- `variant_compare.parquet`

依赖：

- read-only 页面可直接并行开发
- 深度交互只依赖 training/bundle/backtest contract

### 8.7 Agent 6：Shared Contracts / Integration

这是唯一一个必须收口共享热点文件的 agent。

职责：

- 统一 contract
- 顶层接线
- 跨域测试
- 最终集成

建议独占目录 / 文件：

- `v2/src/pm15min/cli.py`
- `v2/src/pm15min/core/config.py`
- `v2/src/pm15min/data/layout/__init__.py`
- `v2/src/pm15min/research/layout.py`
- `v2/src/pm15min/research/contracts.py`
- 顶层 CLI parser / shared config 接线文件

要求：

- 这些文件不能多 agent 同时写
- 否则冲突会非常高

### 8.8 推荐依赖顺序

建议这样排：

1. `Agent 1` 先做 Data Overview contract
2. `Agent 2`、`Agent 3`、`Agent 4`、`Agent 5` 并行做各自 read-model 和页面/接口
3. `Agent 0` 同步搭 console shell / API 骨架
4. `Agent 6` 最后统一接线和补跨域测试

更简化地写：

- `Agent 1 -> Agent 0`
- `Agent 2 -> Agent 3 -> Agent 4 -> Agent 5 -> Agent 0`
- `Agent 6` 最后集成

### 8.9 每个 Agent 的写权限规则

为了降低 merge risk，建议严格执行这些规则：

1. 每个 agent 只改自己负责的目录和自己的测试文件。
2. `layout / contracts / cli / shared config` 只允许 `Agent 6` 改。
3. `live/` 这期尽量只读，不分配写权限。
4. 新控制台壳层单独放 `console/` 包，不要散落回业务域。
5. 先冻结 artifact contract，再让 shell/front-end agent 接 UI。

### 8.10 最适合并行的第一批任务

如果你要回到项目早期启动多 agent，我建议第一批只开这 6 个任务：

1. `Data Overview`
2. `Training Runs Read Model`
3. `Bundle Center Read Model`
4. `Backtests Read Model`
5. `Experiments Matrix Read Model`
6. `Console Shell / API Scaffold`

这批任务现在已经完成。当前更适合并行推进的是：

1. section 页面结构化渲染
2. explainability / sweep 的页面化展示
3. 更完整的动作页面和 row-linked workflow
4. experiment / runtime 的更细粒度 operator 视图

这些任务的 owner、状态、blocker 和完成项，应统一维护在：

- `v2/docs/EXECUTION_BOARD.md`

### 8.11 多 Agent 第二阶段任务

按当前代码，下一批更适合并行推进的任务是：

1. `training / backtest / experiment` 的 section-specific 图表和 summary cards
2. explainability / stake / offset / factor sweep 的页面化 operator 视图
3. 更细一层的 experiment telemetry 与 case-level runtime surface
4. 更完整的动作覆盖和 workflow polish

这些任务现在主要属于 UI/product 收口层，而不是继续补底层研究产物。

---

## 9. 当前最重要的设计约束

1. UI 只读 canonical objects，不读 legacy 和临时目录。
2. training run、bundle、backtest run、experiment run 必须继续分开。
3. 不要为 UI 重写 research/backtest 逻辑。
4. 先复用已有 summary/report/parquet，再补新 artifact。
5. “多金额、多 variant、多 target” 优先落在 experiments 页面，而不是塞进单 run backtest 页面。

---

## 10. 最终判断

如果按当前代码来定范围，这个可视化功能不该再被定义为：

- “先做一个训练 + 回测页面”

而应该定义为：

- “给 `data`、`training_runs`、`model_bundles`、`backtests`、`experiments` 这 5 类 canonical 对象做统一控制台”

现在最值钱的不是继续补一堆新算法，而是把已经存在的：

- 数据覆盖
- 训练诊断
- bundle 诊断
- depth replay
- hybrid fallback
- matrix/leaderboard

先可视化出来。

这样做，后面要补的就不再是底层 contract，而是：

- 图表和 tab 视图
- 更完整的 operator workflow
- 更成熟的页面化展示

推进路径会清晰很多。
