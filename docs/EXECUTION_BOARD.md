# Execution Board

这份文档是 `v2` 可视化训练 / 回测控制台项目的唯一并行执行板。

作用只有一个：

- 让多 agent 并行开发时，所有人都在同一个地方更新 owner、状态、已完成项、下一步和 blocker

如果和聊天记录、临时备注、口头同步冲突，以这份执行板为准。

---

## 1. 全局规则

### 1.1 Agent 配置

所有并行 agent 统一使用：

- `gpt-5.4`
- `reasoning_effort=xhigh`

### 1.2 代码要求

所有 agent 必须遵守：

1. 保持 `v2` 现有包边界，不新造横切大文件。
2. 代码结构清晰，优先稳定门面 + 子模块实现。
3. 不创造屎山代码。
4. 不绕开 canonical object、layout、manifest、summary 体系。
5. UI/API 只做装配，不复制 `data / research / backtests` 核心逻辑。
6. 新增字段和产物时，优先补 `summary / report / manifest / tests`。

### 1.3 更新要求

每个 agent 完成一个明确里程碑后，必须立即更新本执行板。

必须更新：

- `Status`
- `Completed`
- `Next`
- `Blockers`
- `Last Update`

最小状态集合：

- `todo`
- `doing`
- `blocked`
- `done`

---

## 2. 共享热点文件

这些文件默认只允许 `Agent 6` 修改：

- `v2/src/pm15min/cli.py`
- `v2/src/pm15min/core/config.py`
- `v2/src/pm15min/data/layout/__init__.py`
- `v2/src/pm15min/research/layout.py`
- `v2/src/pm15min/research/contracts.py`
- 顶层 CLI parser / shared config 接线文件

如果其他 agent 必须改这些文件，先在本执行板里标记，再协调 ownership。

---

## 3. 当前阶段

当前默认阶段：

- `Phase 3 页面产品化收口`
- 目标：在已完成的分路由 API + 页面壳 + task/runtime/read-model 基础上，优先完成中文页面化，以及 `Backtests > Experiments > Training` 的结果页 / 比较页 / 必要展示收口

当前阶段范围：

- 中文 section 页面文案和 operator copy
- `Backtests` result-first 页面
- `Experiments` compare-first 页面
- `Training Runs` 必要展示页
- selected-row detail workflow
- section-local actions / recent tasks / runtime drilldown 继续收口
- 保持现有 read-model / task / runtime contract 稳定可用

当前阶段暂不提前做：

- 登录 / 多用户 / 权限
- 复杂实时训练监控 dashboard
- explainability / stake / offset sweep 新底层产物
- 为并发或 UI 重写 training / experiment / backtest 内核

本轮执行重点：

1. `中文页面化`
2. `Backtests` 结果页
3. `Experiments` 比较页
4. `Training Runs` 必要展示

本轮已完成：

- `Backtests` 已接通 result-first detail workflow，选中行后自动拉 detail，并优先读 performance / equity / sweep / parity 预览
- `Experiments` 已接通 compare-first detail workflow，选中行后自动拉 detail，并优先读 best case / matrix / variant / compare 预览
- `Training Runs` 已接通最小必要 detail workflow，优先展示 run scope、offset preview、explainability overview
- 高优先级 section 对应 read-model 预览字段已经补齐到页面层可直接消费的程度
- 页面壳、section copy、动作文案、任务 / 结果 / drilldown 提示已经统一中文化
- `Backtests` detail 页已继续收口为更偏结果决策的页面：新增结果摘要卡、扫参领先者、金额领先榜、offset 领先榜、正负向因子榜
- `Experiments` detail 页已继续收口为更偏比较决策的页面：新增比较摘要卡、领先者摘要卡、最佳组合摘要、按 market/group/run 的领先者预览、失败概览
- `Training Runs` detail 页已继续收口为轻量闭环页：新增运行摘要卡、解释性覆盖、打包就绪度，并开始直接回答是否能顺畅进入 bundle/backtest 链路
- `Backtests` / `Experiments` 已进入轻量图表阶段：新增资金曲线图、金额 ROI 图、Offset PnL 图、因子 PnL 图，以及排行榜 ROI 图、矩阵最佳 ROI 图、变体增量图
- `Backtests` / `Experiments` detail 页已补最小页面内筛选：支持 `仅看前 N`，并按关键词 / 市场 / 运行名 / 变体做局部过滤
- 主表 `buildTable` 与 detail 预览表 `renderPreviewTableCard` 已补列点击排序，页面开始具备基础 operator 交互能力
- `Experiments` 已补正式的 `feature_set` 实验入口一层：suite spec 支持 `feature_set_variants`，compare / leaderboard 输出已开始显式带 `feature_set`
- `Experiments` 动作层已补正式入口：`research_experiment_run_suite` 支持 `existing | inline` 两种模式；`inline` 可由 console 直接生成 canonical suite spec，并支持 `feature_set_variants`、`stakes_usd` matrix、runtime/compare policy
- 已新增专门结果页只读 API：`GET /api/console/backtests/stake-sweep` -> `console_backtest_stake_sweep_detail`，`GET /api/console/experiments/matrix` -> `console_experiment_matrix_detail`
- 专门结果页 payload 已按前端约定补齐：身份字段、`summary`、主表 preview、`surface_summary`、`highlights`、`chart_rows`、`rows_by_theme`

本轮进行中：

- 高优先级收口已经完成，当前不再把 `Backtests / Experiments / Training Runs` 记为高优先级未完成项

本轮下一步：

- 下一阶段转到高优先级后的精修项：前端继续消费新结果页 API，收口专门 stake sweep / matrix 页面，以及图表质量和交互筛选

---

## 4. Agent 总览

| Agent | Workstream | Status | Owned Paths | Depends On | Main Output |
| --- | --- | --- | --- | --- | --- |
| 0 | Console Shell / API Scaffold | doing | `v2/src/pm15min/console/` | 1,2,3,4,5 | 控制台壳层 / 路由 / API / 页面产品化收口 |
| 1 | Data Overview | done | `v2/src/pm15min/data/service/`, `v2/src/pm15min/data/pipelines/`, `v2/src/pm15min/data/cli/` | none | 数据总览 read-model |
| 2 | Training Diagnostics | done | `v2/src/pm15min/research/training/`, `v2/src/pm15min/research/datasets/`, `v2/src/pm15min/research/labels/` | data contract | 训练页 read-model |
| 3 | Bundle Center | done | `v2/src/pm15min/research/bundles/`, `v2/src/pm15min/research/service.py` | training contract | bundle read-model / active bundle |
| 4 | Backtest Console | done | `v2/src/pm15min/research/backtests/` | bundle contract, data coverage | backtest read-model |
| 5 | Experiments Matrix | done | `v2/src/pm15min/research/experiments/` | training/bundle/backtest contract | matrix / leaderboard / compare |
| 6 | Shared Contracts / Integration | done | shared hotspots only | 0,1,2,3,4,5 | contract 收口 / 顶层接线 / 测试 |

---

## 5. Agent 详细板

## 5.0 Agent 0

- Workstream: `Console Shell / API Scaffold`
- Status: `doing`
- Owner Paths:
  - `v2/src/pm15min/console/`
- Completed:
  - 新建 `v2/src/pm15min/console/` 包骨架
  - 新建 `console/cli/` 基础文件
  - 新建 `console/read_models/` 基础包
  - 接入 `data/training/bundle/backtest/experiment` read-model
  - 新增 `console/service.py` 稳定门面
  - 新增 `console/http/` stdlib JSON server
  - 新增 `console show-home`
  - 新增 `console serve`
  - 新增分路由 API：`/api/console/home`、`/data-overview`、`/training-runs`、`/bundles`、`/backtests`、`/experiments`
  - 新增 `console/web/` 极简 HTML/CSS/JS 页面壳
  - 新增 `console/actions.py` 动作 catalog / request facade
  - 新增 `console/action_runner.py` 薄执行层
  - 页面壳已真实拉取 section API 并展示 summary/table/json
  - 新增 `/api/console/actions`
  - 新增 `POST /api/console/actions/execute`
  - 首页已接通四个真实动作按钮：`data refresh summary`、`bundle activate`、`research backtest run`、`research experiment run-suite`
  - 各 section 已接通局部动作区：`data overview`、`training runs`、`bundles`、`backtests`、`experiments` 可在本 section 触发动作并查看最近结果
  - `training runs` 已接通 `train run` 局部表单
  - `bundles` 已接通 `bundle build` 局部表单
  - `backtests` 已接通 `backtest run` 局部表单
  - `experiments` 已接通 `experiment run-suite` 局部表单
  - section list/detail payload 已补 `action_context`，页面可用选中行作为默认参数
  - action catalog 已补 section metadata / shell enabled / form fields，可按 section 过滤
  - 页面层 action form 已改为由 action catalog metadata 驱动渲染，避免继续在 HTML 模板里复制字段
  - 已新增 `Selected Row Quick Actions`，可直接基于当前选中行触发本 section 推荐动作
  - 新增 `console/tasks.py` 最小后台 task contract
  - 新增 `/api/console/tasks`
  - `console execute-action --execution-mode async` 已支持后台任务提交
  - 新增 `console list-tasks` / `console show-task`
  - 长动作已支持 async submit + task polling：页面可对 `train/backtest/experiment` 默认走后台任务
  - task progress contract 已补 `current_stage / progress_pct / heartbeat / started_at / finished_at`
  - `console task` 默认执行器已对 `research_train_run` / `research_backtest_run` 直连 runner，而不是只回退 CLI
  - `console task` 默认执行器已对 `research_bundle_build` / `research_activate_bundle` 直连 Python entrypoint，而不是只回退 CLI
  - `training/runner.py` 已接通 reporter，按 offset 和 OOF fold 发 progress / heartbeat
  - `backtests/engine.py` 已接通 reporter，按阶段发 progress；`depth_replay.py` 已补长扫描 heartbeat
  - `experiments/runner.py` 已接通 reporter，按 suite/group/case 发 progress，并透传 training/backtest 子进度
  - `training/runner.py` 已补更细一层阶段：`training_prepare / training_oof / training_artifacts / training_finalize`
  - `data sync` / `data build` 已接入 shell-enabled + async metadata 流
  - `data_overview` 已接通基于 `dataset_rows` 的 selected-row quick actions 和 action context
  - `data sync` / `data build` 已接入页面按钮文案、缺参提示和 quick-action 摘要
  - `/api/console/tasks` list/detail payload 已补 `status_label / progress_summary / result_summary / error_summary / request_summary / filters / status_counts`
  - `/api/console/tasks` 已支持 `action_ids` 多动作过滤
  - `/api/console/tasks` 已支持 `status_group=active|terminal|failed` 粗过滤
  - `console list-tasks` 已补 `--action-id` filter parity
  - `console list-tasks` 已补 `--status-group` filter parity
  - task 写盘时已同步更新 persisted `console_runtime_summary`
  - persisted `console_runtime_summary` 已补 `recent_active_tasks / recent_terminal_tasks / recent_failed_tasks` 和 latest task markers
  - 新增 `console show-runtime-state` 和 `/api/console/runtime-state`，可直接读取 persisted runtime summary
  - `console_home` payload 已开始吃 runtime summary，并补 `runtime_task_count / active_task_count / terminal_task_count / failed_task_count`
  - home 页已补 `Recent Activity` 总览，直接显示 console-wide recent task activity
  - non-home section 已补 `Recent Tasks` 子面板，按当前 section action ids 拉最近 task/history
  - bundles / experiments 页面壳已补更明确的 row-linked workflow copy、task polling 提示和 action result lead copy
  - recent task 行已可 drill down 到完整 task detail，并继续复用现有 action result / json 区做 error / output path 展示
  - task detail / task row 已补 `subject_summary / primary_output_path / result_paths / linked_objects / error_detail` 摘要字段
  - persisted runtime state 已补 `console_runtime_history`，除 `runtime_summary` 外再落独立 history artifact 和恢复逻辑
  - runtime recent row 已补 `task_path / error_detail / result_paths / linked_objects`，方便 operator drilldown
  - `load_console_runtime_state` 已补 `task_briefs / latest_task_briefs / operator_summary / runtime_board`
  - `console list-tasks` / `/api/console/tasks` 已补 `marker / group_by / history_markers / history_groups / task_brief`
  - web 壳层已把 home runtime board、active/failed highlights、linked object drilldown、error drilldown 做成更清晰的 operator 视图
  - `runtime_history` 已正式暴露到 service / section query / HTTP / CLI：`load_console_runtime_history`、`/api/console/runtime-history`、`console show-runtime-history`
  - persisted runtime summary / history 已补 retention metadata，明确 `row_limit / group_row_limit / retained_task_count / dropped_task_count / is_truncated`
  - runtime state / runtime history operator summary 已补 invalid task files 和 truncated history 提示字段
  - home runtime board 已补 runtime warnings，直接提示 invalid task files 和 truncated history window
  - `Phase B` runtime 稳定化已收口到单 shell：persisted runtime state/history、recent activity、section-local history、task drilldown、runtime warnings、细粒度 telemetry 已全部接通
  - training offset 已正式输出 `logreg_coefficients.json`、`lgb_feature_importance.json`、`factor_correlations.parquet`、`factor_direction_summary.json`
  - bundle diagnostics 已开始复制 explainability artifact；console training read-model 已能直接暴露这些文件
  - `research backtest run` / console backtest action 已补 `stake_usd`、`max_notional_usd`、`secondary_bundle_label`、`fallback_reasons`、`parity_json`
  - backtest run 已正式输出 `stake_sweep.parquet`、`offset_summary.parquet`、`factor_pnl.parquet`，并接入 manifest / console read-model artifact map
  - `TrainingRunSpec.parallel_workers` + `research train run --parallel-workers` 已补 offset 级受控并发；LightGBM `n_jobs` 已按 worker budget 自动分配
  - `ExperimentRuntimePolicy.parallel_case_workers` 已补 execution-group 内的受控并发 backtest，先预热共享 cache，再并发跑剩余 case
  - `Backtests` section 已开始从 generic table/json 收口到 result-first detail workflow：选中行后自动拉 detail，并把 performance / sweep / equity / parity 读成 section-specific card 视图
  - `Experiments` section 已开始从 artifact inventory 收口到 compare-first detail workflow：选中行后自动拉 detail，并把 best case / matrix / variant / compare facets 做成 section-specific card 视图
  - `Training Runs` section 已补最小必要 detail workflow：选中行后自动拉 detail，并优先展示 run scope、offset preview、explainability overview，而不是继续把 training monitor 做重
  - backtest read-model 已补 `result_summary / comparison_axes / overview_cards / artifact_previews / equity_curve_preview / stake_sweep_preview / offset_summary_preview / factor_pnl_preview`
  - experiment read-model 已补 `comparison_overview / best_case / best_matrix / best_variant / leaderboard_preview / compare_preview / matrix_summary_preview / variant_compare_preview / compare_facets`
  - training read-model 已补 `run_overview / offset_preview / explainability_overview / overview_cards`
  - 本轮页面化第一批已收口到高优先级 section：`Backtests`、`Experiments`、`Training Runs` 都已具备 selected-row detail auto-fetch 和 section-specific detail renderer
  - 当前执行重心已从“底层 contract / task runtime 补齐”切换到“中文页面化 + 结果页 / 比较页产品化”
  - `console/web/page.py`、`console/web/assets.py`、`console/service.py`、`console/actions.py` 的页面文案、子面板标题、动作文案、operator copy 已统一中文化
  - `Backtests` / `Experiments` / `Training Runs` 的 detail renderer 已统一改成中文结果页表达，任务 / 动作 / drilldown 提示也已中文化
  - `Experiments` read-model 已补 `best_by_group_preview / best_by_market_group_preview / leaderboard_surface_summary / best_combo_summary / variant_surface_summary / failure_overview`
  - `research/experiments/specs.py` 已补 `feature_set_variants` 展开能力；`reports.py / leaderboard.py` 已把 `feature_set` 带进 experiment compare / leaderboard / summary 输出
  - `Training Runs` read-model 已补 `bundle_readiness / metric_summary`，并把 offset preview / explainability overview 补到更适合 bundle/backtest 决策
  - `console/web/assets.py` 已补结果摘要卡、领先者摘要卡、失败概览、打包就绪度，以及轻量 SVG 图表组件、页面内筛选和表格排序装配
  - 验证 `test_console_web + test_console_actions + test_console_service + test_console_http_routes + test_console_cli` `22 passed`
  - 验证 `test_console_web + test_console_research_assets + test_console_analysis_runs + test_console_service + test_console_http + test_console_http_routes + test_console_cli` `27 passed`
  - 新增 `console show-actions`
  - 新增 `console build-action`
  - 新增 `console execute-action`
  - 接线顶层 `pm15min` CLI `console` 域
  - 新增 `v2/tests/test_console_cli.py`
  - 验证本轮 console runtime / service / http / web / cli 子集测试 `39 passed`
- In Progress:
  - 高优先级项已完成，当前进入高优先级后的图表精修与交互增强阶段
- Next:
  - 继续精修 `Backtests` / `Experiments` 图表质量、坐标说明与筛选交互
  - 把 `feature_set` 实验入口继续从 suite spec 能力推进到更顺手的产品入口
  - 视你的新任务切换到下一批并行开发目标
  - 保持 console web / service / http / cli 回归测试持续通过
- Blockers:
  - none
- Last Update:
  - `2026-03-24 15:28`

## 5.1 Agent 1

- Workstream: `Data Overview`
- Status: `done`
- Owner Paths:
  - `v2/src/pm15min/data/service/`
  - `v2/src/pm15min/data/pipelines/`
  - `v2/src/pm15min/data/cli/`
- Completed:
  - 新增 `console/read_models/data_overview.py`
  - 新增 `console/read_models/common.py`
  - 新增 `v2/tests/test_console_data_overview.py`
  - 覆盖 persisted summary fallback + flattened dataset rows
  - 验证 `3 passed`
- In Progress:
  - none
- Next:
  - Phase 2 再补 data action routing / richer operator API
- Blockers:
  - none
- Last Update:
  - `2026-03-23`

## 5.2 Agent 2

- Workstream: `Training Diagnostics`
- Status: `done`
- Owner Paths:
  - `v2/src/pm15min/research/training/`
  - `v2/src/pm15min/research/datasets/`
  - `v2/src/pm15min/research/labels/`
- Completed:
  - 新增 `console/read_models/training_runs.py`
  - 新增 `v2/tests/test_console_research_assets.py`
  - 提供 training run list/detail read-model
  - 暴露 offset summary / pruning / probe / calibration 读取
  - 已补 `overview_cards / run_overview / offset_preview / explainability_overview`
  - 验证 `test_console_research_assets.py` 通过
- In Progress:
  - none
- Next:
  - 如 `Training Runs` 页面化仍缺摘要字段，再按需补轻量 read-model；默认不扩成重训练监控
- Blockers:
  - none
- Last Update:
  - `2026-03-24 11:39`

## 5.3 Agent 3

- Workstream: `Bundle Center`
- Status: `done`
- Owner Paths:
  - `v2/src/pm15min/research/bundles/`
  - `v2/src/pm15min/research/service.py`
- Completed:
  - 新增 `console/read_models/bundles.py`
  - bundle list/detail read-model 已完成
  - active bundle selection 已接入 detail payload
  - diagnostics / offset artifact inventory 已暴露
  - 验证 `2 passed`
- In Progress:
  - none
- Next:
  - Phase 2 再补 bundle compare / bundle diff
- Blockers:
  - none
- Last Update:
  - `2026-03-23`

## 5.4 Agent 4

- Workstream: `Backtest Console`
- Status: `done`
- Owner Paths:
  - `v2/src/pm15min/research/backtests/`
- Completed:
  - 新增 `console/read_models/backtests.py`
  - 新增 `v2/tests/test_console_analysis_runs.py`
  - backtest list/detail read-model 已完成
  - decisions/trades/rejects/equity artifact inventory 已暴露
  - 已补 `result_summary / comparison_axes / overview_cards`
  - 已补 `artifact_previews / equity_curve_preview / stake_sweep_preview / offset_summary_preview / factor_pnl_preview`
  - 验证 `test_console_analysis_runs.py` 通过
- In Progress:
  - none
- Next:
  - 如 `Backtests` 结果页仍缺预览字段，再按需补 read-model，不在 domain 内复制页面逻辑
- Blockers:
  - none
- Last Update:
  - `2026-03-24 11:39`

## 5.5 Agent 5

- Workstream: `Experiments Matrix`
- Status: `done`
- Owner Paths:
  - `v2/src/pm15min/research/experiments/`
- Completed:
  - 新增 `console/read_models/experiments.py`
  - experiment list/detail read-model 已完成
  - leaderboard / compare / matrix_summary / variant_compare artifact inventory 已暴露
  - `experiments/runner.py` 已接通 suite/group/case progress，并透传 training/backtest 子进度
  - experiment runtime progress 已保证单调推进，兼容 resume / rerun / failed case 场景
  - 已补 `comparison_overview / best_case / best_matrix / best_variant`
  - 已补 `leaderboard_preview / compare_preview / matrix_summary_preview / variant_compare_preview / failed_cases_preview / compare_facets`
  - 验证 `test_console_analysis_runs.py` 通过
- In Progress:
  - none
- Next:
  - 如 `Experiments` 比较页仍缺 compare 预览字段，再按需补 read-model，不在 domain 内复制页面逻辑
  - 再视需要补更细粒度的 case-level operator surface
- Blockers:
  - none
- Last Update:
  - `2026-03-24 11:39`

## 5.6 Agent 6

- Workstream: `Shared Contracts / Integration`
- Status: `done`
- Owner Paths:
  - shared hotspots only
- Completed:
  - `pm15min.cli` 已接入 `console` 域
  - console CLI handler dispatch 已完成
  - 共享热点文件未发生多 agent 冲突
  - console 相关测试已覆盖 `read_models / service / http / web / actions / action_runner / cli`
  - 现有顶层 CLI 测试 `46 passed`
- In Progress:
  - none
- Next:
  - Phase 2 再收口 API / progress / explainability 共享 contract
- Blockers:
  - none
- Last Update:
  - `2026-03-23`

---

## 6. 里程碑定义

## Milestone A

- 每个域的 read-model contract 冻结
- 所有 agent owner 路径确认
- 无共享热点冲突

## Milestone B

- 5 个只读页面后端 read-model 准备完成
- console 壳层可以读取并展示 canonical outputs

## Milestone C

- 顶层集成完成
- `console` 已具备分路由 API、页面壳、动作目录和薄执行层
- 关键测试补齐
- 下一阶段转入后台任务层 / progress / explainability / sweep

---

## 7. 更新模板

每次更新请按这个模板追加或改写对应 agent 区域：

```md
- Status: `doing`
- Completed:
  - 完成 xxx
  - 新增文件 xxx
  - 补测试 xxx
- In Progress:
  - 正在做 xxx
- Next:
  - 下一步做 xxx
- Blockers:
  - 依赖 Agent N 冻结 xxx contract
- Last Update:
  - `YYYY-MM-DD HH:mm`
```

---

## 8. 首条执行记录

- `2026-03-24`
  - 当前阶段正式切到 `Phase 3 页面产品化收口`
  - 本轮优先级明确为：`中文页面化`、`Backtests` 结果页、`Experiments` 比较页、`Training Runs` 必要展示
  - `Backtests` / `Experiments` / `Training Runs` 已具备 selected-row detail workflow 和对应 read-model 预览字段
  - 页面中文化已经完成，当前主要剩余工作转为 `Backtests` / `Experiments` 结果面的图表化和可读性收口
- `2026-03-23`
  - 建立执行板
  - 初始化 7 个 agent 的 owner、状态、里程碑和更新模板
- `2026-03-23`
  - Phase 1 只读 console read-model 完成
  - `console` 域已接入顶层 CLI
  - `console.service` 与 `console.http` 已完成
  - Phase 2 已启动：sectioned API + web shell + action facade/thin execution 已完成
  - 页面壳已接通 section API
  - `/api/console/actions` 与 `POST /api/console/actions/execute` 已完成
  - `console show-actions / build-action / execute-action` 已完成
  - 首页动作按钮已接通 `data refresh summary`、`bundle activate`、`research backtest run`、`research experiment run-suite`
  - console 相关测试已覆盖 `read_models / service / http / web / actions / action_runner / cli`
  - `data overview`、`training runs`、`bundles`、`backtests`、`experiments` section 已有局部动作条和最近动作结果面板
  - `training runs`、`bundles`、`backtests`、`experiments` 已有局部表单
  - read-model 已补 `action_context`，页面已支持选中行作为 section 默认参数
  - action catalog 已支持按 section / shell enabled 过滤，并暴露表单字段元数据
  - 页面 action form 已改为 metadata-driven 渲染
  - 已新增 `Selected Row Quick Actions`
  - 已新增后台 task contract、`/api/console/tasks`、CLI task 命令与页面 async polling
  - task progress contract 已补 `current_stage / progress_pct / heartbeat / started_at / finished_at`
  - `console task` 已直连 `training/backtest/experiment` runner，开始吃到真实 runtime progress
  - `data sync` / `data build` 已进入统一 metadata-driven + async task 流
  - `data_overview` 已接通 selected-row quick actions 和 data sync/build row-linked workflow
  - `experiments/runner.py` 已接通 suite/group/case progress，并透传 training/backtest 子进度
  - `console task` 已对 `bundle build / activate bundle` 直连 Python entrypoint
  - `/api/console/tasks` list/detail payload 已补 status/progress/result/error/request summaries
  - `/api/console/tasks` 已支持 `action_ids` 多动作过滤
  - `/api/console/tasks` 已支持 `status_group` 粗过滤
  - `console list-tasks` 已补 `--action-id` filter parity
  - `console list-tasks` 已补 `--status-group` filter parity
  - home 页已补 `Recent Activity` 总览
  - non-home section 已补 `Recent Tasks` 子面板，按当前 section action ids 拉最近 task/history
  - bundles / experiments 页面壳已补更明确的 row-linked workflow 和 task/result 提示
  - recent task 行已可 drill down 到完整 task detail，并复用现有 action result / json 区展示 error / output path
  - task detail / task row 已补 `subject_summary / primary_output_path / result_paths`
  - console + research 相关测试 `119 passed`
