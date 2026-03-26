from __future__ import annotations

from pathlib import Path

from pm15min.console.web import (
    CONSOLE_CSS_PATH,
    CONSOLE_JS_PATH,
    build_console_asset_manifest,
    build_console_css,
    build_console_js,
    build_console_shell_page,
)


def test_console_asset_manifest_and_sources_are_stable() -> None:
    manifest = build_console_asset_manifest()
    css = build_console_css()
    js = build_console_js()

    assert manifest["css_path"] == CONSOLE_CSS_PATH
    assert manifest["js_path"] == CONSOLE_JS_PATH
    assert ".console-shell" in css
    assert ".console-nav-link.is-active" in css
    assert ".console-controls-grid" in css
    assert ".console-json" in css
    assert ".console-action-grid" in css
    assert ".console-action-button" in css
    assert ".console-action-result-empty" in css
    assert ".console-action-result-note" in css
    assert ".console-action-result-note.is-running" in css
    assert ".console-action-result-note.is-complete" in css
    assert ".console-action-result-note.is-failed" in css
    assert ".console-operator-grid" in css
    assert ".console-operator-card" in css
    assert ".console-chart-svg" in css
    assert ".console-chart-legend" in css
    assert ".console-detail-filter-grid" in css
    assert ".console-table-sort" in css
    assert ".console-runtime-board" in css
    assert ".console-runtime-column" in css
    assert ".console-runtime-warning-list" in css
    assert ".console-runtime-warning" in css
    assert ".console-runtime-warning.is-warning" in css
    assert ".console-runtime-warning.is-info" in css
    assert ".console-task-history" in css
    assert ".console-task-history-summary" in css
    assert ".console-task-history-list" in css
    assert ".console-task-history-focus" in css
    assert ".console-task-row" in css
    assert ".console-action-forms" in css
    assert ".console-action-form-card" in css
    assert ".console-form-grid" in css
    assert ".console-detail-view-tabs" in css
    assert ".console-detail-view-button" in css
    assert ".console-detail-view-button.is-active" in css
    assert ".console-detail-view-hint" in css
    assert ".console-guide-grid" in css
    assert ".console-guide-card" in css
    assert ".console-form-section" in css
    assert ".console-toggle-button" in css
    assert ".console-field-help" in css
    assert ".console-field[hidden]" in css
    assert ".console-row-quick-actions" in css
    assert ".console-row-quick-summary" in css
    assert ".console-row-action-card.is-blocked" in css
    assert ".console-table tbody tr.is-selected" in css
    assert "renderActionCatalog" in js
    assert "renderActionForms" in js
    assert "renderActionContext" in js
    assert "renderRowQuickActions" in js
    assert "renderActionResult" in js
    assert "renderActionResultDrilldown" in js
    assert "renderOperatorCard" in js
    assert "renderMetricCardGrid" in js
    assert "renderDetailFilterBar" in js
    assert "renderLineChartCard" in js
    assert "renderBarChartCard" in js
    assert "renderPreviewTableCard" in js
    assert "buildPreviewTableCardElement" in js
    assert "renderRecentTasks" in js
    assert "renderRuntimeBoard" in js
    assert "loadRecentTasks" in js
    assert "refreshRecentTasks" in js
    assert "syncRecentTaskPolling" in js
    assert "openRecentTaskDrilldown" in js
    assert "pollConsoleTask" in js
    assert "activeTaskPolls" in js
    assert "TASK_POLL_INTERVAL_MS" in js
    assert "TASK_POLL_MAX_CONSECUTIVE_ERRORS" in js
    assert "taskStatusPath" in js
    assert "tasksListPath" in js
    assert "ensureActionCatalog" in js
    assert "sectionActionIds" in js
    assert "actionResultsBySection" in js
    assert "actionAdvancedState" in js
    assert "selectedRowsBySection" in js
    assert "detailFilterStateBySection" in js
    assert "detailViewStateBySection" in js
    assert "actionFormValues" in js
    assert "saveActionFormValue" in js
    assert "sectionSupportsDetail" in js
    assert "detailViewOptions" in js
    assert "activeDetailView" in js
    assert "setDetailView" in js
    assert "detailRoutePath" in js
    assert 'replaceAll("_", "-")' in js
    assert "detailQueryForRow" in js
    assert "detailUrlForRow" in js
    assert "loadSectionDetail" in js
    assert "renderSectionDetail" in js
    assert "renderDetailViewSwitcher" in js
    assert "renderTrainingRunDetail" in js
    assert "renderBacktestDetail" in js
    assert "renderBacktestStakeSweepDetail" in js
    assert "renderExperimentDetail" in js
    assert "renderExperimentMatrixDetail" in js
    assert "renderGuideCards" in js
    assert "renderGlobalControls" in js
    assert "renderSectionUsageGuide" in js
    assert "actionFieldBucket" in js
    assert "actionFieldHelpText" in js
    assert "sectionRows" in js
    assert "actionContextFromPayload" in js
    assert "rowSelectionLabel" in js
    assert "actionRequestState" in js
    assert "actionRequestPreview" in js
    assert "rowSelectionStatusCopy" in js
    assert "actionResultLeadCopy" in js
    assert "actionResultPrimaryOutput" in js
    assert "taskHistoryLeadCopy" in js
    assert "taskRowHeadline" in js
    assert "taskRowCopy" in js
    assert "taskMetaValues" in js
    assert "buildTaskRowArticle" in js
    assert "recentTaskHighlights" in js
    assert "taskDrilldownStatusSummary" in js
    assert "runtimeBoardGroups" in js
    assert "runtimeBoardPayload" in js
    assert "runtimeBoardWarnings" in js
    assert "runtimeBoardLeadCopy" in js
    assert "runtimeSummaryLatestEntries" in js
    assert "runtimeSummaryForSection" in js
    assert "linkedObjectSummary" in js
    assert "linkedObjectDetailSummary" in js
    assert "errorDetailSummary" in js
    assert "request_summary" in js
    assert "result_summary" in js
    assert "error_summary" in js
    assert "linked_objects" in js
    assert "linked_object_details" in js
    assert "error_detail" in js
    assert "result_paths" in js
    assert "progress_summary" in js
    assert "subject_summary" in js
    assert "primary_output_path" in js
    assert "taskStatusSummary" in js
    assert "synchronousActionStatusSummary" in js
    assert "runtimeSummaryPayload" in js
    assert "active_tasks" in js
    assert "terminal_tasks" in js
    assert "failed_tasks" in js
    assert "latest_active_task" in js
    assert "latest_terminal_task" in js
    assert "latest_failed_task" in js
    assert "runtime_updated_at" in js
    assert "parseWindowBounds" in js
    assert "活跃任务" in js
    assert "失败任务" in js
    assert "结果路径" in js
    assert "关联对象" in js
    assert "错误钻取" in js
    assert "持久化运行态快照：" in js
    assert "无效任务文件=" in js
    assert "invalid_task_files" in js
    assert "下方可继续钻取：" in js
    assert "console-bootstrap" in js
    assert "history.pushState" in js
    assert "fetch(buildUrl(resolved))" in js
    assert "fetch(actionCatalogPath())" in js
    assert 'execution_mode' in js
    assert "fetch(actionExecutePath()" in js
    assert "data_refresh_summary" in js
    assert "data_sync" in js
    assert "data_build" in js
    assert "research_activate_bundle" in js
    assert "research_bundle_build" in js
    assert "research_train_run" in js
    assert "research_backtest_run" in js
    assert "research_experiment_run_suite" in js
    assert "dataset_rows" in js
    assert "仅看前 N" in js
    assert "关键词" in js
    assert "按市场过滤" in js
    assert "按运行名过滤" in js
    assert "按变体过滤" in js
    assert "清除筛选" in js
    assert "已就绪：选中数据行 + 表单值" in js
    assert "在这里填写命令相关字段。" in js
    assert "已就绪：可激活当前模型包" in js
    assert "新的模型包标签仍需显式指定" in js
    assert "已就绪：可重跑当前 suite 上下文" in js
    assert "实验快捷动作会使用当前 suite 输入和默认值" in js
    assert "后台任务仍在运行" in js
    assert "直到任务进入终态并刷新当前分区" in js
    assert "后台任务执行成功。" in js
    assert "最近心跳" in js
    assert "心跳=" in js
    assert "正在加载所选详情..." in js
    assert "请选择一行后查看该分区的标准详情。" in js
    assert "扫参与因子结论" in js
    assert "结果摘要卡" in js
    assert "扫参领先者" in js
    assert "金额扫参页" in js
    assert "金额扫参摘要卡" in js
    assert "金额扫参全表" in js
    assert '"/stake-sweep"' in js
    assert "资金曲线图" in js
    assert "金额 ROI 图" in js
    assert "Offset PnL 图" in js
    assert "因子 PnL 图" in js
    assert "金额领先榜" in js
    assert "Offset 领先榜" in js
    assert "当前最佳结果" in js
    assert "比较摘要卡" in js
    assert "领先者摘要卡" in js
    assert "矩阵结果页" in js
    assert "矩阵摘要卡" in js
    assert "特征集视角" in js
    assert '"/matrix"' in js
    assert "最佳组合摘要" in js
    assert "变体面对比摘要" in js
    assert "失败概览" in js
    assert "排行榜 ROI 图" in js
    assert "矩阵最佳 ROI 图" in js
    assert "变体增量图" in js
    assert "按市场领先者" in js
    assert "按分组领先者" in js
    assert "按市场 / 分组领先者" in js
    assert "按运行领先者" in js
    assert "训练范围" in js
    assert "运行摘要卡" in js
    assert "解释性覆盖" in js
    assert "打包就绪度" in js
    assert "正在加载任务 " in js
    assert "加载任务详情失败。" in js
    assert "已打开任务 " in js
    assert "正在加载最近任务..." in js
    assert "当前分区还没有记录到最近任务。" in js
    assert "活跃任务高亮" in js
    assert "失败任务高亮" in js
    assert "运行回测" in js
    assert "运行实验套件" in js
    assert "执行数据同步" in js
    assert "执行数据构建" in js
    assert "显示高级参数" in js
    assert "收起高级参数" in js
    assert "最少要填什么" in js
    assert "预测目标" in js
    assert "回测模板" in js
    assert "数据场景" in js
    assert "/api/console/" in js


def test_build_console_shell_page_renders_nav_and_placeholders() -> None:
    html = build_console_shell_page(
        root=Path("/tmp/v2"),
        active_section="bundles",
        api_base="/api/console",
        title="pm15min Console",
    )

    assert "<!doctype html>" in html
    assert 'data-console-shell' in html
    assert 'href="/static/console.css"' in html
    assert 'src="/static/console.js"' in html
    assert 'data-section-id="home"' in html
    assert 'data-section-id="data_overview"' in html
    assert 'data-section-id="training_runs"' in html
    assert 'data-section-id="bundles"' in html
    assert 'data-section-id="backtests"' in html
    assert 'data-section-id="experiments"' in html
    assert 'class="console-nav-link is-active"' in html
    assert '/api/console/bundles' in html
    assert 'data-console-refresh' in html
    assert 'data-console-input="market"' in html
    assert 'data-console-input="bundle_label"' in html
    assert 'data-console-input="run_label"' in html
    assert 'data-console-global-guide' in html
    assert 'data-console-json' in html
    assert 'data-console-action-catalog' in html
    assert 'data-console-action-result-summary' in html
    assert 'data-console-action-drilldown' in html
    assert 'data-console-action-result-command' in html
    assert 'data-console-action-result-parsed' in html
    assert 'data-console-action-result-logs' in html
    assert 'data-console-recent-tasks' in html
    assert 'data-console-runtime-board' in html
    assert 'data-console-action-context' in html
    assert 'data-console-parameter-guide' in html
    assert 'data-console-section-detail' in html
    assert 'data-console-row-quick-actions' in html
    assert 'data-console-action-forms' in html
    assert '<html lang="zh-CN">' in html
    assert '本页怎么用' in html
    assert '动作上下文' in html
    assert '当前选中详情' in html
    assert '选中行快捷操作' in html
    assert '动作表单' in html
    assert '动作目录' in html
    assert '本分区动作' in html
    assert '最近动作结果' in html
    assert '最近任务' in html
    assert '最近活动' in html
    assert '运行看板' in html
    assert '同步和构建入口仍然明确留在下面的表单里' in html
    assert '这里汇总整个控制台的最近任务和运行态计数' in html
    assert '无效任务文件和被截断的历史窗口也会先在这里提示' in html
    assert '首页已经承担起运行态健康、任务活动、结构化输出和失败钻取的总览职责' in html
    assert '在当前数据总览上下文下直接触发标准数据同步/构建动作' in html
    assert '同步/构建表单字段直接来自动作目录' in html
    assert '进行中和失败任务会优先置顶，便于第一时间看到刷新问题' in html
    assert '新的模型包标签始终显式填写' in html
    assert '异步任务会一直轮询到构建或激活结束' in html
    assert '进行中和失败任务会优先置顶，方便及时发现发布问题' in html
    assert '即使没有选中行，也可以直接从当前输入启动实验套件' in html
    assert '实验套件在分组和案例间推进时，运行态进度会持续刷新' in html
    assert '进行中和失败任务会优先置顶，复跑和失败信息都留在同一工作流里阅读' in html
    assert '回测详情是当前最核心的结果视图' in html
    assert '金额 sweep 现在也有专门结果页可切换' in html
    assert '这里应该直接回答当前哪个案例家族、变体、feature_set 或矩阵领先' in html
    assert '如果不想先手写 suite，也可以直接走内联实验入口' in html
    assert '训练模型族' in html
    assert '预测目标' in html
    assert '回测模板' in html
    assert '训练详情页重点是因子解释产物' in html
    assert '"active_section": "bundles"' in html
