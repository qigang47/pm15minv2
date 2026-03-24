from __future__ import annotations

import json
from html import escape
from pathlib import Path
from typing import Any

from pm15min.console.service import list_console_sections

from .assets import CONSOLE_CSS_PATH, CONSOLE_JS_PATH


_HOME_SECTION = {
    "id": "home",
    "title": "首页",
    "notes": "本地控制台入口与导航壳层。",
}

_HOME_PANEL_COPY = {
    "runtime_board": "首页直接展示持久化运行态看板，优先暴露进行中、失败和已结束任务的快照。无效任务文件和被截断的历史窗口也会先在这里提示，然后再往下钻取任务详情。",
    "recent_tasks": "这里汇总整个控制台的最近任务和运行态计数。先在这里判断系统状态，再点进单个任务看结构化输出和错误详情。",
}

_DEFAULT_SECTION_COPY = {
    "intro": "",
    "detail": "选中一行后，这里会加载当前分区对应的标准对象详情。目标是直接给出可读摘要，而不是让你翻原始 JSON。",
    "action_context": "选中行会成为当前分区的默认动作上下文，并驱动下方快捷操作。表单值会覆盖选中行上下文。",
    "quick_actions": "这里放当前分区最常用的操作。按钮会复用选中行上下文，以及你在表单里已经填写的值。",
    "action_forms": "动作专属字段集中放在这里。选中行上下文和顶部默认值仍会自动参与拼装。",
    "action_result": "这里展示最近一次从当前分区触发的动作结果。",
    "recent_tasks": "这里展示当前分区最近的后台任务。进行中和失败任务会优先置顶，但任务仍然从属于当前分区，不额外拆出一套导航。",
}

_DATA_OVERVIEW_SECTION_COPY = {
    "intro": "先选一个数据集行，锚定当前数据上下文。同步和构建入口仍然明确留在下面的表单里，避免控制台从摘要行里猜运维命令。",
    "detail": "这里重点展示当前数据集的覆盖范围、新鲜度和运行态上下文，但页面仍然比回测/实验结果页更轻。",
    "action_context": "选中的数据集行会把当前数据上下文展示出来。真正的同步/构建请求仍然来自动作目录字段，以及顶部的市场 / 周期 / 场景默认值。",
    "quick_actions": "在当前数据总览上下文下直接触发标准数据同步/构建动作。按钮会带上选中数据集作为参考，同时使用你在表单里填写的真实请求参数。",
    "action_forms": "同步/构建表单字段直接来自动作目录。在这里填写命令标识；选中行上下文和顶部默认值仍会自动参与。",
    "action_result": "这里展示最近一次数据动作结果。后台同步/构建任务会持续更新，直到任务结束。",
    "recent_tasks": "这里展示数据同步 / 构建任务历史。进行中和失败任务会优先置顶，便于第一时间看到刷新问题。",
}

_BUNDLES_SECTION_COPY = {
    "intro": "选中一个模型包后，可以直接做激活，也可以沿用同一条来源链路继续构建。新的模型包标签始终显式填写；选中行只负责回填范围、offset 和来源训练链路，除非表单覆盖。",
    "detail": "这里展示当前模型包的来源链路、激活状态和复制过来的诊断产物，方便你在继续构建或切换激活前先看清楚。",
    "action_context": "选中的模型包行会把当前激活范围展示出来。激活动作可以直接复用它的模型包标签；构建动作只继承配置 / 目标 / 来源链路，不会静默覆盖新的模型包标签。",
    "quick_actions": "选中模型包时，快捷操作会绑定当前行；没有选中行时，也可以基于当前配置 / 目标直接操作。",
    "action_forms": "模型包表单字段始终显式展示。你可以在这里指定下一个模型包标签，或覆盖选中行回填进来的 offset 和来源训练运行。",
    "action_result": "这里展示最近一次模型包动作结果。异步任务会一直轮询到构建或激活结束，因此进度和最终选择状态都能直接看到。",
    "recent_tasks": "这里展示当前范围内最近的模型包构建和激活任务。进行中和失败任务会优先置顶，方便及时发现发布问题。",
}

_EXPERIMENTS_SECTION_COPY = {
    "intro": "选中一个实验行后，可以在不重输实验套件名称的前提下复跑同一套上下文。新的运行标签仍来自表单或顶部默认值，不会静默继承旧运行标签；如果不想先手写 suite，也可以直接走内联实验入口。",
    "detail": "这里应该直接回答当前哪个案例家族、变体、feature_set 或矩阵领先，让你不用离开实验套件工作流就能看清对比结果。",
    "action_context": "选中的实验行会把实验套件来源链路保留下来供复跑使用。实验套件默认值可以来自当前行或顶部控件，但新的运行标签仍然显式填写。",
    "quick_actions": "即使没有选中行，也可以直接从当前输入启动实验套件。选中行更多是为了给复跑动作提供锚点。",
    "action_forms": "实验表单字段既支持已有 suite，也支持内联生成 suite spec。选中行只提供复跑上下文和来源提示。",
    "action_result": "这里展示最近一次实验动作结果。实验套件在分组和案例间推进时，运行态进度会持续刷新。",
    "recent_tasks": "这里展示当前实验分区最近的实验套件任务。进行中和失败任务会优先置顶，复跑和失败信息都留在同一工作流里阅读。",
}

_TRAINING_SECTION_COPY = {
    "intro": "选中一个训练运行后，可以先看 offset 级诊断和因子解释，再决定这个运行是否值得打包或和后续运行做比较。",
    "detail": "训练详情页重点是因子解释产物、offset 摘要和必要的最终状态，不把页面做成一个重型训练监控面板。",
    "action_context": "选中的训练行会把当前运行的来源链路展示出来。训练动作真正的参数仍然来自表单或顶部默认值。",
    "quick_actions": "训练快捷操作仍然聚焦在当前市场 / 模型范围下启动或复用一个运行。",
    "action_forms": "训练表单字段始终显式填写，避免新运行静默继承旧时间窗或旧 offset。",
    "action_result": "这里展示最近一次训练动作结果。启动训练时会显示运行态进度，但选中运行的详情仍以最终诊断为主。",
    "recent_tasks": "这里展示最近训练任务。进行中和失败任务会优先置顶，足够盯住运行情况，但不会把页面做成训练监控大盘。",
}

_BACKTESTS_SECTION_COPY = {
    "intro": "选中的回测行就是主结果视图。这个页面应该让你不用手动开产物，就能直接看 PnL、ROI、胜率、扫参结果和一致性 / 运行态摘要。",
    "detail": "回测详情是当前最核心的结果视图：先看关键绩效指标，再看金额扫描、offset 结果、因子表现和产物入口。金额 sweep 现在也有专门结果页可切换。",
    "action_context": "选中的回测行会保留完整规格 / 运行上下文，让复跑动作仍然锚定在正确的模型包、金额、一致性参数和回退设置上。",
    "quick_actions": "回测快捷操作的目标是基于当前结果，快速复跑相邻规格或模型包组合。",
    "action_forms": "回测表单字段仍然是下注金额、一致性参数和回退输入的唯一显式来源。选中行只负责回填范围和身份信息。",
    "action_result": "这里展示最近一次回测动作结果。新任务的异步输出留在这里，但主阅读面仍然是上面的回测结果详情。",
    "recent_tasks": "这里展示最近回测任务。进行中和失败任务会优先置顶，但页面的主优先级仍然是阅读已完成结果的质量。",
}

_SECTION_TITLE_OVERRIDES = {
    "data_overview": "数据总览",
    "training_runs": "训练运行",
    "bundles": "模型包",
    "backtests": "回测结果",
    "experiments": "实验对比",
}

_SECTION_NOTES_OVERRIDES = {
    "data_overview": "查看标准数据摘要、时间范围和覆盖情况。",
    "training_runs": "查看训练运行、offset 摘要与解释性产物。",
    "bundles": "查看模型包链路、来源训练和激活状态。",
    "backtests": "查看回测指标、扫描产物与结果比较。",
    "experiments": "查看实验套件、排行榜与矩阵比较结果。",
}


def build_console_shell_page(
    *,
    root: Path | None = None,
    active_section: str = "home",
    api_base: str = "/api/console",
    title: str = "pm15min 控制台",
) -> str:
    sections = [_HOME_SECTION, *list_console_sections()]
    section_ids = {str(item["id"]) for item in sections}
    resolved_section = active_section if active_section in section_ids else "home"
    bootstrap = {
        "root": None if root is None else str(Path(root)),
        "active_section": resolved_section,
        "api_base": str(api_base),
        "section_ids": [str(item["id"]) for item in sections],
        "defaults": {
            "market": "sol",
            "cycle": "15m",
            "surface": "backtest",
            "profile": "deep_otm",
            "target": "direction",
            "model_family": "deep_otm",
            "spec": "baseline_truth",
            "suite": "",
            "bundle_label": "",
            "run_label": "planned",
        },
    }
    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="zh-CN">',
            "<head>",
            '  <meta charset="utf-8">',
            '  <meta name="viewport" content="width=device-width, initial-scale=1">',
            f"  <title>{escape(title)}</title>",
            f'  <link rel="stylesheet" href="{escape(CONSOLE_CSS_PATH)}">',
            "</head>",
            f'  <body data-console-shell data-api-base="{escape(api_base)}" data-active-section="{escape(resolved_section)}">',
            '    <div class="console-shell">',
            '      <header class="console-hero">',
            '        <p class="console-kicker">本地只读控制台</p>',
            f'        <h1 class="console-title">{escape(title)}</h1>',
            '        <p class="console-subtitle">在不绕开现有 v2 产物体系的前提下，直接浏览标准数据、训练运行、模型包、回测结果和实验输出。</p>',
            '      </header>',
            '      <section class="console-panel console-controls">',
            '        <div class="console-controls-grid">',
            *_control_markup(),
            '        </div>',
            '        <div class="console-guide-grid" data-console-global-guide></div>',
            '      </section>',
            '      <div class="console-layout">',
            '        <aside class="console-sidebar">',
            '          <nav class="console-nav" aria-label="控制台分区">',
            *_nav_markup(sections, active_section=resolved_section),
            '          </nav>',
            '        </aside>',
            '        <main class="console-content" data-console-outlet>',
            *_panel_markup(sections),
            '        </main>',
            '      </div>',
            '    </div>',
            f'    <script id="console-bootstrap" type="application/json">{_bootstrap_json(bootstrap)}</script>',
            f'    <script src="{escape(CONSOLE_JS_PATH)}"></script>',
            "  </body>",
            "</html>",
        ]
    )


def _nav_markup(
    sections: list[dict[str, Any]],
    *,
    active_section: str,
) -> list[str]:
    lines: list[str] = []
    for section in sections:
        section_id = str(section["id"])
        title = _section_title(section)
        notes = _section_notes(section)
        classes = "console-nav-link is-active" if section_id == active_section else "console-nav-link"
        lines.extend(
            [
                f'            <a class="{classes}" href="?section={escape(section_id)}" data-console-nav-link data-section-id="{escape(section_id)}">',
                f'              <span class="console-nav-title">{escape(title)}</span>',
                f'              <span class="console-nav-note">{escape(notes)}</span>',
                "            </a>",
            ]
        )
    return lines


def _panel_markup(sections: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for section in sections:
        section_id = str(section["id"])
        if section_id == "home":
            lines.extend(
                [
                    '          <section class="console-panel" data-console-panel data-section-id="home">',
                    '            <h2 class="console-panel-title">控制台首页</h2>',
                    '            <p class="console-panel-copy">这个壳层刻意保持轻量，但首页已经承担起运行态健康、任务活动、结构化输出和失败钻取的总览职责。</p>',
                    '            <div class="console-grid">',
                    *_home_card_markup(sections[1:]),
                    '            </div>',
                    '            <div class="console-status-bar">',
                    '              <span>模式：只读</span>',
                    '              <span>传输：CLI + JSON HTTP</span>',
                    '              <span>来源：标准摘要 / 报告 / 清单 / parquet 产物</span>',
                    '            </div>',
                    '            <div class="console-section-status" data-console-status></div>',
                    '            <div class="console-grid" data-console-summary></div>',
                    '            <section class="console-subpanel" data-console-runtime-board-wrap>',
                    '              <div class="console-subpanel-head">',
                    '                <h3 class="console-subpanel-title">运行看板</h3>',
                    f'                <p class="console-subpanel-copy">{escape(_HOME_PANEL_COPY["runtime_board"])}</p>',
                    '              </div>',
                    '              <div class="console-runtime-board-wrap" data-console-runtime-board></div>',
                    '            </section>',
                    '            <section class="console-subpanel" data-console-action-catalog-wrap>',
                    '              <div class="console-subpanel-head">',
                    '                <h3 class="console-subpanel-title">动作目录</h3>',
                    '                <p class="console-subpanel-copy">这里展示只读动作描述和标准化命令预览，方便下一步接入执行。</p>',
                    '              </div>',
                    '              <div class="console-action-grid" data-console-action-catalog></div>',
                    '            </section>',
                    '            <section class="console-subpanel" data-console-action-result-wrap>',
                    '              <div class="console-subpanel-head">',
                    '                <h3 class="console-subpanel-title">动作结果</h3>',
                    '                <p class="console-subpanel-copy">这里展示当前首页动作的执行反馈。</p>',
                    '              </div>',
                    '              <div class="console-grid" data-console-action-result-summary></div>',
                    '              <div class="console-operator-grid" data-console-action-drilldown></div>',
                    '              <pre class="console-action-card-code" data-console-action-result-command></pre>',
                    '              <pre class="console-json" data-console-action-result-parsed></pre>',
                    '              <pre class="console-json" data-console-action-result-logs></pre>',
                    '            </section>',
                    '            <section class="console-subpanel" data-console-recent-tasks-wrap>',
                    '              <div class="console-subpanel-head">',
                    '                <h3 class="console-subpanel-title">最近活动</h3>',
                    f'                <p class="console-subpanel-copy">{escape(_HOME_PANEL_COPY["recent_tasks"])}</p>',
                    '              </div>',
                    '              <div class="console-task-history" data-console-recent-tasks></div>',
                    '            </section>',
                    '            <div class="console-table-wrap" data-console-table-wrap></div>',
                    '            <pre class="console-json" data-console-json></pre>',
                    '          </section>',
                ]
            )
            continue
        lines.extend(_section_panel_markup(section))
    return lines


def _section_panel_markup(section: dict[str, Any]) -> list[str]:
    section_id = str(section["id"])
    title = _section_title(section)
    notes = _section_notes(section)
    copy = _section_panel_copy(section_id)
    lines = [
        f'          <section class="console-panel console-placeholder" data-console-panel data-section-id="{escape(section_id)}" hidden>',
        f'            <h2 class="console-panel-title">{escape(title)}</h2>',
        f'            <p class="console-panel-copy">{escape(notes)}</p>',
    ]
    intro = str(copy.get("intro") or "").strip()
    if intro:
        lines.append(f'            <p class="console-panel-copy">{escape(intro)}</p>')
    lines.extend(
        [
            f'            <p class="console-panel-copy"><code>/api/console/{escape(section_id)}</code></p>',
            '            <span class="console-placeholder-note">已接通 API 的分区页面</span>',
            '            <div class="console-section-status" data-console-status></div>',
            '            <div class="console-grid" data-console-summary></div>',
            '            <section class="console-subpanel" data-console-parameter-guide-wrap>',
            '              <div class="console-subpanel-head">',
            '                <h3 class="console-subpanel-title">本页怎么用</h3>',
            '                <p class="console-subpanel-copy">把当前页最常用的步骤和参数解释放在这里，避免你先去猜字段再操作。</p>',
            '              </div>',
            '              <div class="console-guide-grid" data-console-parameter-guide></div>',
            '            </section>',
            '            <section class="console-subpanel" data-console-section-detail-wrap>',
            '              <div class="console-subpanel-head">',
            '                <h3 class="console-subpanel-title">当前选中详情</h3>',
            f'                <p class="console-subpanel-copy">{escape(str(copy["detail"]))}</p>',
            '              </div>',
            '              <div class="console-operator-grid" data-console-section-detail></div>',
            '            </section>',
            '            <section class="console-subpanel" data-console-action-context-wrap>',
            '              <div class="console-subpanel-head">',
            '                <h3 class="console-subpanel-title">动作上下文</h3>',
            f'                <p class="console-subpanel-copy">{escape(str(copy["action_context"]))}</p>',
            '              </div>',
            '              <div class="console-grid" data-console-action-context></div>',
            '            </section>',
            '            <section class="console-subpanel" data-console-row-quick-actions-wrap>',
            '              <div class="console-subpanel-head">',
            '                <h3 class="console-subpanel-title">选中行快捷操作</h3>',
            f'                <p class="console-subpanel-copy">{escape(str(copy["quick_actions"]))}</p>',
            '              </div>',
            '              <div class="console-row-quick-actions" data-console-row-quick-actions></div>',
            '            </section>',
            '            <section class="console-subpanel" data-console-action-forms-wrap>',
            '              <div class="console-subpanel-head">',
            '                <h3 class="console-subpanel-title">动作表单</h3>',
            f'                <p class="console-subpanel-copy">{escape(str(copy["action_forms"]))}</p>',
            '              </div>',
            '              <div class="console-action-forms" data-console-action-forms></div>',
            '            </section>',
            '            <section class="console-subpanel" data-console-action-catalog-wrap>',
            '              <div class="console-subpanel-head">',
            '                <h3 class="console-subpanel-title">本分区动作</h3>',
            '                <p class="console-subpanel-copy">这里列出当前标准分区相关动作。在更复杂的交互接入前，未支持的动作仍以目录形式展示。</p>',
            '              </div>',
            '              <div class="console-action-grid" data-console-action-catalog></div>',
            '            </section>',
            '            <section class="console-subpanel" data-console-action-result-wrap>',
            '              <div class="console-subpanel-head">',
            '                <h3 class="console-subpanel-title">最近动作结果</h3>',
            f'                <p class="console-subpanel-copy">{escape(str(copy["action_result"]))}</p>',
            '              </div>',
            '              <div class="console-grid" data-console-action-result-summary></div>',
            '              <div class="console-operator-grid" data-console-action-drilldown></div>',
            '              <pre class="console-action-card-code" data-console-action-result-command></pre>',
            '              <pre class="console-json" data-console-action-result-parsed></pre>',
            '              <pre class="console-json" data-console-action-result-logs></pre>',
            '            </section>',
                    '            <section class="console-subpanel" data-console-recent-tasks-wrap>',
                    '              <div class="console-subpanel-head">',
                    '                <h3 class="console-subpanel-title">最近任务</h3>',
                    f'                <p class="console-subpanel-copy">{escape(str(copy["recent_tasks"]))}</p>',
                    '              </div>',
            '              <div class="console-task-history" data-console-recent-tasks></div>',
            '            </section>',
            '            <div class="console-table-wrap" data-console-table-wrap></div>',
            '            <pre class="console-json" data-console-json></pre>',
            '          </section>',
        ]
    )
    return lines


def _section_panel_copy(section_id: str) -> dict[str, str]:
    if section_id == "data_overview":
        return _DATA_OVERVIEW_SECTION_COPY
    if section_id == "training_runs":
        return _TRAINING_SECTION_COPY
    if section_id == "bundles":
        return _BUNDLES_SECTION_COPY
    if section_id == "backtests":
        return _BACKTESTS_SECTION_COPY
    if section_id == "experiments":
        return _EXPERIMENTS_SECTION_COPY
    return _DEFAULT_SECTION_COPY


def _home_card_markup(sections: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for section in sections:
        lines.extend(
            [
                '              <article class="console-card">',
                f'                <h3 class="console-card-title">{escape(_section_title(section))}</h3>',
                f'                <p class="console-card-copy">{escape(_section_notes(section))}</p>',
                "              </article>",
            ]
        )
    return lines


def _bootstrap_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True).replace("</", "<\\/")


def _control_markup() -> list[str]:
    fields = (
        ("market", "市场", "sol"),
        ("cycle", "周期", "15m"),
        ("surface", "数据场景", "backtest"),
        ("profile", "策略档案", "deep_otm"),
        ("target", "预测目标", "direction"),
        ("bundle_label", "模型包名", ""),
        ("run_label", "本次运行名", "planned"),
        ("model_family", "训练模型族", "deep_otm"),
        ("spec", "回测模板", "baseline_truth"),
        ("suite", "实验方案", ""),
    )
    lines: list[str] = []
    for field_id, label, value in fields:
        lines.extend(
            [
                '          <label class="console-field">',
                f'            <span class="console-field-label">{escape(label)}</span>',
                f'            <input class="console-input" data-console-input="{escape(field_id)}" value="{escape(value)}">',
                "          </label>",
            ]
        )
    lines.extend(
        [
            '          <div class="console-field console-field-button">',
            '            <span class="console-field-label">刷新</span>',
            '            <button class="console-button" type="button" data-console-refresh>刷新当前分区</button>',
            "          </div>",
        ]
    )
    return lines


def _section_title(section: dict[str, Any]) -> str:
    section_id = str(section.get("id") or "")
    return _SECTION_TITLE_OVERRIDES.get(section_id, str(section.get("title") or section_id))


def _section_notes(section: dict[str, Any]) -> str:
    section_id = str(section.get("id") or "")
    return _SECTION_NOTES_OVERRIDES.get(section_id, str(section.get("notes") or ""))
