from __future__ import annotations


CONSOLE_CSS_PATH = "/static/console.css"
CONSOLE_JS_PATH = "/static/console.js"


def build_console_asset_manifest() -> dict[str, str]:
    return {
        "css_path": CONSOLE_CSS_PATH,
        "js_path": CONSOLE_JS_PATH,
    }


def build_console_css() -> str:
    return """
:root {
  --console-bg: #f4efe4;
  --console-panel: #fffaf2;
  --console-panel-border: #d8ccb3;
  --console-ink: #1d1a16;
  --console-muted: #675f53;
  --console-accent: #8a3d16;
  --console-accent-soft: #efe0c8;
  --console-shadow: rgba(69, 48, 22, 0.10);
  --console-radius: 18px;
  --console-max-width: 1200px;
}

* {
  box-sizing: border-box;
}

html,
body {
  margin: 0;
  min-height: 100%;
  background:
    radial-gradient(circle at top right, rgba(138, 61, 22, 0.12), transparent 28rem),
    linear-gradient(180deg, #faf5ea 0%, var(--console-bg) 55%, #f0e8d9 100%);
  color: var(--console-ink);
  font-family: Georgia, "Times New Roman", serif;
}

body {
  padding: 24px;
}

.console-shell {
  max-width: var(--console-max-width);
  margin: 0 auto;
}

.console-hero,
.console-panel {
  background: var(--console-panel);
  border: 1px solid var(--console-panel-border);
  border-radius: var(--console-radius);
  box-shadow: 0 18px 45px var(--console-shadow);
}

.console-hero {
  padding: 28px 30px;
  margin-bottom: 22px;
}

.console-kicker {
  margin: 0 0 8px 0;
  font-size: 12px;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--console-accent);
}

.console-title {
  margin: 0;
  font-size: clamp(30px, 5vw, 50px);
  line-height: 1;
}

.console-subtitle {
  max-width: 60rem;
  margin: 12px 0 0 0;
  color: var(--console-muted);
  font-size: 17px;
  line-height: 1.55;
}

.console-layout {
  display: grid;
  grid-template-columns: 260px 1fr;
  gap: 20px;
}

.console-sidebar,
.console-content {
  min-width: 0;
}

.console-nav {
  display: grid;
  gap: 10px;
}

.console-nav-link {
  display: block;
  padding: 14px 16px;
  border-radius: 14px;
  border: 1px solid transparent;
  text-decoration: none;
  color: var(--console-ink);
  background: rgba(255, 255, 255, 0.52);
  transition: transform 140ms ease, border-color 140ms ease, background 140ms ease;
}

.console-nav-link:hover {
  transform: translateY(-1px);
  border-color: var(--console-panel-border);
}

.console-nav-link.is-active {
  border-color: var(--console-accent);
  background: var(--console-accent-soft);
}

.console-nav-title {
  display: block;
  font-size: 16px;
  font-weight: 700;
}

.console-nav-note {
  display: block;
  margin-top: 4px;
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.45;
}

.console-content {
  display: grid;
  gap: 18px;
}

.console-controls {
  margin-bottom: 20px;
  padding: 18px 20px;
}

.console-controls-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px;
  align-items: end;
}

.console-field {
  display: grid;
  gap: 6px;
}

.console-field-button {
  align-self: stretch;
}

.console-field-label {
  font-size: 12px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--console-muted);
}

.console-input,
.console-button {
  width: 100%;
  min-height: 42px;
  border-radius: 12px;
  border: 1px solid var(--console-panel-border);
  font: inherit;
}

.console-input {
  padding: 10px 12px;
  background: #fffdf8;
  color: var(--console-ink);
}

.console-button {
  padding: 10px 14px;
  background: var(--console-accent);
  color: #fff9f2;
  cursor: pointer;
}

.console-panel {
  padding: 22px 24px;
}

.console-subpanel {
  margin-top: 18px;
  padding-top: 18px;
  border-top: 1px solid rgba(216, 204, 179, 0.9);
}

.console-subpanel-head {
  display: grid;
  gap: 4px;
  margin-bottom: 12px;
}

.console-subpanel-title {
  margin: 0;
  font-size: 18px;
}

.console-subpanel-copy {
  margin: 0;
  color: var(--console-muted);
  font-size: 14px;
  line-height: 1.55;
}

.console-panel[hidden] {
  display: none;
}

.console-panel-title {
  margin: 0 0 8px 0;
  font-size: 24px;
}

.console-panel-copy {
  margin: 0 0 16px 0;
  color: var(--console-muted);
  line-height: 1.6;
}

.console-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 14px;
}

.console-card {
  border: 1px solid var(--console-panel-border);
  border-radius: 14px;
  padding: 16px;
  background: rgba(255, 255, 255, 0.62);
}

.console-card-title {
  margin: 0 0 6px 0;
  font-size: 18px;
}

.console-card-copy {
  margin: 0;
  color: var(--console-muted);
  font-size: 14px;
  line-height: 1.55;
}

.console-card-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 12px;
}

.console-pill {
  display: inline-flex;
  align-items: center;
  padding: 4px 8px;
  border-radius: 999px;
  background: var(--console-accent-soft);
  color: var(--console-accent);
  font-size: 12px;
  font-weight: 700;
}

.console-placeholder {
  min-height: 280px;
}

.console-section-status {
  margin: 14px 0;
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.5;
}

.console-placeholder-note {
  display: inline-flex;
  align-items: center;
  padding: 8px 12px;
  border-radius: 999px;
  background: var(--console-accent-soft);
  color: var(--console-accent);
  font-size: 13px;
  font-weight: 700;
}

.console-placeholder-list {
  margin: 18px 0 0 0;
  padding-left: 18px;
  color: var(--console-muted);
  line-height: 1.7;
}

.console-table-wrap {
  margin-top: 16px;
  overflow-x: auto;
}

.console-table {
  width: 100%;
  border-collapse: collapse;
  border: 1px solid var(--console-panel-border);
  border-radius: 12px;
  overflow: hidden;
  background: rgba(255, 255, 255, 0.75);
}

.console-table th,
.console-table td {
  padding: 10px 12px;
  border-bottom: 1px solid rgba(216, 204, 179, 0.8);
  text-align: left;
  vertical-align: top;
  font-size: 13px;
}

.console-table th {
  background: rgba(239, 224, 200, 0.7);
  color: var(--console-accent);
}

.console-table th.is-sortable {
  cursor: pointer;
  user-select: none;
}

.console-table th.is-sortable:hover {
  background: rgba(232, 210, 176, 0.88);
}

.console-table-sort {
  margin-left: 6px;
  color: var(--console-muted);
  font-size: 11px;
}

.console-json {
  margin-top: 16px;
  padding: 14px 16px;
  border-radius: 14px;
  border: 1px solid var(--console-panel-border);
  background: #201b17;
  color: #f4efe4;
  overflow: auto;
  font-size: 12px;
  line-height: 1.6;
}

.console-empty {
  margin-top: 16px;
  color: var(--console-muted);
  font-style: italic;
}

.console-action-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
}

.console-action-card {
  border: 1px solid var(--console-panel-border);
  border-radius: 14px;
  padding: 14px 16px;
  background: rgba(255, 255, 255, 0.7);
}

.console-action-card-title {
  margin: 0 0 6px 0;
  font-size: 16px;
}

.console-action-card-copy {
  margin: 0;
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.55;
}

.console-action-card-code {
  margin: 12px 0 0 0;
  padding: 10px 12px;
  border-radius: 10px;
  background: #211c18;
  color: #f4efe4;
  font-size: 12px;
  overflow-x: auto;
}

.console-action-card-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 12px;
}

.console-action-button {
  border: 1px solid var(--console-accent);
  background: var(--console-accent);
  color: #fff9f2;
  border-radius: 10px;
  padding: 8px 12px;
  font: inherit;
  cursor: pointer;
}

.console-action-button[disabled] {
  opacity: 0.5;
  cursor: not-allowed;
}

.console-action-note {
  font-size: 12px;
  color: var(--console-muted);
  align-self: center;
}

.console-action-result-empty {
  color: var(--console-muted);
  font-style: italic;
}

.console-action-result-note {
  grid-column: 1 / -1;
  padding: 12px 14px;
  border-radius: 14px;
  border: 1px solid var(--console-panel-border);
  background: rgba(255, 255, 255, 0.72);
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.55;
}

.console-action-result-note.is-running {
  background: rgba(239, 224, 200, 0.45);
  color: var(--console-accent);
}

.console-action-result-note.is-complete {
  background: rgba(226, 239, 220, 0.55);
  color: #35522e;
}

.console-action-result-note.is-failed {
  background: rgba(244, 218, 212, 0.62);
  color: #7a2f18;
}

.console-operator-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 12px;
  margin-top: 14px;
}

.console-operator-card {
  border: 1px solid var(--console-panel-border);
  border-radius: 14px;
  padding: 14px 16px;
  background: rgba(255, 255, 255, 0.7);
}

.console-operator-card.is-running {
  background: rgba(239, 224, 200, 0.34);
}

.console-operator-card.is-failed {
  background: rgba(244, 218, 212, 0.45);
}

.console-operator-card-title {
  margin: 0 0 8px 0;
  font-size: 15px;
}

.console-operator-card-copy {
  margin: 0 0 10px 0;
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.55;
}

.console-operator-list {
  display: grid;
  gap: 10px;
}

.console-operator-item {
  display: grid;
  gap: 4px;
}

.console-operator-label {
  font-size: 12px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--console-muted);
}

.console-operator-value {
  margin: 0;
  font-size: 13px;
  line-height: 1.55;
  color: var(--console-ink);
  word-break: break-word;
}

.console-operator-code {
  margin: 0;
  padding: 10px 12px;
  border-radius: 10px;
  background: #211c18;
  color: #f4efe4;
  font-size: 12px;
  line-height: 1.5;
  overflow-x: auto;
  white-space: pre-wrap;
}

.console-chart {
  display: grid;
  gap: 12px;
}

.console-chart-svg {
  width: 100%;
  height: auto;
  display: block;
  border-radius: 12px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.72) 0%, rgba(244, 239, 228, 0.92) 100%);
  border: 1px solid rgba(216, 204, 179, 0.9);
}

.console-chart-caption {
  margin: 0;
  color: var(--console-muted);
  font-size: 12px;
  line-height: 1.5;
}

.console-chart-legend {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 8px;
}

.console-chart-legend-item {
  border: 1px solid var(--console-panel-border);
  border-radius: 12px;
  padding: 10px 12px;
  background: rgba(255, 255, 255, 0.68);
}

.console-chart-legend-label {
  display: block;
  color: var(--console-muted);
  font-size: 11px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.console-chart-legend-value {
  display: block;
  margin-top: 4px;
  color: var(--console-ink);
  font-size: 13px;
  line-height: 1.45;
}

.console-runtime-board-wrap {
  display: grid;
  gap: 12px;
}

.console-runtime-board {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 12px;
}

.console-runtime-column {
  border: 1px solid var(--console-panel-border);
  border-radius: 14px;
  padding: 14px 16px;
  background: rgba(255, 255, 255, 0.62);
}

.console-runtime-column.is-running {
  background: rgba(239, 224, 200, 0.34);
}

.console-runtime-column.is-failed {
  background: rgba(244, 218, 212, 0.40);
}

.console-runtime-column-head {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  margin-bottom: 10px;
}

.console-runtime-column-title {
  margin: 0;
  font-size: 16px;
}

.console-runtime-column-copy {
  margin: 0 0 10px 0;
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.5;
}

.console-runtime-column-list {
  display: grid;
  gap: 10px;
}

.console-runtime-warning-list {
  display: grid;
  gap: 12px;
  margin-bottom: 12px;
}

.console-runtime-warning {
  border: 1px solid var(--console-panel-border);
  border-radius: 14px;
  padding: 14px 16px;
  background: rgba(255, 255, 255, 0.72);
}

.console-runtime-warning.is-warning {
  background: rgba(244, 218, 212, 0.52);
}

.console-runtime-warning.is-info {
  background: rgba(239, 224, 200, 0.40);
}

.console-runtime-warning-title {
  margin: 0 0 6px 0;
  font-size: 15px;
}

.console-runtime-warning-copy {
  margin: 0;
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.55;
}

.console-runtime-column .console-task-row {
  padding: 12px 14px;
}

.console-task-history {
  display: grid;
  gap: 12px;
}

.console-task-history-summary {
  padding: 12px 14px;
  border-radius: 14px;
  border: 1px dashed var(--console-panel-border);
  background: rgba(239, 224, 200, 0.3);
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.55;
}

.console-task-history-list {
  display: grid;
  gap: 12px;
}

.console-task-history-focus {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 12px;
}

.console-task-row {
  border: 1px solid var(--console-panel-border);
  border-radius: 14px;
  padding: 14px 16px;
  background: rgba(255, 255, 255, 0.68);
  cursor: pointer;
}

.console-task-row:focus-visible {
  outline: 2px solid var(--console-accent);
  outline-offset: 2px;
}

.console-task-row.is-running {
  background: rgba(239, 224, 200, 0.34);
}

.console-task-row.is-complete {
  background: rgba(226, 239, 220, 0.38);
}

.console-task-row.is-failed {
  background: rgba(244, 218, 212, 0.45);
}

.console-task-row-title {
  margin: 0 0 6px 0;
  font-size: 15px;
}

.console-task-row-copy {
  margin: 0;
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.55;
}

.console-form-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 12px;
}

.console-action-forms {
  display: grid;
  gap: 12px;
}

.console-action-form-card {
  border: 1px solid var(--console-panel-border);
  border-radius: 14px;
  padding: 14px 16px;
  background: rgba(255, 255, 255, 0.62);
}

.console-form-field {
  display: grid;
  gap: 6px;
}

.console-form-section {
  display: grid;
  gap: 10px;
  padding: 12px;
  border: 1px dashed rgba(216, 204, 179, 0.95);
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.48);
}

.console-form-section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.console-form-section-title {
  margin: 0;
  font-size: 14px;
}

.console-toggle-button {
  border: 1px solid var(--console-panel-border);
  background: rgba(255, 255, 255, 0.88);
  color: var(--console-ink);
  border-radius: 999px;
  padding: 6px 12px;
  font: inherit;
  cursor: pointer;
}

.console-field-help {
  color: var(--console-muted);
  font-size: 12px;
  line-height: 1.55;
}

.console-detail-controls {
  display: grid;
  gap: 12px;
}

.console-detail-filter-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 12px;
}

.console-detail-view-switcher {
  display: grid;
  gap: 12px;
}

.console-detail-view-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.console-detail-view-button {
  border: 1px solid var(--console-panel-border);
  background: rgba(255, 255, 255, 0.86);
  color: var(--console-ink);
  border-radius: 999px;
  padding: 8px 14px;
  font: inherit;
  cursor: pointer;
}

.console-detail-view-button.is-active {
  border-color: var(--console-accent);
  background: var(--console-accent-soft);
  color: var(--console-accent);
  font-weight: 700;
}

.console-detail-view-hint {
  margin: 0;
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.55;
}

.console-guide-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
  margin-top: 16px;
}

.console-guide-card {
  border: 1px solid var(--console-panel-border);
  border-radius: 14px;
  padding: 14px 16px;
  background: rgba(255, 255, 255, 0.66);
}

.console-guide-card-title {
  margin: 0 0 6px 0;
  font-size: 15px;
}

.console-guide-card-copy {
  margin: 0;
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.6;
}

.console-guide-card-meta {
  margin-top: 10px;
  color: var(--console-accent);
  font-size: 12px;
  line-height: 1.5;
}

.console-field[hidden] {
  display: none;
}

.console-action-context-empty {
  color: var(--console-muted);
  font-style: italic;
}

.console-row-quick-actions {
  display: grid;
  gap: 12px;
}

.console-row-quick-summary {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
  padding: 12px 14px;
  border: 1px dashed var(--console-panel-border);
  border-radius: 14px;
  background: rgba(239, 224, 200, 0.4);
}

.console-row-quick-copy {
  color: var(--console-muted);
  font-size: 13px;
  line-height: 1.5;
}

.console-row-action-card.is-blocked {
  opacity: 0.78;
}

.console-table tbody tr {
  cursor: pointer;
  transition: background 140ms ease;
}

.console-table tbody tr:hover {
  background: rgba(239, 224, 200, 0.34);
}

.console-table tbody tr.is-selected {
  background: rgba(138, 61, 22, 0.12);
}

.console-status-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin-top: 18px;
  color: var(--console-muted);
  font-size: 13px;
}

@media (max-width: 920px) {
  body {
    padding: 16px;
  }

  .console-layout {
    grid-template-columns: 1fr;
  }
}
""".strip()


def build_console_js() -> str:
    return """
(function () {
  const shell = document.querySelector("[data-console-shell]");
  if (!shell) {
    return;
  }

  const bootstrapNode = document.getElementById("console-bootstrap");
  const navLinks = Array.from(document.querySelectorAll("[data-console-nav-link]"));
  const panels = Array.from(document.querySelectorAll("[data-console-panel]"));
  const refreshButton = document.querySelector("[data-console-refresh]");
  const inputNodes = Array.from(document.querySelectorAll("[data-console-input]"));
  const globalGuideNode = document.querySelector("[data-console-global-guide]");
  const actionResultsBySection = {};
  const recentTasksBySection = {};
  const selectedRowsBySection = {};
  const sectionDetailsBySection = {};
  const selectedRowContextBySection = {};
  const latestPayloadBySection = {};
  const actionFormState = {};
  const actionAdvancedState = {};
  const tableSortState = {};
  const detailFilterStateBySection = {};
  const detailViewStateBySection = {};
  const activeTaskPolls = {};
  const TASK_POLL_INTERVAL_MS = 1500;
  const TASK_POLL_MAX_CONSECUTIVE_ERRORS = 5;
  const SECTION_LABELS = {
    home: "首页",
    data_overview: "数据总览",
    training_runs: "训练运行",
    bundles: "模型包",
    backtests: "回测结果",
    experiments: "实验对比"
  };
  const SECTION_VISIBLE_GLOBAL_FIELDS = {
    home: ["market", "cycle", "surface", "profile", "target", "model_family", "spec", "suite"],
    data_overview: ["market", "cycle", "surface"],
    training_runs: ["market", "cycle", "model_family", "target", "run_label"],
    bundles: ["market", "cycle", "profile", "target", "bundle_label", "run_label"],
    backtests: ["market", "cycle", "profile", "target", "spec", "bundle_label", "run_label"],
    experiments: ["market", "cycle", "profile", "target", "suite", "run_label"]
  };
  const GLOBAL_CONTROL_HELP = {
    market: { title: "市场", copy: "你现在做哪个币就选哪个。比如做 ETH，就填 eth。", meta: "训练 / 回测 / 实验都会跟着它走。" },
    cycle: { title: "周期", copy: "现在主路径默认就是 15m。没有特殊需要就不要改。", meta: "建议先固定 15m，减少混乱。" },
    surface: { title: "数据场景", copy: "只在数据总览页最重要。做研究时优先看 backtest，不要先看 live。", meta: "训练 / 回测默认都应该基于 backtest。" },
    profile: { title: "策略档案", copy: "可以理解成一套策略配置名。现在不确定时先用 deep_otm。", meta: "bundle / backtest 会按这个路径找模型。" },
    target: { title: "预测目标", copy: "模型到底预测什么。当前最常用的是 direction。", meta: "不做专门实验时先保持 direction。" },
    model_family: { title: "训练模型族", copy: "训练器实现族。大多数时候和 profile 保持同名就行。", meta: "现在默认 deep_otm。" },
    spec: { title: "回测模板", copy: "回测规则模板。最安全的起点是 baseline_truth。", meta: "不知道选什么就先别改。" },
    suite: { title: "实验方案", copy: "只有做 Experiments 时才需要。普通训练 / 回测不用填。", meta: "做 feature_set 实验时再用。" },
    bundle_label: { title: "模型包名", copy: "回测要用哪套模型包。通常先用刚训练完并激活的那套。", meta: "例如 eth_bootstrap_http_v1。" },
    run_label: { title: "本次运行名", copy: "给这次任务起名字，方便后面在列表里找。", meta: "建议按市场 + 日期 + 目的命名。" }
  };
  const SECTION_USAGE_GUIDES = {
    data_overview: [
      { title: "先看哪里", copy: "先看 surface=backtest。live 常因为新鲜度阈值显示 error，但不代表研究链路不能用。", meta: "做训练 / 回测时优先看 backtest。" },
      { title: "最重要字段", copy: "重点看时间范围、行数、缺失数据集，不要先看那些非关键 warning。", meta: "关键数据在：binance / oracle_prices / truth / orderbook。" },
      { title: "什么时候要处理", copy: "只有 backtest 缺关键数据，才需要先补数据。单纯 live 过期不影响离线研究。", meta: "先区分 error 和 warning。" }
    ],
    training_runs: [
      { title: "最少要填什么", copy: "只要先填时间窗、offset 列表、运行名，就能跑最小训练。", meta: "不知道怎么选时：offsets=7,8,9。" },
      { title: "常用默认值", copy: "feature_set 先用 deep_otm_v1，label_set 先用 truth，target 先用 direction。", meta: "不做实验时不要乱改。" },
      { title: "什么时候改并发", copy: "parallel_workers 只是在加速时才改。第一次跑先用默认或 2-3。", meta: "并发太大不一定更快。" }
    ],
    bundles: [
      { title: "这页干什么", copy: "把训练运行打成可回测的模型包，然后决定要不要激活。", meta: "没有 bundle 就没法稳定回测。" },
      { title: "最少要填什么", copy: "构建时最重要的是模型包名和来源训练运行。激活时只要选模型包名。", meta: "offsets 通常沿用训练默认。" },
      { title: "推荐顺序", copy: "先训练，再 build bundle，再 activate，最后回测。", meta: "不要跳步骤。" }
    ],
    backtests: [
      { title: "最少要填什么", copy: "先填回测模板、运行名、模型包名、下注金额。其他高级参数先别碰。", meta: "推荐起点：baseline_truth + 5 USD。" },
      { title: "什么最容易看不懂", copy: "secondary bundle / fallback reasons / parity_json 都属于高级实验参数。第一次回测可以完全不填。", meta: "先把最小回测跑通。" },
      { title: "看结果顺序", copy: "先看结果摘要卡，再看金额扫参页，再看因子和 offset。", meta: "不要一上来盯 JSON。" }
    ],
    experiments: [
      { title: "先选哪种模式", copy: "existing=跑已有实验方案；inline=直接在页面里临时拼一套实验。", meta: "第一次建议先用 existing 或最小 inline。" },
      { title: "最少要填什么", copy: "existing 只要 suite + run_label。inline 再补时间窗、feature_set_variants、stakes_usd。", meta: "别一开始把所有高级字段都填满。" },
      { title: "什么时候做实验", copy: "当你已经能稳定训练和回测后，再来比较不同 feature_set 或 stake matrix。", meta: "先保证基础链路通。" }
    ]
  };
  const FIELD_EXPLANATIONS = {
    window_start: "训练或实验开始日期。建议先用数据真正覆盖到的时间段，不要盲填很大的窗口。",
    window_end: "训练或实验结束日期。必须落在已有 backtest 数据覆盖范围内。",
    offsets: "模型预测的未来 offset 列表。第一次先用 7,8,9。",
    feature_set: "因子集合版本。第一次先用 deep_otm_v1。",
    feature_set_variants: "不同因子集的实验候选。格式可以是 baseline:deep_otm_v1,wide:deep_otm_v2。",
    label_set: "标签数据集。默认 truth 就够用。",
    label_source: "标签来源覆盖项。第一次通常不用填。",
    parallel_workers: "训练时 offset 级并发数。只是加速参数。",
    bundle_label: "模型包名字。回测时会按它去找模型。",
    source_training_run: "这个 bundle 来自哪次训练。通常直接填刚刚训练完的 run。",
    spec: "回测模板名。先用 baseline_truth。",
    stake_usd: "单次下注金额。第一次建议先用 5。",
    max_notional_usd: "单次最大名义金额上限。第一次可用 8。",
    secondary_bundle_label: "备用模型包，只给更复杂的混合策略用。",
    fallback_reasons: "策略回退条件。第一次不用填。",
    parity_json: "高级一致性参数。第一次不用填。",
    suite_mode: "existing=跑已有实验方案；inline=在页面里临时生成实验方案。",
    suite: "实验方案名字。existing 模式必填。",
    markets: "inline 实验要覆盖哪些市场。只做 ETH 就填 eth。",
    run_name: "实验内部运行名，用来区分不同实验族。",
    group_name: "实验分组名，用来把一批 case 归在一起。",
    stakes_usd: "实验里要扫的下注金额列表，比如 1,5,10。",
    parallel_case_workers: "实验 case 并发数。只是加速参数。",
    reference_variant_labels: "哪几个变体当作比较基线。第一次不懂可以保留默认。",
    completed_cases: "重复运行实验时，已完成 case 怎么处理。",
    failed_cases: "重复运行实验时，失败 case 怎么处理。",
    model_family: "训练器实现族。一般保持 deep_otm。",
    backtest_spec: "inline 实验里每个 case 用什么回测模板。默认 baseline_truth。",
    variant_label: "默认变体标签。只是命名用途。",
    variant_notes: "给当前变体加说明，方便回看。"
  };
  const DISPLAY_LABELS = {
    section: "分区",
    dataset: "数据集",
    market: "市场",
    cycle: "周期",
    surface: "数据面",
    profile: "配置",
    target: "目标",
    model_family: "模型族",
    feature_set: "特征集",
    feature_sets: "特征集集合",
    feature_set_count: "特征集数",
    label_set: "标签集",
    label_source: "标签来源",
    section_count: "分区数量",
    action_count: "动作数量",
    row_count: "行数",
    offset_count: "offset 数",
    offset: "offset",
    offsets: "offset 列表",
    trades: "成交数",
    roi_pct: "收益率%",
    avg_roi_pct: "平均收益率%",
    win_rate_pct: "胜率%",
    pnl_sum: "PnL",
    cases: "案例数",
    completed_cases: "已完成案例",
    failed_cases: "失败案例",
    resumed_cases: "复用案例",
    recent_tasks: "最近任务",
    active_tasks: "活跃任务",
    terminal_tasks: "结束任务",
    failed_tasks: "失败任务",
    latest_active_task: "最新活跃任务",
    latest_terminal_task: "最新结束任务",
    latest_failed_task: "最新失败任务",
    runtime_updated_at: "运行态更新时间",
    action_catalog: "动作目录",
    action_id: "动作 ID",
    task_id: "任务 ID",
    PnL: "PnL",
    "ROI %": "收益率%",
    "Win Rate %": "胜率%",
    Trades: "成交数",
    Rejects: "拒单数",
    "Stake USD": "下注金额 USD",
    Offsets: "offset 数",
    "Parallel Workers": "并发 worker",
    "Feature Set": "特征集",
    "Label Set": "标签集",
    "Rows Total": "总样本数",
    subject: "对象",
    stage: "阶段",
    heartbeat: "最近心跳",
    progress: "进度",
    started_at: "开始时间",
    finished_at: "结束时间",
    updated_at: "更新时间",
    result: "结果",
    error: "错误",
    error_detail: "错误详情",
    linked_object: "关联对象",
    linked_object_detail: "关联对象详情",
    return_code: "返回码",
    parsed_stdout: "已解析输出",
    status: "状态",
    type: "类型",
    message: "消息",
    result_status: "结果状态",
    last_stderr_line: "最后一行 stderr",
    stderr_excerpt: "stderr 摘要",
    spec: "回测规格",
    spec_name: "回测规格",
    suite: "实验套件",
    suite_name: "实验套件",
    bundle: "模型包",
    bundle_label: "模型包标签",
    run: "运行标签",
    run_label: "运行标签",
    source_training_run: "来源训练运行",
    sync: "同步命令",
    build: "构建命令",
    output: "输出",
    path: "路径",
    run_dir: "运行目录",
    summary_path: "摘要路径",
    manifest_path: "清单路径",
    selection_path: "激活选择路径",
    window: "时间窗口",
    first_offset: "首个 offset",
    rows: "样本数",
    feature_count: "因子数",
    folds_used: "使用折数",
    positive_rate: "正样本率",
    parallel_workers: "并发 worker",
    bundle_ready_offsets: "可直接打包 offset 数",
    bundle_ready_offset_count: "可直接打包 offset 数",
    bundle_missing_offset_count: "缺失打包产物 offset 数",
    is_ready: "是否可直接打包",
    ready_offset_count: "已就绪 offset 数",
    missing_offset_count: "缺失 offset 数",
    missing_artifact_counts: "缺失产物统计",
    required_artifacts: "必需产物",
    auc: "AUC",
    brier: "Brier",
    logloss: "Logloss",
    mean_auc: "平均 AUC",
    mean_brier: "平均 Brier",
    mean_logloss: "平均 Logloss",
    best_auc_offset: "最佳 AUC offset",
    best_brier_offset: "最佳 Brier offset",
    offsets_with_metrics: "有指标的 offset 数",
    offsets_with_auc: "有 AUC 的 offset 数",
    offsets_with_brier: "有 Brier 的 offset 数",
    offsets_with_logloss: "有 Logloss 的 offset 数",
    blend_w_lgb: "LGB 混合权重",
    blend_w_lr: "LR 混合权重",
    bundle_ready: "可直接打包",
    missing_bundle_artifacts: "缺失打包产物",
    offsets_with_explainability: "有解释性产物的 offset 数",
    offsets_with_logreg_coefficients: "有 LogReg 权重的 offset 数",
    offsets_with_lgb_importance: "有 LGB 重要性的 offset 数",
    offsets_with_factor_direction_summary: "有方向汇总的 offset 数",
    offsets_with_factor_correlations: "有相关矩阵的 offset 数",
    top_logreg: "LogReg 重点因子",
    top_lgb: "LGB 重点因子",
    top_positive: "正向因子",
    top_negative: "负向因子",
    feature: "因子",
    factor_correlations: "因子相关文件",
    direction_summary: "方向汇总文件",
    wins: "胜场",
    losses: "败场",
    rejects: "拒单数",
    stake_usd: "下注金额 USD",
    max_notional_usd: "最大名义金额 USD",
    stake_label: "金额档",
    matrix_label: "矩阵标签",
    secondary_bundle: "副模型包",
    secondary_bundle_label: "副模型包",
    variant: "变体",
    fallback_reasons: "回退原因",
    parity: "一致性参数",
    best_stake_by_roi: "最佳 ROI 金额",
    best_stake_roi_pct: "最佳金额 ROI%",
    best_stake_by_pnl: "最佳 PnL 金额",
    best_stake_pnl_sum: "最佳金额 PnL",
    stake_min: "最小金额",
    stake_max: "最大金额",
    roi_min: "最低 ROI%",
    roi_max: "最高 ROI%",
    pnl_min: "最低 PnL",
    pnl_max: "最高 PnL",
    best_offset_by_pnl: "最佳 PnL offset",
    best_offset_pnl_sum: "最佳 offset PnL",
    best_offset_by_roi: "最佳 ROI offset",
    best_offset_roi_pct: "最佳 offset ROI%",
    top_positive_factor: "最佳正向因子",
    top_negative_factor: "最弱负向因子",
    top_positive_correlation_factor: "最强正相关因子",
    top_negative_correlation_factor: "最强负相关因子",
    equity_rows: "资金曲线行数",
    decision_ts: "决策时间",
    trade_number: "交易序号",
    cumulative_pnl: "累计 PnL",
    pnl_correlation: "PnL 相关性",
    abs_pnl_correlation: "|PnL 相关性|",
    latest_cumulative_pnl: "最新累计 PnL",
    stake_sweep_rows: "金额扫参行数",
    offset_summary_rows: "offset 汇总行数",
    factor_pnl_rows: "因子 PnL 行数",
    report_path: "报告路径",
    rank: "排名",
    case_key: "案例键",
    run_name: "运行名",
    detail_view: "结果视图",
    overview: "总览",
    stake_sweep: "金额扫参",
    matrix: "矩阵结果",
    top_roi_pct: "最高 ROI%",
    variant_label: "变体标签",
    reference_variant_label: "基线变体",
    best_completed_variant_label: "最佳完成变体",
    roi_pct_delta_vs_reference: "相对基线 ROI 差值",
    pnl_sum_delta_vs_reference: "相对基线 PnL 差值",
    comparison_vs_reference: "相对基线结论",
    bundle_dir: "模型包目录",
    backtest_run_dir: "回测目录",
    group_name: "分组名",
    training_reused: "复用训练数",
    bundle_reused: "复用模型包数",
    best_matrix_run: "最佳矩阵运行",
    best_matrix_stake: "最佳矩阵金额",
    best_matrix_roi_pct: "最佳矩阵 ROI%",
    best_variant_label: "最佳变体",
    best_variant_run: "最佳变体运行",
    variant_vs_reference: "相对基线对比",
    matrix_parent_run_name: "矩阵主运行",
    matrix_stake_label: "矩阵金额档",
    best_feature_set: "最佳特征集",
    stake_level_count: "金额档数",
    notional_level_count: "名义金额档数",
    best_stake_label: "最佳金额档",
    best_case_feature_set: "最佳案例特征集",
    best_variant_feature_set: "最佳变体特征集",
    best_matrix_stake_label: "最佳矩阵金额档",
    best_run_name: "最佳运行名",
    best_roi_pct: "最佳 ROI%",
    bundle_count: "模型包数",
    leaderboard_row_count: "排行榜行数",
    compare_row_count: "对比表行数",
    matrix_row_count: "矩阵表行数",
    variant_row_count: "变体表行数",
    failed_row_count: "失败表行数",
    market_leader_count: "市场领先者数",
    group_leader_count: "分组领先者数",
    run_leader_count: "运行领先者数",
    best_market: "最佳市场",
    best_market_run_name: "最佳市场运行",
    best_market_variant_label: "最佳市场变体",
    best_market_roi_pct: "最佳市场 ROI%",
    best_group_name: "最佳分组",
    best_group_run_name: "最佳分组运行",
    best_group_variant_label: "最佳分组变体",
    best_group_roi_pct: "最佳分组 ROI%",
    best_run_market: "最佳运行市场",
    best_run_variant_label: "最佳运行变体",
    best_run_roi_pct: "最佳运行 ROI%",
    best_case_market: "最佳案例市场",
    best_case_group_name: "最佳案例分组",
    best_case_run_name: "最佳案例运行",
    best_case_bundle_dir: "最佳案例模型包目录",
    best_case_pnl_sum: "最佳案例 PnL",
    best_matrix_market: "最佳矩阵市场",
    best_matrix_group_name: "最佳矩阵分组",
    best_matrix_run_name: "最佳矩阵运行",
    best_variant_market: "最佳变体市场",
    best_variant_group_name: "最佳变体分组",
    best_variant_roi_delta_vs_reference: "最佳变体 ROI 增量",
    best_variant_pnl_delta_vs_reference: "最佳变体 PnL 增量",
    best_variant_comparison: "最佳变体结论",
    total_pnl_sum: "总 PnL",
    total_trades: "总成交数",
    rows_total: "总样本数",
    positive_rate_avg: "平均正样本率",
    feature_count_range: "因子数范围",
    offsets_with_explainability: "有解释产物的 offset 数",
    offsets_with_logreg_coefficients: "有 LogReg 系数的 offset 数",
    offsets_with_lgb_importance: "有 LGB 重要性的 offset 数",
    offsets_with_factor_direction_summary: "有方向摘要的 offset 数",
    offsets_with_factor_correlations: "有相关矩阵的 offset 数",
    secondary_training_reused_cases: "复用副训练数",
    secondary_bundle_reused_cases: "复用副模型包数",
    market_count: "市场数",
    group_count: "分组数",
    run_name_count: "运行数",
    variant_count: "变体数",
    target_count: "目标数",
    stake_point_count: "金额档数",
    notional_point_count: "名义金额档数",
    stake_usd_values: "金额档集合",
    max_notional_usd_values: "名义金额档集合",
    markets: "市场集合",
    groups: "分组集合",
    run_names: "运行集合",
    variant_labels: "变体集合",
    failed_case_rows: "失败案例行数",
    run_count: "运行数",
    reference_variant_count: "基线变体数",
    first_failed_case: "首个失败案例",
    failure_stage_counts: "失败阶段分布",
    error_type_counts: "错误类型分布",
    market_counts: "失败市场分布",
    group_counts: "失败分组分布",
    is_active: "已激活",
    invalid_task_files: "无效任务文件",
    history_truncated: "历史截断",
    pnl_range_min: "最低累计 PnL",
    pnl_range_max: "最高累计 PnL",
    cumulative_roi_pct: "累计 ROI%",
    cumulative_trades: "累计成交数",
    equity_point_count: "资金曲线点数",
    refresh_summary: "刷新摘要",
    sync_sources: "同步数据",
    build_tables: "构建表",
    train_run: "训练运行",
    build_bundle: "构建模型包",
    activate_bundle: "激活模型包",
    run_backtest: "运行回测",
    run_experiment_suite: "运行实验套件",
    data: "数据",
    research: "研究"
  };
  const STATUS_LABELS = {
    queued: "排队中",
    running: "运行中",
    succeeded: "成功",
    completed: "已完成",
    failed: "失败",
    ok: "成功",
    error: "错误",
    unknown: "未知",
    active: "活跃",
    terminal: "已结束",
    warning: "警告",
    info: "信息",
    Queued: "排队中",
    Running: "运行中",
    Completed: "已完成",
    Failed: "失败"
  };

  let state = {};
  let actionCatalogPromise = null;
  if (bootstrapNode && bootstrapNode.textContent) {
    try {
      state = JSON.parse(bootstrapNode.textContent);
    } catch (_error) {
      state = {};
    }
  }

  function displayLabel(label) {
    const key = String(label || "").trim();
    if (!key) {
      return "";
    }
    if (Object.prototype.hasOwnProperty.call(DISPLAY_LABELS, key)) {
      return DISPLAY_LABELS[key];
    }
    return key.replaceAll("_", " ");
  }

  function sectionLabel(sectionId) {
    const key = normalizeSection(sectionId);
    return SECTION_LABELS[key] || key.replaceAll("_", " ");
  }

  function displayStatus(status) {
    const token = String(status || "").trim();
    if (!token) {
      return "";
    }
    const normalized = token.toLowerCase();
    if (Object.prototype.hasOwnProperty.call(STATUS_LABELS, normalized)) {
      return STATUS_LABELS[normalized];
    }
    if (Object.prototype.hasOwnProperty.call(STATUS_LABELS, token)) {
      return STATUS_LABELS[token];
    }
    return token;
  }

  function translateProgressText(text) {
    const token = String(text || "").trim();
    if (!token) {
      return "";
    }
    if (Object.prototype.hasOwnProperty.call(STATUS_LABELS, token)) {
      return STATUS_LABELS[token];
    }
    return token
      .replaceAll("Queued", "排队中")
      .replaceAll("Running", "运行中")
      .replaceAll("Completed", "已完成")
      .replaceAll("Failed", "失败")
      .replaceAll("stage=", "阶段=")
      .replaceAll("output=", "输出=");
  }

  function isTaskActiveStatus(status) {
    const normalized = String(status || "").trim();
    return normalized === "queued" || normalized === "running";
  }

  function isTaskTerminalStatus(status) {
    const normalized = String(status || "").trim();
    return normalized === "succeeded" || normalized === "failed" || normalized === "ok" || normalized === "error";
  }

  function taskIdFromPayload(payload) {
    return String((payload || {}).task_id || "").trim();
  }

  function formatConsoleTimestamp(value) {
    const token = String(value || "").trim();
    if (!token) {
      return "";
    }
    const parsed = new Date(token);
    if (Number.isNaN(parsed.getTime())) {
      return token;
    }
    return parsed.toLocaleString("zh-CN", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  }

  function heartbeatAgeText(value) {
    const token = String(value || "").trim();
    if (!token) {
      return "";
    }
    const parsed = new Date(token);
    if (Number.isNaN(parsed.getTime())) {
      return "";
    }
    const ageMs = Math.max(0, Date.now() - parsed.getTime());
    const ageSec = Math.floor(ageMs / 1000);
    if (ageSec < 60) {
      return String(ageSec) + " 秒前";
    }
    const ageMin = Math.floor(ageSec / 60);
    if (ageMin < 60) {
      return String(ageMin) + " 分钟前";
    }
    const ageHour = Math.floor(ageMin / 60);
    return String(ageHour) + " 小时前";
  }

  function progressHeartbeatText(progress) {
    if (!progress || typeof progress !== "object") {
      return "";
    }
    const heartbeat = String(progress.heartbeat || "").trim();
    if (!heartbeat) {
      return "";
    }
    const age = heartbeatAgeText(heartbeat);
    const formatted = formatConsoleTimestamp(heartbeat);
    if (age && formatted) {
      return age + " · " + formatted;
    }
    return age || formatted || heartbeat;
  }

  function normalizeSection(sectionId) {
    const fallback = String(state.active_section || "home");
    const value = String(sectionId || fallback).trim();
    return value || "home";
  }

  function sectionApiPath(sectionId) {
    const base = String(shell.dataset.apiBase || "/api/console/").replace(/\\/$/, "");
    return base + "/" + encodeURIComponent(normalizeSection(sectionId).replaceAll("_", "-"));
  }

  function detailRoutePath(sectionId) {
    const section = normalizeSection(sectionId);
    const base = sectionApiPath(section);
    const view = activeDetailView(section);
    if (section === "backtests" && view === "stake_sweep") {
      return base + "/stake-sweep";
    }
    if (section === "experiments" && view === "matrix") {
      return base + "/matrix";
    }
    return base;
  }

  function actionExecutePath() {
    const base = String(shell.dataset.apiBase || "/api/console").replace(/\\/$/, "");
    return base + "/actions/execute";
  }

  function actionCatalogPath() {
    const base = String(shell.dataset.apiBase || "/api/console").replace(/\\/$/, "");
    return base + "/actions";
  }

  function taskStatusPath(taskId) {
    const base = String(shell.dataset.apiBase || "/api/console").replace(/\\/$/, "");
    const url = new URL(base + "/tasks", window.location.origin);
    url.searchParams.set("task_id", String(taskId || ""));
    return url;
  }

  function tasksListPath(actionIds, limit) {
    const base = String(shell.dataset.apiBase || "/api/console").replace(/\\/$/, "");
    const url = new URL(base + "/tasks", window.location.origin);
    const tokens = Array.isArray(actionIds)
      ? actionIds.map((value) => String(value || "").trim()).filter(Boolean)
      : [];
    if (tokens.length) {
      url.searchParams.set("action_ids", tokens.join(","));
    }
    url.searchParams.set("limit", String(limit || 6));
    return url;
  }

  function currentInputs() {
    const values = {};
    inputNodes.forEach((node) => {
      values[String(node.dataset.consoleInput || "")] = String(node.value || "").trim();
    });
    return values;
  }

  function renderGuideCards(node, items) {
    if (!node) {
      return;
    }
    node.innerHTML = "";
    const rows = Array.isArray(items) ? items.filter((item) => item && item.title && item.copy) : [];
    rows.forEach((item) => {
      const article = document.createElement("article");
      article.className = "console-guide-card";
      article.innerHTML =
        '<h3 class="console-guide-card-title">' + escapeHtml(String(item.title || "")) + '</h3>' +
        '<p class="console-guide-card-copy">' + escapeHtml(String(item.copy || "")) + '</p>' +
        ((item.meta && String(item.meta).trim())
          ? ('<p class="console-guide-card-meta">' + escapeHtml(String(item.meta)) + '</p>')
          : "");
      node.appendChild(article);
    });
  }

  function visibleGlobalFieldIds(sectionId) {
    const section = normalizeSection(sectionId);
    return SECTION_VISIBLE_GLOBAL_FIELDS[section] || SECTION_VISIBLE_GLOBAL_FIELDS.home || [];
  }

  function renderGlobalControls(sectionId) {
    const visible = new Set(visibleGlobalFieldIds(sectionId));
    inputNodes.forEach((node) => {
      const wrapper = node.closest(".console-field");
      if (!wrapper) {
        return;
      }
      wrapper.hidden = !visible.has(String(node.dataset.consoleInput || ""));
    });
    renderGuideCards(
      globalGuideNode,
      visibleGlobalFieldIds(sectionId).map((fieldId) => {
        const item = GLOBAL_CONTROL_HELP[fieldId] || {};
        return {
          title: item.title || displayLabel(fieldId),
          copy: item.copy || "",
          meta: item.meta || "",
        };
      })
    );
  }

  function renderSectionUsageGuide(panel, sectionId) {
    if (!panel) {
      return;
    }
    renderGuideCards(
      panel.querySelector("[data-console-parameter-guide]"),
      SECTION_USAGE_GUIDES[normalizeSection(sectionId)] || []
    );
  }

  function writeInputsToUrl(url) {
    const target = url instanceof URL ? url : new URL(window.location.href);
    const values = currentInputs();
    Object.keys(values).forEach((key) => {
      const value = String(values[key] || "").trim();
      if (value) {
        target.searchParams.set(key, value);
      } else {
        target.searchParams.delete(key);
      }
    });
    return target;
  }

  function defaultDetailFilterState(sectionId) {
    const section = normalizeSection(sectionId);
    if (section === "backtests") {
      return { search: "", topN: 5 };
    }
    if (section === "experiments") {
      return { topN: 5, market: "", runName: "", variant: "" };
    }
    return { search: "", topN: 5 };
  }

  function detailViewOptions(sectionId) {
    const section = normalizeSection(sectionId);
    if (section === "backtests") {
      return [
        { id: "overview", label: "回测总览", note: "先看绩效、因子和资金曲线，再决定是否继续下钻。" },
        { id: "stake_sweep", label: "金额扫参页", note: "专门比较 stake / notional 组合，直接回答哪个金额档更优。" }
      ];
    }
    if (section === "experiments") {
      return [
        { id: "overview", label: "实验总览", note: "先看排行榜、领先者和变体结论。" },
        { id: "matrix", label: "矩阵结果页", note: "专门比较 matrix / stake 组合和对应特征集表现。" }
      ];
    }
    return [{ id: "overview", label: "总览", note: "" }];
  }

  function hasDetailViewOptions(sectionId) {
    return detailViewOptions(sectionId).length > 1;
  }

  function defaultDetailView(sectionId) {
    const options = detailViewOptions(sectionId);
    return String((options[0] || {}).id || "overview");
  }

  function activeDetailView(sectionId) {
    const section = normalizeSection(sectionId);
    const allowed = detailViewOptions(section).map((item) => String(item.id || ""));
    const params = new URLSearchParams(window.location.search);
    const fromUrl = String(params.get("detail_view") || "").trim();
    const fromState = String(detailViewStateBySection[section] || "").trim();
    const candidate = fromState || fromUrl;
    if (allowed.includes(candidate)) {
      detailViewStateBySection[section] = candidate;
      return candidate;
    }
    const fallback = defaultDetailView(section);
    detailViewStateBySection[section] = fallback;
    return fallback;
  }

  function setDetailView(sectionId, viewId, options) {
    const section = normalizeSection(sectionId);
    const allowed = detailViewOptions(section).map((item) => String(item.id || ""));
    const candidate = String(viewId || "").trim();
    const next = allowed.includes(candidate) ? candidate : defaultDetailView(section);
    detailViewStateBySection[section] = next;
    const url = new URL(window.location.href);
    if (next === defaultDetailView(section)) {
      url.searchParams.delete("detail_view");
    } else {
      url.searchParams.set("detail_view", next);
    }
    if (!options || options.pushState !== false) {
      window.history.replaceState(
        Object.assign({}, window.history.state || {}, { detail_view: next }),
        "",
        url
      );
    }
    return next;
  }

  function detailFilterState(sectionId) {
    const key = normalizeSection(sectionId);
    if (!detailFilterStateBySection[key]) {
      detailFilterStateBySection[key] = defaultDetailFilterState(key);
    }
    return detailFilterStateBySection[key];
  }

  function updateDetailFilterState(sectionId, nextPatch) {
    const key = normalizeSection(sectionId);
    const current = Object.assign({}, detailFilterState(key));
    detailFilterStateBySection[key] = Object.assign(current, nextPatch || {});
    return detailFilterStateBySection[key];
  }

  function tableSortStateValue(tableKey, options) {
    const key = String(tableKey || "");
    const current = tableSortState[key];
    if (current && current.column) {
      return current;
    }
    const defaultSortBy = String((options || {}).defaultSortBy || "").trim();
    const defaultDirection = String((options || {}).defaultDirection || "desc").trim() || "desc";
    return defaultSortBy ? { column: defaultSortBy, direction: defaultDirection } : { column: "", direction: "desc" };
  }

  function updateTableSortState(tableKey, column, options) {
    const key = String(tableKey || "");
    const current = tableSortStateValue(key, options);
    if (current.column === column) {
      tableSortState[key] = {
        column: column,
        direction: current.direction === "asc" ? "desc" : "asc"
      };
      return tableSortState[key];
    }
    tableSortState[key] = {
      column: column,
      direction: "desc"
    };
    return tableSortState[key];
  }

  function actionFormStorageKey(sectionId, actionId, fieldId) {
    return normalizeSection(sectionId) + "::" + String(actionId || "") + "::" + String(fieldId || "");
  }

  function loadActionFormValue(sectionId, actionId, field, defaultValue) {
    const key = actionFormStorageKey(sectionId, actionId, field.field_id);
    if (Object.prototype.hasOwnProperty.call(actionFormState, key)) {
      return actionFormState[key];
    }
    return defaultValue;
  }

  function saveActionFormValue(sectionId, actionId, fieldId, value) {
    actionFormState[actionFormStorageKey(sectionId, actionId, fieldId)] = String(value || "");
  }

  function actionFormValues(actionId, sectionId) {
    const values = {};
    const selector =
      '[data-console-action-form-input][data-form-section="' + normalizeSection(sectionId) +
      '"][data-action-id="' + String(actionId || "") + '"]';
    Array.from(document.querySelectorAll(selector)).forEach((node) => {
      values[String(node.dataset.fieldId || "")] = String(node.value || "").trim();
    });
    return values;
  }

  function pathToken(value, prefix) {
    const text = String(value || "");
    const parts = text.split("/");
    for (let idx = 0; idx < parts.length; idx += 1) {
      if (parts[idx].startsWith(prefix + "=")) {
        return parts[idx].slice(prefix.length + 1);
      }
    }
    return "";
  }

  function mergeContextValue(context, key, value) {
    if (context[key] !== undefined || value === undefined || value === null) {
      return;
    }
    if (typeof value === "string" && !value.trim()) {
      return;
    }
    context[key] = value;
  }

  function sectionRows(sectionId, payload) {
    if (Array.isArray((payload || {}).rows)) {
      return payload.rows;
    }
    if (normalizeSection(sectionId) === "data_overview" && Array.isArray((payload || {}).dataset_rows)) {
      return payload.dataset_rows;
    }
    return [];
  }

  function actionContextFromPayload(sectionId, payload) {
    const source = payload && typeof payload.action_context === "object"
      ? payload.action_context
      : {};
    const context = Object.assign({}, source);
    const raw = payload || {};
    mergeContextValue(context, "market", raw.market);
    mergeContextValue(context, "cycle", raw.cycle);
    mergeContextValue(context, "surface", raw.surface);
    mergeContextValue(context, "profile", raw.profile);
    mergeContextValue(context, "target", raw.target);
    mergeContextValue(context, "model_family", raw.model_family);
    mergeContextValue(context, "bundle_label", raw.bundle_label);
    mergeContextValue(context, "run_label", raw.run_label);
    mergeContextValue(context, "spec", raw.spec || raw.spec_name);
    mergeContextValue(context, "spec_name", raw.spec_name || raw.spec);
    mergeContextValue(context, "suite", raw.suite || raw.suite_name);
    mergeContextValue(context, "suite_name", raw.suite_name || raw.suite);
    return context;
  }

  function actionContextFromRow(sectionId, row, payloadActionContext) {
    const source = (row && typeof row.action_context === "object" && row.action_context)
      || (payloadActionContext && typeof payloadActionContext === "object" && payloadActionContext)
      || {};
    const merged = Object.assign({}, source);
    mergeContextValue(merged, "market", row && row.market);
    mergeContextValue(merged, "cycle", row && row.cycle);
    mergeContextValue(merged, "surface", row && row.surface);
    mergeContextValue(merged, "profile", row && row.profile);
    mergeContextValue(merged, "target", row && row.target);
    mergeContextValue(merged, "model_family", row && row.model_family);
    mergeContextValue(merged, "feature_set", row && row.feature_set);
    mergeContextValue(merged, "label_set", row && row.label_set);
    mergeContextValue(merged, "bundle_label", row && row.bundle_label);
    mergeContextValue(merged, "run_label", row && row.run_label);
    mergeContextValue(merged, "spec", row && row.spec_name);
    mergeContextValue(merged, "spec_name", row && row.spec_name);
    mergeContextValue(merged, "suite", row && row.suite_name);
    mergeContextValue(merged, "suite_name", row && row.suite_name);
    mergeContextValue(merged, "dataset_name", row && row.dataset_name);
    mergeContextValue(merged, "kind", row && row.kind);
    mergeContextValue(merged, "status", row && row.status);
    mergeContextValue(merged, "location", row && row.location);
    if (!merged.source_training_run && row) {
      merged.source_training_run = pathToken(row.source_training_run_dir || row.training_run_dir || "", "run");
    }
    if (!merged.offsets && row && Array.isArray(row.offsets)) {
      merged.offsets = row.offsets.join(",");
    }
    if (!merged.window_start && row && row.window) {
      const parts = String(row.window).split("_");
      if (parts.length === 2) {
        merged.window_start = parts[0];
        merged.window_end = parts[1];
      }
    }
    return merged;
  }

  function sectionActionContext(sectionId) {
    return Object.assign({}, selectedRowContextBySection[sectionId] || {});
  }

  function effectiveActionContext(sectionId) {
    const selected = sectionActionContext(sectionId);
    if (Object.keys(selected).length) {
      return selected;
    }
    const payload = latestPayloadBySection[normalizeSection(sectionId)] || {};
    const payloadActionContext = actionContextFromPayload(sectionId, payload);
    return actionContextFromRow(sectionId, null, payloadActionContext);
  }

  function applyBootstrapDefaults() {
    const defaults = Object.assign({}, state.defaults || {});
    const params = new URLSearchParams(window.location.search);
    inputNodes.forEach((node) => {
      const key = String(node.dataset.consoleInput || "");
      const nextValue = params.get(key) || defaults[key] || node.value || "";
      node.value = nextValue;
    });
  }

  function sectionQuery(sectionId) {
    const inputs = currentInputs();
    const section = normalizeSection(sectionId);
    if (section === "home") {
      return {};
    }
    if (section === "data_overview") {
      return { market: inputs.market, cycle: inputs.cycle, surface: inputs.surface };
    }
    if (section === "training_runs") {
      return { market: inputs.market, cycle: inputs.cycle, model_family: inputs.model_family, target: inputs.target };
    }
    if (section === "bundles") {
      return { market: inputs.market, cycle: inputs.cycle, profile: inputs.profile, target: inputs.target };
    }
    if (section === "backtests") {
      return { market: inputs.market, cycle: inputs.cycle, profile: inputs.profile, spec: inputs.spec };
    }
    if (section === "experiments") {
      return { suite: inputs.suite };
    }
    return {};
  }

  function buildUrl(sectionId) {
    const url = new URL(sectionApiPath(sectionId), window.location.origin);
    const query = sectionQuery(sectionId);
    Object.keys(query).forEach((key) => {
      const value = String(query[key] || "").trim();
      if (value) {
        url.searchParams.set(key, value);
      }
    });
    return url;
  }

  function sectionSupportsDetail(sectionId) {
    const section = normalizeSection(sectionId);
    return ["training_runs", "bundles", "backtests", "experiments"].includes(section);
  }

  function sectionPayloadLooksLikeDetail(sectionId, payload) {
    const section = normalizeSection(sectionId);
    const dataset = String((payload || {}).dataset || "").trim();
    if (section === "training_runs") {
      return dataset === "console_training_run";
    }
    if (section === "bundles") {
      return dataset === "console_model_bundle";
    }
    if (section === "backtests") {
      return dataset === "console_backtest_run_detail";
    }
    if (section === "experiments") {
      return dataset === "console_experiment_run_detail";
    }
    return false;
  }

  function detailQueryForRow(sectionId, row, payloadActionContext) {
    const section = normalizeSection(sectionId);
    const context = actionContextFromRow(sectionId, row, payloadActionContext);
    if (section === "training_runs") {
      return {
        market: context.market,
        cycle: context.cycle,
        model_family: context.model_family,
        target: context.target,
        run_label: context.run_label,
        run_dir: context.run_dir
      };
    }
    if (section === "bundles") {
      return {
        market: context.market,
        cycle: context.cycle,
        profile: context.profile,
        target: context.target,
        bundle_label: context.bundle_label,
        bundle_dir: context.bundle_dir
      };
    }
    if (section === "backtests") {
      return {
        market: context.market,
        cycle: context.cycle,
        profile: context.profile,
        spec: context.spec_name || context.spec,
        run_label: context.run_label
      };
    }
    if (section === "experiments") {
      return {
        suite: context.suite_name || context.suite,
        run_label: context.run_label
      };
    }
    return {};
  }

  function detailUrlForRow(sectionId, row, payloadActionContext) {
    const url = new URL(detailRoutePath(sectionId), window.location.origin);
    const query = detailQueryForRow(sectionId, row, payloadActionContext);
    Object.keys(query).forEach((key) => {
      const value = query[key];
      if (value === undefined || value === null || String(value).trim() === "") {
        return;
      }
      url.searchParams.set(key, String(value));
    });
    return url;
  }

  function executableActionIds() {
    return new Set([
      "data_refresh_summary",
      "data_sync",
      "data_build",
      "research_train_run",
      "research_bundle_build",
      "research_activate_bundle",
      "research_backtest_run",
      "research_experiment_run_suite"
    ]);
  }

  function homeActionIds() {
    return new Set([
      "data_refresh_summary",
      "research_activate_bundle",
      "research_backtest_run",
      "research_experiment_run_suite"
    ]);
  }

  function sectionActionIds(sectionId) {
    if (sectionId === "home") {
      return null;
    }
    const mapping = {
      data_overview: ["data_refresh_summary", "data_sync", "data_build"],
      training_runs: ["research_train_run"],
      bundles: ["research_bundle_build", "research_activate_bundle"],
      backtests: ["research_backtest_run"],
      experiments: ["research_experiment_run_suite"]
    };
    return mapping[sectionId] || [];
  }

  function sectionTaskActionIds(sectionId) {
    if (normalizeSection(sectionId) === "home") {
      return Array.from(executableActionIds()).sort();
    }
    const ids = sectionActionIds(sectionId);
    return Array.isArray(ids) ? ids.filter(Boolean) : [];
  }

  function actionIsShellEnabled(action) {
    if (typeof action.shell_enabled === "boolean") {
      return action.shell_enabled;
    }
    return executableActionIds().has(String(action.action_id || ""));
  }

  function actionBelongsToSection(action, sectionId) {
    if (sectionId === "home") {
      return homeActionIds().has(String(action.action_id || ""));
    }
    const descriptorSections = Array.isArray(action.section_ids)
      ? action.section_ids.map((value) => String(value || ""))
      : [];
    if (descriptorSections.length) {
      return descriptorSections.includes(sectionId);
    }
    return sectionActionIds(sectionId).includes(String(action.action_id || ""));
  }

  function sectionCatalogActions(sectionId, catalog) {
    if (!catalog || !Array.isArray(catalog.actions) || !catalog.actions.length) {
      return [];
    }
    const scopedActionIds = sectionActionIds(sectionId);
    return catalog.actions.filter((action) => {
      return scopedActionIds === null || actionBelongsToSection(action, sectionId);
    });
  }

  function executableSectionActions(sectionId, catalog) {
    return sectionCatalogActions(sectionId, catalog).filter((action) => actionIsShellEnabled(action));
  }

  async function ensureActionCatalog() {
    if (state.actionCatalog && Array.isArray(state.actionCatalog.actions)) {
      return state.actionCatalog;
    }
    if (!actionCatalogPromise) {
      actionCatalogPromise = fetch(actionCatalogPath())
        .then((response) => response.json().then((payload) => ({ ok: response.ok, payload: payload })))
        .then((result) => {
          if (!result.ok) {
            throw new Error(((result.payload || {}).error || {}).message || "加载动作目录失败。");
          }
          state.actionCatalog = result.payload;
          return state.actionCatalog;
        })
        .catch((error) => {
          actionCatalogPromise = null;
          throw error;
        });
    }
    return actionCatalogPromise;
  }

  async function loadSection(sectionId) {
    const resolved = normalizeSection(sectionId);
    const panel = panels.find((node) => node.dataset.sectionId === resolved);
    if (!panel) {
      return;
    }
    const statusNode = panel.querySelector("[data-console-status]");
    const summaryNode = panel.querySelector("[data-console-summary]");
    const tableWrap = panel.querySelector("[data-console-table-wrap]");
    const jsonNode = panel.querySelector("[data-console-json]");

    if (statusNode) {
      statusNode.textContent = "正在加载分区数据...";
    }
    if (summaryNode) {
      summaryNode.innerHTML = "";
    }
    if (tableWrap) {
      tableWrap.innerHTML = "";
    }

    const catalogPromise = ensureActionCatalog().catch((_error) => null);
    try {
      const response = await fetch(buildUrl(resolved));
      const payload = await response.json();
      latestPayloadBySection[resolved] = payload;
      await catalogPromise;
      renderPayload({
        panel: panel,
        sectionId: resolved,
        statusNode: statusNode,
        summaryNode: summaryNode,
        tableWrap: tableWrap,
        jsonNode: jsonNode,
        payload: payload,
        ok: response.ok
      });
    } catch (error) {
      if (statusNode) {
        statusNode.textContent = "加载分区数据失败。";
      }
      if (jsonNode) {
        jsonNode.textContent = JSON.stringify({ error: String(error) }, null, 2);
      }
    }
  }

  function renderPayload(context) {
    const payload = context.payload || {};
    const rows = sectionRows(context.sectionId, payload);
    const catalog = payload.action_catalog || state.actionCatalog || null;
    const payloadActionContext = actionContextFromPayload(context.sectionId, payload);
    const actionContextNode = context.panel
      ? context.panel.querySelector("[data-console-action-context]")
      : null;
    const rowQuickActionsNode = context.panel
      ? context.panel.querySelector("[data-console-row-quick-actions]")
      : null;
    const actionCatalogNode = context.panel
      ? context.panel.querySelector("[data-console-action-catalog]")
      : null;
    const actionFormsNode = context.panel
      ? context.panel.querySelector("[data-console-action-forms]")
      : null;
    const recentTasksNode = context.panel
      ? context.panel.querySelector("[data-console-recent-tasks]")
      : null;
    const sectionDetailNode = context.panel
      ? context.panel.querySelector("[data-console-section-detail]")
      : null;
    const runtimeBoardNode = context.panel
      ? context.panel.querySelector("[data-console-runtime-board]")
      : null;
    const actionResultContext = context.panel
      ? {
          sectionId: context.sectionId,
          panel: context.panel,
          summaryNode: context.panel.querySelector("[data-console-action-result-summary]"),
          drilldownNode: context.panel.querySelector("[data-console-action-drilldown]"),
          commandNode: context.panel.querySelector("[data-console-action-result-command]"),
          parsedNode: context.panel.querySelector("[data-console-action-result-parsed]"),
          logsNode: context.panel.querySelector("[data-console-action-result-logs]")
        }
      : null;
    const rowQuickActionsContext = rowQuickActionsNode
      ? {
          node: rowQuickActionsNode,
          sectionId: context.sectionId,
          catalog: catalog,
          statusNode: context.statusNode,
          jsonNode: context.jsonNode,
          actionResultContext: actionResultContext
        }
      : null;
    const recentTasksContext = recentTasksNode
      ? {
          sectionId: context.sectionId,
          node: recentTasksNode,
          statusNode: context.statusNode,
          jsonNode: context.jsonNode,
          actionResultContext: actionResultContext
        }
      : null;
    renderSectionUsageGuide(context.panel, context.sectionId);
    if (context.statusNode) {
      context.statusNode.textContent = context.ok
        ? ("已加载 " + displayLabel(payload.dataset || payload.section || context.sectionId))
        : ("请求失败：" + ((payload.error || {}).message || "未知错误"));
    }
    if (sectionDetailNode) {
      renderSectionDetail({
        node: sectionDetailNode,
        sectionId: context.sectionId
      }, sectionPayloadLooksLikeDetail(context.sectionId, payload)
        ? payload
        : (rows.length ? sectionDetailsBySection[context.sectionId] || null : null));
    }
    if (context.summaryNode) {
      context.summaryNode.innerHTML = "";
      const entries = summaryEntries(payload).slice(0, 8);
      entries.forEach((entry) => {
        const article = document.createElement("article");
        article.className = "console-card";
        article.innerHTML =
          '<h3 class="console-card-title">' + escapeHtml(displayLabel(entry.label)) + '</h3>' +
          '<p class="console-card-copy">' + escapeHtml(entry.value) + '</p>';
        context.summaryNode.appendChild(article);
      });
    }
    if (runtimeBoardNode) {
      renderRuntimeBoard(
        {
          sectionId: context.sectionId,
          node: runtimeBoardNode,
          statusNode: context.statusNode,
          jsonNode: context.jsonNode,
          actionResultContext: actionResultContext
        },
        runtimeSummaryPayload(payload)
      );
    }
    if (actionFormsNode) {
      const refreshActionForms = function () {
        renderActionForms({
          node: actionFormsNode,
          sectionId: context.sectionId,
          catalog: catalog,
          onChange: refreshActionForms
        });
        if (rowQuickActionsContext) {
          renderRowQuickActions(rowQuickActionsContext);
        }
      };
      refreshActionForms();
    }
    if (actionContextNode) {
      const fallbackContext = actionContextFromRow(context.sectionId, selectedRowsBySection[context.sectionId] || null, payloadActionContext);
      selectedRowContextBySection[context.sectionId] = fallbackContext;
      renderActionContext({
        node: actionContextNode,
        sectionId: context.sectionId,
        actionContext: fallbackContext
      });
    }
    if (rowQuickActionsContext) {
      renderRowQuickActions(rowQuickActionsContext);
    }
    if (actionCatalogNode) {
      renderActionCatalog({
        node: actionCatalogNode,
        sectionId: context.sectionId,
        catalog: catalog,
        statusNode: context.statusNode,
        jsonNode: context.jsonNode,
        actionResultContext: actionResultContext
      });
    }
    if (actionResultContext) {
      renderActionResult(actionResultContext, actionResultsBySection[context.sectionId] || null);
    }
    if (recentTasksContext) {
      loadRecentTasks(recentTasksContext).catch(function () {
        renderRecentTasks(recentTasksContext, null, false);
      });
    }
    if (context.tableWrap) {
      context.tableWrap.innerHTML = "";
      if (rows.length) {
        context.tableWrap.appendChild(
          buildTable(rows, {
            sectionId: context.sectionId,
            statusNode: context.statusNode,
            payloadActionContext: payloadActionContext,
            onSelect: function (row) {
              selectedRowsBySection[context.sectionId] = row;
              selectedRowContextBySection[context.sectionId] = actionContextFromRow(context.sectionId, row, payloadActionContext);
              if (actionContextNode) {
                renderActionContext({
                  node: actionContextNode,
                  sectionId: context.sectionId,
                  actionContext: selectedRowContextBySection[context.sectionId]
                });
              }
              if (rowQuickActionsContext) {
                renderRowQuickActions(rowQuickActionsContext);
              }
              if (sectionDetailNode) {
                loadSectionDetail(
                  {
                    node: sectionDetailNode,
                    sectionId: context.sectionId,
                    statusNode: context.statusNode
                  },
                  row,
                  payloadActionContext
                );
              }
            }
          })
        );
      } else if (context.sectionId !== "home") {
        const empty = document.createElement("div");
        empty.className = "console-empty";
        empty.textContent = "当前分区没有返回任何行。";
        context.tableWrap.appendChild(empty);
      }
    }
    if (context.jsonNode) {
      context.jsonNode.textContent = JSON.stringify(payload, null, 2);
    }
  }

  async function loadSectionDetail(context, row, payloadActionContext) {
    const node = context && context.node;
    const sectionId = normalizeSection((context || {}).sectionId);
    if (!node) {
      return;
    }
    if (!sectionSupportsDetail(sectionId)) {
      renderSectionDetail(context, null);
      return;
    }
    const detailUrl = detailUrlForRow(sectionId, row, payloadActionContext);
    if (!detailUrl.search) {
      renderSectionDetail(context, null);
      return;
    }
    const selectedKey = rowSelectionKey(row);
    node.innerHTML = "";
    const loading = document.createElement("div");
    loading.className = "console-empty";
    loading.textContent = "正在加载所选详情...";
    node.appendChild(loading);
    try {
      const response = await fetch(detailUrl);
      const payload = await response.json();
      if (selectedKey !== rowSelectionKey(selectedRowsBySection[sectionId] || null)) {
        return;
      }
      sectionDetailsBySection[sectionId] = payload;
      renderSectionDetail(context, payload);
      if (!response.ok && context.statusNode) {
        context.statusNode.textContent = "所选详情请求失败。";
      }
    } catch (_error) {
      if (selectedKey !== rowSelectionKey(selectedRowsBySection[sectionId] || null)) {
        return;
      }
      renderSectionDetail(context, { error: "加载所选详情失败。" });
    }
  }

  function renderSectionDetail(context, payload) {
    const node = context && context.node;
    if (!node) {
      return;
    }
    node.innerHTML = "";
    const sectionId = normalizeSection((context || {}).sectionId);
    if (!sectionSupportsDetail(sectionId)) {
      const empty = document.createElement("div");
      empty.className = "console-empty";
      empty.textContent = "当前分区未启用详情视图。";
      node.appendChild(empty);
      return;
    }
    if (!payload || typeof payload !== "object") {
      if (hasDetailViewOptions(sectionId)) {
        renderDetailViewSwitcher(node, { sectionId: sectionId });
      }
      const empty = document.createElement("div");
      empty.className = "console-empty";
      empty.textContent = "请选择一行后查看该分区的标准详情。";
      node.appendChild(empty);
      return;
    }
    if (payload.error) {
      if (hasDetailViewOptions(sectionId)) {
        renderDetailViewSwitcher(node, { sectionId: sectionId });
      }
      const empty = document.createElement("div");
      empty.className = "console-empty";
      empty.textContent = String(payload.error);
      node.appendChild(empty);
      return;
    }
    if (sectionId === "training_runs") {
      renderTrainingRunDetail(node, payload);
      return;
    }
    if (sectionId === "backtests") {
      if (activeDetailView(sectionId) === "stake_sweep" || String(payload.dataset || "") === "console_backtest_stake_sweep_detail") {
        renderBacktestStakeSweepDetail(node, payload);
        return;
      }
      renderBacktestDetail(node, payload);
      return;
    }
    if (sectionId === "experiments") {
      if (activeDetailView(sectionId) === "matrix" || String(payload.dataset || "") === "console_experiment_matrix_detail") {
        renderExperimentMatrixDetail(node, payload);
        return;
      }
      renderExperimentDetail(node, payload);
      return;
    }
    if (sectionId === "bundles") {
      renderBundleDetail(node, payload);
      return;
    }
    const empty = document.createElement("div");
    empty.className = "console-empty";
    empty.textContent = "当前分区还没有注册详情渲染器。";
    node.appendChild(empty);
  }

  function renderTrainingRunDetail(node, payload) {
    const summary = payload && typeof payload.summary === "object" ? payload.summary : {};
    const overviewCards = Array.isArray((payload || {}).overview_cards) ? payload.overview_cards : [];
    const runOverview = payload && typeof payload.run_overview === "object" ? payload.run_overview : {};
    const offsets = Array.isArray((payload || {}).offset_details) ? payload.offset_details : [];
    const offsetPreview = payload && typeof payload.offset_preview === "object" ? payload.offset_preview : {};
    const explainabilityOverview = payload && typeof payload.explainability_overview === "object"
      ? payload.explainability_overview
      : {};
    const bundleReadiness = payload && typeof payload.bundle_readiness === "object" ? payload.bundle_readiness : {};
    const metricSummary = payload && typeof payload.metric_summary === "object" ? payload.metric_summary : {};
    const firstOffset = offsets[0] && typeof offsets[0] === "object" ? offsets[0] : {};
    const firstOffsetSummary = firstOffset.summary && typeof firstOffset.summary === "object" ? firstOffset.summary : {};
    const explainability = firstOffsetSummary.explainability && typeof firstOffsetSummary.explainability === "object"
      ? firstOffsetSummary.explainability
      : {};
    const topLogreg = previewNames((explainability.top_logreg_coefficients || []), "feature");
    const topLgb = previewNames((explainability.top_lgb_importance || []), "feature");
    const topPositive = previewNames((explainability.top_positive_factors || []), "feature");
    const topNegative = previewNames((explainability.top_negative_factors || []), "feature");
    renderMetricCardGrid(node, {
      title: "运行摘要卡",
      copy: "先用最少的摘要卡快速判断这次训练运行的范围、样本量和并发配置。",
      cards: overviewCards
    });
    renderOperatorCard(node, {
      title: "训练范围",
      copy: "当前训练运行的身份、特征集和标签范围。",
      items: [
        { label: "run", value: payload.run_label },
        { label: "market", value: payload.market },
        { label: "model_family", value: payload.model_family },
        { label: "target", value: payload.target },
        { label: "feature_set", value: payload.feature_set },
        { label: "label_set", value: payload.label_set },
        { label: "window", value: payload.window }
      ]
    });
    renderOperatorCard(node, {
      title: "Offset 概览",
      copy: "当前训练运行覆盖了哪些 offset，以及使用的并发设置。",
      items: [
        { label: "offset_count", value: payload.offset_count },
        { label: "offsets", value: Array.isArray(payload.offsets) ? payload.offsets.join(", ") : "" },
        { label: "parallel_workers", value: summary.parallel_workers },
        { label: "first_offset", value: firstOffset.offset },
        { label: "rows", value: firstOffsetSummary.rows || runOverview.rows_total },
        { label: "positive_rate", value: firstOffsetSummary.positive_rate || runOverview.positive_rate_avg }
      ]
    });
    renderMetricCardGrid(node, {
      title: "解释性覆盖",
      copy: "训练页只保留必要的 explainability 覆盖信息，不扩成重监控。",
      cards: [
        { label: "offset_count", value: payload.offset_count },
        { label: "offsets_with_explainability", value: explainabilityOverview.offsets_with_explainability },
        { label: "offsets_with_logreg_coefficients", value: explainabilityOverview.offsets_with_logreg_coefficients },
        { label: "offsets_with_lgb_importance", value: explainabilityOverview.offsets_with_lgb_importance },
        { label: "offsets_with_factor_direction_summary", value: explainabilityOverview.offsets_with_factor_direction_summary },
        { label: "offsets_with_factor_correlations", value: explainabilityOverview.offsets_with_factor_correlations }
      ]
    });
    renderMetricCardGrid(node, {
      title: "打包就绪度",
      copy: "直接回答这个训练运行能不能顺畅进入 bundle / backtest 链路。",
      cards: [
        { label: "is_ready", value: bundleReadiness.is_ready },
        { label: "ready_offset_count", value: bundleReadiness.ready_offset_count },
        { label: "missing_offset_count", value: bundleReadiness.missing_offset_count },
        { label: "offsets_with_metrics", value: metricSummary.offsets_with_metrics },
        { label: "mean_auc", value: metricSummary.mean_auc },
        { label: "mean_brier", value: metricSummary.mean_brier },
        { label: "best_auc_offset", value: metricSummary.best_auc_offset && metricSummary.best_auc_offset.offset },
        { label: "best_brier_offset", value: metricSummary.best_brier_offset && metricSummary.best_brier_offset.offset }
      ]
    });
    renderOperatorCard(node, {
      title: "因子解释",
      copy: "从首个 offset 预览里抽取的重点因子信号，完整产物仍可在 JSON 与路径区查看。",
      items: [
        { label: "top_logreg", value: topLogreg },
        { label: "top_lgb", value: topLgb },
        { label: "top_positive", value: topPositive },
        { label: "top_negative", value: topNegative },
        { label: "factor_correlations", value: firstOffset.factor_correlations && firstOffset.factor_correlations.path },
        { label: "direction_summary", value: firstOffset.factor_direction_summary_path }
      ]
    });
    renderPreviewTableCard(node, {
      title: "Offset 预览表",
      copy: "保留必要的 offset 级摘要，帮助快速判断这次训练运行的覆盖、质量和打包准备情况。",
      rows: Array.isArray(offsetPreview.rows) ? offsetPreview.rows : [],
      columns: ["offset", "rows", "positive_rate", "auc", "brier", "feature_count", "folds_used", "bundle_ready"],
      limit: 5
    });
  }

  function renderBacktestDetail(node, payload) {
    renderDetailViewSwitcher(node, { sectionId: "backtests" });
    const summary = payload && typeof payload.summary === "object" ? payload.summary : {};
    const overviewCards = Array.isArray((payload || {}).overview_cards) ? payload.overview_cards : [];
    const comparison = payload && typeof payload.comparison_axes === "object" ? payload.comparison_axes : {};
    const equity = payload && typeof payload.equity_curve_preview === "object" ? payload.equity_curve_preview : {};
    const stakeSweep = payload && typeof payload.stake_sweep_preview === "object" ? payload.stake_sweep_preview : {};
    const offsetSummary = payload && typeof payload.offset_summary_preview === "object" ? payload.offset_summary_preview : {};
    const factorPnl = payload && typeof payload.factor_pnl_preview === "object" ? payload.factor_pnl_preview : {};
    const sweepHighlights = payload && typeof payload.sweep_highlights === "object" ? payload.sweep_highlights : {};
    const bestStake = stakeSweep.best_by_roi && typeof stakeSweep.best_by_roi === "object" ? stakeSweep.best_by_roi : {};
    const bestOffset = offsetSummary.best_by_pnl && typeof offsetSummary.best_by_pnl === "object" ? offsetSummary.best_by_pnl : {};
    const topPositive = Array.isArray(factorPnl.top_positive) && factorPnl.top_positive[0] ? factorPnl.top_positive[0] : {};
    const topNegative = Array.isArray(factorPnl.top_negative) && factorPnl.top_negative[0] ? factorPnl.top_negative[0] : {};
    const latestPoint = equity.latest_point && typeof equity.latest_point === "object" ? equity.latest_point : {};
    const pnlRange = equity && typeof equity.pnl_range === "object" ? equity.pnl_range : {};
    const featureSet = summary.feature_set || payload.feature_set || comparison.feature_set || "";
    renderDetailFilterBar(node, {
      sectionId: "backtests",
      payload: payload,
      title: "结果筛选",
      copy: "支持限制预览数量和关键词筛选。修改后回车或失焦生效。",
      fields: [
        { fieldId: "topN", label: "仅看前 N", inputType: "number", min: 1, max: 12, placeholder: "5" },
        { fieldId: "search", label: "关键词", inputType: "text", placeholder: "金额 / offset / 因子" }
      ]
    });
    const stakePreviewRows = applyDetailRowFilter(stakeSweep.preview_rows, "backtests", {
      keys: ["stake_usd", "max_notional_usd", "pnl_sum", "roi_pct"]
    });
    const stakeTopRows = applyDetailRowFilter(stakeSweep.top_by_roi, "backtests", {
      keys: ["stake_usd", "max_notional_usd", "pnl_sum", "roi_pct"]
    });
    const offsetPreviewRows = applyDetailRowFilter(offsetSummary.preview_rows, "backtests", {
      keys: ["offset", "pnl_sum", "avg_roi_pct", "roi_pct"]
    });
    const offsetTopRows = applyDetailRowFilter(offsetSummary.top_by_pnl, "backtests", {
      keys: ["offset", "pnl_sum", "avg_roi_pct", "roi_pct"]
    });
    const factorPreviewRows = applyDetailRowFilter(factorPnl.preview_rows, "backtests", {
      keys: ["feature", "pnl_sum", "avg_roi_pct", "abs_pnl_correlation"]
    });
    const factorBarRows = applyDetailRowFilter(
      []
        .concat(Array.isArray(factorPnl.top_positive) ? factorPnl.top_positive : [])
        .concat(Array.isArray(factorPnl.top_negative) ? factorPnl.top_negative : []),
      "backtests",
      { keys: ["feature", "pnl_sum", "avg_roi_pct", "pnl_correlation"] }
    );
    const equityRows = applyDetailRowFilter(equity.preview_rows, "backtests", {
      keys: ["decision_ts", "trade_number", "cumulative_pnl"]
    });
    renderMetricCardGrid(node, {
      title: "结果摘要卡",
      copy: "先看一排核心结果卡，快速判断这次回测是否值得继续深挖。",
      cards: overviewCards
    });
    renderMetricCardGrid(node, {
      title: "扫参领先者",
      copy: "这里直接汇总金额、offset 和因子的领先者，优先回答“哪个更好”。",
      cards: [
        { label: "best_stake_by_roi", value: sweepHighlights.best_stake_by_roi || bestStake.stake_usd },
        { label: "best_stake_roi_pct", value: sweepHighlights.best_stake_roi_pct || bestStake.roi_pct },
        { label: "best_stake_by_pnl", value: sweepHighlights.best_stake_by_pnl },
        { label: "best_stake_pnl_sum", value: sweepHighlights.best_stake_pnl_sum },
        { label: "best_offset_by_pnl", value: sweepHighlights.best_offset_by_pnl || bestOffset.offset },
        { label: "best_offset_pnl_sum", value: sweepHighlights.best_offset_pnl_sum || bestOffset.pnl_sum },
        { label: "best_offset_by_roi", value: sweepHighlights.best_offset_by_roi },
        { label: "best_offset_roi_pct", value: sweepHighlights.best_offset_roi_pct },
        { label: "top_positive_factor", value: sweepHighlights.top_positive_factor || topPositive.feature },
        { label: "top_negative_factor", value: sweepHighlights.top_negative_factor || topNegative.feature },
        { label: "top_positive_correlation_factor", value: sweepHighlights.top_positive_correlation_factor },
        { label: "top_negative_correlation_factor", value: sweepHighlights.top_negative_correlation_factor }
      ]
    });
    renderOperatorCard(node, {
      title: "核心结果",
      copy: "当前回测最重要的结果指标，先看收益、胜率和交易量。",
      items: [
        { label: "pnl_sum", value: summary.pnl_sum },
        { label: "roi_pct", value: summary.roi_pct },
        { label: "trades", value: summary.trades },
        { label: "win_rate_pct", value: payload.win_rate_pct || summary.win_rate_pct },
        { label: "wins", value: summary.wins },
        { label: "losses", value: summary.losses },
        { label: "rejects", value: summary.rejects },
        { label: "stake_usd", value: summary.stake_usd },
        { label: "max_notional_usd", value: summary.max_notional_usd }
      ]
    });
    renderOperatorCard(node, {
      title: "回测配置",
      copy: "用于区分这次回测和周边结果的规格、模型包、回退和一致性参数。",
      items: [
        { label: "profile", value: payload.profile },
        { label: "spec", value: payload.spec_name },
        { label: "feature_set", value: featureSet },
        { label: "bundle", value: comparison.bundle_label || summary.bundle_label },
        { label: "variant", value: comparison.variant_label || summary.variant_label },
        { label: "secondary_bundle", value: comparison.secondary_bundle_label || summary.secondary_bundle_label },
        { label: "fallback_reasons", value: Array.isArray(comparison.fallback_reasons) ? comparison.fallback_reasons.join(", ") : "" },
        { label: "parity", value: Array.isArray(comparison.parity_keys) ? comparison.parity_keys.join(", ") : "" }
      ]
    });
    renderOperatorCard(node, {
      title: "扫参与因子结论",
      copy: "快速判断哪个金额、哪个 offset、哪些因子表现最好，不用先翻原始产物。",
      items: [
        { label: "best_stake_by_roi", value: sweepHighlights.best_stake_by_roi || bestStake.stake_usd },
        { label: "best_stake_roi_pct", value: sweepHighlights.best_stake_roi_pct || bestStake.roi_pct },
        { label: "best_offset_by_pnl", value: sweepHighlights.best_offset_by_pnl || bestOffset.offset },
        { label: "best_offset_pnl_sum", value: sweepHighlights.best_offset_pnl_sum || bestOffset.pnl_sum },
        { label: "top_positive_factor", value: sweepHighlights.top_positive_factor || topPositive.feature },
        { label: "top_negative_factor", value: sweepHighlights.top_negative_factor || topNegative.feature },
        { label: "top_positive_correlation_factor", value: sweepHighlights.top_positive_correlation_factor },
        { label: "top_negative_correlation_factor", value: sweepHighlights.top_negative_correlation_factor }
      ]
    });
    renderOperatorCard(node, {
      title: "资金轨迹",
      copy: "直接看资金曲线的关键结论，快速判断这次回测是稳步走高还是中途明显回撤。",
      items: [
        { label: "equity_point_count", value: equity.row_count },
        { label: "latest_cumulative_pnl", value: sweepHighlights.latest_cumulative_pnl || latestPoint.cumulative_pnl },
        { label: "pnl_range_min", value: pnlRange.min },
        { label: "pnl_range_max", value: pnlRange.max },
        { label: "cumulative_roi_pct", value: latestPoint.cumulative_roi_pct },
        { label: "cumulative_trades", value: latestPoint.cumulative_trades },
        { label: "report_path", value: payload.report_path }
      ]
    });
    renderOperatorCard(node, {
      title: "产物覆盖",
      copy: "确认这次回测已经产出哪些预览对象，便于继续做金额、offset 和因子比较。",
      items: [
        { label: "stake_sweep_rows", value: stakeSweep.row_count },
        { label: "offset_summary_rows", value: offsetSummary.row_count },
        { label: "factor_pnl_rows", value: factorPnl.row_count },
        { label: "equity_rows", value: equity.row_count }
      ]
    });
    renderLineChartCard(node, {
      title: "资金曲线图",
      copy: "先从图上看累计 PnL 的斜率和波动，再决定要不要下钻每笔交易。",
      rows: equityRows,
      xKey: "trade_number",
      yKey: "cumulative_pnl"
    });
    renderBarChartCard(node, {
      title: "金额 ROI 图",
      copy: "把不同金额档的 ROI 直接画出来，优先判断下注金额往哪边收敛。",
      rows: stakeTopRows,
      xKey: "stake_usd",
      yKey: "roi_pct",
      limit: 5
    });
    renderBarChartCard(node, {
      title: "Offset PnL 图",
      copy: "把 offset 的收益直接画成柱图，快速找出最强和最弱窗口。",
      rows: offsetTopRows,
      xKey: "offset",
      yKey: "pnl_sum",
      limit: 5
    });
    renderBarChartCard(node, {
      title: "因子 PnL 图",
      copy: "把正负向因子放进同一张图，先看方向分布，再去表里确认数值。",
      rows: factorBarRows,
      xKey: "feature",
      yKey: "pnl_sum",
      limit: 6
    });
    renderPreviewTableCard(node, {
      title: "金额扫描预览",
      copy: "直接查看不同 stake 档位下的收益表现，便于快速判断金额设置。",
      rows: stakePreviewRows,
      columns: ["stake_usd", "max_notional_usd", "trades", "pnl_sum", "roi_pct"],
      limit: 6
    });
    renderPreviewTableCard(node, {
      title: "金额领先榜",
      copy: "优先看 ROI 最强的金额组合，用来快速收敛下注金额。",
      rows: stakeTopRows,
      columns: ["stake_usd", "max_notional_usd", "trades", "pnl_sum", "roi_pct"],
      limit: 5
    });
    renderPreviewTableCard(node, {
      title: "Offset 收益预览",
      copy: "按 offset 对比收益和平均 ROI，帮助你判断哪个 offset 更稳定。",
      rows: offsetPreviewRows,
      columns: ["offset", "trades", "pnl_sum", "avg_roi_pct", "roi_pct"],
      limit: 6
    });
    renderPreviewTableCard(node, {
      title: "Offset 领先榜",
      copy: "把 ROI 和 PnL 更强的 offset 提到最前面，帮助你快速锁定更好的窗口。",
      rows: applyDetailRowFilter(offsetSummary.top_by_roi, "backtests", {
        keys: ["offset", "pnl_sum", "avg_roi_pct", "roi_pct"]
      }),
      columns: ["offset", "trades", "pnl_sum", "avg_roi_pct", "roi_pct"],
      limit: 5
    });
    renderPreviewTableCard(node, {
      title: "因子表现预览",
      copy: "直接看因子层面的 PnL 和相关性，便于挑出更好的权重和方向。",
      rows: factorPreviewRows,
      columns: ["feature", "pnl_sum", "avg_roi_pct", "abs_pnl_correlation"],
      limit: 6
    });
    renderPreviewTableCard(node, {
      title: "正负向因子榜",
      copy: "把正向和负向因子分别提出来，方便直接看哪些权重更值得保留或削弱。",
      rows: applyDetailRowFilter(
        []
          .concat(Array.isArray(factorPnl.top_positive) ? factorPnl.top_positive : [])
          .concat(Array.isArray(factorPnl.top_negative) ? factorPnl.top_negative : [])
          .concat(Array.isArray(factorPnl.top_positive_correlation) ? factorPnl.top_positive_correlation.slice(0, 1) : [])
          .concat(Array.isArray(factorPnl.top_negative_correlation) ? factorPnl.top_negative_correlation.slice(0, 1) : []),
        "backtests",
        { keys: ["feature", "pnl_sum", "avg_roi_pct", "pnl_correlation"] }
      ),
      columns: ["feature", "pnl_sum", "avg_roi_pct", "pnl_correlation", "abs_pnl_correlation"],
      limit: 8
    });
    renderPreviewTableCard(node, {
      title: "资金曲线预览",
      copy: "保留最近几笔资金曲线点位，方便快速判断收益是否平滑。",
      rows: equityRows,
      columns: ["decision_ts", "trade_number", "cumulative_pnl", "pnl_sum"],
      limit: 6
    });
  }

  function renderExperimentDetail(node, payload) {
    renderDetailViewSwitcher(node, { sectionId: "experiments" });
    const summary = payload && typeof payload.summary === "object" ? payload.summary : {};
    const comparison = payload && typeof payload.comparison_overview === "object" ? payload.comparison_overview : {};
    const compareSurfaceSummary = payload && typeof payload.compare_surface_summary === "object"
      ? payload.compare_surface_summary
      : {};
    const bestCase = payload && typeof payload.best_case === "object" ? payload.best_case : {};
    const bestMatrix = payload && typeof payload.best_matrix === "object" ? payload.best_matrix : {};
    const bestVariant = payload && typeof payload.best_variant === "object" ? payload.best_variant : {};
    const compareFacets = payload && typeof payload.compare_facets === "object" ? payload.compare_facets : {};
    const leaderboard = payload && typeof payload.leaderboard_preview === "object" ? payload.leaderboard_preview : {};
    const comparePreview = payload && typeof payload.compare_preview === "object" ? payload.compare_preview : {};
    const matrixPreview = payload && typeof payload.matrix_summary_preview === "object" ? payload.matrix_summary_preview : {};
    const variantPreview = payload && typeof payload.variant_compare_preview === "object" ? payload.variant_compare_preview : {};
    const failedCases = payload && typeof payload.failed_cases_preview === "object" ? payload.failed_cases_preview : {};
    const bestByMarket = payload && typeof payload.best_by_market_preview === "object" ? payload.best_by_market_preview : {};
    const bestByGroup = payload && typeof payload.best_by_group_preview === "object" ? payload.best_by_group_preview : {};
    const bestByMarketGroup = payload && typeof payload.best_by_market_group_preview === "object"
      ? payload.best_by_market_group_preview
      : {};
    const bestByRun = payload && typeof payload.best_by_run_preview === "object" ? payload.best_by_run_preview : {};
    const leaderboardSurfaceSummary = payload && typeof payload.leaderboard_surface_summary === "object"
      ? payload.leaderboard_surface_summary
      : {};
    const bestComboSummary = payload && typeof payload.best_combo_summary === "object" ? payload.best_combo_summary : {};
    const variantSurfaceSummary = payload && typeof payload.variant_surface_summary === "object"
      ? payload.variant_surface_summary
      : {};
    const failureOverview = payload && typeof payload.failure_overview === "object" ? payload.failure_overview : {};
    renderDetailFilterBar(node, {
      sectionId: "experiments",
      payload: payload,
      title: "比较筛选",
      copy: "支持限制预览数量，并按市场、运行名、变体过滤。修改后回车或失焦生效。",
      fields: [
        { fieldId: "topN", label: "仅看前 N", inputType: "number", min: 1, max: 12, placeholder: "5" },
        { fieldId: "market", label: "按市场过滤", inputType: "text", placeholder: "sol" },
        { fieldId: "runName", label: "按运行名过滤", inputType: "text", placeholder: "stake_matrix" },
        { fieldId: "variant", label: "按变体过滤", inputType: "text", placeholder: "aggressive" }
      ]
    });
    const leaderboardRows = applyDetailRowFilter(leaderboard.preview_rows, "experiments");
    const bestByMarketRows = applyDetailRowFilter(bestByMarket.rows, "experiments");
    const bestByGroupRows = applyDetailRowFilter(bestByGroup.rows, "experiments");
    const bestByMarketGroupRows = applyDetailRowFilter(bestByMarketGroup.rows, "experiments");
    const bestByRunRows = applyDetailRowFilter(bestByRun.rows, "experiments");
    const compareRows = applyDetailRowFilter(comparePreview.preview_rows, "experiments");
    const matrixRows = applyDetailRowFilter(matrixPreview.preview_rows, "experiments");
    const variantRows = applyDetailRowFilter(variantPreview.preview_rows, "experiments");
    const failedRows = applyDetailRowFilter(failedCases.preview_rows, "experiments");
    const featureSets = uniquePreviewValues(
      []
        .concat(Array.isArray(leaderboard.preview_rows) ? leaderboard.preview_rows : [])
        .concat(Array.isArray(comparePreview.preview_rows) ? comparePreview.preview_rows : [])
        .concat(Array.isArray(variantPreview.preview_rows) ? variantPreview.preview_rows : []),
      "feature_set",
      6
    );
    renderMetricCardGrid(node, {
      title: "比较摘要卡",
      copy: "先用摘要卡回答这次实验覆盖了多大的比较面，以及当前最强结果是谁。",
      cards: [
        { label: "cases", value: compareSurfaceSummary.cases || summary.cases },
        { label: "completed_cases", value: compareSurfaceSummary.completed_cases || summary.completed_cases },
        { label: "failed_cases", value: compareSurfaceSummary.failed_cases || summary.failed_cases },
        { label: "market_count", value: compareSurfaceSummary.market_count },
        { label: "group_count", value: compareSurfaceSummary.group_count },
        { label: "run_name_count", value: compareSurfaceSummary.run_name_count },
        { label: "variant_count", value: compareSurfaceSummary.variant_count },
        { label: "feature_set_count", value: featureSets.length },
        { label: "stake_point_count", value: compareSurfaceSummary.stake_point_count },
        { label: "notional_point_count", value: compareSurfaceSummary.notional_point_count },
        { label: "bundle_count", value: compareSurfaceSummary.bundle_count },
        { label: "leaderboard_row_count", value: compareSurfaceSummary.leaderboard_row_count },
        { label: "compare_row_count", value: compareSurfaceSummary.compare_row_count },
        { label: "top_roi_pct", value: comparison.top_roi_pct || summary.top_roi_pct },
        { label: "best_variant_label", value: comparison.best_variant_label || bestVariant.variant_label },
        { label: "best_matrix_run", value: comparison.best_matrix_parent_run_name || bestMatrix.matrix_parent_run_name }
      ]
    });
    renderMetricCardGrid(node, {
      title: "领先者摘要卡",
      copy: "把市场、分组、运行三个层级的领先者先抽出来，方便快速收敛。",
      cards: [
        { label: "market_leader_count", value: leaderboardSurfaceSummary.market_leader_count },
        { label: "group_leader_count", value: leaderboardSurfaceSummary.group_leader_count },
        { label: "run_leader_count", value: leaderboardSurfaceSummary.run_leader_count },
        { label: "best_market", value: leaderboardSurfaceSummary.best_market },
        { label: "best_market_run_name", value: leaderboardSurfaceSummary.best_market_run_name },
        { label: "best_market_variant_label", value: leaderboardSurfaceSummary.best_market_variant_label },
        { label: "best_group_name", value: leaderboardSurfaceSummary.best_group_name },
        { label: "best_group_run_name", value: leaderboardSurfaceSummary.best_group_run_name },
        { label: "best_run_name", value: leaderboardSurfaceSummary.best_run_name },
        { label: "best_run_variant_label", value: leaderboardSurfaceSummary.best_run_variant_label }
      ]
    });
    renderOperatorCard(node, {
      title: "实验总览",
      copy: "当前实验运行覆盖了多少 case，复用了多少训练/模型包，以及失败数量。",
      items: [
        { label: "suite", value: payload.suite_name },
        { label: "run", value: payload.run_label },
        { label: "cases", value: summary.cases },
        { label: "completed_cases", value: summary.completed_cases },
        { label: "failed_cases", value: summary.failed_cases },
        { label: "resumed_cases", value: summary.resumed_cases },
        { label: "training_reused", value: summary.training_reused_cases },
        { label: "bundle_reused", value: summary.bundle_reused_cases },
        { label: "secondary_training_reused_cases", value: summary.secondary_training_reused_cases },
        { label: "secondary_bundle_reused_cases", value: summary.secondary_bundle_reused_cases }
      ]
    });
    renderOperatorCard(node, {
      title: "当前最佳结果",
      copy: "从排行榜预览里直接给出当前最优结果。",
      items: [
        { label: "run_name", value: bestCase.run_name || comparison.best_case_run_name },
        { label: "feature_set", value: bestCase.feature_set },
        { label: "variant_label", value: bestCase.variant_label || comparison.best_case_variant_label },
        { label: "roi_pct", value: bestCase.roi_pct || comparison.best_case_roi_pct },
        { label: "bundle_dir", value: bestCase.bundle_dir || comparison.best_case_bundle_dir },
        { label: "top_roi_pct", value: summary.top_roi_pct }
      ]
    });
    renderOperatorCard(node, {
      title: "矩阵 / 变体领先者",
      copy: "快速回答现在是哪个矩阵、哪个变体更强。",
      items: [
        { label: "best_matrix_run", value: bestMatrix.matrix_parent_run_name || comparison.best_matrix_parent_run_name },
        { label: "best_matrix_stake", value: bestMatrix.best_matrix_stake_label || comparison.best_matrix_stake_label },
        { label: "best_matrix_roi_pct", value: bestMatrix.best_roi_pct || comparison.best_matrix_roi_pct },
        { label: "best_variant_feature_set", value: bestVariant.feature_set },
        { label: "best_variant_label", value: bestVariant.variant_label || comparison.best_variant_label },
        { label: "best_variant_run", value: bestVariant.run_name || comparison.best_variant_run_name },
        { label: "variant_vs_reference", value: bestVariant.comparison_vs_reference || comparison.best_variant_comparison }
      ]
    });
    renderOperatorCard(node, {
      title: "最佳组合摘要",
      copy: "把最佳案例、最佳矩阵和最佳变体的核心身份信息压成一张摘要卡。",
      items: [
        { label: "best_case_market", value: bestComboSummary.best_case_market },
        { label: "best_case_group_name", value: bestComboSummary.best_case_group_name },
        { label: "best_case_run_name", value: bestComboSummary.best_case_run_name },
        { label: "best_case_feature_set", value: bestCase.feature_set },
        { label: "best_case_variant_label", value: bestComboSummary.best_case_variant_label },
        { label: "best_case_pnl_sum", value: bestComboSummary.best_case_pnl_sum },
        { label: "best_matrix_market", value: bestComboSummary.best_matrix_market },
        { label: "best_matrix_run_name", value: bestComboSummary.best_matrix_run_name },
        { label: "best_matrix_stake_label", value: bestComboSummary.best_matrix_stake_label },
        { label: "best_variant_market", value: bestComboSummary.best_variant_market },
        { label: "best_variant_run_name", value: bestComboSummary.best_variant_run_name },
        { label: "best_variant_roi_delta_vs_reference", value: bestComboSummary.best_variant_roi_delta_vs_reference },
        { label: "best_variant_comparison", value: bestComboSummary.best_variant_comparison }
      ]
    });
    renderOperatorCard(node, {
      title: "比较维度",
      copy: "当前比较表已经覆盖到哪些市场、分组、运行和变体。",
      items: [
        { label: "markets", value: joinPreviewValues(compareFacets.markets) },
        { label: "groups", value: joinPreviewValues(compareFacets.groups) },
        { label: "run_names", value: joinPreviewValues(compareFacets.run_names) },
        { label: "feature_sets", value: joinPreviewValues(featureSets) },
        { label: "variant_labels", value: joinPreviewValues(compareFacets.variant_labels) },
        { label: "failed_case_rows", value: failedCases.row_count }
      ]
    });
    renderOperatorCard(node, {
      title: "变体面对比摘要",
      copy: "优先看变体对比面的覆盖规模、状态分布和相对基线结论。",
      items: [
        { label: "row_count", value: variantSurfaceSummary.row_count },
        { label: "run_count", value: variantSurfaceSummary.run_count },
        { label: "variant_count", value: variantSurfaceSummary.variant_count },
        { label: "reference_variant_count", value: variantSurfaceSummary.reference_variant_count },
        { label: "best_variant_label", value: variantSurfaceSummary.best_variant_label },
        { label: "best_variant_run_name", value: variantSurfaceSummary.best_variant_run_name },
        { label: "best_variant_roi_delta_vs_reference", value: variantSurfaceSummary.best_variant_roi_delta_vs_reference },
        { label: "best_variant_pnl_delta_vs_reference", value: variantSurfaceSummary.best_variant_pnl_delta_vs_reference },
        { label: "comparison_vs_reference", value: joinCountMap(variantSurfaceSummary.comparison_counts) }
      ]
    });
    renderOperatorCard(node, {
      title: "失败概览",
      copy: "先看失败是否集中在某个阶段、某类错误或某个市场，再决定是否重跑。",
      items: [
        { label: "failed_cases", value: failureOverview.failed_cases },
        { label: "row_count", value: failureOverview.row_count },
        { label: "failure_stage_counts", value: joinCountMap(failureOverview.failure_stage_counts) },
        { label: "error_type_counts", value: joinCountMap(failureOverview.error_type_counts) },
        { label: "market_counts", value: joinCountMap(failureOverview.market_counts) },
        { label: "group_counts", value: joinCountMap(failureOverview.group_counts) },
        { label: "first_failed_case", value: failureOverview.first_failed_case && JSON.stringify(failureOverview.first_failed_case) }
      ]
    });
    renderBarChartCard(node, {
      title: "排行榜 ROI 图",
      copy: "先从图上看出哪个运行 / 变体组合收益最高，再去表里确认细节。",
      rows: leaderboardRows.map((row) => ({
            label: [row.run_name, row.variant_label].filter(Boolean).join(" / "),
            roi_pct: row.roi_pct,
          })),
      xKey: "label",
      yKey: "roi_pct",
      limit: 5
    });
    renderBarChartCard(node, {
      title: "矩阵最佳 ROI 图",
      copy: "按矩阵最佳 ROI 画一张图，快速判断哪组矩阵更值得继续追。",
      rows: matrixRows.map((row) => ({
            label: [row.market, row.group_name].filter(Boolean).join(" / "),
            best_roi_pct: row.best_roi_pct,
          })),
      xKey: "label",
      yKey: "best_roi_pct",
      limit: 5
    });
    renderBarChartCard(node, {
      title: "变体增量图",
      copy: "直接看不同变体相对基线的 ROI 增量，快速找出更好的权重配置。",
      rows: variantRows.map((row) => ({
            label: [row.run_name, row.variant_label].filter(Boolean).join(" / "),
            roi_delta: row.roi_pct_delta_vs_reference,
          })),
      xKey: "label",
      yKey: "roi_delta",
      limit: 5
    });
    renderPreviewTableCard(node, {
      title: "排行榜预览",
      copy: "先看当前 leaderboard 前几名，快速判断哪个运行和变体领先。",
      rows: leaderboardRows,
      columns: ["rank", "market", "group_name", "run_name", "feature_set", "variant_label", "pnl_sum", "roi_pct"],
      limit: 5
    });
    renderPreviewTableCard(node, {
      title: "按市场领先者",
      copy: "每个市场只保留当前最强结果，方便先看市场维度谁更好。",
      rows: bestByMarketRows,
      columns: ["market", "group_name", "run_name", "feature_set", "variant_label", "pnl_sum", "roi_pct"],
      limit: 6
    });
    renderPreviewTableCard(node, {
      title: "按分组领先者",
      copy: "每个分组保留一条领先结果，用来快速判断哪类实验更强。",
      rows: bestByGroupRows,
      columns: ["group_name", "market", "run_name", "feature_set", "variant_label", "pnl_sum", "roi_pct"],
      limit: 6
    });
    renderPreviewTableCard(node, {
      title: "按市场 / 分组领先者",
      copy: "进一步细到市场和分组联合维度，看看每块区域最强的组合。",
      rows: bestByMarketGroupRows,
      columns: ["market", "group_name", "run_name", "feature_set", "variant_label", "pnl_sum", "roi_pct"],
      limit: 6
    });
    renderPreviewTableCard(node, {
      title: "按运行领先者",
      copy: "每个运行只保留当前最强变体，用来快速判断哪个运行值得继续追。",
      rows: bestByRunRows,
      columns: ["market", "group_name", "run_name", "feature_set", "variant_label", "pnl_sum", "roi_pct"],
      limit: 6
    });
    renderPreviewTableCard(node, {
      title: "Case 对比预览",
      copy: "直接查看 case 维度的金额、状态和收益，便于挑出更好的组合。",
      rows: compareRows,
      columns: ["case_key", "run_name", "feature_set", "variant_label", "stake_usd", "max_notional_usd", "status", "pnl_sum", "roi_pct"],
      limit: 5
    });
    renderPreviewTableCard(node, {
      title: "矩阵汇总预览",
      copy: "按矩阵视角查看最佳运行、最佳金额档和总收益。",
      rows: matrixRows,
      columns: ["market", "group_name", "best_run_name", "best_matrix_stake_label", "best_variant_label", "best_roi_pct", "total_pnl_sum"],
      limit: 5
    });
    renderPreviewTableCard(node, {
      title: "变体对比预览",
      copy: "重点看变体相对基线的收益差异，快速找出更好的权重配置。",
      rows: variantRows,
      columns: ["run_name", "feature_set", "variant_label", "reference_variant_label", "roi_pct_delta_vs_reference", "pnl_sum_delta_vs_reference", "comparison_vs_reference"],
      limit: 5
    });
    renderPreviewTableCard(node, {
      title: "失败案例预览",
      copy: "失败 case 会保留一小段预览，便于判断问题是否集中在某类运行或变体。",
      rows: failedRows,
      columns: ["case_key", "group_name", "run_name", "feature_set", "variant_label", "status", "error_type"],
      limit: 5
    });
  }

  function renderBacktestStakeSweepDetail(node, payload) {
    renderDetailViewSwitcher(node, { sectionId: "backtests" });
    const summary = payload && typeof payload.summary === "object" ? payload.summary : {};
    const stakeSweep = payload && typeof payload.stake_sweep_preview === "object" ? payload.stake_sweep_preview : {};
    const surfaceSummary = payload && typeof payload.surface_summary === "object"
      ? payload.surface_summary
      : (payload && typeof payload.stake_surface_summary === "object" ? payload.stake_surface_summary : {});
    const highlights = payload && typeof payload.highlights === "object"
      ? payload.highlights
      : (payload && typeof payload.sweep_highlights === "object" ? payload.sweep_highlights : {});
    const rowsByTheme = payload && typeof payload.rows_by_theme === "object" ? payload.rows_by_theme : {};
    const bestByRoi = stakeSweep.best_by_roi && typeof stakeSweep.best_by_roi === "object" ? stakeSweep.best_by_roi : {};
    const bestByPnl = stakeSweep.best_by_pnl && typeof stakeSweep.best_by_pnl === "object" ? stakeSweep.best_by_pnl : {};
    const previewRows = applyDetailRowFilter(
      rowsByTheme.sorted_by_stake || stakeSweep.preview_rows,
      "backtests",
      { keys: ["stake_usd", "max_notional_usd", "trades", "pnl_sum", "roi_pct", "avg_roi_pct"] }
    );
    const topByRoiRows = applyDetailRowFilter(
      rowsByTheme.top_by_roi || stakeSweep.top_by_roi,
      "backtests",
      { keys: ["stake_usd", "pnl_sum", "roi_pct"] }
    );
    const topByPnlRows = applyDetailRowFilter(
      rowsByTheme.top_by_pnl || stakeSweep.top_by_pnl,
      "backtests",
      { keys: ["stake_usd", "pnl_sum", "roi_pct"] }
    );
    const orderedChartRows = previewRows.map((row) => ({
      stake_label: row.stake_label || row.matrix_stake_label || row.stake_usd,
      stake_usd: row.stake_usd,
      pnl_sum: row.pnl_sum,
      roi_pct: row.roi_pct
    }));
    renderDetailFilterBar(node, {
      sectionId: "backtests",
      payload: payload,
      title: "金额扫参筛选",
      copy: "专门为 stake sweep 结果页提供筛选，只保留金额相关比较。",
      fields: [
        { fieldId: "topN", label: "仅看前 N", inputType: "number", min: 1, max: 16, placeholder: "6" },
        { fieldId: "search", label: "关键词", inputType: "text", placeholder: "stake / ROI / PnL" }
      ]
    });
    renderMetricCardGrid(node, {
      title: "金额扫参摘要卡",
      copy: "把金额扫描面的覆盖和边界先压成一排摘要卡，直接判断 sweep 是否足够完整。",
      cards: [
        { label: "row_count", value: surfaceSummary.row_count || stakeSweep.row_count },
        { label: "stake_min", value: surfaceSummary.stake_min },
        { label: "stake_max", value: surfaceSummary.stake_max },
        { label: "roi_min", value: surfaceSummary.roi_min },
        { label: "roi_max", value: surfaceSummary.roi_max },
        { label: "pnl_min", value: surfaceSummary.pnl_min },
        { label: "pnl_max", value: surfaceSummary.pnl_max },
        { label: "feature_set", value: summary.feature_set || payload.feature_set }
      ]
    });
    renderOperatorCard(node, {
      title: "最佳金额组合",
      copy: "专门回答现在应该优先看哪个金额档位。",
      items: [
        { label: "run", value: payload.run_label },
        { label: "profile", value: payload.profile || summary.profile },
        { label: "spec", value: payload.spec_name || summary.spec_name },
        { label: "best_stake_by_roi", value: highlights.best_stake_by_roi || bestByRoi.stake_usd },
        { label: "best_stake_roi_pct", value: highlights.best_stake_roi_pct || bestByRoi.roi_pct },
        { label: "best_stake_by_pnl", value: highlights.best_stake_by_pnl || bestByPnl.stake_usd },
        { label: "best_stake_pnl_sum", value: highlights.best_stake_pnl_sum || bestByPnl.pnl_sum },
        { label: "best_stake_label", value: bestByRoi.stake_label || bestByRoi.matrix_stake_label }
      ]
    });
    renderOperatorCard(node, {
      title: "扫参覆盖",
      copy: "确认这张页只聚焦金额维度，不把 offset / 因子信息塞回来。",
      items: [
        { label: "stake_level_count", value: previewRows.length || stakeSweep.row_count },
        { label: "notional_level_count", value: uniquePreviewValues(previewRows, "max_notional_usd", 12).length },
        { label: "bundle", value: summary.bundle_label || payload.bundle_label },
        { label: "secondary_bundle", value: summary.secondary_bundle_label || payload.secondary_bundle_label }
      ]
    });
    renderLineChartCard(node, {
      title: "金额 ROI 曲线",
      copy: "按金额顺序看 ROI 走势，比单独看 top list 更容易发现拐点。",
      rows: orderedChartRows,
      xKey: "stake_label",
      yKey: "roi_pct"
    });
    renderBarChartCard(node, {
      title: "金额 PnL 图",
      copy: "直接按金额档比较绝对收益，判断更适合放大还是收缩。",
      rows: topByPnlRows.map((row) => ({
        stake_label: row.stake_label || row.matrix_stake_label || row.stake_usd,
        pnl_sum: row.pnl_sum
      })),
      xKey: "stake_label",
      yKey: "pnl_sum",
      limit: 6
    });
    renderBarChartCard(node, {
      title: "金额 ROI 图",
      copy: "聚焦 ROI 领先者，快速看到收益率更强的金额组合。",
      rows: topByRoiRows.map((row) => ({
        stake_label: row.stake_label || row.matrix_stake_label || row.stake_usd,
        roi_pct: row.roi_pct
      })),
      xKey: "stake_label",
      yKey: "roi_pct",
      limit: 6
    });
    renderPreviewTableCard(node, {
      title: "金额扫参全表",
      copy: "这是这次回测金额 sweep 的主表，优先用来决定后续默认 stake。",
      rows: previewRows,
      columns: ["stake_label", "stake_usd", "max_notional_usd", "trades", "pnl_sum", "roi_pct", "avg_roi_pct"],
      limit: 8
    });
    renderPreviewTableCard(node, {
      title: "按 ROI 领先",
      copy: "先看收益率更强的金额档位。",
      rows: topByRoiRows,
      columns: ["stake_label", "stake_usd", "max_notional_usd", "trades", "pnl_sum", "roi_pct"],
      limit: 6
    });
    renderPreviewTableCard(node, {
      title: "按 PnL 领先",
      copy: "再看绝对收益更强的金额档位，避免只盯 ROI。",
      rows: topByPnlRows,
      columns: ["stake_label", "stake_usd", "max_notional_usd", "trades", "pnl_sum", "roi_pct"],
      limit: 6
    });
  }

  function renderExperimentMatrixDetail(node, payload) {
    renderDetailViewSwitcher(node, { sectionId: "experiments" });
    const summary = payload && typeof payload.summary === "object" ? payload.summary : {};
    const matrixPreview = payload && typeof payload.matrix_summary_preview === "object"
      ? payload.matrix_summary_preview
      : {};
    const comparePreview = payload && typeof payload.compare_preview === "object"
      ? payload.compare_preview
      : {};
    const surfaceSummary = payload && typeof payload.surface_summary === "object"
      ? payload.surface_summary
      : (payload && typeof payload.compare_surface_summary === "object" ? payload.compare_surface_summary : {});
    const highlights = payload && typeof payload.highlights === "object" ? payload.highlights : {};
    const rowsByTheme = payload && typeof payload.rows_by_theme === "object" ? payload.rows_by_theme : {};
    const bestMatrix = payload && typeof payload.best_matrix === "object" ? payload.best_matrix : {};
    const bestCase = payload && typeof payload.best_case === "object" ? payload.best_case : {};
    const bestByRun = payload && typeof payload.best_by_run_preview === "object"
      ? payload.best_by_run_preview
      : {};
    const matrixRows = applyDetailRowFilter(
      rowsByTheme.matrix_rows || matrixPreview.preview_rows || matrixPreview.rows,
      "experiments"
    );
    const compareRows = applyDetailRowFilter(
      rowsByTheme.compare_rows || comparePreview.preview_rows || comparePreview.rows,
      "experiments"
    );
    const runRows = applyDetailRowFilter(
      rowsByTheme.leaders_by_run || bestByRun.rows,
      "experiments"
    );
    const featureSets = uniquePreviewValues(
      []
        .concat(matrixRows)
        .concat(compareRows)
        .concat(runRows),
      "feature_set",
      8
    );
    renderDetailFilterBar(node, {
      sectionId: "experiments",
      payload: payload,
      title: "矩阵结果筛选",
      copy: "专门为 matrix 结果页保留市场、运行名和变体筛选。",
      fields: [
        { fieldId: "topN", label: "仅看前 N", inputType: "number", min: 1, max: 16, placeholder: "6" },
        { fieldId: "market", label: "按市场过滤", inputType: "text", placeholder: "sol" },
        { fieldId: "runName", label: "按运行名过滤", inputType: "text", placeholder: "stake_matrix" },
        { fieldId: "variant", label: "按变体过滤", inputType: "text", placeholder: "aggressive" }
      ]
    });
    renderMetricCardGrid(node, {
      title: "矩阵摘要卡",
      copy: "先看 matrix 面到底覆盖了多少组合，以及最强组合落在哪个 run / 特征集上。",
      cards: [
        { label: "cases", value: surfaceSummary.cases || summary.cases },
        { label: "matrix_row_count", value: surfaceSummary.matrix_row_count || matrixPreview.row_count },
        { label: "compare_row_count", value: surfaceSummary.compare_row_count || comparePreview.row_count },
        { label: "market_count", value: surfaceSummary.market_count },
        { label: "group_count", value: surfaceSummary.group_count },
        { label: "run_name_count", value: surfaceSummary.run_name_count },
        { label: "stake_point_count", value: surfaceSummary.stake_point_count },
        { label: "feature_set_count", value: featureSets.length },
        { label: "best_matrix_roi_pct", value: bestMatrix.best_roi_pct || highlights.best_matrix_roi_pct },
        { label: "total_pnl_sum", value: bestMatrix.total_pnl_sum || highlights.total_pnl_sum }
      ]
    });
    renderOperatorCard(node, {
      title: "最佳矩阵组合",
      copy: "直接回答目前最佳 matrix 组合是谁，以及它绑定的特征集和变体。",
      items: [
        { label: "suite", value: payload.suite_name || summary.suite_name },
        { label: "run", value: payload.run_label || summary.run_label },
        { label: "best_matrix_run_name", value: bestMatrix.matrix_parent_run_name || bestMatrix.best_run_name || highlights.best_matrix_run_name },
        { label: "best_matrix_stake_label", value: bestMatrix.best_matrix_stake_label || highlights.best_matrix_stake_label },
        { label: "best_variant_label", value: bestMatrix.best_variant_label || highlights.best_variant_label },
        { label: "best_feature_set", value: bestCase.feature_set || highlights.best_feature_set },
        { label: "best_matrix_roi_pct", value: bestMatrix.best_roi_pct || highlights.best_matrix_roi_pct },
        { label: "total_pnl_sum", value: bestMatrix.total_pnl_sum }
      ]
    });
    renderOperatorCard(node, {
      title: "特征集视角",
      copy: "把 feature_set 拉到台前，方便直接比较哪套特征更值得继续实验。",
      items: [
        { label: "feature_sets", value: joinPreviewValues(featureSets) },
        { label: "feature_set_count", value: featureSets.length },
        { label: "best_case_feature_set", value: bestCase.feature_set },
        { label: "best_variant_feature_set", value: highlights.best_variant_feature_set || payload.best_variant_feature_set }
      ]
    });
    renderBarChartCard(node, {
      title: "矩阵最佳 ROI 图",
      copy: "按 matrix 分组画出最佳 ROI，快速判断哪组最值得追。",
      rows: matrixRows.map((row) => ({
        matrix_label: [row.matrix_parent_run_name || row.best_run_name, row.best_matrix_stake_label].filter(Boolean).join(" / "),
        best_roi_pct: row.best_roi_pct
      })),
      xKey: "matrix_label",
      yKey: "best_roi_pct",
      limit: 6
    });
    renderBarChartCard(node, {
      title: "矩阵总 PnL 图",
      copy: "同一张页上再看总 PnL，避免只盯最佳单点 ROI。",
      rows: matrixRows.map((row) => ({
        matrix_label: [row.matrix_parent_run_name || row.best_run_name, row.best_matrix_stake_label].filter(Boolean).join(" / "),
        total_pnl_sum: row.total_pnl_sum
      })),
      xKey: "matrix_label",
      yKey: "total_pnl_sum",
      limit: 6
    });
    renderBarChartCard(node, {
      title: "运行最佳 ROI 图",
      copy: "按运行维度看谁的最佳组合更强，便于后续继续收敛权重和 feature_set。",
      rows: runRows.map((row) => ({
        run_label: [row.run_name, row.feature_set].filter(Boolean).join(" / "),
        roi_pct: row.roi_pct
      })),
      xKey: "run_label",
      yKey: "roi_pct",
      limit: 6
    });
    renderPreviewTableCard(node, {
      title: "矩阵汇总表",
      copy: "matrix 页的主表，优先看最佳运行、金额档和总收益。",
      rows: matrixRows,
      columns: ["market", "group_name", "matrix_parent_run_name", "best_run_name", "best_matrix_stake_label", "best_variant_label", "best_roi_pct", "total_pnl_sum"],
      limit: 8
    });
    renderPreviewTableCard(node, {
      title: "矩阵案例表",
      copy: "把 matrix 里的 case 直接摊开，看 run / feature_set / variant 的组合收益。",
      rows: compareRows,
      columns: ["case_key", "market", "group_name", "run_name", "feature_set", "variant_label", "stake_usd", "pnl_sum", "roi_pct"],
      limit: 8
    });
    renderPreviewTableCard(node, {
      title: "运行领先者",
      copy: "每个运行保留当前最强组合，用来快速比较不同 run 的上限。",
      rows: runRows,
      columns: ["market", "group_name", "run_name", "feature_set", "variant_label", "pnl_sum", "roi_pct"],
      limit: 6
    });
  }

  function renderBundleDetail(node, payload) {
    renderOperatorCard(node, {
      title: "模型包详情",
      copy: "当前模型包的来源链路与激活范围。",
      items: [
        { label: "bundle_label", value: payload.bundle_label },
        { label: "profile", value: payload.profile },
        { label: "target", value: payload.target },
        { label: "source_training_run", value: payload.source_training_run },
        { label: "offset_count", value: payload.offset_count },
        { label: "is_active", value: payload.is_active }
      ]
    });
  }

  function previewNames(rows, key) {
    if (!Array.isArray(rows)) {
      return "";
    }
    return rows
      .map((row) => row && row[key] !== undefined ? String(row[key]).trim() : "")
      .filter(Boolean)
      .slice(0, 3)
      .join(", ");
  }

  function uniquePreviewValues(rows, key, limit) {
    if (!Array.isArray(rows)) {
      return [];
    }
    const maxItems = Math.max(1, Number(limit || 6));
    const values = [];
    const seen = new Set();
    rows.forEach((row) => {
      const value = row && row[key] !== undefined ? String(row[key]).trim() : "";
      if (!value || seen.has(value)) {
        return;
      }
      seen.add(value);
      values.push(value);
    });
    return values.slice(0, maxItems);
  }

  function joinPreviewValues(values) {
    if (!Array.isArray(values)) {
      return "";
    }
    return values.slice(0, 4).map((value) => String(value)).join(", ");
  }

  function joinCountMap(value) {
    if (!value || typeof value !== "object") {
      return "";
    }
    return Object.keys(value)
      .sort()
      .map((key) => String(key) + "=" + String(value[key]))
      .join(" · ");
  }

  function summaryEntries(payload) {
    const candidates = [];
    const runtimeSummary = runtimeSummaryPayload(payload);
    const keys = [
      "dataset",
      "section",
      "market",
      "cycle",
      "surface",
      "profile",
      "target",
      "spec_name",
      "suite_name",
      "section_count",
      "action_count",
      "row_count",
      "offset_count",
      "trades",
      "roi_pct",
      "pnl_sum",
      "cases",
      "completed_cases"
    ];
    keys.forEach((key) => {
      if (payload[key] !== undefined && payload[key] !== null && payload[key] !== "") {
        candidates.push({ label: key, value: stringifyValue(payload[key]) });
      }
    });
    [
      ["recent_tasks", payload.recent_task_count !== undefined ? payload.recent_task_count : runtimeSummary.recent_task_count],
      ["active_tasks", payload.active_task_count !== undefined ? payload.active_task_count : runtimeSummary.active_task_count],
      ["terminal_tasks", payload.terminal_task_count !== undefined ? payload.terminal_task_count : runtimeSummary.terminal_task_count],
      ["failed_tasks", payload.failed_task_count !== undefined ? payload.failed_task_count : runtimeSummary.failed_task_count],
      ["latest_active_task", payload.latest_active_task_id !== undefined ? payload.latest_active_task_id : runtimeSummary.latest_active_task_id],
      ["latest_terminal_task", payload.latest_terminal_task_id !== undefined ? payload.latest_terminal_task_id : runtimeSummary.latest_terminal_task_id],
      ["latest_failed_task", payload.latest_failed_task_id !== undefined ? payload.latest_failed_task_id : runtimeSummary.latest_failed_task_id],
      ["runtime_updated_at", payload.runtime_updated_at !== undefined ? payload.runtime_updated_at : runtimeSummary.updated_at]
    ].forEach(([label, value]) => {
      if (value !== undefined && value !== null && value !== "") {
        candidates.push({ label: String(label), value: stringifyValue(value) });
      }
    });
    runtimeSummaryLatestEntries(runtimeSummary).forEach((entry) => {
      candidates.push(entry);
    });
    if (payload.action_catalog && payload.action_catalog.action_count !== undefined) {
      candidates.push({ label: "action_catalog", value: stringifyValue(payload.action_catalog.action_count) });
    }
    if (payload.row_count === undefined && Array.isArray(payload.dataset_rows)) {
      candidates.push({ label: "row_count", value: stringifyValue(payload.dataset_rows.length) });
    }
    return candidates;
  }

  function runtimeSummaryPayload(payload) {
    if (payload && typeof payload.runtime_summary === "object" && payload.runtime_summary) {
      return payload.runtime_summary;
    }
    if (payload && typeof payload.runtime_state === "object" && payload.runtime_state) {
      return payload.runtime_state;
    }
    return {};
  }

  function runtimeSummaryForSection(sectionId) {
    return runtimeSummaryPayload(latestPayloadBySection[normalizeSection(sectionId)] || {});
  }

  function runtimeSummaryLatestEntries(runtimeSummary) {
    if (!runtimeSummary || typeof runtimeSummary !== "object") {
      return [];
    }
    const rows = [];
    [
      ["latest_active_task", runtimeSummary.latest_active_task_id],
      ["latest_terminal_task", runtimeSummary.latest_terminal_task_id],
      ["latest_failed_task", runtimeSummary.latest_failed_task_id]
    ].forEach(([label, value]) => {
      if (value !== undefined && value !== null && String(value).trim() !== "") {
        rows.push({ label: displayLabel(label), value: stringifyValue(value) });
      }
    });
    return rows;
  }

  function runtimeBoardGroups(runtimeSummary) {
    const counts = runtimeSummary && typeof runtimeSummary.status_group_counts === "object"
      ? runtimeSummary.status_group_counts
      : {};
    return [
      {
        key: "active",
        title: "活跃任务",
        copy: "仍在排队或运行中的任务，会持续推进运行阶段。",
        rows: Array.isArray((runtimeSummary || {}).recent_active_tasks) ? runtimeSummary.recent_active_tasks : [],
        count: counts.active || 0,
        tone: "is-running"
      },
      {
        key: "failed",
        title: "失败任务",
        copy: "最新失败任务会固定在这里，方便直接展开定位问题。",
        rows: Array.isArray((runtimeSummary || {}).recent_failed_tasks) ? runtimeSummary.recent_failed_tasks : [],
        count: counts.failed || 0,
        tone: "is-failed"
      },
      {
        key: "terminal",
        title: "已结束任务",
        copy: "持久化运行态记录下来的最新已结束任务。",
        rows: Array.isArray((runtimeSummary || {}).recent_terminal_tasks) ? runtimeSummary.recent_terminal_tasks : [],
        count: counts.terminal || 0,
        tone: ""
      }
    ];
  }

  function runtimeBoardPayload(runtimeSummary) {
    if (runtimeSummary && typeof runtimeSummary.runtime_board === "object" && runtimeSummary.runtime_board) {
      return runtimeSummary.runtime_board;
    }
    return runtimeSummary && typeof runtimeSummary === "object" ? runtimeSummary : {};
  }

  function runtimeBoardWarnings(runtimeSummary) {
    const board = runtimeBoardPayload(runtimeSummary);
    return Array.isArray((board || {}).warnings) ? board.warnings : [];
  }

  function runtimeBoardLeadCopy(runtimeSummary) {
    const board = runtimeBoardPayload(runtimeSummary);
    const counts = runtimeSummary && typeof runtimeSummary.status_group_counts === "object"
      ? runtimeSummary.status_group_counts
      : {};
    const parts = [];
    ["active", "terminal", "failed"].forEach((key) => {
      if (counts[key] !== undefined) {
        parts.push(displayLabel(key) + "=" + String(counts[key]));
      }
    });
    if ((runtimeSummary || {}).latest_failed_task_id) {
      parts.push(displayLabel("latest_failed_task") + "=" + String(runtimeSummary.latest_failed_task_id));
    }
    if ((runtimeSummary || {}).updated_at) {
      parts.push("运行态=" + String(runtimeSummary.updated_at));
    }
    const invalidTaskFiles = Array.isArray((board || {}).invalid_task_files) ? board.invalid_task_files : [];
    if (invalidTaskFiles.length) {
      parts.push("无效任务文件=" + String(invalidTaskFiles.length));
    }
    const retention = board && typeof board.retention === "object" ? board.retention : {};
    if (retention.is_truncated) {
      parts.push("保留=" + String(retention.retained_task_count || 0));
      parts.push("裁剪=" + String(retention.dropped_task_count || 0));
    }
    if (!parts.length) {
      return "运行态摘要暂时还没有记录到可展示的操作信号。";
    }
    return "持久化运行态快照：" + parts.join(" · ");
  }

  function renderRuntimeBoard(context, runtimeSummary) {
    const node = context && context.node;
    if (!node) {
      return;
    }
    node.innerHTML = "";
    if (!runtimeSummary || typeof runtimeSummary !== "object" || !Object.keys(runtimeSummary).length) {
      const empty = document.createElement("div");
      empty.className = "console-empty";
      empty.textContent = "运行态摘要暂不可用。";
      node.appendChild(empty);
      return;
    }

    const summary = document.createElement("div");
    summary.className = "console-task-history-summary";
    summary.textContent = runtimeBoardLeadCopy(runtimeSummary);
    node.appendChild(summary);

    const warnings = runtimeBoardWarnings(runtimeSummary);
    if (warnings.length) {
      const warningList = document.createElement("div");
      warningList.className = "console-runtime-warning-list";
      warnings.forEach((warning) => {
        const card = document.createElement("article");
        const severity = String((warning || {}).severity || "").trim();
        card.className = "console-runtime-warning" + (severity ? (" is-" + severity) : "");
        const code = String((warning || {}).code || "").trim();
        const title = code ? displayLabel(code) : "运行态告警";
        const message = String((warning || {}).message || "").trim() || "运行态看板记录了一条操作告警。";
        card.innerHTML =
          '<h3 class="console-runtime-warning-title">' + escapeHtml(title) + '</h3>' +
          '<p class="console-runtime-warning-copy">' + escapeHtml(message) + '</p>';
        warningList.appendChild(card);
      });
      node.appendChild(warningList);
    }

    const grid = document.createElement("div");
    grid.className = "console-runtime-board";
    runtimeBoardGroups(runtimeSummary).forEach((group) => {
      const column = document.createElement("section");
      column.className = "console-runtime-column";
      if (group.tone) {
        column.classList.add(group.tone);
      }
      column.innerHTML =
        '<div class="console-runtime-column-head">' +
          '<h3 class="console-runtime-column-title">' + escapeHtml(group.title) + '</h3>' +
          '<span class="console-pill">' + escapeHtml(String(group.count)) + '</span>' +
        '</div>' +
        '<p class="console-runtime-column-copy">' + escapeHtml(group.copy) + '</p>';
      const list = document.createElement("div");
      list.className = "console-runtime-column-list";
      if (!group.rows.length) {
        const empty = document.createElement("div");
        empty.className = "console-empty";
        empty.textContent = "当前没有记录到" + group.title + "。";
        list.appendChild(empty);
      } else {
        group.rows.slice(0, 3).forEach((row) => {
          list.appendChild(buildTaskRowArticle(context, row));
        });
      }
      column.appendChild(list);
      grid.appendChild(column);
    });
    node.appendChild(grid);
  }

  function actionFormCardCopy(sectionId) {
    if (normalizeSection(sectionId) === "data_overview") {
      return "在这里填写命令相关字段。所选数据行只作为上下文展示，请求参数仍以这些表单字段和顶部默认值为准。";
    }
    if (normalizeSection(sectionId) === "experiments") {
      return "这里既可以直接跑已有 suite，也可以填写内联实验字段生成新的 suite spec。留空字段会回退到所选行上下文和顶部默认值。";
    }
    return "留空字段会回退到所选行上下文和顶部默认值。";
  }

  function actionFieldLabel(action, fieldId) {
    const fields = Array.isArray((action || {}).form_fields) ? action.form_fields : [];
    const match = fields.find((field) => String((field || {}).field_id || "") === String(fieldId || ""));
    if (match && match.label) {
      return String(match.label);
    }
    return displayLabel(fieldId);
  }

  function actionFieldHidden(sectionId, actionId, fieldId) {
    if (normalizeSection(sectionId) !== "experiments" || String(actionId || "") !== "research_experiment_run_suite") {
      return false;
    }
    const alwaysVisible = new Set(["suite_mode", "suite", "run_label"]);
    if (alwaysVisible.has(String(fieldId || ""))) {
      return false;
    }
    const formValues = actionFormValues("research_experiment_run_suite", "experiments");
    return String(formValues.suite_mode || "existing").trim() !== "inline";
  }

  function actionAdvancedStateKey(sectionId, actionId) {
    return normalizeSection(sectionId) + "::" + String(actionId || "");
  }

  function loadActionAdvancedState(sectionId, actionId) {
    return Boolean(actionAdvancedState[actionAdvancedStateKey(sectionId, actionId)]);
  }

  function setActionAdvancedState(sectionId, actionId, nextValue) {
    actionAdvancedState[actionAdvancedStateKey(sectionId, actionId)] = Boolean(nextValue);
  }

  function actionFieldBucket(sectionId, actionId, field) {
    const fieldId = String((field || {}).field_id || "");
    if ((field || {}).required) {
      return "required";
    }
    if (String(actionId || "") === "research_train_run") {
      if (["feature_set", "label_set", "run_label", "parallel_workers"].includes(fieldId)) {
        return "common";
      }
      return "advanced";
    }
    if (String(actionId || "") === "research_bundle_build") {
      if (["offsets", "source_training_run"].includes(fieldId)) {
        return "common";
      }
      return "advanced";
    }
    if (String(actionId || "") === "research_activate_bundle") {
      return fieldId === "notes" ? "advanced" : "required";
    }
    if (String(actionId || "") === "research_backtest_run") {
      if (["bundle_label", "stake_usd", "max_notional_usd"].includes(fieldId)) {
        return "common";
      }
      return "advanced";
    }
    if (String(actionId || "") === "research_experiment_run_suite") {
      if (["suite_mode", "suite", "run_label"].includes(fieldId)) {
        return "required";
      }
      if (["window_start", "window_end", "feature_set_variants", "stakes_usd", "max_notional_usd", "parallel_case_workers"].includes(fieldId)) {
        return "common";
      }
      return "advanced";
    }
    if (normalizeSection(sectionId) === "data_overview") {
      return "common";
    }
    return "advanced";
  }

  function actionFieldHelpText(field) {
    const fieldId = String((field || {}).field_id || "");
    const builtin = String(FIELD_EXPLANATIONS[fieldId] || "").trim();
    const note = String((field || {}).notes || "").trim();
    if (builtin && note) {
      return builtin + " " + note;
    }
    return builtin || note;
  }

  function rowSelectionLabel(row, sectionId) {
    if (!row || typeof row !== "object") {
      return "";
    }
    const section = normalizeSection(sectionId);
    if (section === "data_overview") {
      const datasetName = String(row.dataset_name || "").trim();
      const kind = String(row.kind || "").trim();
      if (datasetName) {
        return kind ? (datasetName + " (" + kind + ")") : datasetName;
      }
      return String(row.location || "").trim();
    }
    if (section === "bundles") {
      const label = String(row.bundle_label || "").trim();
      const scope = [String(row.profile || "").trim(), String(row.target || "").trim()]
        .filter(Boolean)
        .join(" / ");
      const parts = [];
      if (label) {
        parts.push(label);
      }
      if (scope) {
        parts.push(scope);
      }
      if (row.is_active === true) {
        parts.push("已激活");
      }
      return parts.join(" · ");
    }
    if (section === "experiments") {
      const suite = String(row.suite_name || row.suite || "").trim();
      const runLabel = String(row.run_label || "").trim();
      const identity = [suite, runLabel].filter(Boolean).join(" / ");
      if (row.completed_cases !== undefined && row.cases !== undefined) {
        const coverage = String(row.completed_cases) + "/" + String(row.cases) + " 个案例";
        return identity ? (identity + " · " + coverage) : coverage;
      }
      return identity;
    }
    return rowSelectionKey(row);
  }

  function quickActionSummaryCopy(sectionId, selectedRow) {
    const section = normalizeSection(sectionId);
    if (section === "data_overview") {
      return selectedRow
        ? "当前选中的数据行会持续作为参考上下文，真正的同步/构建请求仍由下方表单组合而成。"
        : "快捷动作会复用当前数据总览上下文和下方表单。";
    }
    if (section === "bundles") {
      return selectedRow
        ? "当前选中的模型包会锚定这两个动作。激活会复用其模型包标签；构建只会复用来源链路和 offsets，新的模型包标签仍需显式填写。"
        : "模型包快捷动作会复用当前 profile/target 上下文。需要显式指定模型包标签、offsets 或来源训练运行时，请填写表单。";
    }
    if (section === "experiments") {
      return selectedRow
        ? "当前选中的实验会保留 suite 上下文用于重跑；你也可以在表单里补 feature_set / stake / runtime policy 做新一轮内联实验。"
        : "实验快捷动作会使用当前 suite 输入和默认值；如果不想先手写 suite，也可以直接在表单里内联生成。";
    }
    return "快捷动作会复用当前动作上下文和已填写的表单值。";
  }

  function payloadActionContextForSection(sectionId) {
    return actionContextFromPayload(
      sectionId,
      latestPayloadBySection[normalizeSection(sectionId)] || {}
    );
  }

  function reloadSelectedDetail(sectionId) {
    const resolved = normalizeSection(sectionId);
    const panel = panels.find((node) => node.dataset.sectionId === resolved);
    if (!panel) {
      return;
    }
    const node = panel.querySelector("[data-console-section-detail]");
    if (!node) {
      return;
    }
    const selectedRow = selectedRowsBySection[resolved] || null;
    if (!selectedRow) {
      renderSectionDetail({ node: node, sectionId: resolved }, sectionDetailsBySection[resolved] || null);
      return;
    }
    loadSectionDetail(
      {
        node: node,
        sectionId: resolved,
        statusNode: panel.querySelector("[data-console-status]")
      },
      selectedRow,
      payloadActionContextForSection(resolved)
    );
  }

  function renderDetailViewSwitcher(node, options) {
    if (!node) {
      return;
    }
    const sectionId = normalizeSection((options || {}).sectionId);
    const views = detailViewOptions(sectionId);
    if (views.length < 2) {
      return;
    }
    const activeView = activeDetailView(sectionId);
    const article = document.createElement("article");
    article.className = "console-operator-card console-detail-view-switcher";
    article.innerHTML =
      '<h3 class="console-operator-card-title">结果视图</h3>' +
      '<p class="console-operator-card-copy">' +
        escapeHtml(sectionId === "backtests"
          ? "把回测总览和金额扫参拆开，避免单页塞太多信息。"
          : "把实验总览和矩阵结果拆开，优先回答哪组 matrix / feature_set 更优。") +
      '</p>';
    const tabs = document.createElement("div");
    tabs.className = "console-detail-view-tabs";
    views.forEach((view) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "console-detail-view-button" + (String(view.id) === activeView ? " is-active" : "");
      button.textContent = String(view.label || displayLabel(view.id));
      button.addEventListener("click", function () {
        const previous = activeDetailView(sectionId);
        const next = setDetailView(sectionId, view.id);
        if (next === previous) {
          return;
        }
        reloadSelectedDetail(sectionId);
      });
      tabs.appendChild(button);
    });
    article.appendChild(tabs);
    const current = views.find((view) => String(view.id) === activeView) || {};
    if (current.note) {
      const hint = document.createElement("p");
      hint.className = "console-detail-view-hint";
      hint.textContent = String(current.note);
      article.appendChild(hint);
    }
    node.appendChild(article);
  }

  function quickActionStateNote(action, sectionId, selectedRow, missingArgs) {
    const actionId = String((action || {}).action_id || "");
    const section = normalizeSection(sectionId);
    if (missingArgs.length) {
      const labels = missingArgs.map((key) => actionFieldLabel(action, key));
      if (section === "data_overview") {
        return "请在动作表单中填写：" + labels.join("、") + "。";
      }
      if (section === "bundles" && actionId === "research_activate_bundle" && !selectedRow) {
        return "请先选中模型包行，或填写：" + labels.join("、") + "。";
      }
      if (section === "experiments" && actionId === "research_experiment_run_suite" && !selectedRow) {
        return "请先选中实验行，或填写：" + labels.join("、") + "。";
      }
      return "缺少字段：" + labels.join("、");
    }
    if (section === "data_overview") {
      return selectedRow
        ? "已就绪：选中数据行 + 表单值"
        : "已就绪：分区上下文 + 表单值";
    }
    if (section === "bundles") {
      if (actionId === "research_activate_bundle") {
        return selectedRow ? "已就绪：可激活当前模型包" : "已就绪：模型包标签 + 当前范围";
      }
      if (actionId === "research_bundle_build") {
        return selectedRow
          ? "已就绪：可基于当前模型包来源链路重建；新的模型包标签仍需显式指定"
          : "已就绪：profile/target + 表单值";
      }
    }
    if (section === "experiments" && actionId === "research_experiment_run_suite") {
      return selectedRow ? "已就绪：可重跑当前 suite 上下文" : "已就绪：suite 输入 + 默认值";
    }
    return selectedRow ? "已就绪：来自选中行" : "已就绪：来自分区动作上下文";
  }

  function rowSelectionStatusCopy(sectionId, row) {
    const section = normalizeSection(sectionId);
    const label = rowSelectionLabel(row, section);
    if (!label) {
      return "已更新所选行上下文。";
    }
    if (section === "data_overview") {
      return "已选中数据集 " + label + "。快捷动作会复用这份数据上下文作为参考，但表单参数仍保持显式。";
    }
    if (section === "bundles") {
      return "已选中模型包 " + label + "。激活动作会复用其模型包标签；构建动作会复用其 offsets 和来源链路，同时保持新模型包标签显式填写。";
    }
    if (section === "experiments") {
      return "已选中实验 " + label + "。运行套件动作会复用这个 suite 上下文，除非表单显式覆盖。";
    }
    return "已选中 " + label + "。快捷动作现在会复用这个上下文。";
  }

  function renderActionContext(context) {
    const node = context.node;
    if (!node) {
      return;
    }
    node.innerHTML = "";
    const actionContext = context.actionContext && typeof context.actionContext === "object"
      ? context.actionContext
      : {};
    if (!Object.keys(actionContext).length) {
      const empty = document.createElement("div");
      empty.className = "console-action-context-empty";
      empty.textContent = "暂时还没有动作上下文。请先选中一行，或等待分区返回带 action_context 的数据。";
      node.appendChild(empty);
      return;
    }
    const entries = Object.entries(actionContext)
      .filter((entry) => entry[1] !== undefined && entry[1] !== null && entry[1] !== "")
      .slice(0, 8);
    if (!entries.length) {
      const empty = document.createElement("div");
      empty.className = "console-action-context-empty";
      empty.textContent = "当前所选行没有暴露动作上下文。";
      node.appendChild(empty);
      return;
    }
    entries.forEach(([label, value]) => {
      const article = document.createElement("article");
      article.className = "console-card";
      article.innerHTML =
        '<h3 class="console-card-title">' + escapeHtml(displayLabel(label)) + '</h3>' +
        '<p class="console-card-copy">' + escapeHtml(stringifyValue(value)) + '</p>';
      node.appendChild(article);
    });
  }

  function renderActionForms(context) {
    const node = context.node;
    const actions = executableSectionActions(context.sectionId, context.catalog).filter((action) => {
      return Array.isArray(action.form_fields) && action.form_fields.length > 0;
    });
    const onChange = typeof context.onChange === "function" ? context.onChange : null;
    if (!node) {
      return;
    }
    node.innerHTML = "";
    if (!context.catalog || !Array.isArray(context.catalog.actions) || !context.catalog.actions.length) {
      const empty = document.createElement("div");
      empty.className = "console-empty";
      empty.textContent = "当前没有可用的动作表单。";
      node.appendChild(empty);
      return;
    }
    if (!actions.length) {
      const empty = document.createElement("div");
      empty.className = "console-empty";
      empty.textContent = "当前分区还没有额外表单字段。";
      node.appendChild(empty);
      return;
    }
    actions.forEach((action) => {
      const article = document.createElement("article");
      article.className = "console-action-form-card";
      article.innerHTML =
        '<h3 class="console-action-card-title">' + escapeHtml(action.title || action.action_id || "动作表单") + '</h3>' +
        '<p class="console-action-card-copy">' + escapeHtml(actionFormCardCopy(context.sectionId)) + '</p>';
      const visibleFields = action.form_fields.filter((field) => !actionFieldHidden(context.sectionId, action.action_id, field.field_id));
      const advancedOpen = loadActionAdvancedState(context.sectionId, action.action_id);
      const groups = [
        { id: "required", title: "必填参数", rows: visibleFields.filter((field) => actionFieldBucket(context.sectionId, action.action_id, field) === "required") },
        { id: "common", title: "常用参数", rows: visibleFields.filter((field) => actionFieldBucket(context.sectionId, action.action_id, field) === "common") },
        { id: "advanced", title: "高级参数", rows: visibleFields.filter((field) => actionFieldBucket(context.sectionId, action.action_id, field) === "advanced") },
      ];
      groups.forEach((group) => {
        if (!group.rows.length) {
          return;
        }
        if (group.id === "advanced" && !advancedOpen) {
          const collapsed = document.createElement("section");
          collapsed.className = "console-form-section";
          const head = document.createElement("div");
          head.className = "console-form-section-head";
          const title = document.createElement("h4");
          title.className = "console-form-section-title";
          title.textContent = group.title;
          const button = document.createElement("button");
          button.type = "button";
          button.className = "console-toggle-button";
          button.textContent = "显示高级参数";
          button.addEventListener("click", function () {
            setActionAdvancedState(context.sectionId, action.action_id, true);
            if (onChange) {
              onChange();
            }
          });
          head.appendChild(title);
          head.appendChild(button);
          collapsed.appendChild(head);
          article.appendChild(collapsed);
          return;
        }
        const section = document.createElement("section");
        section.className = "console-form-section";
        const head = document.createElement("div");
        head.className = "console-form-section-head";
        const title = document.createElement("h4");
        title.className = "console-form-section-title";
        title.textContent = group.title;
        head.appendChild(title);
        if (group.id === "advanced") {
          const button = document.createElement("button");
          button.type = "button";
          button.className = "console-toggle-button";
          button.textContent = "收起高级参数";
          button.addEventListener("click", function () {
            setActionAdvancedState(context.sectionId, action.action_id, false);
            if (onChange) {
              onChange();
            }
          });
          head.appendChild(button);
        }
        section.appendChild(head);
        const grid = document.createElement("div");
        grid.className = "console-form-grid";
        group.rows.forEach((field) => {
          const label = document.createElement("label");
          label.className = "console-form-field";
          const titleNode = document.createElement("span");
          titleNode.className = "console-field-label";
          titleNode.textContent = String(field.label || field.field_id || "");
          const input = document.createElement("input");
          input.className = "console-input";
          input.type = String(field.input_type || "text");
          input.dataset.consoleActionFormInput = "true";
          input.dataset.formSection = normalizeSection(context.sectionId);
          input.dataset.actionId = String(action.action_id || "");
          input.dataset.fieldId = String(field.field_id || "");
          input.value = loadActionFormValue(
            context.sectionId,
            action.action_id,
            field,
            String(field.default_value || "")
          );
          if (field.placeholder) {
            input.placeholder = String(field.placeholder);
          }
          if (field.required) {
            input.required = true;
          }
          input.addEventListener("input", function () {
            saveActionFormValue(
              context.sectionId,
              String(action.action_id || ""),
              String(field.field_id || ""),
              input.value
            );
            if (onChange) {
              onChange();
            }
          });
          label.appendChild(titleNode);
          label.appendChild(input);
          const helpText = actionFieldHelpText(field);
          if (helpText) {
            const help = document.createElement("span");
            help.className = "console-field-help";
            help.textContent = helpText;
            label.appendChild(help);
          }
          grid.appendChild(label);
        });
        section.appendChild(grid);
        article.appendChild(section);
      });
      node.appendChild(article);
    });
  }

  function renderActionCatalog(context) {
    const node = context.node;
    if (!node) {
      return;
    }
    node.innerHTML = "";
        if (!context.catalog || !Array.isArray(context.catalog.actions) || !context.catalog.actions.length) {
      const empty = document.createElement("div");
      empty.className = "console-empty";
      empty.textContent = "当前没有可用的动作目录。";
      node.appendChild(empty);
      return;
    }
    const scopedActions = sectionCatalogActions(context.sectionId, context.catalog);
    if (!scopedActions.length) {
      const empty = document.createElement("div");
      empty.className = "console-empty";
      empty.textContent = "当前分区还没有注册动作。";
      node.appendChild(empty);
      return;
    }
    scopedActions.slice(0, 8).forEach((action) => {
      const article = document.createElement("article");
      article.className = "console-action-card";
      const requiredArgs = Array.isArray(action.required_args) ? action.required_args.join(", ") : "";
      const actionId = String(action.action_id || "");
      article.innerHTML =
        '<h3 class="console-action-card-title">' + escapeHtml(action.title || action.action_id || "动作") + '</h3>' +
        '<p class="console-action-card-copy">' + escapeHtml(action.notes || "") + '</p>' +
        '<div class="console-card-meta">' +
          '<span class="console-pill">' + escapeHtml(displayLabel(action.target_domain || "")) + '</span>' +
          '<span class="console-pill">' + escapeHtml(displayLabel(action.command_role || "")) + '</span>' +
        '</div>' +
        '<pre class="console-action-card-code">' + escapeHtml(requiredArgs ? ("必填: " + requiredArgs.split(", ").map(displayLabel).join(", ")) : "必填: 无") + '</pre>';
      const actionsBar = document.createElement("div");
      actionsBar.className = "console-action-card-actions";
      if (actionIsShellEnabled(action)) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "console-action-button";
        button.textContent = actionButtonLabel(actionId);
        button.addEventListener("click", function () {
          runConsoleAction({
            sectionId: context.sectionId,
            actionId: actionId,
            executionMode: String(action.preferred_execution_mode || "sync"),
            statusNode: context.statusNode,
            jsonNode: context.jsonNode,
            button: button,
            actionResultContext: context.actionResultContext
          });
        });
        actionsBar.appendChild(button);
      } else {
        const note = document.createElement("span");
        note.className = "console-action-note";
        note.textContent = "当前阶段仅展示目录";
        actionsBar.appendChild(note);
      }
      article.appendChild(actionsBar);
      node.appendChild(article);
    });
  }

  function requestHasValue(value) {
    if (value === undefined || value === null) {
      return false;
    }
    if (typeof value === "string") {
      return Boolean(value.trim());
    }
    if (Array.isArray(value)) {
      return value.length > 0;
    }
    return true;
  }

  function actionRequestState(action, sectionId) {
    const request = buildActionRequest(String((action || {}).action_id || ""), { sectionId: sectionId });
    const requiredArgs = Array.isArray((action || {}).required_args) ? action.required_args : [];
    return {
      request: request,
      missingArgs: requiredArgs.filter((key) => !requestHasValue(request[key]))
    };
  }

  function actionRequestPreview(action, request) {
    const requiredArgs = Array.isArray((action || {}).required_args) ? action.required_args : [];
    if (!requiredArgs.length) {
      return "必填: 无";
    }
    return requiredArgs.map((key) => {
      const value = requestHasValue(request[key]) ? stringifyValue(request[key]) : "<未填>";
      return displayLabel(key) + "=" + value;
    }).join("\\n");
  }

  function renderRowQuickActions(context) {
    const node = context.node;
    const sectionId = normalizeSection(context.sectionId);
    const actions = executableSectionActions(sectionId, context.catalog);
    const selectedRow = selectedRowsBySection[sectionId] || null;
    const selectedLabel = rowSelectionLabel(selectedRow, sectionId);
    if (!node) {
      return;
    }
    node.innerHTML = "";
    if (!actions.length) {
      const empty = document.createElement("div");
      empty.className = "console-empty";
      empty.textContent = "当前分区没有可执行的快捷动作。";
      node.appendChild(empty);
      return;
    }

    const summary = document.createElement("div");
    summary.className = "console-row-quick-summary";
    summary.innerHTML =
      '<span class="console-pill">' + escapeHtml(selectedRow ? "选中行上下文" : "分区动作上下文") + '</span>' +
      (selectedLabel ? ('<span class="console-pill">' + escapeHtml(selectedLabel) + '</span>') : "") +
      '<span class="console-row-quick-copy">' + escapeHtml(quickActionSummaryCopy(sectionId, selectedRow)) + '</span>';
    node.appendChild(summary);

    const grid = document.createElement("div");
    grid.className = "console-action-grid";
    actions.forEach((action) => {
      const actionId = String(action.action_id || "");
      const requestState = actionRequestState(action, sectionId);
      const missingArgs = requestState.missingArgs;
      const article = document.createElement("article");
      article.className = "console-action-card console-row-action-card";
      article.classList.toggle("is-blocked", missingArgs.length > 0);
      article.innerHTML =
        '<h3 class="console-action-card-title">' + escapeHtml(action.title || action.action_id || "快捷动作") + '</h3>' +
        '<p class="console-action-card-copy">' + escapeHtml(action.notes || "") + '</p>' +
        '<div class="console-card-meta">' +
          '<span class="console-pill">' + escapeHtml(displayLabel(action.target_domain || "")) + '</span>' +
          '<span class="console-pill">' + escapeHtml(displayLabel(action.command_role || "")) + '</span>' +
        '</div>' +
        '<pre class="console-action-card-code">' + escapeHtml(actionRequestPreview(action, requestState.request)) + '</pre>';
      const actionsBar = document.createElement("div");
      actionsBar.className = "console-action-card-actions";
      const button = document.createElement("button");
      button.type = "button";
      button.className = "console-action-button";
      button.textContent = actionButtonLabel(actionId);
      button.disabled = missingArgs.length > 0;
      button.addEventListener("click", function () {
        runConsoleAction({
          sectionId: sectionId,
          actionId: actionId,
          executionMode: String(action.preferred_execution_mode || "sync"),
          statusNode: context.statusNode,
          jsonNode: context.jsonNode,
          button: button,
          actionResultContext: context.actionResultContext
        });
      });
      actionsBar.appendChild(button);
      const note = document.createElement("span");
      note.className = "console-action-note";
      note.textContent = quickActionStateNote(action, sectionId, selectedRow, missingArgs);
      actionsBar.appendChild(note);
      article.appendChild(actionsBar);
      grid.appendChild(article);
    });
    node.appendChild(grid);
  }

  async function runConsoleAction(context) {
    const actionId = String(context.actionId || "");
    const sectionId = normalizeSection(context.sectionId);
    const executionMode = String(context.executionMode || "sync");
    const request = buildActionRequest(actionId, { sectionId: sectionId });
    if (context.statusNode) {
      context.statusNode.textContent = "正在执行动作 " + actionId + "（" + (executionMode === "async" ? "异步" : "同步") + "）...";
    }
    if (context.button) {
      context.button.disabled = true;
    }
    try {
      const response = await fetch(actionExecutePath(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action_id: actionId,
          execution_mode: executionMode,
          request: request
        })
      });
      const payload = await response.json();
      if (context.statusNode) {
        context.statusNode.textContent = response.ok
          ? (executionMode === "async" && payload.task_id
              ? taskStatusSummary(payload)
              : synchronousActionStatusSummary(actionId, payload))
          : ("动作失败：" + ((payload.error || {}).message || "未知错误"));
      }
      actionResultsBySection[sectionId] = payload;
      if (context.actionResultContext) {
        renderActionResult(context.actionResultContext, payload);
      }
      if (context.jsonNode) {
        context.jsonNode.textContent = JSON.stringify(payload, null, 2);
      }
      if (response.ok && executionMode === "async" && payload.task_id) {
        refreshRecentTasks(sectionId, { silent: true });
        pollConsoleTask({
          sectionId: sectionId,
          taskId: String(payload.task_id),
          statusNode: context.statusNode,
          jsonNode: context.jsonNode,
          actionResultContext: context.actionResultContext
        });
      }
    } catch (error) {
      if (context.statusNode) {
        context.statusNode.textContent = "动作请求失败。";
      }
      actionResultsBySection[sectionId] = { error: String(error), status: "error", action_id: actionId };
      if (context.actionResultContext) {
        renderActionResult(context.actionResultContext, { error: String(error) });
      }
      if (context.jsonNode) {
        context.jsonNode.textContent = JSON.stringify({ error: String(error) }, null, 2);
      }
    } finally {
      if (context.button) {
        context.button.disabled = false;
      }
    }
  }

  async function pollConsoleTask(context) {
    const taskId = String(context.taskId || "");
    if (!taskId) {
      return;
    }
    const existing = activeTaskPolls[taskId];
    if (existing) {
      existing.context = context;
      return existing.promise;
    }
    const entry = {
      context: context,
      consecutiveErrors: 0,
      promise: null
    };
    activeTaskPolls[taskId] = entry;
    entry.promise = (async function () {
      while (activeTaskPolls[taskId] === entry) {
        const currentContext = entry.context || context;
        const sectionId = normalizeSection(currentContext.sectionId);
        const shouldRefreshSection = shell.dataset.activeSection === sectionId;
        let terminalPayload = null;
        let shouldStopPolling = false;
        let encounteredError = null;
        let shouldRetry = false;
        try {
          const response = await fetch(taskStatusPath(taskId));
          const payload = await response.json();
          entry.consecutiveErrors = 0;
          actionResultsBySection[sectionId] = payload;
          if (currentContext.actionResultContext) {
            renderActionResult(currentContext.actionResultContext, payload);
          }
          refreshRecentTasks(sectionId, { silent: true });
          if (currentContext.jsonNode) {
            currentContext.jsonNode.textContent = JSON.stringify(payload, null, 2);
          }
          if (currentContext.statusNode) {
            currentContext.statusNode.textContent = taskStatusSummary(payload);
          }
          const status = String(payload.status || "").trim();
          if (isTaskTerminalStatus(status)) {
            terminalPayload = payload;
            shouldStopPolling = true;
          } else {
            shouldRetry = isTaskActiveStatus(status);
            if (!shouldRetry) {
              shouldStopPolling = true;
            }
          }
        } catch (error) {
          entry.consecutiveErrors += 1;
          encounteredError = error;
          if (entry.consecutiveErrors >= TASK_POLL_MAX_CONSECUTIVE_ERRORS) {
            shouldStopPolling = true;
          } else {
            shouldRetry = true;
          }
        }
        if (terminalPayload && shouldRefreshSection) {
          await loadSection(sectionId);
          reloadSelectedDetail(sectionId);
        }
        if (shouldStopPolling) {
          delete activeTaskPolls[taskId];
          if (encounteredError && currentContext.statusNode) {
            currentContext.statusNode.textContent = "轮询任务状态失败：" + String(encounteredError);
          }
          return terminalPayload;
        }
        if (!shouldRetry) {
          delete activeTaskPolls[taskId];
          return null;
        }
        await new Promise((resolve) => window.setTimeout(resolve, TASK_POLL_INTERVAL_MS));
      }
      return null;
    })();
    return entry.promise;
  }

  function buildActionRequest(actionId, options) {
    const inputs = currentInputs();
    const sectionId = normalizeSection((options || {}).sectionId);
    const formValues = actionFormValues(actionId, sectionId);
    const actionContext = effectiveActionContext(sectionId);
    const windowBounds = parseWindowBounds(actionContext.window);
    if (actionId === "data_refresh_summary") {
      return {
        market: inputs.market,
        cycle: inputs.cycle,
        surface: inputs.surface,
        write_state: true
      };
    }
    if (actionId === "data_sync") {
      return {
        sync_command: formValues.sync_command || actionContext.sync_command || "",
        market: actionContext.market || inputs.market,
        cycle: actionContext.cycle || inputs.cycle,
        surface: actionContext.surface || inputs.surface
      };
    }
    if (actionId === "data_build") {
      return {
        build_command: formValues.build_command || actionContext.build_command || "",
        market: actionContext.market || inputs.market,
        cycle: actionContext.cycle || inputs.cycle,
        surface: actionContext.surface || inputs.surface,
        date: formValues.date || actionContext.date || ""
      };
    }
    if (actionId === "research_train_run") {
      return {
        market: actionContext.market || inputs.market,
        cycle: actionContext.cycle || inputs.cycle,
        profile: actionContext.profile || inputs.profile,
        model_family: actionContext.model_family || inputs.model_family,
        target: actionContext.target || inputs.target,
        feature_set: formValues.feature_set || actionContext.feature_set || "deep_otm_v1",
        label_set: formValues.label_set || actionContext.label_set || "truth",
        label_source: formValues.label_source || actionContext.label_source || "",
        window_start: formValues.window_start || actionContext.window_start || windowBounds.start || "",
        window_end: formValues.window_end || actionContext.window_end || windowBounds.end || "",
        offsets: formValues.offsets || actionContext.offsets || "7,8,9",
        run_label: formValues.run_label || inputs.run_label || "planned"
      };
    }
    if (actionId === "research_bundle_build") {
      return {
        market: actionContext.market || inputs.market,
        cycle: actionContext.cycle || inputs.cycle,
        profile: actionContext.profile || inputs.profile,
        model_family: actionContext.model_family || inputs.model_family,
        target: actionContext.target || inputs.target,
        bundle_label: formValues.bundle_label || inputs.bundle_label || "planned",
        source_training_run: formValues.source_training_run || actionContext.run_label || "",
        offsets: formValues.offsets || actionContext.offsets || "7,8,9"
      };
    }
    if (actionId === "research_activate_bundle") {
      return {
        market: actionContext.market || inputs.market,
        cycle: actionContext.cycle || inputs.cycle,
        profile: actionContext.profile || inputs.profile,
        target: actionContext.target || inputs.target,
        bundle_label: formValues.bundle_label || actionContext.bundle_label || inputs.bundle_label,
        notes: formValues.notes || ""
      };
    }
    if (actionId === "research_backtest_run") {
      return {
        market: actionContext.market || inputs.market,
        cycle: actionContext.cycle || inputs.cycle,
        profile: actionContext.profile || inputs.profile,
        target: actionContext.target || inputs.target,
        spec: formValues.spec || actionContext.spec_name || actionContext.spec || inputs.spec,
        run_label: formValues.run_label || inputs.run_label || "planned",
        bundle_label: formValues.bundle_label || actionContext.bundle_label || inputs.bundle_label
      };
    }
    if (actionId === "research_experiment_run_suite") {
      const suiteMode = formValues.suite_mode || "existing";
      return {
        market: actionContext.market || inputs.market,
        cycle: actionContext.cycle || inputs.cycle,
        profile: actionContext.profile || inputs.profile,
        model_family: formValues.model_family || actionContext.model_family || inputs.model_family,
        target: formValues.target || actionContext.target || inputs.target,
        suite_mode: suiteMode,
        suite: formValues.suite || actionContext.suite_name || actionContext.suite || inputs.suite,
        run_label: formValues.run_label || inputs.run_label || "planned",
        window_start: formValues.window_start || actionContext.window_start || windowBounds.start || "",
        window_end: formValues.window_end || actionContext.window_end || windowBounds.end || "",
        markets: formValues.markets || actionContext.market || inputs.market,
        run_name: formValues.run_name || "feature_set_matrix",
        group_name: formValues.group_name || "core",
        feature_set_variants: formValues.feature_set_variants || "",
        stakes_usd: formValues.stakes_usd || "",
        max_notional_usd: formValues.max_notional_usd || "",
        parallel_case_workers: formValues.parallel_case_workers || "",
        reference_variant_labels: formValues.reference_variant_labels || "",
        completed_cases: formValues.completed_cases || "resume",
        failed_cases: formValues.failed_cases || "rerun",
        offsets: formValues.offsets || actionContext.offsets || "7,8,9",
        feature_set: formValues.feature_set || actionContext.feature_set || "deep_otm_v1",
        label_set: formValues.label_set || actionContext.label_set || "truth",
        backtest_spec: formValues.backtest_spec || actionContext.spec_name || actionContext.spec || inputs.spec,
        variant_label: formValues.variant_label || "",
        variant_notes: formValues.variant_notes || ""
      };
    }
    return {};
  }

  function extractActionContext(row) {
    if (!row || typeof row !== "object") {
      return {};
    }
    const raw = row.action_context && typeof row.action_context === "object" ? row.action_context : row;
    const context = {};
    [
      "market",
      "cycle",
      "surface",
      "profile",
      "target",
      "model_family",
      "feature_set",
      "label_set",
      "label_source",
      "bundle_label",
      "spec_name",
      "suite_name",
      "run_label",
      "window",
      "window_start",
      "window_end",
      "offsets",
      "source_training_run",
      "source_training_run_dir",
      "run_dir",
      "bundle_dir",
      "dataset_name",
      "kind",
      "status",
      "location"
    ].forEach((key) => {
      if (raw[key] !== undefined) {
        context[key] = raw[key];
      }
    });
    return context;
  }

  function parseWindowBounds(windowLabel) {
    const token = String(windowLabel || "").trim();
    const match = token.match(/^(\\d{4}-\\d{2}-\\d{2})_(\\d{4}-\\d{2}-\\d{2})$/);
    if (!match) {
      return { start: "", end: "" };
    }
    return { start: match[1], end: match[2] };
  }

  function renderActionResult(context, payload) {
    if (!context) {
      return;
    }
    const summaryNode = context.summaryNode;
    const drilldownNode = context.drilldownNode;
    const commandNode = context.commandNode;
    const parsedNode = context.parsedNode;
    const logsNode = context.logsNode;

    if (summaryNode) {
      summaryNode.innerHTML = "";
    }
    if (drilldownNode) {
      drilldownNode.innerHTML = "";
    }
    if (commandNode) {
      commandNode.textContent = "";
    }
    if (parsedNode) {
      parsedNode.textContent = "";
    }
    if (logsNode) {
      logsNode.textContent = "";
    }

    if (!payload) {
      if (summaryNode) {
        const empty = document.createElement("div");
        empty.className = "console-action-result-empty";
        empty.textContent = "当前还没有执行过动作。";
        summaryNode.appendChild(empty);
      }
      return;
    }

    if (summaryNode) {
      const status = actionResultStatusValue(payload);
      const lead = document.createElement("div");
      lead.className = "console-action-result-note " + actionResultToneClass(status);
      lead.textContent = actionResultLeadCopy(context.sectionId, payload);
      summaryNode.appendChild(lead);
      const executionSummary = payload.execution_summary || {};
      const resultPayload = actionResultDataPayload(payload);
      const subject = String((payload || {}).subject_summary || "").trim() || actionRequestSubject(context.sectionId, payload);
      const primaryOutput = actionResultPrimaryOutput(payload);
      const linkedObject = linkedObjectSummary(payload);
      const linkedObjectDetail = linkedObjectDetailSummary(payload);
      const errorDetail = errorDetailSummary(payload);
      const heartbeat = progressHeartbeatText((payload || {}).progress || {});
      [
        ["section", context.sectionId],
        ["status", displayStatus(payload.status_label || status)],
        ["task_id", payload.task_id],
        ["action_id", payload.action_id],
        ["subject", subject],
        ["stage", (payload.progress || {}).current_stage],
        ["progress", translateProgressText(payload.progress_summary || progressSnapshotText(payload.progress || {}))],
        ["heartbeat", heartbeat],
        ["started_at", payload.started_at],
        ["result", payload.result_summary || (resultPayload && typeof resultPayload === "object" ? resultPayload.dataset : undefined)],
        ["linked_object", linkedObject],
        ["linked_object_detail", linkedObjectDetail],
        [primaryOutput.label, primaryOutput.value],
        ["return_code", payload.return_code],
        ["updated_at", payload.updated_at],
        ["finished_at", payload.finished_at],
        ["parsed_stdout", executionSummary.has_parsed_stdout],
        ["error", payload.error_summary || actionErrorSummary(payload)],
        ["error_detail", errorDetail]
      ]
        .forEach(([label, value]) => {
          if (value === undefined || value === null || value === "") {
            return;
          }
          const article = document.createElement("article");
          article.className = "console-card";
          article.innerHTML =
            '<h3 class="console-card-title">' + escapeHtml(displayLabel(label)) + '</h3>' +
            '<p class="console-card-copy">' + escapeHtml(stringifyValue(value)) + '</p>';
          summaryNode.appendChild(article);
        });
    }
    if (drilldownNode) {
      renderActionResultDrilldown(drilldownNode, payload);
    }
    if (commandNode) {
      commandNode.textContent = actionResultCommandText(payload);
    }
    if (parsedNode) {
      const resultPayload = actionResultDataPayload(payload);
      parsedNode.textContent = resultPayload === null ? "" : JSON.stringify(resultPayload, null, 2);
    }
    if (logsNode) {
      logsNode.textContent = JSON.stringify(
        {
          request: actionRequestPayload(payload),
          request_summary: payload.request_summary || {},
          stdout: payload.stdout || "",
          stderr: payload.stderr || "",
          execution_summary: payload.execution_summary || {},
          progress: payload.progress || null,
          progress_summary: payload.progress_summary || null,
          linked_objects: payload.linked_objects || [],
          linked_object_details: payload.linked_object_details || [],
          result_summary: payload.result_summary || null,
          result_paths: payload.result_paths || [],
          result: payload.result || null,
          error_summary: payload.error_summary || null,
          error_detail: payload.error_detail || null,
          error: payload.error || null
        },
        null,
        2
      );
    }
  }

  function operatorSummaryPairs(summary) {
    if (!summary || typeof summary !== "object") {
      return [];
    }
    return Object.keys(summary).slice(0, 4).map((key) => {
      return displayLabel(key) + "=" + stringifyValue(summary[key]);
    });
  }

  function renderOperatorCard(node, options) {
    if (!node) {
      return;
    }
    const title = String((options || {}).title || "").trim();
    const copy = String((options || {}).copy || "").trim();
    const tone = String((options || {}).tone || "").trim();
    const items = Array.isArray((options || {}).items) ? options.items : [];
    if (!title || !items.length) {
      return;
    }
    const article = document.createElement("article");
    article.className = "console-operator-card";
    if (tone) {
      article.classList.add(tone);
    }
    article.innerHTML =
      '<h3 class="console-operator-card-title">' + escapeHtml(title) + '</h3>' +
      (copy ? ('<p class="console-operator-card-copy">' + escapeHtml(copy) + '</p>') : "");
    const list = document.createElement("div");
    list.className = "console-operator-list";
    items.forEach((item) => {
      const label = String((item || {}).label || "").trim();
      const value = (item || {}).value;
      if (!label || value === undefined || value === null || String(value).trim() === "") {
        return;
      }
      const row = document.createElement("div");
      row.className = "console-operator-item";
      const labelNode = document.createElement("span");
      labelNode.className = "console-operator-label";
      labelNode.textContent = displayLabel(label);
      row.appendChild(labelNode);
      if ((item || {}).code) {
        const codeNode = document.createElement("pre");
        codeNode.className = "console-operator-code";
        codeNode.textContent = stringifyValue(value);
        row.appendChild(codeNode);
      } else {
        const valueNode = document.createElement("p");
        valueNode.className = "console-operator-value";
        valueNode.textContent = stringifyValue(value);
        row.appendChild(valueNode);
      }
      list.appendChild(row);
    });
    if (!list.childNodes.length) {
      return;
    }
    article.appendChild(list);
    node.appendChild(article);
  }

  function renderMetricCardGrid(node, options) {
    if (!node) {
      return;
    }
    const title = String((options || {}).title || "").trim();
    const copy = String((options || {}).copy || "").trim();
    const cards = Array.isArray((options || {}).cards) ? options.cards : [];
    const normalizedCards = cards.filter((card) => {
      const label = String((card || {}).label || "").trim();
      const value = (card || {}).value;
      return Boolean(label) && value !== undefined && value !== null && String(value).trim() !== "";
    });
    if (!title || !normalizedCards.length) {
      return;
    }
    const article = document.createElement("article");
    article.className = "console-operator-card";
    article.innerHTML =
      '<h3 class="console-operator-card-title">' + escapeHtml(title) + '</h3>' +
      (copy ? ('<p class="console-operator-card-copy">' + escapeHtml(copy) + '</p>') : "");
    const grid = document.createElement("div");
    grid.className = "console-grid";
    normalizedCards.forEach((card) => {
      const item = document.createElement("article");
      item.className = "console-card";
      item.innerHTML =
        '<h3 class="console-card-title">' + escapeHtml(displayLabel(card.label)) + '</h3>' +
        '<p class="console-card-copy">' + escapeHtml(stringifyValue(card.value)) + '</p>';
      grid.appendChild(item);
    });
    article.appendChild(grid);
    node.appendChild(article);
  }

  function renderDetailFilterBar(node, options) {
    if (!node) {
      return;
    }
    const sectionId = normalizeSection((options || {}).sectionId);
    const payload = options && options.payload;
    const title = String((options || {}).title || "").trim();
    const copy = String((options || {}).copy || "").trim();
    const fields = Array.isArray((options || {}).fields) ? options.fields : [];
    if (!title || !fields.length) {
      return;
    }
    const state = detailFilterState(sectionId);
    const article = document.createElement("article");
    article.className = "console-operator-card console-detail-controls";
    article.innerHTML =
      '<h3 class="console-operator-card-title">' + escapeHtml(title) + '</h3>' +
      (copy ? ('<p class="console-operator-card-copy">' + escapeHtml(copy) + '</p>') : "");
    const grid = document.createElement("div");
    grid.className = "console-detail-filter-grid";

    const applyFilterPatch = function (patch) {
      updateDetailFilterState(sectionId, patch);
      renderSectionDetail({ node: node, sectionId: sectionId }, payload);
    };

    fields.forEach((field) => {
      const fieldId = String((field || {}).fieldId || "").trim();
      if (!fieldId) {
        return;
      }
      const label = String((field || {}).label || fieldId).trim();
      const inputType = String((field || {}).inputType || "text").trim() || "text";
      const wrapper = document.createElement("label");
      wrapper.className = "console-form-field";
      const labelNode = document.createElement("span");
      labelNode.className = "console-field-label";
      labelNode.textContent = label;
      const input = document.createElement("input");
      input.className = "console-input";
      input.type = inputType;
      input.value = state[fieldId] !== undefined && state[fieldId] !== null ? String(state[fieldId]) : "";
      if (field.placeholder) {
        input.placeholder = String(field.placeholder);
      }
      if (field.min !== undefined) {
        input.min = String(field.min);
      }
      if (field.max !== undefined) {
        input.max = String(field.max);
      }
      const commit = function () {
        const nextValue = inputType === "number"
          ? Math.max(Number(field.min || 1), Number(input.value || field.min || 1))
          : String(input.value || "").trim();
        applyFilterPatch({ [fieldId]: nextValue });
      };
      input.addEventListener("change", commit);
      input.addEventListener("keydown", function (event) {
        if (event.key === "Enter") {
          event.preventDefault();
          commit();
        }
      });
      wrapper.appendChild(labelNode);
      wrapper.appendChild(input);
      grid.appendChild(wrapper);
    });

    const resetWrap = document.createElement("div");
    resetWrap.className = "console-form-field";
    const resetLabel = document.createElement("span");
    resetLabel.className = "console-field-label";
    resetLabel.textContent = "操作";
    const resetButton = document.createElement("button");
    resetButton.type = "button";
    resetButton.className = "console-button";
    resetButton.textContent = "清除筛选";
    resetButton.addEventListener("click", function () {
      detailFilterStateBySection[sectionId] = defaultDetailFilterState(sectionId);
      renderSectionDetail({ node: node, sectionId: sectionId }, payload);
    });
    resetWrap.appendChild(resetLabel);
    resetWrap.appendChild(resetButton);
    grid.appendChild(resetWrap);

    article.appendChild(grid);
    node.appendChild(article);
  }

  function chartNumericValue(value) {
    if (value === undefined || value === null || value === "") {
      return null;
    }
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function chartSeriesRows(rows, options) {
    const xKey = String((options || {}).xKey || "").trim();
    const yKey = String((options || {}).yKey || "").trim();
    if (!xKey || !yKey) {
      return [];
    }
    return (Array.isArray(rows) ? rows : [])
      .map((row) => {
        const xLabel = row && row[xKey] !== undefined && row[xKey] !== null ? String(row[xKey]) : "";
        const yValue = chartNumericValue(row && row[yKey]);
        return {
          xLabel: xLabel,
          yValue: yValue,
          row: row
        };
      })
      .filter((item) => item.xLabel && item.yValue !== null);
  }

  function renderChartLegend(container, items) {
    const rows = Array.isArray(items) ? items.filter((item) => item && item.label && item.value !== undefined && item.value !== null && String(item.value).trim() !== "") : [];
    if (!container || !rows.length) {
      return;
    }
    const legend = document.createElement("div");
    legend.className = "console-chart-legend";
    rows.forEach((item) => {
      const article = document.createElement("article");
      article.className = "console-chart-legend-item";
      article.innerHTML =
        '<span class="console-chart-legend-label">' + escapeHtml(displayLabel(item.label)) + '</span>' +
        '<span class="console-chart-legend-value">' + escapeHtml(stringifyValue(item.value)) + '</span>';
      legend.appendChild(article);
    });
    container.appendChild(legend);
  }

  function renderLineChartCard(node, options) {
    if (!node) {
      return;
    }
    const title = String((options || {}).title || "").trim();
    const copy = String((options || {}).copy || "").trim();
    const series = chartSeriesRows((options || {}).rows, options);
    if (!title || series.length < 2) {
      return;
    }
    const width = 520;
    const height = 200;
    const paddingLeft = 42;
    const paddingRight = 18;
    const paddingTop = 18;
    const paddingBottom = 28;
    const values = series.map((item) => item.yValue);
    const minValue = Math.min.apply(null, values);
    const maxValue = Math.max.apply(null, values);
    const range = maxValue - minValue;
    const xStep = series.length > 1 ? (width - paddingLeft - paddingRight) / (series.length - 1) : 0;
    const yForValue = function (value) {
      if (!range) {
        return (height - paddingBottom + paddingTop) / 2;
      }
      const normalized = (value - minValue) / range;
      return height - paddingBottom - normalized * (height - paddingTop - paddingBottom);
    };
    const points = series.map((item, index) => {
      return {
        x: paddingLeft + index * xStep,
        y: yForValue(item.yValue),
        xLabel: item.xLabel,
        yValue: item.yValue
      };
    });
    const pathData = points.map((point, index) => (index === 0 ? "M" : "L") + point.x.toFixed(1) + " " + point.y.toFixed(1)).join(" ");

    const article = document.createElement("article");
    article.className = "console-operator-card";
    article.innerHTML =
      '<h3 class="console-operator-card-title">' + escapeHtml(title) + '</h3>' +
      (copy ? ('<p class="console-operator-card-copy">' + escapeHtml(copy) + '</p>') : "");
    const chart = document.createElement("div");
    chart.className = "console-chart";
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("class", "console-chart-svg");
    svg.setAttribute("viewBox", "0 0 " + width + " " + height);
    svg.setAttribute("role", "img");
    svg.setAttribute("aria-label", title);

    const baseline = document.createElementNS("http://www.w3.org/2000/svg", "line");
    baseline.setAttribute("x1", String(paddingLeft));
    baseline.setAttribute("x2", String(width - paddingRight));
    baseline.setAttribute("y1", String(height - paddingBottom));
    baseline.setAttribute("y2", String(height - paddingBottom));
    baseline.setAttribute("stroke", "rgba(103, 95, 83, 0.45)");
    baseline.setAttribute("stroke-width", "1");
    svg.appendChild(baseline);

    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", pathData);
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", "#8a3d16");
    path.setAttribute("stroke-width", "3");
    path.setAttribute("stroke-linecap", "round");
    path.setAttribute("stroke-linejoin", "round");
    svg.appendChild(path);

    points.forEach((point) => {
      const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      circle.setAttribute("cx", point.x.toFixed(1));
      circle.setAttribute("cy", point.y.toFixed(1));
      circle.setAttribute("r", "3.5");
      circle.setAttribute("fill", "#8a3d16");
      svg.appendChild(circle);
    });
    chart.appendChild(svg);
    const caption = document.createElement("p");
    caption.className = "console-chart-caption";
    caption.textContent = "起点=" + stringifyValue(minValue) + " · 终点=" + stringifyValue(series[series.length - 1].yValue) + " · 最高=" + stringifyValue(maxValue);
    chart.appendChild(caption);
    renderChartLegend(chart, [
      { label: "起点", value: series[0].xLabel + " / " + stringifyValue(series[0].yValue) },
      { label: "终点", value: series[series.length - 1].xLabel + " / " + stringifyValue(series[series.length - 1].yValue) },
      { label: "最高", value: maxValue },
      { label: "最低", value: minValue }
    ]);
    article.appendChild(chart);
    node.appendChild(article);
  }

  function renderBarChartCard(node, options) {
    if (!node) {
      return;
    }
    const title = String((options || {}).title || "").trim();
    const copy = String((options || {}).copy || "").trim();
    const series = chartSeriesRows((options || {}).rows, options).slice(0, Number((options || {}).limit || 6));
    if (!title || !series.length) {
      return;
    }
    const width = 520;
    const height = 220;
    const paddingLeft = 52;
    const paddingRight = 18;
    const paddingTop = 18;
    const paddingBottom = 36;
    const chartWidth = width - paddingLeft - paddingRight;
    const chartHeight = height - paddingTop - paddingBottom;
    const minValue = Math.min(0, Math.min.apply(null, series.map((item) => item.yValue)));
    const maxValue = Math.max(0, Math.max.apply(null, series.map((item) => item.yValue)));
    const range = maxValue - minValue || 1;
    const zeroY = paddingTop + ((maxValue / range) * chartHeight);
    const slotWidth = chartWidth / series.length;
    const barWidth = Math.max(18, slotWidth * 0.55);

    const article = document.createElement("article");
    article.className = "console-operator-card";
    article.innerHTML =
      '<h3 class="console-operator-card-title">' + escapeHtml(title) + '</h3>' +
      (copy ? ('<p class="console-operator-card-copy">' + escapeHtml(copy) + '</p>') : "");
    const chart = document.createElement("div");
    chart.className = "console-chart";
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("class", "console-chart-svg");
    svg.setAttribute("viewBox", "0 0 " + width + " " + height);
    svg.setAttribute("role", "img");
    svg.setAttribute("aria-label", title);

    const baseline = document.createElementNS("http://www.w3.org/2000/svg", "line");
    baseline.setAttribute("x1", String(paddingLeft));
    baseline.setAttribute("x2", String(width - paddingRight));
    baseline.setAttribute("y1", zeroY.toFixed(1));
    baseline.setAttribute("y2", zeroY.toFixed(1));
    baseline.setAttribute("stroke", "rgba(103, 95, 83, 0.45)");
    baseline.setAttribute("stroke-width", "1");
    svg.appendChild(baseline);

    series.forEach((item, index) => {
      const x = paddingLeft + index * slotWidth + (slotWidth - barWidth) / 2;
      const barHeight = Math.abs(item.yValue / range) * chartHeight;
      const y = item.yValue >= 0 ? zeroY - barHeight : zeroY;
      const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      rect.setAttribute("x", x.toFixed(1));
      rect.setAttribute("y", y.toFixed(1));
      rect.setAttribute("width", barWidth.toFixed(1));
      rect.setAttribute("height", Math.max(barHeight, 2).toFixed(1));
      rect.setAttribute("rx", "6");
      rect.setAttribute("fill", item.yValue >= 0 ? "#8a3d16" : "#b65a34");
      svg.appendChild(rect);
    });
    chart.appendChild(svg);
    const caption = document.createElement("p");
    caption.className = "console-chart-caption";
    caption.textContent = "最高=" + stringifyValue(maxValue) + " · 最低=" + stringifyValue(minValue) + " · 样本=" + String(series.length);
    chart.appendChild(caption);
    renderChartLegend(chart, series.slice(0, 4).map((item) => ({
      label: item.xLabel,
      value: item.yValue
    })));
    article.appendChild(chart);
    node.appendChild(article);
  }

  function buildPreviewTableCardElement(options) {
    const rows = Array.isArray((options || {}).rows) ? options.rows.filter((row) => row && typeof row === "object") : [];
    if (!rows.length) {
      return null;
    }
    const requestedColumns = Array.isArray((options || {}).columns) ? options.columns : [];
    const columns = requestedColumns.length
      ? requestedColumns.filter((column) => rows.some((row) => row[column] !== undefined && row[column] !== null && String(row[column]).trim() !== ""))
      : Array.from(rows.reduce((set, row) => {
          Object.keys(row || {}).slice(0, 8).forEach((key) => set.add(key));
          return set;
        }, new Set())).slice(0, 6);
    if (!columns.length) {
      return null;
    }
    const title = String((options || {}).title || "").trim();
    const copy = String((options || {}).copy || "").trim();
    if (!title) {
      return null;
    }

    const article = document.createElement("article");
    article.className = "console-operator-card";
    article.innerHTML =
      '<h3 class="console-operator-card-title">' + escapeHtml(title) + '</h3>' +
      (copy ? ('<p class="console-operator-card-copy">' + escapeHtml(copy) + '</p>') : "");

    const wrap = document.createElement("div");
    wrap.className = "console-table-wrap";
    const table = document.createElement("table");
    table.className = "console-table";
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    const tableKey = String((options || {}).tableKey || ("preview:" + title));
    const sortState = tableSortStateValue(tableKey, options);
    columns.forEach((column) => {
      const th = document.createElement("th");
      th.classList.add("is-sortable");
      th.innerHTML =
        '<span>' + escapeHtml(displayLabel(column)) + '</span>' +
        '<span class="console-table-sort">' + escapeHtml(sortIndicator(sortState, column)) + '</span>';
      th.addEventListener("click", function () {
        updateTableSortState(tableKey, column, options);
        const next = buildPreviewTableCardElement(options);
        if (next) {
          article.replaceWith(next);
        }
      });
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    sortedRows(rows, sortState).slice(0, Number((options || {}).limit || 5)).forEach((row) => {
      const tr = document.createElement("tr");
      columns.forEach((column) => {
        const td = document.createElement("td");
        td.textContent = stringifyValue(row[column]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    wrap.appendChild(table);
    article.appendChild(wrap);
    return article;
  }

  function renderPreviewTableCard(node, options) {
    if (!node) {
      return;
    }
    const article = buildPreviewTableCardElement(options);
    if (!article) {
      return;
    }
    node.appendChild(article);
  }

  function renderActionResultDrilldown(node, payload) {
    if (!node) {
      return;
    }
    node.innerHTML = "";
    const resultPaths = Array.isArray((payload || {}).result_paths) ? payload.result_paths : [];
    const linkedDetails = Array.isArray((payload || {}).linked_object_details) ? payload.linked_object_details : [];
    const errorDetail = (payload && typeof payload.error_detail === "object") ? payload.error_detail : {};

    renderOperatorCard(node, {
      title: "结果路径",
      copy: "从任务结果载荷中提取出的标准产物路径。",
      items: resultPaths.map((item) => ({
        label: String((item || {}).label || "path"),
        value: String((item || {}).path || "").trim()
      }))
    });

    renderOperatorCard(node, {
      title: "关联对象",
      copy: "根据结果产物反查出的标准对象详情。",
      items: linkedDetails.map((item) => {
        const parts = [];
        const identity = String((item || {}).identity || "").trim();
        const path = String((item || {}).path || "").trim();
        operatorSummaryPairs(item && item.summary).forEach((entry) => parts.push(entry));
        return {
          label: [String((item || {}).title || (item || {}).object_type || "").trim(), identity].filter(Boolean).join(" | "),
          value: [path, parts.join(" · ")].filter(Boolean).join("\\n"),
          code: parts.length > 0 || Boolean(path)
        };
      })
    });

    const errorItems = [
      { label: "type", value: errorDetail.type },
      { label: "message", value: errorDetail.message },
      { label: "result_status", value: errorDetail.result_status },
      { label: "return_code", value: errorDetail.return_code },
      { label: "last_stderr_line", value: errorDetail.last_stderr_line },
      {
        label: "stderr_excerpt",
        value: Array.isArray(errorDetail.stderr_excerpt) ? errorDetail.stderr_excerpt.join("\\n") : "",
        code: true,
      },
    ];
    renderOperatorCard(node, {
      title: "错误钻取",
      copy: "从任务错误载荷和 stderr 摘要中整理出的结构化失败上下文。",
      tone: Object.keys(errorDetail).length ? "is-failed" : "",
      items: errorItems
    });
  }

  function recentTasksContextForSection(sectionId) {
    const panel = panels.find((node) => node.dataset.sectionId === normalizeSection(sectionId));
    if (!panel) {
      return null;
    }
    const node = panel.querySelector("[data-console-recent-tasks]");
    if (!node) {
      return null;
    }
    return {
      sectionId: normalizeSection(sectionId),
      node: node,
      statusNode: panel.querySelector("[data-console-status]"),
      jsonNode: panel.querySelector("[data-console-json]"),
      actionResultContext: {
        sectionId: normalizeSection(sectionId),
        panel: panel,
        summaryNode: panel.querySelector("[data-console-action-result-summary]"),
        drilldownNode: panel.querySelector("[data-console-action-drilldown]"),
        commandNode: panel.querySelector("[data-console-action-result-command]"),
        parsedNode: panel.querySelector("[data-console-action-result-parsed]"),
        logsNode: panel.querySelector("[data-console-action-result-logs]")
      }
    };
  }

  function refreshRecentTasks(sectionId, options) {
    const context = recentTasksContextForSection(sectionId);
    if (!context) {
      return;
    }
    loadRecentTasks(context, options).catch(function () {
      renderRecentTasks(context, null, false);
    });
  }

  async function loadRecentTasks(context, options) {
    if (!context || !context.node) {
      return;
    }
    const silent = Boolean(options && options.silent);
    const actionIds = sectionTaskActionIds(context.sectionId);
    if (!actionIds.length) {
      renderRecentTasks(context, { rows: [], row_count: 0, status_counts: {} }, true);
      return;
    }
    const loadingPayload = recentTasksBySection[context.sectionId] || null;
    if (!silent) {
      renderRecentTasks(context, loadingPayload, true, { loading: true });
    }
    const response = await fetch(tasksListPath(actionIds, 6));
    const payload = await response.json();
    recentTasksBySection[context.sectionId] = payload;
    renderRecentTasks(context, payload, response.ok);
    if (response.ok) {
      syncRecentTaskPolling(context, payload);
    }
  }

  function syncRecentTaskPolling(context, payload) {
    const rows = Array.isArray((payload || {}).rows) ? payload.rows : [];
    const activeRow = rows.find((row) => isTaskActiveStatus((row || {}).status));
    if (!activeRow) {
      return;
    }
    const activeTaskId = taskIdFromPayload(activeRow);
    if (!activeTaskId) {
      return;
    }
    const currentTaskId = taskIdFromPayload(actionResultsBySection[context.sectionId] || {});
    const currentStatus = String(((actionResultsBySection[context.sectionId] || {}).status) || "").trim();
    if (currentTaskId && currentTaskId !== activeTaskId && !isTaskActiveStatus(currentStatus)) {
      return;
    }
    pollConsoleTask({
      sectionId: context.sectionId,
      taskId: activeTaskId,
      statusNode: context.statusNode,
      jsonNode: context.jsonNode,
      actionResultContext: context.actionResultContext
    });
  }

  function taskMetaValues(row) {
    const values = [];
    const taskId = String((row || {}).task_id || "").trim();
    const status = displayStatus((row || {}).status_label || (row || {}).status || "");
    const progress = translateProgressText((row || {}).progress_summary || "");
    const stage = String((((row || {}).progress || {}).current_stage) || "").trim();
    const heartbeat = progressHeartbeatText((row || {}).progress || {});
    const result = String((row || {}).result_summary || "").trim();
    const error = String((row || {}).error_summary || "").trim();
    const output = String((row || {}).primary_output_path || "").trim();
    const updatedAt = String((row || {}).updated_at || "").trim();
    [taskId, status, stage ? ("阶段=" + stage) : "", progress, heartbeat ? ("心跳=" + heartbeat) : "", result, error].forEach((value) => {
      if (value) {
        values.push(value);
      }
    });
    if (output) {
      values.push("输出=" + output);
    }
    if (updatedAt) {
      values.push(updatedAt);
    }
    return values;
  }

  function appendTaskMeta(container, row) {
    const values = taskMetaValues(row);
    if (!container || !values.length) {
      return;
    }
    const meta = document.createElement("div");
    meta.className = "console-card-meta";
    values.forEach((value) => {
      const pill = document.createElement("span");
      pill.className = "console-pill";
      pill.textContent = String(value);
      meta.appendChild(pill);
    });
    container.appendChild(meta);
  }

  function recentTaskHighlights(rows) {
    if (!Array.isArray(rows) || !rows.length) {
      return [];
    }
    const active = rows.find((row) => {
      const status = String((row || {}).status || "").trim();
      return status === "queued" || status === "running";
    });
    const failed = rows.find((row) => String((row || {}).status || "").trim() === "failed");
    const succeeded = rows.find((row) => String((row || {}).status || "").trim() === "succeeded");
    const highlights = [];
    if (active) {
      highlights.push({
        tone: "is-running",
        title: "活跃任务高亮",
        copy: "最新的排队中/运行中任务会固定在最前面，方便第一时间查看实时进度。",
        row: active
      });
    }
    if (failed) {
      highlights.push({
        tone: "is-failed",
        title: "失败任务高亮",
        copy: "最新失败任务会固定在这里，方便快速展开结构化错误详情。",
        row: failed
      });
    } else if (!active && succeeded) {
      highlights.push({
        tone: "",
        title: "最近已结束任务",
        copy: "当前没有活跃或失败任务时，最近结束的任务会固定在这里方便快速查看。",
        row: succeeded
      });
    }
    return highlights;
  }

  function buildTaskRowArticle(context, row) {
    const article = document.createElement("article");
    article.className = "console-task-row " + actionResultToneClass(actionResultStatusValue(row));
    const taskId = String((row || {}).task_id || "").trim();
    if (taskId) {
      article.tabIndex = 0;
      article.setAttribute("role", "button");
      article.setAttribute("aria-label", "打开任务 " + taskId);
      article.addEventListener("click", function () {
        openRecentTaskDrilldown(context, row);
      });
      article.addEventListener("keydown", function (event) {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          openRecentTaskDrilldown(context, row);
        }
      });
    }
    article.innerHTML =
      '<h3 class="console-task-row-title">' + escapeHtml(taskRowHeadline(context.sectionId, row)) + '</h3>' +
      '<p class="console-task-row-copy">' + escapeHtml(taskRowCopy(context.sectionId, row)) + '</p>';
    appendTaskMeta(article, row);
    return article;
  }

  function renderRecentTasks(context, payload, ok, options) {
    const node = context && context.node;
    if (!node) {
      return;
    }
    const loading = Boolean(options && options.loading);
    node.innerHTML = "";
    if (loading) {
      const summary = document.createElement("div");
      summary.className = "console-task-history-summary";
      summary.textContent = "正在加载最近任务...";
      node.appendChild(summary);
      if (!payload || !Array.isArray(payload.rows) || !payload.rows.length) {
        return;
      }
    }
    if (!ok) {
      const summary = document.createElement("div");
      summary.className = "console-task-history-summary";
      summary.textContent = "加载最近任务失败。";
      node.appendChild(summary);
      return;
    }
    const rows = Array.isArray((payload || {}).rows) ? payload.rows : [];
    const summary = document.createElement("div");
    summary.className = "console-task-history-summary";
    summary.textContent = taskHistoryLeadCopy(context.sectionId, payload);
    node.appendChild(summary);
    if (!rows.length) {
      const empty = document.createElement("div");
      empty.className = "console-empty";
      empty.textContent = "当前分区还没有记录到最近任务。";
      node.appendChild(empty);
      return;
    }
    const list = document.createElement("div");
    list.className = "console-task-history-list";
    const highlights = recentTaskHighlights(rows);
    if (highlights.length) {
      const focus = document.createElement("div");
      focus.className = "console-task-history-focus";
      highlights.forEach((entry) => {
        const card = document.createElement("section");
        card.className = "console-operator-card";
        if (entry.tone) {
          card.classList.add(entry.tone);
        }
        card.innerHTML =
          '<h3 class="console-operator-card-title">' + escapeHtml(entry.title) + '</h3>' +
          '<p class="console-operator-card-copy">' + escapeHtml(entry.copy) + '</p>';
        card.appendChild(buildTaskRowArticle(context, entry.row));
        focus.appendChild(card);
      });
      node.appendChild(focus);
    }
    rows.slice(0, 6).forEach((row) => {
      list.appendChild(buildTaskRowArticle(context, row));
    });
    node.appendChild(list);
  }

  async function openRecentTaskDrilldown(context, row) {
    const taskId = String((row || {}).task_id || "").trim();
    if (!taskId) {
      return;
    }
    if (context.statusNode) {
      context.statusNode.textContent = "正在加载任务 " + taskId + " 详情...";
    }
    try {
      const response = await fetch(taskStatusPath(taskId));
      const payload = await response.json();
      actionResultsBySection[context.sectionId] = payload;
      if (context.actionResultContext) {
        renderActionResult(context.actionResultContext, payload);
      }
      if (context.jsonNode) {
        context.jsonNode.textContent = JSON.stringify(payload, null, 2);
      }
      if (context.statusNode) {
        context.statusNode.textContent = taskDrilldownStatusSummary(payload);
      }
      if (response.ok && ["queued", "running"].includes(String(payload.status || ""))) {
        pollConsoleTask({
          sectionId: context.sectionId,
          taskId: taskId,
          statusNode: context.statusNode,
          jsonNode: context.jsonNode,
          actionResultContext: context.actionResultContext
        });
      }
    } catch (error) {
      if (context.statusNode) {
        context.statusNode.textContent = "加载任务详情失败。";
      }
      if (context.jsonNode) {
        context.jsonNode.textContent = JSON.stringify({ error: String(error), task_id: taskId }, null, 2);
      }
    }
  }

  function actionButtonLabel(actionId) {
    if (actionId === "data_refresh_summary") {
      return "刷新摘要";
    }
    if (actionId === "data_sync") {
      return "执行数据同步";
    }
    if (actionId === "data_build") {
      return "执行数据构建";
    }
    if (actionId === "research_train_run") {
      return "启动训练";
    }
    if (actionId === "research_bundle_build") {
      return "构建模型包";
    }
    if (actionId === "research_activate_bundle") {
      return "激活模型包";
    }
    if (actionId === "research_backtest_run") {
      return "运行回测";
    }
    if (actionId === "research_experiment_run_suite") {
      return "运行实验套件";
    }
    return "执行动作";
  }

  function buildTable(rows, options) {
    const sectionId = normalizeSection((options || {}).sectionId);
    const onSelect = typeof (options || {}).onSelect === "function" ? options.onSelect : null;
    const payloadActionContext = (options || {}).payloadActionContext || {};
    const tableKey = String((options || {}).tableKey || ("list:" + sectionId));
    let selectedKey = rowSelectionKey(selectedRowsBySection[sectionId] || null);
    if (selectedKey && !rows.some((row) => rowSelectionKey(row) === selectedKey)) {
      selectedRowsBySection[sectionId] = null;
      selectedRowContextBySection[sectionId] = {};
      selectedKey = "";
    }
    const table = document.createElement("table");
    table.className = "console-table";
    const keys = Array.from(rows.reduce((set, row) => {
      Object.keys(row || {})
        .filter((key) => !["summary", "manifest", "action_context", "artifacts"].includes(key))
        .slice(0, 8)
        .forEach((key) => set.add(key));
      return set;
    }, new Set())).slice(0, 6);
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    const sortState = tableSortStateValue(tableKey, options);
    keys.forEach((key) => {
      const th = document.createElement("th");
      th.classList.add("is-sortable");
      th.innerHTML =
        '<span>' + escapeHtml(displayLabel(key)) + '</span>' +
        '<span class="console-table-sort">' + escapeHtml(sortIndicator(sortState, key)) + '</span>';
      th.addEventListener("click", function () {
        updateTableSortState(tableKey, key, options);
        const nextTable = buildTable(rows, options);
        table.replaceWith(nextTable);
      });
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);
    const tbody = document.createElement("tbody");
    sortedRows(rows, sortState).slice(0, 12).forEach((row, index) => {
      const tr = document.createElement("tr");
      tr.classList.toggle("is-selected", rowSelectionKey(row) === selectedKey);
      if (onSelect) {
        tr.addEventListener("click", function () {
          Array.from(tbody.querySelectorAll("tr")).forEach((node) => node.classList.remove("is-selected"));
          tr.classList.add("is-selected");
          onSelect(row);
          if (options && options.statusNode) {
            options.statusNode.textContent = rowSelectionStatusCopy(sectionId, row);
          }
        });
      }
      keys.forEach((key) => {
        const td = document.createElement("td");
        td.textContent = stringifyValue((row || {})[key]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
      if (!selectedKey && index === 0 && onSelect) {
        selectedRowsBySection[sectionId] = row;
        selectedRowContextBySection[sectionId] = actionContextFromRow(sectionId, row, payloadActionContext);
        selectedKey = rowSelectionKey(row);
        tr.classList.add("is-selected");
        onSelect(row);
      }
    });
    table.appendChild(tbody);
    return table;
  }

  function rowSelectionKey(row) {
    const actionContext = extractActionContext(row);
    return stringifyValue(
      actionContext.run_dir ||
      actionContext.bundle_dir ||
      actionContext.run_label ||
      actionContext.bundle_label ||
      actionContext.suite_name ||
      actionContext.dataset_name ||
      actionContext.location ||
      (row && row.dataset_name) ||
      (row && row.location) ||
      (row && row.path) ||
      ""
    );
  }

  function stringifyValue(value) {
    if (value === null || value === undefined) {
      return "";
    }
    if (typeof value === "boolean") {
      return value ? "是" : "否";
    }
    if (Array.isArray(value)) {
      const primitive = value.every((item) => item === null || ["string", "number", "boolean"].includes(typeof item));
      return primitive ? value.map((item) => stringifyValue(item)).join(", ") : JSON.stringify(value);
    }
    if (typeof value === "object") {
      return JSON.stringify(value);
    }
    return String(value);
  }

  function sortableValue(value) {
    if (value === undefined || value === null || value === "") {
      return { kind: "empty", value: "" };
    }
    const number = Number(value);
    if (Number.isFinite(number)) {
      return { kind: "number", value: number };
    }
    return { kind: "text", value: String(value).toLowerCase() };
  }

  function sortedRows(rows, sortState) {
    const items = Array.isArray(rows) ? rows.slice() : [];
    const column = String((sortState || {}).column || "").trim();
    if (!column) {
      return items;
    }
    const direction = String((sortState || {}).direction || "desc") === "asc" ? 1 : -1;
    return items.sort((left, right) => {
      const a = sortableValue(left && left[column]);
      const b = sortableValue(right && right[column]);
      if (a.kind === "empty" && b.kind !== "empty") {
        return 1;
      }
      if (b.kind === "empty" && a.kind !== "empty") {
        return -1;
      }
      if (a.value < b.value) {
        return -1 * direction;
      }
      if (a.value > b.value) {
        return 1 * direction;
      }
      return 0;
    });
  }

  function sortIndicator(sortState, column) {
    if (!sortState || String(sortState.column || "") !== String(column || "")) {
      return "↕";
    }
    return sortState.direction === "asc" ? "↑" : "↓";
  }

  function applyDetailRowFilter(rows, sectionId, options) {
    const state = detailFilterState(sectionId);
    const keys = Array.isArray((options || {}).keys) ? options.keys : [];
    const limit = Math.max(1, Number(state.topN || 5));
    const section = normalizeSection(sectionId);
    const query = String(state.search || "").trim().toLowerCase();
    const marketQuery = String(state.market || "").trim().toLowerCase();
    const runQuery = String(state.runName || "").trim().toLowerCase();
    const variantQuery = String(state.variant || "").trim().toLowerCase();
    const filtered = (Array.isArray(rows) ? rows : []).filter((row) => {
      if (!row || typeof row !== "object") {
        return false;
      }
      if (section === "experiments") {
        if (marketQuery && !stringifyValue(row.market).toLowerCase().includes(marketQuery)) {
          return false;
        }
        if (runQuery && !stringifyValue(row.run_name).toLowerCase().includes(runQuery)) {
          return false;
        }
        if (variantQuery && !stringifyValue(row.variant_label).toLowerCase().includes(variantQuery)) {
          return false;
        }
        return true;
      }
      if (!query) {
        return true;
      }
      const values = (keys.length ? keys : Object.keys(row)).map((key) => stringifyValue(row[key]).toLowerCase());
      return values.some((value) => value.includes(query));
    });
    return filtered.slice(0, limit);
  }

  function actionRequestPayload(payload) {
    if (!payload || typeof payload !== "object") {
      return {};
    }
    if (payload.request && typeof payload.request === "object") {
      return payload.request;
    }
    if (payload.request_summary && typeof payload.request_summary === "object") {
      return payload.request_summary;
    }
    if (payload.normalized_request && typeof payload.normalized_request === "object") {
      return payload.normalized_request;
    }
    return {};
  }

  function actionRequestSubject(sectionId, payload) {
    const request = actionRequestPayload(payload);
    const section = normalizeSection(sectionId);
    if (section === "bundles") {
      const bundleLabel = String(request.bundle_label || "").trim();
      const sourceRun = String(request.source_training_run || "").trim();
      if (bundleLabel && sourceRun) {
        return "模型包 " + bundleLabel + "，来源训练运行 " + sourceRun;
      }
      if (bundleLabel) {
        return "模型包 " + bundleLabel;
      }
    }
    if (section === "experiments") {
      const suite = String(request.suite || request.suite_name || "").trim();
      const runLabel = String(request.run_label || "").trim();
      const suiteMode = String(request.suite_mode || "").trim();
      if (suite && runLabel) {
        return (suiteMode === "inline" ? "内联实验套件 " : "实验套件 ") + suite + " / 运行 " + runLabel;
      }
      if (suite) {
        return (suiteMode === "inline" ? "内联实验套件 " : "实验套件 ") + suite;
      }
    }
    if (section === "backtests") {
      const spec = String(request.spec || request.spec_name || "").trim();
      const runLabel = String(request.run_label || "").trim();
      if (spec && runLabel) {
        return "回测规格 " + spec + " / 运行 " + runLabel;
      }
    }
    if (section === "training_runs") {
      const runLabel = String(request.run_label || "").trim();
      if (runLabel) {
        return "训练运行 " + runLabel;
      }
    }
    return "";
  }

  function actionResultStatusValue(payload) {
    const status = String((payload || {}).status || ((payload || {}).execution_summary || {}).status || "").trim();
    return status || "unknown";
  }

  function actionResultToneClass(status) {
    if (status === "queued" || status === "running") {
      return "is-running";
    }
    if (status === "succeeded" || status === "ok") {
      return "is-complete";
    }
    if (status === "failed" || status === "error") {
      return "is-failed";
    }
    return "";
  }

  function progressSnapshotText(progress) {
    if (!progress || typeof progress !== "object") {
      return "";
    }
    const parts = [];
    if (progress.summary) {
      parts.push(translateProgressText(progress.summary));
    }
    if (progress.current_stage) {
      parts.push("阶段=" + String(progress.current_stage));
    }
    if (progress.current !== undefined && progress.current !== null && progress.total !== undefined && progress.total !== null) {
      parts.push(String(progress.current) + "/" + String(progress.total));
    }
    if (progress.progress_pct !== undefined && progress.progress_pct !== null) {
      parts.push(String(progress.progress_pct) + "%");
    }
    const heartbeat = progressHeartbeatText(progress);
    if (heartbeat) {
      parts.push("心跳=" + heartbeat);
    }
    return parts.join(" · ");
  }

  function actionErrorSummary(payload) {
    const error = (payload || {}).error;
    if (!error) {
      return "";
    }
    if (typeof error === "string") {
      return error;
    }
    if (error.type && error.message) {
      return String(error.type) + ": " + String(error.message);
    }
    if (error.message) {
      return String(error.message);
    }
    return JSON.stringify(error);
  }

  function taskHistoryLeadCopy(sectionId, payload) {
    const rows = Array.isArray((payload || {}).rows) ? payload.rows : [];
    const counts = payload && typeof payload.status_counts === "object" ? payload.status_counts : {};
    const parts = [];
    const runtimeSummary = runtimeSummaryForSection(sectionId);
    const runtimeCounts = runtimeSummary && typeof runtimeSummary.status_group_counts === "object"
      ? runtimeSummary.status_group_counts
      : {};
    Object.keys(counts || {}).sort().forEach((key) => {
      parts.push(displayLabel(key) + "=" + String(counts[key]));
    });
    if (normalizeSection(sectionId) === "home") {
      ["active", "terminal", "failed"].forEach((key) => {
        if (runtimeCounts[key] !== undefined) {
          parts.push(displayLabel(key) + "=" + String(runtimeCounts[key]));
        }
      });
      if ((runtimeSummary || {}).latest_failed_task_id) {
        parts.push(displayLabel("latest_failed_task") + "=" + String(runtimeSummary.latest_failed_task_id));
      }
    }
    if (!rows.length) {
      return sectionLabel(sectionId) + "还没有记录到最近任务。";
    }
    return "共展示 " + String(rows.length) + " 条最近任务" + (parts.length ? ("：" + parts.join(" · ")) : "。") +
      (normalizeSection(sectionId) === "home" ? " 点击卡片可展开操作钻取。" : "");
  }

  function actionTitle(actionId) {
    const catalog = state.actionCatalog;
    const actions = catalog && Array.isArray(catalog.actions) ? catalog.actions : [];
    const match = actions.find((item) => String((item || {}).action_id || "") === String(actionId || ""));
    return match && match.title ? String(match.title) : String(actionId || "任务");
  }

  function taskRowHeadline(sectionId, row) {
    const actionLabel = actionTitle((row || {}).action_id);
    const subject = String((row || {}).subject_summary || "").trim() || actionRequestSubject(sectionId, row);
    const taskId = String((row || {}).task_id || "").trim();
    if (subject && taskId) {
      return actionLabel + " · " + subject + " · " + taskId;
    }
    if (subject) {
      return actionLabel + " · " + subject;
    }
    if (taskId) {
      return actionLabel + " · " + taskId;
    }
    return actionLabel;
  }

  function taskRowCopy(sectionId, row) {
    const progress = translateProgressText((row || {}).progress_summary || "");
    const progressStage = String((((row || {}).progress || {}).current_stage) || "").trim();
    const heartbeat = progressHeartbeatText((row || {}).progress || {});
    const result = String((row || {}).result_summary || "").trim();
    const error = String((row || {}).error_summary || "").trim();
    const output = String((row || {}).primary_output_path || "").trim();
    const outputLabel = String((row || {}).primary_output_label || "").trim();
    const request = taskRowRequestSummary((row || {}).request_summary || {});
    const outputText = output ? ((outputLabel ? (displayLabel(outputLabel) + "=") : "") + output) : "";
    if (error) {
      return error + (outputText ? (" | " + outputText) : "") + (request ? (" | " + request) : "");
    }
    const progressDetails = [progress, progressStage ? ("阶段=" + progressStage) : "", heartbeat ? ("心跳=" + heartbeat) : ""]
      .filter(Boolean)
      .join(" | ");
    if (progress && result) {
      return progressDetails + " | " + result + (outputText ? (" | " + outputText) : "") + (request ? (" | " + request) : "");
    }
    if (progressDetails) {
      return progressDetails + (outputText ? (" | " + outputText) : "") + (request ? (" | " + request) : "");
    }
    if (result) {
      return result + (outputText ? (" | " + outputText) : "") + (request ? (" | " + request) : "");
    }
    const subject = String((row || {}).subject_summary || "").trim() || actionRequestSubject(sectionId, row);
    if (outputText && request) {
      return outputText + " | " + request;
    }
    if (outputText) {
      return outputText;
    }
    if (subject && request) {
      return subject + " | " + request;
    }
    return request || subject || "当前还没有上报进度或结果摘要。";
  }

  function taskRowRequestSummary(requestSummary) {
    if (!requestSummary || typeof requestSummary !== "object") {
      return "";
    }
    const parts = [];
    [
      ["bundle", requestSummary.bundle_label],
      ["run", requestSummary.run_label],
      ["suite", requestSummary.suite],
      ["spec", requestSummary.spec],
      ["sync", requestSummary.sync_command],
      ["build", requestSummary.build_command]
    ].forEach((entry) => {
      if (entry[1] === undefined || entry[1] === null || String(entry[1]).trim() === "") {
        return;
      }
      parts.push(displayLabel(entry[0]) + "=" + String(entry[1]));
    });
    return parts.join(" · ");
  }

  function actionResultLeadCopy(sectionId, payload) {
    const status = actionResultStatusValue(payload);
    const progressText = String((payload || {}).progress_summary || "").trim() || progressSnapshotText((payload || {}).progress || {});
    const heartbeatText = progressHeartbeatText((payload || {}).progress || {});
    const subject = String((payload || {}).subject_summary || "").trim() || actionRequestSubject(sectionId, payload);
    const subjectText = subject ? (" 请求对象：" + subject + "。") : "";
    const drilldownSignals = [];
    if (Array.isArray((payload || {}).result_paths) && payload.result_paths.length) {
      drilldownSignals.push(String(payload.result_paths.length) + " 条结果路径");
    }
    if (Array.isArray((payload || {}).linked_object_details) && payload.linked_object_details.length) {
      drilldownSignals.push(String(payload.linked_object_details.length) + " 个关联对象");
    }
    if (errorDetailSummary(payload)) {
      drilldownSignals.push("结构化错误详情");
    }
    const drilldownText = drilldownSignals.length
      ? (" 下方可继续钻取：" + drilldownSignals.join(" · ") + "。")
      : "";
    if (status === "queued" || status === "running") {
      return "后台任务仍在运行" +
        (progressText ? ("：" + translateProgressText(progressText) + "。") : "。") +
        (heartbeatText ? (" 最近心跳：" + heartbeatText + "。") : "") +
        " 这个面板会自动轮询任务状态，直到任务进入终态并刷新当前分区。" +
        subjectText +
        drilldownText;
    }
    if (status === "succeeded") {
      const resultSummary = String((payload || {}).result_summary || "").trim();
      return "后台任务执行成功。" + (resultSummary ? (" 结果：" + resultSummary + "。") : " 下方可查看解析结果和原始任务载荷。") + subjectText + drilldownText;
    }
    if (status === "failed") {
      const error = String((payload || {}).error_summary || "").trim() || actionErrorSummary(payload);
      return "后台任务执行失败。" + (error ? (" 错误：" + error + "。") : " 重试前请先检查下方错误和日志。") + subjectText + drilldownText;
    }
    if (status === "ok") {
      return "动作已同步完成。下方展示了解析后的 stdout 或结果载荷。" + subjectText + drilldownText;
    }
    if (status === "error") {
      return "动作同步执行失败。请检查下方 stderr 或结构化错误。" + subjectText + drilldownText;
    }
    return "这是当前分区最近一次执行载荷。" + subjectText + drilldownText;
  }

  function taskDrilldownStatusSummary(payload) {
    const taskId = String((payload || {}).task_id || "").trim();
    const prefix = taskId ? ("已打开任务 " + taskId + "。") : "已打开任务详情。";
    const primaryOutput = actionResultPrimaryOutput(payload);
    const linkedDetail = linkedObjectDetailSummary(payload) || linkedObjectSummary(payload);
    const errorDetail = errorDetailSummary(payload);
    const extras = [];
    if (primaryOutput.label && primaryOutput.value) {
      extras.push(displayLabel(primaryOutput.label) + "=" + primaryOutput.value);
    }
    if (linkedDetail) {
      extras.push("关联=" + linkedDetail);
    }
    if (errorDetail) {
      extras.push("详情=" + errorDetail);
    }
    return prefix + taskStatusSummary(payload) + (extras.length ? (" " + extras.join(" · ")) : "");
  }

  function actionResultCommandText(payload) {
    if ((payload || {}).command_preview) {
      return String(payload.command_preview);
    }
    const error = String((payload || {}).error_summary || "").trim() || actionErrorSummary(payload);
    return error ? error : "";
  }

  function actionResultDataPayload(payload) {
    if (payload && payload.result !== undefined && payload.result !== null) {
      return payload.result;
    }
    if (payload && payload.parsed_stdout !== undefined && payload.parsed_stdout !== null) {
      return payload.parsed_stdout;
    }
    if (payload && payload.request_summary && typeof payload.request_summary === "object") {
      return { request_summary: payload.request_summary };
    }
    return null;
  }

  function actionResultPrimaryOutput(payload) {
    const topLevelLabel = String((payload || {}).primary_output_label || "").trim();
    const topLevelPath = String((payload || {}).primary_output_path || "").trim();
    if (topLevelLabel && topLevelPath) {
      return { label: topLevelLabel, value: topLevelPath };
    }
    const resultPayload = actionResultDataPayload(payload);
    if (!resultPayload || typeof resultPayload !== "object") {
      return { label: "", value: "" };
    }
    const candidates = [
      ["bundle_dir", resultPayload.bundle_dir],
      ["selection_path", resultPayload.selection_path],
      ["run_dir", resultPayload.run_dir],
      ["summary_path", resultPayload.summary_path],
      ["report_path", resultPayload.report_path],
      ["manifest_path", resultPayload.manifest_path]
    ];
    for (let idx = 0; idx < candidates.length; idx += 1) {
      const candidate = candidates[idx];
      if (candidate[1] !== undefined && candidate[1] !== null && String(candidate[1]).trim()) {
        return { label: String(candidate[0]), value: candidate[1] };
      }
    }
    return { label: "", value: "" };
  }

  function linkedObjectSummary(payload) {
    const rows = Array.isArray((payload || {}).linked_objects) ? payload.linked_objects : [];
    if (!rows.length) {
      return "";
    }
    const first = rows[0] || {};
    const title = String(first.title || first.object_type || "").trim();
    const path = String(first.path || "").trim();
    if (title && path) {
      return title + " @ " + path;
    }
    return title || path;
  }

  function linkedObjectDetailSummary(payload) {
    const rows = Array.isArray((payload || {}).linked_object_details) ? payload.linked_object_details : [];
    if (!rows.length) {
      return "";
    }
    const first = rows[0] || {};
    const title = String(first.title || first.object_type || "").trim();
    const identity = String(first.identity || "").trim();
    const summary = first.summary && typeof first.summary === "object" ? first.summary : {};
    const summaryKeys = Object.keys(summary).slice(0, 2);
    const summaryText = summaryKeys.map((key) => displayLabel(key) + "=" + stringifyValue(summary[key])).join(" · ");
    return [title, identity, summaryText].filter(Boolean).join(" | ");
  }

  function errorDetailSummary(payload) {
    const detail = (payload && typeof payload.error_detail === "object") ? payload.error_detail : {};
    if (detail.last_stderr_line) {
      return String(detail.last_stderr_line);
    }
    if (Array.isArray(detail.stderr_excerpt) && detail.stderr_excerpt.length) {
      return String(detail.stderr_excerpt[detail.stderr_excerpt.length - 1]);
    }
    if (detail.message) {
      return String(detail.message);
    }
    return "";
  }

  function taskStatusSummary(payload) {
    const taskId = String((payload || {}).task_id || "");
    const status = actionResultStatusValue(payload);
    const progressText = String((payload || {}).progress_summary || "").trim() || progressSnapshotText((payload || {}).progress || {});
    const heartbeatText = progressHeartbeatText((payload || {}).progress || {});
    const prefix = taskId ? ("任务 " + taskId) : "任务";
    if (status === "queued" || status === "running") {
      return prefix + " " + displayStatus(status) +
        (progressText ? ("：" + translateProgressText(progressText)) : "") +
        (heartbeatText ? (" · 最近心跳 " + heartbeatText) : "");
    }
    if (status === "succeeded") {
      return prefix + " 已完成" +
        (progressText ? ("：" + translateProgressText(progressText)) : "") +
        (heartbeatText ? (" · 最近心跳 " + heartbeatText) : "");
    }
    if (status === "failed") {
      const error = String((payload || {}).error_summary || "").trim() || actionErrorSummary(payload);
      return prefix + " 失败" + (error ? ("：" + error) : "");
    }
    return prefix + " 状态 " + displayStatus(status);
  }

  function synchronousActionStatusSummary(actionId, payload) {
    const status = actionResultStatusValue(payload);
    if (status === "ok" || status === "succeeded") {
      return "动作 " + actionId + " 执行成功。";
    }
    const error = actionErrorSummary(payload);
    return "动作 " + actionId + " 执行失败" + (error ? ("：" + error) : "。");
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function renderSection(sectionId, options) {
    const resolved = normalizeSection(sectionId);
    renderGlobalControls(resolved);
    navLinks.forEach((node) => {
      node.classList.toggle("is-active", node.dataset.sectionId === resolved);
    });
    panels.forEach((node) => {
      node.hidden = node.dataset.sectionId !== resolved;
    });
    if (!options || options.pushState !== false) {
      const url = writeInputsToUrl(new URL(window.location.href));
      url.searchParams.set("section", resolved);
      window.history.pushState({ section: resolved }, "", url);
    }
    shell.dataset.activeSection = resolved;
    shell.dataset.sectionApiPath = sectionApiPath(resolved);
    loadSection(resolved);
  }

  navLinks.forEach((node) => {
    node.addEventListener("click", function (event) {
      event.preventDefault();
      renderSection(node.dataset.sectionId || "home");
    });
  });

  if (refreshButton) {
    refreshButton.addEventListener("click", function () {
      const section = shell.dataset.activeSection || "home";
      const url = writeInputsToUrl(new URL(window.location.href));
      url.searchParams.set("section", normalizeSection(section));
      window.history.replaceState({ section: normalizeSection(section) }, "", url);
      loadSection(section);
    });
  }

  inputNodes.forEach((node) => {
    const commit = function () {
      renderSection(shell.dataset.activeSection || "home");
    };
    node.addEventListener("change", commit);
    node.addEventListener("keydown", function (event) {
      if (event.key === "Enter") {
        event.preventDefault();
        commit();
      }
    });
  });

  window.addEventListener("popstate", function () {
    applyBootstrapDefaults();
    const params = new URLSearchParams(window.location.search);
    renderSection(params.get("section"), { pushState: false });
  });

  applyBootstrapDefaults();
  const params = new URLSearchParams(window.location.search);
  ensureActionCatalog().catch(function () {
    return null;
  }).finally(function () {
    renderSection(params.get("section") || state.active_section || "home", { pushState: false });
  });
})();
""".strip()
