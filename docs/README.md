# Docs Index

`docs/` 现在只保留长期有效的项目文档，不再混放阶段性 checklist、执行板、单次验证记录和 dated audit。

当前保留文档：

- `DATA_TECHNICAL_PLAN.md`
  - `data` 域的长期 layout 和边界。
- `RESEARCH_TECHNICAL_PLAN.md`
  - `research` 域的长期对象模型、active bundle 规则、workflow 和脚本边界。
- `LIVE_TECHNICAL_PLAN.md`
  - `live` 域的长期 runtime contract。
- `LIVE_OPERATOR_RUNBOOK.md`
  - operator 值班手册。
- `DEEP_OTM_BASELINE_UP_DIAGNOSIS.md`
  - `deep_otm_baseline` 的 `Up` 方向诊断、因子分组和后续收缩方向。

清理规则：

- 长期规范和 runbook 留在 `docs/`
- 一次性验证、执行看板、review、checklist、dated audit 不留在 `docs/`
- 这类内容应进入 `var/`、实验产物目录、PR 或 issue
- 当前 active bundle、selection 切换和运行时状态不再手抄进文档，直接以 `research/active_bundles/.../selection.json` 与相关 CLI 为准
