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
  - `deep_otm_baseline` 的纯诊断文档，只保留病灶、证据、因子分组和诊断边界。
- `DEEP_OTM_BASELINE_RETRAIN_PLAN.md`
  - `deep_otm_baseline` 的重训方案文档，和诊断文档分开维护；当前已经落了完整的重训框架，包括目标、基线定义、训练目标、样本口径、加权、特征方案、切分、实验矩阵、评估、通过条件和决策流程。

Repo 内自动化入口：

- `auto_research/program.md`
  - 外部 `codex` 每一轮读取的仓库内研究指令。
- `auto_research/README.md`
  - `codex_background_loop.sh`、`status_autorun.sh`、`run_one_experiment.sh` 等控制面脚本的使用说明。
- `scripts/`
  - 普通研究工具、导入脚本、运维监控脚本和仓库维护脚本的归类目录。

入口包：

- `pm5min/`
  - 5 分钟入口包。
  - 复用共享的 `pm15min` 实现，不是整套代码再分叉一份。
  - 只把周期敏感的默认值和 5 分钟 profile 单独分离出来。

清理规则：

- 长期规范和 runbook 留在 `docs/`
- 一次性验证、执行看板、review、checklist、dated audit 不留在 `docs/`
- 这类内容应进入 `var/`、实验产物目录、PR 或 issue
- 当前 active bundle、selection 切换和运行时状态不再手抄进文档，直接以 `research/active_bundles/.../selection.json` 与相关 CLI 为准
