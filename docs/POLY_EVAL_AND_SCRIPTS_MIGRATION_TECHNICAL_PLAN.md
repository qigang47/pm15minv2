# Poly Eval And Scripts Migration Notes

这份文档只保留当前仍然有效的迁移原则，不再保留大段已经过时的分阶段计划。

目标：

- 说明哪些能力已经进入正式 package。
- 说明哪些东西继续留在 `scripts/` 更合理。
- 说明新能力迁移时的判断标准。

## 1. 当前状态

从当前代码结构看，原先散落在 legacy `poly_eval/` 与脚本层的大部分正式语义，已经有对应的 package 落点：

- 评估方法与 scope
  - `src/pm15min/research/evaluation/`
  - `src/pm15min/research/evaluation/methods/`

- 训练、bundle、backtest、experiment
  - `src/pm15min/research/datasets/`
  - `src/pm15min/research/training/`
  - `src/pm15min/research/bundles/`
  - `src/pm15min/research/backtests/`
  - `src/pm15min/research/experiments/`

- live / data runtime 的正式入口
  - `src/pm15min/live/`
  - `src/pm15min/data/`

## 2. 当前仍留在 `scripts/` 的内容

当前仓库里的 `scripts/` 仍然有价值，但定位应更克制：

- `scripts/entrypoints/`
  - 正式 shell wrapper
  - 负责环境初始化与进程包装

- `scripts/research/run_grouped_backtest_grid.py`
  - 专项批量编排工具

- `scripts/import_direction_model_bundles.py`
  - 明确的一次性或管理型导入脚本

- `scripts/monitor_*`
  - operator / admin 辅助脚本

结论：

- `scripts/` 可以保留，但不要重新变成业务主逻辑的落点。
- 长期复用的核心语义优先进 `src/pm15min/...`。

## 3. 迁移判断标准

一个 legacy 脚本或评估能力要不要迁进 package，按这四条判断：

1. 是否被重复使用，而不是一次性操作。
2. 是否有稳定输入输出 contract。
3. 是否应该被 CLI、service、tests 复用。
4. 是否应该写入 `research/`、`data/`、`var/` 的 canonical layout。

满足这些条件时，优先迁成 package。

更适合继续留在 `scripts/` 的场景：

- 一次性 importer
- 临时运维工具
- 包装多个正式命令的 shell glue
- 明显只在人工值班时使用的辅助脚本

## 4. 对新工作的要求

以后新增类似能力时，默认顺序应是：

1. 先把核心语义写进 `src/pm15min/...`
2. 给它补 CLI / service / tests
3. 再视需要加 thin script 或 shell wrapper

反过来不建议：

- 先堆脚本，再长期依赖脚本成为主入口
- 让脚本直接持有一堆独占业务逻辑

## 5. 当前建议

- `research/evaluation/` 继续作为 poly eval 正式归宿。
- `scripts/entrypoints/` 继续作为 shell 层正式入口。
- `scripts/research/`、`scripts/monitor_*`、importer 类脚本继续保持薄封装，不再扩张成第二套架构。
