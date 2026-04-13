# PM5Min Console Task 4 Hardening Design

## Goal

在不扩大 Task 4 范围的前提下，收口这轮 console 控制面的两个遗留问题：

1. 把 `runtime/task` 只读视图整形从 `src/pm5min/console/service.py` 拆出去，避免 `service.py` 继续膨胀成总管文件。
2. 补齐 CLI 边界测试，明确锁住 `show-runtime-state`、`show-runtime-history`、`list-tasks`、`show-task` 这组读命令必须走本地 `service`，不能回流到 compat。

## Constraints

- 15m 和 5m 继续保持上层业务分离。
- `src/pm5min/console/compat.py` 仍然只保留执行、异步提交、HTTP 服务三个入口。
- 不改 CLI 对外行为，不改控制台返回结构。
- 只做本地模块内的职责重排和边界测试补强。

## Design

### 1. Service 保持“薄聚合层”

`src/pm5min/console/service.py` 保留这些职责：

- console 首页组装
- action catalog / action plan 转发
- data / research 读模型转发
- 调用 runtime/task 视图整形模块

它不再保存大段 task/runtime 结果整形细节。

### 2. 新增本地 runtime/task 视图模块

新增 `src/pm5min/console/runtime_views.py`，只负责把 `src/pm5min/console/tasks.py` 读出的底层记录整理成 console 视图载荷，包括：

- runtime summary 视图
- runtime history 视图
- task list 视图
- task detail 视图
- 相关的 marker、group、brief、warning、result-path 等辅助整形函数

这样 `tasks.py` 继续只负责底层任务文件读取，`runtime_views.py` 负责展示层整形，`service.py` 只做聚合转发。

### 3. 边界测试补到 runtime/task 读路由

在 `tests/test_pm5min_cli.py` 里新增一组代表性测试，验证：

- `show-runtime-state`
- `show-runtime-history`
- `list-tasks`
- `show-task`

都从 `pm5min.console.handlers` 调本地 `service` 导入名，而不是 compat。

测试继续沿用现有风格：

- 一部分用 import/source 文本检查锁边界
- 一部分 monkeypatch `pm5min.console.handlers.*` 来验证实际 CLI 路由
- 不把 execute/submit/serve 的 compat 路由混进这组测试

## Risk Management

- 不直接重写现有 task/runtime 逻辑，只做提取，降低行为回归风险。
- 拆分后跑已有 task/runtime 单测和 console CLI 冒烟，确认输出结构不变。
- 如果拆分过程中出现输出结构变化，以测试保持兼容为准，不顺手做接口“优化”。
