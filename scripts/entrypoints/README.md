`scripts/entrypoints/` 是当前仓库的 canonical shell 入口。

这些脚本默认以当前仓库根目录作为 rewrite root：

- 代码：`src/`
- 数据：`data/`
- 研究产物：`research/`
- 运行日志：`var/`

`_python_env.sh` 会同时兼容两种场景：

- 当前仓库直接作为 rewrite root 运行
- 当前仓库被放在更大 workspace 的 `v2/` 子目录下运行

`.env` 读取顺序：

1. `PM15MIN_ENV_FILE`
2. `<rewrite-root>/.env`
3. `<rewrite-root>/../.env`

这些脚本只负责：

- 激活 Python 运行环境
- 设置 `PYTHONPATH`
- 做薄包装的进程与日志管理

真正状态仍以 `var/` 下的 canonical state / logs 为准。
