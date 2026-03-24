`v2/scripts/entrypoints/` 是 `v2` standalone 运行面的 canonical shell 入口。

- 这些脚本默认以 `v2/` 目录为项目根：
  - 代码：`src/`
  - 数据：`data/`
  - 研究产物：`research/`
  - 运行日志：`var/`
- `.env` 读取顺序：
  1. `PM15MIN_ENV_FILE`
  2. `<v2-root>/.env`
  3. `<v2-root>/../.env`

当前 monorepo 顶层 `scripts/entrypoints/start_v2_*` 只是兼容 wrapper，真正实现已经在这里。
