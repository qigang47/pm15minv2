# Data Domain Specification

这份文档只描述当前仓库里长期有效的 `data` 域 contract。

目标：

- 说明 `data/` 和 `var/` 下哪些目录是长期 canonical layout。
- 说明 `data` 域负责什么，不负责什么。
- 说明运维和 research 应该读哪些产物，不应该直接依赖哪些中间态。

如果代码和文档冲突，以 `src/pm15min/data/` 与 `src/pm15min/core/layout.py` 为准。

## 1. 边界

`data` 域只负责数据事实和可复用的数据产物：

- 外部源拉取与同步。
- 高频 orderbook 录制与热缓存。
- canonical parquet 表构建。
- 人读导出和基础 summary。

`data` 域不负责：

- 模型训练和 bundle 管理。
- live 打分、决策、下单。
- 实验编排和评估报告。

这些分别属于：

- `research`
- `live`

## 2. 根目录

在当前仓库根目录下，`RewriteLayout` 的长期根目录固定为：

```text
src/
data/
research/
var/
tests/
```

其中：

- `src/pm15min/data/` 存实现。
- `data/` 存持久化数据。
- `var/live/` 存 runtime state、日志、热缓存。
- `var/research/` 存 research runtime cache、locks、logs。

`core/layout.py` 同时兼容两种运行方式：

- 当前仓库直接作为 `v2` 根目录运行。
- 当前仓库被嵌入到更大的 workspace，并以 `<workspace>/v2` 形式运行。

文档默认按“当前仓库根目录就是 rewrite root”来写。

## 3. 目录分层

`data/` 长期分两层 surface：

- `data/live/`
- `data/backtest/`

每个 surface 只允许三类产物：

1. `sources/`
2. `tables/`
3. `exports/`

语义固定如下：

- `sources/` 是原始或近原始外部数据。
- `tables/` 是仓库内长期稳定的 canonical tables。
- `exports/` 只给人读，不允许反向成为上游依赖。

基本格式约定：

- canonical tables: `parquet`
- 高频原始盘口: `ndjson.zst`
- manifest / state: `json`
- runtime log: `jsonl`
- 人读导出: `csv`

## 4. 当前 canonical 路径

### 4.1 市场与真值数据

常见 canonical 表路径：

```text
data/<surface>/tables/markets/cycle=<cycle>/asset=<asset>/data.parquet
data/<surface>/tables/orderbook_index/cycle=<cycle>/asset=<asset>/date=<date>/data.parquet
```

常见 source 路径：

```text
data/<surface>/sources/polymarket/market_catalogs/cycle=<cycle>/asset=<asset>/snapshot_ts=<ts>/data.parquet
data/<surface>/sources/polymarket/oracle_prices/cycle=<cycle>/asset=<asset>/data.parquet
data/<surface>/sources/polymarket/orderbooks/cycle=<cycle>/asset=<asset>/date=<date>/depth.ndjson.zst
data/<surface>/sources/binance/klines_1m/symbol=<symbol>/data.parquet
```

`research` 和 `live` 默认应优先读取：

- `data/.../tables/...`

而不是直接扫：

- `data/.../sources/...`

只有 replay、审计或特殊 loader 才允许显式读取 source 层。

### 4.2 live runtime 配套产物

`data` 域会把部分 runtime 产物写进 `var/live/`，因为它们属于运行态，不属于历史数据仓：

```text
var/live/state/orderbooks/cycle=<cycle>/asset=<asset>/...
var/live/logs/data/recorders/cycle=<cycle>/asset=<asset>/...
```

这类产物包括：

- recorder state
- recorder jsonl log
- `recent.parquet` 热缓存
- foundation runtime summary

规则：

- 需要长期复现的历史事实放 `data/`。
- 只服务当前运行进程的状态和日志放 `var/`。

## 5. CLI 面

当前公开的 `data` CLI 顶层命令为：

```text
show-config
show-layout
show-summary
show-orderbook-coverage
sync
build
export
record
run
```

最常用的 operator / 开发命令：

```bash
PYTHONPATH=src python -m pm15min data show-layout --market sol --cycle 15m --surface live
PYTHONPATH=src python -m pm15min data show-summary --market sol --cycle 15m --surface backtest
PYTHONPATH=src python -m pm15min data run orderbook-fleet --markets btc,eth,sol,xrp --cycle 15m --surface live --iterations 1
```

实际支持的子命令以 `PYTHONPATH=src python -m pm15min data --help` 为准。

## 6. 对下游的约束

### 6.1 对 research

`research` 应长期依赖：

- `data/backtest/tables/...`
- 必要时通过 loader 受控读取 `data/backtest/sources/...`

不应该：

- 在 research 模块里手搓目录扫描 source 文件。
- 把 `exports/` 当成训练输入。
- 直接依赖 `var/live/` 的运行时状态。

### 6.2 对 live

`live` 可以依赖：

- `data/live/tables/...`
- `var/live/state/orderbooks/.../recent.parquet`
- `var/live/logs/...` 中的 recorder/runtime 观察信息

不应该：

- 把 `backtest` surface 当成实盘 runtime 的直接输入。
- 把一次性导出的 CSV 当成 canonical source。

## 7. 维护规则

长期规则只有几条：

- 新数据对象优先落到 `sources/` 或 `tables/`，不要先造临时目录。
- 需要给人看的产物才放 `exports/`。
- 涉及路径约定的改动，先改 `layout` / `service`，再改文档。
- 运行期一次性验证、手工 checklist、单日审计记录，不放 `docs/`，放到 `var/`、PR 或单次 artifact 目录。
