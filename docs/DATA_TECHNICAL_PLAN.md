# v2 Data Domain Specification

这份文档是 `v2` Data 域的正式规范，不是随手记录。

目标只有一个：以后任何人看 `v2/data/`，都能立刻知道：

- 这个文件是什么
- 它属于 `live` 还是 `backtest`
- 它是 raw source、canonical table，还是 export
- 它由哪个命令生成
- 下游应该读哪里，不该读哪里

如果代码和文档冲突，以 `v2/src/pm15min/data/` 为准；但后续开发必须把代码改回和本文档一致，而不是继续发散。

---

## 1. 设计原则

### 1.1 总原则

- `data` 只负责“数据事实”和“规范化表”。
- `data` 不负责交易决策，不负责模型训练逻辑，不负责回测策略逻辑。
- `data` 的输出必须稳定、可重复生成、可被 live/research 长期依赖。

### 1.2 三层分离

`v2 data` 固定分成三层：

1. `sources/`
   - 原始或近原始外部数据
   - 可以保留外部字段风格
   - 可以按抓取时间、年/月、日分区

2. `tables/`
   - v2 内唯一 canonical source of truth
   - 字段命名、主键、去重逻辑必须稳定
   - live/research 只允许从这里读

3. `exports/`
   - 给人看的 CSV
   - 方便检查、同步、分析
   - 绝不允许上游代码反向依赖

### 1.3 surface 分离

Data 域固定有两个面：

- `surface=live`
- `surface=backtest`

它们的职责不同：

- `live`
  - 面向实盘运行
  - 优先接直接可用、接近 UI/交易口径的数据
  - 允许写 runtime state / recorder state

- `backtest`
  - 面向训练、回测、分析
  - 优先可复现、可回放、可审计的数据
  - 更强调完整历史和可追溯性

绝对禁止：

- 把 `live` 的 source 目录当成回测固定输入
- 把 `backtest` 的临时修补 CSV 当成 live 直接依赖

### 1.4 文件格式原则

- Canonical table 默认：`parquet`
- 高频原始盘口：`ndjson.zst`
- 运行状态：`json`
- 运行日志：`jsonl`
- 给人看的导出：`csv`

### 1.5 时间和主键原则

- 全部时间统一按 UTC 解释
- `*_ts`：秒级 epoch，`int64`
- `*_ts_ms`：毫秒级 epoch，`int64`
- canonical table 必须明确主键
- upsert 时按固定主键 + 固定排序字段去重

---

## 2. 顶层目录规则

`v2` 的 data 目录必须长这样：

```text
v2/
  data/
    live/
      sources/
      tables/
      exports/
    backtest/
      sources/
      tables/
      exports/
  var/
    live/
      cache/
      state/
      logs/
    backtest/
      cache/
      state/
      logs/
```

注意：

- `v2/data/` 存业务数据
- `v2/var/` 存运行态和临时态
- `v2/data/live` 和 `v2/data/backtest` 是并列结构，不是父子覆盖

---

## 3. 路径命名规则

### 3.1 分区字段

统一使用显式分区名：

- `surface=live` 不写在路径里，因为它已经体现在 `v2/data/live/...`
- `cycle=5m|15m`
- `asset=btc|eth|sol|xrp`
- `symbol=BTCUSDT`
- `date=YYYY-MM-DD`
- `year=YYYY`
- `month=MM`
- `snapshot_ts=YYYY-MM-DDTHH-MM-SSZ`

### 3.2 不允许的命名

以下都视为 legacy 风格，不允许新增：

- `foo_20260318_final_v2_fixed.csv`
- `oracle_prices_latest.csv`
- `tmp_truth_new.csv`
- `sol_merged.csv`

原因：

- 看文件名不知道口径
- 看文件名不知道 surface
- 看文件名不知道是不是 canonical

### 3.3 允许的命名

固定目录 + 固定文件名：

- `.../data.parquet`
- `.../depth.ndjson.zst`
- `.../oracle_prices.csv`
- `.../truth.csv`
- `.../state.json`
- `.../recorder.jsonl`

文件意义由目录决定，不由花哨文件名决定。

---

## 4. 完整目录树

下面是 Data 域的标准树，后续新增数据集必须在这棵树里找位置，不允许重新发明目录：

```text
v2/data/
  live/
    sources/
      binance/
        klines_1m/
          symbol=BTCUSDT/data.parquet
          symbol=ETHUSDT/data.parquet
          symbol=SOLUSDT/data.parquet
          symbol=XRPUSDT/data.parquet
      polymarket/
        market_catalogs/
          cycle=15m/asset=sol/snapshot_ts=2026-03-19T09-00-00Z/data.parquet
        oracle_prices/
          cycle=15m/asset=sol/data.parquet
        orderbooks/
          cycle=15m/asset=sol/date=2026-03-19/depth.ndjson.zst
      chainlink/
        streams/
          asset=sol/year=2026/month=03/data.parquet
        datafeeds/
          asset=sol/year=2026/month=03/data.parquet

    tables/
      markets/
        cycle=15m/asset=sol/data.parquet
      orderbook_index/
        cycle=15m/asset=sol/date=2026-03-19/data.parquet
      oracle_prices/
        cycle=15m/asset=sol/data.parquet
      truth/
        cycle=15m/asset=sol/data.parquet

    exports/
      oracle_prices/
        cycle=15m/asset=sol/oracle_prices.csv
      truth/
        cycle=15m/asset=sol/truth.csv

  backtest/
    sources/
      binance/
      polymarket/
      chainlink/
    tables/
      markets/
      orderbook_index/
      oracle_prices/
      truth/
    exports/
      oracle_prices/
      truth/

v2/var/
  live/
    cache/
    state/
      orderbooks/
        cycle=15m/asset=sol/state.json
      summary/
        cycle=15m/asset=sol/latest.json
        cycle=15m/asset=sol/latest.manifest.json
    logs/
      data/
        recorders/
          cycle=15m/asset=sol/recorder.jsonl

  backtest/
    cache/
    state/
    logs/
```

---

## 5. Canonical 数据集定义

下面每个数据集都定义：

- 路径
- 文件格式
- 主键
- 生成命令
- 下游是否允许直接读取

---

### 5.1 `market_catalog_snapshot`

原始快照目录：

```text
v2/data/<surface>/sources/polymarket/market_catalogs/cycle=<cycle>/asset=<asset>/snapshot_ts=<timestamp>/data.parquet
```

示例：

```text
v2/data/live/sources/polymarket/market_catalogs/cycle=15m/asset=sol/snapshot_ts=2026-03-19T09-00-00Z/data.parquet
```

含义：

- 某次从 Polymarket/Gamma 拉到的市场快照
- 允许重复快照并存
- 允许用于审计“当时看到了什么”

格式：

- `parquet`

主键：

- 快照内部主键：`market_id`
- 整个 snapshot 的外部定位主键：目录里的 `snapshot_ts`

生成命令：

```bash
PYTHONPATH=v2/src python -m pm15min data sync market-catalog --market sol --cycle 15m --surface backtest
```

```bash
PYTHONPATH=v2/src python -m pm15min data sync market-catalog --market sol --cycle 15m --surface live
```

```bash
PYTHONPATH=v2/src python -m pm15min data sync legacy-market-catalog --market sol --cycle 15m --surface live
```

口径规则：

- `surface=backtest`
  - 走 Gamma `closed events`
  - 用于可回放的历史市场目录

- `surface=live`
  - 走 Gamma `active markets`
  - 用于当前实盘可交易窗口

- `legacy-market-catalog`
  - 只用于把旧仓库里已经真实存在的 market CSV 迁入 canonical path
  - 不改变 canonical contract，只改变导入来源

下游读取规则：

- live/research 不应直接读取 snapshot
- 只允许 pipeline 读取

---

### 5.1a `binance_klines_1m_source`

路径：

```text
v2/data/<surface>/sources/binance/klines_1m/symbol=<SYMBOL>/data.parquet
```

示例：

```text
v2/data/live/sources/binance/klines_1m/symbol=SOLUSDT/data.parquet
```

用途：

- 这是 live / research 特征主干依赖的 canonical Binance 1m source
- 允许持续增量刷新
- 不再依赖 legacy `spot_<SYMBOL>_1m.parquet` 作为运行入口

格式：

- `parquet`

主键：

- `open_time`

关键字段：

- `open_time`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `close_time`
- `quote_asset_volume`
- `number_of_trades`
- `taker_buy_base_volume`
- `taker_buy_quote_volume`
- `ignore`

生成命令：

```bash
PYTHONPATH=v2/src python -m pm15min data sync binance-klines-1m --market sol --surface live
```

规则：

- 这是 clean-room direct sync
- 新数据按 `open_time` 增量 upsert
- 未来收盘时间还没到的 in-flight candle 不写入 canonical source

---

### 5.2 `market_catalog`

canonical 路径：

```text
v2/data/<surface>/tables/markets/cycle=<cycle>/asset=<asset>/data.parquet
```

示例：

```text
v2/data/backtest/tables/markets/cycle=15m/asset=sol/data.parquet
```

用途：

- 这是该 surface 下市场目录的 canonical 真相
- recorder / oracle builder / truth builder 都应基于它工作

格式：

- `parquet`

主键：

- `market_id`

关键字段：

- `market_id`
- `condition_id`
- `asset`
- `cycle`
- `cycle_start_ts`
- `cycle_end_ts`
- `token_up`
- `token_down`
- `slug`
- `question`
- `resolution_source`
- `event_id`
- `event_slug`
- `event_title`
- `series_slug`
- `closed_ts`
- `source_snapshot_ts`

下游读取规则：

- live 可读
- research 可读
- 这是允许被直接依赖的 canonical table

补充说明：

- `v2/data/live/tables/markets/...`
  - 可以同时包含 legacy 导入的历史 live market rows
  - 也可以继续累积 Gamma active market rows

- 判断当前是否能做 live quote，不只看这个表是否存在
  - 还要看 signal snapshot 的 `cycle_start_ts / decision_ts`
  - 是否在该表中仍然能找到对应 market row

---

### 5.3 `direct_polymarket_oracle_prices_source`

路径：

```text
v2/data/<surface>/sources/polymarket/oracle_prices/cycle=<cycle>/asset=<asset>/data.parquet
```

示例：

```text
v2/data/live/sources/polymarket/oracle_prices/cycle=15m/asset=sol/data.parquet
```

用途：

- 存直接从 Polymarket UI/API 获取的 `price_to_beat` / `final_price`
- 这是你说的“实盘口径”
- live 的开盘价语义优先从这里来

当前来源：

- `/api/past-results`
- `/api/crypto/crypto-price`

格式：

- `parquet`

主键：

- `asset + cycle_start_ts`

关键字段：

- `asset`
- `cycle`
- `cycle_start_ts`
- `cycle_end_ts`
- `price_to_beat`
- `final_price`
- `has_price_to_beat`
- `has_final_price`
- `has_both`
- `completed`
- `incomplete`
- `cached`
- `api_timestamp_ms`
- `http_status`
- `source`
- `source_priority`
- `fetched_at`

来源优先级：

- `polymarket_api_crypto_price` 比 `polymarket_api_past_results` 更新鲜时可覆盖
- 统一通过 `source_priority` 决定保留哪一行

生成命令：

```bash
PYTHONPATH=v2/src python -m pm15min data sync direct-oracle-prices --market sol --surface live
```

下游读取规则：

- builder 可以直接读
- live 如果需要“官方开盘价口径”，可以只读这个 source
- research 不应直接依赖 source；应依赖 `tables/oracle_prices`

---

### 5.4 `chainlink_streams_source`

路径：

```text
v2/data/<surface>/sources/chainlink/streams/asset=<asset>/year=<YYYY>/month=<MM>/data.parquet
```

示例：

```text
v2/data/backtest/sources/chainlink/streams/asset=sol/year=2026/month=03/data.parquet
```

用途：

- 保存从 Polygon RPC 解码出的 Chainlink streams 报文
- 用作 oracle price 回退源
- 用作 settlement truth 关联源

格式：

- `parquet`

主键：

- `tx_hash + perform_idx + value_idx`

关键字段：

- `asset`
- `tx_hash`
- `block_number`
- `observation_ts`
- `extra_ts`
- `benchmark_price_raw`
- `price`
- `report_feed_id`
- `requester`
- `path`
- `perform_idx`
- `value_idx`
- `source_file`
- `ingested_at`

生成命令：

```bash
PYTHONPATH=v2/src python -m pm15min data sync streams-rpc --market sol --surface backtest
```

下游读取规则：

- builder 可读
- live/research 不应直接依赖这个 source

---

### 5.4a `chainlink_datafeeds_source`

路径：

```text
v2/data/<surface>/sources/chainlink/datafeeds/asset=<asset>/year=<YYYY>/month=<MM>/data.parquet
```

示例：

```text
v2/data/backtest/sources/chainlink/datafeeds/asset=sol/year=2026/month=03/data.parquet
```

用途：

- 保存从 Polygon RPC 直接扫描到的 Chainlink Data Feeds `AnswerUpdated` 历史
- 提供比 streams 更长的价格历史参考
- 为后续 datafeeds-based 分析、审计和标签对照提供 canonical source

格式：

- `parquet`

主键：

- `tx_hash + log_index`

关键字段：

- `asset`
- `feed_name`
- `proxy_address`
- `aggregator_address`
- `decimals`
- `block_number`
- `tx_hash`
- `log_index`
- `round_id`
- `updated_at`
- `updated_at_iso`
- `answer_raw`
- `answer`
- `source_file`
- `ingested_at`

生成命令：

```bash
PYTHONPATH=v2/src python -m pm15min data sync datafeeds-rpc --market sol --surface backtest
```

下游读取规则：

- analysis / builder 可读
- live/research 不应直接依赖这个 source
- canonical label / oracle 仍应优先读 `tables/`

---

### 5.5 `settlement_truth_source`

路径：

```text
v2/data/<surface>/sources/polymarket/settlement_truth/cycle=<cycle>/asset=<asset>/data.parquet
```

示例：

```text
v2/data/backtest/sources/polymarket/settlement_truth/cycle=15m/asset=sol/data.parquet
```

用途：

- 存从 on-chain resolution + streams 对齐得到的原始 truth source
- 是 `tables/truth` 的上游之一

格式：

- `parquet`

主键：

- `market_id + cycle_end_ts`

关键字段：

- `market_id`
- `condition_id`
- `asset`
- `cycle`
- `cycle_start_ts`
- `cycle_end_ts`
- `slug`
- `question`
- `resolution_source`
- `winner_side`
- `label_updown`
- `onchain_resolved`
- `stream_match_exact`
- `full_truth`
- `stream_price`
- `stream_extra_ts`
- `source_file`
- `ingested_at`

生成命令：

```bash
PYTHONPATH=v2/src python -m pm15min data sync settlement-truth-rpc --market sol --surface backtest
```

下游读取规则：

- builder 可读
- research 不应直接依赖 source

---

### 5.6 `orderbook_depth`

路径：

```text
v2/data/<surface>/sources/polymarket/orderbooks/cycle=<cycle>/asset=<asset>/date=<YYYY-MM-DD>/depth.ndjson.zst
```

示例：

```text
v2/data/live/sources/polymarket/orderbooks/cycle=15m/asset=sol/date=2026-03-19/depth.ndjson.zst
```

用途：

- 高频 raw depth 流
- append-only

格式：

- `ndjson.zst`

记录级主键：

- 逻辑上：`captured_ts_ms + market_id + token_id + side`

关键字段：

- `captured_ts_ms`
- `source_ts_ms`
- `market_id`
- `token_id`
- `side`
- `asset`
- `cycle`
- `asks`
- `bids`
- `source`

生成命令：

```bash
PYTHONPATH=v2/src python -m pm15min data record orderbooks --market sol --cycle 15m --surface live --loop
```

```bash
PYTHONPATH=v2/src python -m pm15min data run orderbook-fleet --markets btc,eth,sol,xrp --cycle 15m --surface live --loop --iterations 0
```

下游读取规则：

- builder / analysis 可读
- live 主流程不应该直接把这个当最终输入
- recorder 默认轮询频率当前已收紧到 `0.35s`
- live surface 还会额外把最近约 `15` 分钟的轻量 index 维护到：
  - `v2/var/live/state/orderbooks/cycle=<cycle>/asset=<asset>/recent.parquet`
- `recent` 窗口当前支持 `--recent-window-minutes` 调整，不要求固定死在 `15`

---

### 5.7 `orderbook_index`

路径：

```text
v2/data/<surface>/tables/orderbook_index/cycle=<cycle>/asset=<asset>/date=<YYYY-MM-DD>/data.parquet
```

示例：

```text
v2/data/backtest/tables/orderbook_index/cycle=15m/asset=sol/date=2026-03-19/data.parquet
```

用途：

- 高频 depth 的轻量索引
- 方便快速扫描和回测抽样

格式：

- `parquet`

主键：

- `captured_ts_ms + market_id + token_id + side`

关键字段：

- `captured_ts_ms`
- `market_id`
- `token_id`
- `side`
- `best_ask`
- `best_bid`
- `ask_size_1`
- `bid_size_1`
- `spread`

生成命令：

```bash
PYTHONPATH=v2/src python -m pm15min data record orderbooks --market sol --surface live --iterations 1
```

```bash
PYTHONPATH=v2/src python -m pm15min data build orderbook-index --market sol --surface live --date 2026-03-13
```

```bash
PYTHONPATH=v2/src python -m pm15min data sync legacy-orderbook-depth --market sol --surface live
```

规则：

- recorder 运行时可以直接写当天 `orderbook_index`
- live recorder 还会维护一个滚动 `recent.parquet`
  - 目标是给 live quote / operator 读取更小、更热的近窗 orderbook 视图
  - 当前窗口默认保留最近 `15` 分钟
- 对已经存在的 canonical `depth.ndjson.zst`，允许后补：
  - `data build orderbook-index --date <YYYY-MM-DD>`

- `legacy-orderbook-depth`
  - 只负责把旧仓库真实存在的 `orderbook_depth_YYYYMMDD.ndjson.zst`
    迁到 canonical source path
  - 不负责把 `.gz` 或单 market `jsonl.gz` 混进 canonical path

---

### 5.8 `oracle_prices_15m`

路径：

```text
v2/data/<surface>/tables/oracle_prices/cycle=15m/asset=<asset>/data.parquet
```

示例：

```text
v2/data/live/tables/oracle_prices/cycle=15m/asset=sol/data.parquet
```

用途：

- 这是 `price_to_beat / final_price` 的 canonical table
- research/backtest/live 都应该读这里，而不是去拼 legacy CSV

格式：

- `parquet`

主键：

- `asset + cycle_start_ts`

关键字段：

- `asset`
- `cycle_start_ts`
- `cycle_end_ts`
- `price_to_beat`
- `final_price`
- `source_price_to_beat`
- `source_final_price`
- `has_price_to_beat`
- `has_final_price`
- `has_both`

优先级规则：

- 开盘价优先 direct Polymarket oracle source
- 收盘价优先 direct Polymarket oracle source
- direct 缺失时，回退 streams
- `source_price_to_beat` / `source_final_price` 必须反映最终字段的真实来源

生成命令：

```bash
PYTHONPATH=v2/src python -m pm15min data build oracle-prices-15m --market sol --surface live
```

---

### 5.9 `truth_15m`

路径：

```text
v2/data/<surface>/tables/truth/cycle=15m/asset=<asset>/data.parquet
```

示例：

```text
v2/data/backtest/tables/truth/cycle=15m/asset=sol/data.parquet
```

用途：

- 15m label 的 canonical table
- 训练 / 回测 / 对账都应该读这里

格式：

- `parquet`

主键：

- `asset + cycle_end_ts`

关键字段：

- `asset`
- `cycle_start_ts`
- `cycle_end_ts`
- `market_id`
- `condition_id`
- `winner_side`
- `label_updown`
- `resolved`
- `truth_source`
- `full_truth`

优先级规则：

- `settlement_truth` 优先级高于 `oracle_prices`
- 同一 `asset + cycle_end_ts` 只能保留一条 canonical truth

生成命令：

```bash
PYTHONPATH=v2/src python -m pm15min data build truth-15m --market sol --surface backtest
```

---

### 5.10 `exports`

导出路径：

```text
v2/data/<surface>/exports/oracle_prices/cycle=15m/asset=<asset>/oracle_prices.csv
v2/data/<surface>/exports/truth/cycle=15m/asset=<asset>/truth.csv
```

用途：

- 给人看
- 给外部同步
- 给临时排查

规则：

- 永远从 canonical parquet 导出
- export 不能反向成为上游输入

---

## 6. live / backtest 分面规则

### 6.1 为什么要分

你提出的要求是对的：

- live 的开盘价必须有 Polymarket direct oracle 口径
- backtest 可以用更完整、可回放、可修复的历史源

所以两套面必须同时存在。

### 6.2 live 面

推荐 live 依赖：

- `data/live/tables/markets/...`
- `data/live/sources/polymarket/oracle_prices/...`
- `data/live/tables/oracle_prices/...`
- `data/live/sources/polymarket/orderbooks/...`
- `data/live/tables/orderbook_index/...`

live 面的特点：

- 更新快
- 优先 direct source
- 允许 incomplete row
- 允许 runtime state

### 6.3 backtest 面

推荐 backtest 依赖：

- `data/backtest/tables/markets/...`
- `data/backtest/sources/chainlink/streams/...`
- `data/backtest/sources/polymarket/settlement_truth/...`
- `data/backtest/tables/oracle_prices/...`
- `data/backtest/tables/truth/...`
- `data/backtest/tables/orderbook_index/...`

backtest 面的特点：

- 可复现
- 可审计
- 更强调完整性和去重

### 6.4 不允许的跨面依赖

不允许：

- `research` 从 `data/live/sources/...` 直接训练
- `live` 从 `data/backtest/exports/...` 直接拿价格
- `truth` 在 `live` 面和 `backtest` 面之间相互覆盖

---

## 7. 命令与产物对应表

### 7.0 数据面 summary / audit 入口

命令：

```bash
PYTHONPATH=v2/src python -m pm15min data show-summary --market sol --surface live
```

如需把当前 summary 落成 canonical audit state：

```bash
PYTHONPATH=v2/src python -m pm15min data show-summary --market sol --surface live --write-state
```

用途：

- 汇总当前 surface 下核心 canonical source/table 的存在性
- 输出基础质量指标：
  - `row_count`
  - `duplicate_count`
  - `null_key_count`
  - `time_range`
- 输出可直接消费的审计指标：
  - `freshness_range`
  - `stale_issue_datasets`
  - `low_row_count_issue_datasets`
  - `dataset_audits`
  - `alignment_checks`
  - `completeness`
  - `issues`
- 给 operator / researcher 一个“这套 data 现在齐不齐、缺什么”的统一入口
- `--write-state` 时会把当前 summary 落到：
  - `v2/var/<surface>/state/summary/cycle=<cycle>/asset=<asset>/latest.json`
  - `v2/var/<surface>/state/summary/cycle=<cycle>/asset=<asset>/latest.manifest.json`
  - `v2/var/<surface>/state/summary/cycle=<cycle>/asset=<asset>/snapshots/snapshot_ts=<ts>/summary.json`
  - `v2/var/<surface>/state/summary/cycle=<cycle>/asset=<asset>/snapshots/snapshot_ts=<ts>/manifest.json`

当前覆盖的核心数据集：

- `binance_klines_1m_source`
- `market_catalog_table`
- `direct_oracle_source`
- `settlement_truth_source`
- `oracle_prices_table`
- `truth_table`
- `chainlink_streams_source`
- `chainlink_datafeeds_source`
- `orderbook_index_table`
- `orderbook_depth_source`

当前 audit 结论至少会给出：

- `status`
  - `ok`
  - `warning`
  - `error`

- `critical_missing_datasets`
- `warning_missing_datasets`
- `duplicate_issue_datasets`
- `null_key_issue_datasets`
- `stale_issue_datasets`
- `low_row_count_issue_datasets`
- `dataset_error_datasets`
- `dataset_warning_datasets`
- `dataset_audits`
  - 每个 dataset 的 `min_row_count` / `max_age_seconds` / `max_partition_age_days` 检查结果
- `alignment_checks`
  - 目前至少覆盖：
    - `oracle_prices_table` 是否落后于 `direct_oracle_source`
    - `truth_table` 是否落后于 `settlement_truth_source`
    - live 面的 `orderbook_index_table` 是否落后于最新 `orderbook_depth_source`
- `completeness`
  - 汇总 healthy / warning / error / missing dataset 数量
  - 输出 `completeness_ratio` / `healthy_ratio`
  - 输出 `blocking_issue_count` / `warning_issue_count`
- `issues`
  - 把 missing / stale / row-count / alignment 等问题规整成稳定 issue inventory

当前 manifest 会额外提供：

- `object_type=data_summary_manifest`
- `paths`
  - 明确 summary / manifest 的 latest 与 snapshot 路径
- `expected_datasets`
  - 区分 critical / non-critical 期待集
- `dataset_inventory`
  - 给每个 dataset 一个更紧凑的 inventory 视图

当前 live 审计重点：

- Binance 1m 是否过期
- market catalog snapshot 是否过旧
- direct oracle source 是否过旧
- orderbook index / depth 是否仍在最新日期

当前 backtest 审计重点：

- 当前 canonical backtest 主线能不能直接消费
  - 当前主线优先看：
    - `binance_klines_1m_source`
    - `oracle_prices_table`
    - `truth_table`
- builder 侧输入是否仍然足够重建
  - 例如：
    - `market_catalog_table`
    - `chainlink_streams_source`
    - `settlement_truth_source`

这意味着当前 backtest completeness 的判断口径是：

- 如果 consumer tables 缺失
  - 直接算 blocking error
- 如果只是 builder 输入缺失，但当前 consumer tables 还在
  - 先记成 warning
  - 不默认阻断当前 canonical backtest 主线

### 7.1 市场目录

命令：

```bash
PYTHONPATH=v2/src python -m pm15min data sync market-catalog --market sol --cycle 15m --surface backtest
```

写出：

- snapshot
  - `v2/data/backtest/sources/polymarket/market_catalogs/cycle=15m/asset=sol/snapshot_ts=.../data.parquet`
- canonical
  - `v2/data/backtest/tables/markets/cycle=15m/asset=sol/data.parquet`

### 7.2 直接 Polymarket oracle price

命令：

```bash
PYTHONPATH=v2/src python -m pm15min data sync direct-oracle-prices --market sol --surface live
```

写出：

- `v2/data/live/sources/polymarket/oracle_prices/cycle=15m/asset=sol/data.parquet`

### 7.3 Streams RPC

命令：

```bash
PYTHONPATH=v2/src python -m pm15min data sync streams-rpc --market sol --surface backtest
```

写出：

- `v2/data/backtest/sources/chainlink/streams/asset=sol/year=2026/month=03/data.parquet`

### 7.3a Datafeeds RPC

命令：

```bash
PYTHONPATH=v2/src python -m pm15min data sync datafeeds-rpc --market sol --surface backtest
```

写出：

- `v2/data/backtest/sources/chainlink/datafeeds/asset=sol/year=2026/month=03/data.parquet`

### 7.4 Settlement truth RPC

命令：

```bash
PYTHONPATH=v2/src python -m pm15min data sync settlement-truth-rpc --market sol --surface backtest
```

写出：

- `v2/data/backtest/sources/polymarket/settlement_truth/cycle=15m/asset=sol/data.parquet`

### 7.5 Orderbook recorder

命令：

```bash
PYTHONPATH=v2/src python -m pm15min data record orderbooks --market sol --cycle 15m --surface live --loop --iterations 0
```

写出：

- raw depth
  - `v2/data/live/sources/polymarket/orderbooks/cycle=15m/asset=sol/date=2026-03-19/depth.ndjson.zst`
- index
  - `v2/data/live/tables/orderbook_index/cycle=15m/asset=sol/date=2026-03-19/data.parquet`
- state
  - `v2/var/live/state/orderbooks/cycle=15m/asset=sol/state.json`
- log
  - `v2/var/live/logs/data/recorders/cycle=15m/asset=sol/recorder.jsonl`

### 7.6 Live foundation runtime

命令：

```bash
PYTHONPATH=v2/src python -m pm15min data run live-foundation --market sol --iterations 1
```

loop 版本：

```bash
PYTHONPATH=v2/src python -m pm15min data run live-foundation --market sol --loop --iterations 0
```

职责：

- 刷新 active market catalog
- 刷新 canonical Binance 1m
- 刷新 direct Polymarket oracle source
- 重建 canonical `oracle_prices_15m`
- 触发一次 canonical orderbook recorder

写出：

- foundation state
  - `v2/var/live/state/foundation/cycle=15m/asset=sol/state.json`
- foundation log
  - `v2/var/live/logs/data/foundation/cycle=15m/asset=sol/refresh.jsonl`

说明：

- one-shot 适合手动补齐当前窗口
- loop 适合让 `signal / quote / decision` 长时间保持同一时间语义

### 7.7 Oracle builder

命令：

```bash
PYTHONPATH=v2/src python -m pm15min data build oracle-prices-15m --market sol --surface live
```

写出：

- canonical oracle table
  - `v2/data/live/tables/oracle_prices/cycle=15m/asset=sol/data.parquet`

### 7.7 Truth builder

命令：

```bash
PYTHONPATH=v2/src python -m pm15min data build truth-15m --market sol --surface backtest
```

写出：

- canonical truth table
  - `v2/data/backtest/tables/truth/cycle=15m/asset=sol/data.parquet`

### 7.8 Exports

命令：

```bash
PYTHONPATH=v2/src python -m pm15min data export oracle-prices-15m --market sol --surface live
PYTHONPATH=v2/src python -m pm15min data export truth-15m --market sol --surface backtest
```

写出：

- `v2/data/live/exports/oracle_prices/cycle=15m/asset=sol/oracle_prices.csv`
- `v2/data/backtest/exports/truth/cycle=15m/asset=sol/truth.csv`

---

## 8. 优先级和回退规则

### 8.1 `oracle_prices_15m`

优先级固定为：

1. direct Polymarket source
2. streams source

更细一点：

- `price_to_beat`
  - 优先 `direct_oracle_source.price_to_beat`
  - 否则回退 `streams` 在 `cycle_start_ts` 的 price

- `final_price`
  - 优先 `direct_oracle_source.final_price`
  - 否则回退 `streams` 在 `cycle_end_ts` 的 price

### 8.2 `truth_15m`

优先级固定为：

1. settlement truth
2. oracle prices

这代表：

- 如果链上 resolution + exact stream match 已经有了，就不允许被 oracle price 推翻
- oracle price 只做缺口兜底

---

## 9. 旧路径到新路径映射

### 9.1 市场目录

旧：

```text
data/markets/sol/data/polymarket/markets/all/...
data/markets/_shared/oracle/polymarket_stream_15m_markets*.csv
```

新：

```text
v2/data/<surface>/sources/polymarket/market_catalogs/cycle=15m/asset=sol/snapshot_ts=.../data.parquet
v2/data/<surface>/tables/markets/cycle=15m/asset=sol/data.parquet
```

### 9.2 direct Polymarket oracle prices

旧：

```text
data/markets/_shared/oracle/polymarket_oracle_prices_15m_*.csv
data/markets/_shared/oracle/_autofetch/.../*.csv
data/markets/_shared/oracle/manual/*.csv
```

新：

```text
v2/data/live/sources/polymarket/oracle_prices/cycle=15m/asset=<asset>/data.parquet
v2/data/live/tables/oracle_prices/cycle=15m/asset=<asset>/data.parquet
```

### 9.3 orderbook depth

旧：

```text
data/markets/<asset>/data/polymarket/raw/orderbooks_full/orderbook_depth_YYYYMMDD.ndjson.zst
```

新：

```text
v2/data/live/sources/polymarket/orderbooks/cycle=15m/asset=<asset>/date=YYYY-MM-DD/depth.ndjson.zst
```

### 9.4 streams

旧：

```text
data/markets/_shared/oracle/streams_reports_registry_all_*.csv
```

新：

```text
v2/data/backtest/sources/chainlink/streams/asset=<asset>/year=YYYY/month=MM/data.parquet
```

### 9.5 settlement truth

旧：

```text
data/markets/_shared/oracle/settlement_truth_15m_*/polymarket_15m_settlement_truth.csv
```

新：

```text
v2/data/backtest/sources/polymarket/settlement_truth/cycle=15m/asset=<asset>/data.parquet
```

---

## 10. 代码边界

`v2/src/pm15min/data/` 内部结构必须保持：

```text
data/
  cli.py
  config.py
  layout.py
  contracts.py
  io/
  sources/
  pipelines/
  queries/
```

规则：

- `sources/` 只和外部系统通信
- `pipelines/` 只做 source -> table / table -> export
- `queries/` 只读
- `io/` 只做文件格式和原子写入

严禁：

- `data` import legacy repo 模块
- `data` import `v2/live` / `v2/research`
- `live` 直接读 `exports/`

---

## 11. 当前实现状态

已实现：

- `surface=live|backtest` 分面
- Binance `klines_1m` clean-room direct sync
- `market_catalog` snapshot + canonical
  - `backtest` 走 closed history
  - `live` 走 active market catalog
- legacy market catalog -> canonical import
- Polymarket orderbook direct source
- orderbook index
  - 支持从 canonical `depth.ndjson.zst` 回填
- legacy orderbook depth -> canonical source import
- recorder runtime + state + jsonl logs
- live foundation runtime
  - market catalog + binance + direct oracle + orderbooks
- streams RPC sync
- datafeeds RPC sync
- settlement truth RPC sync
- direct Polymarket oracle price sync
- `oracle_prices_15m` canonical builder
- `truth_15m` canonical builder
- truth / oracle exports
- data summary / audit state
  - 已包含 freshness / row-count / source-table lag 的第一批规则
  - 已包含 summary manifest / completeness report 第一版

未实现或待继续：

- 5m oracle/truth canonical tables
- 更细粒度 completeness 规则可继续按实盘反馈扩展

---

## 12. 开发时必须遵守的检查表

新增任何 data 代码前先检查：

1. 它属于 `sources`、`tables`、`exports` 里的哪一层？
2. 它属于 `live` 还是 `backtest`？
3. 它的 canonical 主键是什么？
4. 它是否已经有固定路径，不需要发明新目录？
5. 它是否会被 live/research 直接依赖？
6. 如果会，被依赖的是不是 `tables/` 而不是 `sources/`？
7. 它是否把 runtime 文件错误地写进了 `data/` 而不是 `var/`？

只要这 7 条里有 1 条答不上来，就不要落代码。
