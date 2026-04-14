# 当前正确路径记录

更新时间：2026-04-12

这份文档记录当前真正生效的主路径。
默认以服务器路径为准。

## 主仓库路径

- 服务器主路径：`/home/huatai/qigang/pm15min/v2`
- 本机镜像路径：`/Users/gangqi/Downloads/poly/v2`

## 当前主用研究入口

- 研究入口：`/home/huatai/qigang/pm15min/v2/auto_research/program.md`
- 当前主会话目录：`/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch`
- 当前主会话说明：`/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/session.md`
- 当前主结果表：`/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/results.tsv`
- 当前重启批次日志目录：`/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/bootstrap`
- 后台自动研究目录：`/home/huatai/qigang/pm15min/v2/var/research/autorun`
- 4 币持续保活脚本：`/home/huatai/qigang/pm15min/v2/auto_research/bootstrap_keepalive.sh`
- 4 币持续保活状态目录：`/home/huatai/qigang/pm15min/v2/var/research/bootstrap-keepalive`

## 当前主会话归档

- 本轮重开前的旧会话归档：`/home/huatai/qigang/pm15min/v2/sessions/archive/deep_otm_baseline_40factor_2usd_max5_autoresearch_20260412T123347Z`
- 本轮清理掉的旧空壳运行归档根目录：`/home/huatai/qigang/pm15min/v2/research/experiments/runs_archive`

## 当前 4 币主用实验

### BTC

- 套件文件：`/home/huatai/qigang/pm15min/v2/research/experiments/suite_specs/baseline_focus_feature_search_btc_reversal_40plus_2usd_5max_20260409.json`
- 当前运行标签：`auto_btc_40plus_2usd_5max_reset_20260412`
- 当前运行目录：`/home/huatai/qigang/pm15min/v2/research/experiments/runs/suite=baseline_focus_feature_search_btc_reversal_40plus_2usd_5max_20260409/run=auto_btc_40plus_2usd_5max_reset_20260412`

### ETH

- 套件文件：`/home/huatai/qigang/pm15min/v2/research/experiments/suite_specs/baseline_focus_feature_search_eth_reversal_40plus_2usd_5max_20260409.json`
- 当前运行标签：`auto_eth_40plus_2usd_5max_reset_20260412`
- 当前运行目录：`/home/huatai/qigang/pm15min/v2/research/experiments/runs/suite=baseline_focus_feature_search_eth_reversal_40plus_2usd_5max_20260409/run=auto_eth_40plus_2usd_5max_reset_20260412`

### SOL

- 当前主用纯 40 因子套件文件：`/home/huatai/qigang/pm15min/v2/research/experiments/suite_specs/baseline_focus_feature_search_sol_reversal_40main_2usd_5max_20260412.json`
- 当前运行标签：`auto_sol_40main_2usd_5max_frozen_20260412_cycle002`
- 当前运行目录：`/home/huatai/qigang/pm15min/v2/research/experiments/runs/suite=baseline_focus_feature_search_sol_reversal_40main_2usd_5max_20260412/run=auto_sol_40main_2usd_5max_frozen_20260412_cycle002`

### XRP

- 套件文件：`/home/huatai/qigang/pm15min/v2/research/experiments/suite_specs/baseline_focus_feature_search_xrp_reversal_40plus_2usd_5max_20260409.json`
- 当前运行标签：`auto_xrp_40plus_2usd_5max_reset_20260412`
- 当前运行目录：`/home/huatai/qigang/pm15min/v2/research/experiments/runs/suite=baseline_focus_feature_search_xrp_reversal_40plus_2usd_5max_20260409/run=auto_xrp_40plus_2usd_5max_reset_20260412`

## 当前 15m 回测主数据路径

以下 4 类路径现在都已经补齐到当前冻结窗口，可直接作为主路径使用。

### BTC

- 市场表：`/home/huatai/qigang/pm15min/v2/data/backtest/tables/markets/cycle=15m/asset=btc/data.parquet`
- 直连价格：`/home/huatai/qigang/pm15min/v2/data/backtest/sources/polymarket/oracle_prices/cycle=15m/asset=btc/data.parquet`
- 真值表：`/home/huatai/qigang/pm15min/v2/data/backtest/tables/truth/cycle=15m/asset=btc/data.parquet`
- 研究标签：`/home/huatai/qigang/pm15min/v2/research/label_frames/cycle=15m/asset=btc/label_set=truth/data.parquet`

### ETH

- 市场表：`/home/huatai/qigang/pm15min/v2/data/backtest/tables/markets/cycle=15m/asset=eth/data.parquet`
- 直连价格：`/home/huatai/qigang/pm15min/v2/data/backtest/sources/polymarket/oracle_prices/cycle=15m/asset=eth/data.parquet`
- 真值表：`/home/huatai/qigang/pm15min/v2/data/backtest/tables/truth/cycle=15m/asset=eth/data.parquet`
- 研究标签：`/home/huatai/qigang/pm15min/v2/research/label_frames/cycle=15m/asset=eth/label_set=truth/data.parquet`

### SOL

- 市场表：`/home/huatai/qigang/pm15min/v2/data/backtest/tables/markets/cycle=15m/asset=sol/data.parquet`
- 直连价格：`/home/huatai/qigang/pm15min/v2/data/backtest/sources/polymarket/oracle_prices/cycle=15m/asset=sol/data.parquet`
- 真值表：`/home/huatai/qigang/pm15min/v2/data/backtest/tables/truth/cycle=15m/asset=sol/data.parquet`
- 研究标签：`/home/huatai/qigang/pm15min/v2/research/label_frames/cycle=15m/asset=sol/label_set=truth/data.parquet`

### XRP

- 市场表：`/home/huatai/qigang/pm15min/v2/data/backtest/tables/markets/cycle=15m/asset=xrp/data.parquet`
- 直连价格：`/home/huatai/qigang/pm15min/v2/data/backtest/sources/polymarket/oracle_prices/cycle=15m/asset=xrp/data.parquet`
- 真值表：`/home/huatai/qigang/pm15min/v2/data/backtest/tables/truth/cycle=15m/asset=xrp/data.parquet`
- 研究标签：`/home/huatai/qigang/pm15min/v2/research/label_frames/cycle=15m/asset=xrp/label_set=truth/data.parquet`

## 当前确认有效的冻结窗口

- 训练窗口截止：`2026-03-27`
- 决策/回放窗口：`2026-03-28` 到 `2026-04-10`

截至这次修复完成时，上面 4 个币在这个冻结窗口内：

- 市场表缺口：`0`
- 直连价格缺口：`0`
- 真值表缺口：`0`
- 研究标签缺口：`0`

## 已退役或不要再用的旧线

- 34 因子延续线：不再是本轮主线
- 38 band 延续线：不再是本轮主线
- 旧 `XRP 38band` 空壳运行目录：已归档，不再作为当前主路径
- 旧 `SOL 40plus` 混入 39 因子版本：已停用，当前以 `40main` 为准

## 一句话原则

以后如果要查当前服务器真正生效的路径，优先看这 4 类：

1. `auto_research/program.md`
2. `sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/`
3. `research/experiments/runs/suite=.../run=...`
4. `data/backtest/...` 和 `research/label_frames/...`
