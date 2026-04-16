# 当前正确路径记录

更新时间：2026-04-14

这份文档记录当前真正生效的主路径。
需要区分“本地仓库最新布局”和“服务器当前已部署布局”。

## 主仓库路径

- 服务器主路径：`/home/huatai/qigang/pm15min/v2`
- 本机镜像路径：`/Users/gangqi/Downloads/poly/v2`

## 本地仓库最新研究入口

- 本地研究入口：`/Users/gangqi/Downloads/poly/v2/auto_research/program.md`
- 本地控制面脚本目录：`/Users/gangqi/Downloads/poly/v2/auto_research/`
- 本地项目说明文件：`/Users/gangqi/Downloads/poly/v2/research/AGENTS.md`

## 服务器当前已部署研究入口

- 服务器研究入口：`/home/huatai/qigang/pm15min/v2/scripts/research/program.md`
- 服务器控制面脚本目录：`/home/huatai/qigang/pm15min/v2/scripts/research/`
- 服务器项目说明文件：`/home/huatai/qigang/pm15min/v2/AGENTS.md`
- 说明：服务器目前还没有 `auto_research/` 目录；如果要切到本地这套新布局，需要同步整套 `auto_research/`、相关 `src/` 变更和新的 `research/AGENTS.md`

## 当前主会话和运行时目录

- 当前主会话目录：`/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch`
- 当前主会话说明：`/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/session.md`
- 当前主结果表：`/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/results.tsv`
- 当前重启批次日志目录：`/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/bootstrap`
- 后台自动研究目录：`/home/huatai/qigang/pm15min/v2/var/research/autorun`
- 本地 4 币持续保活脚本：`/Users/gangqi/Downloads/poly/v2/auto_research/bootstrap_keepalive.sh`
- 4 币持续保活状态目录：`/home/huatai/qigang/pm15min/v2/var/research/bootstrap-keepalive`

## Dense Dual-Track Sessions

- Direction dense 会话目录：`/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_direction_dense_autoresearch`
- Reversal dense 会话目录：`/home/huatai/qigang/pm15min/v2/sessions/deep_otm_baseline_reversal_dense_autoresearch`
- Direction dense 运行时目录：`/home/huatai/qigang/pm15min/v2/var/research/autorun/direction_dense`
- Reversal dense 运行时目录：`/home/huatai/qigang/pm15min/v2/var/research/autorun/reversal_dense`
- 共享 dense 队列：`/home/huatai/qigang/pm15min/v2/var/research/autorun/experiment-queue.json`

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

## 本轮目标冻结窗口

- 训练窗口截止：`2026-03-31`
- 决策/回放窗口：`2026-04-01` 到 `2026-04-15`

当前服务器后续补数、补标签、重跑自动研究时，都应以这个窗口为准。
只有在 K 线、真值表、研究标签都补到这个窗口末端之后，才算重新验证完成。

## 已退役或不要再用的旧线

- 34 因子延续线：不再是本轮主线
- 38 band 延续线：不再是本轮主线
- 旧 `XRP 38band` 空壳运行目录：已归档，不再作为当前主路径
- 旧 `SOL 40plus` 混入 39 因子版本：已停用，当前以 `40main` 为准

## 一句话原则

以后如果要查“当前服务器真正生效的路径”，优先看这 4 类：

1. `scripts/research/program.md`
2. `sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/`
3. `research/experiments/runs/suite=.../run=...`
4. `data/backtest/...` 和 `research/label_frames/...`

如果要查“本地仓库下一次准备部署的新布局”，优先看：

1. `auto_research/program.md`
2. `auto_research/`
3. `research/AGENTS.md`
4. `src/`
