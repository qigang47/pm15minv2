# 地址交易复盘：2026-03-15 之后

## 范围与口径

- 地址：`0x4520...37f8`
- 时间范围：`2026-03-15 00:00 UTC` 到 `2026-03-30`
- 数据来源：
  - Polymarket Data API：`activity`、`positions`、`closed-positions`
  - 本地策略配置：`src/pm15min/live/profiles/catalog.py`
  - 本地模型诊断：`research/model_bundles/.../diagnostics/metrics.json`
  - 本地训练 OOF：`research/training_runs/.../oof_predictions.parquet`
  - 本地标签：`research/label_frames/cycle=15m/.../label_set=truth/data.parquet`

本报告使用以下地址级结算规则：

1. 按 `conditionId + outcomeIndex` 聚合买入记录。
2. 若 `closed-positions` 能匹配到同一 `conditionId + outcomeIndex`，记为 `redeemed_closed`。
3. 若 `activity` 里出现 `REDEEM`，且该 `conditionId` 只买过单边，记为 `redeemed_by_condition`。
4. 若市场已结束、当前持仓价值为 `0`、也没有可归因的 redeem 回款，记为 `no_redeem_loss`。
5. 当天仍未结束的市场，记为 `pending_today_or_later`。

说明：

- 这段地址历史里只有 `BUY` 和 `REDEEM`，没有 `SELL`，所以“没 redeem 回来就是输了”这个口径基本成立。
- 有 `2` 个 `conditionId` 出现了双边都买，导致其中 `4` 个组的 condition 级 redeem 归因存在歧义；报告里已单独标记为 caveat，但不改变主结论。

## 地址级结果

### 总览

- 交易组数：`120`
- 已结算：`114`
- 未结算或状态缺口：`6`
- 按 redeem 口径记为赢单：`37`
- 按 redeem 口径记为输单：`77`
- 结算胜率：`32.46%`
- 总投入：`563.91 USDC`
- 总回款：`666.99 USDC`
- 净收益：`+106.54 USDC`
- 总 ROI：`+18.89%`

状态分布：

- `no_redeem_loss`：`77`
- `redeemed_closed`：`25`
- `redeemed_by_condition`：`12`
- `pending_today_or_later`：`4`
- `missing_state`：`2`

### 分资产分方向

| asset | outcome | positions | wins | cost_usdc | recovered_usdc | net_pnl_usdc | losses | win_rate_pct | roi_pct |
|:--|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| btc | Up | 10 | 1 | 26.0000 | 7.9297 | -17.8480 | 9 | 10.00 | -68.65 |
| eth | Down | 1 | 1 | 2.0000 | 7.6211 | 5.6211 | 0 | 100.00 | 281.06 |
| eth | Up | 17 | 4 | 40.4600 | 28.3143 | -11.8317 | 13 | 23.53 | -29.24 |
| sol | Down | 28 | 9 | 146.2357 | 158.5188 | 13.1071 | 19 | 32.14 | 8.96 |
| sol | Up | 7 | 1 | 63.7900 | 26.7857 | -36.5871 | 6 | 14.29 | -57.36 |
| xrp | Down | 45 | 20 | 232.4277 | 418.1530 | 186.9615 | 25 | 44.44 | 80.44 |
| xrp | Up | 6 | 1 | 53.0000 | 19.6759 | -32.8873 | 5 | 16.67 | -62.05 |

### 直接结论

- 盈利几乎全部来自 `XRP Down`。
- `SOL Down` 小幅赚钱，但稳定性一般。
- `BTC/ETH/SOL/XRP` 的 `Up` 侧整体都差，尤其 `BTC Up`、`SOL Up`、`XRP Up`。
- 你的实盘体验“`Down` 好用，`Up` 不好用”在地址流水层面是成立的。

### Deep OTM 语义更正

- 这里不能按普通“方向延续”来理解。`deep_otm_baseline` 买的是便宜尾部票，赚钱依赖的是“窗口内先偏离，结算时再反着回来”。
- 买 `Down` 能赚钱，通常是窗口内价格先往上走，让 `Down` 变便宜，但最后又回落到 `Down` 结算。
- 买 `Up` 能赚钱，通常是窗口内价格先往下走，让 `Up` 变便宜，但最后必须在剩余几分钟里拉回到 `Up` 结算。
- 所以真正该问的不是“模型是否天然偏 `Down`”，而是“它是不是更会识别 `Up` 反转不会完成，却不够会识别 `Up` 反转最终会完成”。

从 `2026-03-15` 起的 `40` 个已结算 `Up` 实盘窗口看，baseline 的行为非常不对称：

| sample | pred DOWN n | pred DOWN correct | pred DOWN acc | pred UP n | pred UP correct | pred UP acc |
|:--|--:|--:|--:|--:|--:|--:|
| all settled bought UP | 30 | 25 | 83.33% | 10 | 2 | 20.00% |
| local_feature_frame bought UP | 10 | 9 | 90.00% | 6 | 1 | 16.67% |

这说明：

- baseline 在 `Up` setup 里更像一个“反转否决器”，而不是一个“反转确认器”。
- 它大部分时候把这些 `Up` 机会判回 `Down`，而且通常判对。
- 真正放行 `Up` 的时候，准确率反而很差。

在本地原生特征窗口里，最干净的对比是：

| local bought-UP slice | n | ret_from_cycle_open | first_half_ret | q_bs_up_strike | cycle_range_pos | bb_pos_20 | macd_z |
|:--|--:|--:|--:|--:|--:|--:|--:|
| pred DOWN and correct | 9 | -0.001259 | -0.001339 | 0.372422 | 0.278376 | -0.073695 | -0.423379 |
| pred UP and wrong | 5 | 0.000391 | -0.000579 | 0.635411 | 0.797645 | 0.328720 | 0.991203 |

含义很直接：

- 当价格仍然偏低、区间位置偏下、动量仍偏负时，baseline 往往继续判 `Down`，而且多数时候是对的。
- 当价格已经在 `7/8/9` 分钟附近反弹到窗口高位、`q_bs_up_strike` 和 `macd_z` 都明显转强时，baseline 才更容易翻到 `Up`。
- 但对 deep OTM 来说，这种“晚段反弹已经看起来不错”的时刻，恰恰最容易高估终点反转，因为留给最终结算的时间已经不多了。
- 所以 `Down` 看起来更稳，不是因为模型天然会做空，而是因为它更会识别“这类 `Up` 反转其实走不完”。

### 尚未结算的单

| asset | outcome | cost_usdc | title |
|:--|:--|--:|:--|
| xrp | Up | 1.5 | XRP Up or Down - March 30, 5:15AM-5:30AM ET |
| eth | Up | 2.0 | Ethereum Up or Down - March 30, 4:45AM-5:00AM ET |
| sol | Up | 2.0 | Solana Up or Down - March 30, 3:15AM-3:30AM ET |
| xrp | Up | 1.5 | XRP Up or Down - March 30, 2:45AM-3:00AM ET |

## 原因分析

### 1. 策略层

#### 1.1 这不是对称做多空策略，而是“低价尾部票筛选”

`deep_otm_baseline` 的关键配置：

- `entry_price_min = 0.01`
- `entry_price_max = 0.30`
- `min_dir_prob_default = 0.60`
- `min_net_edge_default = 0.0`
- `min_net_edge_by_offset = {7: 0.0, 8: 0.0, 9: 0.0}`
- `offsets = (7, 8, 9)`

这意味着 baseline 的核心不是“选方向”，而是：

- 只做价格不高于 `0.30` 的便宜票
- 只要求方向概率过线
- 几乎不要求额外净边际

从地址真实成交均价看，这个特征很明显：

| asset | outcome | positions | cost_usdc | shares | avg_entry |
|:--|:--|--:|--:|--:|--:|
| btc | Up | 11 | 26.0448 | 99.7243 | 0.2612 |
| eth | Down | 1 | 2.0000 | 7.6211 | 0.2624 |
| eth | Up | 18 | 42.4600 | 163.4840 | 0.2597 |
| sol | Down | 29 | 148.3357 | 1162.9200 | 0.1276 |
| sol | Up | 8 | 65.7900 | 368.4400 | 0.1786 |
| xrp | Down | 45 | 232.4277 | 887.6080 | 0.2619 |
| xrp | Up | 8 | 56.0000 | 215.3720 | 0.2600 |

结论：

- 地址上的实盘基本都在买 `0.13` 到 `0.26` 这一档的便宜票。
- 这类票天然不是对称机会。
- 一旦某个方向只是“便宜”但没有真实 edge，连续亏损会很快发生。

#### 1.2 方向预算严重不均衡

从已结算记录看，实际下注方向非常偏：

- `BTC`：几乎只做 `Up`
- `ETH`：几乎只做 `Up`
- `SOL`：明显偏 `Down`
- `XRP`：强烈偏 `Down`

这不是“市场给了你对称机会，你两边都做”，而是：

- 你把 `BTC/ETH` 当成 `Up` 桶
- 把 `XRP/SOL` 当成 `Down` 桶

所以组合收益不是“模型平均能力”，而是“资产和方向配对是否刚好踩中 regime”。

#### 1.3 baseline 没有把“分类对”转成“交易值”

baseline 里 `min_net_edge_by_offset = 0`，本质上是在说：

- 只要方向概率足够，就允许交易
- 但没有强制要求 `p_side - price - fee` 留出足够安全垫

这会导致：

- 统计上能分类对的票，不一定有正的交易期望
- 便宜票尤其容易出现“胜率不够覆盖赔率结构”的问题

### 2. 因子层

#### 2.1 因子过度集中在“strike 相对位置”

四个资产、三个 offset 的正向因子几乎都被同一组主导：

- `q_bs_up_strike`
- `bb_pos_20`
- `cycle_range_pos`
- `ret_from_cycle_open`
- `ret_from_strike`
- `first_half_ret`

其中 `q_bs_up_strike` 在所有资产上都是第一大正向因子，强度远高于其他因子。

这说明当前因子栈在做的事情更像：

- 读价格相对 strike 的位置
- 读价格在区间中的站位
- 读短周期 continuation / extension

而不是：

- 读“这个便宜票到底是不是假便宜”
- 读“这个反向单有没有真实回归条件”
- 读“当前盘口/流动性/事件状态下，便宜票是否只是垃圾尾部”

#### 2.2 缺少专门保护 `Up` 便宜票的因子

现在的负向因子主要是：

- `volume_z`
- `volume_z_3`
- `dow_sin`
- `adx_14`
- `regime_trend`
- `trade_intensity`

这些因子对噪音和时段有一定惩罚，但不够回答一个关键问题：

“当前这个 `Up` 便宜票，是有 reversal 机会，还是只是单纯快输完了？”

这正是地址表现里最明显的问题：

- `XRP Down` 可以赚钱
- `BTC/ETH/SOL/XRP Up` 大多不行

因子层的症结不是完全没信息，而是：

- 因子足够做排序
- 但不够做“低价尾部票的生死判别”

### 3. 模型层

#### 3.1 模型本身没有天然的 `Down` 偏置

active baseline bundle 的 OOF 表现，`UP/DOWN` 精度几乎是对称的：

| asset | pred_side | n | precision_pct | avg_conf |
|:--|:--|--:|--:|--:|
| btc | DOWN | 14680 | 76.94 | 0.7482 |
| btc | UP | 14480 | 77.51 | 0.7539 |
| eth | DOWN | 14658 | 78.13 | 0.7605 |
| eth | UP | 14523 | 78.50 | 0.7641 |
| sol | DOWN | 14865 | 77.15 | 0.7497 |
| sol | UP | 14107 | 78.25 | 0.7624 |
| xrp | DOWN | 14857 | 78.72 | 0.7666 |
| xrp | UP | 14147 | 78.68 | 0.7659 |

模型诊断里 AUC 也很高：

- `btc`: `0.8266` 到 `0.8780`
- `eth`: `0.8370` 到 `0.8870`
- `sol`: `0.8345` 到 `0.8830`
- `xrp`: `0.8424` 到 `0.8878`

结论：

- 模型不是“学会了只会做 Down”
- 训练层并没有出现明显的单边崩坏

#### 3.2 真正的问题是“分类目标”和“交易目标”不一致

当前模型优化的是：

- AUC
- Brier
- Logloss

这些指标回答的是：

- 概率排序好不好
- 概率校准稳不稳

但它们不直接回答：

- 买入 `0.26` 的 `Up` 有没有正期望
- 手续费之后值不值得买
- 同样 78% 的分类精度，在不同 payoff 桶里能不能赚钱

所以这里更准确的表述是：

- 模型是“统计上有用”
- 但没有直接对“可交易 EV”建模

#### 3.3 地址收益说明问题主要发生在“模型输出到下单”的转换

如果模型天然偏 `Down`，那么训练 OOF 上应该能看到 `UP` 精度明显更差。  
但实际没有。

因此实盘里 `UP` 差、`DOWN` 好，更像是：

- 策略阈值筛出来的交易集合不对称
- 市场报价对 `UP` 侧更不友好
- 组合配置把 `BTC/ETH` 几乎都压在 `Up` 上

### 4. 市场层

#### 4.1 从标签看，市场本身并没有普遍强烈偏 `Down`

从 `2026-03-15` 起的 15m truth 标签分布：

| asset | rows | up | down | up_pct | down_pct |
|:--|--:|--:|--:|--:|--:|
| btc | 1299 | 654 | 645 | 50.35 | 49.65 |
| eth | 1299 | 652 | 647 | 50.19 | 49.81 |
| sol | 1298 | 649 | 649 | 50.00 | 50.00 |
| xrp | 1299 | 627 | 672 | 48.27 | 51.73 |

市场层含义：

- `BTC/ETH/SOL` 基本是 50/50
- 只有 `XRP` 有温和的 `Down` 偏置

所以地址表现不是“因为整个市场都在跌，所以 Down 天然赚钱”。

真正发生的是：

- 你在 `BTC/ETH` 这种近似 50/50 的市场里，主要押了 `Up`
- 你在 `XRP` 这种略偏 `Down` 的市场里，重仓押了 `Down`

也就是说，收益主要来自：

- `XRP Down` 的 regime 命中

而亏损主要来自：

- `BTC/ETH Up` 在均衡市场里没有稳定 edge
- `SOL/XRP Up` 的 cheap tail 反弹不成立

#### 4.2 市场微结构可能对 `Down` 更友好

地址结果显示：

- `XRP Down` 的 ROI 高达 `+80.44%`
- `SOL Down` 也为正
- 但 `UP` 基本都差

这通常意味着：

- 某些资产在这段时间的 15m 市场里，`Down` 方向更容易出现可兑现的尾部定价错误
- `Up` 方向虽然也有低价票，但很多只是“低价”，不是真低估

因此 `Down` 的优势更像是：

- 特定时间窗
- 特定资产
- 特定市场结构

而不是一个可直接泛化到所有资产和方向的永久规律。

## 归因结论

### 最可能的主因排序

1. `策略问题`  
   当前 baseline 实际上是“低价尾部票策略”，而不是对称方向策略；组合分配又把 `BTC/ETH` 主要押在 `Up`，把 `XRP` 主要押在 `Down`，结果收益完全取决于 regime 是否踩中。

2. `市场问题`  
   `2026-03-15` 之后真正有稳定优势的是 `XRP Down`，而不是整个市场普遍偏空。你把大量资金放进了没有结构性优势的 `Up` 桶。

3. `因子问题`  
   因子过度依赖 `q_bs_up_strike` 和区间位置类特征，缺少对“便宜票是否真有 reversal edge”的专门过滤。

4. `模型问题`  
   模型统计精度并不差，也没有明显 `Down` 偏置；问题更多出现在目标函数和交易转化层，而不是分类器本身崩了。

## 建议

### 策略

- 把 `Up` 和 `Down` 彻底拆成两套预算，不要共用一个 direction sleeve。
- 给 `BTC/ETH Up` 单独做 kill switch；在重新验证前应视为暂停方向。
- baseline 不应继续使用 `min_net_edge = 0`；至少要引入手续费后 EV 下限。
- 对 `entry_price <= 0.30` 再分层，不要把所有便宜票当成同一类机会。

### 因子

- 降低 `q_bs_up_strike` 的支配权，避免单一 moneyness 因子压倒其他信息。
- 增加专门识别“假便宜 `Up`”的因子：
  - 反弹确认
  - 短时 order flow 反转
  - 尾部流动性修复
  - 报价质量和盘口厚度

### 模型

- 不要只优化分类指标，加入交易目标：
  - `pnl-weighted`
  - `edge-weighted`
  - `fee-adjusted EV`
- 做 `asset x side` 分桶校准，不要只做统一概率校准。
- 给 `Up` 建 side-specific meta filter，而不是继续和 `Down` 共用同一决策头。

### 市场

- 当前最值得单独保留的是 `XRP Down`，其次是 `SOL Down`。
- `BTC/ETH Up` 在这段时间没有被市场证明有效，应视为失效桶。
- 每天滚动监控 `asset x side` 的：
  - 胜率
  - ROI
  - 平均买入价
  - 回款率

## 一句话总结

这段时间地址层面的真实问题不是“模型只会做 Down”，而是：

`模型统计上并不天然偏 Down，但在 deep OTM 语义下，它更会否决“窗口内下跌后最终能拉回的 Up 反转”，却不够会确认这类反转真的能走完；于是 Down 侧更像是在吃“反转失败”，Up 侧则持续被晚段假反弹误导。`
