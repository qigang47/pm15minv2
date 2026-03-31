# Deep OTM Baseline Up 诊断

## 目的

这份文档总结当前 `deep_otm_baseline` 实盘里 `Up` 方向表现差的主要原因，并把 `bs_q_replace_direction` 这套实际在用的 `64` 个因子，按“建议保留 / 建议压缩 / 明显缺失”做一轮整理。

这不是一次新的回测报告，而是基于现有成交对齐结果做的策略诊断，重点回答三个问题：

1. 问题更像阈值问题，还是因子问题。
2. 问题更像模型问题，还是模型学到了错误的证据。
3. 现有因子哪些该保留，哪些更像重复表达，哪些关键因子目前缺失。

## 使用的数据

- 实盘成交窗口对齐结果：
  - `research/address_trade_windows/run=20260330_start=20260315_profile=deep_otm_baseline/trade_windows.csv`
- 成交窗口对应因子权重：
  - `research/address_trade_windows/run=20260330_start=20260315_profile=deep_otm_baseline/trade_factor_weights_long.csv`
- 地址级分析摘要：
  - `research/address_trade_windows/run=20260330_start=20260315_profile=deep_otm_baseline/report.md`
  - `research/address_trade_windows/run=20260330_start=20260315_profile=deep_otm_baseline/factor_side_analysis.md`
- live 概率门槛逻辑：
  - `src/pm15min/live/guards/__init__.py`
- 当前 live profile：
  - `src/pm15min/live/profiles/catalog.py`
- 实际 feature set 定义：
  - `src/pm15min/research/features/registry.py`

## 先说结论

### 1. 这不是一句“阈值太低”能解释的问题

当前 live 不是简单拿原始概率和 `0.6` 比，而是用更保守的区间概率过门槛。

但即使这样，仍然存在“模型当时非常看好 `Up`，最后还是错”的窗口。

典型例子：

- `XRP Up or Down - March 23, 5:15AM-5:30AM ET`
  - `p_signal = 0.9068`
  - `predicted_prob = 0.9420`
  - 最终仍然错误
- `Solana Up or Down - March 21, 5:45AM-6:00AM ET`
  - `p_signal = 0.8270`
  - `predicted_prob = 0.7755`
  - 最终仍然错误

这说明单纯把 `0.6` 提高到 `0.65` 或 `0.7`，不能真正切掉最危险的错单。

### 2. 主因更像因子问题，不像模型结构问题

“模型看 `Up` 但最后错”的窗口里，经常不是单边模型单独失误，而是 `LGB` 和 `LR` 都一起给出较高 `Up` 概率。

这更像：

- 两个模型同时被同一组证据说服
- 证据本身容易把“局部反弹”误当成“最终能翻盘”

所以更准确的说法不是“模型坏了”，而是：

`当前最强的那批因子，更擅长识别“窗口后段已经开始反弹”，不擅长识别“这次反弹是否足够早、足够强、足够持续，最终能收成 Up”。`

### 3. 还有一块单独存在的实盘方向对齐问题

已结算的 `Up` 窗口中，亏损单不只来自“模型高概率看多却看错”。

更大的一部分来自：

- 实际买了 `Up`
- 但匹配到的模型当时更偏 `Down`

这部分不能简单算成模型预测失败，说明除了因子问题之外，还存在一块需要单独追的实盘方向对齐问题。

## 关键观察

### 1. 已结算 `Up` 窗口里，模型看 `Up` 的正确率确实低，模型看 `Down` 的正确率明显高

在这批已结算 `Up` 窗口里：

- 模型看 `Down`：`30` 次，其中 `25` 次正确，正确率 `83.3%`
- 模型看 `Up`：`10` 次，其中 `2` 次正确，正确率 `20.0%`

只看更干净的本地特征窗口时，这个对比更明显：

- 模型看 `Down`：`10` 次，其中 `9` 次正确，正确率 `90.0%`
- 模型看 `Up`：`6` 次，其中 `1` 次正确，正确率 `16.7%`

这说明当前 baseline 在 deep OTM 语义下，更像：

- `Up` 反转否决器

而不像：

- `Up` 反转确认器

### 2. 问题不是“模型从不看 `Up`”

问题不在于模型完全不会翻去看 `Up`。相反，它有时会在很高概率下看 `Up`，而且错得很重。

所以真正的问题不是：

- “阈值放太低，什么都进”

而更像是：

- “一旦某组后段反弹信号同时转强，模型会被说服得过头”

### 3. 错误 `Up` 的推动因子高度集中

在“模型看 `Up` 但最后错”的窗口里，反复出现的主要推动因子是：

- `q_bs_up_strike`
- `ret_from_cycle_open`
- `first_half_ret`
- `macd_z`
- `bb_pos_20`
- `ma_gap_15`
- `cycle_range_pos`

这些因子大多都在表达同一个大方向：

- 价格已经在反弹
- 价格已经回到区间较高位置
- 动量已经转强
- 相对 strike 看起来也更像 `Up`

问题是，这些都更像“局部反弹开始成立”，不等于“最终结算能翻回 `Up`”。

### 4. 真正最缺的不是更多相似因子，而是更强的 `Up` 否决因子

当前最强的一组因子，更容易回答：

- 现在看起来是不是在反弹

但不够会回答：

- 反弹是不是来得太晚
- 反弹有没有足够持续性
- 当前离真正翻回 `Up` 还有多远
- 按剩余时间和最近速度，来不来得及
- 当前盘口条件下，这张 `Up` 是真便宜还是垃圾便宜

## 当前实盘实际使用的 feature set

当前这批 `baseline` 已结算实盘窗口，匹配到的 feature set 不是较小的 `deep_otm_v1`，而是：

- `bs_q_replace_direction`

这一套共有 `64` 个因子：

1. `ret_1m`
2. `ret_3m`
3. `ret_5m`
4. `ret_15m`
5. `ret_30m`
6. `ret_60m`
7. `ma_gap_5`
8. `ma_gap_15`
9. `ema_gap_12`
10. `ma_15_slope`
11. `bb_pos_20`
12. `rv_30`
13. `rv_30_lag1`
14. `atr_14`
15. `gk_vol_30`
16. `rs_vol_30`
17. `rr_30`
18. `macd_hist`
19. `rsi_14`
20. `rsi_14_lag1`
21. `median_gap_20`
22. `price_pos_iqr_20`
23. `vwap_gap_60`
24. `adx_14`
25. `regime_trend`
26. `regime_high_vol`
27. `taker_buy_ratio`
28. `taker_buy_ratio_z`
29. `taker_buy_ratio_lag1`
30. `trade_intensity`
31. `volume_z`
32. `obv_z`
33. `vwap_gap_20`
34. `donch_pos_20`
35. `hour_sin`
36. `hour_cos`
37. `dow_sin`
38. `dow_cos`
39. `bias_60`
40. `vol_price_corr_15`
41. `volume_z_3`
42. `vol_ratio_5_60`
43. `z_ret_30m`
44. `z_ret_60m`
45. `ret_from_cycle_open`
46. `pullback_from_cycle_high`
47. `rebound_from_cycle_low`
48. `cycle_range_pos`
49. `first_half_ret`
50. `second_half_ret_proxy`
51. `ret_1m_lag1`
52. `ret_1m_lag2`
53. `ret_5m_lag1`
54. `ret_15m_lag1`
55. `delta_rsi`
56. `delta_rsi_5`
57. `macd_z`
58. `macd_extreme`
59. `rsi_divergence`
60. `momentum_agree`
61. `ret_from_strike`
62. `basis_bp`
63. `has_cl_strike`
64. `q_bs_up_strike`

## 因子三分类

下面这三类不是“最终真理”，而是为了第一轮收缩和补因子时更有抓手。

### 一类：建议保留的核心代表项

这些因子本身不是问题，反而是当前体系里最应该保留的骨架。但每一类只应保留少量代表项，不要整组都保留。

- `q_bs_up_strike`
  - 当前最核心的 strike 概率代理。
- `ret_from_strike`
  - 用来回答“离真正翻盘还有多远”。
- `basis_bp`
  - 用来回答外部价格和结算锚点之间有没有明显偏差。
- `ret_from_cycle_open`
  - 当前窗口路径的核心描述。
- `first_half_ret`
  - 用来区分“前半段已经怎样走了”。
- `cycle_range_pos`
  - 保留一个区间站位因子即可。
- `rv_30`
  - 作为统一波动尺度锚点。
- `macd_z`
  - 保留一个主要动量确认因子。
- `volume_z` 或 `obv_z`
  - 保留一到两个流量确认因子即可。
- `vwap_gap_60` 或 `bias_60`
  - 保留一个中一点的价格背景因子。
- `regime_high_vol`
  - 保留一个简单环境标签。

### 二类：建议压缩或合并的高重复因子

这些不一定全删，但更像是在重复表达相似信息。当前最大的问题不是没有信号，而是很多信号都在重复说“价格在弹 / 位置变好看了”。

#### 1. 短中周期收益率家族

- `ret_1m`
- `ret_3m`
- `ret_5m`
- `ret_15m`
- `ret_30m`
- `ret_60m`
- `z_ret_30m`
- `z_ret_60m`
- `ret_1m_lag1`
- `ret_1m_lag2`
- `ret_5m_lag1`
- `ret_15m_lag1`

建议：

- 只留少数几个最有区分度的时间尺度
- 其余作为候选删除

#### 2. 价格站位 / 偏离家族

- `ma_gap_5`
- `ma_gap_15`
- `ema_gap_12`
- `ma_15_slope`
- `bb_pos_20`
- `median_gap_20`
- `price_pos_iqr_20`
- `donch_pos_20`
- `vwap_gap_20`
- `vwap_gap_60`
- `bias_60`

建议：

- 只留每个逻辑家族的代表项
- 尤其要避免多个“价格已经回到较高位置”的因子一起把 `Up` 推假强

#### 3. 波动率家族

- `rv_30`
- `rv_30_lag1`
- `atr_14`
- `gk_vol_30`
- `rs_vol_30`
- `rr_30`

建议：

- 第一轮压缩到 `rv_30` 加最多一到两个补充项

#### 4. 动量 / 震荡家族

- `macd_hist`
- `rsi_14`
- `rsi_14_lag1`
- `delta_rsi`
- `delta_rsi_5`
- `macd_z`
- `macd_extreme`
- `rsi_divergence`
- `momentum_agree`

建议：

- 留 `macd_z`
- 其他只保留最能补充不同信息的少数项

#### 5. 量能 / 流向家族

- `taker_buy_ratio`
- `taker_buy_ratio_z`
- `taker_buy_ratio_lag1`
- `trade_intensity`
- `volume_z`
- `volume_z_3`
- `obv_z`
- `vol_price_corr_15`
- `vol_ratio_5_60`

建议：

- 保留一到两个“流向”
- 保留一到两个“放量”
- 其余压缩

#### 6. 日历家族

- `hour_sin`
- `hour_cos`
- `dow_sin`
- `dow_cos`

建议：

- 保留观察，但优先级低
- 它们不太像当前 `Up` 失败的主因

### 三类：当前明显缺失的关键因子

这部分最重要。现在不是缺更多“价格正在反弹”的因子，而是缺真正能拦住假反弹的因子。

#### 1. 剩余时间是否足够

当前缺少直接回答下面问题的因子：

- 离结算还剩多久
- 当前离翻回 `Up` 还有多远
- 按最近斜率和波动，来不来得及

这是最值得补的因子组。

#### 2. 反弹是否来得太晚

当前很多错误 `Up` 更像：

- 到后半段才明显走强
- 看起来很猛
- 但已经太晚

需要补一个“反弹开始时点”或“最后几分钟才转强”的专门否决因子。

#### 3. 反弹是否连续，而不是只抽一下

当前因子更容易识别“已经有一段反弹”。  
但还缺少：

- 是否连续两个小阶段都在修复
- 是否反弹后又开始停滞
- 是否只是最后一脚抽动

#### 4. 预测市场自己的盘口质量

当前多数因子来自外部价格路径，不足以回答：

- 当前 `Up` 盘口厚不厚
- 便宜是不是因为没人接
- 一追价会不会立刻恶化

对 deep OTM `Up` 来说，这类因子应该比额外再加一个技术指标更有价值。

#### 5. “垃圾便宜票”识别

当前最缺的一类因子不是推动因子，而是否决因子：

- 这张 `Up` 很便宜，是因为真有反转空间
- 还是因为本来就很难赢

## 第一轮建议

### 1. 不要先改模型结构

当前证据更支持：

- 先压缩因子家族
- 先补 `Up` 专用否决因子

而不是一上来换模型或加更复杂模型。

### 2. 先把 64 个压到 24 到 30 个左右

原则不是平均砍，而是：

- 每个逻辑家族只留少量代表项
- 特别压缩那些一起把 `Up` 推假强的家族

### 3. 优先补三类 `Up` 否决因子

- 剩余时间是否够
- 反弹是否来得太晚
- 盘口和成交质量是否支持继续翻盘

### 4. 单独追方向不对齐问题

很多亏掉的 `Up` 单，模型当时其实更偏 `Down`。  
这部分需要和因子诊断分开追，不然会把“预测问题”和“执行对齐问题”混在一起。

## 最后的判断

当前 `deep_otm_baseline` 的核心问题更像：

- 不是单纯 `0.6` 太低
- 不是模型完全不会判断方向
- 而是当前最强的一组因子，把“局部反弹已经成立”看得太像“最终能收成 Up”

一句话总结：

`现在的问题更像是：同一类“晚段反弹 / 位置修复 / 动量转强”因子太多，真正负责拦住假 Up 的关键因子不够。`
