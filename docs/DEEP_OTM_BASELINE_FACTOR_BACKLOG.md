# Deep OTM Baseline Factor Backlog

这份文档补在：

- `docs/DEEP_OTM_BASELINE_UP_DIAGNOSIS.md`
- `docs/DEEP_OTM_BASELINE_RETRAIN_PLAN.md`

之间。

分工如下：

- `DEEP_OTM_BASELINE_UP_DIAGNOSIS.md`
  - 解释当前病灶是什么。
- `DEEP_OTM_BASELINE_RETRAIN_PLAN.md`
  - 解释后续实验和录用流程怎么跑。
- `DEEP_OTM_BASELINE_FACTOR_BACKLOG.md`
  - 只负责回答一个问题：
    - 当前因子库到底还缺什么，哪些该优先补，哪些不要再继续堆。

## 1. 先说结论

当前问题不再是“普通技术因子不够多”。

更像是：

- 同类价格反弹因子已经很多
- 真正关键的否决信息还不够
- 自动搜索虽然已经试过很多组合，但大多还在同一批家族里来回替换

对这套 `deep_otm_baseline` 语义，更应该优先补的不是更多：

- 均线偏离
- RSI / MACD 变体
- 更多不同长度的收益率

而是下面三类：

1. 剩余时间压力
2. 反弹持续性 / 假突破识别
3. 预测市场盘口质量 / “垃圾便宜票”过滤

## 2. 当前因子库现状

从 `src/pm15min/research/features/registry.py` 看，当前注册表共有 `76` 个基础因子，按大类 roughly 分布为：

- `price`: `39`
- `volume`: `13`
- `cycle`: `8`
- `strike`: `9`
- `cross_asset`: `3`
- `calendar`: `4`

这说明当前库的主体仍然是：

- 价格路径
- 波动
- 成交活跃度
- 行权价相对位置

而不是：

- 盘口深度
- 盘口不平衡
- 时间到终局的显式约束
- 反弹后的持续时间与破坏速度

## 3. 已经覆盖得比较多的部分

这部分不是完全没用，而是当前已经很多，不该继续无上限加宽。

### 3.1 短中周期收益率和滞后收益率

例如：

- `ret_1m`
- `ret_3m`
- `ret_5m`
- `ret_15m`
- `ret_30m`
- `ret_60m`
- `ret_1m_lag1`
- `ret_1m_lag2`
- `ret_5m_lag1`
- `ret_15m_lag1`
- `z_ret_30m`
- `z_ret_60m`

这一类已经足够密。

### 3.2 价格站位 / 修复 / 偏离

例如：

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

这一类已经很容易把“局部已经修复”误当成“最终会赢”。

### 3.3 动量 / 振荡变体

例如：

- `macd_hist`
- `macd_z`
- `macd_extreme`
- `rsi_14`
- `rsi_14_lag1`
- `delta_rsi`
- `delta_rsi_5`
- `rsi_divergence`
- `momentum_agree`

这一类现在继续横向扩张，边际价值已经不高。

## 4. 库里已有，但当前没被充分用好的部分

这部分不是“缺失”，而是“值得重新拉回搜索空间”。

### 4.1 跨市场联动

当前注册表里有：

- `btc_ret_5m`
- `btc_vol_30m`
- `rel_strength_15m`

但在现有 `focus_xrp_*` 集合里，这三项没有被用进去。

这对 `ETH / SOL / XRP` 尤其不合理。

因为在 deep OTM 低价票里，很多假反弹本身并不是币种自己结构变强，而只是被：

- `BTC` 带动
- 整体风险偏好抬了一下
- 或者本币相对 `BTC` 的强弱并不支持继续追

所以后续搜索应当明确要求：

- `BTC` 之外的币，至少保留 `1` 个跨市场因子

### 4.2 状态变化率

当前注册表里有，但几乎没被用上的还有：

- `rv_30_change`
- `taker_buy_ratio_change`

这两项的价值不是“再描述一次当前位置”，而是回答：

- 波动是在突然扩张还是已经衰减
- 主动买是否正在加速，还是只是一脚脉冲

这类“变化率”比再加一个绝对水平因子更接近你现在缺的持续性判断。

### 4.3 现有 strike 派生因子没有被稳定当成主轴

虽然仓库里已经有：

- `move_z_strike`
- `strike_abs_z`
- `strike_flip_count_cycle`
- `q_bs_up_strike_centered`

但自动搜索目前还没有稳定把它们当成“核心否决轴”。

后续搜索更应该围绕：

- 离翻盘还有多远
- 翻盘是否已经发生过
- 翻盘后是否反复来回切

来组织，而不是继续主要围绕“价格最近在涨没涨”组织。

## 5. 最值得优先补的新因子类别

下面这三类是当前最该补的。

### 5.1 A 类：显式剩余时间压力

这是第一优先级。

当前 `q_bs_up_strike` 已经把时间折进了一个综合值里，但它没有把时间本身拆开给模型看。

建议新增：

1. `minutes_left_to_settle`
   - 离结算还剩几分钟。
2. `time_progress_ratio`
   - 当前已经走完本周期的比例。
3. `required_move_per_minute`
   - 还差多少才能翻盘，按剩余分钟平摊，每分钟需要走多少。
4. `required_move_z_per_minute`
   - 上一项再按当前波动归一化。
5. `time_to_first_strike_cross`
   - 本周期第一次翻到正确一侧发生在第几分钟。
6. `minutes_since_first_strike_cross`
   - 如果已经翻过，已经站住了多久。
7. `minutes_since_cycle_low`
   - 当前这波反弹距离本周期低点过去了多久。

这组因子主要回答：

- 现在看起来在反弹，但是否已经太晚
- 当前离翻盘还远不远
- 即使方向对了，时间是否还够

### 5.2 B 类：反弹持续性 / 假突破识别

这是第二优先级。

当前因子更擅长识别“已经拉起来了”，不够擅长识别“拉起来以后能不能稳住”。

建议新增：

1. `strike_side_hold_minutes`
   - 当前已经连续站在正确一侧多久。
2. `strike_side_hold_ratio`
   - 本周期到当前为止，有多少时间站在正确一侧。
3. `minutes_since_last_strike_flip`
   - 最近一次翻边过去多久。
4. `strike_flip_rate`
   - 本周期翻边频率。
5. `post_cross_max_favorable_excursion`
   - 首次翻盘后最多又向正确方向走了多少。
6. `post_cross_max_adverse_excursion`
   - 首次翻盘后又回吐了多少。
7. `recent_sign_change_count`
   - 最近几分钟方向符号切换次数。
8. `late_rebound_flag`
   - 主要上涨是否发生在后半段甚至最后几分钟。

这组因子主要回答：

- 是真的稳住了，还是只抽一下
- 是早反弹、慢慢站稳，还是临近结算才猛拉
- 当前路径是顺滑推进，还是来回甩

### 5.3 C 类：盘口质量 / 假便宜过滤

这是第三优先级，但对实盘价值非常高。

这类信息仓库里不是完全没有，而是主要还停留在：

- live 流动性风控
- orderbook 执行层

还没有系统进入研究因子。

建议新增：

1. `decision_spread_bps`
   - 决策时刻买一卖一价差。
2. `decision_top1_imbalance`
   - 最优一档同侧 / 对侧厚度不平衡。
3. `decision_top3_imbalance`
   - 前三档厚度不平衡。
4. `depth_fill_ratio_2usd`
   - 按你的 `2 USD` 真实下单额，当前能否顺利吃满。
5. `depth_slippage_bps_2usd`
   - 为了成交这笔钱需要付出多少滑点。
6. `same_side_depth_usd_3ticks`
   - 三档内同侧总深度。
7. `opposite_side_depth_usd_3ticks`
   - 三档内对手侧总深度。
8. `quote_age_ms`
   - 决策时快照是否过旧。

这组因子主要回答：

- 这张票便宜，是因为真有反转空间，还是因为盘口空心
- 表面概率低，但真实 2 美元下单根本不友好
- 一旦追单，价格是不是立刻恶化

## 6. 需要新链路但值得考虑的扩展

这部分不是最先做，但方向是对的。

### 6.1 现货 / 永续活跃度与基差状态

实盘流动性模块已经在看：

- spot / perp 成交额窗口
- spot / perp 成交笔数窗口
- spot / perp spread
- basis
- open interest

如果要进一步补“当前反弹是不是有真实外部支持”，这组很有价值。

建议后续考虑：

- `spot_quote_ratio`
- `perp_quote_ratio`
- `spot_trades_ratio`
- `perp_trades_ratio`
- `spot_spread_bps`
- `perp_spread_bps`
- `basis_bps_live`
- `open_interest_usd`

### 6.2 盘口恢复能力

这类属于更高级的盘口质量指标。

例如：

- 被打一档后多久恢复
- 一次抬价后，后续档位会不会立刻回补
- 盘口更新速度是否支持持续追价

这类信息通常对“假便宜票”非常有杀伤力，但实现成本也更高。

## 7. 现阶段不建议优先做的方向

下面这些不是永远不做，而是当前优先级不高。

### 7.1 再继续加更多常规技术指标

例如继续补：

- 更多 MA 长度
- 更多 RSI 长度
- 更多 MACD 变体
- 更多普通动量窗口

当前边际收益预计很低。

### 7.2 先上很重的大模型

例如直接把重点放到：

- 深层盘口神经网络
- 很重的序列模型
- 大量复杂结构堆叠

当前更可能是输入信息结构不对，而不是模型深度不够。

### 7.3 继续单纯往更宽集扩张

如果不先补新类别，只是继续从 `40` 扩到 `56 / 64 / 80`，更容易发生：

- 同类家族越堆越多
- 稀疏成交窗口里误把局部反弹当终局胜利

## 8. 对自动搜索的约束建议

如果这份 backlog 要喂给自动研究，建议同步收紧下面几条。

### 8.1 家族覆盖约束

对 `ETH / SOL / XRP` 的候选集合，建议要求：

- 至少 `1` 个跨市场因子
- 至少 `4` 个 strike / time 压力相关因子
- 至少 `3` 个持续性 / 翻边稳定性因子

### 8.2 重复家族上限

建议限制：

- 纯收益率与其 lag / zscore 同家族总数不要过多
- 价格站位 / 均线偏离同家族不要过多
- 纯动量振荡类不要过多

原则上应当优先做：

- 家族替换
- 类别补齐

而不是：

- 同家族内部反复换长度

### 8.3 录用顺序建议

对于 dense 搜索，录用顺序更应该偏向：

1. 先看是否捕到更多有效窗口
2. 再看交易数是否上来
3. 再看 ROI 和胜率是否稳

也就是说，现阶段更应优先找到：

- 真正能抓住更多正确反转窗口的组合

而不是：

- 只靠少数几笔看起来更干净的组合

## 9. 推荐实施顺序

按投入产出比，建议顺序如下。

### 第一步：先把库里已有但没充分用上的项拉回搜索

- `btc_ret_5m`
- `btc_vol_30m`
- `rel_strength_15m`
- `rv_30_change`
- `taker_buy_ratio_change`

这是最便宜的一步。

### 第二步：补显式时间压力因子

优先补：

- `minutes_left_to_settle`
- `required_move_per_minute`
- `required_move_z_per_minute`
- `minutes_since_first_strike_cross`

这是当前最值得做的一步。

### 第三步：补持续性 / 假突破因子

优先补：

- `strike_side_hold_minutes`
- `minutes_since_last_strike_flip`
- `strike_flip_rate`
- `late_rebound_flag`

### 第四步：把盘口质量正式接进研究因子

优先补：

- `decision_spread_bps`
- `depth_fill_ratio_2usd`
- `depth_slippage_bps_2usd`
- `decision_top3_imbalance`

## 10. 最后的判断

当前这套策略最缺的，不是再多几种“价格已经涨起来”的证据。

真正该补的是：

- 时间是否还够
- 反弹是否站稳
- 盘口是否真支持这张便宜票

如果这三类不补，后面即使继续跑很多轮自动搜索，也更可能只是：

- 在同一批反弹家族里反复换因子
- 继续把局部反弹误判成终局胜利

## 参考方向

下面这些外部工作，不是直接照搬，而是说明：

- 盘口不平衡
- 订单流
- 时间到事件
- 跨市场价格发现

确实是值得补进来的信息方向。

- The Price Impact of Generalized Order Flow Imbalance
  - https://arxiv.org/abs/2112.02947
- Using Deep Learning for price prediction by exploiting stationary limit order book features
  - https://arxiv.org/abs/1810.09965
- Deep attentive survival analysis in limit order books: estimating fill probabilities with convolutional-transformers
  - https://doi.org/10.1080/14697688.2023.2286351
- Scaling properties and universality of first-passage-time probabilities in financial markets
  - https://doi.org/10.1103/PhysRevE.84.066110
- Trading activity and price discovery in Bitcoin futures markets
  - https://doi.org/10.1016/j.jempfin.2021.03.001
