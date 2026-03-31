# Factor Side Analysis

## Scope

- Data scope: `2026-03-15` 이후地址交易窗口。
- This report uses only `feature_source=local_feature_frame` settled windows for the main factor conclusions, to avoid fallback OHLCV noise.
- The factor tables are built from `trade_windows.csv` and `trade_factor_weights_long.csv`.

## 1. Address-Level Outcome

Bought side performance:

| trade_side   |   positions |   wins |   win_rate_pct |   pnl_usdc |   cost_usdc |   roi_pct |
|:-------------|------------:|-------:|---------------:|-----------:|------------:|----------:|
| DOWN         |          73 |     29 |        39.7260 |   200.0686 |    378.6634 |   52.8355 |
| UP           |          16 |      2 |        12.5000 |   -78.5429 |    125.9400 |  -62.3654 |

Matched model predicted-side performance:

| pred_side   |   positions |   wins |   win_rate_pct |   pnl_usdc |   cost_usdc |   roi_pct |
|:------------|------------:|-------:|---------------:|-----------:|------------:|----------:|
| DOWN        |          47 |     15 |        31.9149 |    72.2032 |    263.3101 |   27.4213 |
| UP          |          42 |     16 |        38.0952 |    49.3226 |    241.2933 |   20.4409 |

UP trades split by matched model side:

| pred_side   |   positions |   wins |   win_rate_pct |   avg_pred_prob |   avg_entry |   pnl_usdc |
|:------------|------------:|-------:|---------------:|----------------:|------------:|-----------:|
| DOWN        |          10 |      1 |        10.0000 |          0.6265 |      0.2455 |   -47.1607 |
| UP          |           6 |      1 |        16.6667 |          0.6341 |      0.2688 |   -31.3822 |

Key reading:

- Bought `DOWN` windows did much better than bought `UP` windows: `DOWN` win rate `39.73%`, ROI `+52.84%`; `UP` win rate `12.50%`, ROI `-62.37%`.
- But the matched model predictions are not themselves strongly DOWN-biased: predicted `UP` windows still had `38.10%` win rate and `+20.44%` ROI, predicted `DOWN` windows had `31.91%` win rate and `+27.42%` ROI.
- This means `UP` 输得多，不是简单的“模型只会做 DOWN”，而是 deep OTM 语义下，baseline 更擅长否决很多不会完成的 `Up` 反转，但不擅长确认哪些 `Up` 反转真的能走完整个窗口。
- In local-feature rows, bought `UP` windows were only `16` trades; among them `10` matched model `DOWN`, `6` matched model `UP`.

### Deep OTM 口径纠偏

- 这里不能按普通“看涨就买 `UP`，看跌就买 `DOWN`”来理解。
- 对 deep OTM 来说，买 `DOWN` 能赚钱，通常是窗口内价格先上去，让 `DOWN` 变便宜，但最终又回落到 `DOWN` 结算。
- 买 `UP` 能赚钱，通常是窗口内价格先下去，让 `UP` 变便宜，但最终必须在剩余几分钟里拉回到 `UP` 结算。
- 所以最关键的不对称，不是“模型整体更偏 `DOWN`”，而是“它对 `Up` 反转 setup 的放行条件和否决条件是不是错位了”。

从 `2026-03-15` 之后的已结算 `UP` 实盘窗口看，这个错位非常明显：

| sample | pred DOWN n | pred DOWN correct | pred DOWN acc | pred UP n | pred UP correct | pred UP acc |
|:--|--:|--:|--:|--:|--:|--:|
| all settled bought UP | 30 | 25 | 83.33% | 10 | 2 | 20.00% |
| local_feature_frame bought UP | 10 | 9 | 90.00% | 6 | 1 | 16.67% |

这说明 baseline 在 `UP` setup 里更像“反转否决器”：

- 当它继续判 `DOWN` 时，多数时候是对的，说明很多窗口内下跌后的 `UP` 反转根本没走完。
- 当它真的翻到 `UP` 时，错误率反而很高，说明它放行 `UP` 的依据更像“局部反弹看起来不错”，而不是“最终结算足够可能翻回 `UP`”。

在本地原生特征窗口里，这个差异也能直接看到：

| local bought-UP slice | n | ret_from_cycle_open | first_half_ret | q_bs_up_strike | cycle_range_pos | bb_pos_20 | macd_z |
|:--|--:|--:|--:|--:|--:|--:|--:|
| pred DOWN and correct | 9 | -0.001259 | -0.001339 | 0.372422 | 0.278376 | -0.073695 | -0.423379 |
| pred UP and wrong | 5 | 0.000391 | -0.000579 | 0.635411 | 0.797645 | 0.328720 | 0.991203 |

这个表的含义是：

- `pred DOWN and correct` 那组，价格还在区间下部，`ret_from_cycle_open` 和 `first_half_ret` 仍偏负，`macd_z` 也偏负。模型看到的是“下跌后的反转还没成熟”，所以继续判 `DOWN`，而且大多判对。
- `pred UP and wrong` 那组，价格已经弹到区间高位，`q_bs_up_strike`、`cycle_range_pos`、`bb_pos_20`、`macd_z` 都明显更高。模型看到的是“晚段反弹已经很像会翻 `UP`”。
- 但 deep OTM 真正需要的是 terminal reversal，不是 local bounce。到了 `7/8/9` 分钟才开始明显变强，往往已经太晚，剩余时间不够把最终结算真正拉回 `UP`。
- 所以你感受到的“`DOWN` 反转能抓，`UP` 反转不行”，更准确地说是：baseline 更会抓 `UP` 反转失败，却不够会抓 `UP` 反转成功。

## 2. What Fires On Winning DOWN Windows

Bought `DOWN` and won (`29` local trades): top logreg support frequency in top5 absolute contributors:

| feature               |   count |
|:----------------------|--------:|
| gk_vol_30             |      11 |
| q_bs_up_strike        |       7 |
| bias_60               |       7 |
| rv_30                 |       7 |
| second_half_ret_proxy |       5 |
| rv_30_lag1            |       4 |
| bb_pos_20             |       3 |
| ma_gap_15             |       3 |
| donch_pos_20          |       2 |
| macd_z                |       2 |

Bought `DOWN` and won: top LGB support frequency in top5 absolute contributors:

| feature                |   count |
|:-----------------------|--------:|
| q_bs_up_strike         |      11 |
| macd_z                 |       9 |
| first_half_ret         |       8 |
| vwap_gap_60            |       7 |
| rsi_14                 |       5 |
| ret_from_cycle_open    |       5 |
| delta_rsi              |       5 |
| ret_30m                |       4 |
| rebound_from_cycle_low |       3 |
| hour_cos               |       3 |

Interpretation:

- DOWN winners are still driven mostly by price-location and short-horizon momentum factors, not by a unique “DOWN-only” factor family.
- The recurring names are `q_bs_up_strike`, `gk_vol_30`, `rv_30`, `bb_pos_20`, `rv_30_lag1`, plus LGB-side `macd_z`, `first_half_ret`, `ret_from_cycle_open`, `vwap_gap_60`.
- In other words, winning DOWN windows usually happen when the model sees weak short-term path structure and poor strike-relative position, then the market keeps moving in that direction long enough to settle there.

## 3. What Fires On Losing UP Windows

Bought `UP` and lost (`14` local trades): top logreg support frequency in top5 absolute contributors:

| feature        |   count |
|:---------------|--------:|
| gk_vol_30      |       5 |
| ma_gap_15      |       4 |
| rv_30_lag1     |       4 |
| ema_gap_12     |       3 |
| macd_z         |       2 |
| q_bs_up_strike |       2 |
| ret_1m         |       2 |
| bb_pos_20      |       1 |
| ma_gap_5       |       1 |
| macd_hist      |       1 |

Bought `UP` and lost: top LGB support frequency in top5 absolute contributors:

| feature                |   count |
|:-----------------------|--------:|
| q_bs_up_strike         |       7 |
| macd_z                 |       3 |
| ret_from_cycle_open    |       3 |
| cycle_range_pos        |       2 |
| ret_1m_lag1            |       2 |
| first_half_ret         |       2 |
| obv_z                  |       1 |
| rebound_from_cycle_low |       1 |
| regime_high_vol        |       1 |
| delta_rsi              |       1 |

Interpretation:

- Losing UP windows are again driven by the same family of short-horizon rebound / price-position factors: `q_bs_up_strike`, `ret_from_cycle_open`, `first_half_ret`, `cycle_range_pos`, `ret_1m`, `ma_gap_15`, `macd_z`.
- The problem is not that UP uses a completely different bad factor family. The problem is that the current UP logic is over-triggered by the same “cheap / bounce / intracycle recovery” signals, and those signals are not selective enough.
- `q_bs_up_strike` appears in both good and bad UP calls, but on losing UP windows it appears more often and with stronger average support. The same is true for `ret_from_cycle_open` and `first_half_ret` on the LGB side.

## 4. Narrow Slice: Bought UP, Model Also Predicted UP, But Still Lost

This is the cleanest slice for “why did the model call UP and still get it wrong”.

- Sample size: `5` losing trades vs `1` winning trade with the same bought-UP / predicted-UP alignment.

Top5 logreg absolute-contributor frequency on `bought UP + predicted UP + loss`:

| feature          |   count |
|:-----------------|--------:|
| ma_gap_15        |       3 |
| macd_z           |       2 |
| q_bs_up_strike   |       2 |
| gk_vol_30        |       1 |
| price_pos_iqr_20 |       1 |
| ret_1m           |       1 |
| rv_30            |       1 |
| rv_30_lag1       |       1 |

Top5 LGB absolute-contributor frequency on `bought UP + predicted UP + loss`:

| feature                |   count |
|:-----------------------|--------:|
| q_bs_up_strike         |       3 |
| ret_from_cycle_open    |       3 |
| first_half_ret         |       2 |
| macd_z                 |       2 |
| cycle_range_pos        |       1 |
| rebound_from_cycle_low |       1 |
| ret_30m                |       1 |
| ret_3m                 |       1 |
| ret_60m                |       1 |

The recurring names here are:

- Logreg side: `bb_pos_20`, `ma_gap_15`, `rv_30_lag1`, `macd_z`, `q_bs_up_strike`.
- LGB side: `q_bs_up_strike`, `ret_from_cycle_open`, `first_half_ret`, `macd_z`, `ret_30m`.

Interpretation:

- These losing UP calls are largely “short-horizon rebound / bounce continuation” calls.
- The model is seeing: price is not too far from strike, intracycle return is improving, first-half path is decent, momentum is turning, so UP looks plausible.
- But settlement still goes DOWN. In deep OTM terms, these are late-bounce calls that over-extrapolate local rebound into terminal reversal.

## 5. Which Factors Actually Differentiate UP Wins From UP Losses

Features more present in `predicted UP wins` than `predicted UP losses` (LGB delta):

| feature           |   delta_logreg |   delta_lgb |   mean_logreg_win |   mean_logreg_loss |   mean_lgb_win |   mean_lgb_loss |
|:------------------|---------------:|------------:|------------------:|-------------------:|---------------:|----------------:|
| first_half_ret    |         0.0059 |      0.2457 |            0.0347 |             0.0289 |         0.4499 |          0.2041 |
| ret_3m            |        -0.0155 |      0.0237 |            0.0073 |             0.0227 |         0.0043 |         -0.0194 |
| rs_vol_30         |         0.0885 |      0.0129 |            0.0798 |            -0.0087 |         0.0091 |         -0.0038 |
| ret_5m            |         0.0029 |      0.0122 |            0.0072 |             0.0043 |        -0.0005 |         -0.0126 |
| ma_gap_5          |        -0.0399 |      0.0116 |           -0.0161 |             0.0239 |         0.0033 |         -0.0083 |
| rv_30_lag1        |        -0.0069 |      0.0087 |           -0.0597 |            -0.0528 |         0.0073 |         -0.0014 |
| dow_cos           |         0.0162 |      0.0083 |            0.0201 |             0.0039 |         0.0075 |         -0.0009 |
| vol_price_corr_15 |         0.0016 |      0.0082 |            0.0075 |             0.0058 |         0.0127 |          0.0046 |

Features more present in `predicted UP losses` than `predicted UP wins` (LGB delta):

| feature                |   delta_logreg |   delta_lgb |   mean_logreg_win |   mean_logreg_loss |   mean_lgb_win |   mean_lgb_loss |
|:-----------------------|---------------:|------------:|------------------:|-------------------:|---------------:|----------------:|
| ret_from_cycle_open    |         0.0149 |     -0.1091 |            0.0485 |             0.0336 |         0.1819 |          0.2911 |
| q_bs_up_strike         |        -0.0090 |     -0.0722 |            0.4359 |             0.4450 |        -0.0293 |          0.0429 |
| ret_30m                |        -0.0165 |     -0.0323 |           -0.0140 |             0.0025 |        -0.0224 |          0.0098 |
| rsi_14                 |        -0.0122 |     -0.0279 |           -0.0165 |            -0.0044 |        -0.0309 |         -0.0030 |
| macd_z                 |        -0.0385 |     -0.0273 |            0.0062 |             0.0448 |        -0.0218 |          0.0055 |
| delta_rsi              |        -0.0039 |     -0.0197 |           -0.0042 |            -0.0004 |        -0.0095 |          0.0102 |
| macd_hist              |        -0.0191 |     -0.0197 |           -0.0368 |            -0.0177 |        -0.0097 |          0.0100 |
| rebound_from_cycle_low |         0.0087 |     -0.0166 |            0.0054 |            -0.0034 |        -0.0176 |         -0.0010 |

Interpretation:

- The features that overfire in losing UP calls are `ret_from_cycle_open`, `q_bs_up_strike`, `ret_30m`, `macd_z`, `delta_rsi`, `delta_rsi_5`, `rebound_from_cycle_low`.
- The features that help UP calls survive are much weaker and less stable: `rs_vol_30`, `ema_gap_12`, `rr_30`, some mild `vwap_gap_60` / `vol_price_corr_15` effects.
- So UP is not failing because there is zero signal. UP is failing because its positive trigger set is strong and common, while its veto / filtering set is weak.

## 6. Bottom Line

- `DOWN` and `UP` are not being driven by two cleanly different factor families. A lot of the same price-location / rebound factors fire on both sides.
- The real asymmetry is this: `UP` has too many false positives, especially when a late-window rebound already looks strong enough to tempt the model into flipping `UP`.
- The worst repeat offenders on the losing UP side are: `q_bs_up_strike`, `ret_from_cycle_open`, `first_half_ret`, `bb_pos_20`, `cycle_range_pos`, `macd_z`, and very short-horizon return gaps like `ret_1m` / `ma_gap_15`.
- A second issue is execution alignment: many bought-UP windows are not aligned with the matched model direction at all. But even on the aligned slice, the bigger problem is still that baseline is better at vetoing unfinished `UP` reversals than confirming mature ones.
