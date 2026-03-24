# v2 Live Domain Specification

这份文档是 `v2` Live 域的正式技术方案。

目标只有一个：以后任何人看 `v2/live`、`v2/research/active_bundles`、`v2/var/live`，都能立刻知道：

- 当前实盘到底读哪一套权重
- live 特征从哪里来
- 黑名单在哪里生效
- 打分结果写到哪里
- 后续 decision / guard / execution 应该往哪里接

如果后续代码和文档冲突，以最终 `v2/src/pm15min/live/` 实现为准；但实现应尽快改回和本文档一致，而不是继续发散。

---

## 1. 设计原则

### 1.1 总原则

- `live` 只负责运行时行为：
  - 读取 active bundle
  - 构造 live 特征
  - 做在线打分
  - 套 runtime guard
  - 生成 decision / order intent / execution state

- `live` 不负责：
  - 原始数据抓取
  - 训练
  - 离线回测
  - 实验编排

这些分别属于：

- `v2/data`
- `v2/research`

### 1.2 Source of Truth

当前 `deep_otm` 实盘语义必须以旧实盘代码为主，不以临时研究脚本或聊天结论为主。

本次 `v2/live` 对齐的 legacy source of truth：

- profile / offsets / live run 选择
  - `live_trading/configs/profiles.py`
  - `live_trading/configs/shared.py`

- live 信号语义
  - `live_trading/core/signal_engine.py`
  - `src/inference.py`

- live 因子主干
  - `src/features/feature_engineering.py`
  - `src/features/strike_moneyness.py`

- runtime feature guard
  - `live_trading/core/feature_guard.py`

- Deep OTM 现状文档
  - `docs/DEEP_OTM_V6_STRATEGY.md`
  - `docs/DEEP_OTM_FACTOR_TRAINING_PLAN.md`
  - `docs/ALPHA_PROGRESS_20260317.md`

### 1.3 一个资产只有一个 active 实盘权重入口

研究阶段可以有很多 bundle。

但在 live 里，同一个：

- `asset`
- `profile`
- `target`

只能有一个 active bundle。

这个 active 入口必须显式写在：

```text
v2/research/active_bundles/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/selection.json
```

绝对禁止继续依赖：

- “最新目录”
- “mtime 最大”
- “train_v6_* 看起来最像那个”

来猜当前实盘权重。

---

## 2. 当前 live 范围

当前 `v2/live` 只先覆盖 `deep_otm direction` 主线。

### 2.1 当前实盘范围

- `profile=deep_otm`
- `target=direction`
- `cycle=15m`
- `markets=sol,xrp`
- `offsets=7,8,9`

### 2.2 baseline 只做参考，不做实盘默认

baseline 也需要 canonical 化，但不能和 live profile 混在一起。

因此当前 registry 固定分开：

- `profile=deep_otm`
  - 当前实盘 active bundle

- `profile=deep_otm_baseline`
  - 当前 baseline reference

### 2.3 reversal 当前不是 live 主线

`reversal` 不再作为当前 canonical live target。

旧导入的 reversal bundle 只允许留在 quarantine：

```text
v2/var/quarantine/model_bundles_reversal_legacy/
```

不允许继续作为 `v2/live` 默认输入。

---

## 3. Live 只读哪些输入

### 3.1 来自 `v2/research`

live 只允许读：

1. active bundle registry
2. active registry 指向的 canonical model bundle

不允许 live 直接读：

- `v2/research/training_runs`
- 旧仓库 `data/markets/<coin>/artifacts_runs`
- 旧仓库 `data/markets/<coin>/archived_runs`

### 3.2 来自 `v2/data/live`

live 特征构建只允许读 `v2/data/live` 的 canonical 数据：

- Binance 1m
  - `v2/data/live/sources/binance/klines_1m/symbol=<SYMBOL>/data.parquet`

- Oracle / strike 对齐表
  - `v2/data/live/tables/oracle_prices/cycle=15m/asset=<asset>/data.parquet`

- 后续要接的 live 决策输入
  - market catalog
  - orderbook raw / orderbook index

### 3.3 不允许直接读 legacy 运行态

不允许 `v2/live` 直接读取：

- `data/markets/<coin>/logs/live_trading/*`
- 旧 runner cache
- 旧 `.env` 派生临时状态

这些只能作为盘点参考，不是 `v2` runtime 输入。

---

## 4. 顶层目录规则

`v2/live` 的运行时目录必须长这样：

```text
v2/
  research/
    active_bundles/
      cycle=15m/asset=xrp/profile=deep_otm/target=direction/selection.json

  research/
    model_bundles/
      cycle=15m/asset=xrp/profile=deep_otm/target=direction/bundle=.../

  var/
    live/
      state/
        signals/
        quotes/
        decisions/
        liquidity/
        regime/
        open_orders/
        positions/
        execution/
        orders/
        cancel/
        redeem/
        runner/
      logs/
        runner/
```

含义固定：

- `research/active_bundles`
  - 声明当前 live 应该读哪个 bundle

- `research/model_bundles`
  - 保存所有可部署 bundle

- `var/live/state`
  - 保存 live runtime state

- `var/live/logs`
  - 保存 live runtime logs

---

## 5. Active Bundle Contract

### 5.1 `selection.json`

路径：

```text
v2/research/active_bundles/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/selection.json
```

至少包含：

- `market`
- `cycle`
- `profile`
- `target`
- `bundle_label`
- `bundle_dir`
- `source_run_dir`
- `usage`
- `activated_at`
- `notes`
- `metadata`

### 5.2 解析规则

默认 bundle 解析顺序固定：

1. 先读 `selection.json`
2. 如果存在，就直接使用其中的 `bundle_dir`
3. 只有 registry 缺失时，才允许回退到 `model_bundles/` 默认搜索逻辑

### 5.3 当前已经接入的命令

查看当前 active bundle：

```bash
PYTHONPATH=v2/src python -m pm15min research show-active-bundle --market xrp --profile deep_otm --target direction
```

切换 active bundle：

```bash
PYTHONPATH=v2/src python -m pm15min research activate-bundle --market xrp --profile deep_otm --target direction --bundle-label <bundle_label>
```

---

## 6. Live Feature Contract

### 6.1 原则

live 特征不按“训练时起了什么名字”做弱绑定，而按 active bundle 的 `bundle_config.json` 做强绑定。

也就是说：

- bundle 决定 live 当前真正需要哪些列
- feature builder 必须能产出这些列
- 不允许 live 自己拍脑袋删列或换列

### 6.2 当前 deep_otm live 主线

当前 live 主线不是 `reversal`，而是 `direction`。

当前实盘 active run：

- `SOL`
  - `train_v6_sol_stage2_pool_drop_volume_z_3_end0309_dist_20260317_161410`

- `XRP`
  - `train_v6_xrp_stage6_q_capacity_cross_drop_vol_price_corr_15_vwap_gap_20_end0309_dist_20260318_162707`

### 6.3 `v6_user_core`

`v6_user_core` 是当前 deep OTM 稀疏方向主干的正式 feature-set 名，不需要额外造一个别名。

它的核心列集合是：

```text
ret_1m
ret_3m
ret_15m
rv_30
atr_14
adx_14
regime_high_vol
taker_buy_ratio
taker_buy_ratio_z
volume_z
vwap_gap_20
bias_60
vol_price_corr_15
volume_z_3
vol_ratio_5_60
ret_from_cycle_open
move_z
pullback_from_cycle_high
rebound_from_cycle_low
cycle_range_pos
ret_1m_lag1
ret_5m_lag1
delta_rsi
delta_rsi_5
macd_z
btc_ret_5m
btc_vol_30m
rel_strength_15m
ret_from_strike
basis_bp
has_cl_strike
```

注意：

- `v6_user_core` 是稀疏核
- 当前 live run 可以比它更偏 alpha-search，列集合不一定完全相同
- 真正 runtime 所需列，仍以 active bundle 的 `feature_columns` 为准

### 6.4 现役 live bundle 常见附加列

当前 `SOL/XRP` 现役 direction run 还会用到这些列：

- `ret_30m`
- `bb_pos_20`
- `price_pos_iqr_20`
- `hour_sin`
- `hour_cos`
- `rv_30_lag1`
- `rsi_divergence`
- `q_bs_up_strike`
- `q_bs_up_strike_centered`
- `ma_gap_5`

### 6.5 公式口径必须对齐 legacy

当前 `v2/live` 的特征公式，必须按旧实盘代码对齐：

- price / volume / cycle 主干：
  - `src/features/feature_engineering.py`

- strike / BS 因子：
  - `src/features/strike_moneyness.py`

重点不允许偷换的列包括：

- `bb_pos_20`
- `price_pos_iqr_20`
- `delta_rsi`
- `delta_rsi_5`
- `macd_z`
- `rsi_divergence`
- `vol_price_corr_15`
- `vol_ratio_5_60`
- `regime_high_vol`
- `q_bs_up_strike`
- `q_bs_up_strike_centered`

---

## 7. Strike / Oracle Augmentation

### 7.1 当前必须保留的 strike 族列

live 最终特征必须支持：

- `ret_from_strike`
- `basis_bp`
- `has_cl_strike`
- `q_bs_up_strike`
- `q_bs_up_strike_centered`

### 7.2 真实口径

这些列不是原始 Binance K 线列，而是 live 在线增强列。

因此：

- 纯 Binance raw feature frame 看不到这些列是正常的
- 但进入模型前，live 必须把这些列补齐

### 7.3 v2 的规则

`v2/live` 里 strike augmentation 必须成为独立能力，不允许继续隐含塞在大脚本里。

建议固定模块职责：

- `live/strike_provider.py`
  - 负责拿 strike / price_to_beat / fallback

- `live/features.py`
  - 负责把 strike augmentation 合到实时特征帧

### 7.4 当前状态

当前 `v2` 先用 `v2/data/live/tables/oracle_prices/...` 生成基础 strike 因子。

后续必须继续补齐：

- streams 边界价
- strike cache
- RTDS / chainlink fallback
- `has_cl_strike` 的真实运行语义

---

## 8. Blacklist Policy

### 8.1 黑名单生效时机

黑名单必须在：

1. live 特征构建完成之后
2. 模型打分之前

执行方式固定为：

- 被屏蔽列直接置 `0.0`

### 8.2 当前 deep_otm 实盘口径

按当前 Deep OTM 文档口径：

- `sol`
  - `delta_rsi_5`

- `xrp`
  - `ret_5m`
  - `ma_gap_5`

### 8.3 canonical 规则

`v2` 里黑名单应该明确分成两层：

1. profile-level live blacklist
   - 表达当前实盘策略真实想屏蔽哪些列

2. bundle-level allowed blacklist columns
   - 表达这个 bundle 允许哪些列被置零而不破坏推理契约

不能继续只靠：

- 老 `shared.py` 的默认黑名单
- 或 bundle 文件里顺手带上的历史字段

来隐式决定实盘行为。

---

## 9. Signal Contract

### 9.1 live 打分对象

当前 live 最小打分对象是 `signal_snapshot`。

路径：

```text
v2/var/live/state/signals/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/latest.json
v2/var/live/state/signals/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/snapshots/snapshot_ts=<ts>/signal.json
```

### 9.2 至少包含的字段

- `market`
- `profile`
- `cycle`
- `target`
- `bundle_label`
- `bundle_dir`
- `active_bundle_selection_path`
- `builder_feature_set`
- `bundle_feature_set`
- `latest_feature_decision_ts`
- `offset_signals`

每个 `offset_signal` 至少包含：

- `offset`
- `decision_ts`
- `cycle_start_ts`
- `cycle_end_ts`
- `p_signal`
- `p_up`
- `p_down`
- `recommended_side`
- `confidence`
- `edge`
- `score_valid`
- `score_reason`
- `coverage`

### 9.3 当前命令

```bash
PYTHONPATH=v2/src python -m pm15min live score-latest --market xrp --profile deep_otm
```

这条命令当前做的事是：

1. 读取 active bundle
2. 从 `v2/data/live` 构造最新特征帧
3. 对 bundle 的所有 offset 打分
4. 输出当前 signal snapshot

---

## 10. Quote Contract

### 10.1 quote snapshot 是独立对象

`decision` 不应该直接自己拼价格。

应该先有独立 `quote_snapshot`：

```text
v2/var/live/state/quotes/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/latest.json
v2/var/live/state/quotes/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/snapshots/snapshot_ts=<ts>/quote.json
```

### 10.2 quote snapshot 输入

quote snapshot 只允许依赖：

- `signal_snapshot`
- `v2/data/live/tables/markets/...`
- `v2/data/live/tables/orderbook_index/...`

如果这些 canonical 输入缺失，必须显式返回缺失状态，不允许伪造报价。

还有一个必须明确的约束：

- `signal_snapshot`
- `markets`
- `orderbook_index`

必须是同一时间语义下可对齐的一组输入。

也就是说，如果：

- signal 还停在旧时间窗
- 但 live market catalog 已经刷新到更新的一批 active markets

那么 `quote_snapshot` 返回 `market_row_missing` 是正确行为，不是应该靠兼容层掩盖的问题。

盘口时间选择规则也要明确：

- 优先取 `decision_ts` 之前最近的一条盘口索引
- 如果旧实盘语义是“先打分，再立刻拉盘口”，允许接受很小容忍窗内的 post-decision 盘口
- 但不允许跨很久之后再回填，否则 quote 就失真了

### 10.3 当前命令

```bash
PYTHONPATH=v2/src python -m pm15min live quote-latest --market sol --profile deep_otm
```

当前这条命令会：

1. 读取最新 signal snapshot
2. 尝试按 `cycle_start_ts / decision_ts` 对齐 canonical market row
3. 优先从热窗口 `orderbooks/recent.parquet` 读取近窗 quote，缺失时再回退 canonical `orderbook_index`
4. 如果输入缺失，就明确返回 `missing_quote_inputs`

补充说明：

- 如果要让 `quote-latest` 在当前 live 窗口返回 `ok`
  - 不仅要有最新 active market catalog
  - 还需要该 decision window 之前就已经录到对应的热窗口 quote 或 `orderbook_index`

- 因此 one-shot 补录不能替代连续 recorder
  - 它可以补 canonical data
  - 但不保证能回到更早的 decision timestamp 直接出 live quote

- 当前推荐入口不是手工分别跑很多条命令
  - 而是先跑 `pm15min data run live-foundation`
  - 让 market catalog / binance / oracle / orderbooks 一起刷新
- 如果要把四个 canonical 市场的热盘口持续维护起来
  - 直接跑 `pm15min data run orderbook-fleet --markets btc,eth,sol,xrp --loop --iterations 0`

---

## 11. Signal Semantics

### 10.1 当前主线只做 direction

当前 `deep_otm` live 主线是 `direction`：

- `p_signal = P(UP)`
- `p_up = P(UP)`
- `p_down = 1 - P(UP)`

### 10.2 reversal 只保留兼容能力

`reversal` 映射逻辑仍应在 `research/inference/scorer.py` 保留：

- 当前价在 strike 上方时，`P(reversal)` 映射到 `P(DOWN)`
- 当前价在 strike 下方时，`P(reversal)` 映射到 `P(UP)`

但这不是当前 live 主线。

---

## 12. Guard / Decision / Execution 分层

### 11.1 三层职责

后续 `v2/live` 必须拆成三层：

1. `signal`
   - 只负责模型打分

2. `decision`
   - 套 runtime guard
   - 生成 trade / no-trade 结论和 reject reason

3. `execution`
   - 订单价格
   - orderbook / fill guard
   - retry / cancel / redeem

### 11.2 当前还没完全迁完的旧逻辑

后续必须继续从 legacy 迁入：

- 更完整的 runner-level 风险告警 / 汇总分层
- 更明确的 canonical live CLI 边界收紧
- 不会假装已经具备完整的 regime-aware portfolio accounting

当前已经落到 `v2` 的 guard：

- profile + bundle blacklist compatibility
- NaN feature guard
- external liquidity guard
- regime controller state + decision guard
- probability threshold
- ret_30m direction guard
- tail-space guard
- quote-aware entry price band
- quote-aware net edge guard
- quote-aware ROI guard

当前已经落到 `v2` 的 execution simulate：

- L1 fill proxy
- full depth orderbook fill
- orderbook limit reprice
- read-only retry policy
- read-only cancel policy contract
- read-only redeem policy contract

### 11.3 明确禁止

不允许重新做成一个大脚本，把：

- 特征构建
- 模型打分
- reject reason
- 下单逻辑
- retry
- orderbook

全塞在一起。

---

## 13. CLI 规划

### 12.1 当前已经存在

- `live show-config`
- `live show-layout`
- `live score-latest`
- `live quote-latest`
- `live check-latest`
- `live decide-latest`
- `live runner-once`
- `live runner-loop`
- `live execution-simulate`
- `live execute-latest`
- `live sync-account-state`
- `live sync-liquidity-state`
- `live apply-cancel-policy`
- `live apply-redeem-policy`
- `live show-latest-runner`
- `live show-ready`
- `research show-active-bundle`
- `research activate-bundle`

### 12.2 下一阶段应该补齐

- runner 监控 / 风控 / 告警收口
- 继续收紧非 canonical live CLI 边界

当前这块已经完成一轮更明确的收口：

- canonical operator 只读入口：
  - `live check-trading-gateway`
  - `live show-ready`
  - `live show-latest-runner`
- compatibility inspection 入口：
  - `live show-config`
  - `live show-layout`
  - 这两个命令当前保留非 canonical market/profile 的查看能力
  - 但输出里必须显式带：
    - `canonical_live_scope`
    - `cli_boundary`
    - `profile_spec_resolution`
  - 如果请求的是非 canonical profile
    - 必须显式说明当前是否落到了 fallback profile spec

当前 `runner` 的边界：

- `runner-once`
  - 先跑一轮 canonical live foundation
  - 再刷新 / 复用一轮最新 `liquidity state`
  - 再跑一轮 `decide-latest`
    - 其中会基于最新 feature frame + liquidity state 生成 `regime state`
  - 再跑一轮 `execution` snapshot
  - 默认会继续执行：
    - 真实下单
    - account state sync
    - cancel policy side effect
    - redeem policy side effect
  - 可通过 `--no-side-effects` 关闭
  - 可通过 `--dry-run-side-effects` 保留编排但不真正落单 / cancel / redeem

- `runner-loop`
  - 按固定 sleep 周期重复执行 `runner-once` 主线
  - 会持续写入 `liquidity` / `decision` / `execution` / `orders` / `cancel` / `redeem` runtime state
  - 默认同样进入真实 side effect 编排
  - 对 `orders` / `cancel` / `redeem` action 默认做：
    - 基于 `action_key` 的幂等跳过
    - 基于最近真实 attempt 的 retry 节流
    - 单步 side-effect 异常降级成 error payload，而不是直接打断整轮 iteration
  - 当前 iteration summary 已显式写出：
    - `risk_summary`
    - `runner_health`
    - `risk_alerts`
    - `risk_alert_summary`

当前 canonical target 边界：

- `live score-latest`
- `live quote-latest`
- `live check-latest`
- `live decide-latest`
- `live execution-simulate`
- `live execute-latest`
- `live runner-once`
- `live runner-loop`

这些命令当前只允许：

- `target=direction`

不再允许把 `target=reversal` 当成真实 side-effect 主线运行。

当前 read-only 边界：

- canonical operator 入口：
  - `live check-trading-gateway`
  - `live show-ready`
  - `live show-latest-runner`
- compatibility inspection 入口：
  - `live show-config`
  - `live show-layout`
  - 这些命令不负责给 operator 做 readiness 判断
  - 它们的职责是：
    - 展示请求配置/路径
    - 同时显式告诉你请求 scope 是否仍在 canonical live 范围内

当前 `execution-simulate` 的边界：

- 读取同一轮 decision snapshot
- 生成只读 execution snapshot
- 当前只做：
  - order type 选择
  - quote 价格读取
  - L1 fill proxy
  - 全深度 orderbook fill
  - orderbook limit reprice
  - read-only retry policy
  - read-only cancel policy contract
  - read-only redeem policy contract
  - 读取最新 `open_orders` / `positions` snapshot 做 policy 判定
- 当前还不做：
  - 真实下单 side effect
  - cancel / redeem side effect

当前 retry policy 的只读 contract：

- 如果 execution 因 orderbook/depth 原因被 block
  - 会返回 `pre_submit_depth_retry`
  - 语义对应 legacy 里的 orderbook fast retry

- 如果 execution 达到 `plan`
  - 会返回 `post_submit_order_retry`
  - 如果 order type 是 `FAK`，还会返回 `post_submit_fak_retry`
  - 如果 profile 启用了同窗重复成交，也会返回 `same_decision_repeat`

当前 cancel / redeem 的只读 contract：

- `cancel_policy`
  - source-of-truth 不是单独一个 `cancel_markets_when_minutes_left`
  - 至少同时依赖：
    - 当前 order type 是否会留下 resting order
    - market 的 `cycle_end_ts`
    - live open orders state
  - 当前 `deep_otm` 默认单型是 `FAK`
    - 因此默认会明确返回 `order_type_has_no_resting_order`
  - 对未来会留下挂单的 order type
    - 若缺少 `open_orders` snapshot，会返回 `open_orders_snapshot_missing`
    - 若 snapshot 可用且当前 market 有 open orders 且已进入 cancel window，会返回 `open_orders_present_in_cancel_window`
    - 不会假装已经具备真实 cancel side effect

- `redeem_policy`
  - source-of-truth 以 `condition_id + positions API redeemable state` 为主
  - `condition_id` 来自 canonical market catalog
  - redeemable 判定沿用 legacy `auto_redeem` 语义：
    - 需要 positions state
    - 需要 `redeemable`
    - 需要 `currentValue > 0` 或 `cashPnl > 0`
    - index set 由 `1 << outcomeIndex` 推导
  - 若缺少 `positions` snapshot，会返回 `positions_snapshot_missing`
  - 若 snapshot 可用且当前 `condition_id` 命中 redeem plan，会返回 `redeemable_positions_present`
  - 当前 execution snapshot 只输出 contract，不在这一步直接做真实 redeem side effect

当前已经显式接入的 liquidity guard：

- `live sync-liquidity-state`
  - 直接从 Binance spot + USDT perpetual REST 拉取：
    - 1m klines
    - bookTicker
    - open interest
  - 用 latest snapshot 持久化 `soft_fail_count / hard_fail_count / raw_fail_streak / raw_pass_streak / blocked_state`
  - source-of-truth 明确在 `liquidity state`，不把外部 REST 细节塞进 `decision`

- `live score-latest` / `check-latest` / `decide-latest`
  - 只读消费最新 `liquidity state`
  - 若 snapshot 判成 `blocked`
    - 会在 decision guard 里拒绝当前 market
  - 若 snapshot 缺失
    - 当前按 fail-open 处理，不会伪装自己已经具备 `regime controller`

当前已经显式接入的 regime controller：

- `live score-latest` / `check-latest` / `decide-latest`
  - 会基于：
    - 最新 feature frame 的 `ret_15m / ret_30m`
    - 最新 `liquidity state`
  - 生成独立的 `regime state`
    - 持久化 `state / target_state / pressure / pending_target / pending_count`
  - decision 当前已经会消费这个 `regime state`
    - 支持 `offset disabled`
    - 支持 `DEFENSE + direction pressure`
    - 支持 `min_dir_prob boost`
    - 支持 `DEFENSE trade-count cap`

- `live execution-simulate` / `live execute-latest`
  - 当前已经会消费 `regime state`
    - 若 `regime_apply_stake_scale=True`
      - 会按 `CAUTION / DEFENSE` multiplier 下调 `requested_notional_usd`
      - 会显式写出：
        - `stake_base_usd`
        - `stake_multiplier`
        - `stake_regime_state`

- `live show-latest-runner` / `live show-ready`
  - 当前已经会显式输出一阶 `capital_usage_summary`
    - 会把最新账户快照里的：
      - open-order notional
      - positions current value
      - positions cash pnl
    - 和当前：
      - focus market
      - execution budget
      - regime trade-count cap
    - 压成 operator 视图
  - 当前也会显式输出 foundation degradation chain：
    - `foundation_reason`
    - `foundation_issue_codes`
    - `foundation_degraded_tasks`
  - 当前也会显式输出 side-effect 后段分流字段：
    - `order_action_status`
    - `order_action_reason`
    - `account_state_status`
    - `cancel_action_status`
    - `cancel_action_reason`
    - `redeem_action_status`
    - `redeem_action_reason`
  - `show-ready` 当前还会主动带：
    - `open_orders` probe
    - `positions` probe
    - `gateway_failed_probes`
    - `operator_smoke_summary`
    - 针对 `foundation_ok_with_errors + oracle_direct_rate_limited` 的定向 `next_actions`
  - 可以显式区分：
    - 主路径 smoke 已通，但本轮 strategy no-trade
    - gateway / probe / runner infra 仍未通过

当前真实 operator 观测（2026-03-20，含 `14:09-14:11 UTC` 历史记录与 `14:28 UTC` 最新补记）：

- `sol + direct`
  - 第一轮 `show-ready`
    - `operator_smoke_summary.status=operational`
    - `operator_smoke_summary.reason=strategy_reject_only`
    - 当时是主路径已通，但本轮 strategy no-trade
  - 补跑一轮 `runner-once --dry-run-side-effects` 后
    - `decision.status=accept`
    - `execution.status=plan`
    - `order_action.status=ok`
    - `runner_health.blocking_issue_count=0`
  - 但再看 `show-ready`
    - `operator_smoke_summary.status=operational`
    - `operator_smoke_summary.reason=path_operational`
    - `ready_for_side_effects=false`
  - 当时真正拦住 operator 的，不再是 gateway / probe / runner 主路径，而是 `foundation.status=ok_with_errors`
  - 到 `2026-03-20 14:28 UTC` 左右再次执行：
    - gateway probe 仍健康：
      - `open_orders row_count=0`
      - `positions row_count=2002`
    - `show-ready` 返回：
      - `status=not_ready`
      - `primary_blocker=decision_not_accept`
      - `operator_smoke_summary.status=operational`
      - `operator_smoke_summary.reason=strategy_reject_only`
      - `foundation_status=ok_with_errors`
      - `foundation_issue_codes=["oracle_direct_rate_limited"]`
    - `next_actions` 已直接包含：
      - 等待 rate-limit window
      - 重跑 `data run live-foundation` 或 `runner-once --dry-run-side-effects`
      - 把 `oracle_prices_table` 作为 fail-open fallback
  - 这说明实时市场变化会让 `primary_blocker` 在“策略拒单”和“仅 foundation warning”之间切换，但 operator 视角已经能把两者拆开

- `xrp + direct`
  - 第一轮 `show-ready`
    - 还是 `runner_missing`
  - 补跑一轮 `runner-once --dry-run-side-effects` 后
    - 同样前移到：
      - `decision.status=accept`
      - `execution.status=plan`
      - `order_action.status=ok`
      - `runner_health.blocking_issue_count=0`
  - 再看 `show-ready`
    - `operator_smoke_summary.status=operational`
    - `operator_smoke_summary.reason=path_operational`
    - `ready_for_side_effects=false`

- 这两条线当前共同的剩余问题是：
  - live foundation 的 `oracle` 子任务会被 Polymarket direct `/api/crypto/crypto-price` rate limit
  - 当前不只在 foundation log 里记录 `Too Many Requests`
  - 也已经显式暴露到：
    - `risk_summary.foundation.reason`
    - `risk_summary.foundation.issue_codes`
    - `operator_summary.foundation_reason`
    - `operator_summary.foundation_issue_codes`
    - `operator_summary.foundation_degraded_tasks`
  - 运行态目前按 `fail_open` 回退到已有 `oracle_prices_table`
  - 所以：
    - 主路径 smoke 已通
    - `show-ready` 可能因为策略拒单或 foundation warning 保持 not-ready
    - 这属于 strategy/foundation 层解释，不属于交易接入故障

- 当前 regime 还没做的事：
  - 不会假装已经具备完整的 cash/equity 级 portfolio accounting
  - 更细的 portfolio-level regime accounting / capital usage 汇总

当前已经显式接入的 side effect：

- `live execute-latest`
  - 先生成最新 execution snapshot
  - 只在 `execution.status == plan` 时提交真实订单
  - 会把 `order_request` 规整成稳定 `action_key`
  - 对最近已成功的同一 action 跳过重复提交
  - 对最近失败的同一 action 按 retry interval 节流
  - `runner` 默认会复用同一套提交逻辑

- `live apply-cancel-policy`
  - 读取最新 `open_orders` snapshot
  - 用 canonical live market catalog 对齐 `cycle_end_ts`
  - 对落入 cancel window 的挂单执行真实 `cancel_order`
  - 会按候选 `order_id` 集合生成稳定 `action_key`
  - 对最近失败的同一批候选 cancel 做节流，避免 runner 高频重复打同一批单
  - `runner` 默认会复用同一套 cancel 逻辑

- `live apply-redeem-policy`
  - 读取最新 `positions` snapshot
  - 使用按 `condition_id` 聚合后的 redeem plan
  - 对 plan 中每个 condition 执行真实 `redeemPositions`
  - 会按 `condition_id + index_sets` 生成稳定 `action_key`
  - 对最近已成功的同一 redeem action 跳过重复执行
  - `runner` 默认会复用同一套 redeem 逻辑

---

## 14. 路径与状态对象规划

### 13.1 signals

```text
v2/var/live/state/signals/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/latest.json
v2/var/live/state/signals/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/snapshots/snapshot_ts=<ts>/signal.json
```

### 13.2 decisions

```text
v2/var/live/state/decisions/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/latest.json
v2/var/live/state/decisions/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/snapshots/snapshot_ts=<ts>/decision.json
```

当前 `live decide-latest` 已经做的事：

- 读取同一轮 `signal_snapshot`
- 构造同一轮 `quote_snapshot`
- 对每个 offset 应用 quote-aware runtime guards
- 输出 decision snapshot

### 13.3 guard materialization

当前没有单独的 canonical `guards/` state 目录。

guard 结果目前固定落在：

- `decision snapshot`
- `liquidity state`
- `regime state`

### 13.4 execution

```text
v2/var/live/state/execution/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/latest.json
v2/var/live/state/execution/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/snapshots/snapshot_ts=<ts>/execution.json
```

当前 execution snapshot 至少会包含：

- `execution.status`
- `execution.execution_reasons`
- `execution.depth_plan`
- `execution.repriced_metrics`
- `execution.retry_policy`
- `execution.cancel_policy`
- `execution.redeem_policy`
- `execution.market_cycle_end_ts`
- `execution.condition_id`

### 13.5 liquidity state

```text
v2/var/live/state/liquidity/cycle=<cycle>/asset=<asset>/profile=<profile>/latest.json
v2/var/live/state/liquidity/cycle=<cycle>/asset=<asset>/profile=<profile>/snapshots/snapshot_ts=<ts>/liquidity.json
```

当前 `live sync-liquidity-state` 已经做的事：

- 读取 profile 里的 Binance spot/perp liquidity thresholds
- 直接拉取 Binance 现货 / 合约 1m klines、bookTicker、open interest
- 产出 raw result + temporal filter 之后的 blocked state
- 将 `soft_fail_count / hard_fail_count / raw_fail_streak / raw_pass_streak` 持久化

### 13.6 regime state

```text
v2/var/live/state/regime/cycle=<cycle>/asset=<asset>/profile=<profile>/latest.json
v2/var/live/state/regime/cycle=<cycle>/asset=<asset>/profile=<profile>/snapshots/snapshot_ts=<ts>/regime.json
```

当前 `regime state` 已经做的事：

- 复用 latest `liquidity state`
- 复用当前 live feature frame 的 `ret_15m / ret_30m`
- 输出与 legacy 对齐的：
  - `state`
  - `target_state`
  - `pressure`
  - `reason_codes`
  - `pending_target`
  - `pending_count`

### 13.7 account state

```text
v2/var/live/state/open_orders/asset=<asset>/latest.json
v2/var/live/state/open_orders/asset=<asset>/snapshots/snapshot_ts=<ts>/orders.json
v2/var/live/state/positions/asset=<asset>/latest.json
v2/var/live/state/positions/asset=<asset>/snapshots/snapshot_ts=<ts>/positions.json
```

当前 `live sync-account-state` 已经做的事：

- 读取当前环境里的 Polymarket 账户配置
- 拉取最新 open orders snapshot
- 拉取最新 positions snapshot
- 将 positions 规整成按 `condition_id` 聚合的 redeem plan

当前这个命令还不做的事：

- 真实下单
- 真实 cancel side effect
- 真实 redeem side effect

### 13.8 cancel / redeem actions

```text
v2/var/live/state/orders/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/latest.json
v2/var/live/state/orders/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/snapshots/snapshot_ts=<ts>/order.json
v2/var/live/state/cancel/cycle=<cycle>/asset=<asset>/profile=<profile>/latest.json
v2/var/live/state/cancel/cycle=<cycle>/asset=<asset>/profile=<profile>/snapshots/snapshot_ts=<ts>/cancel.json
v2/var/live/state/redeem/cycle=<cycle>/asset=<asset>/profile=<profile>/latest.json
v2/var/live/state/redeem/cycle=<cycle>/asset=<asset>/profile=<profile>/snapshots/snapshot_ts=<ts>/redeem.json
```

当前这些 action 已经做的事：

- `orders`
  - 保存每次真实下单 action 的请求 / 响应结果
  - 保存 `action_key / attempt / last_attempt_* / gate`
- `cancel`
  - 保存每次 cancel policy 执行的候选单与执行结果
  - 保存同一批候选 cancel 的幂等 / retry gate
- `redeem`
  - 保存每次 redeem policy 执行的候选 condition 与执行结果
  - 保存同一批 redeem condition 的幂等 / retry gate

### 13.9 quotes

```text
v2/var/live/state/quotes/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/latest.json
v2/var/live/state/quotes/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/snapshots/snapshot_ts=<ts>/quote.json
```

### 13.10 runner

```text
v2/var/live/state/runner/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/latest.json
v2/var/live/state/runner/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/snapshots/snapshot_ts=<ts>/run.json
v2/var/live/logs/runner/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/runner.jsonl
```

---

## 15. 当前结论

当前 `v2/live` 的正确主线应该是：

1. `research/active_bundles` 明确当前实盘权重
2. `research/model_bundles` 保存可部署 bundle
3. `live` 从 `v2/data/live` 读取 canonical 数据
4. `live` 按 legacy v6 公式构造实时特征
5. `live` 按 profile 黑名单 + bundle 允许项做置零
6. `live` 用 active bundle 打分
7. `live` 把结果写入 `v2/var/live/state/signals`
8. `live` 继续产出 `quote` / `decision` / `execution` state
9. 当前 `decision` 已接入只读 `liquidity guard`，source-of-truth 是独立的 `liquidity state`
10. 当前已接入显式真实下单 / cancel / redeem side effect，以及 `runner` 默认自动编排
11. 当前 `orders` / `cancel` / `redeem` action 已带基础幂等、失败节流、单步失败降级；剩余主线缺口主要回到 `regime controller` 与 runner 监控风控收口

最重要的约束只有两条：

- 实验权重可以很多，但实盘永远只有一个 active 入口
- live 行为必须以旧实盘运行代码语义为主，而不是以临时研究近似为主
