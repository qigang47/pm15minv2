# Codex Research Program

This is the canonical standalone autoresearch entry for the ETH midprice
direction line.

## Canonical References

- Active session:
  `sessions/deep_otm_midprice_direction_eth_autoresearch/session.md`
- Active results log:
  `sessions/deep_otm_midprice_direction_eth_autoresearch/results.tsv`

## Frozen Formal Window

- training window ends at `2026-04-15`
- decision / backtest window is `2026-04-15` through `2026-05-07`
- suite seed: `baseline_midprice_direction_eth_2usd_5max_train0415_backtest0507_20260501`
- baseline run to compare against:
  `auto_eth_direction_entry45_50_prob60_formal_after_full_backfill_20260424`

## Objective

- coin: `eth`
- target fixed to `direction`
- run full formal experiments only
- keep up to two ETH formal experiments live at a time
- do not use the shared SOL/XRP quick-screen queue
- every cycle must either verify a live ETH run, analyze a completed ETH result,
  or launch the next ETH formal follow-up

## Midpoint Entry Policy

- compare the direction signal against the `0.50` midpoint
- UP is eligible only when the model is clearly above the midpoint
- DOWN is eligible only when the model is clearly below the midpoint
- use `0.60` as the minimum selected-side direction probability threshold
- accepted orderbook entry price must stay in `0.45-0.50`
- stake is `2usd`, with Kelly-style scaling capped at `10usd`
- keep the `5` filled-trades-per-offset formal judge fixed; do not raise this
  cap to increase sample size
- this is not the `<= 0.30` reversal profitable-pool rule

## Strategy Lock

- do not change the midpoint entry policy
- do not change the `0.60` probability threshold
- do not change the `0.45-0.50` accepted entry band
- do not change stake sizing, max-trade caps, target, or frozen formal window
  unless the operator explicitly changes the strategy
- treat probability gate and entry-band gate results as diagnostics about
  missing factor information or model-input quality, not permission to loosen
  execution rules
- every new branch must write a failure-to-factor thesis: which failed gate,
  which missing information, which factor family or supported model-input
  change, and why it should pass the existing strategy unchanged

## Decision Rules

- rank by formal trade count first, then net profit, then ROI
- sparse profitable runs are not enough; target higher coverage inside the fixed
  max5 judge before treating a family as usable
- 10 trades is still sparse; use it only as a diagnostic result, not a promotion
  result
- if the current result is positive but sparse, widen or replace the factor
  family to increase count while keeping the midpoint policy and max5 judge
  unchanged
- if count improves but quality collapses, revert to the best previous family and
  make a smaller factor or weight adjustment
- if a run fails, repair the failure before opening a new branch
- every completed sparse result must be classified by the dominant blocker:
  probability gate, entry-band gate, conversion gap, feature-density gap, or
  data/label failure
- do not review ROI as a promotion signal while trades remain below `56`

## Allowed Research Levers

- change the exact factor set when the latest completed run is sparse or noisy
- change the feature-count bucket instead of assuming 56 factors is fixed
- allowed feature-count ladder: `24 / 32 / 40 / 48 / 56 / 64`
- if trades are too sparse, widen the feature-count bucket or add missing
  timing, persistence, cross-asset, or volatility-state information
- if trades are frequent but quality is poor, shrink the feature-count bucket or
  remove noisy/redundant short-horizon price families
- change the model family or ensemble recipe when factor changes stop improving
  trade count or quality
- model changes may include the current tree model, CatBoost-capable variants,
  or a bounded three-model ensemble when the repository supports that path
- if several consecutive completed follow-ups reuse the same model family and
  same feature-count bucket, the next non-repair cycle must prioritize either a
  feature-count change or a supported model/ensemble change before another
  cosmetic same-family factor retry
- after `3` consecutive sparse completions below `56` trades, the next
  non-repair cycle must change feature-count bucket or supported model/ensemble;
  do not launch another same-width, same-model factor shuffle
- if the same feature-count bucket has failed `3` times and a wider bucket
  remains available, move one bucket wider before another same-width retry
- if the same model family has failed `3` times and the blocker is probability
  or calibration, use a supported model/ensemble change before another factor
  identity change
- keep only one primary lever per follow-up: factor identity, feature-count
  bucket, weighting, or model/ensemble
- keep the midpoint entry policy and frozen formal window unchanged while
  testing these levers

## Launch Rules

- launch through `auto_research/run_one_experiment_background.sh`
- use `--launch-mode formal`
- pass `--market eth`
- pass `--expected-concurrency 2`
- write bootstrap logs under this session's `bootstrap/` directory
- use a unique run label for every follow-up so results are not overwritten

## Hard Constraints

- do not launch BTC, SOL, or XRP from this ETH session
- do not open reversal or hybrid runs from this session
- do not enqueue into the shared dense queue
- do not change the frozen formal window unless explicitly instructed
