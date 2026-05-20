# Codex Research Program

This is the canonical dense autoresearch entry for the `deep_otm_baseline`
`direction` line in `sol+xrp` mode.

## Canonical References

- Active session:
  `sessions/deep_otm_baseline_direction_dense_sol_xrp_autoresearch/session.md`
- Active results log:
  `sessions/deep_otm_baseline_direction_dense_sol_xrp_autoresearch/results.tsv`

## Frozen Dense Window

- frozen decision / backtest window from `2026-04-15` through `2026-05-07`
- dense goal: 10-20 trades per coin per day
- target band over `2026-04-15` through `2026-05-07`: `110-220` trades per coin

## Dense Direction Objective

- run only the dense `direction` track
- target fixed to `direction`
- coins: `sol`, `xrp`
- sparse winners cannot become frontiers
- count must be checked before ROI

## Dense Count Gate

- reject `< 56` trades per coin over the frozen window
- classify `56-139` as `subtarget`
- classify `140-280` as `on_target`
- classify `281+` as `over_target`

## Strategy Lock

- do not change the strategy gates to create more trades
- keep the `<= 0.30` final tradeable winner-side entry rule fixed
- keep the frozen window, target, stake, and max-trade judge fixed unless the
  operator explicitly changes the strategy
- treat `probability_gate` and `entry_price_gate` as diagnostics about missing
  factor information or model-input quality, not permission to loosen execution
  rules
- every new branch must write a failure-to-factor thesis: which failed gate,
  which missing information, which factor family or supported model-input
  change, and why it should pass the existing strategy unchanged

## Width Search Band

- feature-set width is not fixed to `40`
- allowed width ladder: `30 / 34 / 38 / 40 / 44 / 48 / 56`
- move width by one bucket per bounded cycle only
- below `56` trades, prefer the next wider bucket before another same-width cosmetic swap
- inside `140-280` trades, keep width stable and prefer family replacement before changing width again
- if count is clearly excessive and quality degrades, consider only the next narrower bucket

## Allowed Research Levers

- change the exact factor set when a coin-track is sparse, noisy, or repeatedly
  missing profitable-pool captures
- change the feature-count bucket instead of assuming one fixed width
- change weight settings, including `winner_in_band_weight`,
  `offset_weight_overrides`, and named `weight_variants`, when winner-side rows
  are visible but not converted into enough captures
- change the model family or ensemble recipe when factor, width, and weight
  changes stop improving count or capture quality
- model changes may include the current tree model, CatBoost-capable variants,
  or a bounded three-model ensemble when the repository already supports that
  launch path
- keep only one primary lever per follow-up: factor identity, feature-count
  bucket, weighting, or model/ensemble
- do not invent a new trainer or unsupported model path inside the quick-screen
  loop; use only repository-supported training and launch options

## Profitable Offset Pool Gate

- profitable offset pool is coin-level and shared by both dense tracks
- profitable offset pool window is `2026-04-15` through `2026-05-07` at `2usd`
- one `offset` equals one exact window
- count a capture only when the candidate reaches a final tradeable winner-side entry at `<= 0.30`
- target about `70%` profitable-pool coverage before spending a full formal slot
- formal frontier decisions still require full orderbook validation

## Candidate Ranking

- in quick screen, rank candidates by profitable-pool capture quality first, then by total trades, and only then by ROI
- a `reject_sparse` candidate cannot outrank a `subtarget` or `on_target` candidate only because its ROI looks better
- for formal promotion, rank candidates in this order: dense gate, total trades, winner-side or capture evidence, then ROI

## Required Funnel Diagnosis

- every completed quick-screen summary must inspect `density_bottleneck`
- record the primary bottleneck before designing the next branch:
  `probability_gate`, `entry_price_gate`, `conversion_gap`,
  `quote_coverage_gap`, `low_trade_density`, or `balanced_or_mixed`
- if `probability_gate` dominates, the next non-repair branch must be a
  supported model-family or model-input experiment that improves recognition
  under the existing probability gate, not another cosmetic factor shuffle
- if `entry_price_gate` dominates, the next non-repair branch must test factor
  families that identify tradeable low-price winners inside the existing entry
  rule
- if `conversion_gap` dominates, use training weight or factor-input rework
  before changing factor identity
- if `quote_coverage_gap` dominates, repair data/orderbook coverage before
  launching a new branch
- if `low_trade_density` dominates and no single reject reason dominates, change
  feature-count bucket or factor family before reviewing ROI

## Forced Stagnation Escalation

- after `3` consecutive completed sparse screens on the same coin/track, do not
  enqueue another same-width, same-model, same-family retry
- after `3` same-width sparse retries below `56` trades, the next non-repair
  branch must change feature-count bucket when a wider bucket remains available
- after `3` same-model sparse retries and `density_bottleneck` says
  `probability_gate`, the next non-repair branch must change supported
  model/ensemble or model-input recipe
- SOL and XRP must be routed independently:
  - XRP direction near the current `40-55` trade band should prioritize density
    release around the best capture family until it reaches `56+`
  - SOL direction below `20` trades should skip weight-only cosmetic retries and
    use feature-count, factor-family, or model/calibration rework

## Failure Routing

- if a coin-track hits `3` consecutive completed fast screens with zero profitable-pool captures, do not stay in cosmetic same-parent tweaks
- if the historical digest marks `next_route=weight_search_first`, first try weight search with `winner_in_band_weight`, `offset_weight_overrides`, or named `weight_variants`
- use weight-first routing when the line can already find winner-side pool rows but still fails to convert enough of them into captures
- if the historical digest marks `next_route=factor_rework_first`, switch factor family or width first and do not spend that cycle on weight-only retries
- factor-first routing means the line still does not show enough proven winner-side recognition, so a material feature change is required before another threshold-style pass

## Hard Constraints

- run SOL/XRP dense work through the shared queue in `quick_screen` mode only
- this direction track owns up to `5` quick-screen slots; do not stop at one
  successor per coin when open slots remain
- SOL and XRP may each have multiple queued same-track branches at the same time
  if every branch changes a meaningful lever such as width, factor family,
  weighting, or supported model/calibration
- do not directly launch full formal SOL/XRP experiments from this session
- every SOL/XRP queue item must launch with `--launch-mode quick_screen`, `--quick-screen-top-k 1`, and `--quick-screen-train-parallel-workers 2`
- a full formal promotion requires an explicit operator instruction in a separate step; quick-screen evidence alone is not permission to start formal
- do not launch BTC or ETH from this session
- do not open `reversal` or `hybrid` runs in this session
- sparse winners cannot become frontiers even if ROI looks strong
- count must be checked before ROI in every formal frontier decision
- keep track decisions coin-specific under the frozen dense window
