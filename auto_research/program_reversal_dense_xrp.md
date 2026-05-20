# Codex Research Program

This is the canonical dense autoresearch entry for the `deep_otm_baseline`
`reversal` line in `xrp`-only mode.

## Canonical References

- Active session:
  `sessions/deep_otm_baseline_reversal_dense_xrp_autoresearch/session.md`
- Active results log:
  `sessions/deep_otm_baseline_reversal_dense_xrp_autoresearch/results.tsv`

## Frozen Dense Window

- training window ends at `2026-04-15`
- decision / backtest window is `2026-04-15` through `2026-05-07`
- dense goal: 10-20 trades per coin per day
- target band over `2026-04-15` through `2026-05-07`: `110-220` trades per coin

## Dense Reversal Objective

- run only the dense `reversal` track
- target fixed to `reversal`
- coins: `xrp`
- sparse winners cannot become frontiers
- count must be checked before ROI

## Dense Count Gate

- reject `< 56` trades per coin over the frozen window
- classify `56-139` as `subtarget`
- classify `140-280` as `on_target`
- classify `281+` as `over_target`

## Width Search Band

- feature-set width is not fixed to `40`
- allowed width ladder: `30 / 34 / 38 / 40 / 44 / 48 / 56`
- move width by one bucket per bounded cycle only
- below `56` trades, prefer the next wider bucket before another same-width cosmetic swap
- inside `140-280` trades, keep width stable and prefer family replacement before changing width again
- if count is clearly excessive and quality degrades, consider only the next narrower bucket

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

## Failure Routing

- if a coin-track hits `3` consecutive completed fast screens with zero profitable-pool captures, do not stay in cosmetic same-parent tweaks
- if the historical digest marks `next_route=weight_search_first`, first try weight search with `winner_in_band_weight`, `offset_weight_overrides`, or named `weight_variants`
- use weight-first routing when the line can already find winner-side pool rows but still fails to convert enough of them into captures
- if the historical digest marks `next_route=factor_rework_first`, switch factor family or width first and do not spend that cycle on weight-only retries
- factor-first routing means the line still does not show enough proven winner-side recognition, so a material feature change is required before another threshold-style pass

## Hard Constraints

- do not open `direction` or `hybrid` runs in this session
- sparse winners cannot become frontiers even if ROI looks strong
- count must be checked before ROI in every formal frontier decision
- keep track decisions coin-specific under the frozen dense window
