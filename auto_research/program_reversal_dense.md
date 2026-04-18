# Codex Research Program

This is the canonical dense autoresearch entry for the `deep_otm_baseline`
`reversal` line.

## Canonical References

- Active session:
  `sessions/deep_otm_baseline_reversal_dense_autoresearch/session.md`
- Active results log:
  `sessions/deep_otm_baseline_reversal_dense_autoresearch/results.tsv`

## Frozen Dense Window

- frozen decision / backtest window from `2026-04-01` through `2026-04-15`
- dense goal: 10-20 trades per coin per day
- target band over `2026-04-01` through `2026-04-15`: `150-300` trades per coin

## Dense Reversal Objective

- run only the dense `reversal` track
- target fixed to `reversal`
- coins: `btc`, `eth`, `sol`, `xrp`
- sparse winners cannot become frontiers
- count must be checked before ROI

## Dense Count Gate

- reject `< 56` trades per coin over the frozen window
- classify `56-139` as `subtarget`
- classify `140-280` as `on_target`
- classify `281+` as `over_target`

## Width Search Band

- feature-set width is not fixed to `40`
- allowed width ladder: `30 / 34 / 38 / 40 / 44 / 48`
- move width by one bucket per bounded cycle only
- below `56` trades, prefer the next wider bucket before another same-width cosmetic swap
- inside `140-280` trades, keep width stable and prefer family replacement before changing width again
- if count is clearly excessive and quality degrades, consider only the next narrower bucket

## Profitable Offset Pool Gate

- profitable offset pool is coin-level and shared by both dense tracks
- profitable offset pool window is `2026-04-01` through `2026-04-15` at `2usd`
- one `offset` equals one exact window
- count a capture only when the candidate reaches a final tradeable winner-side entry at `<= 0.30`
- target about `70%` profitable-pool coverage before spending a full formal slot
- formal frontier decisions still require full orderbook validation

## Hard Constraints

- do not open `direction` or `hybrid` runs in this session
- sparse winners cannot become frontiers even if ROI looks strong
- count must be checked before ROI in every formal frontier decision
- keep track decisions coin-specific under the frozen dense window
