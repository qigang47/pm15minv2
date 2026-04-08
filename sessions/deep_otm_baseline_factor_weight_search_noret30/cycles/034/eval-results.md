# Cycle 034 Evaluation

## Checked latest artifacts before action

At `2026-04-07 22:00-22:58 HKT`, I re-read `program.md`, `session.md`, `results.tsv`, and the latest cycle artifacts before making any change.

- Cycle `033` had already identified `ETH 38_v3` as the only fresh near-`40` candidate worth real `1 USD / max3` evidence.
- A partial cycle `034` launch already existed for `baseline_focus_feature_search_eth_reversal_38band_20260407`, so opening a new label would have duplicated the current queue head.
- The required single action for this cycle was therefore to inspect, recover if needed, and continue the same ETH formal label rather than opening another coin or another family.

## Existing run inspection this cycle

Before rerunning anything, I inspected the active ETH `38band` formal run state:

- `research/experiments/runs/suite=baseline_focus_feature_search_eth_reversal_38band_20260407/run=auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407` already existed and initially contained only seed-case-started events.
- The first launch had produced training and bundle artifacts, but no formal backtest outputs had appeared for more than ten minutes after those artifacts stopped changing.
- I confirmed the original launch chain was no longer progressing, so repeating the **same** suite / run label counted as recovery rather than a duplicate launch.
- After recovery, direct process and file inspection showed the ETH runner actively reading the orderbook source files for `2026-03-31` through `2026-04-03`, so the resumed run is genuinely progressing under the same label.

## Formal action this cycle

I resumed the existing ETH formal validation with the canonical runner:

- suite: `baseline_focus_feature_search_eth_reversal_38band_20260407`
- run label: `auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407`
- market: `eth`

Cycle-local recovery artifact:

- `sessions/deep_otm_baseline_factor_weight_search_noret30/cycles/034/eth-38band-formal-resume.log`

No code or suite spec was changed in this cycle.

## Summary captured this cycle

I used `scripts/research/summarize_experiment.py` against the in-progress run directory after the first resumed case finished and stored the partial canonical summary here:

- `sessions/deep_otm_baseline_factor_weight_search_noret30/cycles/034/eth-38band-formal-partial-summary.json`

Current completed real-evidence result inside this still-active run:

- `focus_eth_38_v3`: `16` trades, `-0.2966` pnl, `-1.8537%` ROI.

The run directory now contains finished formal artifacts for that completed case, including:

- `research/experiments/runs/suite=baseline_focus_feature_search_eth_reversal_38band_20260407/run=auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407/summary.json`
- `research/experiments/runs/suite=baseline_focus_feature_search_eth_reversal_38band_20260407/run=auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407/leaderboard.csv`
- `research/backtests/cycle=15m/asset=eth/profile=deep_otm_baseline/spec=baseline_truth/run=auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407-eth-focus_search-focus_search__fs_38_v3__max3__stake_1usd__max_3usd-backtest-77f78db3/summary.json`

## Current active state

The ETH suite is **not finished yet**.

- After the `38_v3` case completed, `suite.jsonl` advanced into the next execution group: `focus_search__fs_38_v4__max3`.
- The same ETH formal label remains active, so this cycle did **not** open any additional formal coin.
- For the current search line, the active formal market count remains `1`, which stays inside the `<= 2` cap from `program.md`.

## Interpretation

- The first fresh real-evidence case is negative: `ETH 38_v3` does **not** currently improve on the earlier positive-but-thin ETH `40_v2` control story.
- Even so, this does not close the ETH `38band` question yet because `38_v4` is now running under the same formal label.
- The right next step is to finish this same ETH suite rather than opening another new formal label prematurely.

## Next trigger

- The next cycle should inspect or resume `auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407` until the remaining ETH `38band` comparison(s) finish.
- Once the suite fully finishes, summarize the final run again with `scripts/research/summarize_experiment.py` and then decide whether ETH `38band` should be rejected or compared more explicitly against the old ETH `40_v2` real baseline.
- Do not open a new BTC / SOL / XRP formal promotion before this ETH label settles.
