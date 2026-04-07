# Cycle 034 Evaluation

## Checked latest artifacts before action

At `2026-04-07 21:59-22:13 HKT`, I re-read `program.md`, `session.md`, `results.tsv`, and the latest cycle `032/033` artifacts before taking any new action.

- Cycle `033` already established that `ETH 38_v3` is the only fresh near-`40` candidate that clears the “worth formal evidence” bar.
- I inspected the relevant experiment directories before launching anything new and confirmed there was no existing formal run label yet for `baseline_focus_feature_search_eth_reversal_38band_20260407` under `auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407`.
- No other active real `1 USD / max3` formal market run needed protection this cycle, so opening one `ETH` slot stayed within the `<= 2` formal-run cap.

## Formal action launched this cycle

I launched exactly one canonical formal orderbook run:

- Script: `scripts/research/run_one_experiment.sh`
- Suite: `baseline_focus_feature_search_eth_reversal_38band_20260407`
- Run label: `auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407`
- Market: `eth`
- Session log: `sessions/deep_otm_baseline_factor_weight_search_noret30/cycles/034/eth-38band-formal.log`
- Launch capture: `sessions/deep_otm_baseline_factor_weight_search_noret30/cycles/034/eth-38band-formal-launch.out`

Fresh artifacts created during this launch:

- Training run directory: `research/training_runs/cycle=15m/asset=eth/model_family=deep_otm/target=reversal/run=auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407-eth-reversal-train-off7-8-9-00c12caf`
- Model bundle directory: `research/model_bundles/cycle=15m/asset=eth/profile=deep_otm_baseline/target=reversal/bundle=auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407-eth-reversal-bundle-off7-8-9-dc3f32c8`

## State observed before ending the cycle

The formal launch did not complete cleanly inside this cycle:

- `sessions/deep_otm_baseline_factor_weight_search_noret30/cycles/034/eth-38band-formal.log` and `sessions/deep_otm_baseline_factor_weight_search_noret30/cycles/034/eth-38band-formal-launch.out` both end with `Terminated: 15`, so the runner was stopped before it could finish the formal experiment.
- The training run and model bundle were both built successfully during this cycle, so the interrupted work reached model preparation even though the formal experiment itself did not finish.
- No `research/backtests/...auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407...` directory exists.
- The transient experiment run directory is no longer present after termination, so there is no persisted `summary.json`, `leaderboard.csv`, `report.md`, or `logs/suite.jsonl` left behind for the formal run itself.
- I explicitly attempted the standard summary command anyway; it failed because there is no surviving completed experiment summary, with outputs saved to `sessions/deep_otm_baseline_factor_weight_search_noret30/cycles/034/eth-38band-formal-summary-attempt.out` and `sessions/deep_otm_baseline_factor_weight_search_noret30/cycles/034/eth-38band-formal-summary-attempt.err`.
- A direct scan of the surviving training and bundle artifacts found no traceback or explicit failure line explaining the termination.

I saved an explicit inspection snapshot here:

- `sessions/deep_otm_baseline_factor_weight_search_noret30/cycles/034/eth-38band-formal-inspection.json`

## Decision from this cycle

- `ETH 38band` remains the next formal candidate, but this cycle did **not** leave behind an active completed-or-resumable formal experiment directory.
- I did not open `BTC`, `SOL`, or `XRP` formal validation because spending the single-cycle slot on the `ETH` launch attempt was still the correct queue decision.
- No code change or suite-spec edit was needed; the existing `ETH 38band` suite already matches the current search policy.

## Constraint check

- Only one formal market launch attempt was opened this cycle.
- I did not launch any duplicate suite/run-label pair.
- Because the launch was terminated and no formal experiment run directory survived, this cycle ends with `0` active new formal market runs.
- The launched suite preserves the required train and decision windows and keeps `disable_ret_30m_direction_guard = true`.

## Next trigger

- The next cycle should relaunch or recover `auto_focus_feature_search_eth_reversal_38band_formal1usd_r1_20260407` under the same label, using the surviving training and bundle artifacts rather than opening a different formal candidate.
- Once a completed experiment run directory finally produces `summary.json`, the next cycle should immediately run `scripts/research/summarize_experiment.py` and record the real `1 USD / max3` leaderboard.
- `BTC` remains the secondary near-`40` watch item, while `SOL` and `XRP` should stay out of formal promotion until they stop producing zero-trade quick-screen outcomes.
