# Research Session

## Cycles completed

- `000`
  - Switched active automation objective from old direction rescue to reversal quick-screen continuation.
  - Registered 4 active quick-screen runs for BTC ETH SOL XRP and instructed background cycles not to duplicate them.

- `001`
  - Confirmed all four April 6 reversal weight-search quick screens finished, so there are no duplicate active runs left from that batch.
  - Recorded current per-coin quick-screen leaders: BTC fs30/current_default, ETH fs30/offset_reversal_mild, XRP fs30/offset_reversal_strong, while SOL remains effectively zero-trade.
  - Ran one SOL follow-up quick screen that removed each extra factor from focus_sol_34_v4 under no_vol_weight.
  - The new SOL run did not improve tradeability; base34 and drop_dow_sin tied best at 0 trades and 4 backed winners in band.

  - Follow-up phase now requires 4-coin concurrency instead of single-coin continuation.
  - Active follow-up quick-screen batch: BTC drop30, ETH drop30, SOL drop33, XRP drop30.

- `002`
  - Confirmed the synchronized BTC ETH SOL XRP reversal follow-up batch from cycle 001 is still running under the background loop.
  - No quick-screen summaries exist yet for BTC drop30, ETH drop30, SOL drop33, or XRP drop30, so this cycle did not launch any duplicate work.
  - Recorded a monitor-only cycle; the next cycle should summarize the four-coin batch after the current runs finish.

- `003`
  - Confirmed the synchronized BTC ETH SOL XRP reversal feature-drop batch finished and summarized the four leaderboards together.
  - BTC and ETH kept their base30 leaders; XRP improved slightly with drop_atr_14 but still lacks traded winners; SOL still has 0 trades even with drop_dow_sin.

  - Created the next synchronized obv_z swap follow-up batch: BTC swaps macd_z, ETH swaps atr_14, SOL swaps dow_sin, and XRP swaps atr_14.
  - Launched the four new quick-screen suites and left them running for the next monitoring cycle.

- `004`
  - Recovered the stalled synchronized BTC ETH SOL XRP obv_z swap batch by rerunning the missing quick-screen outputs.
  - Summarized the completed batch with the standard summary tool after adding quick-screen summary support.
  - BTC base30 held, ETH drop_atr_14 led, SOL swap_obv_z only improved a tie-break while still at 0 trades, and XRP drop_atr_14 held.

  - Created the next synchronized strike_flip_count_cycle swap batch across BTC ETH SOL XRP.
  - Finished the four new strike_flip_count_cycle quick-screen suites in the same cycle after rerunning them in the foreground.
  - ETH strike_flip_count_cycle became the new leader, XRP strike_flip_count_cycle nudged ahead on a tie-break, BTC base30 held, and SOL remained zero-trade with the obv_z swap still best.

- `005`
  - Completed the synchronized BTC ETH SOL XRP vwap_gap_20 swap quick-screen batch and summarized all four runs with the standard experiment summary tool.
  - BTC base30 held, ETH strike_flip_count_cycle held, SOL obv_z held at zero trades, and XRP vwap_gap_20 moved ahead on backed-winner tie-breaks.
  - Checked the active BTC ETH SOL XRP strike_flip_count_cycle formal promotion runs and confirmed they are still incomplete, so this cycle did not duplicate them.

- `006`
  - Completed the synchronized BTC ETH SOL XRP rv_30_lag1 swap quick-screen batch and summarized all four runs.
  - BTC and ETH controls held, SOL stayed zero-trade, and XRP rv_30_lag1 edged ahead only on tie-breaks.
  - Summarized the newly finished BTC 1 USD formal promotion run, which came in negative on real evidence.
  - ETH SOL XRP formal promotion runs are still incomplete, so this cycle did not duplicate them.

- `007`
  - Confirmed the synchronized BTC ETH SOL XRP strike_flip formal promotion batch is still active under live worker processes.
  - BTC and XRP each show only 1 completed formal case out of 3 variants, while ETH and SOL still have no finished formal artifacts.
  - No new experiment or code change was made because duplicating the active formal batch would violate the one-cycle rules.

- `008`
  - Rechecked the synchronized BTC ETH SOL XRP strike_flip formal promotion batch after cycle 007 and confirmed the four active python workers are still accumulating CPU time.
  - BTC and XRP still show only partial one-case artifacts, while ETH and SOL still have no finished formal artifacts beyond the suite log.
  - No new experiment or recovery was launched because rerunning the current four-coin formal batch would still be a duplicate.

- `009`
  - Rechecked the synchronized BTC ETH SOL XRP strike_flip formal promotion batch with status_autorun.sh plus direct process, log, and artifact inspection.
  - BTC ETH and XRP now each show one completed formal case but no suite has finished; SOL still has no finished formal artifact.
  - The current environment already has four active formal market workers plus an extra same-label ETH recovery launch, so this cycle intentionally started nothing new.

- `010`
  - Rechecked the active BTC ETH SOL XRP strike_flip formal promotion batch and confirmed the formal workload still exceeds the preferred 2-coin cap from program.md.
  - Recorded the newly visible XRP partial formal result: drop_atr_14 completed with 2 trades, -2.0 pnl, and -100.0% ROI at 1 USD / max3.
  - Did not launch any new experiment because the current formal batch remains active or partial, with SOL still lacking a completed formal artifact.

- `011`
  - Confirmed the synchronized BTC ETH SOL XRP strike_flip formal promotion batch has fully finished and is no longer an active duplicate risk.
  - Recorded final formal evidence: BTC base30 best but negative, ETH strike_flip best but negative, SOL still zero-trade, and XRP still -100% across variants.
  - Did not launch a new experiment in the same cycle; queued rv_30_lag1 as the next formal family to validate because it is the strongest remaining completed quick-screen line.

- `012`
  - Checked the queued rv_30_lag1 formal line, confirmed no existing XRP rv_30_lag1 formal run directory, and launched one XRP formal market while staying under the 2-coin cap.
  - Summarized the current XRP formal partial results after two completed cases: rv_30_lag1 and strike_flip_count_cycle each finished with 2 trades, -2.0 pnl, and -100.0% ROI at 1 USD / max3.
  - Did not launch any additional formal or quick-screen work because the same XRP run label still has the vwap_gap_20 control case in progress.

- `013`
  - Summarized the now-completed XRP rv_30_lag1 formal run and confirmed the whole XRP family finished at -100% ROI on real evidence.
  - Launched the first BTC rv_30_lag1 1 USD formal validation and recorded it as the only new active formal market this cycle.

- `014`
  - Retried the stalled BTC rv_30_lag1 formal run under the same suite and run label instead of opening a duplicate.
  - The resumed BTC run still stops after the remaining rv_30_lag1 seed-case stage, so this cycle recorded a recovery-only result.
  - Captured a standard summary snapshot showing the only completed BTC evidence is still the negative base30 control.

- `015`
  - Confirmed the relevant rv_30_lag1 formal landscape before acting: XRP already complete, ETH/SOL not started, and BTC was the only unfinished same-family formal run.
  - Resumed the existing BTC rv_30_lag1 formal run with the canonical formal runner instead of launching any duplicate or new coin.
  - Completed the full BTC 1 USD / max3 formal leaderboard: base30 stayed best at -2.9237% ROI, strike_flip finished at -14.8839%, and rv_30_lag1 finished worst at -25.3852%.
  - Queued ETH rv_30_lag1 as the next open formal slot because BTC and XRP are now finished for this family.

- `016`
  - Confirmed the queued ETH rv_30_lag1 formal line still had no existing formal run directory, while BTC/XRP were already complete and no other incomplete formal run remained active.
  - Launched the ETH rv_30_lag1 1 USD / max3 formal validation under the canonical formal runner and recorded the partial suite state with the standard summary script.
  - At bookkeeping stop, strike_flip_count_cycle led the partial ETH leaderboard at -5.9055% ROI across 23 trades, while rv_30_lag1 trailed at -26.7632% ROI across 14 trades.
  - Left the vwap_gap_20 control in progress and queued the next cycle to resume or summarize the same ETH run label instead of opening a duplicate.

- `017`
  - Retried the stalled ETH rv_30_lag1 formal run under auto_eth_reversal_rv30lag1_formal1usd_r1_20260407 instead of opening any duplicate or new coin.
  - The recovery attempt resumed the two completed cases but still left the vwap_gap_20 control stuck before any new backtest artifact appeared.
  - Recorded a standard summary snapshot and stopped without launching SOL so the unresolved ETH run keeps the queue head.

- `018`
  - Retried the stalled ETH rv_30_lag1 formal run under auto_eth_reversal_rv30lag1_formal1usd_r1_20260407 instead of launching any duplicate or new coin.
  - This recovery progressed the missing vwap_gap_20 control through fresh training and bundle artifacts, but it still did not produce a formal backtest directory.
  - Recorded a standard summary snapshot and kept SOL queued behind unresolved ETH rather than opening a new formal slot.

- `019`
  - Retried the stalled ETH rv_30_lag1 formal run under auto_eth_reversal_rv30lag1_formal1usd_r1_20260407 instead of launching any duplicate or new coin.
  - The recovery eventually completed the missing vwap_gap_20 control and finished the full ETH 1 USD / max3 formal leaderboard.
  - strike_flip_count_cycle stayed best for ETH at -5.9055% ROI across 23 trades, vwap_gap_20 finished second at -15.1157%, and rv_30_lag1 stayed worst at -26.7632%.
  - Queued SOL rv_30_lag1 as the next open formal slot because ETH is now fully summarized.

- `020`
  - Re-read the 2026-04-07 operator override and confirmed the synchronized 40-factor quick-screen batch now outranks the older queued SOL rv_30_lag1 formal idea.
  - Inspected the four BTC ETH SOL XRP 40family quick-screen run directories and found that all four exist but still lack any finished quick-screen summary artifact.
  - Recorded a monitor-only cycle and deliberately launched no duplicate quick-screen or formal run.

- `021`
  - Confirmed the synchronized BTC ETH SOL XRP 40-factor quick-screen batch is now fully complete, with BTC finishing during this cycle after ETH SOL and XRP had already produced outputs.
  - Summarized all four 40-factor quick-screen runs; ETH 40_v2 led clearly, BTC 40_v4 was secondary, and SOL/XRP remained zero-trade.
  - Launched exactly one new formal orderbook run for ETH under auto_focus_feature_search_eth_reversal_40family_formal1usd_r1_20260407 and left it active for the next cycle.

- `022`
  - Summarized the completed ETH 40family formal validation and confirmed focus_eth_40_v2 delivered 12 trades with a small positive ROI at 1 USD / max3.
  - Launched the BTC 40family 1 USD / max3 formal validation under the next available slot and confirmed the new run reached seed-case start without duplicating an existing label.

- `023`
  - Re-read the latest program and session artifacts after concurrent background updates had already advanced the top-level session to cycle 022.
  - Captured the fully completed ETH 40family formal summary; the final 3-case result ends with focus_eth_40_v4 highest on ROI, while focus_eth_40_v2 and v3 stay nearly flat but much more active.
  - Confirmed BTC 40family formal is already active under its existing label, so this cycle launched nothing new and recorded a monitor-only handoff.

- `024`
  - Re-read `program.md`, `session.md`, `results.tsv`, and the latest BTC/ETH 40family artifacts before acting, then confirmed BTC remained the only incomplete duplicate-sensitive 1 USD formal run.
  - Resumed the existing BTC 40family formal run under `auto_focus_feature_search_btc_reversal_40family_formal1usd_r1_20260407` instead of opening any new formal label.
  - Completed the full BTC 40family 1 USD / max3 formal leaderboard: `focus_btc_40_v2` and `focus_btc_40_v3` both finished at `0` trades / `0.0` pnl / `0.0%` ROI, while `focus_btc_40_v4` traded `5` times and finished at `-5.0` pnl / `-100.0%` ROI.
  - BTC therefore has no acceptable promoted winner from the current `40_v2/v3/v4` family, while ETH remains the only coin in this 40family line with positive real evidence.

- `025`
  - Compared ETH 40family leaders against the old 30_v4 control on the real 1 USD / max3 judge instead of opening another blind coin promotion.
  - Confirmed ETH 40_v4 still leads on ROI while ETH 40_v2 stays the higher-trade stability candidate and the old 30_v4 control is clearly negative.

- `026`
  - Compared the completed BTC and ETH 40family formal outcomes against the old quick-screen batch and found the 40_v2 and 40_v3 definitions are duplicated across all four coins.
  - Added a synchronized BTC ETH SOL XRP 40bridge quick-screen batch with one interpretable per-coin bridge change inside the same near-40-factor search band.
  - Completed and summarized the four new quick-screen suites; ETH improved most cleanly, BTC improved slightly, while SOL and XRP remain unsolved.

- `027`
  - Re-read the program and latest cycle-026 artifacts, confirmed the ETH 40bridge formal label did not exist yet, and verified the active 40bridge line had no duplicate-sensitive formal run.
  - Ran the single ETH 1 USD / max3 real-orderbook validation for baseline_focus_feature_search_eth_reversal_40bridge_20260407 under the canonical formal runner and completed the 2-case run without failures.
  - Summarized the completed ETH 40bridge formal run and found that the control 40_v2 stayed slightly positive at 12 trades, while the strike-flip bridge variant lost money despite 20 trades.
  - Rejected the ETH 40bridge promotion on real evidence; BTC 40bridge remains the only plausible secondary formal follow-up from this batch.

- `029`
  - BTC 40bridge formal is now complete: the strike-flip bridge produced 1 trade and +269.87% ROI, while BTC 40_v2 stayed at 0 trades.

- `030`
  - Read program.md plus the latest session artifacts, including the completed 028 ETH bridge recovery and the partial 029 BTC launch artifacts, before taking action.
  - Recovered the existing BTC 40bridge formal run under the same label and completed both formal cases with the canonical runner, without opening any distinct new BTC candidate.
  - Reconciled session bookkeeping after cycle 029 had already recorded the same BTC bridge decision; cycle 030 preserves the recovery path and confirms the same 1-trade positive bridge outcome over the zero-trade BTC 40_v2 control.

- `032`
  - Confirmed the latest SOL follow-up failed and used the next single-cycle slot on XRP's unresolved near-40 replacement-family search.
  - Ran the previously unused XRP 38band quick-screen against the existing 40_v4 control and summarized it with the standard experiment summary tool.
  - XRP 38_v4 only tied the old 40_v4 control on backed winners while still producing zero trades, so XRP remains blocked from formal promotion.
## What's been tried

- Round-1 reversal focus-width quick-screen selected BTC 30/34, ETH 30/34, SOL 34/30, XRP 34/30 as current best starting sets.
- Round-2 reversal weight quick-screen completed for all 4 coins with current_default, no_vol_weight, offset_reversal_mild, and offset_reversal_strong.

- Round-2 reversal weight search is now summarized: BTC and ETH look usable, XRP remains weak, and SOL still fails the no-zero-trade rule.
- SOL one-factor removal follow-up around focus_sol_34_v4 showed drop_dow_sin is neutral, while removing strike_abs_z, move_z, or taker_buy_ratio_lag1 makes the leaderboard worse.

- Weight-search winners were frozen per coin before follow-up: BTC current_default, ETH offset_reversal_mild, SOL no_vol_weight, XRP offset_reversal_strong.

- Checked the synchronized four-coin reversal feature-drop follow-up batch and confirmed all four runs are still in progress.

- Summarized the four-coin reversal feature-drop batch: BTC base30 held, ETH base30 held, SOL drop_dow_sin still had 0 trades, and XRP drop_atr_14 only modestly improved coverage.

- Launched a synchronized four-coin obv_z swap batch using the weakest recent removable factor per coin as the replacement target.

- Recovered the stalled four-coin obv_z swap batch and summarized it with the standard experiment summary tool.
- Added quick-screen summary support to scripts/research/summarize_experiment.py via src/pm15min/research/automation/control_plane.py.

- Launched a synchronized four-coin strike_flip_count_cycle swap batch as the next unused 38/40-band replacement family.
- Completed the synchronized four-coin strike_flip_count_cycle swap batch: ETH materially improved, XRP only nudged ahead on tie-breaks, BTC still preferred base30, and SOL stayed at 0 trades.

- Completed the synchronized four-coin vwap_gap_20 swap follow-up batch: BTC base30 held, ETH strike_flip_count_cycle held, SOL obv_z stayed best at 0 trades, and XRP vwap_gap_20 became the new tie-break leader.

- Completed the synchronized four-coin rv_30_lag1 swap follow-up batch: BTC and ETH held, SOL stayed at 0 trades, and XRP rv_30_lag1 moved ahead only on tie-breaks.
- Summarized the finished BTC strike_flip formal promotion run: base30 posted -2.92% ROI and -0.8092 pnl at 1 USD / max3.

- Rechecked the synchronized BTC ETH SOL XRP strike_flip formal promotion batch and recorded a monitor-only cycle because all four runs remain active or partial.

- Rechecked the synchronized BTC ETH SOL XRP strike_flip formal promotion batch with status_autorun.sh plus direct process, log, and artifact inspection.

- Rechecked the synchronized BTC ETH SOL XRP strike_flip formal promotion batch after the ETH recovery launch and recorded another monitor-only cycle because all suites remain incomplete.

- Summarized the newly finished XRP strike_flip formal partial result: drop_atr_14 posted -100.0% ROI and -2.0 pnl at 1 USD / max3 while the suite continued into the next variant.

- Summarized the completed four-coin strike_flip formal promotion batch: BTC base30 stayed least-bad at -2.92% ROI, ETH strike_flip won ETH on real evidence but still lost money, SOL stayed at 0 trades, and XRP stayed at -100.0% ROI.

- Started XRP rv_30_lag1 1 USD / max3 formal validation against strike_flip_count_cycle and vwap_gap_20 controls; the first two completed cases both landed at -100.0% ROI with 2 trades.

- XRP rv_30_lag1 formal is now fully summarized and failed across all three variants on real evidence.
- BTC rv_30_lag1 formal has been launched under auto_btc_reversal_rv30lag1_formal1usd_r1_20260407 and should be resumed rather than duplicated.

- Retried the BTC rv_30_lag1 formal run under the same label; after recovery the run still records only the base30 control at -2.92% ROI with the rv_30_lag1 case incomplete.

- Completed the BTC rv_30_lag1 real 1 USD / max3 formal validation against the base30 and strike_flip controls; neither challenger beat the base30 control.
- Started ETH rv_30_lag1 1 USD / max3 formal validation against strike_flip_count_cycle and vwap_gap_20 controls; the first two completed cases currently favor strike_flip_count_cycle at -5.91% ROI versus -26.76% for rv_30_lag1.

- Retried the ETH rv_30_lag1 formal run under the same label; after recovery the run still records only strike_flip_count_cycle and rv_30_lag1 while the vwap_gap_20 control remains stuck before any backtest artifact appears.

- Retried the ETH rv_30_lag1 formal run under the same label and advanced the stuck vwap_gap_20 control through new training and bundle artifacts, but the formal backtest output is still missing.

- Completed the ETH rv_30_lag1 1 USD / max3 formal validation against strike_flip_count_cycle and vwap_gap_20 controls; strike_flip_count_cycle stayed best, vwap_gap_20 finished second, and rv_30_lag1 remained worst on real evidence.

- Re-read the 2026-04-07 operator override and treated the synchronized BTC ETH SOL XRP 40-factor quick-screen batch as the active queue head.
- Inspected the four 40family run directories and confirmed they still have no quick_screen_summary.json, summary.json, or matching autorun log.

- The synchronized 40-factor quick-screen batch is now complete: BTC 40_v4, ETH 40_v2, SOL 40_v2, and XRP 40_v4 are the current per-coin quick-screen leaders.
- ETH is the first 40-factor candidate promoted to real 1 USD / max3 validation because it is the only batch member with non-zero traded winners in band.

- ETH 40family formal validation now has real positive evidence: focus_eth_40_v2 finished with 12 trades and about +3.92% ROI at 1 USD / max3.
- BTC 40family formal validation has been opened as the next comparison point against the ETH 40_v2 leader.

- ETH 40family formal is now fully summarized: v4 leads on ROI with only 2 trades, while v2 and v3 remain near-flat but much more active at 12 trades each.
- BTC 40family formal is now the only incomplete 1 USD formal run and should be resumed rather than duplicated.

- BTC 40family formal is now fully summarized: `focus_btc_40_v2` and `focus_btc_40_v3` both ended at `0` trades, while `focus_btc_40_v4` was the only trading variant and finished at `-100.0%` ROI across `5` trades.
- ETH remains the only coin in the current 40family line with positive real 1 USD evidence; BTC still needs another idea before any further formal promotion.

- Completed the BTC 40family 1 USD / max3 formal validation: focus_btc_40_v2 and focus_btc_40_v3 stayed at 0 trades, while focus_btc_40_v4 lost all 5 trades for -100.0% ROI.

- ETH 40family vs legacy 30_v4 real 1 USD / max3 comparison is now complete; 40_v4 wins on ROI and 40_v2 stays the activity-oriented alternative.
- BTC 40family remains complete and negative, so BTC needs a fresh 40family follow-up rather than a repeat of the completed label.

- Ran a new synchronized 40bridge quick-screen batch across BTC ETH SOL and XRP using one per-coin swap or drop from the 40_v2 controls.
- ETH swap_atr_14_for_strike_flip_count_cycle improved trade rows from 11 to 18 without reducing traded winners.
- BTC swap_macd_z_for_strike_flip_count_cycle lifted the 40-line from 0 to 1 trade row, while SOL and XRP still failed the non-zero-trade gate.

- ETH 40bridge formal: control 40_v2 finished at +0.0392% ROI across 12 trades, while the strike-flip bridge variant finished at -21.4830% ROI across 20 trades.

- ETH 40bridge formal validation is now complete and rejects swap_atr_14_for_strike_flip_count_cycle on real evidence.

- Ran BTC 40bridge 1 USD / max3 formal validation against the BTC 40_v2 control; the bridge cleared the no-zero-trade gate and won the direct comparison on 1 trade.

- BTC 40bridge formal is now complete: swap_macd_z_for_strike_flip_count_cycle beats the BTC 40_v2 control on real evidence, but only on a single trade.

- Completed the unused XRP 38band quick screen: 38_v4 tied the old 40_v4 control on 1 backed winner in band but still had 0 trades, while 38_v3 was weaker.
## Open issues
- Need continuous Codex background loop to monitor active quick-screen runs, summarize completed runs, and launch the next one-factor follow-up automatically.

- SOL reversal still has 0 trades; the next cycle should test another unused 38/40-band replacement family against the obv_z swap control before any promotion run.
- XRP strike_flip_count_cycle only wins on tie-breaks and still has 0 traded winners; the next cycle needs a replacement family that improves actual traded-winner counts.

- The active BTC ETH SOL XRP strike_flip_count_cycle formal promotion runs are still incomplete and should be summarized before any duplicate promotion launch.
- SOL reversal still has 0 trades, so the next quick-screen follow-up still needs another unused replacement family around the obv_z control.
- XRP vwap_gap_20 improves backed-winner coverage but still has 0 traded winners, so the next follow-up should aim for actual traded-winner gains.

- ETH SOL XRP strike_flip formal promotion runs are still incomplete and should be summarized before any duplicate promotion launch.
- SOL reversal still has 0 trades after rv_30_lag1, so the next quick-screen follow-up still needs another unused replacement family around the obv_z control.
- XRP rv_30_lag1 now leads only on tie-breaks and still has 0 traded winners, so the next follow-up should target actual traded-winner gains.
- BTC base30 now has negative real 1 USD evidence, so if the remaining formal promotions also fail, BTC needs another replacement-family follow-up before any new promotion.

- The active BTC ETH SOL XRP strike_flip formal promotion batch still needs completion and summary before any duplicate or new promotion launch.
- If ETH or SOL still show no finished artifacts in the next cycle despite live workers, treat that batch as stalled and recover it instead of launching a duplicate.

- The active BTC ETH SOL XRP strike_flip formal promotion batch still needs a full completion so the next cycle can summarize the real 1 USD evidence.
- If ETH or SOL later stop accruing CPU time while still lacking finished artifacts, recover that run with the same suite and run label instead of starting a duplicate.

- The active BTC ETH SOL XRP strike_flip formal promotion batch still needs full completion and summary before any new promotion launch.
- The current environment already exceeds the 2-coin formal-run cap from program.md, so no additional formal runs should start until the active count drops.
- ETH currently has a duplicate same-label recovery launch; once the batch settles, confirm only one ETH worker remains before any future recovery.

- The active strike_flip formal batch still exceeds the preferred 2-coin cap, so no new formal launch should start until the current workload clears.
- SOL still has no completed formal artifact in the active strike_flip batch and should be rechecked for genuine liveness before any recovery attempt.

- No coin has an accepted positive real winner yet from the current reversal follow-up line; the next cycle should validate the completed rv_30_lag1 family before widening further.
- SOL reversal still has 0 real trades even after the completed strike_flip formal batch, so a new synchronized replacement-family search is still likely if rv_30_lag1 also fails.

- The XRP rv_30_lag1 formal run auto_xrp_reversal_rv30lag1_formal1usd_r1_20260407 still needs the vwap_gap_20 control case to finish; the next cycle should inspect or resume the same run label instead of launching a duplicate.
- SOL still has 0 real trades in this reversal line, so if XRP rv_30_lag1 also fails, the next replacement-family batch should keep extra focus on solving SOL zero-trade.

- BTC rv_30_lag1 formal currently has only seed-case-started evidence and still lacks summary.json, so the next cycle should inspect or recover the same run label.
- ETH and SOL still do not have rv_30_lag1 formal evidence; choose the next slot only after BTC settles.

- BTC rv_30_lag1 formal still stalls after seed-case-started for the rv_30_lag1 variant; the next cycle should inspect or recover that same label again before opening ETH.
- ETH and SOL still do not have rv_30_lag1 formal evidence; choose the next formal slot only after BTC either finishes or is explicitly abandoned.

- ETH rv_30lag1 formal is now running under auto_eth_reversal_rv30lag1_formal1usd_r1_20260407, with two completed cases already summarized; the next cycle should inspect or resume the same label until the remaining vwap_gap_20 control finishes.
- SOL reversal still has the unresolved zero-trade problem and should not be treated as solved just because BTC formal validation completed.

- ETH rv_30_lag1 formal still stalls on the vwap_gap_20 control even after same-label recovery, so the next cycle should inspect or recover that same label again before launching SOL.

- ETH rv_30_lag1 formal now has fresh vwap_gap_20 training and bundle artifacts but still lacks the matching formal backtest directory, so the next cycle should inspect or recover the same label again before launching SOL.

- ETH rv_30_lag1 formal is now complete, so the next cycle should launch the queued SOL rv_30_lag1 1 USD / max3 formal validation under the next open slot.

- The synchronized BTC ETH SOL XRP 40-factor quick-screen batch still needs finished outputs before any new formal promotion or SOL-specific formal launch should begin.

- ETH 40family formal run auto_focus_feature_search_eth_reversal_40family_formal1usd_r1_20260407 is active and should be summarized after it finishes.
- BTC is the next 40family promotion candidate if a second formal slot is needed; SOL and XRP still fail the no-zero-trade rule.

- BTC 40family formal run is still active and must be summarized after completion or resumed if it stalls.
- SOL and XRP 40family candidates still remain blocked by zero-trade quick-screen outcomes.

- BTC 40family formal is now complete and should not be rerun under the same label; the next cycle should use its finished failure profile when choosing any BTC follow-up.
- ETH 40family still has a stability question: v4 leads on ROI but only 2 trades, while v2 and v3 are far more active with only marginal positive edge.
- SOL and XRP still fail the no-zero-trade gate on their 40family quick screens, so neither coin should be promoted to a new formal run until a fresh follow-up materially improves tradeability.

- BTC 40family formal is now complete and negative, so the next cycle should compare BTC against the completed ETH result before any further 40family formal promotion.
- ETH remains the only positive 40family formal coin, but its best-ROI winner still rests on just 2 trades, so any widening should stay cautious.

- ETH still has an unresolved promotion choice between 40_v4 high ROI on 2 trades and 40_v2 near-flat ROI on 12 trades.
- SOL and XRP still fail the no-zero-trade gate and should not be promoted until a new quick-screen follow-up materially improves tradeability.

- ETH still needs real 1 USD / max3 formal validation for the new 40bridge candidate before the batch can be accepted.
- SOL and XRP remain at zero trades in the current 40-line search band and need further interpretable follow-up changes.

- Decide whether the next formal slot should test BTC 40bridge or whether the search should return to a new synchronized 40-band quick-screen batch; SOL and XRP remain below the no-zero-trade gate.

- BTC 40bridge is now the only remaining secondary bridge candidate worth a formal slot if the batch gets one more real-evidence check.
- SOL and XRP still fail the non-zero-trade gate and should not be promoted from the current bridge batch.

- Need the next synchronized near-40 follow-up batch for the still-unsolved coins; ETH bridge is rejected, BTC bridge is only thinly validated, and SOL/XRP remain unresolved.

- BTC now has tentative positive 40bridge evidence, but it is only a 1-trade result and should not yet be treated as a stable accepted winner.
- SOL and XRP still fail the non-zero-trade gate in the current 40-band line and should stay out of formal promotion until a new synchronized follow-up improves tradeability.
- SOL 40obvbridge quick-screen is now complete: the old `focus_sol_34_v6_swap_dow_sin_for_obv_z` control still leads with `4` backed winners in band, while both new near-40 bridge variants remain at `0` trades and fall back to only `1` backed winner in band.
- Because the new SOL near-40 bridges failed to improve tradeability, the next cycle should not open SOL formal validation; it should spend the single-cycle slot on XRP's unresolved near-40 replacement-family search unless the user explicitly asks for another SOL retry.

- XRP still stays at zero trades across the current near-40 40family, 40bridge, and 38band lines, so it should not be promoted until a more substantive replacement idea exists.
