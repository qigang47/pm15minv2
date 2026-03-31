# Approx Replay

- profile: `deep_otm_baseline`
- replay_scope: settled traded windows from `2026-03-15`
- pricing_rule:
  - bought side uses realized `avg_entry`
  - opposite side uses approximate binary complement price `1 - avg_entry`
- note:
  - this is a fast approximation, not a full historical orderbook replay

## Summary

- settled_windows: `114`
- old_accepts: `18`
- new_accepts: `18`
- changed_windows: `0`
- old_win_rate: `33.33%`
- new_win_rate: `33.33%`
- old_unit_roi_sum: `7.1040`
- new_unit_roi_sum: `7.1040`
- up_accept_rows: `4`
- down_accept_rows: `14`

## By Feature Source

| feature_source | settled | old_accepts | new_accepts |
|:--|--:|--:|--:|
| external_ohlcv_fallback | 25 | 3 | 3 |
| local_feature_frame | 89 | 15 | 15 |

## Reading

- On your real traded sample, dual-candidate selection did not change a single window.
- The reason is not that the comparison failed to run; it is that the alternative side still did not clear the baseline probability gate.
- In the `66` windows where model argmax and your bought side were opposite, the cheap side was rejected by exactly one dominant reason: `prob`.
- So on traded data too, the current blocker is still the hard `min_dir_prob` gate, not the lack of side comparison.
