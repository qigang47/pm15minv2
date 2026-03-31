# Live Dual-Candidate Replay

- profile: `deep_otm`
- replay_date: `2026-03-31`
- source_scope: local persisted live snapshots under `var/live/state/signals` + matching `quotes` + historical `decisions`
- assets covered: `sol`, `xrp`

## Summary

- snapshots replayed: `12`
- old accepts: `4`
- new accepts: `4`
- snapshots with changed final decision: `0`
- total offset rows inspected: `36`
- offsets where both `UP` and `DOWN` cleared current probability threshold: `0`
- offsets where gross quote ROI favored the opposite side vs model argmax: `12`

## Reading

- The new dual-candidate code is active, but under the current `deep_otm` probability gate it did not change any final live decision in this snapshot sample.
- The reason is structural: `0/36` offsets had both sides above the current probability threshold, so the gate itself still acts as a one-side selector.
- Even so, `12/36` offsets had quote-implied gross ROI that favored the opposite side from the model argmax. That means pricing sometimes prefers the other side, but the probability gate prevents that side from becoming executable.
- So the current state is: dual-candidate scoring is implemented, but with the existing `min_dir_prob` regime it mostly has no room to fire.

## By Asset

| asset   |   snapshots |   old_accept |   new_accept |   changed |   total_offsets |   both_above_threshold_offsets |   roi_flip_offsets |
|:--------|------------:|-------------:|-------------:|----------:|----------------:|-------------------------------:|-------------------:|
| sol     |           9 |            2 |            2 |         0 |              27 |                              0 |                  9 |
| xrp     |           3 |            2 |            2 |         0 |               9 |                              0 |                  3 |

## Example Offsets Where ROI Preferred The Opposite Side

| asset   | snapshot_ts          |   offset | model_side   | roi_side   |   p_up |   p_down |   quote_up_ask |   quote_down_ask |   roi_up_gross |   roi_down_gross |   threshold |
|:--------|:---------------------|---------:|:-------------|:-----------|-------:|---------:|---------------:|-----------------:|---------------:|-----------------:|------------:|
| sol     | 2026-03-20T13-11-32Z |        7 | UP           | DOWN       | 0.7176 |   0.2824 |         0.9500 |           0.0700 |        -0.2446 |           3.0339 |      0.6200 |
| sol     | 2026-03-20T13-11-32Z |        8 | UP           | DOWN       | 0.7240 |   0.2760 |         0.9500 |           0.0700 |        -0.2379 |           2.9431 |      0.6000 |
| sol     | 2026-03-20T13-11-32Z |        9 | UP           | DOWN       | 0.7849 |   0.2151 |         0.9700 |           0.0500 |        -0.1908 |           3.3013 |      0.6000 |
| sol     | 2026-03-20T14-10-20Z |        8 | UP           | DOWN       | 0.5068 |   0.4932 |         0.8100 |           0.1800 |        -0.3743 |           1.7400 |      0.6000 |
| sol     | 2026-03-20T14-21-06Z |        7 | UP           | DOWN       | 0.5826 |   0.4174 |         0.6300 |           0.3800 |        -0.0752 |           0.0983 |      0.6200 |
| sol     | 2026-03-20T14-21-06Z |        8 | UP           | DOWN       | 0.5068 |   0.4932 |         0.6300 |           0.3800 |        -0.1956 |           0.2979 |      0.6000 |
| sol     | 2026-03-20T14-21-06Z |        9 | UP           | DOWN       | 0.5960 |   0.4040 |         0.6300 |           0.3800 |        -0.0540 |           0.0633 |      0.6000 |
| sol     | 2026-03-20T15-34-52Z |        8 | UP           | DOWN       | 0.6322 |   0.3678 |         0.7700 |           0.2700 |        -0.1790 |           0.3624 |      0.6000 |
| sol     | 2026-03-20T15-35-27Z |        8 | UP           | DOWN       | 0.6322 |   0.3678 |         0.6800 |           0.3400 |        -0.0704 |           0.0819 |      0.6000 |
| xrp     | 2026-03-20T14-10-21Z |        8 | UP           | DOWN       | 0.5335 |   0.4665 |         0.9400 |           0.0700 |        -0.4324 |           5.6637 |      0.6000 |
| xrp     | 2026-03-20T15-24-51Z |        7 | UP           | DOWN       | 0.5302 |   0.4698 |         0.8800 |           0.1500 |        -0.3975 |           2.1321 |      0.6000 |
| xrp     | 2026-03-20T15-24-51Z |        8 | UP           | DOWN       | 0.7907 |   0.2093 |         0.8800 |           0.1500 |        -0.1015 |           0.3952 |      0.6000 |

## Files

- snapshot comparison: `research/live_dual_candidate_replay/run=20260331_profile=deep_otm/snapshot_comparison.csv`
- roi flips: `research/live_dual_candidate_replay/run=20260331_profile=deep_otm/roi_flip_offsets.csv`
