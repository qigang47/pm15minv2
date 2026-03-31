# Address Trade Window Analysis

- user_address: `0x4520B7...37f8`
- profile: `deep_otm_baseline`
- start_ts: `2026-03-15T00:00:00+00:00`
- settlement_cutoff_day: `2026-03-30`
- trade_windows_csv: `/Users/gangqi/Downloads/poly/v2/research/address_trade_windows/run=20260330_start=20260315_profile=deep_otm_baseline/trade_windows.csv`
- factor_weights_csv: `/Users/gangqi/Downloads/poly/v2/research/address_trade_windows/run=20260330_start=20260315_profile=deep_otm_baseline/trade_factor_weights_long.csv`

## Overview

|   trade_windows |   settled_windows |   pending_windows |   factor_rows | profile           | start_ts                  | settlement_cutoff_day   |
|----------------:|------------------:|------------------:|--------------:|:------------------|:--------------------------|:------------------------|
|             125 |               114 |                 9 |          8000 | deep_otm_baseline | 2026-03-15T00:00:00+00:00 | 2026-03-30              |

## Status Counts

| status                 |   count |
|:-----------------------|--------:|
| no_redeem_loss         |      77 |
| redeemed_closed        |      25 |
| redeemed_by_condition  |      12 |
| pending_today_or_later |       9 |
| missing_state          |       2 |

## Bundle Usage

| bundle_label                                                       |   trade_windows |
|:-------------------------------------------------------------------|----------------:|
| truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |              51 |
| truth0315_sol_grid_20260326-sol-direction-bundle-off7-8-9-f3fc5ba8 |              36 |
| unified_truth0328_eth_baseline_20260328                            |              19 |
| unified_truth0328_btc_baseline_20260328                            |              11 |
| unified_truth0328_xrp_baseline_20260328                            |               5 |
| unified_truth0328_sol_baseline_20260328                            |               3 |

## Feature Source Coverage

| feature_source          |   trade_windows |
|:------------------------|----------------:|
| local_feature_frame     |              90 |
| external_ohlcv_fallback |              35 |

## Feature Alignment Coverage

| asset   |   trade_windows |   aligned_windows |   missing_windows |   aligned_pct |
|:--------|----------------:|------------------:|------------------:|--------------:|
| btc     |              11 |                11 |                 0 |           100 |
| eth     |              19 |                19 |                 0 |           100 |
| sol     |              39 |                39 |                 0 |           100 |
| xrp     |              56 |                56 |                 0 |           100 |

## Largest Loss Windows

| asset   | outcome   |   cost_usdc | title                                            | bundle_label                                                       |   matched_offset |
|:--------|:----------|------------:|:-------------------------------------------------|:-------------------------------------------------------------------|-----------------:|
| sol     | Down      |       17.7  | Solana Up or Down - March 25, 7:15AM-7:30AM ET   | truth0315_sol_grid_20260326-sol-direction-bundle-off7-8-9-f3fc5ba8 |                8 |
| sol     | Up        |       17.29 | Solana Up or Down - March 25, 7:45AM-8:00AM ET   | truth0315_sol_grid_20260326-sol-direction-bundle-off7-8-9-f3fc5ba8 |                7 |
| xrp     | Up        |       16.5  | XRP Up or Down - March 22, 8:45PM-9:00PM ET      | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                7 |
| xrp     | Down      |       12.4  | XRP Up or Down - March 21, 7:30PM-7:45PM ET      | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                7 |
| xrp     | Up        |        9    | XRP Up or Down - March 22, 11:45AM-12:00PM ET    | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                7 |
| sol     | Up        |        9    | Solana Up or Down - March 22, 10:30PM-10:45PM ET | truth0315_sol_grid_20260326-sol-direction-bundle-off7-8-9-f3fc5ba8 |                7 |
| sol     | Down      |        9    | Solana Up or Down - March 20, 10:00PM-10:15PM ET | truth0315_sol_grid_20260326-sol-direction-bundle-off7-8-9-f3fc5ba8 |                7 |
| xrp     | Up        |        7.5  | XRP Up or Down - March 23, 7:30AM-7:45AM ET      | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                8 |
| xrp     | Down      |        7.5  | XRP Up or Down - March 20, 11:30AM-11:45AM ET    | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                8 |
| sol     | Up        |        7.5  | Solana Up or Down - March 22, 11:45PM-12:00AM ET | truth0315_sol_grid_20260326-sol-direction-bundle-off7-8-9-f3fc5ba8 |                8 |
| sol     | Up        |        7.5  | Solana Up or Down - March 23, 3:00AM-3:15AM ET   | truth0315_sol_grid_20260326-sol-direction-bundle-off7-8-9-f3fc5ba8 |                8 |
| xrp     | Down      |        7.5  | XRP Up or Down - March 21, 10:45PM-11:00PM ET    | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                8 |

## Largest Win Windows

| asset   | outcome   |   cost_usdc |   recovered_usdc |   pnl_usdc | title                                            | bundle_label                                                       |   matched_offset |
|:--------|:----------|------------:|-----------------:|-----------:|:-------------------------------------------------|:-------------------------------------------------------------------|-----------------:|
| xrp     | Down      |     6       |          35.2941 |    29.2941 | XRP Up or Down - March 20, 5:30PM-5:45PM ET      | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                7 |
| sol     | Down      |     6       |          34.3254 |    28.3255 | Solana Up or Down - March 20, 11:15AM-11:30AM ET | truth0315_sol_grid_20260326-sol-direction-bundle-off7-8-9-f3fc5ba8 |                8 |
| xrp     | Down      |     7.5     |          34.3753 |    26.8753 | XRP Up or Down - March 20, 6:30PM-6:45PM ET      | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                9 |
| xrp     | Down      |    10       |          36.433  |    26.4331 | XRP Up or Down - March 18, 10:15AM-10:30AM ET    | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                7 |
| sol     | Down      |     5.46    |          30.0969 |    24.637  | Solana Up or Down - March 24, 10:00AM-10:15AM ET | truth0315_sol_grid_20260326-sol-direction-bundle-off7-8-9-f3fc5ba8 |                7 |
| xrp     | Down      |     9       |          30      |    21      | XRP Up or Down - March 17, 7:45AM-8:00AM ET      | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                7 |
| xrp     | Down      |     6       |          25.7576 |    19.7576 | XRP Up or Down - March 19, 11:30AM-11:45AM ET    | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                9 |
| sol     | Up        |     7.5     |          26.7857 |    19.2857 | Solana Up or Down - March 22, 8:45AM-9:00AM ET   | truth0315_sol_grid_20260326-sol-direction-bundle-off7-8-9-f3fc5ba8 |                7 |
| xrp     | Down      |     6.3     |          24.2308 |    17.9308 | XRP Up or Down - March 21, 11:30AM-11:45AM ET    | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                7 |
| xrp     | Down      |     6.3     |          23.8746 |    17.5747 | XRP Up or Down - March 22, 9:30PM-9:45PM ET      | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                8 |
| xrp     | Down      |     5.99999 |          23.163  |    17.163  | XRP Up or Down - March 15, 2:00PM-2:15PM ET      | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                7 |
| xrp     | Down      |     5.99999 |          22.8867 |    16.8868 | XRP Up or Down - March 16, 4:15AM-4:30AM ET      | truth0315_xrp_grid_20260326-xrp-direction-bundle-off7-8-9-6539e205 |                7 |

## Pending Windows

| asset   | outcome   |   cost_usdc | title                                            |
|:--------|:----------|------------:|:-------------------------------------------------|
| xrp     | Up        |        1.5  | XRP Up or Down - March 30, 2:45AM-3:00AM ET      |
| sol     | Up        |        2    | Solana Up or Down - March 30, 3:15AM-3:30AM ET   |
| eth     | Up        |        2    | Ethereum Up or Down - March 30, 4:45AM-5:00AM ET |
| xrp     | Up        |        1.5  | XRP Up or Down - March 30, 5:15AM-5:30AM ET      |
| xrp     | Up        |        1.45 | XRP Up or Down - March 30, 5:45AM-6:00AM ET      |
| sol     | Up        |        2    | Solana Up or Down - March 30, 5:45AM-6:00AM ET   |
| xrp     | Up        |        2.17 | XRP Up or Down - March 30, 6:30AM-6:45AM ET      |
| sol     | Up        |        1.4  | Solana Up or Down - March 30, 8:15AM-8:30AM ET   |
| xrp     | Up        |        1.5  | XRP Up or Down - March 30, 8:45AM-9:00AM ET      |

## Notes

- `trade_windows.csv` is one row per traded window (`conditionId + outcomeIndex`).
- `trade_factor_weights_long.csv` is one row per traded window x feature.
- `matched_offset` uses the latest supported decision row (`offset 7/8/9`) not later than the first trade timestamp; if none exists, the earliest future row is used and flagged by `used_future_decision_row`.
- Bundle selection is date-aware: it prefers the latest compatible dated bundle not later than the trade date; when none exists locally, it falls back to the earliest compatible dated bundle after the trade date.
- `logreg_contribution` is computed on standardized features (`StandardScaler` output times logistic coefficient).
- `lgb_shap_value` comes from `pred_contrib=True` on the LightGBM model for the matched trade row.
- `feature_row_missing` means neither the local feature frame nor the supplemental Kraken-based rebuild could cover that traded window.
- `feature_source=external_ohlcv_fallback` means the row was rebuilt only for analysis using external 1m OHLCV candles plus Polymarket oracle prices; it does not overwrite canonical research artifacts.
- The external OHLCV fallback preserves price/volume structure, but Binance-specific taker-buy columns are unavailable there and are filled as `0.0`, so flow-style factors should be read more cautiously on fallback rows.
