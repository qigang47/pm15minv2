# Model Bundle Import State

这份文档只描述目前已经落到 `v2/research/model_bundles` 的真实 bundle，不讨论旧仓库里其它候选 run。

## 1. 当前 canonical direction bundle

### 1.1 Current live

- `SOL`
  - `usage=live_current`
  - source run:
    - `data/markets/sol/artifacts_runs/train_v6_sol_stage2_pool_drop_volume_z_3_end0309_dist_20260317_161410`
  - canonical bundle:
    - `v2/research/model_bundles/cycle=15m/asset=sol/profile=deep_otm/target=direction/bundle=legacy-train_v6_sol_stage2_pool_drop_volume_z_3_end0309_dist_20260317_161410`
  - active registry:
    - `v2/research/active_bundles/cycle=15m/asset=sol/profile=deep_otm/target=direction/selection.json`

- `XRP`
  - `usage=live_current`
  - source run:
    - `data/markets/xrp/artifacts_runs/alpha_search_stage6_qcentered_capacity_cross_repair_end0309_dist/train_v6_xrp_stage6_q_capacity_cross_drop_vol_price_corr_15_vwap_gap_20_end0309_dist_20260318_162707`
  - canonical bundle:
    - `v2/research/model_bundles/cycle=15m/asset=xrp/profile=deep_otm/target=direction/bundle=legacy-train_v6_xrp_stage6_q_capacity_cross_drop_vol_price_corr_15_vwap_gap_20_end0309_dist_20260318_162707`
  - active registry:
    - `v2/research/active_bundles/cycle=15m/asset=xrp/profile=deep_otm/target=direction/selection.json`

### 1.2 Baseline reference

- `SOL`
  - `usage=baseline_reference`
  - source run:
    - `data/markets/sol/artifacts_runs/by_group/B_bs_replace_direction/bs_replace/bs_q_replace/base/train_v6_bs_q_replace_sol_20260313_163701`
  - canonical bundle:
    - `v2/research/model_bundles/cycle=15m/asset=sol/profile=deep_otm_baseline/target=direction/bundle=legacy-train_v6_bs_q_replace_sol_20260313_163701`
  - active registry:
    - `v2/research/active_bundles/cycle=15m/asset=sol/profile=deep_otm_baseline/target=direction/selection.json`

- `XRP`
  - `usage=baseline_reference`
  - source run:
    - `data/markets/xrp/artifacts_runs/by_group/B_bs_replace_direction/bs_replace/bs_q_replace/base/train_v6_bs_q_replace_xrp_20260313_163602`
  - canonical bundle:
    - `v2/research/model_bundles/cycle=15m/asset=xrp/profile=deep_otm_baseline/target=direction/bundle=legacy-train_v6_bs_q_replace_xrp_20260313_163602`
  - active registry:
    - `v2/research/active_bundles/cycle=15m/asset=xrp/profile=deep_otm_baseline/target=direction/selection.json`

## 2. Offset layout

每个 bundle 都只保留 `offset=7/8/9`，并统一整理成：

```text
.../bundle=<bundle_label>/
  manifest.json
  offsets/
    offset=7/
      bundle_config.json
      feature_cols.joblib
      feature_schema.json
      models/
        lgbm_sigmoid.joblib
        logreg_sigmoid.joblib
        catboost.joblib          # if present in legacy source
        xgb.joblib               # if present in legacy source
      calibration/
        blend_weights.json
        cat_temperature.json     # if present in legacy source
        reliability_bins*.json
      diagnostics/
        metrics.json             # if present in legacy source
        final_model_probe.json   # if present in legacy source
```

## 3. Reversal policy

- `reversal` 不再作为当前 canonical live bundle 保留在 `v2/research/model_bundles`
- 之前误导入的 legacy reversal bundle 已移到：
  - `v2/var/quarantine/model_bundles_reversal_legacy/`

## 4. Why this split exists

- `profile=deep_otm`
  - 表示当前实盘真正使用的 direction bundle

- `profile=deep_otm_baseline`
  - 表示当前最重要的 baseline reference
  - 避免和 `profile=deep_otm` 混在一起，导致默认解析 bundle 时误选 baseline

## 5. Resolution rule

- 默认解析 bundle 时，先读：
  - `v2/research/active_bundles/.../selection.json`
- 只有在 active registry 缺失时，才回退到 `model_bundles` 目录下的默认搜索逻辑

## 6. Operations

查看当前 active bundle：

```bash
PYTHONPATH=v2/src python -m pm15min research show-active-bundle --market sol --profile deep_otm --target direction
```

切换 active bundle：

```bash
PYTHONPATH=v2/src python -m pm15min research activate-bundle --market sol --profile deep_otm --target direction --bundle-label legacy-train_v6_sol_stage2_pool_drop_volume_z_3_end0309_dist_20260317_161410
```

按当前 active bundle 做 live 打分：

```bash
PYTHONPATH=v2/src python -m pm15min live score-latest --market xrp --profile deep_otm
```

原则：

- `model_bundles/` 可以很多
- `active_bundles/selection.json` 每个 `asset + profile + target` 只保留一个当前入口
