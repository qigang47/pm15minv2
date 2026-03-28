# Model Bundle State

这份文档只记录当前仓库里长期有用的 bundle 入口规则，以及截至 2026-03-29 观测到的 active selection。

规则优先级：

1. `research/active_bundles/.../selection.json`
2. `research/model_bundles/...`

也就是说：

- `model_bundles/` 可以有很多候选 bundle
- `active_bundles/selection.json` 才是 live 和 operator 默认要看的当前入口

## 1. 当前 active selections（2026-03-29）

### 1.1 live 当前 profile

- `sol`
  - `profile=deep_otm`
  - `target=direction`
  - active bundle:
    - `legacy-train_v6_sol_stage2_pool_drop_volume_z_3_end0309_dist_20260317_161410`
  - selection:
    - `research/active_bundles/cycle=15m/asset=sol/profile=deep_otm/target=direction/selection.json`

- `xrp`
  - `profile=deep_otm`
  - `target=direction`
  - active bundle:
    - `legacy-train_v6_xrp_stage6_q_capacity_cross_drop_vol_price_corr_15_vwap_gap_20_end0309_dist_20260318_162707`
  - selection:
    - `research/active_bundles/cycle=15m/asset=xrp/profile=deep_otm/target=direction/selection.json`

### 1.2 baseline 参考 profile

- `btc`
  - `profile=deep_otm_baseline`
  - `target=direction`
  - active bundle:
    - `unified_truth0328_btc_baseline_20260328`

- `eth`
  - `profile=deep_otm_baseline`
  - `target=direction`
  - active bundle:
    - `unified_truth0328_eth_baseline_20260328`

- `sol`
  - `profile=deep_otm_baseline`
  - `target=direction`
  - active bundle:
    - `unified_truth0328_sol_baseline_20260328`

- `xrp`
  - `profile=deep_otm_baseline`
  - `target=direction`
  - active bundle:
    - `unified_truth0328_xrp_baseline_20260328`

对应 selection 文件都在：

```text
research/active_bundles/cycle=15m/asset=<asset>/profile=<profile>/target=direction/selection.json
```

## 2. bundle 目录结构

当前 bundle 目录长期约定为：

```text
research/model_bundles/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/bundle=<bundle_label>/
  manifest.json
  offsets/
    offset=<n>/
      bundle_config.json
      feature_cols.joblib
      feature_schema.json
      models/
      calibration/
      diagnostics/
```

## 3. 操作规则

- 默认读取 bundle 时，先读 `selection.json`。
- 只有在显式指定 bundle label 或做 inventory 时，才直接看 `model_bundles/`。
- 不要把 training run 目录直接当 live bundle。

## 4. 常用命令

查看当前 active bundle：

```bash
PYTHONPATH=src python -m pm15min research show-active-bundle --market sol --profile deep_otm --target direction
```

切换 active bundle：

```bash
PYTHONPATH=src python -m pm15min research activate-bundle --market sol --profile deep_otm --target direction --bundle-label <bundle_label>
```

按当前 active bundle 做 live 打分：

```bash
PYTHONPATH=src python -m pm15min live score-latest --market sol --profile deep_otm
```

## 5. 维护规则

- 这个文档只保留当前 active 入口和目录规则。
- 更细的训练来源、历史迁移过程、一次性导入记录不再留在这里。
- 当 `selection.json` 发生切换时，同步更新本文件即可。
