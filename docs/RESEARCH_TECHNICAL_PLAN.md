# Research Domain Specification

这份文档只保留当前仓库里长期有效的 `research` 域规范。

目标：

- 说明 `research/` 下各类对象的职责和路径。
- 说明训练、bundle、backtest、experiment、evaluation 的主链。
- 说明 `research` 与 `data`、`live` 的边界。

如果代码和文档冲突，以 `src/pm15min/research/`、`src/pm15min/research/layout.py` 为准。

## 1. 边界

`research` 负责离线建模与评估：

- feature frame
- label frame
- training set
- training run
- model bundle
- backtest
- experiment
- evaluation

`research` 不负责：

- 原始数据采集
- live runtime state
- 真实交易 side effects

这些分别属于：

- `data`
- `live`

## 2. 目录布局

`ResearchLayout` 当前长期维护的根目录为：

```text
research/
  feature_frames/
  label_frames/
  training_sets/
  training_runs/
  model_bundles/
  active_bundles/
  backtests/
  experiments/
    suite_specs/
    runs/
  evaluations/

var/research/
  cache/
  locks/
  logs/
  tmp/
```

其中：

- `research/` 存长期产物。
- `var/research/` 存运行态 cache、锁、日志、临时文件。

## 3. 当前 canonical 对象

### 3.1 数据集对象

```text
research/feature_frames/cycle=<cycle>/asset=<asset>/feature_set=<feature_set>/source_surface=<surface>/
research/label_frames/cycle=<cycle>/asset=<asset>/label_set=<label_set>/
research/training_sets/cycle=<cycle>/asset=<asset>/feature_set=<feature_set>/label_set=<label_set>/target=<target>/window=<window>/offset=<offset>/
```

每个对象通常包含：

- `data.parquet`
- `manifest.json`

### 3.2 训练和 bundle

```text
research/training_runs/cycle=<cycle>/asset=<asset>/model_family=<family>/target=<target>/run=<run_label>/
research/model_bundles/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/bundle=<bundle_label>/
research/active_bundles/cycle=<cycle>/asset=<asset>/profile=<profile>/target=<target>/selection.json
```

规则：

- `model_bundles/` 可以存在多个候选 bundle。
- `active_bundles/.../selection.json` 是当前唯一 active 入口。
- live 默认只认 active selection，不直接凭目录猜默认 bundle。

### 3.3 回测、实验、评估

```text
research/backtests/cycle=<cycle>/asset=<asset>/profile=<profile>/spec=<spec>/run=<run_label>/
research/experiments/suite_specs/<suite>.json
research/experiments/runs/suite=<suite>/run=<run_label>/
research/evaluations/<category>/asset=<asset>/scope=<scope>/run=<run_label>/
```

## 4. 依赖关系

长期依赖顺序固定为：

1. `data` 提供 canonical tables 与必要 source loaders
2. `research.datasets` / `labels` 构建训练输入
3. `research.training` 产出 training run
4. `research.bundles` 产出 deployable model bundle
5. `research.backtests` 用 bundle 做 replay
6. `research.experiments` 组织 suite 和 compare
7. `research.evaluation` 生成评估与报告

不允许跳层：

- 不要让 backtest 直接读取临时脚本输出。
- 不要让 live 直接读 training run 原始目录来代替 bundle。
- 不要让 experiment 依赖 exports 或手工 CSV。

## 5. CLI 面

当前公开的 `research` 顶层命令为：

```text
show-config
show-layout
list-runs
list-bundles
show-active-bundle
activate-bundle
build
train
bundle
backtest
experiment
evaluate
```

常用命令：

```bash
PYTHONPATH=src python -m pm15min research show-layout --market sol --cycle 15m
PYTHONPATH=src python -m pm15min research list-runs --market sol --target direction
PYTHONPATH=src python -m pm15min research show-active-bundle --market sol --profile deep_otm --target direction
```

更细的子命令以 `PYTHONPATH=src python -m pm15min research --help` 为准。

## 6. 与 legacy 的关系

当前仓库仍然会参考 legacy 代码和旧训练产物，但 `research` 域本身应遵守这几条：

- 正式代码只放在 `src/pm15min/research/`。
- 长期产物只放在 `research/` 和 `var/research/`。
- legacy 数据或脚本只能作为 importer、参考实现或对照样本。
- 新能力先收进 package，再决定是否保留 thin script。

## 7. 当前实现重点

从目录结构看，当前已形成稳定模块边界：

- `research/features/`
- `research/labels/`
- `research/datasets/`
- `research/training/`
- `research/bundles/`
- `research/backtests/`
- `research/experiments/`
- `research/evaluation/`
- `research/inference/`

文档层面应假设这些包已经是正式主线，而不是“待设计愿景”。

## 8. 维护规则

- 文档只记录长期 contract，不记录某一轮的临时计划板。
- 测试数量、单日通过数、一次性 parity 审计，不放进长期规范文档。
- 需要 operator 使用的内容放 runbook；需要开发者理解边界的内容放本文件；一次性验证放 `var/` 或 PR。
