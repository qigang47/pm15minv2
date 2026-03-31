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
- 当前 active bundle 的真实状态应从 `selection.json` 或 `research show-active-bundle` 读取，不再把某一天的 bundle label 额外抄进 `docs/`。

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
- 已经抽出的共享 contract，应优先放在 `core/` 或其他明确的 shared 层，而不是让 `research` 继续直接引用 `live` 内部常量。

当前已经从 `live` 下沉到 shared 层的例子：

- `core/retry_contracts.py`
  - 共享重试原因和 message hint 常量
- `core/orderbook_index.py`
  - 共享 orderbook index 读取、去重和按时间选行 helper

`research/workflows/` 用于显式编排跨对象 follow-up，而不是把这些副作用重新塞回 `data` 或临时脚本。

当前已进入正式 workflow 的例子：

- `research build backfill-followups`
  - data backfill 之后显式重建 `research` follow-up artifacts。

## 5. CLI 面与依赖模式

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
PYTHONPATH=src python -m pm15min research build backfill-followups --markets sol,xrp --cycle 15m --source-surface backtest
PYTHONPATH=src python -m pm15min research build training-set --market sol --window-start 2026-03-01 --window-end 2026-03-01 --offset 7 --dependency-mode fail_fast
PYTHONPATH=src python -m pm15min research train run --market sol --window-start 2026-03-01 --window-end 2026-03-01 --offsets 7,8 --dependency-mode fail_fast
PYTHONPATH=src python -m pm15min research backtest run --market sol --profile deep_otm --spec baseline_truth --bundle-label <bundle> --dependency-mode fail_fast
```

更细的子命令以 `PYTHONPATH=src python -m pm15min research --help` 为准。

其中几条长期规则值得单独记住：

- `show-active-bundle` / `activate-bundle`
  - 是 active bundle 的 canonical 读写入口。
- `build backfill-followups`
  - 是 data backfill 之后的显式 research follow-up workflow。
- `build feature-frame`
- `build label-frame`
- `build training-set`
- `train run`
- `backtest run`
  - 当前默认都是 `fail_fast`，因为这些入口都会消费或生成上游依赖敏感的 research artifacts，适合作为显式依赖检查入口。
- 这些入口都支持 `--dependency-mode {auto_repair,fail_fast}`。

`dependency-mode` 的长期语义：

- `auto_repair`
  - 允许 research 入口按当前 freshness 规则补齐或重建上游依赖。
- `fail_fast`
  - 只做检查；依赖缺失或过期时直接失败，不偷偷修系统。
  - CLI 会返回非零退出码并打印依赖未准备好的明确错误，而不是静默触发 rebuild。

当前默认值策略：

- `build feature-frame`
  - 默认 `fail_fast`
- `build label-frame`
  - 默认 `fail_fast`
- `build backfill-followups`
  - 默认 `fail_fast`
- `build training-set`
  - 默认 `fail_fast`
- `train run`
  - 默认 `fail_fast`
- `backtest run`
  - 默认 `fail_fast`
- 需要保留“边检查边自动补依赖”的场景
  - 显式传 `--dependency-mode auto_repair`

### 5.1 当前 rollout 顺序

当前这轮解耦按下面顺序推进：

1. 先切掉 `data -> research` 的隐式写入
   - data backfill 不再默认顺手重建 `research/label_frames`
   - follow-up rebuild 改由 `research build backfill-followups` 显式触发
2. 再把 `freshness` 拆成 inspect / prepare 两层
   - inspect 只报告 stale / missing
   - prepare 再决定 `fail_fast` 或 `auto_repair`
3. 再把 research CLI 默认值切到严格模式
   - `build feature-frame`
   - `build label-frame`
   - `build backfill-followups`
   - `build training-set`
   - `train run`
   - `backtest run`
   这些入口当前默认都已经是 `fail_fast`
4. 最后才继续拆更深层的跨域耦合
   - 例如 `research/backtests -> live` 的共享 contract 下沉

这样做的原因很简单：

- 先收口副作用边界
- 再收紧默认行为
- 最后才动更深的模块依赖

当前下一步优先级：

1. 继续把 `research/backtests -> live` 的共享 contract 抽到 shared 层
2. 保持 CLI 默认严格，只有显式 `auto_repair` 才允许入口修系统
3. 每推进一刀都先补测试，再动 import 边界或默认值

## 6. 与 legacy 的关系

当前仓库仍然会参考 legacy 代码和旧训练产物，但 `research` 域本身应遵守这几条：

- 正式代码只放在 `src/pm15min/research/`。
- 长期产物只放在 `research/` 和 `var/research/`。
- legacy 数据或脚本只能作为 importer、参考实现或对照样本。
- 新能力先收进 package，再决定是否保留 thin script。

与 `scripts/` 的长期边界也固定为：

- `research/evaluation/` 是 poly eval 与评估方法的正式归宿。
- `scripts/entrypoints/` 只保留 shell wrapper、环境装配和薄运行包装。
- `scripts/research/run_grouped_backtest_grid.py`
- importer / monitor 脚本
  - 可以保留为批量编排或运维工具，但不应重新变成第二套研究主线。

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
- `research/workflows/`

文档层面应假设这些包已经是正式主线，而不是“待设计愿景”。

## 8. 维护规则

- 文档只记录长期 contract，不记录某一轮的临时计划板。
- 测试数量、单日通过数、一次性 parity 审计，不放进长期规范文档。
- 需要 operator 使用的内容放 runbook；需要开发者理解边界的内容放本文件；一次性验证放 `var/` 或 PR。
