# pm15min v2

`v2/` 是当前唯一的 canonical 实现。

旧仓库里的 `live_trading/`、旧 `src/`、旧脚本、旧数据目录仍然有参考价值，但现在真正应该读、改、维护、加测试的代码，都在 `v2/`。

这份 README 的目标不是“简短介绍一下项目”，而是做一份能直接代替口头讲解的代码地图：你只看这一个文件，就应该知道：

- 这套系统整体在做什么
- `data / research / live` 三条线怎么衔接
- 每个目录为什么存在
- 主要文件各自负责哪一段链路
- 如果你要改某个行为，应该先去看哪里

默认不展开 `__pycache__`。但这份 README 不再默认忽略 `__init__.py`，因为在 v2 里很多 `__init__.py` 其实就是稳定门面，不只是包标记。

## 一句话总览

这套代码把 Polymarket 相关的多源数据整理成 canonical 数据面，在研究域里把它们变成 feature、label、training run、bundle、backtest、evaluation，然后在 live 域里消费 active bundle、最新市场状态和账户状态，产出 signal、quote、decision、execution，再根据交易网关执行下单、撤单、赎回和状态落盘；现在还额外提供了一个只读 `console` 层，用来把 canonical data/research 产物组织成更适合 UI/API 消费的 read-model。

## 四条主线怎么连起来

按运行时真实依赖看，主链路是：

1. `core/`
   提供全仓库共同语言：市场注册、仓库路径、跨域基础配置。
2. `data/`
   从 Binance、Chainlink、Polymarket、legacy 产物里拉原始数据，沉淀成 canonical source/table/export。
3. `research/`
   在 canonical 数据之上构建 feature frame、label frame、training set、training run、bundle、backtest、evaluation、experiment。
4. `live/`
   读取 active bundle、live data foundation、账户状态、风险状态，组装 signal、quote、decision、execution，并把副作用交给 trading adapter。
5. `console/`
   只读消费 `data` 和 `research` 的 canonical outputs，提供稳定的 read-model、CLI 和轻量 JSON HTTP 入口，给后续可视化控制台使用。

把它压成一句更实用的话就是：

- `data` 负责“把世界变成稳定数据”
- `research` 负责“把稳定数据变成可用模型和离线结论”
- `live` 负责“把模型和最新状态变成实时动作”
- `console` 负责“把现有 canonical 产物变成可展示、可查询的只读视图”

## 推荐阅读顺序

### 如果你第一次进这个仓库

按这个顺序读，心智负担最小：

1. `src/pm15min/cli.py`
2. `src/pm15min/core/assets.py`
3. `src/pm15min/core/layout.py`
4. `src/pm15min/data/layout/__init__.py`
5. `src/pm15min/research/layout.py`
6. `src/pm15min/live/runtime.py`
7. `src/pm15min/live/service/__init__.py`
8. `src/pm15min/live/runner/__init__.py`

读完这 8 个入口，你会知道：

- 仓库怎么分域
- 路径怎么统一
- 数据、研究、实盘怎么串起来
- 哪些文件是稳定门面，哪些文件是具体实现

### 如果你只关心某一条线

- 想看数据底座：`data/layout/__init__.py -> data/config.py -> data/cli/__init__.py -> data/pipelines/* -> data/service/__init__.py`
- 想看研究产物：`research/layout.py -> research/config.py -> research/service.py -> research/datasets/* -> research/training/* -> research/bundles/* -> research/backtests/*`
- 想看实盘主线：`live/runtime.py -> live/cli/__init__.py -> live/service/__init__.py -> live/signal/* -> live/quotes/* -> live/execution/* -> live/actions/* -> live/runner/* -> live/trading/*`

## 根目录地图

- `pyproject.toml`
  项目依赖、打包方式、测试发现规则。
- `src/`
  v2 全部源码，真正要维护的实现都在这里。
- `tests/`
  行为测试、架构护栏、CLI 接线检查、跨域 parity 检查。
- `docs/`
  设计文档、重构审查、operator runbook、路线图。
- `scripts/`
  少量迁移或导入脚本。
- `data/`
  v2 的 canonical 数据产物目录。
- `research/`
  v2 的研究产物目录，包括 feature/label/training/bundle/backtest/evaluation。
- `var/`
  运行态日志、状态、缓存、临时产物。

### 运行态重要目录

- `data/live/`
  live surface 的 canonical 数据。
- `data/backtest/`
  backtest surface 的 canonical 数据。
- `research/model_bundles/`
  可部署 bundle 的正式落盘目录。
- `research/active_bundles/`
  当前 active bundle 的单一指针。
- `research/backtests/`
  回测结果目录。
- `research/evaluations/`
  评估结果目录。
- `var/live/`
  live runner、quotes、decision、execution、account、regime、readiness 等状态快照。
- `var/research/`
  research 的缓存、锁、日志、临时结果。

## `src/pm15min/` 顶层文件

- `src/pm15min/__init__.py`
  顶层包最小出口。目前只保留版本号这类轻量元信息，不承载业务逻辑。
- `src/pm15min/__main__.py`
  让 `python -m pm15min` 直接走到顶层 CLI。
- `src/pm15min/cli.py`
  全局 CLI 总入口。它不直接做业务，只负责把命令分发给 `data`、`research`、`live` 三个域，并提供一个最顶层的 `layout` 检查命令。

## `core/`

`core/` 是三条业务线都共享的基础设施层。它不负责“业务策略”，只负责定义共同语言。

### 先看哪里

1. `core/assets.py`
2. `core/layout.py`
3. `core/config.py`

### 文件说明

- `core/__init__.py`
  包标记。
- `core/assets.py`
  市场注册表。把 `btc / eth / sol / xrp` 统一成 `AssetSpec`，定义市场 slug、资产名、Binance symbol 等标准字段。几乎所有域的配置工厂都会先经过这里。
- `core/config.py`
  跨域基础配置 dataclass。这里的 `BaseConfig / LiveConfig / ResearchConfig / DataConfig` 更像仓库级统一接口，给其他域提供共同的 `asset + layout + to_dict()` 语言。
- `core/layout.py`
  仓库路径总标准。把 workspace 根目录、`v2/` 根目录、legacy 参考目录、per-market 运行目录统一抽象成 `WorkspaceLayout / RewriteLayout / LegacyMarketReferenceLayout / MarketLayout`。它还有一个重要设计：rewrite 路径统一命名成 `rewrite_*`，legacy 参考路径统一命名成 `legacy_reference_*`，避免运行态路径和旧参考路径在 payload 里混淆。
- `core/types.py`
  少量跨域共享的小类型和基础约定，避免高层模块为了一个小 alias 互相依赖。

## `data/`

`data/` 负责把原始世界拉进来，并整理成 `research/` 和 `live/` 都能稳定消费的 canonical 数据面。

可以把 `data/` 理解成 5 层：

1. `sources/` 直接碰外部世界
2. `pipelines/` 把外部数据落成 canonical source/table/export
3. `queries/` 统一读 canonical 数据
4. `service/` 给 operator/CLI 做 summary、audit、manifest
5. `cli/` 暴露 sync/build/export/record/run 命令

### 推荐阅读顺序

1. `data/layout/__init__.py`
2. `data/config.py`
3. `data/cli/__init__.py`
4. `data/pipelines/foundation_runtime.py`
5. `data/service/__init__.py`

### 顶层文件

- `data/__init__.py`
  `data` 域对外最小出口。主要暴露 `DataConfig`、`DataLayout`、`MarketDataLayout` 和常用的 cycle helper，让外部不必知道内部 `layout/` 子包细节。
- `data/config.py`
  `data` 域配置对象。把 `market / cycle / surface`、抓取窗口、超时、批量大小等参数落成结构化配置，并把对应的 `MarketDataLayout` 绑定进来。
- `data/contracts.py`
  `data` 域基础 dataclass 契约。目前主要覆盖 market catalog、orderbook snapshot、orderbook index row，这些结构会在 pipeline、table 构建和测试里反复复用。

### `data/cli/`

- `data/cli/__init__.py`
  `data` 域稳定 CLI 门面。这里把 parser、handler、config builder、pipeline entrypoint 全部接起来，是外部进入 `data` 域的正式入口。
- `data/cli/args.py`
  复用参数 helper。负责把 `market / cycle / surface` 这类反复出现的 CLI 参数收口，避免各命令自己复制粘贴。
- `data/cli/handlers.py`
  真正的命令分发层。把 parser 解析出的参数转换成 `DataConfig` 和 pipeline/service 调用，并统一打印/返回 payload。
- `data/cli/parser.py`
  只定义命令树，不放业务。这里是 `show / sync / build / export / record / run` 的完整参数结构。

### `data/layout/`

- `data/layout/__init__.py`
  `data` 域最关键的文件。定义 canonical source/table/export/state/log 的目录结构，以及 `DataLayout -> MarketDataLayout` 的路径推导规则。
- `data/layout/helpers.py`
  `cycle / surface` 标准化、时间标签、公共 layout helper。它是 layout 路径拼装时的“词法层”。
- `data/layout/paths.py`
  更底层的路径拼接 helper，例如 cycle/asset 目录、snapshot 文件、年月分区文件怎么生成。

### `data/io/`

- `data/io/__init__.py`
  包标记。
- `data/io/json_files.py`
  JSON/JSONL 的原子写入、追加和基础文件 I/O 工具。live 和 research 也会间接复用这些 helper。
- `data/io/ndjson_zst.py`
  `ndjson.zst` 的读写工具，主要服务 orderbook depth 这种高频、可压缩的逐行数据。
- `data/io/parquet.py`
  Parquet 读写和原子落盘 helper，是 canonical source/table 的底层文件格式封装。

### `data/sources/`

- `data/sources/__init__.py`
  包标记。
- `data/sources/binance_spot.py`
  直接对接 Binance spot 1m klines。负责请求、基础规范化和 DataFrame 化。
- `data/sources/chainlink_rpc.py`
  直接对接 Chainlink streams/datafeeds 的 RPC 读取。它是 `direct_sync.py` 这类 pipeline 的底层来源。
- `data/sources/orderbook_provider.py`
  orderbook provider 抽象层。把读取 orderbook 的接口统一起来，方便 runtime 和测试用不同 provider。
- `data/sources/polygon_rpc.py`
  Polygon RPC 的底层调用与分页/块区间查询封装，给 Chainlink/settlement truth 这类读取做公共基础。
- `data/sources/polymarket_clob.py`
  Polymarket CLOB orderbook 抓取与规范化。它是 orderbook recorder 和 live quote 的底座之一。
- `data/sources/polymarket_gamma.py`
  Polymarket 市场目录抓取、分页和记录标准化。最终驱动 `market_catalog` pipeline。
- `data/sources/polymarket_oracle_api.py`
  Polymarket 直接 oracle/price-to-beat/final-price 接口访问层。

### `data/pipelines/`

- `data/pipelines/__init__.py`
  包标记。
- `data/pipelines/binance_klines.py`
  把 Binance 1m K 线拉取并落到 canonical source。它解决的是“live/research 都要用的基础价格序列怎么稳定拿到”。
- `data/pipelines/direct_oracle_prices.py`
  从 Polymarket 直接 oracle API 抓取价格并落到 canonical source，服务 live foundation 和 research 对齐。
- `data/pipelines/direct_sync.py`
  从 RPC 直接同步 streams、datafeeds、settlement truth。这个文件负责 chunks、时间窗口、写分区和直接来源的标准化。
- `data/pipelines/export_tables.py`
  把 canonical table 导出成更适合人读或外部消费的 CSV/表文件。
- `data/pipelines/foundation_runtime.py`
  live data foundation 编排器。定期刷新 live 最依赖的底座数据，例如 market catalog、binance、oracle、orderbook，并控制不同刷新的节奏。
- `data/pipelines/market_catalog.py`
  抓市场目录并构建 canonical 市场表，是 orderbook、quote、live routing 的上游。
- `data/pipelines/oracle_prices.py`
  把 oracle source 转成规范化的 oracle price table。
- `data/pipelines/orderbook_fleet.py`
  多市场 orderbook recorder 调度器，负责 fleet 级运行、市场列表解析和循环控制。
- `data/pipelines/orderbook_recent.py`
  处理 recent orderbook 窗口、最近快照抽取和热路径视图，给 recorder/runtime 或下游读取做更轻量的 recent 辅助。
- `data/pipelines/orderbook_recording.py`
  记录 raw depth 并构建日级 orderbook index。它是 Polymarket 高频盘口数据进入 v2 的关键落盘流程。
- `data/pipelines/orderbook_runtime.py`
  orderbook recorder 的循环控制和 runtime 封装，负责把一次录制变成可持续运行的命令。
- `data/pipelines/source_ingest.py`
  把 legacy 目录里的旧数据导入 v2 canonical source。它是“重写完成后如何吃历史资产”的兼容桥。
- `data/pipelines/truth.py`
  从 settlement/source 数据构建 canonical truth table，给 research labels/backtests 使用。

### `data/queries/`

- `data/queries/__init__.py`
  包标记。
- `data/queries/loaders.py`
  统一读取 canonical source/table。live 和 research 不应该各自发明自己的读法，这个文件就是统一入口。

### `data/service/`

- `data/service/__init__.py`
  `data` 域 summary/audit 的正式门面。负责把 datasets、audit、issues、completeness、manifest 组装成一个 operator 友好的总 payload。
- `data/service/audit.py`
  audit 总编排。把关键缺失、重复、空键、对齐问题和 dataset 级状态汇总成一份总 audit。
- `data/service/audit_alignment.py`
  对齐检查，例如 source/table 间的时间 lag、覆盖差异、时间边界是否合理。
- `data/service/audit_dataset_checks.py`
  dataset 级规则执行器。给每个数据集跑 freshness、行数、分区完整性等检查。
- `data/service/audit_rules.py`
  审计规则注册表，定义 critical dataset、不同 surface 的规则要求。
- `data/service/datasets.py`
  数据集清单构建器。负责列出每类 source/table 的路径、存在性、时间范围、重复数、空键数等基础统计。
- `data/service/reporting.py`
  把 datasets + audit 变成 issue inventory、completeness report、manifest 的报告层。
- `data/service/shared.py`
  summary/audit/reporting 共用 helper，例如时间规范化、重复数统计、null key 计数、时间范围抽取。

## `research/`

`research/` 负责把 canonical 数据变成 feature、label、training set、training run、model bundle、backtest、evaluation、experiment。

如果说 `data/` 是“生产稳定原料”，那 `research/` 就是“生产离线结论和可部署模型”。

### 推荐阅读顺序

1. `research/layout.py`
2. `research/config.py`
3. `research/contracts.py`
4. `research/service.py`
5. `research/datasets/*`
6. `research/features/*`
7. `research/labels/*`
8. `research/training/*`
9. `research/bundles/*`
10. `research/backtests/*`

### 顶层文件

- `research/__init__.py`
  包标记。
- `research/config.py`
  `research` 域配置对象。把 `market / cycle / profile / target / feature_set / source_surface` 这类研究参数统一落成配置。
- `research/layout.py`
  `research` 域最重要的文件。定义 feature frame、label frame、training set、training run、bundle、active bundle、backtest、experiment、evaluation 的目录结构。
- `research/layout_helpers.py`
  layout 层辅助函数，例如 slug、window label、run label、target/source_surface 的标准化。
- `research/manifests.py`
  统一生成和读写 `manifest.json`。研究域几乎所有产物目录都有 manifest，这个文件就是规范中心。
- `research/service.py`
  研究域资产管理门面。负责列 training run、列 bundle、读 active bundle、写 active bundle、描述 runtime。
- `research/contracts.py`
  `research` 域公共契约出口，对外统一导出 frames/training/runs 这几组 spec。
- `research/_contracts_frames.py`
  feature frame、label frame 等 frame 级 spec 定义。
- `research/_contracts_training.py`
  training set、training run 这类训练过程 spec 定义。
- `research/_contracts_runs.py`
  bundle、backtest、evaluation、experiment 这类“运行结果对象”的 spec 定义。

### `research/cli*`

- `research/cli.py`
  `research` 域 CLI 正式入口。把 build dataset、train、bundle、backtest、experiment、evaluation、activate bundle 等入口接到一起。
- `research/cli_args.py`
  研究域 CLI 复用参数 helper。
- `research/cli_parser.py`
  研究域命令树和参数定义。
- `research/cli_handlers.py`
  把 CLI 参数落成 `ResearchConfig`、spec 和具体调用，是研究域 CLI 的 handler 编排层。

### `research/bundles/`

- `research/bundles/__init__.py`
  包标记。
- `research/bundles/active_registry.py`
  active bundle 注册表。它负责把“当前线上/当前默认用哪个 bundle”收口成单一指针。
- `research/bundles/builder.py`
  把 training run 产物封装成 model bundle，包括 manifest、模型文件、feature contract 等部署侧需要的产物。
- `research/bundles/loader.py`
  解析 bundle 目录、读取 bundle manifest、定位 bundle 文件，是 live/inference 读取 bundle 的入口。

### `research/datasets/`

- `research/datasets/__init__.py`
  包标记。
- `research/datasets/feature_frames.py`
  从 canonical 数据构建 feature frame，是 feature engineering 的正式落盘入口。
- `research/datasets/loaders.py`
  研究数据集的公共加载器，用于训练、回测、评估复用。
- `research/datasets/training_sets.py`
  把 feature frame 和 label frame 对齐并拼成训练集。

### `research/features/`

- `research/features/__init__.py`
  包标记。
- `research/features/base.py`
  特征构建基础约定和通用列/接口。
- `research/features/builders.py`
  特征总装配器。把多个 feature family 拼成最终 feature frame。
- `research/features/cross_asset.py`
  跨资产特征，例如参考市场、相对变化、联动结构。
- `research/features/cycle.py`
  周期位相、时间位置、窗口结构相关特征。
- `research/features/price.py`
  价格序列类特征。
- `research/features/pruning.py`
  特征裁剪、去除、选择相关逻辑，服务实验和训练简化。
- `research/features/registry.py`
  `feature_set -> builder/column contract` 注册表，是 live/research 对齐特征集的重要连接点。
- `research/features/strike.py`
  与 strike、price-to-beat、合约位置相关的特征。
- `research/features/volume.py`
  成交量、活跃度、流动性 proxy 类特征。

### `research/labels/`

- `research/labels/__init__.py`
  包标记。
- `research/labels/alignment.py`
  label 对齐约束，处理 feature/label 时间边界和切片一致性。
- `research/labels/datasets.py`
  label frame 的正式落盘入口。
- `research/labels/direction.py`
  direction label 规则。
- `research/labels/frames.py`
  label frame 总装配器。
- `research/labels/loaders.py`
  label 构建所需 source/table 的统一读取。
- `research/labels/reversal.py`
  reversal label 规则。
- `research/labels/runtime.py`
  label runtime 编排，负责把 label 构建流程变成可执行入口。
- `research/labels/runtime_visibility.py`
  label runtime 的可观测性、可见性或排查辅助，帮助理解一轮标签构建到底看到了什么。
- `research/labels/sources.py`
  label 构建阶段对 source 数据的抽象与预处理入口。

### `research/training/`

- `research/training/__init__.py`
  包标记。
- `research/training/calibration.py`
  训练后校准逻辑，例如概率校准和相关报告。
- `research/training/metrics.py`
  训练指标汇总与标准输出。
- `research/training/probes.py`
  训练过程中的探针、诊断和附加检查。
- `research/training/reports.py`
  训练报告、汇总表、可读摘要。
- `research/training/runner.py`
  一次训练 run 的总编排入口。
- `research/training/splits.py`
  训练/验证/测试切分规则。
- `research/training/trainers.py`
  真正的模型训练器实现。
- `research/training/weights.py`
  样本权重、窗口权重、训练权重策略。

### `research/inference/`

- `research/inference/__init__.py`
  包标记。
- `research/inference/scorer.py`
  用 bundle 对 feature snapshot 或 offset 逐点打分，是 live signal scoring 会复用的推理入口。

### `research/backtests/`

- `research/backtests/__init__.py`
  包标记。
- `research/backtests/decision_engine_parity.py`
  检查 live decision engine 和离线回放版本是否保持一致。
- `research/backtests/decision_quote_surface.py`
  研究 decision 与 quote surface 的关系，帮助观察不同条件下的决策边界。
- `research/backtests/depth_replay.py`
  回放 orderbook depth，用于更细粒度的成交/盘口模拟。
- `research/backtests/engine.py`
  标准 backtest 入口。负责加载 bundle、读取特征、跑离线决策与结果聚合。
- `research/backtests/fills.py`
  成交模拟层，尤其是基于 depth 的 fill 估算和成交细节。
- `research/backtests/guard_parity.py`
  检查 live guard 逻辑与研究态 guard 仿真是否一致。
- `research/backtests/hybrid.py`
  混合式回放/分析逻辑，组合多种 backtest 输入或策略视角。
- `research/backtests/liquidity_proxy.py`
  构造离线可用的流动性 proxy，使 live liquidity 逻辑能在研究态近似重现。
- `research/backtests/live_state_parity.py`
  对齐 live state 构造逻辑和离线 replay 逻辑。
- `research/backtests/orderbook_surface.py`
  从 orderbook 角度构建分析 surface，用于观察盘口质量和执行环境。
- `research/backtests/policy.py`
  backtest 期间使用的 policy 规则、参数化行为和策略约束。
- `research/backtests/regime_parity.py`
  检查 live regime 逻辑与研究态 replay 的一致性。
- `research/backtests/replay_loader.py`
  统一加载 replay 所需输入。
- `research/backtests/reports.py`
  生成 backtest 报告、摘要、图表所需的中间结构。
- `research/backtests/retry_contract.py`
  定义 backtest 或实验运行中与 retry/容错相关的契约。
- `research/backtests/settlement.py`
  处理与结算真值相关的回放问题。
- `research/backtests/taxonomy.py`
  回测结果分类、标签化和结果层级结构。

### `research/evaluation/`

- `research/evaluation/__init__.py`
  包标记。
- `research/evaluation/abm_eval.py`
  ABM 风格评估的总入口。
- `research/evaluation/calibration.py`
  概率校准评估的正式 run 入口。
- `research/evaluation/common.py`
  评估流程公共工具、输入输出规范和中间处理。
- `research/evaluation/drift.py`
  漂移评估入口，用于观察训练后分布变化和部署风险。
- `research/evaluation/poly_eval.py`
  面向 Polymarket 业务语义的综合评估入口。
- `research/evaluation/poly_eval_scopes.py`
  poly eval 的 scope 选择与切片定义。

#### `research/evaluation/methods/`

- `research/evaluation/methods/__init__.py`
  评估方法子包出口。
- `research/evaluation/methods/binary_metrics.py`
  二分类指标。
- `research/evaluation/methods/control_variate.py`
  control variate 方法。
- `research/evaluation/methods/copula_risk.py`
  copula 风险评估。
- `research/evaluation/methods/copulas.py`
  copula 相关建模工具。
- `research/evaluation/methods/decision.py`
  与决策结果相关的评估方法。
- `research/evaluation/methods/events.py`
  事件级评估方法。
- `research/evaluation/methods/pipeline.py`
  把多种评估方法串起来的流水线层。
- `research/evaluation/methods/production_stack.py`
  更接近 production stack 视角的综合评估方法。
- `research/evaluation/methods/time_slices.py`
  按时间切片汇总评估结果。
- `research/evaluation/methods/trade_metrics.py`
  交易结果、命中、盈亏、执行质量相关指标。

#### `research/evaluation/methods/abm/`

- `research/evaluation/methods/abm/__init__.py`
  ABM 方法子包出口。
- `research/evaluation/methods/abm/simulation.py`
  ABM 模拟细节实现。

#### `research/evaluation/methods/probability/`

- `research/evaluation/methods/probability/__init__.py`
  概率方法子包出口。
- `research/evaluation/methods/probability/importance_sampling.py`
  importance sampling 方法。
- `research/evaluation/methods/probability/mc_convergence.py`
  Monte Carlo 收敛检查。
- `research/evaluation/methods/probability/mc_estimators.py`
  Monte Carlo 估计器。
- `research/evaluation/methods/probability/path_models.py`
  路径模型相关概率方法。
- `research/evaluation/methods/probability/types.py`
  概率方法共享类型。

#### `research/evaluation/methods/smc/`

- `research/evaluation/methods/smc/__init__.py`
  SMC 方法子包出口。
- `research/evaluation/methods/smc/particle_filter.py`
  particle filter/SMC 具体实现。

### `research/experiments/`

- `research/experiments/__init__.py`
  包标记。
- `research/experiments/cache.py`
  实验复用缓存层，避免重复构建相同中间产物。
- `research/experiments/compare_policy.py`
  不同 variant/配置之间的比较策略。
- `research/experiments/leaderboard.py`
  实验结果排行榜与 top-k 摘要。
- `research/experiments/orchestration.py`
  实验 suite 的调度与编排规则。
- `research/experiments/reports.py`
  实验报告和摘要输出。
- `research/experiments/runner.py`
  experiment suite 执行入口。
- `research/experiments/specs.py`
  experiment suite 规范、解析和 runtime policy。

## `live/`

`live/` 负责把 active bundle、最新市场状态、账户状态和风控规则拼起来，得到 signal、quote、decision、execution，然后决定是否执行副作用。

可以把 `live/` 拆成 7 层心智模型：

1. `runtime / cli / layout`
   解释 live scope、路径、命令边界
2. `signal / quotes / guards / profiles`
   形成“应该不应该做事”的判断
3. `execution`
   把决策变成可执行计划
4. `actions`
   把计划变成下单、撤单、赎回等副作用 payload
5. `trading`
   真正和 adapter/gateway 打交道
6. `runner / readiness / operator`
   把整套流程跑起来并给 operator 可见性
7. `account / liquidity / regime / capital_usage / oracle`
   给主链路提供状态上下文

### 推荐阅读顺序

1. `live/runtime.py`
2. `live/cli/__init__.py`
3. `live/service/__init__.py`
4. `live/signal/service.py`
5. `live/quotes/__init__.py`
6. `live/signal/decision.py`
7. `live/execution/__init__.py`
8. `live/actions/__init__.py`
9. `live/runner/__init__.py`
10. `live/trading/service.py`

### 顶层文件

- `live/__init__.py`
  `live` 域包说明，不承载核心业务。
- `live/runtime.py`
  `live` 域边界解释器。定义 canonical live scope、profile resolution、active bundle 读取和 operator 入口建议。读懂这个文件，就知道“哪些 live 命令是正式 operator 路径，哪些只是兼容性检查入口”。
- `live/persistence.py`
  live JSON 状态的通用落盘 helper，是 account/quotes/decision/execution/regime 等状态快照共同依赖的底层工具。

### `live/cli/`

- `live/cli/__init__.py`
  `live` 域 CLI 正式门面。把 `show-* / score / quote / decide / runner / execute / sync / redeem` 这些命令接到具体 service/runner API。
- `live/cli/common.py`
  共享参数、payload 打印、live config 构建、canonical target 检查等通用 CLI helper。
- `live/cli/parser.py`
  live 子命令树和参数定义。

### `live/layout/`

- `live/layout/__init__.py`
  定义 live 状态目录、latest/snapshot 约定和不同状态组的标准落盘位置。
- `live/layout/paths.py`
  live 路径拼装 helper。
- `live/layout/state_specs.py`
  不同状态组的元信息定义，例如文件名、latest 路径和分组约定。

### `live/profiles/`

- `live/profiles/__init__.py`
  profile 门面，给外部提供 `resolve_live_profile_spec` 和常用 profile 导出。
- `live/profiles/catalog.py`
  live profile 注册表，列出有哪些 profile、默认 profile 是谁。
- `live/profiles/spec.py`
  单个 profile 的阈值、风控开关、时间窗、stake/edge 限制等规范。

### `live/account/`

- `live/account/__init__.py`
  account 门面，统一导出账户快照、落盘和摘要相关 API。
- `live/account/persistence.py`
  positions/open orders/account 状态的落盘与读取。
- `live/account/state.py`
  构建账户状态快照，负责拉 open orders、positions，并把 raw 结果整理成 live 统一视图。
- `live/account/summary.py`
  把账户状态压成更短、更适合 operator 消费的摘要。

### `live/guards/`

- `live/guards/__init__.py`
  guard 总门面，统一导出各类 guard reason 计算。
- `live/guards/account.py`
  与账户状态直接相关的 guard，例如仓位、挂单、账户侧约束。
- `live/guards/features.py`
  feature 完整性、NaN、bundle/feature_set 兼容性相关 guard。
- `live/guards/quote.py`
  entry price、ROI、盘口边界、价格质量相关 guard。
- `live/guards/regime.py`
  regime、liquidity、市场状态相关 guard。

### `live/signal/`

- `live/signal/__init__.py`
  包标记。
- `live/signal/service.py`
  signal 主链路编排。把 score、quote、decision 三段连接起来，是 live 上游逻辑的真正主轴之一。
- `live/signal/scoring.py`
  signal scoring 稳定门面，负责对外暴露 `score_live_latest` 并编排 scoring 结果结构。
- `live/signal/scoring_bundle.py`
  bundle 选择、feature set 解析、live feature context、liquidity/regime 上下文准备。
- `live/signal/scoring_offsets.py`
  逐个 offset 的具体打分逻辑，包括 blacklist、coverage、NaN、score row 组装。
- `live/signal/utils.py`
  signal 侧通用 helper，例如特征帧构建、coverage、blacklist、snapshot 落盘。
- `live/signal/decision.py`
  把 signal、quote、guards、account/liquidity/regime 上下文组装成 decision snapshot。

### `live/quotes/`

- `live/quotes/__init__.py`
  quote 门面，对外提供 quote snapshot 构建和辅助常量。
- `live/quotes/hot_cache.py`
  orderbook recent/hot cache 读取与摘要，用于 readiness/operator 视图和 quote 侧快速检查。
- `live/quotes/market.py`
  从 market catalog、oracle、基础市场表中读取 quote 需要的 market 侧信息。
- `live/quotes/orderbook.py`
  从 orderbook 角度提取 best bid/ask、spread、depth 等 quote 输入。
- `live/quotes/row_builder.py`
  构造单个 offset 的 quote row。
- `live/quotes/service.py`
  quote 稳定门面与主编排。
- `live/quotes/snapshot_builder.py`
  quote snapshot 总装配器。
- `live/quotes/snapshot_persistence.py`
  quote snapshot 落盘。

### `live/liquidity/`

- `live/liquidity/__init__.py`
  liquidity 门面，对外暴露 snapshot 构建和常用 helper。
- `live/liquidity/fetch.py`
  抓 Binance spot/perp 侧流动性输入和原始指标。
- `live/liquidity/policy.py`
  liquidity policy 门面，统一 raw 检查、temporal filter、阈值计算。
- `live/liquidity/policy_raw.py`
  原始 liquidity 指标与 fail check 计算。
- `live/liquidity/policy_temporal.py`
  temporal filter、recovering 状态、连续失败判定。
- `live/liquidity/policy_thresholds.py`
  不同 market/profile 下的 liquidity 阈值展开。
- `live/liquidity/state.py`
  构建和落盘 liquidity snapshot。

### `live/regime/`

- `live/regime/__init__.py`
  regime 门面，对外提供 regime snapshot 构建和摘要。
- `live/regime/controller.py`
  regime 判定与状态控制逻辑。
- `live/regime/persistence.py`
  regime snapshot 落盘。
- `live/regime/state.py`
  组装 regime snapshot，并接入 liquidity/profile 等上下文。

### `live/execution/`

- `live/execution/__init__.py`
  execution 门面，统一暴露 execution snapshot 构建和落盘。
- `live/execution/depth.py`
  基于 depth 的价格/数量规划和 fill 估算基础。
- `live/execution/order_policy.py`
  订单级 policy，例如 cancel/redeem 相关匹配与策略细节。
- `live/execution/policy.py`
  execution policy 门面，把 order policy、retry policy 和 helper 组起来。
- `live/execution/policy_helpers.py`
  execution policy 侧公共辅助逻辑，例如 open order 匹配和账户上下文整形。
- `live/execution/retry_policy.py`
  冷却、重试、防重复执行等策略。
- `live/execution/service.py`
  execution snapshot 主组装。
- `live/execution/utils.py`
  execution 侧通用小工具。

### `live/actions/`

- `live/actions/__init__.py`
  action 门面，对外统一导出 submit/cancel/redeem 的正式入口。
- `live/actions/builders.py`
  构造 cancel/redeem/order submit payload 和 action key/signature。
- `live/actions/cancel.py`
  撤单策略执行流程。
- `live/actions/gate.py`
  side effect 执行前的 gate、去重和节流控制。
- `live/actions/order_submit.py`
  下单提交策略执行流程。
- `live/actions/persistence.py`
  action 类快照落盘。
- `live/actions/redeem.py`
  赎回策略执行流程。
- `live/actions/service.py`
  action 编排门面，把 cancel/redeem/order submit 暴露成稳定 API。
- `live/actions/utils.py`
  action 共用 helper 和兼容导出。

### `live/trading/`

- `live/trading/__init__.py`
  trading 包的对外导出面，汇总 auth/contracts/gateway/service。
- `live/trading/auth.py`
  交易鉴权与环境变量配置加载。
- `live/trading/contracts.py`
  交易域统一契约和 dataclass。
- `live/trading/direct_adapter.py`
  direct adapter 实现，直接与交易 API/数据 API 打交道。
- `live/trading/gateway.py`
  交易 gateway 抽象接口。
- `live/trading/legacy_adapter.py`
  legacy adapter 桥接实现，允许在严格边界内复用旧交易能力。
- `live/trading/normalize.py`
  订单、持仓、成交等对象的归一化。
- `live/trading/positions_api.py`
  direct 路径下的 positions 读取。
- `live/trading/redeem_relayer.py`
  链上/中继赎回执行逻辑。
- `live/trading/service.py`
  adapter 选择、env 配置解析、gateway 构建、gateway 说明。

### `live/gateway/`

- `live/gateway/__init__.py`
  包标记。
- `live/gateway/capabilities.py`
  描述不同 gateway/adapter 支持的能力集合。
- `live/gateway/checks.py`
  gateway health check 门面。
- `live/gateway/probes.py`
  open orders、positions 等只读 probe。
- `live/gateway/service.py`
  gateway 检查主组装层。

### `live/readiness/`

- `live/readiness/__init__.py`
  operator 视角的 readiness 门面，负责回答“现在能不能跑 / 缺什么 / 下一步看什么”。
- `live/readiness/state.py`
  readiness 所需状态抽取与路径汇总。

### `live/operator/`

- `live/operator/__init__.py`
  包标记。
- `live/operator/actions.py`
  把当前状态翻译成 operator 下一步动作建议。
- `live/operator/categories.py`
  问题分类与状态分桶。
- `live/operator/followups.py`
  follow-up 门面，把不同 blocker/side-effect 的后续动作聚合起来。
- `live/operator/followups_blockers.py`
  foundation/decision/execution blocker 的动作建议。
- `live/operator/followups_side_effects.py`
  账户、撤单、赎回等副作用后的 follow-up 建议。
- `live/operator/rejects.py`
  把 guard/reject 原因翻译成更适合 operator 的解释。
- `live/operator/smoke.py`
  轻量 smoke check 总结。
- `live/operator/summary.py`
  operator 看板式摘要。
- `live/operator/utils.py`
  operator 文本、风险提示、读取辅助。

### `live/capital_usage/`

- `live/capital_usage/__init__.py`
  capital usage 对外门面。
- `live/capital_usage/context.py`
  capital usage 所需上下文准备。
- `live/capital_usage/overview.py`
  资金使用概览、比率和 fallback summary。
- `live/capital_usage/service.py`
  capital usage 主计算逻辑。

### `live/redeem/`

- `live/redeem/__init__.py`
  redeem loop 的正式 API 门面，负责把赎回循环接到 action 层和日志落盘。
- `live/redeem/runtime.py`
  redeem loop 的循环控制和 runtime helper。

### `live/oracle/`

- `live/oracle/__init__.py`
  oracle 子包出口。
- `live/oracle/strike_cache.py`
  strike/price-to-beat 相关缓存读取和组织。
- `live/oracle/strike_runtime.py`
  strike cache/runtime 更新逻辑，服务需要 strike 侧上下文的 live 链路。

### `live/service/`

- `live/service/__init__.py`
  live 域总门面。它不是“所有逻辑都写在这里”，而是统一暴露 score/quote/decide/simulate/execute/sync/readiness 等稳定 patch 点。
- `live/service/facade_helpers.py`
  facade 共享 helper、alias 和 wiring bridge，减少 `service/__init__.py` 本体复杂度。
- `live/service/operation.py`
  更高层的 live operation 编排，例如 execute latest、sync account/liquidity、cancel/redeem policy。
- `live/service/wiring.py`
  service 内部依赖注入和 wiring 组装。

### `live/runner/`

- `live/runner/__init__.py`
  runner 总门面。把单次迭代、循环运行、foundation 调用、副作用执行、日志落盘串起来。
- `live/runner/api.py`
  面向 CLI 的 runner 薄 API。
- `live/runner/diagnostics.py`
  diagnostics 门面，聚合 risk/health 诊断。
- `live/runner/diagnostics_health.py`
  runner health 诊断与状态摘要。
- `live/runner/diagnostics_risk.py`
  runner risk 诊断、alerts 和 alert summary。
- `live/runner/iteration.py`
  一轮 live pipeline 具体做什么，是 runner 主流程最核心的实现之一。
- `live/runner/runtime.py`
  runner loop、sleep、iteration 计数、循环持久化节奏。
- `live/runner/service.py`
  runner 的兼容层与稳定导出边界。
- `live/runner/utils.py`
  runner 共用工具和错误 payload 构造。

## `tests/`

测试文件的命名基本就是代码地图。想知道某块有没有稳定下来，先看对应测试。

### 顶层与架构护栏

- `tests/conftest.py`
  测试公共夹具、临时目录和通用环境约定。
- `tests/test_architecture_guards.py`
  架构边界护栏：deprecated shim 是否正确指向 v2、`MarketLayout` 是否正确切分 rewrite/legacy 路径，以及 v2 是否违规导入 `live_trading`、旧 `src`、旧脚本或 `poly_eval` 等历史边界外代码。
- `tests/test_cli.py`
  顶层 CLI、domain CLI、命令接线和参数入口。

### `data` 相关测试

- `tests/test_data_layout.py`
  `data/layout` 路径规范。
- `tests/test_data_orderbook.py`
  orderbook 记录结构和 index row 构造。
- `tests/test_data_market_catalog.py`
  市场目录标准化。
- `tests/test_data_orderbook_provider.py`
  orderbook provider 抽象与 provider 侧行为。
- `tests/test_data_orderbook_fleet.py`
  多市场 orderbook fleet 调度。
- `tests/test_data_builders.py`
  oracle/truth/orderbook index/source ingest 等 build 流程。
- `tests/test_data_pipelines.py`
  data pipeline 组合行为。
- `tests/test_data_direct_sync.py`
  RPC 直连同步链路。
- `tests/test_data_exports.py`
  canonical table 导出。
- `tests/test_data_recorder_runtime.py`
  orderbook recorder runtime。
- `tests/test_data_foundation_runtime.py`
  live foundation runtime。
- `tests/test_data_service.py`
  summary/audit/issues/completeness。

### `research` 相关测试

- `tests/test_research_layout.py`
  research 路径规范。
- `tests/test_research_manifests.py`
  manifest 读写。
- `tests/test_research_active_bundles.py`
  active bundle 读写与切换。
- `tests/test_research_builders.py`
  research build/bundle 相关流程。
- `tests/test_research_bundle_parity.py`
  bundle 构造或读取的一致性。
- `tests/test_research_feature_pruning.py`
  feature pruning 行为。
- `tests/test_research_training_parity.py`
  训练链路一致性。
- `tests/test_research_training_datasets_parity.py`
  训练数据集构造的一致性。
- `tests/test_research_inference_scorer.py`
  inference scorer 行为。
- `tests/test_research_backtest_parity.py`
  标准 backtest parity。
- `tests/test_research_backtest_runtime_parity.py`
  backtest runtime 侧 parity。
- `tests/test_research_backtest_phase_b.py`
  phase-b 类回测流程。
- `tests/test_research_backtest_policy.py`
  backtest policy 规则。
- `tests/test_research_backtest_replay.py`
  replay 行为。
- `tests/test_research_backtest_depth_replay.py`
  depth replay 行为。
- `tests/test_research_backtest_depth_parity.py`
  depth 相关 parity。
- `tests/test_research_backtest_fills.py`
  fill 模拟行为。
- `tests/test_research_backtest_live_state_parity.py`
  live state 离线重建 parity。
- `tests/test_research_backtest_regime_parity.py`
  regime parity。
- `tests/test_research_backtest_liquidity_proxy.py`
  liquidity proxy。
- `tests/test_research_backtest_decision_engine_parity.py`
  decision engine parity。
- `tests/test_research_backtest_decision_quote_surface.py`
  decision/quote surface 结果。
- `tests/test_research_evaluation_methods.py`
  evaluation 通用方法。
- `tests/test_research_evaluation_decision_methods.py`
  decision 相关 evaluation 方法。
- `tests/test_research_evaluation_probability_methods.py`
  probability 方法。
- `tests/test_research_evaluation_abm_methods.py`
  ABM 方法。
- `tests/test_research_evaluation_copula_methods.py`
  copula 方法。
- `tests/test_research_evaluation_pipeline_stack_methods.py`
  pipeline stack 方法。
- `tests/test_research_experiment_cache.py`
  实验缓存。
- `tests/test_research_experiment_compare_policy.py`
  实验比较策略。
- `tests/test_research_experiment_matrix_parity.py`
  实验矩阵/parity。
- `tests/test_research_experiment_parity_specs.py`
  实验 spec 解析与 parity。
- `tests/test_research_experiment_reports.py`
  实验报告。
- `tests/test_research_experiment_runtime_resume.py`
  实验运行恢复/resume。

### `live` 相关测试

- `tests/test_live_account.py`
  account state/summary/persistence。
- `tests/test_live_actions.py`
  cancel/redeem/order submit 行为。
- `tests/test_live_execution.py`
  execution 规划与策略。
- `tests/test_live_guards.py`
  guard/reject 边界。
- `tests/test_live_liquidity.py`
  liquidity snapshot 与 policy。
- `tests/test_live_quotes.py`
  quote 组装逻辑。
- `tests/test_live_regime.py`
  regime 判定与状态。
- `tests/test_live_service.py`
  live facade 稳定契约。
- `tests/test_live_runner.py`
  runner 单次迭代与循环行为。
- `tests/test_live_redeem_loop.py`
  redeem loop。
- `tests/test_live_strike_runtime.py`
  strike runtime/oracle 相关逻辑。
- `tests/test_live_trading_auth.py`
  trading auth。
- `tests/test_live_trading_direct_adapter.py`
  direct adapter。
- `tests/test_live_trading_legacy_adapter.py`
  legacy adapter。
- `tests/test_live_trading_redeem_relayer.py`
  redeem relayer。
- `tests/test_live_trading_service.py`
  trading service / gateway 组装。

## 如果你想改某块，先去哪里

- 想改全局命令入口：`src/pm15min/cli.py`
- 想改仓库路径规则：`core/layout.py`
- 想改市场注册或 symbol 映射：`core/assets.py`
- 想改 canonical 数据路径：`data/layout/__init__.py`
- 想改外部数据来源：`data/sources/*`
- 想改 canonical 数据构建：`data/pipelines/*`
- 想改 data audit/summary：`data/service/*`
- 想改研究产物路径：`research/layout.py`
- 想改 feature：`research/features/*`
- 想改 label：`research/labels/*`
- 想改训练：`research/training/*`
- 想改 bundle 生命周期：`research/bundles/*`
- 想改 backtest：`research/backtests/*`
- 想改 live 主入口：`live/service/__init__.py`、`live/runner/__init__.py`
- 想改 live 打分：`live/signal/*`
- 想改 live quote/decision：`live/quotes/*`、`live/signal/decision.py`
- 想改 live 执行计划：`live/execution/*`
- 想改 live 副作用：`live/actions/*`
- 想改交易接入：`live/trading/*`
- 想改 operator/readiness：`live/readiness/*`、`live/operator/*`

## 常用命令

在仓库根目录执行：

```bash
PYTHONPATH=v2/src python -m pm15min --help
PYTHONPATH=v2/src python -m pm15min layout --market sol --json
PYTHONPATH=v2/src python -m pm15min data show-layout --market sol --cycle 15m --surface live
PYTHONPATH=v2/src python -m pm15min data run live-foundation --market sol --surface live
PYTHONPATH=v2/src python -m pm15min research list-runs --market sol --profile deep_otm --target direction
PYTHONPATH=v2/src python -m pm15min console show-home
PYTHONPATH=v2/src python -m pm15min console show-actions
PYTHONPATH=v2/src python -m pm15min console build-action --action-id data_refresh_summary --request-json '{"market":"sol"}'
PYTHONPATH=v2/src python -m pm15min console show-data-overview --market sol --cycle 15m --surface backtest
PYTHONPATH=v2/src python -m pm15min console serve --host 127.0.0.1 --port 8765
PYTHONPATH=v2/src python -m pm15min live show-config --market sol --profile deep_otm
PYTHONPATH=v2/src python -m pm15min live runner-once --market sol --profile deep_otm --target direction
PYTHONPATH=v2/src pytest -q v2/tests
```

## 相关文档

- `docs/DATA_TECHNICAL_PLAN.md`
  Data 域正式方案。
- `docs/RESEARCH_TECHNICAL_PLAN.md`
  Research 域正式方案。
- `docs/LIVE_TECHNICAL_PLAN.md`
  Live 域正式方案。
- `docs/LIVE_OPERATOR_RUNBOOK.md`
  实盘 operator 视角运行手册。
- `docs/MODEL_BUNDLE_IMPORT_STATE.md`
  bundle 导入状态。
- `docs/EXECUTION_BOARD.md`
  多 agent 并行开发执行板。
- `docs/V2_TRAINING_BACKTEST_VISUALIZATION_PLAN.md`
  训练 / bundle / backtest 可视化规划。
- `docs/REWRITE_STATUS_AND_ROADMAP.md`
  当前完成度与路线图。
- `docs/V2_REFACTOR_REVIEW_20260320.md`
  当前重构审查与风险记录。
- `docs/CURRENT_REMAINING_CHECKLIST.md`
  剩余待办。

## 最后一句

这套代码现在的核心判断不是“还要不要继续拆”，而是：

- 边界已经大体稳定
- 真正应该继续投入的是让每个边界里的业务逻辑更可靠
- 如果你一时不知道从哪里开始，就回到：
  `cli.py -> core/layout.py -> data/layout -> research/layout -> live/runtime -> live/service -> live/runner`

这是整套 v2 的最短主干。
# pm15minv2
