# Live Operator Runbook

这份 runbook 只回答值班视角的问题：

1. 实盘入口是什么。
2. 上线前先检查什么。
3. 运行中看哪些状态和日志。
4. 异常时先用哪些命令定位。

长期技术边界以 `docs/LIVE_TECHNICAL_PLAN.md` 为准。

## 1. 当前 operator 入口

### 1.1 Python CLI

在仓库根目录执行：

```bash
PYTHONPATH=src python -m pm15min live ...
```

最常用命令：

```bash
PYTHONPATH=src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct
PYTHONPATH=src python -m pm15min live show-ready --market sol --profile deep_otm --adapter direct
PYTHONPATH=src python -m pm15min live show-latest-runner --market sol --profile deep_otm --target direction
PYTHONPATH=src python -m pm15min live runner-once --market sol --profile deep_otm --target direction --dry-run-side-effects
```

### 1.2 shell entrypoints

长期 shell 入口在：

```text
scripts/entrypoints/
```

主要脚本：

- `start_v2_live_foundation.sh`
- `start_v2_live_trading.sh`
- `start_v2_orderbook_fleet.sh`
- `start_v2_auto_redeem.sh`

## 2. 上线前最小检查

最小顺序固定为：

1. 检查交易接入
2. 检查 readiness
3. 必要时 dry-run 一轮 runner
4. 再启动长期 loop

对应命令：

```bash
PYTHONPATH=src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct --probe-open-orders --probe-positions
PYTHONPATH=src python -m pm15min live show-ready --market sol --profile deep_otm --adapter direct
PYTHONPATH=src python -m pm15min live runner-once --market sol --profile deep_otm --target direction --adapter direct --dry-run-side-effects
```

重点不要省略：

- `check-trading-gateway`
- `show-ready`

它们分别回答：

- 接入层能不能工作
- 当前状态是否允许 side effects

## 3. 运行中主要看什么

### 3.1 operator 命令

优先看：

- `show-latest-runner`
- `show-ready`

它们比直接翻日志更快，因为已经做了归类和摘要。

### 3.2 canonical 状态目录

重点目录：

```text
var/live/state/
var/live/logs/
```

常看对象：

- runner summary / snapshots
- orderbook recorder state
- orderbook hot cache
- redeem runner latest state
- account / positions / open orders snapshots

### 3.3 orderbook 相关

如果怀疑 quote 输入不完整，先看：

- `show-ready`
- `show-latest-runner`
- `var/live/state/orderbooks/...`
- `var/live/logs/data/recorders/...`

典型问题：

- 热缓存缺失
- 热缓存过旧
- recorder 没在跑
- recorder 在跑但没写到 recent.parquet

## 4. 常见排查顺序

### 4.1 `show-ready` 不 ready

先看：

1. `orderbook` 热缓存是否 `missing / stale / empty`
2. gateway probe 是否失败
3. active bundle 是否存在
4. 最新 runner / account / liquidity 状态是否缺失

建议命令：

```bash
PYTHONPATH=src python -m pm15min live show-ready --market sol --profile deep_otm --adapter direct
PYTHONPATH=src python -m pm15min live check-trading-gateway --market sol --profile deep_otm --adapter direct --probe-open-orders --probe-positions
PYTHONPATH=src python -m pm15min research show-active-bundle --market sol --profile deep_otm --target direction
```

### 4.2 runner 没产生新 summary

先确认：

- `runner-loop` 进程是否在跑
- 依赖的数据 runtime 是否新鲜
- `show-ready` 是否长期拒绝

然后再看：

- `var/live/logs/runner/...`
- `var/live/state/.../latest.json`

### 4.3 redeem loop 不工作

先看：

- `apply-redeem-policy` 单次是否可跑
- `redeem-loop` 的 `latest.json`
- `var/live/logs/redeem_runner/...`

## 5. operator 原则

- 先看 canonical CLI 摘要，再看底层文件。
- 先确认数据和接入是否 ready，再追业务逻辑。
- 真实状态以 `var/live/` 为准，不以 shell wrapper 输出为准。
- 需要长期保留的值班知识写在本 runbook，不把一次性验证记录继续留在 `docs/`。

## 6. 受管代理切换

如果服务器代理节点不稳定，可以启用仓库里的受管代理切换脚本：

```bash
bash scripts/maintenance/install_managed_proxy_timer.sh
```

如果机器上的真实代理入口不是配置文件里的固定端口，而是当前在线的单个本地代理口，可以在安装时直接指定：

```bash
PM15MIN_MANAGED_PROXY_CANDIDATE_PORTS="20171" \
PM15MIN_MANAGED_PROXY_SCHEME="http" \
PM15MIN_MANAGED_PROXY_HEALTHCHECK_URLS="https://gamma-api.polymarket.com/markets?limit=1 https://clob.polymarket.com/time" \
bash scripts/maintenance/install_managed_proxy_timer.sh
```

它会做三件事：

- 每 5 分钟探测一次当前配置的受管代理入口
- 尽量保留当前还能正常工作的入口，只有当前入口失效时才切到别的入口
- 把选中的入口写到 `~/.local/state/pm15min-managed-proxy/active_proxy.env`

如果用了上面的显式端口模式，探测对象就会改成你指定的那组端口，而不是去读 `config.json`。

当前状态文件在：

```text
~/.local/state/pm15min-managed-proxy/state.json
~/.local/state/pm15min-managed-proxy/active_proxy.env
```

当前接线规则是：

- `auto_research/run_one_experiment.sh`
- `scripts/entrypoints/start_v2_live_foundation.sh`

这两个入口默认会尝试加载受管代理。

`start_v2_orderbook_fleet.sh` 目前不走这套自动代理切换，默认仍保持原来的直连行为，目的是减少 orderbook 录制因为代理切换造成的瞬时空档。
