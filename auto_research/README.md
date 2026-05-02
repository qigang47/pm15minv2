# Research Automation

This repository keeps the experiment control plane inside the repo. The external decision-maker is `codex`.

## Directory Boundary

Autoresearch code is intentionally split across two places:

- `auto_research/`
  - operator-facing entrypoints
  - shell wrappers
  - queue / status / session helper scripts
  - active `program*.md` instructions
  - this directory is the runnable surface for starting, stopping, and inspecting autoresearch
- `src/pm15min/research/automation/`
  - reusable Python control-plane logic
  - queue-state reconciliation
  - Codex prompt construction
  - dense decision policy
  - experiment summary parsing
  - quick-screen evaluation helpers used by the runnable surface

Practical rule:

- if a file is mainly a launch surface, shell entrypoint, operator command, or active program note, keep it in `auto_research/`
- if a file implements decision logic, parsing, ranking, queue reconciliation, or reusable automation behavior, keep it in `src/pm15min/research/automation/`
- tests for both layers stay under `tests/`

Things that do **not** define the intended structure:

- `auto_research/__pycache__/`
- `src/pm15min/research/automation/__pycache__/`

Those are runtime byproducts, not source layout.

Main entrypoints:

- `auto_research/program.md`
  - repo-local research instructions for each Codex cycle
- `auto_research/program_direction_dense.md`
  - repo-local dense direction instructions
- `auto_research/program_reversal_dense.md`
  - repo-local dense reversal instructions
- `auto_research/program_direction_midprice_btc.md`
  - standalone BTC midprice direction autoresearch instructions
- `auto_research/program_direction_midprice_eth.md`
  - standalone ETH midprice direction autoresearch instructions
- `auto_research/run_one_experiment.sh`
  - run one formal experiment suite
- `auto_research/run_one_experiment_background.sh`
  - launch one formal experiment suite in the background and return immediately
- `auto_research/experiment_queue.py`
  - enqueue formal launches and repairs, inspect queue state, or run one queue-supervisor pass
- `auto_research/experiment_queue_supervisor.sh`
  - keep up to the configured live-experiment cap running from the repo-local queue
- `auto_research/summarize_experiment.py`
  - extract a compact JSON summary from a finished experiment run
- `auto_research/update_session.py`
  - append one cycle into the active session files
- `auto_research/codex_background_loop.sh`
  - start, stop, restart, or run one background Codex cycle
- `auto_research/start_direction_dense.sh`
  - start, stop, restart, or run one dense direction background instance
- `auto_research/start_reversal_dense.sh`
  - start, stop, restart, or run one dense reversal background instance
- `auto_research/start_direction_midprice_btc.sh`
  - start, stop, restart, or run the standalone BTC midprice direction background instance
- `auto_research/start_direction_midprice_eth.sh`
  - start, stop, restart, or run the standalone ETH midprice direction background instance
- `auto_research/start_midprice_direction_stack.sh`
  - start, stop, restart, or inspect both standalone BTC/ETH midprice direction instances
- `auto_research/status_autorun.sh`
  - inspect background status plus incomplete experiment runs
- `auto_research/status_dense_autorun.sh`
  - inspect both dense background instances

Core automation modules:

- `src/pm15min/research/automation/control_plane.py`
  - shared status inspection, session/run summarization, and Codex cycle prompt generation
- `src/pm15min/research/automation/queue_state.py`
  - queue item state model, reconciliation, refill, reseed, and launch selection
- `src/pm15min/research/automation/dense_policy.py`
  - dense quick-screen and formal comparison ordering rules
- `src/pm15min/research/automation/quick_screen.py`
  - profitable-offset-pool fast-screen evaluation logic
- `src/pm15min/research/automation/focus_feature_search.py`
  - focused feature-family search helpers

Default session resolution:

- `codex_background_loop.sh` now resolves the session directory from the active program file
- if `SESSION_DIR` is unset, it reads the first `Active session:` reference in `auto_research/program.md`
- this keeps the background loop from mixing a new program with an old session log

Default Codex home behavior:

- `codex_background_loop.sh` now defaults to `CODEX_HOME_MODE=isolated`
- it copies a minimal safe subset from `~/.codex/` into `var/research/autorun/codex-home/.codex/`
- this avoids two common operator problems:
  - broken user-installed skills under the main `~/.codex/skills/`
  - unwritable session storage under the main `~/.codex/sessions/`

Operator overrides:

- `CODEX_HOME_MODE=inherit`
  - use the operator's main `~/.codex/` directly
- `CODEX_HOME_DIR=/custom/path`
  - choose a different isolated home root
- `CODEX_MODEL=gpt-5.5`
  - default background decision model; override only for deliberate special cycles
- `CODEX_REASONING_EFFORT=high`
  - default background reasoning level used by `codex exec`
- `CODEX_PROMPT_BUDGET_MODE=compact`
  - omit large prompt sections and rely on precomputed summaries unless the cycle is blocked
- `CODEX_MODEL_AUTO_COMPACT_TOKEN_LIMIT=60000`
  - ask Codex to compact earlier instead of carrying very large context between background turns
- `LOOP_SLEEP_SEC=1800` for BTC/ETH formal, `900` for SOL/XRP quick-screen
  - default wrapper cadence is slower for formal lines and faster for quick-screen lines; experiment workers continue independently
- `QUEUE_SUPERVISOR_LOOP_SLEEP_SEC=5`
  - shared queue dispatch cadence; keep this separate from `LOOP_SLEEP_SEC` so quick-screen workers are refilled promptly
- `CODEX_SECONDARY_BASE_URL=...`
  - background-only second provider endpoint, intended for a secondary Nimabo key before the normal fallback provider
- `CODEX_SECONDARY_API_KEY=...`
  - matching API key for the secondary provider endpoint
- `FALLBACK_ENV_PATH=/custom/path/codex-fallback.env`
  - load background-only fallback provider settings from a local env file
- `CODEX_SHARED_FALLBACK_ENV_PATH=var/research/autorun/codex-fallback.env`
  - shared background fallback settings loaded before the per-track fallback env, useful for newly split autorun tracks
- `CODEX_FALLBACK_BASE_URL=...`
  - backup responses endpoint used only when the primary background provider fails transiently
- `CODEX_FALLBACK_API_KEY=...`
  - matching API key for the backup endpoint
- `CODEX_NETWORK_PROXY_MODE=direct|inherit|managed`
  - choose whether the primary provider path clears proxy env vars or inherits them
- `CODEX_OFFICIAL_NETWORK_PROXY_MODE=direct|inherit|managed`
  - choose the network mode for the official auth path separately from the primary provider path
- `CODEX_OFFICIAL_PRIORITY=first|fallback`
  - choose whether official ChatGPT auth runs before external providers or only after they fail
- `MAX_CONSECUTIVE_FAILURES=5`
  - change the consecutive failure threshold recorded in status
- `CODEX_STOP_ON_CONSECUTIVE_FAILURES=0|1`
  - when `0`, provider outages are recorded but the background loop keeps waiting for the next cycle

Default background retry order:

- official login from `CODEX_OFFICIAL_AUTH_PATH`
- secondary Nimabo key from `CODEX_SECONDARY_BASE_URL` + `CODEX_SECONDARY_API_KEY`
- optional backup provider from `CODEX_FALLBACK_BASE_URL` + `CODEX_FALLBACK_API_KEY`
- when `CODEX_OFFICIAL_PRIORITY=fallback`, official login moves to the final fallback position

Dense wrapper defaults:

- `start_direction_dense.sh` and `start_reversal_dense.sh` pin `CODEX_OFFICIAL_AUTH_PATH` to the shared `var/research/autorun/codex-official-auth.json`
- the dense wrappers keep `CODEX_NETWORK_PROXY_MODE=direct` unless you override it
- the dense wrappers keep `CODEX_OFFICIAL_NETWORK_PROXY_MODE=managed` unless you override it, so official auth uses the managed proxy env file
- the dense wrappers keep `CODEX_OFFICIAL_PRIORITY=first` unless you override it

## Runtime Flow

This is the actual runtime split for the dense dual-track setup:

- there are normally **two** long-lived Codex decision loops
  - one dense `direction` loop
  - one dense `reversal` loop
- there is normally **one** shared queue supervisor
  - it consumes queued work for both dense tracks
- there are many short- or long-lived experiment workers underneath
  - those are the processes that actually train, bundle, quick-screen, or run formal suites

Flowchart:

```text
start_direction_dense.sh          start_reversal_dense.sh
          |                                  |
          v                                  v
codex_background_loop.sh         codex_background_loop.sh
   (Direction Codex)                (Reversal Codex)
          |                                  |
          v                                  v
 read current line state             read current line state
 (program / session / queue / runs)  (program / session / queue / runs)
          |                                  |
          v                                  v
 direction decision                  reversal decision
 (hold / repair / next branch)      (hold / repair / next branch)
           \                                /
            \                              /
             v                            v
                 experiment_queue.py enqueue
                            |
                            v
                   shared experiment queue
                            |
                            v
             experiment_queue_supervisor.sh
                  (shared refill loop)
                            |
                            v
                  run_one_experiment.sh
                            |
                            v
                     choose launch mode
                     /                \
                    v                  v
            quick_screen             formal
      run_quick_screen_suite.py   pm15min research experiment run-suite
                     \                /
                      \              /
                       v            v
                      run summaries / logs
                               |
                               v
               control_plane.py re-reads results
                               |
                 +-------------+-------------+
                 |                           |
                 v                           v
          back to Direction Codex      back to Reversal Codex
```

One-screen mental model:

- 上面两条是“做决策”的
  - 方向线一个
  - 反转线一个
- 中间共享队列是“存下一步动作”的
- 队列监督器是“把动作变成真实运行”的
- 最下面的实验进程才是“真正吃 CPU 做训练和回测”的
- 实验结果再回流到上面的 Codex，进入下一轮决策

If you want to quickly answer “who is responsible for what”, use this mapping:

- `start_*_dense.sh`
  - start one decision line
- `codex_background_loop.sh`
  - decide what should happen next
- `experiment_queue.py`
  - record that decision as queue state
- `experiment_queue_supervisor.sh`
  - keep machine occupancy near the configured caps
- `run_one_experiment.sh`
  - launch one worker in the selected mode
- `quick_screen` / `formal`
  - do the real compute work

In practice the moving parts are:

1. Start the two dense research lines.
   - `./auto_research/start_direction_dense.sh start`
   - `./auto_research/start_reversal_dense.sh start`
   - each wrapper binds its own `PROGRAM_PATH`, `SESSION_DIR`, and `AUTORUN_DIR`
   - each wrapper then runs `auto_research/codex_background_loop.sh`

2. Each Codex loop reads the current line state and makes one bounded decision cycle.
   - it reads the active `program_*.md`
   - it inspects queue state, live workers, incomplete runs, and recent completed runs
   - it writes back cycle notes under that line's session directory
   - it decides whether to leave current workers alone, repair a stalled line, or enqueue a next branch

3. Codex does **not** directly keep machine occupancy full by itself.
   - Codex mainly decides what should exist next
   - those decisions are written into the shared repo-local queue through `auto_research/experiment_queue.py enqueue`

4. The shared queue supervisor turns queued decisions into real running workers.
   - `auto_research/experiment_queue_supervisor.sh` runs `supervise-once` in a loop
   - it reconciles queue state against actual live workers
   - it launches or resumes work until it reaches the configured caps
   - current defaults are:
     - `MAX_LIVE_RUNS=8`
     - `TRACK_SLOT_CAPS_JSON={"direction_dense":4,"reversal_dense":4}`
     - `MIN_AVAILABLE_MEM_GB=2`
     - `MAX_QUEUED_ITEMS=24`
     - `QUEUE_SUPERVISOR_LOOP_SLEEP_SEC=5`

5. A launched experiment goes through `auto_research/run_one_experiment.sh`.
   - that wrapper is the single experiment entry surface
   - it decides whether the worker runs:
     - `quick_screen`
     - or full `formal` experiment mode
   - the queue supervisor currently defaults to `quick_screen` launch mode unless overridden

6. The actual heavy work happens below the shell wrappers.
   - `scripts/research/run_quick_screen_suite.py`
     - fast profitable-offset-pool screening
   - `python -m pm15min research experiment run-suite`
     - full formal train / bundle / backtest flow
   - these worker processes are what consume CPU and memory

7. Results flow back upward into the next Codex cycle.
   - run directories under `research/experiments/runs/...` accumulate summaries and logs
   - the control plane re-reads those artifacts on the next cycle
   - Codex then decides the next repair, hold, or follow-up branch

Simple mental model:

- `start_direction_dense.sh` / `start_reversal_dense.sh`
  - start the two decision-makers
- `codex_background_loop.sh`
  - thinks and writes next actions
- `experiment_queue.py`
  - stores those actions
- `experiment_queue_supervisor.sh`
  - keeps turning queued actions into live workers
- `run_one_experiment.sh`
  - starts one worker in quick-screen or formal mode
- experiment workers
  - do the real compute

Operational consequence:

- seeing two `codex` processes is normal in the dense setup
  - one belongs to direction
  - one belongs to reversal
- seeing many experiment workers is also normal
  - those are separate from the two Codex decision loops
- if occupancy drops while the queue supervisor is healthy, the usual cause is not SSH or Codex death
  - it usually means one track has stopped generating new queue items, or the queue currently has no refillable candidate for that track

Current dense expectation:

- direction and reversal together target up to 16 live workers total
- the intended steady state is usually near:
  - 8 direction workers
  - 8 reversal workers
- temporary dips below 16 can still happen when a track finishes workers faster than its Codex loop produces the next refillable candidates

Typical commands:

```bash
./auto_research/experiment_queue_supervisor.sh start
./auto_research/codex_background_loop.sh start
./auto_research/status_autorun.sh
./auto_research/codex_background_loop.sh stop
./auto_research/experiment_queue_supervisor.sh stop
```

Dense instance commands:

```bash
./auto_research/start_direction_dense.sh start
./auto_research/start_reversal_dense.sh start
./auto_research/status_dense_autorun.sh
./auto_research/start_direction_dense.sh stop
./auto_research/start_reversal_dense.sh stop
```

Standalone BTC/ETH midprice direction commands:

```bash
./auto_research/start_midprice_direction_stack.sh start
./auto_research/start_midprice_direction_stack.sh status
./auto_research/start_midprice_direction_stack.sh stop
```

These BTC/ETH lines intentionally do not use the shared SOL/XRP quick-screen
queue. Each coin has its own Codex background loop, its own session, and one
full formal midprice-direction experiment live at a time.

Queue one formal follow-up instead of launching it immediately:

```bash
./auto_research/experiment_queue.py enqueue \
  --market btc \
  --suite baseline_focus_feature_search_btc_reversal_40v6_bias60_2usd_5max_20260413 \
  --run-label auto_btc_40v6_bias60_2usd_5max_20260413 \
  --action launch \
  --reason "queue from codex cycle"
```

Long-lived formal worker launch:

```bash
./auto_research/run_one_experiment_background.sh \
  --suite baseline_focus_feature_search_btc_reversal_40v4_novolweight_2usd_5max_20260413 \
  --run-label auto_btc_40v4_novolweight_2usd_5max_r1_20260413 \
  --market btc
```

Legacy note:

- `bootstrap_keepalive.sh` is now legacy-only for older fixed bootstrap lines.
- Dynamic autoresearch occupancy should use the queue supervisor instead.
