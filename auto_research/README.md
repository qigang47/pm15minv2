# Research Automation

This repository keeps the experiment control plane inside the repo. The external decision-maker is `codex`.

Main entrypoints:

- `auto_research/program.md`
  - repo-local research instructions for each Codex cycle
- `auto_research/program_direction_dense.md`
  - repo-local dense direction instructions
- `auto_research/program_reversal_dense.md`
  - repo-local dense reversal instructions
- `auto_research/run_one_experiment.sh`
  - run one formal experiment suite
- `auto_research/run_one_experiment_background.sh`
  - launch one formal experiment suite in the background and return immediately
- `auto_research/experiment_queue.py`
  - enqueue formal launches and repairs, inspect queue state, or run one queue-supervisor pass
- `auto_research/experiment_queue_supervisor.sh`
  - keep up to four live formal experiments running from the repo-local queue
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
- `auto_research/status_autorun.sh`
  - inspect background status plus incomplete experiment runs
- `auto_research/status_dense_autorun.sh`
  - inspect both dense background instances

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
- `CODEX_SECONDARY_BASE_URL=...`
  - background-only second provider endpoint, intended for a secondary Nimabo key before the normal fallback provider
- `CODEX_SECONDARY_API_KEY=...`
  - matching API key for the secondary provider endpoint
- `FALLBACK_ENV_PATH=/custom/path/codex-fallback.env`
  - load background-only fallback provider settings from a local env file
- `CODEX_FALLBACK_BASE_URL=...`
  - backup responses endpoint used only when the primary background provider fails transiently
- `CODEX_FALLBACK_API_KEY=...`
  - matching API key for the backup endpoint
- `MAX_CONSECUTIVE_FAILURES=5`
  - change the background stop threshold

Background retry order:

- primary Nimabo provider from the normal Codex home
- secondary Nimabo key from `CODEX_SECONDARY_BASE_URL` + `CODEX_SECONDARY_API_KEY`
- `ai.changyou.club` from `CODEX_FALLBACK_BASE_URL` + `CODEX_FALLBACK_API_KEY`
- official login fallback from `CODEX_OFFICIAL_AUTH_PATH`

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
