# Research Automation

This repository keeps the experiment control plane inside the repo. The external decision-maker is `codex`.

Main entrypoints:

- `program.md`
  - repo-local research instructions for each Codex cycle
- `scripts/research/run_one_experiment.sh`
  - run one formal experiment suite
- `scripts/research/run_one_experiment_background.sh`
  - launch one formal experiment suite in the background and return immediately
- `scripts/research/experiment_queue.py`
  - enqueue formal launches and repairs, inspect queue state, or run one queue-supervisor pass
- `scripts/research/experiment_queue_supervisor.sh`
  - keep up to three live formal experiments running from the repo-local queue
- `scripts/research/summarize_experiment.py`
  - extract a compact JSON summary from a finished experiment run
- `scripts/research/update_session.py`
  - append one cycle into the active session files
- `scripts/research/codex_background_loop.sh`
  - start, stop, restart, or run one background Codex cycle
- `scripts/research/status_autorun.sh`
  - inspect background status plus incomplete experiment runs

Default session resolution:

- `codex_background_loop.sh` now resolves the session directory from the active program file
- if `SESSION_DIR` is unset, it reads the first `Active session:` reference in `program.md`
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
./scripts/research/experiment_queue_supervisor.sh start
./scripts/research/codex_background_loop.sh start
./scripts/research/status_autorun.sh
./scripts/research/codex_background_loop.sh stop
./scripts/research/experiment_queue_supervisor.sh stop
```

Queue one formal follow-up instead of launching it immediately:

```bash
./scripts/research/experiment_queue.py enqueue \
  --market btc \
  --suite baseline_focus_feature_search_btc_reversal_40v6_bias60_2usd_5max_20260413 \
  --run-label auto_btc_40v6_bias60_2usd_5max_20260413 \
  --action launch \
  --reason "queue from codex cycle"
```

Long-lived formal worker launch:

```bash
./scripts/research/run_one_experiment_background.sh \
  --suite baseline_focus_feature_search_btc_reversal_40v4_novolweight_2usd_5max_20260413 \
  --run-label auto_btc_40v4_novolweight_2usd_5max_r1_20260413 \
  --market btc
```

Legacy note:

- `bootstrap_keepalive.sh` is now legacy-only for older fixed bootstrap lines.
- Dynamic autoresearch occupancy should use the queue supervisor instead.
