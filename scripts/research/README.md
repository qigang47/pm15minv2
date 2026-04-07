# Research Automation

This repository keeps the experiment control plane inside the repo. The external decision-maker is `codex`.

Main entrypoints:

- `program.md`
  - repo-local research instructions for each Codex cycle
- `scripts/research/run_one_experiment.sh`
  - run one formal experiment suite
- `scripts/research/summarize_experiment.py`
  - extract a compact JSON summary from a finished experiment run
- `scripts/research/update_session.py`
  - append one cycle into the active session files
- `scripts/research/codex_background_loop.sh`
  - start, stop, restart, or run one background Codex cycle
- `scripts/research/status_autorun.sh`
  - inspect background status plus incomplete experiment runs

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
- `MAX_CONSECUTIVE_FAILURES=5`
  - change the background stop threshold

Typical commands:

```bash
./scripts/research/codex_background_loop.sh start
./scripts/research/status_autorun.sh
./scripts/research/codex_background_loop.sh stop
```
