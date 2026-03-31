# Repository Guidelines

## Project Structure & Module Organization
This repository uses a Python `src/` layout. Maintain source code in `src/pm15min/`, organized by domain: `core/`, `data/`, `research/`, `live/`, and `console/`. Keep tests in `tests/` and mirror the domain naming with files like `test_live_runner.py` or `test_research_backtest_fills.py`. Long-lived design docs live in `docs/`. Use `scripts/entrypoints/` for canonical shell wrappers and `scripts/research/` for one-off research automation.

Treat `data/`, `research/`, and `var/` as runtime or generated artifact roots. Do not hand-edit outputs there unless the task is explicitly about generated data layout.

## Build, Test, and Development Commands
- `PYTHONPATH=src python -m pm15min --help`: inspect the top-level CLI.
- `PYTHONPATH=src python -m pm15min layout --market sol --json`: verify canonical layout resolution.
- `PYTHONPATH=src python -m pm15min data show-layout --market sol --cycle 15m --surface live`: inspect data paths for a market.
- `PYTHONPATH=src python -m pm15min console serve --host 127.0.0.1 --port 8765`: run the local console UI.
- `PYTHONPATH=src pytest -q tests`: run the full test suite.
- `PYTHONPATH=src pytest -q tests/test_live_runner.py`: run a focused test module while iterating.

## Coding Style & Naming Conventions
Use 4-space indentation, Python 3.11+ syntax, and explicit type hints. Follow the existing module patterns: `layout.py` for path logic, `config.py` for structured config, `service.py` for domain APIs, `runtime.py` for loops/orchestration, and `*_parity.py` for live/offline consistency checks. Prefer `snake_case` for modules, functions, and test files. Keep imports domain-clean; architecture guards already enforce boundaries such as `data/` not importing `research/`.

## Testing Guidelines
Write `pytest` tests as `test_*.py` under `tests/`. Add or update tests with every behavior change, especially around CLI wiring, parity logic, and persistence/layout rules. Favor targeted unit tests first, then run `PYTHONPATH=src pytest -q tests` before opening a PR.

## Commit & Pull Request Guidelines
Recent history mixes terse subjects like `更新` with clearer conventional messages such as `feat:` and `fix:`. Prefer a short imperative subject, optionally scoped with `feat:` or `fix:`, and explain behavior changes in the body when needed.

PRs should summarize the affected domain, list the commands/tests you ran, link any related issue or runbook context, and include screenshots only when changing the console HTTP/UI surface. Keep generated artifacts out of diffs unless they are the point of the change.
