# Repository Guidelines

## Project Structure & Module Organization
- `src/`: runtime code for the local router/web app (`main.py`, `router.py`, `web.py`, `router_session.py`, `store.py`).
- `src/static/index.html`: browser UI assets.
- `tests/`: pytest suite (`test_routing.py`, `test_web.py`, `test_store.py`, `test_e2e.py`, etc.).
- `tools/`: operational scripts for remote daemon deployment and tunnels (`deploy.sh`, `start_tunnels.sh`, `orchestrator_daemon.py`).
- `docs/`: design and operational docs (`message-flow.md`, `broker-guide.md`).
- `config.example.json`: baseline config template; copy to `config.json` for local use.

## Build, Test, and Development Commands
- `uv sync --extra dev`: install project + dev dependencies.
- `make run`: start router + web UI (`http://127.0.0.1:8080`).
- `make test`: run unit/integration tests (excludes paid E2E test).
- `make test-e2e`: run real Claude API E2E flow (`tests/test_e2e.py`, costs money).
- `make deploy HOST=user@server NAME=my-server`: deploy daemon helper to a remote host.
- `make tunnels`: start SSH tunnel helper script.

## Coding Style & Naming Conventions
- Target Python `>=3.10`; use 4-space indentation and PEP 8 spacing.
- Prefer type hints for public functions and stateful attributes (see `src/router.py` patterns).
- Use `snake_case` for functions/variables/modules, `PascalCase` for classes, and descriptive test names like `test_route_running_non_mention_is_deferred`.
- No repo-level formatter/linter is currently enforced in `pyproject.toml`; keep imports tidy and formatting consistent with existing files.
- Keep modules focused: routing logic in `router.py`, persistence in `store.py`, HTTP/WebSocket handling in `web.py`.

## Testing Guidelines
- Framework: `pytest` with `pytest-asyncio` and `pytest-aiohttp`; async mode is configured in `pyproject.toml`.
- Place tests in `tests/test_*.py`; mirror runtime module behavior.
- Add or update tests with every behavior change, especially routing, channel mapping, and websocket flows.
- Run `make test` before opening a PR; run `make test-e2e` only when validating end-to-end orchestration behavior.

## Commit & Pull Request Guidelines
- Follow existing commit style: short imperative subject lines, e.g. `Fix stuck routing bug...`, `Add full-stack E2E test...`.
- Keep commits scoped to a single concern (routing, frontend, setup, docs, etc.).
- PRs should include a clear summary of behavior changes.
- Link the issue/task when available.
- Include test evidence (`make test` output; include E2E notes if run).
- Include UI screenshots or short recordings for `src/static/index.html` changes.

## Security & Configuration Tips
- Do not commit real credentials, tokens, or private hostnames in `config.json`.
- Treat `tests/test_e2e.py` and deployment scripts as production-adjacent: review target hosts, callback ports, and tunnel flags before running.
