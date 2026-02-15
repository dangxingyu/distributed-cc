# Extra Instructions

Optional notes for the orchestrator and permission evaluator.
Put anything here that doesn't fit neatly into `config.yaml` — the AI agents
will read this file alongside the YAML config for additional context.

## Server Notes

### server-a
- This server is behind a corporate VPN; SSH may be slow.
- The auth module uses PostgreSQL — never drop tables.
- Prefer running tests with `make test-unit` (fast) over `make test` (includes integration).

### local
- Used for quick prototyping only; don't push commits from here.

## General Rules
- Always run `make lint` before committing.
- The `main` branch is protected — work on feature branches.
- Ask before installing new pip packages.
