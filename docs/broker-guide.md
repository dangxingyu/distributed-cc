# Daemon Operations Guide

This document describes how to deploy, configure, launch, and maintain
the orchestrator daemon (`tools/orchestrator_daemon.py`) on each server.

---

## 1. Prerequisites on Remote Server

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) (auto-installed by deploy scripts if missing)
- Claude Code CLI and/or Codex CLI installed and authenticated, depending on the configured provider
- SSH access from the local router machine

## 2. Deploy Daemon Script

Copy the daemon to the remote. Default location: `~/.distributed-cc/`.

```bash
ssh {host} "mkdir -p {daemon_dir}/dcc_runtime"
scp tools/orchestrator_daemon.py {host}:{daemon_dir}/orchestrator_daemon.py
scp tools/dcc_runtime/*.py {host}:{daemon_dir}/dcc_runtime/
```

Or use the deploy script from your laptop:
```bash
make deploy HOST={host} NAME={server_name}
```

## 3. Install Dependencies

The daemon uses `uv` for fast, reliable dependency management.

```bash
ssh {host} "command -v uv >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/install.sh | sh"
ssh {host} "uv venv {daemon_dir}/.venv && uv pip install --python {daemon_dir}/.venv/bin/python3 claude-agent-sdk aiohttp mcp"
```

Always use `{daemon_dir}/.venv/bin/python3` to run the daemon.

## 4. Environment Configuration

Before launching the daemon, the following environment variables may need to be set.

| Variable | Purpose | Example |
|---|---|---|
| `CLAUDE_CONFIG_DIR` | Claude Code config/auth location | `/nfs/shared/.claude` |
| `CLAUDE_CACHE_DIR` | Claude cache directory (models, etc.) | `/nfs/shared/.claude/cache` |
| `DCC_PROVIDER` | Default provider when `/task` payload omits it | `claude` or `codex` |
| `DCC_CODEX_SANDBOX_MODE` | Default Codex sandbox mode | `workspace-write` |
| `DCC_CODEX_APPROVAL_POLICY` | Default Codex approval policy | `never` |
| `DAEMON_NAME` | This daemon's name (also set via `--name`) | `server-a` |
| `CALLBACK_URL` | Router callback URL (also set via `--callback-url`) | `http://127.0.0.1:9120` |
| `PATH` | Ensure correct Python/Node/etc. | Prepend NFS tool paths |

Provider notes:
- Claude runtime uses `claude-agent-sdk` and the host Claude CLI auth/config.
- Codex runtime launches `codex app-server --listen ws://127.0.0.1:<port>` and bridges daemon tools through a local FastMCP HTTP endpoint.
- By default the daemon generates a stable `CODEX_HOME` under `~/.distributed-cc/codex/{project_id}/{orchestrator|worker}` containing `config.toml`, `instructions/`, and a linked or copied `auth.json`.

## 5. Launch Daemon

The daemon is a per-server process — start it once per server:

```bash
{daemon_dir}/.venv/bin/python3 {daemon_dir}/orchestrator_daemon.py \
    --port 8200 \
    --name {server_name} \
    --callback-url http://127.0.0.1:9120
```

The daemon runs a split-channel architecture when tasks are submitted:
- **Orchestrator**: plans, investigates, reviews worker reports, decides next step.
  Uses MCP tools: `assign_worker`, `task_complete`, `ask_user`, `update_task_list`, `append_log`, `update_worker_config`.
- **Worker**: executes assignments with full tool access (Read, Write, Bash, Grep, etc.).
  Submits results via `submit_report` MCP tool.

Execution backends:
- `provider=claude`: uses `claude-agent-sdk` with persisted SDK session IDs.
- `provider=codex`: uses Codex app-server with persisted thread IDs plus a local FastMCP bridge for daemon tools.

When calling `/task`, the payload may include provider-aware runtime fields:
- `provider`
- `model`
- `session_model`
- `permission_mode`
- `sandbox_mode`
- `approval_policy`

The orchestrator maintains two persistent files per project:
- `task_list.md` — research plan (overwritten on each update)
- `LOG.md` — lab notebook (append-only, timestamped record of investigation)

### HTTP API

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/register` | POST | Register a project (project_id, project_dir, name) |
| `/task` | POST | Start autonomous loop (supports `max_iterations`, default `0` = no cap, and `continuous_mode`) |
| `/interrupt` | POST | Inject user message (supports `urgency`: `normal`/`urgent`) |
| `/status` | GET | Current status (idle/running/done/stuck/error) |
| `/stream` | GET | SSE stream of progress events |
| `/stop` | POST | Stop current task |
| `/health` | GET | Health check |

### Making daemon persistent

Option A — **tmux** (simple, good for dev):
```bash
tmux new-session -d -s daemon "\
    source {env_file} && \
    {daemon_dir}/.venv/bin/python3 {daemon_dir}/orchestrator_daemon.py \
        --port 8200 --name {server_name} --callback-url http://127.0.0.1:9120"
```

Option B — **systemd** (robust, auto-restart):
```ini
# /etc/systemd/system/cc-daemon.service
[Unit]
Description=Distributed CC Orchestrator Daemon ({server_name})

[Service]
ExecStart={daemon_dir}/.venv/bin/python3 {daemon_dir}/orchestrator_daemon.py --port 8200 --name {server_name} --callback-url http://127.0.0.1:9120
EnvironmentFile={env_file}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Option C — **nohup** (minimal):
```bash
source {env_file}
nohup {daemon_dir}/.venv/bin/python3 {daemon_dir}/orchestrator_daemon.py \
    --port 8200 --name {server_name} --callback-url http://127.0.0.1:9120 \
    > {log_dir}/{server_name}-daemon.log 2>&1 &
echo $! > {daemon_dir}/{server_name}.pid
```

## 6. Verify Daemon is Running

From the local machine (after SSH tunnel is up):

```bash
curl http://127.0.0.1:{local_port}/health
# Expected: {"status": "ok", "daemon": "{server_name}", "projects": [...]}
```

From the remote machine directly:

```bash
curl http://127.0.0.1:8200/health
```

## 7. SSH Tunnel Setup

Each remote server needs two tunnels:

```bash
ssh -N \
    -L {local_port}:localhost:8200 \
    -R 9120:localhost:9120 \
    -o ServerAliveInterval=30 \
    {host}
```

- `-L` lets the local router reach the remote daemon
- `-R` lets the remote daemon call back to the local router's permission/progress endpoints

## 8. Stop Daemon

tmux:
```bash
ssh {host} "tmux kill-session -t daemon"
```

systemd:
```bash
ssh {host} "sudo systemctl stop cc-daemon"
```

nohup/pid:
```bash
ssh {host} "kill \$(cat {daemon_dir}/{server_name}.pid)"
```

## 9. State Persistence

Task state is persisted to `~/.distributed-cc/state/{project_id}.json` at each
iteration boundary. This includes task ID, iteration count, orchestrator/worker
session IDs, status, and summary. On daemon restart, the state file is available
for inspection but tasks are not auto-resumed (the user decides via the web UI).

Runtime sessions are persisted by the underlying provider and can be resumed across
daemon restarts:
- Claude resumes via SDK session IDs.
- Codex resumes via app-server thread IDs.

## 10. Troubleshooting

| Symptom | Check |
|---|---|
| Daemon unreachable | Is SSH tunnel up? `curl localhost:{local_port}/health` |
| Permission callback fails | Is reverse tunnel up? From remote: `curl localhost:9120/health` |
| Claude auth error | Is Claude Code authenticated on remote? Run `claude --version` |
| Codex launch/auth error | Is Codex CLI installed and authenticated? Run `codex --version` and verify `codex app-server` works on remote |
| Wrong Python/packages | Is the daemon running via `{daemon_dir}/.venv/bin/python3`? |
| Task stuck at iteration 1 | Check daemon logs — the provider runtime may have crashed or auth may have expired |

## Template Variables

When executing commands from this guide, substitute these placeholders
with values from `config.json`:

| Placeholder | Source |
|---|---|
| `{host}` | `config.json` → `machines[].host` or legacy `servers[].host` |
| `{server_name}` | `config.json` → `machines[].name` or legacy `servers[].name` |
| `{local_port}` | `config.json` → `machines[].broker_port` or legacy `servers[].broker_port` |
| `{daemon_dir}` | Default: `~/.distributed-cc` |
| `{env_file}` | Per-server env/bashrc to source |
| `{log_dir}` | Log directory |
