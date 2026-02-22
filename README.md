# Distributed Claude Code

Run Claude Code across multiple machines from one local web chat.

It gives you:
- One chat UI on your laptop (`http://localhost:8080`)
- One daemon per server (`:8200`)
- Two agent roles per task on each daemon:
  - **Orchestrator** — PhD-student-level researcher: plans, investigates, reviews, decides
  - **Worker** — implementation agent: executes assignments with full tool access

The orchestrator maintains two persistent artifacts per project:
- `task_list.md` — research plan (overwritten on update)
- `LOG.md` — lab notebook (append-only record of hypotheses, findings, decisions)

## What You Type vs What Happens

```text
You (web UI) -> Router (local) -> Daemon (remote)
                               -> Orchestrator (plans, uses MCP tools)
                               -> Worker assignment via assign_worker tool
                               -> Worker report via submit_report tool
                               -> Repeat until task_complete called
```

The UI shows:
- Chat panel: orchestrator/worker exchanges and status updates
- Monitor panel: tool calls, logs, iteration progress, and task list

## Quick Start (10 Minutes)

### 1. Install locally

```bash
git clone https://github.com/dangxingyu/distributed-cc.git
cd distributed-cc
uv sync --extra dev
```

### 2. Start router + web UI

```bash
make run
```

Open `http://localhost:8080`.

### 3. Set up a server from the UI

In chat, run:

```text
/setup user@your-server
```

The setup agent will:
- SSH into the server
- install daemon dependencies
- update local `config.json`
- tell you tunnel commands

Use follow-up messages to provide context (conda path, project path, cluster constraints).

### 4. Open SSH tunnel(s)

One tunnel command per server:

```bash
ssh -N -L 8201:localhost:8200 -R 9120:localhost:9120 user@your-server
```

- `-L` exposes remote daemon locally
- `-R` allows daemon progress callbacks back to your laptop

### 5. Connect a channel and run a task

```text
/connect my-project
Investigate why training loss plateaus; maybe reward hacking.
```

## Core Commands

| Command | Purpose |
|---|---|
| `/connect <project-id>` | Connect current channel to a project |
| `/connect` | Show current connection |
| `/status` | Show daemon/task status |
| `/stop` | Stop current running task (router or orchestrator) |
| `/setup <user@host>` | Interactive server setup via router |
| `/setup` | Health check configured servers |

### Mentions (always work)

| You send | Behavior |
|---|---|
| `@router <msg>` | Direct message to the router (sysadmin) |
| `@orchestrator <msg>` | Direct message to orchestrator (interrupt if running) |

### Routing without mentions

| Channel state | Plain message goes to |
|---|---|
| No project connected | Router (sysadmin brain) |
| Project connected, idle | Orchestrator (starts new task) |
| Project connected, running | Queued as next task |

## Configuration

The router reads `config.json` at startup.

Current common format:

```json
{
  "servers": [
    {
      "name": "h100",
      "host": "ubuntu@host-or-ip",
      "work_dir": "/home/ubuntu/project",
      "broker_port": 8201
    },
    {
      "name": "local",
      "host": null,
      "work_dir": "/Users/you/project",
      "broker_port": 8200
    }
  ],
  "orchestrator": {
    "model": "claude-opus-4-6"
  }
}
```

Field meanings:
- `name`: project ID used by `/connect`
- `host`: SSH destination, or `null` for local
- `work_dir`: project directory on that machine
- `broker_port`: local forwarded port that maps to remote `:8200`

Start from:

```bash
cp config.example.json config.json
```

## Manual Setup (If You Skip `/setup`)

On remote server:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
mkdir -p ~/.distributed-cc
scp tools/orchestrator_daemon.py user@server:~/.distributed-cc/orchestrator_daemon.py
ssh user@server "cd ~/.distributed-cc && uv venv .venv && uv pip install --python .venv/bin/python3 claude-agent-sdk aiohttp"
ssh user@server "tmux new-session -d -s daemon '~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/orchestrator_daemon.py --port 8200 --name server-a --callback-url http://127.0.0.1:9120'"
```

From laptop:

```bash
ssh -N -L 8201:localhost:8200 -R 9120:localhost:9120 user@server
curl http://127.0.0.1:8201/health
```

Prerequisite on each remote machine:
- Claude Code CLI installed and authenticated

## Troubleshooting

### Router cannot reach daemon
- Confirm tunnel is active:
  - `curl http://127.0.0.1:<broker_port>/health`
- Check daemon process on server:
  - `tmux ls`
  - daemon running with `~/.distributed-cc/.venv/bin/python3`

### No progress events in UI
- Check reverse tunnel (`-R 9120:localhost:9120`) is present
- Verify local callback server is up (router running)

### `/connect <id>` says unknown project
- Confirm `config.json` has matching `servers[].name`
- Restart router after config changes

## Testing

```bash
make test      # unit/integration tests (no paid API calls)
make test-e2e  # real Claude API calls (costs money)
```

## Project Layout

```text
src/
  main.py            entrypoint (router + web + callback server)
  router.py          command routing, daemon IO, status, queueing
  router_session.py  local sysadmin Claude session (@router, /setup)
  web.py             HTTP/WebSocket chat backend
  store.py           JSON persistence
  static/
    index.html       frontend

tools/
  orchestrator_daemon.py  remote daemon (orchestrator + worker MCP tools)
  deploy.sh               helper to copy/install daemon remotely
  start_tunnels.sh        helper for SSH tunnels

docs/
  message-flow.md       event routing and message classification
  broker-guide.md       daemon deployment and operations
  design-philosophy.md  architecture rationale
```

## Requirements

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) on each remote server
