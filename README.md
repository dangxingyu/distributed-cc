# distributed-cc

Distribute Claude Code sessions across multiple servers with an autonomous orchestrator.

```
┌─────────────────────────────────────┐
│  Laptop (Router)                    │
│  src/main.py → Web UI :8080        │
│  Callback HTTP :9120 (permissions)  │
└──────┬──────────────────────────────┘
       │ SSH tunnels (-L/-R)
       ├──────────────────────────────┐
       │                              │
┌──────▼──────────────────┐   ┌──────▼──────────────────┐
│  Server A               │   │  Server B               │
│  orchestrator_daemon.py │   │  orchestrator_daemon.py │
│  :8200                  │   │  :8201                  │
│                         │   │                         │
│  RALPH Loop             │   │  RALPH Loop             │
│  (Agent SDK → tools)    │   │  (Agent SDK → tools)    │
└─────────────────────────┘   └─────────────────────────┘
```

Each project maps to a Slack-like channel in the web UI. The user sends a task, the **router** relays it to a remote **orchestrator daemon**, which autonomously works on it via a RALPH loop (Reason → Act → Learn → Plan → Hypothesize). Progress streams back in real-time via SSE. User messages to a running task become interruptions injected at the next iteration boundary.

The orchestrator daemon uses the Claude Agent SDK directly — it reads files, writes code, runs tests, and evaluates results autonomously, like a PhD student working on a research task. It only pauses when genuinely stuck and needing user input.

## Usage

Open the **web app** at `http://localhost:8080`. Create a channel, connect it to a project with `/connect <project-id>`, and send your task.

| Action | What happens |
|--------|-------------|
| Send message (project idle) | Starts a new RALPH loop task on the daemon |
| Send message (task running) | Queued as interruption for next iteration |
| `/connect <project-id>` | Link channel to a remote project |
| `/stop` | Stop the running task |
| `/status` | Show current task status and iteration |

Progress events (tool calls, text output, iteration markers) stream into the monitor panel. Permission escalations for unknown tools appear as inline cards with approve/deny buttons.

## Setup

Three components: **daemon** on each remote server, **SSH tunnels**, and the **router** on your laptop.

### Step 1: Install the daemon on each remote server

The daemon is an autonomous agent that receives tasks and works on them independently.

**Option A — Deploy from your laptop**:
```bash
make deploy HOST=user@server-a NAME=server-a
```

**Option B — One-line installer** (on the remote server):
```bash
curl -fsSL https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools/install-broker.sh | bash
```

Both install to `~/.distributed-cc/` on the remote, set up a venv, and install dependencies (`claude-agent-sdk`, `aiohttp`).

**Prerequisite**: [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) must be installed and authenticated on each remote server.

### Step 2: Start the daemon

On the remote server, start the daemon in a persistent session (tmux/screen):

```bash
# In tmux on server-a:
~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/orchestrator_daemon.py \
    --port 8200 --name server-a --callback-url http://127.0.0.1:9120
```

The daemon listens on `:8200` and runs RALPH loop tasks autonomously. One daemon per server.

### Step 3: Open SSH tunnels

Each remote server needs two tunnels from your laptop:

- **Forward tunnel** (`-L`): lets the router send tasks to the remote daemon
- **Reverse tunnel** (`-R`): lets the remote daemon send permission escalations back

```bash
# One command per server (from your laptop):
ssh -N \
    -L 8201:localhost:8200 \
    -R 9120:localhost:9120 \
    user@server-a
```

For multiple servers, use different local ports (`-L 8201`, `-L 8202`, etc.).

### Step 4: Configure the router

```bash
git clone https://github.com/dangxingyu/distributed-cc.git
cd distributed-cc
uv sync --extra dev

cp config.example.json config.json
```

Edit `config.json` — the router reads this on startup to discover orchestrator daemons:

```json
{
  "orchestrators": [
    {
      "project_id": "my-project",
      "name": "server-a",
      "host": "user@server-a",
      "broker_port": 8201,
      "project_dir": "/path/to/project",
      "max_iterations": 20
    },
    {
      "project_id": "local-dev",
      "name": "local",
      "host": null,
      "broker_port": 8200,
      "project_dir": "/Users/me/project"
    }
  ]
}
```

### Step 5: Start the router

```bash
make run    # Web app at localhost:8080
```

The router starts the web UI on `:8080` and a callback HTTP server on `:9120` (for daemon permission escalations). It connects to each daemon, registers projects, and begins listening for SSE progress events.

### Startup order summary

1. **Daemon** on each remote server (persistent — start once)
2. **SSH tunnels** from your laptop (persistent — start once per server)
3. **Router** on your laptop (start per work session)

## Configuration

The router reads `config.json` from the working directory on startup. Key fields:

- **orchestrators** — list of remote/local daemons with project IDs, ports, and directories
- **broker_port** — local port for the SSH forward tunnel (must match `-L` flag)
- **max_iterations** — maximum RALPH loop iterations before auto-stopping (default: 20)

Optionally, create a `config.md` alongside `config.json` for extra instructions
(server notes, rules, preferences). The daemon reads it for additional context.
See `config.example.md` for an example.

## Testing

```bash
make test         # Unit/integration tests (no Claude calls, 70 tests)
make test-e2e     # End-to-end tests (costs money)
```

## Project Structure

```
src/
  main.py          — entry point, wires Router + WebChat + callback server
  router.py        — thin relay: routes messages, manages permissions, SSE listener
  web.py           — web chat frontend (localhost:8080)
  store.py         — JSON file persistence (messages, channels, notes, logs)
  session.py       — server config data model
  formatter.py     — output formatting utilities
  static/
    index.html     — single-page chat UI

tools/
  orchestrator_daemon.py  — autonomous RALPH loop daemon (deploys to remote servers)
  remote_broker.py        — legacy broker (kept for backward compat)
  deploy.sh               — deploy daemon via SSH/SCP
  install-broker.sh       — one-line remote installer
  start_tunnels.sh        — SSH tunnel helper

config.example.json  — example configuration
config.example.md    — example extra instructions (optional)
```

## Documentation

- [Design Philosophy](docs/design-philosophy.md) — Core design philosophy (Professor → PhD Student → Claude Code model)
- [Daemon Guide](docs/broker-guide.md) — Daemon deployment & operations

## Requirements

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) for dependency management
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) installed and authenticated on each remote server
