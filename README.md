# Distributed Claude Code

Run Claude Code sessions across multiple servers from a single chat interface on your laptop.

```
You (laptop)                          Remote servers
┌──────────────────────┐
│  Web UI :8080        │     SSH      ┌──────────────────────┐
│  ┌────────────────┐  │◄────────────►│  Server A            │
│  │  Router        │  │              │  Daemon :8200        │
│  │  /setup        │  │     SSH      │  (autonomous agent)  │
│  │  /connect      │  │◄────────────►├──────────────────────┤
│  │  @orchestrator │  │              │  Server B            │
│  └────────────────┘  │              │  Daemon :8200        │
└──────────────────────┘              └──────────────────────┘
```

Send a task, and an autonomous agent on the remote server works on it — reading files, writing code, running tests — streaming progress back in real-time. Think of it as having PhD students on different machines that you supervise from one place.

## Quick Start

### 1. Install

```bash
git clone https://github.com/dangxingyu/distributed-cc.git
cd distributed-cc
uv sync --extra dev
```

### 2. Start the router

```bash
make run
# Open http://localhost:8080
```

This starts the router and web UI on your laptop. No daemons need to be running yet — the router connects to daemons lazily when you first interact with a project.

### 3. Set up a remote server

The easiest way is the built-in `/setup` command. In the web UI, type:

```
/setup user@your-server.example.com
```

The sysadmin session (a local Claude Code agent) will:
- SSH into the server and probe the environment (OS, Python, GPUs, etc.)
- Install the daemon and dependencies
- Update your local `config.json`
- Tell you what SSH tunnel command to run

Type follow-up messages to give it context (e.g. "this machine uses conda", "the project is in /data/my-repo"). Type `/done` when finished.

Or set things up manually — see [Manual Setup](#manual-setup) below.

### 4. Open SSH tunnels

Each remote server needs two tunnels (one command per server):

```bash
ssh -N -L 8201:localhost:8200 -R 9120:localhost:9120 user@your-server
```

- `-L` lets your laptop reach the remote daemon
- `-R` lets the daemon stream progress back to you

Use different local ports for each server (`8201`, `8202`, etc.).

### 5. Start working

Create a channel, connect it to a project, and send your task:

```
/connect my-project
Investigate why the training loss plateaus — might be reward hacking
```

The remote agent starts working autonomously. You'll see tool calls, text output, and iteration progress streaming in real-time via the monitor panel.

## Commands and messaging

| Command | Description |
|---------|-------------|
| `/connect <project-id>` | Link this channel to a remote project |
| `/connect` | Show current connection and available projects |
| `/status` | Show current task status, iteration count, and queued tasks |
| `/stop` | Stop the running task |
| `/setup <user@host>` | Deploy a daemon to a new server (interactive) |
| `/setup` | Health-check all configured servers |
| `/done` | Exit setup mode |

### Messaging while a task is running

| What you type | What happens |
|---------------|-------------|
| Regular message | Queued as the next task (starts after current one finishes) |
| `@orchestrator <message>` | Urgent interrupt — injected at the next iteration boundary |
| `@orchestrator /stop` | Commands also work with the `@orchestrator` prefix |

## Configuration

The router reads `config.json` on startup to know about available servers. Daemons are contacted lazily — only when you first interact with a project.

```json
{
  "servers": [
    {
      "name": "h100",
      "host": "ubuntu@209.20.157.135",
      "work_dir": "/home/ubuntu",
      "broker_port": 8201
    },
    {
      "name": "local",
      "host": null,
      "work_dir": "/path/to/project",
      "broker_port": 8200
    }
  ],
  "orchestrator": {
    "model": "claude-opus-4-6"
  }
}
```

- **name** — project identifier (used with `/connect`)
- **host** — SSH destination (`user@host`), or `null` for local
- **work_dir** — working directory on the remote server
- **broker_port** — local port for the SSH tunnel (must match your `-L` flag)

Copy `config.example.json` to get started: `cp config.example.json config.json`

## Manual Setup

If you prefer to set up servers yourself instead of using `/setup`:

**On the remote server:**

```bash
# Install uv (if missing)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Deploy the daemon
mkdir -p ~/.distributed-cc
# Copy orchestrator_daemon.py to the server (from your laptop):
# scp tools/orchestrator_daemon.py user@server:~/.distributed-cc/

# Install dependencies
cd ~/.distributed-cc
uv venv .venv
uv pip install --python .venv/bin/python3 claude-agent-sdk aiohttp

# Start the daemon (in tmux for persistence)
tmux new-session -d -s daemon \
  '.venv/bin/python3 orchestrator_daemon.py --port 8200 --name server-a --callback-url http://127.0.0.1:9120'
```

**Prerequisite**: [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) must be installed and authenticated on each remote server.

**Verify it's running** (after SSH tunnel is up):

```bash
curl http://127.0.0.1:8201/health
```

## Testing

```bash
make test         # Unit tests (114 tests, no API calls)
make test-e2e     # End-to-end tests (calls Claude, costs money)
```

## Project Structure

```
src/
  main.py         — entry point: Router + WebChat + callback server
  router.py       — routes messages to daemons, manages urgency/deferral
  setup.py        — sysadmin session for /setup (local Agent SDK)
  web.py          — multi-client web chat (HTTP + WebSocket)
  store.py        — JSON file persistence (messages, logs, channel-project mapping)
  static/
    index.html    — single-page chat UI

tools/
  orchestrator_daemon.py  — autonomous agent daemon (runs on remote servers)

docs/
  design-philosophy.md    — Professor → PhD Student → Claude Code model
  broker-guide.md         — detailed daemon operations guide
```

## Requirements

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) on each remote server
