# distributed-cc

Distribute Claude Code sessions across multiple servers from a single orchestrator.

```
┌─────────────────────────────────────────────┐
│  Orchestrator (your laptop)                 │
│                                             │
│  src/main.py ──→ CLI or Telegram frontend   │
│       │                                     │
│  Permission/Clarification HTTP server :9120 │
└──────┬──────────────────────────────────────┘
       │ SSH tunnels
       ├──────────────────────────────┐
       │                              │
┌──────▼──────────┐          ┌───────▼─────────┐
│  Server A       │          │  Server B       │
│  remote_broker  │          │  remote_broker  │
│  :8200          │          │  :8200          │
│                 │          │                 │
│  Claude Code    │          │  Claude Code    │
│  Agent SDK      │          │  Agent SDK      │
└─────────────────┘          └─────────────────┘
```

The orchestrator routes tasks to remote servers, each running a broker daemon that drives Claude Code via the Agent SDK. Permission requests and clarification questions are forwarded back to the orchestrator through SSH reverse tunnels.

## Quick Start

### 1. Orchestrator (main node)

```bash
# Clone and install
git clone https://github.com/dangxingyu/distributed-cc.git
cd distributed-cc
uv sync --extra dev --extra telegram

# Configure
cp config.example.yaml config.yaml
# Edit config.yaml with your servers, paths, and settings

# Run
make run              # CLI mode
make run-telegram     # Telegram bot mode
```

### 2. Remote Nodes (broker)

Three options to install the broker on each remote server:

**Option A — One-line installer** (easiest):
```bash
curl -fsSL https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools/install-broker.sh | bash
```

**Option B — Deploy script** (from orchestrator):
```bash
make deploy HOST=user@server-a NAME=server-a
```

**Option C — Manual**:
```bash
ssh user@server-a
mkdir -p ~/.distributed-cc
# Copy tools/remote_broker.py and tools/broker_session.py to ~/.distributed-cc/
curl -LsSf https://astral.sh/uv/install.sh | sh  # install uv if needed
uv venv ~/.distributed-cc/.venv
uv pip install --python ~/.distributed-cc/.venv/bin/python3 claude-agent-sdk aiohttp
```

Then start the broker daemon (once per server, e.g. in tmux):
```bash
~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/remote_broker.py --port 8200 --name server-a
```

Register sessions from each project directory:
```bash
cd /path/to/your/project
~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/broker_session.py start
# Optional: --name custom-name --desc "Description"
```

The broker heartbeats to the orchestrator, so sessions are discovered automatically.

### 3. SSH Tunnels

Each remote server needs two tunnels — one forward (orchestrator → broker) and one reverse (broker → orchestrator):

```bash
ssh -N \
    -L 8201:localhost:8200 \
    -R 9120:localhost:9120 \
    user@server-a
```

Or use the helper script:
```bash
make tunnels
```

Edit `tools/start_tunnels.sh` to configure your servers.

## Configuration

See `config.example.yaml` for all options. Key sections:

- **servers** — list of remote/local servers with broker ports (sessions register dynamically)
- **orchestrator** — model selection for routing and sessions
- **permission** — callback HTTP server port
- **telegram** — bot token and allowed user IDs (for Telegram mode)

Optionally, create a `config.md` alongside `config.yaml` for extra instructions
(server notes, rules, preferences). The orchestrator and permission evaluator
will read it for additional context. See `config.example.md` for an example.

## Testing

```bash
make test         # Unit/integration tests (no Claude calls)
make test-e2e     # End-to-end tests (costs money)
```

## Project Structure

```
src/
  main.py          — entry point, wires components together
  orchestrator.py  — routes tasks to sessions, handles callbacks
  session.py       — manages server connections and sessions
  store.py         — SQLite persistence
  permission.py    — evaluates permission requests
  cli.py           — terminal REPL frontend
  bot.py           — Telegram bot frontend
  formatter.py     — output formatting

tools/
  remote_broker.py   — broker daemon for remote servers
  broker_session.py  — CLI to register/unregister sessions with broker
  deploy.sh          — deploy broker via SSH/SCP
  install-broker.sh  — one-line remote installer
  start_tunnels.sh   — SSH tunnel helper

config.example.yaml  — example configuration (YAML)
config.example.md    — example extra instructions (optional)
```

## Requirements

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) for dependency management
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) installed and authenticated on each remote server
