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

Each project works like a Slack channel: the user talks to the orchestrator, the orchestrator assigns work to remote Claude Code workers, workers report back, and the orchestrator evaluates results — all visible in one conversation stream. The orchestrator maintains a single persistent Claude session per chat that accumulates context across tasks.

Remote servers each run a broker daemon that drives Claude Code via the Agent SDK. Permission requests and clarification questions are forwarded back to the orchestrator through SSH reverse tunnels.

## Message Routing

Messages are routed based on prefix. This lets you interact with a busy orchestrator without blocking.

### CLI mode

In CLI mode, unprefixed messages go directly to the orchestrator (single-user ergonomics). You can still use the `@orchestrator` prefix, but it's optional.

```
you> fix the auth bug in login.py          # → direct to orchestrator
you> @orchestrator check the test results  # → same thing, explicit prefix
you> @orchestrator /stop                   # → cancel all running worker tasks
```

### Telegram mode

In Telegram, unprefixed messages become **channel notes** — ambient observations stored and auto-injected into the orchestrator's next interaction. Use `@orchestrator` to send direct messages.

```
the CI is failing on server-b              # → stored as channel note
@orchestrator deploy the fix to staging    # → direct message to orchestrator
@orchestrator /stop                        # → cancel all running worker tasks
```

### How it works

| Input | Behavior |
|---|---|
| `@orchestrator <text>` | Direct message. Queued non-blocking if orchestrator is busy (you get a "(queued)" ack). |
| `@orchestrator /stop` | Cancels all running worker tasks for the channel. |
| `<text>` (CLI) | Direct message (CLI defaults to direct mode). |
| `<text>` (Telegram) | Channel note — stored and prepended as `[CHANNEL NOTES]` to the next orchestrator message. You get a "(noted)" ack. |

**Channel notes** are useful for leaving context while the orchestrator is busy evaluating a worker result. For example, "I noticed the linting config uses tabs not spaces" gets picked up on the next orchestrator interaction without interrupting current work.

**Non-blocking queue**: When the orchestrator session lock is held (e.g., evaluating a worker result), direct messages are queued and processed in order once the lock is released. No messages are lost.

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
- **orchestrator** — model selection, tool permission config (auto-approve, deny, escalate-to-human)
- **http** — callback HTTP server port for broker permission/clarification requests
- **telegram** — bot token and allowed user IDs (for Telegram mode)
- **data** — directory for JSON persistence files

Optionally, create a `config.md` alongside `config.yaml` for extra instructions
(server notes, rules, preferences). The orchestrator reads it for additional context.
See `config.example.md` for an example.

## Testing

```bash
make test         # Unit/integration tests (no Claude calls)
make test-e2e     # End-to-end tests (costs money)
```

## Project Structure

```
src/
  main.py          — entry point, wires components together
  orchestrator.py  — message routing, persistent Claude session per chat, task evaluation
  session.py       — manages server connections and remote broker sessions
  store.py         — JSON file persistence (messages, tasks, workers, notes)
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
