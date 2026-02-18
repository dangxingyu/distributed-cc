# distributed-cc

Distribute Claude Code sessions across multiple servers from a single orchestrator.

```
┌─────────────────────────────────────────────┐
│  Orchestrator (your laptop)                 │
│                                             │
│  src/main.py ──→ Web, CLI, or Telegram       │
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

### Web mode

In web mode (`make run-web`), the chat runs at `http://localhost:8080` over WebSocket. Behaves like CLI mode — unprefixed messages go directly to the orchestrator. Orchestrator replies, worker dispatches, and task results stream back in real-time. Permission and clarification escalations appear as inline cards with action buttons.

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

## Setup

There are three components to get running: the **broker** on each remote server, **SSH tunnels** connecting them, and the **orchestrator** on your laptop.

```
Your laptop                          Remote server (e.g. server-a)
──────────                           ─────────────────────────────
Orchestrator (:9120 callback)        Broker daemon (:8200)
     │                                    │
     │  SSH tunnel                        │  Claude Code Agent SDK
     │  -L 8201:localhost:8200  ──────►   │  runs tasks in project dirs
     │  -R 9120:localhost:9120  ◄──────   │  callbacks for permissions
     │                                    │
Web UI (:8080) / CLI / Telegram      Session: /path/to/project
```

### Step 1: Install the broker on each remote server

The broker is a lightweight daemon that receives tasks from the orchestrator and runs Claude Code via the Agent SDK. Install it once per server.

**Option A — One-line installer** (on the remote server):
```bash
curl -fsSL https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools/install-broker.sh | bash
```

**Option B — Deploy from your laptop**:
```bash
make deploy HOST=user@server-a NAME=server-a
```

Both options install to `~/.distributed-cc/` on the remote, set up a venv, and install dependencies (`claude-agent-sdk`, `aiohttp`).

**Prerequisite**: [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) must be installed and authenticated on each remote server (`claude` command available and logged in).

### Step 2: Start the broker daemon

On the remote server, start the broker in a persistent session (tmux/screen):

```bash
# In tmux on server-a:
~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/remote_broker.py \
    --port 8200 --name server-a
```

The broker listens on `:8200` and manages Claude Code sessions. One broker per server — it handles multiple projects/sessions.

### Step 3: Open SSH tunnels

Each remote server needs two tunnels from your laptop:

- **Forward tunnel** (`-L`): lets the orchestrator send tasks to the remote broker
- **Reverse tunnel** (`-R`): lets the remote broker send permission/clarification callbacks back to the orchestrator

```bash
# One command per server (run from your laptop, e.g. in a tmux pane):
ssh -N \
    -L 8201:localhost:8200 \
    -R 9120:localhost:9120 \
    user@server-a
```

For multiple servers, use different local ports (`-L 8201`, `-L 8202`, etc.). The reverse tunnel port (`9120`) is the same for all servers — it's the orchestrator's callback port.

**Helper script**: Edit `tools/start_tunnels.sh` with your servers, then run:
```bash
make tunnels   # starts all tunnels in background, Ctrl+C to stop
```

**Local broker** (same machine): No tunnels needed. The orchestrator connects directly to `localhost:8200`.

### Step 4: Configure the orchestrator

```bash
git clone https://github.com/dangxingyu/distributed-cc.git
cd distributed-cc
uv sync --extra dev --extra telegram

cp config.example.yaml config.yaml
```

Edit `config.yaml` — the key section is `servers`, where each entry's `broker_port` must match the local side of your SSH tunnel:

```yaml
servers:
  - name: server-a
    host: user@server-a.example.com
    broker_port: 8201       # matches: ssh -L 8201:localhost:8200

  - name: server-b
    host: user@server-b.example.com
    broker_port: 8202       # matches: ssh -L 8202:localhost:8200

  - name: local
    host: null              # no SSH needed
    broker_port: 8200       # direct connection
```

### Step 5: Start the orchestrator

```bash
make run-web          # Web chat at localhost:8080
make run              # CLI mode (terminal REPL)
make run-telegram     # Telegram bot mode
```

The orchestrator starts a callback HTTP server on `:9120` (for broker permission requests) and connects to brokers via the configured ports.

### Step 6: Register project sessions (optional)

Workers are created dynamically by the orchestrator when you ask it to do work — it calls the broker's `/register` endpoint automatically. You can also pre-register sessions manually on the remote server:

```bash
# On server-a, from a project directory:
cd /path/to/your/project
~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/broker_session.py start
# Optional: --name custom-name --desc "Training run"
```

The broker heartbeats registered sessions to the orchestrator, so they appear automatically.

### Startup order summary

1. **Broker** on each remote server (persistent — start once)
2. **SSH tunnels** from your laptop (persistent — start once per server)
3. **Orchestrator** on your laptop (start per work session)

The broker and tunnels can be left running indefinitely. Only the orchestrator needs restarting between sessions.

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
  models.py        — data models (WorkItem, WorkPlan)
  web.py           — web chat frontend (localhost:8080)
  cli.py           — terminal REPL frontend
  bot.py           — Telegram bot frontend
  formatter.py     — output formatting
  static/
    index.html     — single-page chat UI

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
