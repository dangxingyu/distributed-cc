# Distributed Claude Code

<p align="center">
  <img src="docs/assets/distributed-cc-logo.jpeg" alt="Distributed Claude Code logo" width="720" />
</p>

*Work as an advisor!* Run Claude Code across multiple servers from one local chat interface (Web UI or Telegram), with a persistent **PhDLoop** runtime (Orchestrator + Worker).

<p align="center">
  <img src="docs/assets/illustration.jpeg" alt="System structure, binding model, and runtime flow" width="920" />
</p>

## Naming

- **AdvisorLoop**: the full workflow including user + router + remote runtime.
- **PhDLoop**: the autonomous per-project execution loop (orchestrator + worker).

## Why This Exists

Distributed Claude Code is optimized for this workflow:

1. Humans should not manually track 10+ concurrent sessions.
2. The orchestrator should keep progress moving autonomously, while the user gives high-level advice and course corrections.

Core model:

- **Router** (local): message routing + setup/sysadmin helper.
- **Daemon** (remote per machine): long-lived orchestrator runtime.
- **Orchestrator** (remote): plans, delegates, verifies, updates project memory.
- **Worker** (remote): executes concrete assignments.

Persistent artifacts per project:

- `task_list.md`: current research/task plan
- `LOG.md`: lab notebook (decisions, findings, pivots)

## Quick Start (Web, Recommended)

### 1. Install

```bash
git clone https://github.com/dangxingyu/distributed-cc.git
cd distributed-cc
uv sync --extra dev
```

### 2. Run locally

```bash
make run
```

Open `http://localhost:8080`.

### 3. Create your first setup channel

Click **+ New Setup Channel** in the sidebar.

- Fill `Channel name`
- Fill `Server` as `user@host`
- Click `Create + setup`

The UI sends `/setup user@host` automatically.

### 4. Complete project setup

After machine setup finishes, continue with:

```text
/setup-project <workdir or instruction>
```

This step creates/updates one project entry and gives you `/connect <project-id>`.

### 5. Connect and run work

```text
/connect <project-id>
Investigate why training loss plateaus and propose a fix.
```

## Core Abstractions (Read First)

- **Server (machine)**: connectivity/runtime endpoint (`host` + `broker_port`) where a daemon runs.
- **Project**: one concrete workdir on a server, identified by `project_id`.
- **Channel**: one conversation thread in UI; a channel is connected to at most one project at a time.
- **Orchestrator/Worker sessions**: persistent Claude sessions owned by a project (not by a channel).

Binding rules:

- One server can host many projects.
- Many channels can point to the same project.
- If channels share a project, they observe the same orchestrator/worker runtime state.
- Router setup sessions are per-channel; execution sessions (orchestrator/worker) are per-project.

## Setup Model (Important)

### `/setup` = machine setup only

`/setup user@host` handles server infrastructure only:

- daemon deployment/start
- local tunnel wiring
- machine-level entry update in `config.json`

It **must not** set project workdirs or project CLAUDE memory.

### `/setup-project` = project setup only

`/setup-project ...` handles project configuration only:

- resolve concrete workdir
- create/update one project entry
- prepare project memory files in the workdir
- verify health on selected broker port

## Core Commands

| Command | Purpose |
|---|---|
| `/connect <project-id>` | Connect current channel to one project |
| `/connect` | Show current connection |
| `/status` | Show current project/daemon status |
| `/stop` | Stop current running router/orchestrator task |
| `/setup <user@host> [--manual-tunnel]` | Machine setup (daemon + tunnel) |
| `/setup` | Health-check configured machine endpoints |
| `/setup-project <workdir or instruction>` | Project setup on an existing machine |
| `/setup_project <workdir or instruction>` | Telegram-friendly alias |

Mentions:

- `@router ...`: direct router/sysadmin message
- `@orchestrator ...`: urgent message to running orchestrator

## Routing Behavior

Without mentions:

- No project connected: message goes to **Router**
- Project connected + idle: message starts new orchestrator task
- Project connected + running: message is queued as non-urgent next-task context

Urgency while running:

- Plain message: non-urgent guidance
- `@orchestrator ...`: urgent interruption

## Heartbeat Behavior

There are two heartbeat modes:

1. **Running heartbeat**: if progress stalls, daemon queues a nudge.
2. **Standby heartbeat**: while `done/idle`, daemon can wake orchestrator for lightweight triage only when meaningful signals exist (for example queued advisor messages or unchecked `task_list.md` items).

Defaults:

- `HEARTBEAT_INTERVAL_SECONDS=45`
- `HEARTBEAT_IDLE_SECONDS=180`
- `STANDBY_HEARTBEAT_SECONDS=1800` (~30 minutes)
- `STANDBY_WAKE_MAX_ITERATIONS=1`

## Configuration

Start from template:

```bash
cp config.example.json config.json
```

Common config shape:

```json
{
  "servers": [
    {
      "name": "proj-a",
      "host": "ubuntu@host-or-ip",
      "work_dir": "/home/ubuntu/project-a",
      "broker_port": 8201
    }
  ],
  "orchestrator": {
    "model": "claude-opus-4-6",
    "session_model": "claude-opus-4-6",
    "permission_mode": "bypassPermissions"
  }
}
```

Field notes:

- `servers[].name`: project id used by `/connect`
- `servers[].host`: SSH destination (`null` for local)
- `servers[].work_dir`: project root on target machine
- `servers[].broker_port`: local forwarded port to daemon `:8200`
- `orchestrator.*`: default daemon runtime model/policy

`config.json` is local-only. Never commit real hosts/tokens.

## Telegram Mode

Run:

```bash
TELEGRAM_BOT_TOKEN=<token> uv run python -m src --frontend telegram
```

Then add the bot to a group or DM it directly.

## Manual Daemon Setup (Optional)

Only needed if you do not use `/setup`.

Remote server:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
mkdir -p ~/.distributed-cc
scp tools/orchestrator_daemon.py user@server:~/.distributed-cc/orchestrator_daemon.py
ssh user@server "cd ~/.distributed-cc && uv venv .venv && uv pip install --python .venv/bin/python3 claude-agent-sdk aiohttp"
ssh user@server "tmux new-session -d -s daemon '~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/orchestrator_daemon.py --port 8200 --name server-a --callback-url http://127.0.0.1:9120'"
```

Local laptop:

```bash
ssh -N -L 8201:localhost:8200 -R 9120:localhost:9120 user@server
curl http://127.0.0.1:8201/health
```

## Troubleshooting

### Router cannot reach daemon

- Check tunnel health: `curl http://127.0.0.1:<broker_port>/health`
- Check daemon process remotely (`tmux ls`)

### No progress in UI

- Verify reverse tunnel `-R 9120:localhost:9120`
- Confirm router process is running locally

### `/connect <id>` unknown project

- Confirm `config.json` has that `projects[].project_id` (or `servers[].name` in legacy config)
- Restart router after config edits

## Testing

```bash
make test
make test-e2e
make test-e2e E2E_JOBS=4
```

## Project Layout

```text
src/
  main.py            entrypoint (router + frontend + callback server)
  router.py          routing, daemon IO, queueing
  router_session.py  local setup/sysadmin Claude session
  web.py             HTTP/WebSocket backend
  telegram_chat.py   Telegram backend
  store.py           JSON persistence
  static/index.html  Web UI

tools/
  orchestrator_daemon.py  remote daemon runtime
  deploy.sh               deploy helper
  start_tunnels.sh        tunnel helper

docs/
  design-philosophy.md
  message-flow.md
  broker-guide.md
  native-prompts.md
```

## Requirements

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)
- Claude Code CLI on remote machines
