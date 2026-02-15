# Broker Operations Guide

This document describes how to deploy, configure, launch, and maintain
the remote broker (`tools/remote_broker.py`) on each server. The setup
agent should follow these steps, adapting paths and environment variables
based on the per-server notes in `setup.md`.

---

## 1. Prerequisites on Remote Server

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (auto-installed by deploy scripts if missing)
- Claude Code CLI installed and authenticated (the Agent SDK uses its auth)
- SSH access from the local orchestrator machine

## 2. Deploy Broker Script

Copy the broker to the remote. Default location: `~/.distributed-cc/remote_broker.py`.

```bash
ssh {host} "mkdir -p {broker_dir}"
scp tools/remote_broker.py {host}:{broker_dir}/remote_broker.py
```

If `setup.md` specifies a non-default `broker_dir` (e.g., on NFS), use that path instead.

## 3. Install Dependencies

The broker uses `uv` for fast, reliable dependency management. Install uv first
(if not already present), then create a self-contained venv.

```bash
ssh {host} "command -v uv >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/install.sh | sh"
ssh {host} "uv venv {broker_dir}/.venv && uv pip install --python {broker_dir}/.venv/bin/python3 claude-agent-sdk aiohttp"
```

Always use `{broker_dir}/.venv/bin/python3` to run the broker.

## 4. Environment Configuration

Before launching the broker, the following environment variables may need to be set.
Check `setup.md` for per-server values.

| Variable | Purpose | Example |
|---|---|---|
| `CLAUDE_CONFIG_DIR` | Claude Code config/auth location | `/nfs/shared/.claude` |
| `CLAUDE_CACHE_DIR` | Cache directory (models, etc.) | `/nfs/shared/.claude/cache` |
| `ORCH_URL` | Orchestrator callback URL | `http://127.0.0.1:9120` |
| `SERVER_NAME` | This server's name in config.yaml | `server-a` |
| `PATH` | Ensure correct Python/Node/etc. | Prepend NFS tool paths |

If `setup.md` specifies a custom bashrc or env file, source it before launching:

```bash
source {env_file}
```

## 5. Launch Broker

Standard launch command:

```bash
cd {work_dir}
{broker_dir}/.venv/bin/python3 {broker_dir}/remote_broker.py \
    --port 8200 \
    --name {server_name} \
    --work-dir {work_dir}
```

### Making it persistent

Option A — **tmux** (simple, good for dev):
```bash
tmux new-session -d -s broker "\
    source {env_file} && \
    cd {work_dir} && \
    {broker_dir}/.venv/bin/python3 {broker_dir}/remote_broker.py \
        --port 8200 --name {server_name} --work-dir {work_dir}"
```

Option B — **systemd** (robust, auto-restart):
```ini
# /etc/systemd/system/cc-broker.service
[Unit]
Description=Claude Code Broker ({server_name})

[Service]
WorkingDirectory={work_dir}
ExecStart={broker_dir}/.venv/bin/python3 {broker_dir}/remote_broker.py --port 8200 --name {server_name} --work-dir {work_dir}
EnvironmentFile={env_file}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Option C — **nohup** (minimal):
```bash
source {env_file}
cd {work_dir}
nohup {broker_dir}/.venv/bin/python3 {broker_dir}/remote_broker.py \
    --port 8200 --name {server_name} --work-dir {work_dir} \
    > {log_dir}/{server_name}-broker.log 2>&1 &
echo $! > {broker_dir}/{server_name}.pid
```

## 6. Verify Broker is Running

From the local machine (after SSH tunnel is up):

```bash
curl http://127.0.0.1:{broker_port}/health
# Expected: {"status": "ok", "server": "{server_name}"}
```

From the remote machine directly:

```bash
curl http://127.0.0.1:8200/health
```

## 7. SSH Tunnel Setup

Each remote server needs two tunnels:

```bash
ssh -N \
    -L {local_broker_port}:localhost:8200 \
    -R 9120:localhost:9120 \
    -o ServerAliveInterval=30 \
    {host}
```

- `-L` lets the local orchestrator reach the remote broker
- `-R` lets the remote broker call back to the local orchestrator's permission/clarification endpoints

## 8. Stop Broker

tmux:
```bash
ssh {host} "tmux kill-session -t broker"
```

systemd:
```bash
ssh {host} "sudo systemctl stop cc-broker"
```

nohup/pid:
```bash
ssh {host} "kill \$(cat {broker_dir}/{server_name}.pid)"
```

## 9. Restart Broker

Same as stop + launch. Session state is not lost — Claude Code sessions
are persisted on disk and resumed via `--resume {session_id}` by the
Agent SDK.

## 10. Troubleshooting

| Symptom | Check |
|---|---|
| Broker unreachable | Is SSH tunnel up? `curl localhost:{broker_port}/health` |
| Permission callback fails | Is reverse tunnel up? From remote: `curl localhost:9120/health` |
| Agent SDK auth error | Is Claude Code authenticated on remote? Run `claude --version` |
| Wrong Python/packages | Is the broker running via `{broker_dir}/.venv/bin/python3`? |
| Session state missing | Check `CLAUDE_CONFIG_DIR` points to persistent storage |

## Template Variables

When executing commands from this guide, substitute these placeholders
with values from `config.yaml` and `setup.md`:

| Placeholder | Source |
|---|---|
| `{host}` | `config.yaml` → `servers[].host` |
| `{server_name}` | `config.yaml` → `servers[].name` |
| `{work_dir}` | `config.yaml` → `servers[].work_dir` |
| `{broker_port}` | `config.yaml` → `servers[].broker_port` |
| `{python_bin}` | `setup.md` → Python path for uv (default: auto-detected by uv) |
| `{broker_dir}` | `setup.md` → broker install location (default: `~/.distributed-cc`) |
| `{env_file}` | `setup.md` → per-server env/bashrc to source |
| `{log_dir}` | `setup.md` → log directory |
