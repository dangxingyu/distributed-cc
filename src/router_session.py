from __future__ import annotations

"""RouterSession: local sysadmin Claude session for infrastructure management.

The router's local brain — a persistent Claude session that handles all
infrastructure tasks: deploying daemons, managing config, SSH operations,
health checks, and any other sysadmin work the user sends via @router.

Features:
  - SSH into servers, detect environments, install daemons
  - Manage local config.json and remote CLAUDE.md files
  - Read optional local config.md as user setup/environment notes
  - Health-check deployed daemons
  - General infrastructure tasks via @router messages
  - Persistent session with resume (context carries across follow-ups)
"""

import asyncio
import json
import logging
import os
from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage
from claude_agent_sdk.types import (
    AssistantMessage,
    SystemMessage,
    TextBlock,
    ToolUseBlock,
    ToolResultBlock,
)

log = logging.getLogger(__name__)


def _install_sdk_event_compat() -> None:
    """Treat unknown CLI *_event payloads as SystemMessage to avoid stream aborts."""
    try:
        import claude_agent_sdk._internal.client as sdk_client_internal
        import claude_agent_sdk._internal.message_parser as sdk_message_parser
    except Exception:
        return

    parse_message_fn = getattr(sdk_message_parser, "parse_message", None)
    if not callable(parse_message_fn):
        return
    if getattr(parse_message_fn, "_dcc_event_compat", False):
        return

    def parse_message_compat(data):
        msg_type = data.get("type") if isinstance(data, dict) else None
        if isinstance(msg_type, str) and msg_type.endswith("_event"):
            return SystemMessage(subtype=msg_type, data=data)
        return parse_message_fn(data)

    setattr(parse_message_compat, "_dcc_event_compat", True)
    sdk_message_parser.parse_message = parse_message_compat
    if hasattr(sdk_client_internal, "parse_message"):
        sdk_client_internal.parse_message = parse_message_compat


_install_sdk_event_compat()


# ── System Prompt ────────────────────────────────────────────────────────

SYSADMIN_PROMPT = """\
You are a sysadmin assistant for the Distributed Claude Code system. Your job is
to deploy and configure orchestrator daemons on remote servers, write CLAUDE.md
files that give daemons context about their server constraints, and manage local
configuration files (`config.json`, optional `config.md`).

=== CAPABILITIES ===

- **SSH access**: You can SSH into servers using the user's existing SSH config.
  Use `ssh user@host "command"` via the Bash tool.
- **Deploy daemon**: Copy `tools/orchestrator_daemon.py` to the remote server,
  set up a Python venv with dependencies, and launch the daemon.
- **Manage local tunnels**: Create/refresh local SSH tunnels in the background
  so the router can reach remote daemons without extra manual steps.
- **Write CLAUDE.md**: Generate a CLAUDE.md on the remote server that documents
  server constraints, available resources, and rules for the daemon.
- **Manage config.json**: Read and update the local `config.json` to add/modify
  server entries. Always show diffs before applying changes.
- **Read config.md notes**: If `config.md` exists in the project root, read it
  as user notes about setup/environment preferences before making setup decisions.
- **Health check**: Verify daemons are reachable via `curl`.

=== DEPLOYMENT PROCEDURE ===

When asked to set up a server (e.g., `/setup user@server`):

1. **Probe the environment** via SSH:
   - OS, architecture, available memory/disk
   - Python version, conda/venv availability
   - GPU availability (nvidia-smi), SLURM (squeue), etc.
   - Existing Claude Code installation (`claude --version`)
   - Network constraints (can it reach the internet?)

2. **Install prerequisites** if missing:
   - Ensure Python 3.10+ is available
   - Install `uv` if missing: `curl -LsSf https://astral.sh/uv/install.sh | sh`
   - Install Claude Code CLI if missing (needs Node.js)

3. **Deploy the daemon**:
   ```bash
   ssh user@host "mkdir -p ~/.distributed-cc"
   scp tools/orchestrator_daemon.py user@host:~/.distributed-cc/orchestrator_daemon.py
   ssh user@host "cd ~/.distributed-cc && uv venv .venv && uv pip install --python .venv/bin/python3 claude-agent-sdk aiohttp"
   ```

4. **Launch the daemon** (via tmux for persistence):
   ```bash
   ssh user@host "tmux new-session -d -s daemon '~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/orchestrator_daemon.py --port 8200 --name SERVER_NAME --callback-url http://127.0.0.1:9120'"
   ```

5. **Set up SSH tunnels automatically on the local machine**:
   ```bash
   ssh -N -L LOCAL_PORT:localhost:8200 -R 9120:localhost:9120 -o ServerAliveInterval=30 user@host
   ```
   - Prefer persistent background tunnel management:
     - First choice: `tmux` session (e.g., `dcc-tunnel-HOST`)
     - Fallback: `nohup ... &` with PID/log files under `~/.distributed-cc/`
   - Never leave a blocking foreground tunnel command running.
   - If an existing tunnel for the same host/port already exists, reuse or
     replace it cleanly.

6. **Update config.json** locally:
   - Read current config, add the new server entry
   - Show the diff before writing

7. **Verify** the daemon is reachable:
   ```bash
   curl http://127.0.0.1:LOCAL_PORT/health
   ```

=== CONFIG ===

Read `config.json` in the project root to see the current server configuration.

The config.json has this structure:
```json
{
  "servers": [
    {
      "name": "server-name",
      "host": "user@hostname",    // SSH destination, null for local
      "work_dir": "/path/to/project",
      "broker_port": 8200,        // local port for SSH tunnel
      "max_iterations": 0         // optional, 0 means no cap
    }
  ]
}
```

Each server needs a unique `broker_port` for its SSH tunnel.

=== USER NOTES (CONFIG.MD) ===

If `config.md` exists at the project root, treat it as user-authored setup and
environment notes. It can include constraints like preferred tools, tunnel
style, cluster rules, or safety requirements.

Priority order when deciding actions:
1) Explicit current user instruction in chat
2) `config.md` notes (if present)
3) Default behavior in this prompt

If `config.md` is absent, continue normally.

=== CLAUDE.MD GENERATION ===

When deploying a daemon, generate a CLAUDE.md on the remote server at the
project's working directory. This file tells the daemon about its environment:

- Server name and role
- Resource limits (CPU cores, memory, GPU, disk)
- Environment constraints (module system, conda, SLURM job limits)
- Network rules (proxy, no internet, etc.)
- Project-specific instructions the user provides

Example:
```markdown
# Server: della-gpu
## Environment
- SLURM cluster, submit jobs via sbatch
- 4x A100 GPUs available via SLURM
- Use conda environment: ml-env
- Load modules: cudatoolkit/12.2, anaconda3/2024.1

## Constraints
- No internet access from compute nodes
- Max job time: 72 hours
- Scratch space: /scratch/gpfs/user
```

=== HEALTH CHECK ===

When asked to check health (`/setup` with no args):
- Read config.json to find all servers
- For each server, try `curl http://127.0.0.1:BROKER_PORT/health`
- Report which daemons are up/down

=== PROJECT SETUP MODE ===

When asked to set up a project (`/setup-project ...`):
- Router will provide a fixed template prompt and include the user instruction verbatim.
- Treat that user instruction as the source of truth (work_dir or free-form request).
- Prefer reusing an existing machine entry (host + broker_port) from config.json.
- Add/update exactly one project entry in config.json and show a diff before writing.
- Avoid redeploying daemon or starting a new tunnel unless truly needed.
- Readiness gate for declaring success:
  - `work_dir` exists on the target machine (create with `mkdir -p` if missing)
  - `work_dir` is writable (`touch` + remove a temp file in that directory)
  - selected daemon is reachable (`curl http://127.0.0.1:BROKER_PORT/health`)
- If any readiness check fails, do not claim completion. Return a "NOT READY" result
  with the failing check and exact command/output snippet.
- End with a concise structured summary containing:
  - project_id
  - work_dir
  - host
  - broker_port
  - `/connect <project_id>` command

=== RULES ===

- Use the user's existing SSH config (don't ask for passwords)
- Show diffs before modifying config.json
- Be concise — report what you did, not what you're about to do
- Avoid step-by-step narration (e.g., repeated "Let me ..."). Prefer action/result updates.
- If something fails, diagnose and suggest fixes
- The daemon script is at `tools/orchestrator_daemon.py` relative to the project root
- Default to full automation for `/setup user@host`: deploy daemon, update
  config, start/refresh local tunnel, and verify health.
- If user explicitly passes manual mode, skip tunnel auto-start and print the
  exact tunnel command they should run.
- For `/setup-project`, keep router-level parsing minimal: use the injected
  template and the user's raw instruction to decide the config change.
- Never claim success before verification; do not make future-tense promises like
  "it will be created later".
- If the same remediation attempt fails twice, switch strategy or ask the user for
  an explicit decision point.
"""


class RouterSession:
    """Local sysadmin Claude session for the router — handles infra and config tasks."""

    def __init__(self, cwd: str = "."):
        self._cwd = os.path.abspath(cwd)
        self._session_id: str | None = None
        self._is_running = False
        self._progress_callback: callable | None = None
        self._log_callback: callable | None = None
        self._last_stream_text: str = ""
        self._saw_stream_text: bool = False

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def session_id(self) -> str | None:
        return self._session_id

    def set_callbacks(
        self,
        progress: callable | None = None,
        log: callable | None = None,
    ):
        """Set callbacks for streaming progress to the UI."""
        self._progress_callback = progress
        self._log_callback = log

    async def run(self, user_message: str) -> str:
        """Run a sysadmin task. Returns the result text."""
        os.environ.pop("CLAUDECODE", None)

        self._is_running = True
        try:
            return await self._run_inner(user_message)
        finally:
            self._is_running = False

    async def _run_inner(self, user_message: str) -> str:
        self._last_stream_text = ""
        self._saw_stream_text = False

        options = ClaudeAgentOptions(
            permission_mode="bypassPermissions",
            model="sonnet",
            cwd=self._cwd,
        )

        if self._session_id:
            options.resume = self._session_id
        else:
            options.system_prompt = SYSADMIN_PROMPT

        done_event = asyncio.Event()
        result_text = ""

        async for message in query(
            prompt=_prompt_stream(user_message, done_event),
            options=options,
        ):
            if isinstance(message, AssistantMessage):
                await self._forward_progress(message)
            elif isinstance(message, SystemMessage):
                if self._log_callback:
                    try:
                        await self._log_callback(f"router -> [{message.subtype}]")
                    except Exception:
                        pass
            elif isinstance(message, ResultMessage):
                result_text = message.result or ""
                self._session_id = message.session_id
                done_event.set()

        return result_text

    async def _forward_progress(self, message: AssistantMessage):
        """Forward intermediate assistant output via callbacks."""
        for block in message.content:
            if isinstance(block, TextBlock):
                text = block.text.strip()
                if text:
                    self._saw_stream_text = True
                    self._last_stream_text = text
                if text and self._progress_callback:
                    try:
                        await self._progress_callback(text)
                    except Exception:
                        pass
            elif isinstance(block, ToolUseBlock):
                tool_msg = f"{block.name}"
                if block.name in ("Bash", "Write", "Edit"):
                    snippet = json.dumps(block.input, ensure_ascii=False)
                    tool_msg += f": {snippet}"
                if self._log_callback:
                    try:
                        await self._log_callback(f"router -> {tool_msg}")
                    except Exception:
                        pass
            elif isinstance(block, ToolResultBlock):
                if block.is_error and self._log_callback:
                    content = block.content if isinstance(block.content, str) else str(block.content or "")
                    try:
                        await self._log_callback(f"[router ERROR] {content}")
                    except Exception:
                        pass

    def should_emit_final_result(self, result_text: str) -> bool:
        """Whether the final ResultMessage text should be surfaced to the UI."""
        final_text = (result_text or "").strip()
        if not final_text:
            return False
        if self._saw_stream_text and final_text == self._last_stream_text.strip():
            return False
        return True


# ── SDK Helper ───────────────────────────────────────────────────────────

async def _prompt_stream(text: str, done: asyncio.Event | None = None):
    """Wrap a string prompt into an AsyncIterable for the Agent SDK.

    Required when can_use_tool is set — the SDK needs streaming mode.
    When `done` is provided, keeps the stream alive until the event is set.
    """
    yield {
        "type": "user",
        "session_id": "",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
    }
    if done is not None:
        await done.wait()
