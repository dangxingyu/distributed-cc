from __future__ import annotations

"""SetupSession: local sysadmin Claude session for server setup and daemon deployment.

Mental model: Professor tells sysadmin "set up della-gpu, it's a SLURM node" →
Sysadmin (this local Claude session) handles all plumbing → PhD students (remote
daemons) get a CLAUDE.md with their server constraints.

Features:
  - SSH into servers, detect environments, install daemons
  - Generate config.json entries and CLAUDE.md files
  - Health-check deployed daemons
  - Persistent session with resume (context carries across follow-ups)
"""

import asyncio
import json
import logging
import os
from pathlib import Path

from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage
from claude_agent_sdk.types import (
    AssistantMessage,
    PermissionResultAllow,
    TextBlock,
    ToolUseBlock,
    ToolResultBlock,
)

log = logging.getLogger(__name__)


# ── System Prompt ────────────────────────────────────────────────────────

SYSADMIN_PROMPT = """\
You are a sysadmin assistant for the Distributed Claude Code system. Your job is
to deploy and configure orchestrator daemons on remote servers, write CLAUDE.md
files that give daemons context about their server constraints, and manage the
local config.json.

=== CAPABILITIES ===

- **SSH access**: You can SSH into servers using the user's existing SSH config.
  Use `ssh user@host "command"` via the Bash tool.
- **Deploy daemon**: Copy `tools/orchestrator_daemon.py` to the remote server,
  set up a Python venv with dependencies, and launch the daemon.
- **Write CLAUDE.md**: Generate a CLAUDE.md on the remote server that documents
  server constraints, available resources, and rules for the daemon.
- **Manage config.json**: Read and update the local `config.json` to add/modify
  server entries. Always show diffs before applying changes.
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

5. **Set up SSH tunnels** (inform the user, don't run in background):
   ```bash
   ssh -N -L LOCAL_PORT:localhost:8200 -R 9120:localhost:9120 -o ServerAliveInterval=30 user@host
   ```

6. **Update config.json** locally:
   - Read current config, add the new server entry
   - Show the diff before writing

7. **Verify** the daemon is reachable:
   ```bash
   curl http://127.0.0.1:LOCAL_PORT/health
   ```

=== CURRENT CONFIG ===

```json
{config_snapshot}
```

=== CONFIG FORMAT ===

The config.json has this structure:
```json
{{
  "servers": [
    {{
      "name": "server-name",
      "host": "user@hostname",    // SSH destination, null for local
      "work_dir": "/path/to/project",
      "broker_port": 8200         // local port for SSH tunnel
    }}
  ],
  "orchestrator": {{
    "model": "claude-opus-4-6",
    "session_model": "claude-opus-4-6"
  }}
}}
```

Each server needs a unique `broker_port` for its SSH tunnel.

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

=== RULES ===

- Use the user's existing SSH config (don't ask for passwords)
- Show diffs before modifying config.json
- Be concise — report what you did, not what you're about to do
- If something fails, diagnose and suggest fixes
- The daemon script is at `tools/orchestrator_daemon.py` relative to the project root
"""


class SetupSession:
    """Wraps a local Claude Agent SDK session for sysadmin tasks."""

    def __init__(self, cwd: str = "."):
        self._cwd = os.path.abspath(cwd)
        self._session_id: str | None = None
        self._is_running = False
        self._progress_callback: callable | None = None
        self._log_callback: callable | None = None

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

    def _get_config_snapshot(self) -> str:
        """Read current config.json for injection into the system prompt."""
        config_path = os.path.join(self._cwd, "config.json")
        try:
            with open(config_path) as f:
                return f.read()
        except FileNotFoundError:
            return '{"servers": [], "orchestrator": {}}'

    def _make_can_use_tool(self):
        """Create tool permission callback. Auto-approve everything."""

        async def can_use_tool(tool_name: str, input_data: dict, context=None):
            return PermissionResultAllow()

        return can_use_tool

    async def run(self, user_message: str) -> str:
        """Run a sysadmin task. Returns the result text."""
        os.environ.pop("CLAUDECODE", None)

        self._is_running = True
        try:
            return await self._run_inner(user_message)
        finally:
            self._is_running = False

    async def _run_inner(self, user_message: str) -> str:
        options = ClaudeAgentOptions(
            can_use_tool=self._make_can_use_tool(),
            model="sonnet",
            cwd=self._cwd,
            allowed_tools=["Read", "Write", "Edit", "Bash", "Glob", "Grep"],
            sandbox={"enabled": False},
        )

        if self._session_id:
            options.resume = self._session_id
        else:
            config_snapshot = self._get_config_snapshot()
            options.system_prompt = SYSADMIN_PROMPT.format(
                config_snapshot=config_snapshot
            )

        done_event = asyncio.Event()
        result_text = ""

        async for message in query(
            prompt=_prompt_stream(user_message, done_event),
            options=options,
        ):
            if isinstance(message, AssistantMessage):
                await self._forward_progress(message)
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
                if text and self._progress_callback:
                    try:
                        await self._progress_callback(text)
                    except Exception:
                        pass
            elif isinstance(block, ToolUseBlock):
                tool_msg = f"{block.name}"
                if block.name in ("Bash", "Write", "Edit"):
                    snippet = json.dumps(block.input, ensure_ascii=False)[:300]
                    tool_msg += f": {snippet}"
                if self._log_callback:
                    try:
                        await self._log_callback(f"setup -> {tool_msg}")
                    except Exception:
                        pass
            elif isinstance(block, ToolResultBlock):
                if block.is_error and self._log_callback:
                    content = block.content if isinstance(block.content, str) else str(block.content or "")
                    try:
                        await self._log_callback(f"[setup ERROR] {content[:500]}")
                    except Exception:
                        pass


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
