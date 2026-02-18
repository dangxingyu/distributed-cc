"""Router: thin relay between the web UI and remote orchestrator daemons.

Replaces the 1500-line orchestrator.py with a ~300-line relay that:
  1. Routes user messages: idle → POST /task, running → POST /interrupt
  2. Listens to SSE from each remote daemon, forwards to web UI
  3. Handles permission escalations (daemon → router → web UI → router → daemon)
  4. Maps channels to projects (channel ↔ project_id)
"""

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass, field

import aiohttp

log = logging.getLogger(__name__)


@dataclass
class RemoteOrchestrator:
    """A remote orchestrator daemon configuration."""
    project_id: str
    name: str
    host: str | None = None       # SSH dest, None = local
    broker_port: int = 8200       # Port the daemon listens on
    project_dir: str = ""
    max_iterations: int = 20
    status: str = "unknown"       # idle/running/done/stuck/disconnected


class Router:
    """Routes messages between web UI and remote orchestrator daemons."""

    def __init__(self, cwd: str = "."):
        self._cwd = os.path.abspath(cwd)
        self._orchestrators: dict[str, RemoteOrchestrator] = {}

        # Channel → project_id mapping (persisted in store)
        self._channel_project: dict[int, str] = {}

        # HTTP client for daemon communication
        self._http: aiohttp.ClientSession | None = None

        # SSE listener tasks
        self._sse_tasks: dict[str, asyncio.Task] = {}

        # Permission escalation: request_id → asyncio.Future
        self._pending_permissions: dict[str, asyncio.Future] = {}
        self._pending_meta: dict[str, dict] = {}

        # Callbacks for sending to web UI
        self._progress_callback = None     # (project_id, event) → forward to UI
        self._status_callback = None       # (project_id, status) → update UI
        self._escalation_callback = None   # (request_id, data) → send to web UI

        # Per-channel status callbacks (for typing indicator)
        self._channel_status_callbacks: dict[int, callable] = {}

    async def init(self):
        """Initialize: load config, create HTTP client."""
        self._http = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        self._load_config()

        # Register projects on daemons
        for orch in self._orchestrators.values():
            asyncio.create_task(self._register_project(orch))

    async def close(self):
        """Shutdown: cancel SSE listeners, close HTTP client."""
        for task in self._sse_tasks.values():
            task.cancel()
        if self._http:
            await self._http.close()

    def _load_config(self):
        """Read config.json from working directory."""
        config_path = os.path.join(self._cwd, "config.json")
        if not os.path.exists(config_path):
            log.info("No config.json in %s, using defaults", self._cwd)
            return

        with open(config_path) as f:
            cfg = json.load(f)
        log.info("Loaded config.json from %s", self._cwd)

        for o in cfg.get("orchestrators", []):
            orch = RemoteOrchestrator(
                project_id=o["project_id"],
                name=o.get("name", o["project_id"]),
                host=o.get("host"),
                broker_port=o.get("broker_port", 8200),
                project_dir=o.get("project_dir", ""),
                max_iterations=o.get("max_iterations", 20),
            )
            self._orchestrators[orch.project_id] = orch

    def _daemon_url(self, orch: RemoteOrchestrator) -> str:
        """Get the daemon HTTP URL (via SSH tunnel)."""
        return f"http://127.0.0.1:{orch.broker_port}"

    async def _register_project(self, orch: RemoteOrchestrator):
        """Register a project on its daemon."""
        url = f"{self._daemon_url(orch)}/register"
        try:
            async with self._http.post(url, json={
                "project_id": orch.project_id,
                "project_dir": orch.project_dir,
                "name": orch.name,
            }) as resp:
                if resp.status == 200:
                    log.info(f"Registered project {orch.project_id} on daemon")
                    orch.status = "idle"
                    self._start_sse_listener(orch)
                else:
                    body = await resp.text()
                    log.warning(f"Failed to register {orch.project_id}: {body}")
                    orch.status = "disconnected"
        except aiohttp.ClientError as e:
            log.warning(f"Cannot reach daemon for {orch.project_id}: {e}")
            orch.status = "disconnected"

    # ── SSE Listener ──────────────────────────────────────────────────

    def _start_sse_listener(self, orch: RemoteOrchestrator):
        """Start listening to SSE stream from a daemon."""
        old = self._sse_tasks.get(orch.project_id)
        if old and not old.done():
            old.cancel()
        self._sse_tasks[orch.project_id] = asyncio.create_task(
            self._sse_listen_loop(orch)
        )

    async def _sse_listen_loop(self, orch: RemoteOrchestrator):
        """Listen to SSE events from a daemon and forward to web UI."""
        url = f"{self._daemon_url(orch)}/stream?project_id={orch.project_id}"
        while True:
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=None)
                ) as http:
                    async with http.get(url) as resp:
                        async for line in resp.content:
                            text = line.decode("utf-8", errors="replace").strip()
                            if not text or text.startswith(":"):
                                continue
                            if text.startswith("data: "):
                                data_str = text[6:]
                                try:
                                    event = json.loads(data_str)
                                    await self._handle_sse_event(orch.project_id, event)
                                except json.JSONDecodeError:
                                    pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"SSE connection lost for {orch.project_id}: {e}")
                orch.status = "disconnected"
                await asyncio.sleep(5)  # Reconnect after delay

    async def _handle_sse_event(self, project_id: str, event: dict):
        """Handle a progress event from a daemon."""
        event_type = event.get("type", "")
        data = event.get("data", "")
        iteration = event.get("iteration", 0)

        orch = self._orchestrators.get(project_id)
        if orch:
            if event_type == "done":
                orch.status = "done"
            elif event_type == "stuck":
                orch.status = "stuck"
            elif event_type == "error":
                orch.status = "error"
            elif event_type == "iteration":
                orch.status = "running"

        # Forward to progress callback (web UI)
        if self._progress_callback:
            try:
                await self._progress_callback(project_id, event)
            except Exception:
                log.warning("Progress callback failed", exc_info=True)

        # Update channel status
        for chat_id, pid in self._channel_project.items():
            if pid == project_id:
                await self._update_channel_status(chat_id, event_type, data, iteration)

    async def _update_channel_status(
        self, chat_id: int, event_type: str, data: str, iteration: int,
    ):
        """Update a channel's status indicator."""
        cb = self._channel_status_callbacks.get(chat_id)
        if not cb:
            return

        if event_type in ("done", "stuck", "error"):
            try:
                await cb({"type": event_type, "data": data, "iteration": iteration})
            except Exception:
                pass
        elif event_type == "iteration":
            try:
                await cb({"type": "busy", "data": data, "iteration": iteration})
            except Exception:
                pass

    # ── Message Routing ───────────────────────────────────────────────

    async def route_message(
        self,
        chat_id: int,
        text: str,
        send_reply: callable,
        send_log: callable = None,
    ):
        """Route a user message to the appropriate daemon.

        If idle → POST /task (start new task)
        If running → POST /interrupt (inject message)
        """
        project_id = self._channel_project.get(chat_id)
        if not project_id:
            await send_reply(
                "No project connected. Use `/connect <project-id>` to link this channel.\n"
                f"Available projects: {', '.join(self._orchestrators.keys()) or '(none)'}"
            )
            return

        # Handle /connect command (already connected, show status)
        stripped = text.strip()
        if stripped.startswith("/connect"):
            parts = stripped.split(None, 1)
            if len(parts) > 1:
                new_pid = parts[1].strip()
                await self._connect_channel(chat_id, new_pid, send_reply)
            else:
                await send_reply(f"Connected to project: **{project_id}**")
            return

        # Handle /stop
        if stripped.lower() in ("/stop", "@orchestrator /stop"):
            await self._stop_task(chat_id, project_id, send_reply)
            return

        # Handle /status
        if stripped.lower() in ("/status",):
            await self._show_status(chat_id, project_id, send_reply)
            return

        orch = self._orchestrators.get(project_id)
        if not orch:
            await send_reply(f"Project `{project_id}` not found in config.")
            return

        if orch.status == "disconnected":
            await send_reply(f"Daemon for `{project_id}` is disconnected. Check SSH tunnel.")
            return

        # Route based on status
        if orch.status in ("idle", "done", "stuck", "error", "unknown"):
            await self._start_task(chat_id, project_id, text, send_reply, send_log)
        else:
            # Running → interrupt
            await self._interrupt_task(chat_id, project_id, text, send_reply)

    async def _start_task(
        self,
        chat_id: int,
        project_id: str,
        task_text: str,
        send_reply: callable,
        send_log: callable = None,
    ):
        """Start a new task on the daemon."""
        orch = self._orchestrators[project_id]
        url = f"{self._daemon_url(orch)}/task"

        try:
            async with self._http.post(url, json={
                "project_id": project_id,
                "task": task_text,
                "max_iterations": orch.max_iterations,
            }) as resp:
                result = await resp.json()
                if result.get("ok"):
                    orch.status = "running"
                    if send_log:
                        await send_log(f"Task started on {orch.name}")
                else:
                    error = result.get("error", "Unknown error")
                    await send_reply(f"Failed to start task: {error}")
        except aiohttp.ClientError as e:
            await send_reply(f"Cannot reach daemon for `{project_id}`: {e}")
            orch.status = "disconnected"

    async def _interrupt_task(
        self,
        chat_id: int,
        project_id: str,
        message: str,
        send_reply: callable,
    ):
        """Inject an interrupt message into a running task."""
        orch = self._orchestrators[project_id]
        url = f"{self._daemon_url(orch)}/interrupt"

        try:
            async with self._http.post(url, json={
                "project_id": project_id,
                "message": message,
            }) as resp:
                result = await resp.json()
                if result.get("ok"):
                    await send_reply("(queued — will be picked up at next iteration)")
                else:
                    await send_reply(f"Failed to interrupt: {result.get('error', '?')}")
        except aiohttp.ClientError as e:
            await send_reply(f"Cannot reach daemon: {e}")

    async def _stop_task(self, chat_id: int, project_id: str, send_reply: callable):
        """Stop a running task."""
        orch = self._orchestrators.get(project_id)
        if not orch:
            await send_reply("No project connected.")
            return

        url = f"{self._daemon_url(orch)}/stop"
        try:
            async with self._http.post(url, json={
                "project_id": project_id,
            }) as resp:
                result = await resp.json()
                if result.get("ok"):
                    await send_reply("(stopping task...)")
                else:
                    await send_reply(f"Stop: {result.get('reason', 'No running task')}")
        except aiohttp.ClientError as e:
            await send_reply(f"Cannot reach daemon: {e}")

    async def _show_status(self, chat_id: int, project_id: str, send_reply: callable):
        """Show status of a project's task."""
        orch = self._orchestrators.get(project_id)
        if not orch:
            await send_reply("No project connected.")
            return

        url = f"{self._daemon_url(orch)}/status?project_id={project_id}"
        try:
            async with self._http.get(url) as resp:
                data = await resp.json()
                status = data.get("status", "unknown")
                iteration = data.get("iteration", 0)
                max_iter = data.get("max_iterations", 0)
                summary = data.get("summary", "")

                lines = [f"**{orch.name}** ({project_id}): {status}"]
                if status == "running":
                    lines.append(f"Iteration: {iteration}/{max_iter}")
                if summary:
                    lines.append(f"Summary: {summary}")
                await send_reply("\n".join(lines))
        except aiohttp.ClientError as e:
            await send_reply(f"Cannot reach daemon: {e}")

    # ── Channel ↔ Project Mapping ─────────────────────────────────────

    async def connect_channel(self, chat_id: int, project_id: str) -> bool:
        """Connect a channel to a project. Returns True on success."""
        if project_id not in self._orchestrators:
            return False
        self._channel_project[chat_id] = project_id
        return True

    async def _connect_channel(self, chat_id: int, project_id: str, send_reply: callable):
        """Handle /connect command."""
        if project_id not in self._orchestrators:
            available = ", ".join(self._orchestrators.keys()) or "(none)"
            await send_reply(f"Unknown project: `{project_id}`. Available: {available}")
            return

        self._channel_project[chat_id] = project_id
        orch = self._orchestrators[project_id]
        await send_reply(f"Connected to **{orch.name}** (`{project_id}`)")

    def get_channel_project(self, chat_id: int) -> str | None:
        """Get the project_id for a channel."""
        return self._channel_project.get(chat_id)

    def get_project_status(self, project_id: str) -> str:
        """Get the status of a project."""
        orch = self._orchestrators.get(project_id)
        return orch.status if orch else "unknown"

    def list_orchestrators(self) -> list[RemoteOrchestrator]:
        """List all configured orchestrators."""
        return list(self._orchestrators.values())

    # ── Status Callbacks ──────────────────────────────────────────────

    def set_channel_status_callback(self, chat_id: int, callback: callable):
        """Register a status callback for a channel."""
        self._channel_status_callbacks[chat_id] = callback

    def remove_channel_status_callback(self, chat_id: int):
        """Remove a status callback."""
        self._channel_status_callbacks.pop(chat_id, None)

    def set_progress_callback(self, callback: callable):
        """Set the global progress callback (for web UI)."""
        self._progress_callback = callback

    # ── Permission Escalation ─────────────────────────────────────────

    async def handle_permission_escalation(self, data: dict) -> dict:
        """Handle a permission escalation from a daemon.

        Called by the HTTP callback server. Creates a pending future,
        sends the request to the web UI, and waits for resolution.
        """
        request_id = uuid.uuid4().hex[:12]
        future = asyncio.get_event_loop().create_future()
        self._pending_permissions[request_id] = future
        self._pending_meta[request_id] = data

        # Notify web UI via escalation callback
        if self._escalation_callback:
            await self._escalation_callback(request_id, data)

        try:
            result = await asyncio.wait_for(future, timeout=300)
            return result
        except asyncio.TimeoutError:
            self._pending_permissions.pop(request_id, None)
            self._pending_meta.pop(request_id, None)
            return {"approved": False, "reason": "Timeout"}

    def resolve_permission(self, request_id: str, approved: bool, reason: str = "") -> bool:
        """Resolve a pending permission escalation (from web UI)."""
        future = self._pending_permissions.pop(request_id, None)
        self._pending_meta.pop(request_id, None)
        if future and not future.done():
            future.set_result({"approved": approved, "reason": reason})
            return True
        return False

    def set_escalation_callback(self, callback):
        self._escalation_callback = callback

    # ── Health Check ──────────────────────────────────────────────────

    async def check_health(self, project_id: str) -> bool:
        """Check if a daemon is reachable."""
        orch = self._orchestrators.get(project_id)
        if not orch:
            return False
        try:
            url = f"{self._daemon_url(orch)}/health"
            async with self._http.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                return resp.status == 200
        except Exception:
            return False
