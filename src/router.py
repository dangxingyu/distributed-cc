"""Router: relay between web UI and remote orchestrator daemons.

Responsibilities:
  1. Route user messages: idle -> POST /task, running -> urgent interrupt or deferred queue
  2. Listen to daemon SSE and ingest callback progress with dedupe
  3. Track channel <-> project mappings (with optional persistence hook)
  4. Support setup-mode orchestration via SetupSession
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass

import aiohttp

from .setup import SetupSession

log = logging.getLogger(__name__)


@dataclass
class RemoteOrchestrator:
    """A remote orchestrator daemon configuration."""

    project_id: str
    name: str
    host: str | None = None
    broker_port: int = 8200
    project_dir: str = ""
    max_iterations: int = 20
    status: str = "unknown"  # idle/running/done/stuck/error/disconnected


class Router:
    """Routes messages between the web UI and remote orchestrator daemons."""

    def __init__(self, cwd: str = "."):
        self._cwd = os.path.abspath(cwd)
        self._orchestrators: dict[str, RemoteOrchestrator] = {}

        # Channel -> project_id mapping
        self._channel_project: dict[int, str] = {}

        # HTTP client for daemon communication
        self._http: aiohttp.ClientSession | None = None

        # SSE listener tasks
        self._sse_tasks: dict[str, asyncio.Task] = {}

        # Progress dedupe caches
        self._recent_event_ids: dict[str, deque[str]] = {}
        self._recent_event_id_set: dict[str, set[str]] = {}
        self._recent_event_signatures: dict[str, dict[str, float]] = {}

        # Deferred non-urgent tasks while running
        self._deferred_tasks: dict[str, list[dict]] = {}

        # Callback to web layer
        self._progress_callback = None  # async (project_id, event)

        # Optional callback to persist channel mapping
        self._mapping_persist_callback = None  # async (chat_id, project_id|None)

        # Setup mode — per-channel sessions and tasks
        self._setup_sessions: dict[int, SetupSession] = {}
        self._setup_channels: set[int] = set()
        self._setup_tasks: dict[int, asyncio.Task] = {}

    async def init(self):
        self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        self._load_config()

    async def close(self):
        for task in self._sse_tasks.values():
            task.cancel()
        if self._http:
            await self._http.close()

    def _load_config(self):
        """Load config.json.

        Supports both:
          - new schema: {"orchestrators": [{project_id, name, project_dir, ...}]}
          - legacy schema: {"servers": [{name, work_dir, ...}]}
        """
        config_path = os.path.join(self._cwd, "config.json")
        if not os.path.exists(config_path):
            log.info("No config.json in %s, using defaults", self._cwd)
            return

        with open(config_path) as f:
            cfg = json.load(f)
        log.info("Loaded config.json from %s", self._cwd)

        orchestrators = cfg.get("orchestrators")
        if orchestrators:
            for o in orchestrators:
                project_id = o.get("project_id")
                if not project_id:
                    continue
                orch = RemoteOrchestrator(
                    project_id=project_id,
                    name=o.get("name", project_id),
                    host=o.get("host"),
                    broker_port=o.get("broker_port", 8200),
                    project_dir=o.get("project_dir", ""),
                    max_iterations=o.get("max_iterations", 20),
                )
                self._orchestrators[project_id] = orch
            return

        for s in cfg.get("servers", []):
            name = s.get("name")
            if not name:
                continue
            project_id = s.get("project_id", name)
            orch = RemoteOrchestrator(
                project_id=project_id,
                name=s.get("name", project_id),
                host=s.get("host"),
                broker_port=s.get("broker_port", 8200),
                project_dir=s.get("work_dir", s.get("project_dir", "")),
                max_iterations=s.get("max_iterations", 20),
            )
            self._orchestrators[project_id] = orch

    def _daemon_url(self, orch: RemoteOrchestrator) -> str:
        return f"http://127.0.0.1:{orch.broker_port}"

    async def _register_project(self, orch: RemoteOrchestrator):
        url = f"{self._daemon_url(orch)}/register"
        try:
            async with self._http.post(
                url,
                json={
                    "project_id": orch.project_id,
                    "project_dir": orch.project_dir,
                    "name": orch.name,
                },
            ) as resp:
                if resp.status == 200:
                    log.info("Registered project %s on daemon", orch.project_id)
                    orch.status = "idle"
                    self._start_sse_listener(orch)
                else:
                    body = await resp.text()
                    log.warning("Failed to register %s: %s", orch.project_id, body)
                    orch.status = "disconnected"
        except aiohttp.ClientError as e:
            log.warning("Cannot reach daemon for %s: %s", orch.project_id, e)
            orch.status = "disconnected"

    async def _ensure_registered(self, orch: RemoteOrchestrator) -> bool:
        """Lazily register with daemon on first interaction. Returns True if ready."""
        if orch.status not in ("unknown", "disconnected"):
            return True
        await self._register_project(orch)
        return orch.status not in ("unknown", "disconnected")

    # -- SSE -----------------------------------------------------------

    def _start_sse_listener(self, orch: RemoteOrchestrator):
        old = self._sse_tasks.get(orch.project_id)
        if old and not old.done():
            old.cancel()
        self._sse_tasks[orch.project_id] = asyncio.create_task(self._sse_listen_loop(orch))

    async def _sse_listen_loop(self, orch: RemoteOrchestrator):
        url = f"{self._daemon_url(orch)}/stream?project_id={orch.project_id}"
        while True:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as http:
                    # Sync state on (re)connect to catch events missed during gap
                    await self._sync_daemon_status(orch, http)
                    async with http.get(url) as resp:
                        async for line in resp.content:
                            text = line.decode("utf-8", errors="replace").strip()
                            if not text or text.startswith(":"):
                                continue
                            if text.startswith("data: "):
                                try:
                                    event = json.loads(text[6:])
                                except json.JSONDecodeError:
                                    continue
                                await self.ingest_progress_event(orch.project_id, event, source="sse")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("SSE connection lost for %s: %s", orch.project_id, e)
                orch.status = "disconnected"
                await asyncio.sleep(5)

    async def _sync_daemon_status(self, orch: RemoteOrchestrator, http: aiohttp.ClientSession):
        """Poll /status to sync orch.status after SSE (re)connect."""
        try:
            status_url = f"{self._daemon_url(orch)}/status?project_id={orch.project_id}"
            async with http.get(status_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status = data.get("status", "idle")
                    orch.status = status
                    log.info("Synced status for %s: %s", orch.project_id, status)
        except Exception as e:
            log.debug("Status sync failed for %s: %s", orch.project_id, e)

    async def ingest_progress_event(self, project_id: str, event: dict, source: str = "unknown") -> bool:
        """Single ingestion path for both SSE and callback progress events.

        Returns True when processed, False when deduped.
        """
        if self._is_duplicate_event(project_id, event):
            return False

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

        if self._progress_callback:
            try:
                await self._progress_callback(project_id, event)
            except Exception:
                log.warning("Progress callback failed", exc_info=True)

        if event_type in ("done", "error"):
            await self._maybe_start_deferred_task(project_id)

        return True

    def _is_duplicate_event(self, project_id: str, event: dict) -> bool:
        event_id = event.get("event_id")
        if event_id:
            ids = self._recent_event_ids.setdefault(project_id, deque(maxlen=256))
            id_set = self._recent_event_id_set.setdefault(project_id, set())
            if event_id in id_set:
                return True
            if len(ids) == ids.maxlen:
                oldest = ids[0]
                id_set.discard(oldest)
            ids.append(event_id)
            id_set.add(event_id)
            return False

        signature_raw = json.dumps(
            {
                "type": event.get("type", ""),
                "data": event.get("data", ""),
                "iteration": event.get("iteration", 0),
                "ts": event.get("ts", ""),
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        signature = hashlib.sha1(signature_raw.encode("utf-8")).hexdigest()

        now = time.monotonic()
        bucket = self._recent_event_signatures.setdefault(project_id, {})

        to_delete = [k for k, ts in bucket.items() if now - ts > 30]
        for key in to_delete:
            bucket.pop(key, None)

        if signature in bucket:
            return True

        bucket[signature] = now
        return False

    # -- message routing ------------------------------------------------

    async def route_message(self, chat_id: int, text: str, send_reply: callable, send_log: callable = None):
        stripped = text.strip()

        # @router prefix — direct message to the router/setup session
        addressed_to_router, router_body = self._strip_prefix(stripped, "@router")
        if addressed_to_router:
            if not router_body:
                await send_reply("Message is empty after `@router` prefix.", sender="system")
                return
            self._setup_channels.add(chat_id)
            await self._handle_setup(chat_id, router_body, send_reply, send_log)
            return

        # setup mode routing (works without connected project)
        if stripped.startswith("/setup"):
            self._setup_channels.add(chat_id)
            await self._handle_setup(chat_id, stripped, send_reply, send_log)
            return

        if chat_id in self._setup_channels:
            if stripped.startswith("/connect") or stripped == "/done":
                self._setup_channels.discard(chat_id)
                if stripped == "/done":
                    await send_reply("Exited setup mode.")
                    return
            else:
                await self._handle_setup(chat_id, stripped, send_reply, send_log)
                return

        addressed_to_orchestrator, orchestrator_body = self._strip_prefix(stripped, "@orchestrator")
        command, command_arg = self._parse_command(orchestrator_body if addressed_to_orchestrator else stripped)

        if command == "/connect":
            if command_arg:
                await self._connect_channel(chat_id, command_arg, send_reply)
            else:
                project_id = self._channel_project.get(chat_id)
                if project_id:
                    await send_reply(f"Connected to project: **{project_id}**")
                else:
                    available = ", ".join(self._orchestrators.keys()) or "(none)"
                    await send_reply(f"Not connected. Use `/connect <project-id>`. Available: {available}")
            return

        project_id = self._channel_project.get(chat_id)
        if not project_id:
            await send_reply(
                "No project connected. Use `/connect <project-id>` to link this channel.\n"
                f"Available projects: {', '.join(self._orchestrators.keys()) or '(none)'}"
            )
            return

        if command == "/stop":
            await self._stop_task(chat_id, project_id, send_reply)
            return

        if command == "/status":
            await self._show_status(chat_id, project_id, send_reply)
            return

        orch = self._orchestrators.get(project_id)
        if not orch:
            await send_reply(f"Project `{project_id}` not found in config.")
            return

        if not await self._ensure_registered(orch):
            await send_reply(f"Cannot reach daemon for `{project_id}`. Is the daemon running and SSH tunnel open?")
            return

        effective_text = orchestrator_body if addressed_to_orchestrator else stripped
        if not effective_text:
            await send_reply("Message is empty after `@orchestrator` prefix.")
            return

        if orch.status == "stuck":
            # Orchestrator is blocked on ask_user — deliver as interrupt (the answer)
            await self._interrupt_task(chat_id, project_id, effective_text, send_reply)
            return

        if orch.status in ("idle", "done", "error", "unknown"):
            await self._start_task(chat_id, project_id, effective_text, send_reply, send_log)
            return

        if addressed_to_orchestrator:
            await self._interrupt_task(chat_id, project_id, effective_text, send_reply)
            return

        queue_size = self._enqueue_deferred_task(project_id, chat_id, stripped)
        await send_reply(f"(queued as next task #{queue_size} — use `@orchestrator ...` for urgent interruption)")

    def _strip_prefix(self, text: str, prefix: str) -> tuple[bool, str]:
        stripped = text.strip()
        lower = stripped.lower()
        if not lower.startswith(prefix):
            return False, stripped

        remainder = stripped[len(prefix):].lstrip()
        return True, remainder

    def _parse_command(self, text: str) -> tuple[str | None, str]:
        stripped = text.strip()
        if not stripped.startswith("/"):
            return None, ""

        parts = stripped.split(None, 1)
        cmd = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""
        if cmd in ("/connect", "/stop", "/status"):
            return cmd, arg
        return None, ""

    def _enqueue_deferred_task(self, project_id: str, chat_id: int, text: str) -> int:
        queue = self._deferred_tasks.setdefault(project_id, [])
        queue.append({"chat_id": chat_id, "text": text, "ts": time.time()})
        return len(queue)

    async def _maybe_start_deferred_task(self, project_id: str):
        queue = self._deferred_tasks.get(project_id) or []
        if not queue:
            return

        orch = self._orchestrators.get(project_id)
        if not orch or orch.status == "running":
            return

        next_task = queue.pop(0)
        remaining = len(queue)
        ok, error = await self._start_task_request(project_id, next_task["text"])
        if ok:
            orch.status = "running"
            # Notify via progress callback so the web layer can inform the user
            if self._progress_callback:
                snippet = next_task["text"][:200]
                note = f"Starting queued task: {snippet}"
                if remaining:
                    note += f" ({remaining} more in queue)"
                try:
                    await self._progress_callback(project_id, {
                        "type": "text",
                        "data": f"@orchestrator {note}",
                        "iteration": 0,
                        "ts": time.time(),
                    })
                except Exception:
                    pass
        else:
            log.warning("Failed to start deferred task for %s: %s", project_id, error)

    async def _start_task_request(self, project_id: str, task_text: str) -> tuple[bool, str]:
        orch = self._orchestrators[project_id]
        url = f"{self._daemon_url(orch)}/task"
        try:
            async with self._http.post(
                url,
                json={
                    "project_id": project_id,
                    "task": task_text,
                    "max_iterations": orch.max_iterations,
                },
            ) as resp:
                result = await resp.json()
                if result.get("ok"):
                    return True, ""
                return False, result.get("error", "Unknown error")
        except aiohttp.ClientError as e:
            orch.status = "disconnected"
            return False, str(e)

    async def _start_task(
        self,
        chat_id: int,
        project_id: str,
        task_text: str,
        send_reply: callable,
        send_log: callable = None,
    ):
        ok, error = await self._start_task_request(project_id, task_text)
        orch = self._orchestrators[project_id]
        if ok:
            orch.status = "running"
            if send_log:
                await send_log(f"Task started on {orch.name}")
            return

        await send_reply(f"Failed to start task: {error}")

    async def _interrupt_task(self, chat_id: int, project_id: str, message: str, send_reply: callable):
        orch = self._orchestrators[project_id]
        url = f"{self._daemon_url(orch)}/interrupt"

        try:
            async with self._http.post(url, json={"project_id": project_id, "message": message}) as resp:
                result = await resp.json()
                if result.get("ok"):
                    await send_reply("(urgent interrupt queued — injected after current action)")
                else:
                    await send_reply(f"Failed to interrupt: {result.get('error', '?')}")
        except aiohttp.ClientError as e:
            await send_reply(f"Cannot reach daemon: {e}")

    async def _stop_task(self, chat_id: int, project_id: str, send_reply: callable):
        orch = self._orchestrators.get(project_id)
        if not orch:
            await send_reply("No project connected.")
            return

        url = f"{self._daemon_url(orch)}/stop"
        try:
            async with self._http.post(url, json={"project_id": project_id}) as resp:
                result = await resp.json()
                if result.get("ok"):
                    await send_reply("(stopping task...)")
                else:
                    await send_reply(f"Stop: {result.get('reason', 'No running task')}")
        except aiohttp.ClientError as e:
            await send_reply(f"Cannot reach daemon: {e}")

    async def _show_status(self, chat_id: int, project_id: str, send_reply: callable):
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
                queued = len(self._deferred_tasks.get(project_id, []))

                lines = [f"**{orch.name}** ({project_id}): {status}"]
                if status == "running":
                    lines.append(f"Iteration: {iteration}/{max_iter}")
                if queued:
                    lines.append(f"Queued tasks: {queued}")
                if summary:
                    lines.append(f"Summary: {summary}")
                await send_reply("\n".join(lines))
        except aiohttp.ClientError as e:
            await send_reply(f"Cannot reach daemon: {e}")

    # -- setup mode ----------------------------------------------------

    async def _handle_setup(self, chat_id: int, text: str, send_reply: callable, send_log: callable = None):
        if chat_id not in self._setup_sessions:
            self._setup_sessions[chat_id] = SetupSession(cwd=self._cwd)

        session = self._setup_sessions[chat_id]

        # Wrap send_reply to tag setup messages as "router" sender
        async def setup_reply(msg: str):
            await send_reply(msg, sender="router")

        session.set_callbacks(progress=setup_reply, log=send_log)

        stripped = text.strip()
        if stripped.startswith("/setup"):
            arg = stripped[len("/setup") :].strip()
            if arg:
                prompt = (
                    f"Set up a new server: {arg}\n\n"
                    "Probe the environment via SSH, deploy the daemon, "
                    "and update config.json. Follow the deployment procedure."
                )
            else:
                prompt = (
                    "Run a health check on all configured servers. "
                    "Read config.json and curl each daemon's /health endpoint."
                )
        else:
            prompt = stripped

        async def _run():
            try:
                result = await session.run(prompt)
                if result:
                    await setup_reply(result)
                self.reload_config()
            except Exception as e:
                log.exception("Setup session error: %s", e)
                await send_reply(f"Setup error: {e}", sender="system")

        # Cancel any in-flight setup task for this channel before starting a new one
        old_task = self._setup_tasks.get(chat_id)
        if old_task and not old_task.done():
            old_task.cancel()

        self._setup_tasks[chat_id] = asyncio.create_task(_run())

    def reload_config(self):
        old_ids = set(self._orchestrators.keys())
        self._orchestrators.clear()
        self._load_config()
        new_ids = set(self._orchestrators.keys())

        for pid in new_ids - old_ids:
            asyncio.create_task(self._register_project(self._orchestrators[pid]))

        for pid in old_ids - new_ids:
            task = self._sse_tasks.pop(pid, None)
            if task:
                task.cancel()

    # -- channel/project mapping --------------------------------------

    def set_mapping_persist_callback(self, callback):
        self._mapping_persist_callback = callback

    async def _persist_channel_mapping(self, chat_id: int, project_id: str | None):
        if self._mapping_persist_callback:
            await self._mapping_persist_callback(chat_id, project_id)

    async def connect_channel(self, chat_id: int, project_id: str) -> bool:
        if project_id not in self._orchestrators:
            return False
        self._channel_project[chat_id] = project_id
        await self._persist_channel_mapping(chat_id, project_id)
        return True

    async def disconnect_channel(self, chat_id: int):
        self._channel_project.pop(chat_id, None)
        await self._persist_channel_mapping(chat_id, None)

    async def _connect_channel(self, chat_id: int, project_id: str, send_reply: callable):
        if project_id not in self._orchestrators:
            available = ", ".join(self._orchestrators.keys()) or "(none)"
            await send_reply(f"Unknown project: `{project_id}`. Available: {available}")
            return

        await self.connect_channel(chat_id, project_id)
        orch = self._orchestrators[project_id]
        await send_reply(f"Connected to **{orch.name}** (`{project_id}`)")

    def hydrate_channel_mapping(self, chat_id: int, project_id: str) -> bool:
        if project_id not in self._orchestrators:
            return False
        self._channel_project[chat_id] = project_id
        return True

    def get_channel_project(self, chat_id: int) -> str | None:
        return self._channel_project.get(chat_id)

    def get_channels_for_project(self, project_id: str) -> list[int]:
        return [chat_id for chat_id, pid in self._channel_project.items() if pid == project_id]

    def has_project(self, project_id: str) -> bool:
        return project_id in self._orchestrators

    def get_project_status(self, project_id: str) -> str:
        orch = self._orchestrators.get(project_id)
        return orch.status if orch else "unknown"

    def list_orchestrators(self) -> list[RemoteOrchestrator]:
        return list(self._orchestrators.values())

    # -- callbacks -----------------------------------------------------

    def set_progress_callback(self, callback: callable):
        self._progress_callback = callback

    # -- health --------------------------------------------------------

    async def check_health(self, project_id: str) -> bool:
        orch = self._orchestrators.get(project_id)
        if not orch:
            return False
        try:
            url = f"{self._daemon_url(orch)}/health"
            async with self._http.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                return resp.status == 200
        except Exception:
            return False
