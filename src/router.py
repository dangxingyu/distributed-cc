"""Router: relay between web UI and remote orchestrator daemons.

Responsibilities:
  1. Route user messages: idle -> POST /task, running -> urgent interrupt or deferred queue
  2. Listen to daemon SSE and ingest callback progress with dedupe
  3. Track channel <-> project mappings (with optional persistence hook)
  4. Local sysadmin session via RouterSession (@router, /setup)
"""

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass

import aiohttp

from .router_session import RouterSession

log = logging.getLogger(__name__)


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _preview(text: str, limit: int = 120) -> str:
    compact = " ".join((text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[:limit] + "..."


@dataclass
class RemoteOrchestrator:
    """A remote orchestrator daemon configuration."""

    project_id: str
    name: str
    host: str | None = None
    broker_port: int = 8200
    project_dir: str = ""
    max_iterations: int = 0
    model: str = ""
    session_model: str = ""
    permission_mode: str = ""
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
        self._last_event_id: dict[str, str] = {}

        # Deferred non-urgent tasks while running
        self._deferred_tasks: dict[str, list[dict]] = {}

        # Callback to web layer
        self._progress_callback = None  # async (project_id, event)

        # Optional callback to persist channel mapping
        self._mapping_persist_callback = None  # async (chat_id, project_id|None)

        # Router sessions — per-channel sysadmin Claude sessions
        self._router_sessions: dict[int, RouterSession] = {}
        self._router_tasks: dict[int, asyncio.Task] = {}
        self._debug_flow = _env_flag("DCC_DEBUG_FLOW")

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
          - split schema: {"machines": [...], "projects": [...]}
          - legacy schema: {"servers": [{name, work_dir, ...}]}
        """
        config_path = os.path.join(self._cwd, "config.json")
        if not os.path.exists(config_path):
            log.info("No config.json in %s, using defaults", self._cwd)
            return

        with open(config_path) as f:
            cfg = json.load(f)
        log.info("Loaded config.json from %s", self._cwd)

        orch_cfg = cfg.get("orchestrator", {})
        default_model = str(orch_cfg.get("model", "")).strip()
        default_session_model = str(orch_cfg.get("session_model", "")).strip()
        default_permission_mode = str(orch_cfg.get("permission_mode", "")).strip()

        def _as_int(value, fallback: int) -> int:
            try:
                return int(value)
            except (TypeError, ValueError):
                return fallback

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
                    broker_port=_as_int(o.get("broker_port", 8200), 8200),
                    project_dir=o.get("project_dir", ""),
                    max_iterations=_as_int(o.get("max_iterations", 0), 0),
                    model=str(o.get("model", default_model)).strip(),
                    session_model=str(o.get("session_model", default_session_model)).strip(),
                    permission_mode=str(o.get("permission_mode", default_permission_mode)).strip(),
                )
                self._orchestrators[project_id] = orch
            return

        servers = cfg.get("servers", [])
        machines = cfg.get("machines", [])
        projects = cfg.get("projects", [])

        # Split schema: project-level entries resolve machine/server connectivity.
        if projects:
            machines_by_name: dict[str, dict] = {}
            for m in machines:
                name = str(m.get("name", "")).strip()
                if name:
                    machines_by_name[name] = m

            servers_by_name: dict[str, dict] = {}
            for s in servers:
                name = str(s.get("name", "")).strip()
                if name:
                    servers_by_name[name] = s

            for p in projects:
                project_id = str(p.get("project_id", "")).strip()
                if not project_id:
                    continue

                machine_name = str(p.get("machine", "")).strip()
                server_name = str(p.get("server", "")).strip()
                machine_ref = machines_by_name.get(machine_name) if machine_name else None
                server_ref = servers_by_name.get(server_name) if server_name else None
                base = machine_ref or server_ref or {}

                host = p.get("host", base.get("host"))
                broker_port = _as_int(p.get("broker_port", base.get("broker_port", 8200)), 8200)
                project_dir = str(
                    p.get(
                        "work_dir",
                        p.get("project_dir", base.get("work_dir", base.get("project_dir", ""))),
                    )
                )
                max_iterations = _as_int(p.get("max_iterations", base.get("max_iterations", 0)), 0)
                model = str(p.get("model", base.get("model", default_model))).strip()
                session_model = str(
                    p.get("session_model", base.get("session_model", default_session_model))
                ).strip()
                permission_mode = str(
                    p.get("permission_mode", base.get("permission_mode", default_permission_mode))
                ).strip()

                orch = RemoteOrchestrator(
                    project_id=project_id,
                    name=str(p.get("name", project_id)),
                    host=host,
                    broker_port=broker_port,
                    project_dir=project_dir,
                    max_iterations=max_iterations,
                    model=model,
                    session_model=session_model,
                    permission_mode=permission_mode,
                )
                self._orchestrators[project_id] = orch

            # Keep plain server entries connectable unless shadowed by project_id.
            for s in servers:
                name = str(s.get("name", "")).strip()
                if not name:
                    continue
                project_id = str(s.get("project_id", name))
                if project_id in self._orchestrators:
                    continue
                orch = RemoteOrchestrator(
                    project_id=project_id,
                    name=str(s.get("name", project_id)),
                    host=s.get("host"),
                    broker_port=_as_int(s.get("broker_port", 8200), 8200),
                    project_dir=str(s.get("work_dir", s.get("project_dir", ""))),
                    max_iterations=_as_int(s.get("max_iterations", 0), 0),
                    model=str(s.get("model", default_model)).strip(),
                    session_model=str(s.get("session_model", default_session_model)).strip(),
                    permission_mode=str(s.get("permission_mode", default_permission_mode)).strip(),
                )
                self._orchestrators[project_id] = orch
            return

        for s in servers:
            name = s.get("name")
            if not name:
                continue
            project_id = s.get("project_id", name)
            orch = RemoteOrchestrator(
                project_id=project_id,
                name=s.get("name", project_id),
                host=s.get("host"),
                broker_port=_as_int(s.get("broker_port", 8200), 8200),
                project_dir=s.get("work_dir", s.get("project_dir", "")),
                max_iterations=_as_int(s.get("max_iterations", 0), 0),
                model=str(s.get("model", default_model)).strip(),
                session_model=str(s.get("session_model", default_session_model)).strip(),
                permission_mode=str(s.get("permission_mode", default_permission_mode)).strip(),
            )
            self._orchestrators[project_id] = orch

    def _daemon_url(self, orch: RemoteOrchestrator) -> str:
        return f"http://127.0.0.1:{orch.broker_port}"

    def _resp_status(self, resp) -> int:
        """Best-effort status code extraction for real responses and test mocks."""
        status = getattr(resp, "status", 200)
        return status if isinstance(status, int) else 200

    async def _resp_json(self, resp):
        """Parse JSON even when Content-Type is wrong, with mock compatibility."""
        try:
            return await resp.json(content_type=None)
        except TypeError:
            # AsyncMock in tests may not accept keyword args
            return await resp.json()

    async def _register_project(self, orch: RemoteOrchestrator):
        url = f"{self._daemon_url(orch)}/register"
        try:
            async with self._http.post(
                url,
                timeout=aiohttp.ClientTimeout(total=5),
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
        backoff_seconds = 2
        while True:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as http:
                    # Sync state on (re)connect to catch events missed during gap
                    await self._sync_daemon_status(orch, http)
                    async with http.get(url) as resp:
                        if resp.status != 200:
                            body = await resp.text()
                            raise RuntimeError(f"SSE connect failed ({resp.status}): {body}")
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
                backoff_seconds = 2
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("SSE connection lost for %s: %s", orch.project_id, e)
                orch.status = "disconnected"
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)

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
        await self._sync_missed_events(orch, http)

    async def _sync_missed_events(self, orch: RemoteOrchestrator, http: aiohttp.ClientSession):
        """Fetch events that may have been missed during temporary disconnects."""
        last_event_id = self._last_event_id.get(orch.project_id, "")
        if not last_event_id:
            return

        try:
            async with http.get(
                f"{self._daemon_url(orch)}/events",
                params={"project_id": orch.project_id, "after_event_id": last_event_id},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                status = self._resp_status(resp)
                if status != 200:
                    return
                payload = await self._resp_json(resp)
        except Exception as e:
            log.debug("Missed-event sync failed for %s: %s", orch.project_id, e)
            return

        events = payload.get("events", [])
        if not isinstance(events, list):
            return
        if payload.get("truncated"):
            log.warning(
                "Replay window truncated for %s; some historical events may be unavailable",
                orch.project_id,
            )
        if self._debug_flow and events:
            log.info(
                "[flow] replay project=%s after=%s count=%s",
                orch.project_id,
                last_event_id,
                len(events),
            )

        for event in events:
            if not isinstance(event, dict):
                continue
            await self.ingest_progress_event(orch.project_id, event, source="replay")

    async def ingest_progress_event(self, project_id: str, event: dict, source: str = "unknown") -> bool:
        """Single ingestion path for both SSE and callback progress events.

        Returns True when processed, False when deduped.
        """
        if self._is_duplicate_event(project_id, event):
            if self._debug_flow:
                log.info(
                    "[flow] dedupe project=%s source=%s event_id=%s type=%s iter=%s",
                    project_id,
                    source,
                    event.get("event_id", ""),
                    event.get("type", ""),
                    event.get("iteration", 0),
                )
            return False

        event_type = event.get("type", "")
        event_id = str(event.get("event_id", "")).strip()
        data = str(event.get("data", ""))
        iteration_raw = event.get("iteration", 0)
        try:
            iteration = int(iteration_raw)
        except (TypeError, ValueError):
            iteration = 0
        if event_id:
            self._last_event_id[project_id] = event_id

        if self._debug_flow:
            log.info(
                "[flow] ingest project=%s source=%s event_id=%s type=%s iter=%s data_len=%s preview=%s",
                project_id,
                source,
                event.get("event_id", ""),
                event_type,
                iteration,
                len(data),
                _preview(data),
            )

        orch = self._orchestrators.get(project_id)
        if orch:
            if event_type == "done":
                orch.status = "done"
            elif event_type == "stopped":
                orch.status = "stopped"
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

        if event_type in ("done", "error", "stopped"):
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

    async def route_message(self, chat_id: int, text: str, send_reply: callable, send_log: callable = None, send_typing: callable = None):
        stripped = text.strip()
        if self._debug_flow:
            log.info(
                "[flow] route chat=%s project=%s text=%s",
                chat_id,
                self._channel_project.get(chat_id),
                _preview(stripped),
            )

        # ── Direct messages (@router, @orchestrator) — always work ──

        addressed_to_router, router_body = self._strip_prefix(stripped, "@router")
        if addressed_to_router:
            if not router_body:
                await send_reply("Message is empty after `@router` prefix.", sender="system")
                return
            await self._handle_router_message(chat_id, router_body, send_reply, send_log, send_typing)
            return

        addressed_to_orchestrator, orchestrator_body = self._strip_prefix(stripped, "@orchestrator")

        # ── Commands ──

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

        # /setup* is a shorthand for @router
        if stripped.startswith("/setup-project") or stripped.startswith("/setup"):
            await self._handle_router_message(chat_id, stripped, send_reply, send_log, send_typing)
            return

        project_id = self._channel_project.get(chat_id)

        if command == "/stop":
            # Stop router task if running, otherwise stop orchestrator task
            router_task = self._router_tasks.get(chat_id)
            if router_task and not router_task.done():
                router_task.cancel()
                await send_reply("Router session stopped.", sender="system")
                return
            if project_id:
                await self._stop_task(chat_id, project_id, send_reply)
            else:
                await send_reply("Nothing to stop.", sender="system")
            return

        if command == "/status":
            if project_id:
                await self._show_status(chat_id, project_id, send_reply)
            else:
                await send_reply("No project connected.", sender="system")
            return

        # ── Plain messages — route based on channel state ──

        if project_id:
            # Project connected → orchestrator
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
                await self._interrupt_task(
                    chat_id,
                    project_id,
                    effective_text,
                    send_reply,
                    urgency="urgent" if addressed_to_orchestrator else "normal",
                )
            elif orch.status in ("idle", "done", "error", "stopped", "unknown"):
                await self._start_task(chat_id, project_id, effective_text, send_reply, send_log)
            elif addressed_to_orchestrator:
                await self._interrupt_task(chat_id, project_id, effective_text, send_reply, urgency="urgent")
            else:
                queue_size = self._enqueue_deferred_task(project_id, chat_id, stripped)
                await send_reply(f"(queued as next task #{queue_size} — use `@orchestrator ...` for urgent interruption)")
        else:
            # No project connected → router (sysadmin brain)
            await self._handle_router_message(chat_id, stripped, send_reply, send_log, send_typing)

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

    def _parse_setup_command(self, text: str) -> dict[str, object]:
        """Parse /setup command forms.

        Supported:
          /setup
          /setup --health
          /setup user@host
          /setup user@host --full
          /setup user@host --manual-tunnel
        """
        body = text[len("/setup") :].strip()
        if not body:
            return {"mode": "health"}

        tokens = body.split()
        flags = [t.lower() for t in tokens if t.startswith("--")]
        args = [t for t in tokens if not t.startswith("--")]

        allowed_flags = {"--full", "--manual-tunnel", "--health"}
        unknown_flags = [f for f in flags if f not in allowed_flags]
        if unknown_flags:
            return {
                "mode": "error",
                "error": (
                    f"Unknown /setup flag(s): {', '.join(unknown_flags)}. "
                    "Usage: `/setup`, `/setup --health`, or "
                    "`/setup user@host [--full|--manual-tunnel]`."
                ),
            }

        if "--health" in flags:
            if args or len(flags) > 1:
                return {
                    "mode": "error",
                    "error": (
                        "`--health` cannot be combined with a host or other flags. "
                        "Use `/setup --health`."
                    ),
                }
            return {"mode": "health"}

        if not args:
            return {
                "mode": "error",
                "error": (
                    "Missing host. Usage: `/setup user@host [--full|--manual-tunnel]` "
                    "or `/setup --health`."
                ),
            }

        host = args[0]
        auto_tunnel = "--manual-tunnel" not in flags
        if "--full" in flags:
            auto_tunnel = True
        return {"mode": "setup", "host": host, "auto_tunnel": auto_tunnel}

    def _parse_setup_project_command(self, text: str) -> dict[str, object]:
        """Parse /setup-project command forms.

        Supported:
          /setup-project <workdir>
          /setup-project <free-form instruction>
        """
        body = text[len("/setup-project") :].strip()
        if not body:
            return {
                "mode": "error",
                "error": (
                    "Missing instruction. Usage: `/setup-project <workdir or instruction>`."
                ),
            }
        return {"mode": "setup_project", "instruction": body}

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

        next_task = queue[0]
        ok, error = await self._start_task_request(project_id, next_task["text"])
        if ok:
            queue.pop(0)
            remaining = len(queue)
            orch.status = "running"
            # Notify via progress callback so the web layer can inform the user
            if self._progress_callback:
                snippet = next_task["text"]
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
            retries = int(next_task.get("retries", 0)) + 1
            next_task["retries"] = retries
            log.warning(
                "Failed to start deferred task for %s (attempt %s): %s",
                project_id,
                retries,
                error,
            )
            if self._progress_callback:
                snippet = next_task["text"]
                try:
                    await self._progress_callback(project_id, {
                        "type": "text",
                        "data": (
                            "@orchestrator Failed to start queued task: "
                            f"{snippet} (attempt {retries}, will retry)"
                        ),
                        "iteration": 0,
                        "ts": time.time(),
                    })
                except Exception:
                    pass

    async def _start_task_request(self, project_id: str, task_text: str) -> tuple[bool, str]:
        orch = self._orchestrators[project_id]
        url = f"{self._daemon_url(orch)}/task"
        payload = {
            "project_id": project_id,
            "task": task_text,
            "max_iterations": orch.max_iterations,
        }
        if orch.model:
            payload["model"] = orch.model
        if orch.session_model:
            payload["session_model"] = orch.session_model
        if orch.permission_mode:
            payload["permission_mode"] = orch.permission_mode
        try:
            async with self._http.post(
                url,
                json=payload,
            ) as resp:
                status = self._resp_status(resp)
                try:
                    result = await self._resp_json(resp)
                except Exception:
                    body = await resp.text()
                    return False, f"Daemon returned non-JSON response (HTTP {status}): {body}"
                if status == 200 and result.get("ok"):
                    return True, ""
                return False, result.get("error", f"HTTP {status}")
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

    async def _interrupt_task(
        self,
        chat_id: int,
        project_id: str,
        message: str,
        send_reply: callable,
        urgency: str = "normal",
    ):
        orch = self._orchestrators[project_id]
        url = f"{self._daemon_url(orch)}/interrupt"

        try:
            async with self._http.post(
                url,
                json={"project_id": project_id, "message": message, "urgency": urgency},
            ) as resp:
                status = self._resp_status(resp)
                try:
                    result = await self._resp_json(resp)
                except Exception:
                    body = await resp.text()
                    await send_reply(f"Interrupt failed (HTTP {status}): {body}")
                    return
                if status == 200 and result.get("ok"):
                    if urgency == "urgent":
                        await send_reply("(urgent interrupt queued — injected after current action)")
                    else:
                        await send_reply("(message queued for orchestrator)")
                else:
                    await send_reply(f"Failed to interrupt: {result.get('error', f'HTTP {status}')}")
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
                status = self._resp_status(resp)
                try:
                    result = await self._resp_json(resp)
                except Exception:
                    body = await resp.text()
                    await send_reply(f"Stop failed (HTTP {status}): {body}")
                    return
                if status == 200 and result.get("ok"):
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
                status_code = self._resp_status(resp)
                try:
                    data = await self._resp_json(resp)
                except Exception:
                    body = await resp.text()
                    await send_reply(f"Status failed (HTTP {status_code}): {body}")
                    return
                if status_code != 200:
                    await send_reply(f"Status failed: {data.get('error', f'HTTP {status_code}')}")
                    return
                status = data.get("status", "unknown")
                iteration = data.get("iteration", 0)
                max_iter = data.get("max_iterations", 0)
                summary = data.get("summary", "")
                queued = len(self._deferred_tasks.get(project_id, []))

                lines = [f"**{orch.name}** ({project_id}): {status}"]
                if status == "running":
                    if isinstance(max_iter, int) and max_iter > 0:
                        lines.append(f"Iteration: {iteration}/{max_iter}")
                    else:
                        lines.append(f"Iteration: {iteration} (no cap)")
                if queued:
                    lines.append(f"Queued tasks: {queued}")
                if summary:
                    lines.append(f"Summary: {summary}")
                await send_reply("\n".join(lines))
        except aiohttp.ClientError as e:
            await send_reply(f"Cannot reach daemon: {e}")

    # -- router session ------------------------------------------------

    async def _safe_send_typing(
        self,
        send_typing,
        active: bool,
        sender: str = "router",
        token: str | None = None,
    ):
        if not send_typing:
            return
        try:
            if token:
                await send_typing(active, sender, token)
            else:
                await send_typing(active, sender)
        except TypeError:
            # Backward compatibility with callbacks that only accept (active, sender).
            await send_typing(active, sender)

    async def _handle_router_message(self, chat_id: int, text: str, send_reply: callable, send_log: callable = None, send_typing: callable = None):
        if chat_id not in self._router_sessions:
            self._router_sessions[chat_id] = RouterSession(cwd=self._cwd)

        session = self._router_sessions[chat_id]

        # Wrap send_reply to tag setup messages as "router" sender
        async def setup_reply(msg: str):
            await send_reply(msg, sender="router")

        session.set_callbacks(progress=setup_reply, log=send_log)

        stripped = text.strip()
        if stripped.startswith("/setup-project"):
            setup_project_req = self._parse_setup_project_command(stripped)
            if setup_project_req.get("mode") == "error":
                await setup_reply(str(setup_project_req.get("error", "Invalid /setup-project command.")))
                return
            instruction = str(setup_project_req["instruction"])
            prompt = (
                "PROJECT SETUP MODE (/setup-project)\n\n"
                "SCOPE: Project configuration ONLY. Do NOT deploy/redeploy daemons. "
                "Do NOT create/modify tunnels.\n\n"
                "USER INSTRUCTION (verbatim):\n"
                f"{instruction}\n\n"
                "Objective:\n"
                "- Create/update exactly one project entry so user can run `/connect <project_id>`.\n"
                "- Reuse existing machine + daemon + tunnel.\n\n"
                "Steps:\n"
                "1) Read config.json, and read config.md if present.\n"
                "2) Resolve one concrete absolute work_dir from the user instruction.\n"
                "3) If no concrete work_dir is derivable, return NOT READY with one precise "
                "clarifying question. Do not edit config/files before resolution.\n"
                "4) Pick host and broker_port from existing machine setup.\n"
                "5) Ensure work_dir exists and is writable.\n"
                "6) Ensure work_dir/CLAUDE.md exists.\n"
                "   - If CLAUDE.md already exists: NEVER overwrite whole file.\n"
                "   - Preserve existing content and only append/update a managed section.\n"
                "7) Update exactly one project entry in config.json (show diff first).\n"
                "8) Verify daemon health on selected broker_port.\n\n"
                "Response format:\n"
                "- READY: project_id, work_dir, host, broker_port, `/connect <project_id>`, evidence.\n"
                "- NOT READY: failing check + exact command/output + next action.\n"
            )
        elif stripped.startswith("/setup"):
            setup_req = self._parse_setup_command(stripped)
            mode = setup_req.get("mode")
            if mode == "error":
                await setup_reply(str(setup_req.get("error", "Invalid /setup command.")))
                return
            if mode == "setup":
                host = str(setup_req["host"])
                auto_tunnel = bool(setup_req.get("auto_tunnel", True))
                if auto_tunnel:
                    prompt = (
                        f"MACHINE SETUP MODE (/setup): {host}\n\n"
                        "SCOPE: Server infrastructure ONLY. Do NOT add project entries.\n\n"
                        "CRITICAL boundary: This command sets up machine connectivity only.\n"
                        "- Do NOT create or modify project/work_dir entries.\n"
                        "- Do NOT create or edit work_dir/CLAUDE.md in this mode.\n\n"
                        "If config.md exists, read it first and honor explicit user preferences.\n\n"
                        "Steps:\n"
                        "1) Probe remote environment via SSH and install prerequisites.\n"
                        "2) Deploy and start daemon persistently.\n"
                        "3) Update config.json machine-level connectivity (host + unique broker_port).\n"
                        "4) Start or refresh LOCAL background SSH tunnel:\n"
                        f"   ssh -N -L BROKER_PORT:localhost:8200 -R 9120:localhost:9120 "
                        f"-o ServerAliveInterval=30 -o ServerAliveCountMax=3 "
                        f"-o ExitOnForwardFailure=yes -o BatchMode=yes {host}\n"
                        "   Use tmux/nohup background mode only; no blocking foreground process.\n"
                        "5) Verify local health: curl http://127.0.0.1:BROKER_PORT/health\n\n"
                        "Response format:\n"
                        "- READY: host, broker_port, daemon status, tunnel status.\n"
                        "- NOT READY: failing check + exact command/output + next action.\n"
                        "- Include one line reminder: `Next: run /setup-project <workdir or instruction>`.\n"
                    )
                else:
                    prompt = (
                        f"MACHINE SETUP MODE (/setup --manual-tunnel): {host}\n\n"
                        "SCOPE: Server infrastructure ONLY. Do NOT add project entries.\n\n"
                        "CRITICAL boundary: machine setup only; no project/work_dir/CLAUDE.md updates.\n"
                        "If config.md exists, read it first.\n\n"
                        "Steps:\n"
                        "1) Probe environment and deploy daemon.\n"
                        "2) Update config.json machine-level connectivity (host + unique broker_port).\n"
                        "3) Print exact tunnel command for user to run manually.\n"
                        "4) After user confirms tunnel is up, verify health.\n\n"
                        "Response format:\n"
                        "- READY: host, broker_port, tunnel command, daemon status.\n"
                        "- NOT READY: failing check + exact command/output + next action.\n"
                        "- Include one line reminder: `Next: run /setup-project <workdir or instruction>`.\n"
                    )
            else:
                prompt = (
                    "Run health checks for configured daemon endpoints from config.json. "
                    "Read config.md first if present. "
                    "Return a concise table: host, broker_port, health(up/down), error(if any)."
                )
        else:
            prompt = stripped

        typing_token = f"router-{uuid.uuid4().hex}"

        async def _run():
            await self._safe_send_typing(send_typing, True, "router", typing_token)
            try:
                result = await session.run(prompt)
                # Reload config before announcing completion so immediate /connect sees new projects.
                self.reload_config()
                if session.should_emit_final_result(result):
                    await setup_reply(result)
            except Exception as e:
                log.exception("Router session error: %s", e)
                await send_reply(f"Router error: {e}", sender="system")
            finally:
                await self._safe_send_typing(send_typing, False, "router", typing_token)

        # Cancel any in-flight setup task for this channel before starting a new one
        old_task = self._router_tasks.get(chat_id)
        if old_task and not old_task.done():
            old_task.cancel()

        self._router_tasks[chat_id] = asyncio.create_task(_run())

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

    async def refresh_project_status(self, project_id: str) -> str:
        """Best-effort daemon status refresh for UI reconnect/switch scenarios."""
        orch = self._orchestrators.get(project_id)
        if not orch:
            return "unknown"
        if not self._http:
            return orch.status

        try:
            if not await self._ensure_registered(orch):
                orch.status = "disconnected"
                return orch.status

            url = f"{self._daemon_url(orch)}/status?project_id={project_id}"
            async with self._http.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                status_code = self._resp_status(resp)
                if status_code != 200:
                    if status_code >= 500:
                        orch.status = "disconnected"
                    return orch.status

                data = await self._resp_json(resp)
                status = str(data.get("status", "unknown")).strip() or "unknown"
                orch.status = status
                await self._sync_missed_events(orch, self._http)
                return status
        except aiohttp.ClientError:
            orch.status = "disconnected"
            return orch.status
        except Exception:
            return orch.status

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
