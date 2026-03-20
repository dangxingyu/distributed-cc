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
from .runtime_config import normalized_runtime_fragment, resolve_runtime_settings

log = logging.getLogger(__name__)

NO_PROJECT_CONNECTED_MESSAGE = "No project connected. Use /connect <project-id> or /setup-project <workdir>."


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _preview(text: str, limit: int = 120) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[:limit] + "..."


def _norm_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalized_machines(cfg: dict) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for item in cfg.get("machines", []):
        if not isinstance(item, dict):
            continue
        rows.append(
            (
                _norm_text(item.get("name")),
                _norm_text(item.get("host")),
                _norm_text(item.get("broker_port")),
            )
        )
    rows.sort()
    return rows


def _normalized_projects(cfg: dict) -> list[tuple[str, str, str, str, str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str, str, str, str, str]] = []
    for item in cfg.get("projects", []):
        if not isinstance(item, dict):
            continue
        rows.append(
            (
                _norm_text(item.get("project_id")),
                _norm_text(item.get("name")),
                _norm_text(item.get("machine")),
                _norm_text(item.get("server")),
                _norm_text(item.get("host")),
                _norm_text(item.get("broker_port")),
                _norm_text(item.get("work_dir")),
                _norm_text(item.get("project_dir")),
                normalized_runtime_fragment(item),
            )
        )
    rows.sort()
    return rows


def _normalized_orchestrators(cfg: dict) -> list[tuple[str, str, str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str, str, str]] = []
    for item in cfg.get("orchestrators", []):
        if not isinstance(item, dict):
            continue
        rows.append(
            (
                _norm_text(item.get("project_id")),
                _norm_text(item.get("name")),
                _norm_text(item.get("host")),
                _norm_text(item.get("broker_port")),
                _norm_text(item.get("project_dir")),
                _norm_text(item.get("work_dir")),
                normalized_runtime_fragment(item),
            )
        )
    rows.sort()
    return rows


def _normalized_servers_project_fields(cfg: dict) -> list[tuple[str, str, str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str, str, str]] = []
    for item in cfg.get("servers", []):
        if not isinstance(item, dict):
            continue
        rows.append(
            (
                _norm_text(item.get("name")),
                _norm_text(item.get("project_id")),
                _norm_text(item.get("work_dir")),
                _norm_text(item.get("project_dir")),
                _norm_text(item.get("machine")),
                _norm_text(item.get("server")),
                normalized_runtime_fragment(item),
            )
        )
    rows.sort()
    return rows


def _normalized_servers_machine_fields(cfg: dict) -> list[tuple[str, str, str, str]]:
    rows: list[tuple[str, str, str, str]] = []
    for item in cfg.get("servers", []):
        if not isinstance(item, dict):
            continue
        rows.append(
            (
                _norm_text(item.get("name")),
                _norm_text(item.get("host")),
                _norm_text(item.get("broker_port")),
                normalized_runtime_fragment(item),
            )
        )
    rows.sort()
    return rows


def _normalized_orchestrator_defaults(cfg: dict) -> tuple[str, str, str, str, str, str, str]:
    defaults = cfg.get("orchestrator")
    if not isinstance(defaults, dict):
        return ("", "", "", "", "", "", "")
    settings = resolve_runtime_settings(defaults)
    return (
        settings.provider,
        settings.model,
        settings.session_model,
        settings.permission_mode,
        settings.sandbox_mode,
        settings.approval_policy,
        normalized_runtime_fragment(defaults),
    )


MISSING_CONFIG_SNAPSHOT = "__DCC_CONFIG_WAS_MISSING__"
DEFERRED_RETRY_INITIAL_SECONDS = 0.5
DEFERRED_RETRY_MAX_SECONDS = 10.0
CHANNEL_CONTEXT_HISTORY_MAX = 24
DOCTOR_CONTEXT_MESSAGES = 10
PLUGIN_CONTEXT_MESSAGES = 10
AUTO_RECOVER_TUNNEL = os.getenv("DCC_AUTO_RECOVER_TUNNEL", "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


@dataclass
class RemoteOrchestrator:
    """A remote orchestrator daemon configuration."""

    project_id: str
    name: str
    host: str | None = None
    broker_port: int = 8200
    project_dir: str = ""
    max_iterations: int = 0
    provider: str = "claude"
    model: str = ""
    session_model: str = ""
    permission_mode: str = ""
    sandbox_mode: str = ""
    approval_policy: str = ""
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
        self._deferred_retry_tasks: dict[str, asyncio.Task] = {}

        # Listener lists (multiple frontends)
        self._progress_listeners: list[callable] = []
        self._mapping_persist_listeners: list[callable] = []

        # Channel source cache (frontend that owns each channel)
        self._channel_source: dict[int, str | None] = {}

        # Router sessions — per-channel sysadmin Claude sessions
        self._router_sessions: dict[int, RouterSession] = {}
        self._router_tasks: dict[int, asyncio.Task] = {}
        self._channel_context_history: dict[int, deque[str]] = {}
        self._debug_flow = _env_flag("DCC_DEBUG_FLOW")
        self._last_health_detail: dict[str, str] = {}

    async def init(self):
        self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        self._load_config()

    async def close(self):
        for task in self._sse_tasks.values():
            task.cancel()
        for task in self._deferred_retry_tasks.values():
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
                runtime = resolve_runtime_settings(cfg.get("orchestrator"), o)
                orch = RemoteOrchestrator(
                    project_id=project_id,
                    name=o.get("name", project_id),
                    host=o.get("host"),
                    broker_port=_as_int(o.get("broker_port", 8200), 8200),
                    project_dir=o.get("project_dir", ""),
                    max_iterations=_as_int(o.get("max_iterations", 0), 0),
                    provider=runtime.provider,
                    model=runtime.model,
                    session_model=runtime.session_model,
                    permission_mode=runtime.permission_mode,
                    sandbox_mode=runtime.sandbox_mode,
                    approval_policy=runtime.approval_policy,
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
                runtime = resolve_runtime_settings(cfg.get("orchestrator"), base, p)

                orch = RemoteOrchestrator(
                    project_id=project_id,
                    name=str(p.get("name", project_id)),
                    host=host,
                    broker_port=broker_port,
                    project_dir=project_dir,
                    max_iterations=max_iterations,
                    provider=runtime.provider,
                    model=runtime.model,
                    session_model=runtime.session_model,
                    permission_mode=runtime.permission_mode,
                    sandbox_mode=runtime.sandbox_mode,
                    approval_policy=runtime.approval_policy,
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
                runtime = resolve_runtime_settings(cfg.get("orchestrator"), s)
                orch = RemoteOrchestrator(
                    project_id=project_id,
                    name=str(s.get("name", project_id)),
                    host=s.get("host"),
                    broker_port=_as_int(s.get("broker_port", 8200), 8200),
                    project_dir=str(s.get("work_dir", s.get("project_dir", ""))),
                    max_iterations=_as_int(s.get("max_iterations", 0), 0),
                    provider=runtime.provider,
                    model=runtime.model,
                    session_model=runtime.session_model,
                    permission_mode=runtime.permission_mode,
                    sandbox_mode=runtime.sandbox_mode,
                    approval_policy=runtime.approval_policy,
                )
                self._orchestrators[project_id] = orch
            return

        for s in servers:
            name = s.get("name")
            if not name:
                continue
            project_id = s.get("project_id", name)
            runtime = resolve_runtime_settings(cfg.get("orchestrator"), s)
            orch = RemoteOrchestrator(
                project_id=project_id,
                name=s.get("name", project_id),
                host=s.get("host"),
                broker_port=_as_int(s.get("broker_port", 8200), 8200),
                project_dir=s.get("work_dir", s.get("project_dir", "")),
                max_iterations=_as_int(s.get("max_iterations", 0), 0),
                provider=runtime.provider,
                model=runtime.model,
                session_model=runtime.session_model,
                permission_mode=runtime.permission_mode,
                sandbox_mode=runtime.sandbox_mode,
                approval_policy=runtime.approval_policy,
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
        if not self._http:
            # Can happen in tests or before Router.init(); retry lazily later.
            orch.status = "unknown"
            return

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

    async def _ensure_registered(
        self,
        orch: RemoteOrchestrator,
        require_health: bool = True,
    ) -> bool:
        """Lazily register with daemon on first interaction. Returns True if ready."""
        if orch.status not in ("unknown", "disconnected"):
            return True

        # Re-hydrated channels often hit this path right after local router restart.
        # If the SSH tunnel died with the previous process, run health first so
        # auto-recover can recreate the tunnel before attempting /register.
        if require_health and self._http:
            if not await self.check_health(orch.project_id):
                orch.status = "disconnected"
                return False

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

        for listener in self._progress_listeners:
            try:
                await listener(project_id, event)
            except Exception:
                log.warning("Progress listener failed", exc_info=True)

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

    async def route_message(
        self,
        chat_id: int,
        text: str,
        send_reply: callable,
        send_log: callable = None,
        send_typing: callable = None,
        user_message_id: str | None = None,
    ):
        stripped = text.strip()
        if stripped:
            self._record_channel_context(chat_id, stripped)
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

        # Setup/diagnostics commands are shorthand for @router
        lower_stripped = stripped.lower()
        if (
            lower_stripped.startswith("/setup-project")
            or lower_stripped.startswith("/setup")
            or lower_stripped.startswith("/doctor")
            or lower_stripped.startswith("/upgrade-check")
            or lower_stripped.startswith("/orchestrator_plugin")
            or lower_stripped.startswith("/orchestrator-plugin")
            or lower_stripped.startswith("/worker_plugin")
            or lower_stripped.startswith("/worker-plugin")
        ):
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
                await send_reply(NO_PROJECT_CONNECTED_MESSAGE, sender="system")
            return

        if command == "/queue":
            if project_id:
                await self._handle_queue_command(project_id, command_arg, send_reply)
            else:
                await send_reply(NO_PROJECT_CONNECTED_MESSAGE, sender="system")
            return

        # ── Plain messages — route based on channel state ──

        if project_id:
            # Project connected → orchestrator
            orch = self._orchestrators.get(project_id)
            if not orch:
                await send_reply(f"Project `{project_id}` not found in config.")
                return

            if not await self._ensure_registered(orch):
                detail = self._last_health_detail.get(project_id, "")
                detail_suffix = f" Detail: `{_preview(detail, 180)}`." if detail else ""
                await send_reply(
                    f"Cannot reach daemon for `{project_id}`. Is the daemon running and SSH tunnel open?"
                    f"{detail_suffix} Try `/doctor {project_id}` for systematic diagnosis."
                )
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
                queue_size = self._enqueue_deferred_task(
                    project_id,
                    chat_id,
                    stripped,
                    message_id=user_message_id,
                )
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
        if cmd in ("/connect", "/stop", "/status", "/queue"):
            return cmd, arg
        return None, ""

    def _parse_positive_index(self, raw: str) -> int | None:
        try:
            idx = int(raw)
        except (TypeError, ValueError):
            return None
        if idx <= 0:
            return None
        return idx

    def _format_queue_preview(self, project_id: str) -> str:
        queue = self._deferred_tasks.get(project_id, [])
        if not queue:
            return f"Queue for `{project_id}` is empty."

        lines = [f"Queue for `{project_id}` ({len(queue)}):"]
        for i, item in enumerate(queue, 1):
            text = str(item.get("text", "")).strip()
            if not text:
                text = "(empty)"
            retries = int(item.get("retries", 0) or 0)
            meta = []
            chat_id = item.get("chat_id")
            if isinstance(chat_id, int):
                meta.append(f"ch:{chat_id}")
            if retries > 0:
                meta.append(f"retry:{retries}")
            suffix = f" ({', '.join(meta)})" if meta else ""
            lines.append(f"{i}. {text}{suffix}")
        return "\n".join(lines)

    async def _handle_queue_command(self, project_id: str, command_arg: str, send_reply: callable):
        queue = self._deferred_tasks.setdefault(project_id, [])
        arg = (command_arg or "").strip()
        if not arg or arg.lower() == "list":
            await send_reply(self._format_queue_preview(project_id))
            return

        parts = arg.split()
        sub = parts[0].lower()

        if sub == "clear":
            cleared = len(queue)
            queue.clear()
            self._cancel_deferred_retry(project_id)
            await send_reply(f"Cleared {cleared} queued task(s) for `{project_id}`.")
            return

        if sub in ("delete", "del", "rm"):
            if len(parts) != 2:
                await send_reply("Usage: `/queue delete <index>`")
                return
            idx = self._parse_positive_index(parts[1])
            if idx is None or idx > len(queue):
                await send_reply(f"Invalid queue index: `{parts[1]}`")
                return
            removed = queue.pop(idx - 1)
            await send_reply(f"Removed queued task #{idx}: {removed.get('text', '')}")
            return

        if sub == "edit":
            if len(parts) < 3:
                await send_reply("Usage: `/queue edit <index> <new text>`")
                return
            idx = self._parse_positive_index(parts[1])
            if idx is None or idx > len(queue):
                await send_reply(f"Invalid queue index: `{parts[1]}`")
                return
            new_text = arg.split(None, 2)[2].strip()
            if not new_text:
                await send_reply("New queued text cannot be empty.")
                return
            queue[idx - 1]["text"] = new_text
            queue[idx - 1]["ts"] = time.time()
            await send_reply(f"Updated queued task #{idx}.")
            return

        if sub == "move":
            if len(parts) != 3:
                await send_reply("Usage: `/queue move <from_index> <to_index>`")
                return
            from_idx = self._parse_positive_index(parts[1])
            to_idx = self._parse_positive_index(parts[2])
            if from_idx is None or to_idx is None:
                await send_reply("Indices must be positive integers.")
                return
            if from_idx > len(queue) or to_idx > len(queue):
                await send_reply(
                    f"Indices out of range. Queue size is {len(queue)}."
                )
                return
            if from_idx == to_idx:
                await send_reply("No change: source and destination indices are the same.")
                return
            item = queue.pop(from_idx - 1)
            queue.insert(to_idx - 1, item)
            await send_reply(f"Moved queued task #{from_idx} -> #{to_idx}.")
            return

        await send_reply(
            "Unknown `/queue` action. Use: `/queue`, `/queue edit <idx> <text>`, "
            "`/queue delete <idx>`, `/queue move <from> <to>`, `/queue clear`."
        )

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

    def _parse_plugin_setup_command(self, text: str) -> dict[str, object]:
        stripped = text.strip()
        lower = stripped.lower()
        prefixes = (
            ("/orchestrator_plugin", "orchestrator"),
            ("/orchestrator-plugin", "orchestrator"),
            ("/worker_plugin", "worker"),
            ("/worker-plugin", "worker"),
        )
        for prefix, role in prefixes:
            if lower.startswith(prefix):
                instruction = stripped[len(prefix):].strip()
                if not instruction:
                    return {
                        "mode": "error",
                        "error": (
                            f"Missing instruction. Usage: `{prefix} <plugin instruction>`."
                        ),
                    }
                return {
                    "mode": "plugin_setup",
                    "role": role,
                    "instruction": instruction,
                    "prefix": prefix,
                }
        return {"mode": "none"}

    def _config_path(self) -> str:
        return os.path.join(self._cwd, "config.json")

    def _capture_config_snapshot(self) -> tuple[str | None, dict | None]:
        path = self._config_path()
        if not os.path.exists(path):
            return MISSING_CONFIG_SNAPSHOT, {}
        try:
            with open(path) as f:
                raw = f.read()
        except Exception:
            return None, None
        try:
            parsed = json.loads(raw)
        except Exception:
            return raw, None
        if not isinstance(parsed, dict):
            return raw, None
        return raw, parsed

    def _restore_config_snapshot(self, raw: str | None):
        if raw is None:
            return
        try:
            path = self._config_path()
            if raw == MISSING_CONFIG_SNAPSHOT:
                if os.path.exists(path):
                    os.remove(path)
                return
            with open(path, "w") as f:
                f.write(raw)
        except Exception:
            log.warning("Failed to restore config.json snapshot", exc_info=True)

    def _check_setup_scope_guard(
        self,
        scope_mode: str | None,
        before_raw: str | None,
        before_cfg: dict | None,
    ) -> tuple[bool, str]:
        if not scope_mode or before_cfg is None:
            return True, ""

        _after_raw, after_cfg = self._capture_config_snapshot()
        if after_cfg is None:
            # Keep setup resilient when config.json is missing or malformed.
            return True, ""

        violations: list[str] = []
        if scope_mode == "machine_setup":
            if _normalized_projects(before_cfg) != _normalized_projects(after_cfg):
                violations.append("`projects` entries changed")
            if _normalized_orchestrators(before_cfg) != _normalized_orchestrators(after_cfg):
                violations.append("`orchestrators` entries changed")
            if _normalized_servers_project_fields(before_cfg) != _normalized_servers_project_fields(after_cfg):
                violations.append("project-related fields in `servers` changed")
        elif scope_mode == "project_setup":
            if _normalized_machines(before_cfg) != _normalized_machines(after_cfg):
                violations.append("`machines` entries changed")
            if _normalized_servers_machine_fields(before_cfg) != _normalized_servers_machine_fields(after_cfg):
                violations.append("machine connectivity fields in `servers` changed")
            if _normalized_orchestrator_defaults(before_cfg) != _normalized_orchestrator_defaults(after_cfg):
                violations.append("top-level `orchestrator` defaults changed")

        if not violations:
            return True, ""

        self._restore_config_snapshot(before_raw)
        if scope_mode == "machine_setup":
            return (
                False,
                "Scope guard blocked out-of-scope config changes during `/setup` "
                f"(machine setup only): {', '.join(violations)}. "
                "Reverted `config.json` to pre-setup state. "
                "Re-run `/setup` for machine setup, then use `/setup-project` separately.",
            )
        return (
            False,
            "Scope guard blocked out-of-scope config changes during `/setup-project` "
            f"(project setup only): {', '.join(violations)}. "
            "Reverted `config.json` to pre-setup state. "
            "Re-run `/setup-project` without editing machine connectivity.",
        )

    def _build_plugin_setup_prompt(self, chat_id: int, role: str, instruction: str) -> str:
        role = role.strip().lower()
        if role not in ("orchestrator", "worker"):
            raise ValueError(f"Invalid plugin role: {role}")

        project_id = self._channel_project.get(chat_id)
        orch = self._orchestrators.get(project_id) if project_id else None
        if orch:
            connected = (
                f"{project_id} (name={orch.name}, host={orch.host}, broker_port={orch.broker_port}, "
                f"project_dir={orch.project_dir}, status={orch.status})"
            )
        elif project_id:
            connected = f"{project_id} (missing from current config)"
        else:
            connected = "(none)"

        known_projects = ", ".join(sorted(self._orchestrators.keys())) or "(none)"
        raw_history = list(self._channel_context_history.get(chat_id) or [])
        recent = [
            line for line in raw_history
            if not (
                line.lower().startswith("/orchestrator_plugin")
                or line.lower().startswith("/orchestrator-plugin")
                or line.lower().startswith("/worker_plugin")
                or line.lower().startswith("/worker-plugin")
            )
        ]
        recent = recent[-PLUGIN_CONTEXT_MESSAGES:]
        recent_lines = "\n".join(f"- {line}" for line in recent) if recent else "- (none)"

        role_file = f".claude/mcp/{role}.json"
        cmd_name = f"/{role}_plugin"

        return (
            f"MCP PLUGIN SETUP MODE ({cmd_name})\n\n"
            "Goal: configure role-specific MCP servers for one project without editing config.json.\n\n"
            "Context snapshot:\n"
            f"- channel_id: {chat_id}\n"
            f"- target_role: {role}\n"
            f"- target_plugin_file: {role_file}\n"
            f"- connected_project: {connected}\n"
            f"- known_projects: {known_projects}\n"
            f"- user_instruction: {instruction}\n"
            "- recent_channel_messages:\n"
            f"{recent_lines}\n\n"
            "Workflow:\n"
            "1) Read config.json and config.md (if present).\n"
            "2) Resolve EXACTLY one target project_id by priority: explicit user instruction > connected project.\n"
            "   - If ambiguous or missing, ask one precise clarifying question and STOP.\n"
            "3) Determine host, broker_port, and work_dir from that project.\n"
            "4) Configure role-specific MCP file at `<work_dir>/.claude/mcp/<role>.json`:\n"
            "   - Canonical schema:\n"
            "     {\n"
            "       \"mcp_servers\": {\n"
            "         \"name\": {\"command\": \"...\", \"args\": [\"...\"], \"env\": {\"K\": \"V\"}}\n"
            "       }\n"
            "     }\n"
            "   - Keep existing unrelated server entries unless user asks to replace.\n"
            "   - Do NOT overwrite `.claude/roles/*.md` in this flow.\n"
            "5) Install/check prerequisites for requested MCP servers on target machine.\n"
            "   - Validate executable commands (npx/python binaries, package availability).\n"
            "6) Validate plugin JSON syntax and show a concise diff/evidence.\n"
            "7) Verify daemon endpoint health for target broker_port:\n"
            "   - `curl http://127.0.0.1:<broker_port>/health`\n"
            "   - require `status == \"ok\"` and non-empty `daemon`.\n"
            "8) Explain activation behavior:\n"
            "   - orchestrator plugin: effective next orchestrator query cycle\n"
            "   - worker plugin: effective on next worker assignment\n\n"
            "Strict rules:\n"
            "- Do NOT change machine/project mappings in config.json unless user explicitly asks.\n"
            "- Do NOT leak secrets in chat; reference env vars for sensitive values.\n\n"
            "Response format:\n"
            "- READY: project_id, role, plugin_file, servers configured, validation evidence, activation note.\n"
            "- NOT READY: failing check + exact command/output + next action.\n"
        )

    def _record_channel_context(self, chat_id: int, text: str):
        compact = " ".join((text or "").split())
        if not compact:
            return
        if len(compact) > 500:
            compact = compact[:500] + "..."
        history = self._channel_context_history.get(chat_id)
        if history is None:
            history = deque(maxlen=CHANNEL_CONTEXT_HISTORY_MAX)
            self._channel_context_history[chat_id] = history
        history.append(compact)

    def _build_doctor_prompt(self, chat_id: int, hint: str) -> str:
        project_id = self._channel_project.get(chat_id)
        orch = self._orchestrators.get(project_id) if project_id else None
        host_value = str(orch.host).strip() if orch and orch.host else "<host>"
        broker_port_value = str(orch.broker_port) if orch else "<broker_port>"

        if orch:
            connected = (
                f"{project_id} (name={orch.name}, host={orch.host}, broker_port={orch.broker_port}, "
                f"project_dir={orch.project_dir}, status={orch.status})"
            )
        elif project_id:
            connected = f"{project_id} (missing from current config)"
        else:
            connected = "(none)"

        known_projects = ", ".join(sorted(self._orchestrators.keys())) or "(none)"
        raw_history = list(self._channel_context_history.get(chat_id) or [])
        recent = [
            line
            for line in raw_history
            if not line.lower().startswith("/doctor")
        ]
        recent = recent[-DOCTOR_CONTEXT_MESSAGES:]
        recent_lines = "\n".join(f"- {line}" for line in recent) if recent else "- (none)"

        hint_text = hint or "(none)"
        recommended_cmd = "uv run python tools/doctor.py --timeout 5"
        if project_id:
            recommended_cmd = (
                f"uv run python tools/doctor.py --project {project_id} --timeout 5"
            )

        return (
            "DOCTOR MODE (/doctor)\n\n"
            "Goal: diagnose communication and remote-setup issues for this channel.\n\n"
            "Context snapshot:\n"
            f"- channel_id: {chat_id}\n"
            f"- connected_project: {connected}\n"
            f"- known_projects: {known_projects}\n"
            f"- user_hint: {hint_text}\n"
            "- recent_channel_messages:\n"
            f"{recent_lines}\n\n"
            f"Recommended first command: `{recommended_cmd}`\n\n"
            "Workflow:\n"
            "1) Determine target using priority: explicit user_hint > connected_project > recent messages.\n"
            "2) Run systematic checks first with local tool:\n"
            "   - if a project_id is known: `uv run python tools/doctor.py --project <project_id> --timeout 5`\n"
            "   - else: `uv run python tools/doctor.py --timeout 5`\n"
            "3) MANDATORY quick checks (run before concluding root cause when host/port are known):\n"
            f"   - local endpoint owner: `lsof -nP -iTCP:{broker_port_value} -sTCP:LISTEN`\n"
            f"   - local tunnel process: `ps aux | rg \"ssh.*-L {broker_port_value}:localhost:8200\" | rg -v rg`\n"
            f"   - remote daemon process: `ssh {host_value} \"ps -ef | rg orchestrator_daemon.py | rg -v rg\"`\n"
            f"   - remote daemon port owner: `ssh {host_value} \"lsof -nP -iTCP:8200 -sTCP:LISTEN || ss -ltnp | rg :8200\"`\n"
            "4) For each failing check, run focused follow-ups and classify root cause:\n"
            "   - tunnel down / wrong broker_port mapping\n"
            "   - wrong service on port (legacy broker or non-orchestrator daemon)\n"
            "   - daemon process absent/crashed or remote port conflict\n"
            "   - register/status failure from project_dir mismatch/permissions\n"
            "5) If safe, apply minimal fixes and re-run doctor once to verify.\n"
            "6) Return concise structured output:\n"
            "   - TARGET\n"
            "   - CHECK RESULTS\n"
            "   - ROOT CAUSE\n"
            "   - FIX APPLIED (or exact next commands)\n"
            "   - FINAL STATUS\n"
            "Hard rule: do not claim 'tunnel down' unless quick-check evidence includes BOTH local endpoint owner and local tunnel process checks.\n"
        )

    def _build_upgrade_check_prompt(self, chat_id: int, hint: str) -> str:
        project_id = self._channel_project.get(chat_id)
        orch = self._orchestrators.get(project_id) if project_id else None

        if orch:
            connected = (
                f"{project_id} (name={orch.name}, host={orch.host}, broker_port={orch.broker_port}, "
                f"project_dir={orch.project_dir}, status={orch.status})"
            )
        elif project_id:
            connected = f"{project_id} (missing from current config)"
        else:
            connected = "(none)"

        known_projects = ", ".join(sorted(self._orchestrators.keys())) or "(none)"
        raw_history = list(self._channel_context_history.get(chat_id) or [])
        recent = [
            line
            for line in raw_history
            if not (
                line.lower().startswith("/doctor")
                or line.lower().startswith("/upgrade-check")
            )
        ]
        recent = recent[-DOCTOR_CONTEXT_MESSAGES:]
        recent_lines = "\n".join(f"- {line}" for line in recent) if recent else "- (none)"
        hint_text = hint or "(none)"

        return (
            "UPGRADE CHECK MODE (/upgrade-check)\n\n"
            "Goal: check whether remote daemon/runtime is aligned with latest upstream (GitHub), "
            "not only with local files.\n\n"
            "Context snapshot:\n"
            f"- channel_id: {chat_id}\n"
            f"- connected_project: {connected}\n"
            f"- known_projects: {known_projects}\n"
            f"- user_hint: {hint_text}\n"
            "- recent_channel_messages:\n"
            f"{recent_lines}\n\n"
            "Workflow:\n"
            "1) Resolve target host/project by priority: explicit user_hint > connected_project > recent messages.\n"
            "2) Determine latest upstream version from GitHub:\n"
            "   - `git config --get remote.origin.url`\n"
            "   - `git ls-remote <origin-url> refs/heads/main`\n"
            "   - fetch latest daemon source hash from GitHub:\n"
            "     `curl -fsSL https://raw.githubusercontent.com/<owner>/<repo>/main/tools/orchestrator_daemon.py | shasum -a 256`\n"
            "3) Collect local reference hash (for context only):\n"
            "   - `shasum -a 256 tools/orchestrator_daemon.py`\n"
            "4) Collect remote deployed daemon identity/version:\n"
            "   - `ssh <host> \"shasum -a 256 ~/.distributed-cc/orchestrator_daemon.py || sha256sum ~/.distributed-cc/orchestrator_daemon.py\"`\n"
            "   - `ssh <host> \"ps -ef | grep orchestrator_daemon.py | grep -v grep\"`\n"
            "   - `curl http://127.0.0.1:<broker_port>/health` (must include `status == \\\"ok\\\"` and non-empty `daemon`)\n"
            "5) Collect key remote runtime versions:\n"
            "   - Claude CLI version (`claude --version`) and/or Codex CLI version (`codex --version`) when available\n"
            "   - daemon venv package versions (`claude-agent-sdk`, `aiohttp`, `mcp`)\n"
            "6) Compare remote vs GitHub latest and classify: UP_TO_DATE / DRIFTED / UNKNOWN.\n\n"
            "Strict rules:\n"
            "- Do NOT upgrade automatically in this command.\n"
            "- If drift is detected, provide minimal exact upgrade commands and risks.\n"
            "- Then ask exactly: `Proceed with upgrade now? (yes/no)`.\n"
            "- Only execute upgrade after explicit user confirmation in a follow-up message.\n\n"
            "Response format:\n"
            "- TARGET\n"
            "- VERSION SNAPSHOT\n"
            "- DRIFT DETECTED (yes/no + evidence)\n"
            "- UPGRADE PLAN (if needed)\n"
            "- ACTION NEEDED\n"
        )

    def _enqueue_deferred_task(
        self,
        project_id: str,
        chat_id: int,
        text: str,
        message_id: str | None = None,
    ) -> int:
        queue = self._deferred_tasks.setdefault(project_id, [])
        queue.append(
            {
                "chat_id": chat_id,
                "text": text,
                "message_id": (message_id or "").strip() or None,
                "ts": time.time(),
            }
        )
        return len(queue)

    def pop_last_deferred_task_for_channel(self, chat_id: int) -> dict | None:
        """Pop newest deferred task authored by this channel."""
        project_id = self._channel_project.get(chat_id)
        if not project_id:
            return None
        queue = self._deferred_tasks.get(project_id) or []
        if not queue:
            return None

        for idx in range(len(queue) - 1, -1, -1):
            item = queue[idx]
            if item.get("chat_id") != chat_id:
                continue
            queue.pop(idx)
            if not queue:
                self._cancel_deferred_retry(project_id)
            text = str(item.get("text", "")).strip()
            if not text:
                return None
            message_id_raw = str(item.get("message_id", "")).strip()
            return {
                "text": text,
                "message_id": message_id_raw or None,
                "chat_id": chat_id,
                "project_id": project_id,
            }
        return None

    def restore_deferred_task_for_channel(
        self,
        chat_id: int,
        text: str,
        message_id: str | None = None,
    ) -> int | None:
        project_id = self._channel_project.get(chat_id)
        if not project_id:
            return None
        body = str(text or "").strip()
        if not body:
            return None
        return self._enqueue_deferred_task(
            project_id=project_id,
            chat_id=chat_id,
            text=body,
            message_id=message_id,
        )

    def _cancel_deferred_retry(self, project_id: str):
        task = self._deferred_retry_tasks.pop(project_id, None)
        if task and not task.done():
            task.cancel()

    def _deferred_retry_delay(self, retries: int) -> float:
        retries = max(1, retries)
        delay = DEFERRED_RETRY_INITIAL_SECONDS * (2 ** (retries - 1))
        return min(delay, DEFERRED_RETRY_MAX_SECONDS)

    def _is_retryable_start_error(self, error: str) -> bool:
        lowered = str(error or "").lower()
        return "already has a running task" in lowered or "http 409" in lowered

    def _schedule_deferred_retry(self, project_id: str, retries: int):
        existing = self._deferred_retry_tasks.get(project_id)
        if existing and not existing.done():
            return

        delay = self._deferred_retry_delay(retries)

        async def _retry():
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                return

            current = self._deferred_retry_tasks.get(project_id)
            if current is asyncio.current_task():
                self._deferred_retry_tasks.pop(project_id, None)

            try:
                await self._maybe_start_deferred_task(project_id)
            except Exception:
                log.warning("Deferred retry failed for %s", project_id, exc_info=True)

        self._deferred_retry_tasks[project_id] = asyncio.create_task(_retry())

    async def _maybe_start_deferred_task(self, project_id: str):
        queue = self._deferred_tasks.get(project_id) or []
        if not queue:
            self._cancel_deferred_retry(project_id)
            return

        orch = self._orchestrators.get(project_id)
        if not orch or orch.status == "running":
            return

        next_task = queue[0]
        ok, error = await self._start_task_request(project_id, next_task["text"])
        if ok:
            self._cancel_deferred_retry(project_id)
            queue.pop(0)
            remaining = len(queue)
            orch.status = "running"
            # Notify via progress listeners so frontends can inform the user
            snippet = next_task["text"]
            note = f"Starting queued task: {snippet}"
            if remaining:
                note += f" ({remaining} more in queue)"
            deferred_event = {
                "type": "text",
                "data": f"@orchestrator {note}",
                "iteration": 0,
                "ts": time.time(),
            }
            for listener in self._progress_listeners:
                try:
                    await listener(project_id, deferred_event)
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
            if self._is_retryable_start_error(error):
                # Daemon can briefly report "already running" right after a done event.
                # Keep queue head and retry with backoff instead of surfacing a user-facing failure.
                self._schedule_deferred_retry(project_id, retries)
                return

            snippet = next_task["text"]
            fail_event = {
                "type": "text",
                "data": (
                    "@orchestrator Failed to start queued task: "
                    f"{snippet} (attempt {retries})"
                ),
                "iteration": 0,
                "ts": time.time(),
            }
            for listener in self._progress_listeners:
                try:
                    await listener(project_id, fail_event)
                except Exception:
                    pass

    async def _start_task_request(self, project_id: str, task_text: str) -> tuple[bool, str]:
        orch = self._orchestrators[project_id]
        url = f"{self._daemon_url(orch)}/task"
        payload = {
            "project_id": project_id,
            "task": task_text,
            "max_iterations": orch.max_iterations,
            "provider": orch.provider or "claude",
        }
        if orch.model:
            payload["model"] = orch.model
        if orch.session_model:
            payload["session_model"] = orch.session_model
        if orch.permission_mode:
            payload["permission_mode"] = orch.permission_mode
        if orch.sandbox_mode:
            payload["sandbox_mode"] = orch.sandbox_mode
        if orch.approval_policy:
            payload["approval_policy"] = orch.approval_policy
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
            await send_reply(NO_PROJECT_CONNECTED_MESSAGE)
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
            await send_reply(NO_PROJECT_CONNECTED_MESSAGE)
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
        lower_stripped = stripped.lower()
        scope_mode: str | None = None
        plugin_req = self._parse_plugin_setup_command(stripped)
        if plugin_req.get("mode") == "error":
            await setup_reply(str(plugin_req.get("error", "Invalid plugin setup command.")))
            return
        if plugin_req.get("mode") == "plugin_setup":
            role = str(plugin_req.get("role", ""))
            instruction = str(plugin_req.get("instruction", ""))
            prompt = self._build_plugin_setup_prompt(chat_id, role, instruction)
        elif lower_stripped.startswith("/setup-project"):
            scope_mode = "project_setup"
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
                "8) Verify daemon health on selected broker_port.\n"
                "   - `curl http://127.0.0.1:BROKER_PORT/health` must return JSON with:\n"
                "     `status == \"ok\"` and non-empty `daemon` field.\n"
                "   - If response only has legacy keys (for example `server` without `daemon`),\n"
                "     treat as NOT READY (wrong service on this port).\n"
                "9) Verify remote process identity on the target host:\n"
                "   - process/command line must include `orchestrator_daemon.py`.\n"
                "   - if `remote_broker.py` or other legacy service owns port 8200, return NOT READY.\n\n"
                "Response format:\n"
                "- READY: project_id, work_dir, host, broker_port, `/connect <project_id>`, evidence.\n"
                "- NOT READY: failing check + exact command/output + next action.\n"
            )
        elif lower_stripped.startswith("/setup"):
            setup_req = self._parse_setup_command(stripped)
            mode = setup_req.get("mode")
            if mode == "error":
                await setup_reply(str(setup_req.get("error", "Invalid /setup command.")))
                return
            if mode == "setup":
                scope_mode = "machine_setup"
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
                        "   - Prefer user-level systemd service (Restart=always) when available.\n"
                        "   - Fallback to tmux; last resort nohup.\n"
                        "3) Update config.json machine-level connectivity (host + unique broker_port).\n"
                        "4) Start or refresh LOCAL background SSH tunnel:\n"
                        f"   ssh -N -L BROKER_PORT:localhost:8200 -R 9120:localhost:9120 "
                        f"-o ServerAliveInterval=30 -o ServerAliveCountMax=3 "
                        f"-o ExitOnForwardFailure=yes -o BatchMode=yes {host}\n"
                        "   - Prefer autossh or user-level systemd service for self-healing.\n"
                        "   - Fallback to tmux/nohup; never leave blocking foreground process.\n"
                        "5) Verify local health: curl http://127.0.0.1:BROKER_PORT/health\n"
                        "   - Require JSON `status == \"ok\"` and non-empty `daemon` field.\n"
                        "   - If payload only has `server` or misses `daemon`, treat as NOT READY.\n"
                        "6) Verify remote process identity:\n"
                        "   - command line includes `orchestrator_daemon.py`.\n"
                        "   - ensure no stale `remote_broker.py` is owning port 8200.\n\n"
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
                        "   - Prefer user-level systemd service when available; else tmux/nohup.\n"
                        "2) Update config.json machine-level connectivity (host + unique broker_port).\n"
                        "3) Print exact tunnel command for user to run manually.\n"
                        "4) After user confirms tunnel is up, verify health/signature.\n"
                        "   - `curl .../health` must include `status == \"ok\"` and non-empty `daemon`.\n"
                        "5) Verify remote process identity:\n"
                        "   - command line includes `orchestrator_daemon.py` and not legacy broker.\n\n"
                        "Response format:\n"
                        "- READY: host, broker_port, tunnel command, daemon status.\n"
                        "- NOT READY: failing check + exact command/output + next action.\n"
                        "- Include one line reminder: `Next: run /setup-project <workdir or instruction>`.\n"
                    )
            else:
                prompt = (
                    "Run health checks for configured daemon endpoints from config.json. "
                    "Read config.md first if present. "
                    "For each endpoint, verify `/health` JSON signature: "
                    "`status == \"ok\"` and non-empty `daemon`. "
                    "Flag legacy/invalid payloads (for example `server` without `daemon`) as DOWN. "
                    "Return a concise table: host, broker_port, health(up/down), daemon_name, error(if any)."
                )
        elif lower_stripped.startswith("/doctor"):
            doctor_hint = stripped[len("/doctor"):].strip()
            prompt = self._build_doctor_prompt(chat_id, doctor_hint)
        elif lower_stripped.startswith("/upgrade-check"):
            upgrade_hint = stripped[len("/upgrade-check"):].strip()
            prompt = self._build_upgrade_check_prompt(chat_id, upgrade_hint)
        else:
            prompt = stripped

        typing_token = f"router-{uuid.uuid4().hex}"
        snapshot_raw, snapshot_cfg = self._capture_config_snapshot() if scope_mode else (None, None)

        async def _run():
            await self._safe_send_typing(send_typing, True, "router", typing_token)
            try:
                result = await session.run(prompt)
                scope_ok, scope_error = self._check_setup_scope_guard(
                    scope_mode,
                    snapshot_raw,
                    snapshot_cfg,
                )
                # Reload config before announcing completion so immediate /connect sees new projects.
                self.reload_config()
                if not scope_ok:
                    await setup_reply(scope_error)
                    return
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
            if self._http:
                asyncio.create_task(self._register_project(self._orchestrators[pid]))

        for pid in old_ids - new_ids:
            task = self._sse_tasks.pop(pid, None)
            if task:
                task.cancel()
            self._cancel_deferred_retry(pid)

    # -- channel/project mapping --------------------------------------

    def add_mapping_persist_listener(self, callback):
        if callback not in self._mapping_persist_listeners:
            self._mapping_persist_listeners.append(callback)

    def remove_mapping_persist_listener(self, callback):
        try:
            self._mapping_persist_listeners.remove(callback)
        except ValueError:
            pass

    async def _persist_channel_mapping(self, chat_id: int, project_id: str | None):
        for listener in self._mapping_persist_listeners:
            try:
                await listener(chat_id, project_id)
            except Exception:
                log.warning("Mapping persist listener failed", exc_info=True)

    async def connect_channel(self, chat_id: int, project_id: str, source: str | None = None) -> bool:
        if project_id not in self._orchestrators:
            return False
        self._channel_project[chat_id] = project_id
        if source is not None:
            self._channel_source[chat_id] = source
        await self._persist_channel_mapping(chat_id, project_id)
        return True

    async def disconnect_channel(self, chat_id: int):
        self._channel_project.pop(chat_id, None)
        self._channel_source.pop(chat_id, None)
        await self._persist_channel_mapping(chat_id, None)

    async def _connect_channel(self, chat_id: int, project_id: str, send_reply: callable):
        if project_id not in self._orchestrators:
            should_reload = not self._orchestrators
            router_task = self._router_tasks.get(chat_id)
            if router_task and not router_task.done():
                should_reload = True
            if should_reload:
                self.reload_config()
        if project_id not in self._orchestrators:
            available = ", ".join(self._orchestrators.keys()) or "(none)"
            router_task = self._router_tasks.get(chat_id)
            if router_task and not router_task.done():
                await send_reply(
                    f"Project `{project_id}` is not visible yet (router setup still running). "
                    f"Wait for setup READY, then retry `/connect {project_id}`. "
                    f"Available now: {available}"
                )
            else:
                await send_reply(f"Unknown project: `{project_id}`. Available: {available}")
            return

        orch = self._orchestrators[project_id]
        if self._http:
            health_ok = await self.check_health(project_id)
            if not health_ok:
                orch.status = "disconnected"
                detail = self._last_health_detail.get(project_id, "")
                detail_suffix = f" Detail: `{_preview(detail, 180)}`." if detail else ""
                await send_reply(
                    f"Cannot connect `{project_id}`: daemon is unreachable at "
                    f"http://127.0.0.1:{orch.broker_port}. Check tunnel/daemon, then retry."
                    f"{detail_suffix} Try `/doctor {project_id}` for systematic diagnosis."
                )
                return

            if not await self._ensure_registered(orch, require_health=False):
                await send_reply(
                    f"Cannot connect `{project_id}`: daemon is up but project registration failed. "
                    f"Check project_dir `{orch.project_dir}` and daemon logs, then retry."
                )
                return

        await self.connect_channel(chat_id, project_id)
        await send_reply(
            f"Connected to **{orch.name}** (`{project_id}`). "
            "Note: task queue and runtime are project-scoped and shared across channels "
            "connected to the same project."
        )

    def hydrate_channel_mapping(self, chat_id: int, project_id: str, source: str | None = None) -> bool:
        if project_id not in self._orchestrators:
            return False
        self._channel_project[chat_id] = project_id
        if source is not None:
            self._channel_source[chat_id] = source
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

    # -- listeners -----------------------------------------------------

    def add_progress_listener(self, callback: callable):
        if callback not in self._progress_listeners:
            self._progress_listeners.append(callback)

    def remove_progress_listener(self, callback: callable):
        try:
            self._progress_listeners.remove(callback)
        except ValueError:
            pass

    # -- channel source cache ------------------------------------------

    def set_channel_source(self, chat_id: int, source: str | None):
        self._channel_source[chat_id] = source

    def get_channel_source(self, chat_id: int) -> str | None:
        return self._channel_source.get(chat_id)

    # -- health --------------------------------------------------------

    async def check_health(self, project_id: str) -> bool:
        orch = self._orchestrators.get(project_id)
        if not orch:
            self._last_health_detail[project_id] = "unknown project_id"
            return False
        ok, detail = await self._check_health_once(orch)
        if ok:
            self._last_health_detail[project_id] = detail
            return True

        if self._should_attempt_tunnel_recover(orch, detail):
            recovered, recover_detail = await self._try_auto_recover_tunnel(orch)
            if recovered:
                ok2, detail2 = await self._check_health_once(orch)
                if ok2:
                    self._last_health_detail[project_id] = (
                        f"auto-recovered tunnel; {detail2}"
                    )
                    return True
                detail = f"{detail2}; auto-recover attempted ({recover_detail})"
            elif recover_detail:
                detail = f"{detail}; auto-recover failed ({recover_detail})"

        self._last_health_detail[project_id] = detail
        return False

    async def _check_health_once(self, orch: RemoteOrchestrator) -> tuple[bool, str]:
        if not self._http:
            return False, "router HTTP client is not initialized"
        try:
            url = f"{self._daemon_url(orch)}/health"
            async with self._http.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    return False, f"HTTP {resp.status}: {_preview(body, 160)}"
                payload = await self._resp_json(resp)
                if not isinstance(payload, dict):
                    return False, "health payload is not JSON object"
                status = str(payload.get("status", "")).strip().lower()
                daemon_name = str(payload.get("daemon", "")).strip()
                if status != "ok":
                    return False, f"status={payload.get('status')!r}"
                # Signature guard: old/foreign services may return {"status":"ok"}
                # but do not expose orchestrator_daemon identity.
                if not daemon_name:
                    if "server" in payload:
                        return False, "missing `daemon` (legacy/foreign service)"
                    return False, "missing `daemon` field"
                return True, f"daemon={daemon_name}"
        except Exception as e:
            return False, str(e)

    def _should_attempt_tunnel_recover(self, orch: RemoteOrchestrator, detail: str) -> bool:
        if not AUTO_RECOVER_TUNNEL:
            return False
        if not orch.host:
            return False
        lowered = (detail or "").lower()
        if "cannot connect to host 127.0.0.1" in lowered:
            return True
        if "connect call failed ('127.0.0.1'" in lowered:
            return True
        if "connection refused" in lowered and "127.0.0.1" in lowered:
            return True
        return False

    async def _try_auto_recover_tunnel(self, orch: RemoteOrchestrator) -> tuple[bool, str]:
        if not orch.host:
            return False, "missing host"

        cmd = [
            "ssh",
            "-fNT",
            "-L",
            f"{orch.broker_port}:localhost:8200",
            "-R",
            "9120:localhost:9120",
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=3",
            "-o",
            "ExitOnForwardFailure=yes",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            str(orch.host),
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.communicate()
                return False, "ssh tunnel command timed out"
            out = (stdout or b"").decode(errors="replace").strip()
            err = (stderr or b"").decode(errors="replace").strip()
            if proc.returncode == 0:
                detail = out or err or "ssh tunnel started"
                log.info(
                    "Auto-recovered SSH tunnel for %s on local port %s",
                    orch.host,
                    orch.broker_port,
                )
                return True, _preview(detail, 160)
            detail = err or out or f"ssh exited with code {proc.returncode}"
            return False, _preview(detail, 200)
        except FileNotFoundError:
            return False, "ssh command not found"
        except Exception as e:
            return False, str(e)
