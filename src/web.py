"""Web chat frontend.

Multi-channel chat UI served over HTTP + WebSocket. Each channel is connected
to a remote orchestrator daemon via Router. Progress events are persisted for
all mapped channels and streamed to currently viewing clients.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from pathlib import Path

from aiohttp import WSMsgType, web

from .router import Router
from .progress_rules import chat_messages_from_event, log_entries_from_event
from .store import Store

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


class WebChat:
    def _status_label(self, status: str | None) -> str:
        normalized = str(status or "").strip().lower()
        if normalized == "running":
            return "Running"
        if normalized == "busy":
            return "Busy"
        if normalized == "stuck":
            return "Stuck"
        if normalized == "done":
            return "Done"
        if normalized == "stopped":
            return "Stopped"
        if normalized == "error":
            return "Error"
        if normalized == "idle":
            return "Idle"
        if normalized == "disconnected":
            return "Disconnected"
        if normalized == "unconnected":
            return "Unconnected"
        return normalized or "Unknown"

    def _runtime_label(self, provider: str | None, permission_mode: str | None, sandbox_mode: str | None, approval_policy: str | None) -> str:
        bits: list[str] = []
        provider_text = str(provider or "").strip().lower()
        if provider_text:
            bits.append("Codex" if provider_text == "codex" else "Claude")
        sandbox_text = str(sandbox_mode or "").strip()
        approval_text = str(approval_policy or "").strip()
        permission_text = str(permission_mode or "").strip()
        if sandbox_text:
            bits.append(sandbox_text)
        if approval_text:
            bits.append(f"approval:{approval_text}")
        elif permission_text:
            bits.append(permission_text)
        return " · ".join(bits)

    def _project_runtime_payload(self, project_id: str | None) -> dict:
        if not project_id:
            return {}
        orch = self._router._orchestrators.get(project_id)
        if not orch:
            return {}
        return {
            "project_id": orch.project_id,
            "name": orch.name,
            "host": orch.host,
            "status": orch.status,
            "project_dir": orch.project_dir,
            "provider": orch.provider,
            "model": orch.model,
            "session_model": orch.session_model,
            "permission_mode": orch.permission_mode,
            "sandbox_mode": orch.sandbox_mode,
            "approval_policy": orch.approval_policy,
            "queue_size": len(self._router._deferred_tasks.get(project_id, [])),
            "health_detail": self._router._last_health_detail.get(project_id, ""),
        }

    def __init__(
        self,
        router: Router,
        store: Store,
        host: str = "127.0.0.1",
        port: int = 8080,
    ):
        self._router = router
        self._store = store
        self._host = host
        self._port = port
        self._runner: web.AppRunner | None = None
        self._app: web.Application | None = None

        # Multi-client websocket state
        self._clients: dict[str, web.WebSocketResponse] = {}
        self._client_active_channel: dict[str, int | None] = {}
        self._background_tasks: set[asyncio.Task] = set()
        self._debug_flow = _env_flag("DCC_DEBUG_FLOW")

        self.SOURCE = "web"

        # Wire listeners
        self._router.add_progress_listener(self._handle_progress)
        self._router.add_mapping_persist_listener(self._persist_channel_mapping)

    async def start(self):
        await self._hydrate_channel_mappings()

        self._app = web.Application()
        self._app.router.add_get("/", self._handle_index)
        self._app.router.add_get("/api/history", self._handle_history)
        self._app.router.add_get("/api/channels", self._handle_channels_list)
        self._app.router.add_post("/api/channels", self._handle_channels_create)
        self._app.router.add_delete("/api/channels/{id}", self._handle_channels_delete)
        self._app.router.add_get("/api/channels/{id}/members", self._handle_channels_members)
        self._app.router.add_get("/api/logs", self._handle_logs)
        self._app.router.add_get("/api/projects", self._handle_projects_list)
        self._app.router.add_get("/api/setup-notes", self._handle_setup_notes)
        self._app.router.add_get("/ws", self._handle_ws)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        log.info("Web chat on http://%s:%s", self._host, self._port)

    async def stop(self):
        self._router.remove_progress_listener(self._handle_progress)
        self._router.remove_mapping_persist_listener(self._persist_channel_mapping)

        for ws in list(self._clients.values()):
            if not ws.closed:
                await ws.close()
        self._clients.clear()
        self._client_active_channel.clear()

        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        if self._runner:
            await self._runner.cleanup()

    async def _hydrate_channel_mappings(self):
        mapping = await self._store.get_channel_project_map_by_source(
            self.SOURCE,
            include_legacy=True,
        )
        for chat_id, project_id in mapping.items():
            self._router.hydrate_channel_mapping(chat_id, project_id, source=self.SOURCE)

    async def _persist_channel_mapping(self, chat_id: int, project_id: str | None):
        await self._store.set_channel_project(chat_id, project_id)

    # -- HTTP handlers -------------------------------------------------

    async def _handle_index(self, request: web.Request) -> web.Response:
        return web.FileResponse(STATIC_DIR / "index.html")

    async def _handle_history(self, request: web.Request) -> web.Response:
        channel_str = request.query.get("channel")
        if not channel_str:
            return web.json_response({"error": "missing channel param"}, status=400)
        try:
            channel_id = int(channel_str)
        except ValueError:
            return web.json_response({"error": "invalid channel id"}, status=400)

        limit_raw = request.query.get("limit")
        limit: int | None = None
        if limit_raw is not None and limit_raw != "":
            try:
                limit = int(limit_raw)
            except ValueError:
                return web.json_response({"error": "invalid limit"}, status=400)
            if limit <= 0:
                return web.json_response({"error": "limit must be > 0"}, status=400)

        messages = await self._store.get_recent_messages(channel_id, limit=limit)
        return web.json_response(messages)

    async def _handle_channels_list(self, request: web.Request) -> web.Response:
        channels = await self._store.get_channel_list(
            source=self.SOURCE,
            include_legacy=True,
        )
        for ch in channels:
            project_id = self._router.get_channel_project(ch["id"])
            ch["project_id"] = project_id
            ch["project_status"] = (
                self._router.get_project_status(project_id) if project_id else "unconnected"
            )
        return web.json_response(channels)

    async def _handle_channels_create(self, request: web.Request) -> web.Response:
        body = await request.json()
        name = body.get("name", "").strip()
        if not name:
            return web.json_response({"error": "name is required"}, status=400)

        project_id = body.get("project_id", "").strip()
        if project_id and not self._router.has_project(project_id):
            return web.json_response({"error": f"unknown project_id: {project_id}"}, status=400)

        chat_id = await self._store.create_channel(name, project_id=project_id or None, source=self.SOURCE)

        if project_id:
            await self._router.connect_channel(chat_id, project_id, source=self.SOURCE)
        else:
            self._router.set_channel_source(chat_id, self.SOURCE)

        return web.json_response({"id": chat_id, "name": name, "project_id": project_id or None})

    async def _handle_channels_delete(self, request: web.Request) -> web.Response:
        try:
            channel_id = int(request.match_info["id"])
        except (ValueError, KeyError):
            return web.json_response({"error": "invalid channel id"}, status=400)

        await self._store.delete_channel(channel_id)
        await self._router.disconnect_channel(channel_id)

        for client_id, active_channel in list(self._client_active_channel.items()):
            if active_channel == channel_id:
                self._client_active_channel[client_id] = None

        return web.json_response({"ok": True})

    async def _handle_channels_members(self, request: web.Request) -> web.Response:
        try:
            channel_id = int(request.match_info["id"])
        except (ValueError, KeyError):
            return web.json_response({"error": "invalid channel id"}, status=400)

        viewer_count = sum(1 for ch in self._client_active_channel.values() if ch == channel_id)
        members = [
            {"name": "You", "role": "user", "detail": f"{viewer_count} active viewer(s)"},
            {"name": "Router", "role": "router", "detail": "local relay"},
        ]

        project_id = self._router.get_channel_project(channel_id)
        if project_id:
            orch = self._router._orchestrators.get(project_id)
            if orch:
                queue_size = len(self._router._deferred_tasks.get(project_id, []))
                runtime_label = self._runtime_label(
                    orch.provider,
                    orch.permission_mode,
                    orch.sandbox_mode,
                    orch.approval_policy,
                )
                location_bits = [bit for bit in [project_id, orch.host, orch.project_dir] if bit]
                health_detail = self._router._last_health_detail.get(project_id, "")
                members.append(
                    {
                        "name": f"Orchestrator ({orch.name})",
                        "role": "orchestrator",
                        "detail": self._status_label(orch.status),
                        "detail2": runtime_label,
                        "detail3": health_detail or " · ".join(location_bits),
                        "queue_size": queue_size,
                    }
                )
                worker_detail = "Running" if orch.status == "running" else "Busy" if orch.status == "busy" else "Idle"
                if orch.status == "stuck":
                    worker_detail = "Stuck"
                members.append(
                    {
                        "name": f"Worker ({orch.name})",
                        "role": "worker",
                        "detail": worker_detail,
                        "detail2": runtime_label,
                        "detail3": f"Queued tasks: {queue_size}" if queue_size else "Queue empty",
                    }
                )

        return web.json_response(members)

    async def _handle_logs(self, request: web.Request) -> web.Response:
        channel_str = request.query.get("channel")
        if not channel_str:
            return web.json_response({"error": "missing channel param"}, status=400)
        try:
            channel_id = int(channel_str)
        except ValueError:
            return web.json_response({"error": "invalid channel id"}, status=400)

        limit_raw = request.query.get("limit")
        limit: int | None = None
        if limit_raw is not None and limit_raw != "":
            try:
                limit = int(limit_raw)
            except ValueError:
                return web.json_response({"error": "invalid limit"}, status=400)
            if limit <= 0:
                return web.json_response({"error": "limit must be > 0"}, status=400)

        logs = await self._store.get_logs(channel_id, limit=limit)
        return web.json_response(logs)

    async def _handle_projects_list(self, request: web.Request) -> web.Response:
        result = []
        for orch in self._router.list_orchestrators():
            result.append(self._project_runtime_payload(orch.project_id))
        return web.json_response(result)

    async def _handle_setup_notes(self, request: web.Request) -> web.Response:
        cwd = Path(getattr(self._router, "_cwd", os.getcwd()))
        notes_path = cwd / "config.md"
        notes = ""
        if notes_path.exists():
            try:
                notes = notes_path.read_text(encoding="utf-8").strip()
            except OSError:
                notes = ""
        return web.json_response({"notes": notes})

    # -- WebSocket -----------------------------------------------------

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        client_id = uuid.uuid4().hex
        self._clients[client_id] = ws
        self._client_active_channel[client_id] = None

        log.info("WebSocket client connected: %s", client_id)

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    await self._handle_ws_message(client_id, msg.data)
                elif msg.type == WSMsgType.ERROR:
                    log.error("WebSocket error for %s: %s", client_id, ws.exception())
        finally:
            self._clients.pop(client_id, None)
            self._client_active_channel.pop(client_id, None)
            log.info("WebSocket client disconnected: %s", client_id)

        return ws

    async def _handle_ws_message(self, client_id: str, raw: str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            await self._ws_send_to_client(client_id, {"type": "error", "text": "Invalid JSON"})
            return

        msg_type = data.get("type")

        if msg_type == "switch_channel":
            channel_id = data.get("channel_id")
            if channel_id is None:
                await self._ws_send_to_client(client_id, {"type": "error", "text": "missing channel_id"})
                return

            try:
                channel_id = int(channel_id)
            except (TypeError, ValueError):
                await self._ws_send_to_client(client_id, {"type": "error", "text": "invalid channel_id"})
                return

            channel_ids = set(await self._store.get_all_channel_ids())
            if channel_id not in channel_ids:
                await self._ws_send_to_client(client_id, {"type": "error", "text": "unknown channel_id"})
                return

            self._client_active_channel[client_id] = channel_id
            project_id = self._router.get_channel_project(channel_id)
            status = self._router.get_project_status(project_id) if project_id else "unconnected"
            await self._ws_send_to_client(
                client_id,
                {
                    "type": "channel_switched",
                    "channel_id": channel_id,
                    "project_id": project_id,
                    "project_status": status,
                    "project": self._project_runtime_payload(project_id),
                },
            )

            if project_id:
                self._start_background_task(
                    self._refresh_switched_channel_status(
                        client_id=client_id,
                        channel_id=channel_id,
                        project_id=project_id,
                        initial_status=status,
                    )
                )
            return

        if msg_type == "message":
            channel_id = self._client_active_channel.get(client_id)
            if channel_id is None:
                await self._ws_send_to_client(client_id, {"type": "error", "text": "No channel selected"})
                return

            text = data.get("text", "").strip()
            if not text:
                return
            client_msg_id = data.get("client_msg_id")

            try:
                message_id = await self._store.add_message(channel_id, "user", text, sender="user")
            except Exception as e:
                await self._ws_send_to_client(client_id, {"type": "error", "text": f"Failed to persist message: {e}"})
                return

            if client_msg_id:
                await self._ws_send_to_client(
                    client_id,
                    {
                        "type": "message_ack",
                        "client_msg_id": str(client_msg_id),
                        "message_id": message_id,
                        "ts": time.time(),
                    },
                )

            async def send_reply(msg: str, sender: str = "system"):
                ts = time.time()
                await self._store.add_message(channel_id, "assistant", msg, sender=sender)
                await self._ws_send_to_channel(channel_id, {"type": "reply", "text": msg, "sender": sender, "ts": ts})

            async def send_log(msg: str):
                ts = time.time()
                await self._store.add_log(channel_id, msg)
                await self._ws_send_to_channel(channel_id, {"type": "log", "text": msg, "ts": ts})

            async def send_typing(active: bool, sender: str = "router", token: str | None = None):
                payload = {"type": "typing", "active": active, "sender": sender}
                if token:
                    payload["token"] = token
                await self._ws_send_to_channel(channel_id, payload)

            async def _route_message():
                try:
                    try:
                        await self._router.route_message(
                            channel_id,
                            text,
                            send_reply,
                            send_log,
                            send_typing,
                            user_message_id=message_id,
                        )
                    except TypeError as e:
                        # Compatibility for tests/custom mocks that haven't added
                        # the optional `user_message_id` parameter yet.
                        if "user_message_id" not in str(e):
                            raise
                        await self._router.route_message(channel_id, text, send_reply, send_log, send_typing)
                except Exception as e:
                    log.exception("Routing failed for channel %s", channel_id)
                    ts = time.time()
                    line = f"[ERROR] Routing failure: {e}"
                    await self._store.add_log(channel_id, line)
                    await self._ws_send_to_channel(channel_id, {"type": "log", "text": line, "ts": ts})
                    await self._store.add_message(
                        channel_id,
                        "assistant",
                        f"Routing failure: {e}",
                        sender="system",
                    )
                    await self._ws_send_to_channel(
                        channel_id,
                        {"type": "reply", "text": f"Routing failure: {e}", "sender": "system", "ts": ts},
                    )

            self._start_background_task(_route_message())
            return

        if msg_type == "recall_queued_message":
            channel_id = self._client_active_channel.get(client_id)
            if channel_id is None:
                await self._ws_send_to_client(client_id, {"type": "error", "text": "No channel selected"})
                return
            recalled = self._router.pop_last_deferred_task_for_channel(channel_id)
            retracted = False
            if recalled is not None:
                recalled_text = str(recalled.get("text", "")).strip()
                recalled_message_id = str(recalled.get("message_id", "")).strip() or None
                if recalled_message_id:
                    retracted = await self._store.pop_user_message_by_id(channel_id, recalled_message_id)
                if not retracted:
                    retracted = await self._store.pop_last_user_message(channel_id, recalled_text)
                await self._ws_send_to_channel(
                    channel_id,
                    {
                        "type": "message_retracted",
                        "sender": "user",
                        "text": recalled_text,
                        "message_id": recalled_message_id,
                        "ok": retracted,
                        "ts": time.time(),
                    },
                )
            await self._ws_send_to_client(
                client_id,
                {
                    "type": "queue_recall",
                    "ok": recalled is not None,
                    "retracted": retracted,
                    "text": (str(recalled.get("text", "")).strip() if recalled else ""),
                    "message_id": (str(recalled.get("message_id", "")).strip() if recalled else ""),
                    "ts": time.time(),
                },
            )
            return

        if msg_type == "restore_retracted_message":
            channel_id = self._client_active_channel.get(client_id)
            if channel_id is None:
                await self._ws_send_to_client(client_id, {"type": "error", "text": "No channel selected"})
                return
            text = str(data.get("text", "")).strip()
            message_id = str(data.get("message_id", "")).strip() or None
            if not text:
                await self._ws_send_to_client(client_id, {"type": "error", "text": "missing text"})
                return
            queue_size = self._router.restore_deferred_task_for_channel(
                channel_id,
                text,
                message_id=message_id,
            )
            if queue_size is None:
                await self._ws_send_to_client(client_id, {"type": "error", "text": "No project connected"})
                return
            restored_message_id = await self._store.add_message(
                channel_id,
                "user",
                text,
                sender="user",
                message_id=message_id,
            )
            await self._ws_send_to_channel(
                channel_id,
                {
                    "type": "message_restored",
                    "sender": "user",
                    "text": text,
                    "message_id": restored_message_id,
                    "queue_size": queue_size,
                    "ts": time.time(),
                },
            )
            await self._ws_send_to_client(
                client_id,
                {
                    "type": "queue_restore",
                    "ok": True,
                    "text": text,
                    "message_id": restored_message_id,
                    "queue_size": queue_size,
                    "ts": time.time(),
                },
            )
            return

        await self._ws_send_to_client(client_id, {"type": "error", "text": f"Unknown message type: {msg_type}"})

    async def _refresh_switched_channel_status(
        self,
        client_id: str,
        channel_id: int,
        project_id: str,
        initial_status: str,
    ):
        """Refresh project status after fast switch ACK and push update if changed."""
        try:
            status = await self._router.refresh_project_status(project_id)
        except Exception:
            return

        if status == initial_status:
            return
        if self._client_active_channel.get(client_id) != channel_id:
            return

        await self._ws_send_to_client(
            client_id,
            {
                "type": "channel_status",
                "channel_id": channel_id,
                "project_id": project_id,
                "project_status": status,
                "project": self._project_runtime_payload(project_id),
                "iteration": 0,
                "data": "",
            },
        )

    def _start_background_task(self, coro):
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    # -- Progress callback --------------------------------------------

    async def _handle_progress(self, project_id: str, event: dict):
        chat_ids = self._router.get_channels_for_project(project_id)
        if not chat_ids:
            if self._debug_flow:
                log.info(
                    "[flow/web] drop progress (no channel mapping) project=%s event_id=%s type=%s",
                    project_id,
                    event.get("event_id", ""),
                    event.get("type", ""),
                )
            return

        # Only emit to channels owned by this frontend (or legacy untagged ones)
        owned_chat_ids = [
            cid for cid in chat_ids
            if self._router.get_channel_source(cid) in (self.SOURCE, None)
        ]
        if not owned_chat_ids:
            return

        event_type = event.get("type", "")
        data_text = event.get("data", "")
        iteration = event.get("iteration", 0)
        ts = event.get("ts")

        if self._debug_flow:
            log.info(
                "[flow/web] progress project=%s mapped_channels=%s event_id=%s type=%s iter=%s data_len=%s",
                project_id,
                owned_chat_ids,
                event.get("event_id", ""),
                event_type,
                iteration,
                len(str(data_text)),
            )

        for chat_id in owned_chat_ids:
            await self._persist_and_emit_progress(chat_id, project_id, event_type, data_text, iteration, ts)

    async def _persist_and_emit_progress(
        self,
        chat_id: int,
        project_id: str,
        event_type: str,
        data_text: str,
        iteration: int,
        ts: float | None,
    ):
        if self._debug_flow:
            log.info(
                "[flow/web] emit channel=%s project=%s type=%s iter=%s",
                chat_id,
                project_id,
                event_type,
                iteration,
            )

        for line in log_entries_from_event(event_type, data_text, tool_use_prefix="→"):
            await self._store.add_log(chat_id, line)
            if event_type != "log_update":
                await self._ws_send_to_channel(chat_id, {"type": "log", "text": line, "ts": ts})

        for sender, text in chat_messages_from_event(event_type, data_text):
            await self._store.add_message(chat_id, "assistant", text, sender=sender)
            await self._ws_send_to_channel(chat_id, {"type": "reply", "text": text, "sender": sender, "ts": ts})

        if event_type == "iteration":
            await self._ws_send_to_channel(
                chat_id,
                {"type": "progress", "data": data_text, "iteration": iteration, "ts": ts},
            )
        elif event_type == "done":
            await self._ws_send_to_channel(
                chat_id,
                {
                    "type": "progress",
                    "data": data_text,
                    "iteration": iteration,
                    "status": "done",
                    "ts": ts,
                },
            )
        elif event_type == "stopped":
            await self._ws_send_to_channel(
                chat_id,
                {
                    "type": "progress",
                    "data": data_text,
                    "iteration": iteration,
                    "status": "stopped",
                    "ts": ts,
                },
            )
        elif event_type == "stuck":
            await self._ws_send_to_channel(
                chat_id,
                {
                    "type": "progress",
                    "data": data_text,
                    "iteration": iteration,
                    "status": "stuck",
                    "ts": ts,
                },
            )
        elif event_type == "task_list":
            await self._ws_send_to_channel(
                chat_id,
                {"type": "task_list", "data": data_text, "iteration": iteration, "ts": ts},
            )
        elif event_type == "log_update":
            await self._ws_send_to_channel(
                chat_id,
                {"type": "log_update", "data": data_text, "iteration": iteration, "ts": ts},
            )
        elif event_type == "error":
            await self._ws_send_to_channel(
                chat_id,
                {
                    "type": "progress",
                    "data": data_text,
                    "iteration": iteration,
                    "status": "error",
                    "ts": ts,
                },
            )

        project_status = self._router.get_project_status(project_id)
        await self._ws_broadcast(
            {
                "type": "channel_status",
                "channel_id": chat_id,
                "project_id": project_id,
                "project_status": project_status,
                "iteration": iteration,
                "data": data_text,
            }
        )

    # -- WS send helpers ----------------------------------------------

    async def _ws_send_to_client(self, client_id: str, data: dict):
        ws = self._clients.get(client_id)
        if not ws or ws.closed:
            return
        try:
            await ws.send_json(data)
        except (ConnectionError, RuntimeError):
            log.warning("Failed to send WebSocket message to %s", client_id)

    async def _ws_send_to_channel(self, channel_id: int, data: dict):
        for client_id, active_channel in list(self._client_active_channel.items()):
            if active_channel == channel_id:
                await self._ws_send_to_client(client_id, data)

    async def _ws_broadcast(self, data: dict):
        for client_id in list(self._clients.keys()):
            await self._ws_send_to_client(client_id, data)
