"""Web chat frontend.

Multi-channel chat UI served over HTTP + WebSocket. Each channel is connected
to a remote orchestrator daemon via Router. Progress events are persisted for
all mapped channels and streamed to currently viewing clients.
"""

import asyncio
import json
import logging
import time
import uuid
from pathlib import Path

from aiohttp import WSMsgType, web

from .router import Router
from .store import Store

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


class WebChat:
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

        # Wire callbacks
        self._router.set_progress_callback(self._handle_progress)
        self._router.set_mapping_persist_callback(self._persist_channel_mapping)

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
        self._app.router.add_get("/ws", self._handle_ws)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        log.info("Web chat on http://%s:%s", self._host, self._port)

    async def stop(self):
        for ws in list(self._clients.values()):
            if not ws.closed:
                await ws.close()
        self._clients.clear()
        self._client_active_channel.clear()

        if self._runner:
            await self._runner.cleanup()

    async def _hydrate_channel_mappings(self):
        mapping = await self._store.get_channel_project_map()
        for chat_id, project_id in mapping.items():
            self._router.hydrate_channel_mapping(chat_id, project_id)

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

        messages = await self._store.get_recent_messages(channel_id)
        return web.json_response(messages)

    async def _handle_channels_list(self, request: web.Request) -> web.Response:
        channels = await self._store.get_channel_list()
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

        chat_id = await self._store.create_channel(name, project_id=project_id or None)

        if project_id:
            await self._router.connect_channel(chat_id, project_id)

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
                members.append(
                    {
                        "name": f"Orchestrator ({orch.name})",
                        "role": "orchestrator",
                        "detail": f"{project_id} — {orch.status}",
                    }
                )
                members.append(
                    {
                        "name": f"Worker ({orch.name})",
                        "role": "worker",
                        "detail": "active" if orch.status == "running" else "idle",
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

        logs = await self._store.get_logs(channel_id)
        return web.json_response(logs)

    async def _handle_projects_list(self, request: web.Request) -> web.Response:
        result = []
        for orch in self._router.list_orchestrators():
            result.append(
                {
                    "project_id": orch.project_id,
                    "name": orch.name,
                    "host": orch.host,
                    "status": orch.status,
                    "project_dir": orch.project_dir,
                }
            )
        return web.json_response(result)

    # -- WebSocket -----------------------------------------------------

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        client_id = uuid.uuid4().hex[:12]
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

            self._client_active_channel[client_id] = int(channel_id)
            project_id = self._router.get_channel_project(int(channel_id))
            status = self._router.get_project_status(project_id) if project_id else "unconnected"
            await self._ws_send_to_client(
                client_id,
                {
                    "type": "channel_switched",
                    "channel_id": int(channel_id),
                    "project_id": project_id,
                    "project_status": status,
                },
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

            await self._store.add_message(channel_id, "user", text)

            async def send_reply(msg: str):
                ts = time.time()
                await self._store.add_message(channel_id, "assistant", msg)
                await self._ws_send_to_channel(channel_id, {"type": "reply", "text": msg, "ts": ts})

            async def send_log(msg: str):
                ts = time.time()
                await self._store.add_log(channel_id, msg)
                await self._ws_send_to_channel(channel_id, {"type": "log", "text": msg, "ts": ts})

            asyncio.create_task(self._router.route_message(channel_id, text, send_reply, send_log))
            return

        if msg_type == "permission_response":
            # Backward compatibility for older frontend cards.
            await self._ws_send_to_client(
                client_id,
                {
                    "type": "error",
                    "text": "Permission escalation is not active in this build.",
                },
            )
            return

        await self._ws_send_to_client(client_id, {"type": "error", "text": f"Unknown message type: {msg_type}"})

    # -- Progress callback --------------------------------------------

    async def _handle_progress(self, project_id: str, event: dict):
        chat_ids = self._router.get_channels_for_project(project_id)
        if not chat_ids:
            return

        event_type = event.get("type", "")
        data_text = event.get("data", "")
        iteration = event.get("iteration", 0)
        ts = event.get("ts")

        for chat_id in chat_ids:
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
        if event_type == "text":
            await self._store.add_log(chat_id, data_text)
            await self._ws_send_to_channel(chat_id, {"type": "log", "text": data_text, "ts": ts})
            if data_text.startswith("@orchestrator") or data_text.startswith("@worker"):
                await self._store.add_message(chat_id, "assistant", data_text)
                await self._ws_send_to_channel(chat_id, {"type": "reply", "text": data_text, "ts": ts})
        elif event_type == "tool_use":
            line = f"→ {data_text}"
            await self._store.add_log(chat_id, line)
            await self._ws_send_to_channel(chat_id, {"type": "log", "text": line, "ts": ts})
        elif event_type == "tool_error":
            line = f"[ERROR] {data_text}"
            await self._store.add_log(chat_id, line)
            await self._ws_send_to_channel(chat_id, {"type": "log", "text": line, "ts": ts})
        elif event_type == "iteration":
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
            if data_text:
                msg = f"@orchestrator Task complete: {data_text}"
                await self._store.add_message(chat_id, "assistant", msg)
                await self._ws_send_to_channel(chat_id, {"type": "reply", "text": msg, "ts": ts})
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
            if data_text:
                msg = f"@orchestrator Needs input: {data_text}"
                await self._store.add_message(chat_id, "assistant", msg)
                await self._ws_send_to_channel(chat_id, {"type": "reply", "text": msg, "ts": ts})
        elif event_type == "task_list":
            await self._ws_send_to_channel(
                chat_id,
                {"type": "task_list", "data": data_text, "iteration": iteration, "ts": ts},
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
            if data_text:
                line = f"[ERROR] {data_text}"
                await self._store.add_log(chat_id, line)
                await self._ws_send_to_channel(chat_id, {"type": "log", "text": line, "ts": ts})
                msg = f"@orchestrator Error: {data_text}"
                await self._store.add_message(chat_id, "assistant", msg)
                await self._ws_send_to_channel(chat_id, {"type": "reply", "text": msg, "ts": ts})

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
