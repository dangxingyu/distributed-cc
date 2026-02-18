"""Web chat frontend — talk to the orchestrator daemon from localhost:8080.

Multi-channel chat UI served over HTTP + WebSocket. Each channel is connected
to a remote orchestrator daemon via the Router. Progress events from daemons
are streamed to the UI in real-time.
"""

import asyncio
import json
import logging
from pathlib import Path

from aiohttp import web, WSMsgType

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
        self._ws: web.WebSocketResponse | None = None
        self._active_channel: int | None = None
        self._runner: web.AppRunner | None = None
        self._app: web.Application | None = None

        # Wire callbacks
        self._router.set_progress_callback(self._handle_progress)

    async def start(self):
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
        log.info(f"Web chat on http://{self._host}:{self._port}")

    async def stop(self):
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._runner:
            await self._runner.cleanup()

    # ── HTTP handlers ──────────────────────────────────────────────────

    async def _handle_index(self, request: web.Request) -> web.Response:
        index_path = STATIC_DIR / "index.html"
        return web.FileResponse(index_path)

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
        # Enrich with project info
        for ch in channels:
            project_id = self._router.get_channel_project(ch["id"])
            ch["project_id"] = project_id
            ch["project_status"] = self._router.get_project_status(project_id) if project_id else "unconnected"
        return web.json_response(channels)

    async def _handle_channels_create(self, request: web.Request) -> web.Response:
        body = await request.json()
        name = body.get("name", "").strip()
        if not name:
            return web.json_response({"error": "name is required"}, status=400)
        chat_id = await self._store.create_channel(name)

        # Auto-connect if project_id provided
        project_id = body.get("project_id", "").strip()
        if project_id:
            await self._router.connect_channel(chat_id, project_id)

        return web.json_response({"id": chat_id, "name": name, "project_id": project_id})

    async def _handle_channels_delete(self, request: web.Request) -> web.Response:
        try:
            channel_id = int(request.match_info["id"])
        except (ValueError, KeyError):
            return web.json_response({"error": "invalid channel id"}, status=400)
        await self._store.delete_channel(channel_id)
        return web.json_response({"ok": True})

    async def _handle_channels_members(self, request: web.Request) -> web.Response:
        try:
            channel_id = int(request.match_info["id"])
        except (ValueError, KeyError):
            return web.json_response({"error": "invalid channel id"}, status=400)

        members = [
            {"name": "You", "role": "user"},
        ]
        project_id = self._router.get_channel_project(channel_id)
        if project_id:
            orch = self._router._orchestrators.get(project_id)
            if orch:
                members.append({
                    "name": f"Orchestrator ({orch.name})",
                    "role": "orchestrator",
                    "detail": f"{project_id} — {orch.status}",
                })

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
        """GET /api/projects — list configured orchestrator projects."""
        result = []
        for orch in self._router.list_orchestrators():
            result.append({
                "project_id": orch.project_id,
                "name": orch.name,
                "host": orch.host,
                "status": orch.status,
                "project_dir": orch.project_dir,
            })
        return web.json_response(result)

    # ── WebSocket handler ──────────────────────────────────────────────

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = ws
        self._active_channel = None

        log.info("WebSocket client connected")

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    await self._handle_ws_message(msg.data)
                elif msg.type == WSMsgType.ERROR:
                    log.error(f"WebSocket error: {ws.exception()}")
        finally:
            if self._ws is ws:
                if self._active_channel is not None:
                    self._router.remove_channel_status_callback(self._active_channel)
                self._ws = None
                self._active_channel = None
            log.info("WebSocket client disconnected")

        return ws

    async def _handle_ws_message(self, raw: str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            await self._ws_send({"type": "error", "text": "Invalid JSON"})
            return

        msg_type = data.get("type")

        if msg_type == "switch_channel":
            channel_id = data.get("channel_id")
            if channel_id is None:
                await self._ws_send({"type": "error", "text": "missing channel_id"})
                return
            if self._active_channel is not None:
                self._router.remove_channel_status_callback(self._active_channel)
            self._active_channel = int(channel_id)

            # Register status callback for this channel
            async def send_status(event: dict):
                await self._ws_send({"type": "progress", **event})
            self._router.set_channel_status_callback(self._active_channel, send_status)

            # Send current project status
            project_id = self._router.get_channel_project(self._active_channel)
            status = self._router.get_project_status(project_id) if project_id else "unconnected"
            await self._ws_send({
                "type": "channel_switched",
                "channel_id": self._active_channel,
                "project_id": project_id,
                "project_status": status,
            })

        elif msg_type == "message":
            if self._active_channel is None:
                await self._ws_send({"type": "error", "text": "No channel selected"})
                return

            text = data.get("text", "").strip()
            if not text:
                return

            chat_id = self._active_channel

            async def send_reply(msg: str):
                await self._store.add_message(chat_id, "assistant", msg)
                await self._ws_send({"type": "reply", "text": msg})

            async def send_log(msg: str):
                await self._store.add_log(chat_id, msg)
                await self._ws_send({"type": "log", "text": msg})

            # Save user message
            await self._store.add_message(chat_id, "user", text)

            # Route everything through the router (including /connect)
            asyncio.create_task(
                self._router.route_message(chat_id, text, send_reply, send_log)
            )

        else:
            await self._ws_send({"type": "error", "text": f"Unknown message type: {msg_type}"})

    # ── Progress callback (from router) ───────────────────────────────

    async def _handle_progress(self, project_id: str, event: dict):
        """Handle a progress event from a daemon (via router).

        Routing:
          text, tool_use, tool_error → monitor log (not chat)
          iteration → progress indicator
          done, stuck → chat reply + progress indicator
          error → progress indicator
        """
        # Find which channel this project is connected to
        for chat_id, pid in self._router._channel_project.items():
            if pid == project_id and chat_id == self._active_channel:
                event_type = event.get("type", "")
                data_text = event.get("data", "")
                iteration = event.get("iteration", 0)

                if event_type == "text":
                    # Intermediate orchestrator text → monitor only
                    await self._ws_send({"type": "log", "text": data_text})
                    await self._store.add_log(chat_id, data_text)
                elif event_type == "tool_use":
                    await self._ws_send({"type": "log", "text": f"→ {data_text}"})
                    await self._store.add_log(chat_id, f"→ {data_text}")
                elif event_type == "tool_error":
                    await self._ws_send({"type": "log", "text": f"[ERROR] {data_text}"})
                    await self._store.add_log(chat_id, f"[ERROR] {data_text}")
                elif event_type == "iteration":
                    await self._ws_send({
                        "type": "progress",
                        "data": data_text,
                        "iteration": iteration,
                    })
                elif event_type == "done":
                    await self._ws_send({
                        "type": "progress",
                        "data": data_text,
                        "iteration": iteration,
                        "status": "done",
                    })
                    if data_text:
                        await self._ws_send({"type": "reply", "text": f"Task complete: {data_text}"})
                        await self._store.add_message(chat_id, "assistant", f"Task complete: {data_text}")
                elif event_type == "stuck":
                    await self._ws_send({
                        "type": "progress",
                        "data": data_text,
                        "iteration": iteration,
                        "status": "stuck",
                    })
                    if data_text:
                        await self._ws_send({"type": "reply", "text": f"Needs input: {data_text}"})
                        await self._store.add_message(chat_id, "assistant", f"Needs input: {data_text}")
                elif event_type == "error":
                    await self._ws_send({
                        "type": "progress",
                        "data": data_text,
                        "iteration": iteration,
                        "status": "error",
                    })
                    if data_text:
                        await self._ws_send({"type": "log", "text": f"[ERROR] {data_text}"})
                        await self._store.add_log(chat_id, f"[ERROR] {data_text}")
                break

    # ── Helpers ────────────────────────────────────────────────────────

    async def _ws_send(self, data: dict):
        """Send JSON over WebSocket, guarding against closed connection."""
        if self._ws and not self._ws.closed:
            try:
                await self._ws.send_json(data)
            except (ConnectionError, RuntimeError):
                log.warning("Failed to send WebSocket message (connection closed)")
