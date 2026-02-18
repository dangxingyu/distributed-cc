"""Web chat frontend — talk to the orchestrator from localhost:8080.

Multi-channel chat UI served over HTTP + WebSocket. Each channel has its
own orchestrator session, workers, messages, and notes.  The left sidebar
lists channels and lets users create / switch between them.
"""

import asyncio
import json
import logging
from pathlib import Path

from aiohttp import web, WSMsgType

from .orchestrator import Orchestrator
from .store import Store

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


class WebChat:
    def __init__(
        self,
        orchestrator: Orchestrator,
        store: Store,
        host: str = "127.0.0.1",
        port: int = 8080,
    ):
        self._orchestrator = orchestrator
        self._store = store
        self._host = host
        self._port = port
        self._ws: web.WebSocketResponse | None = None
        self._active_channel: int | None = None
        self._runner: web.AppRunner | None = None
        self._app: web.Application | None = None

        # Wire escalation sender
        self._orchestrator.set_send_telegram(self._send_escalation)

    async def start(self):
        self._app = web.Application()
        self._app.router.add_get("/", self._handle_index)
        self._app.router.add_get("/api/history", self._handle_history)
        self._app.router.add_get("/api/channels", self._handle_channels_list)
        self._app.router.add_post("/api/channels", self._handle_channels_create)
        self._app.router.add_delete("/api/channels/{id}", self._handle_channels_delete)
        self._app.router.add_get("/api/channels/{id}/members", self._handle_channels_members)
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
        return web.json_response(channels)

    async def _handle_channels_create(self, request: web.Request) -> web.Response:
        body = await request.json()
        name = body.get("name", "").strip()
        if not name:
            return web.json_response({"error": "name is required"}, status=400)
        chat_id = await self._store.create_channel(name)
        return web.json_response({"id": chat_id, "name": name})

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
            {"name": "Orchestrator", "role": "orchestrator"},
        ]
        workers = await self._store.get_channel_workers(channel_id)
        for w in workers:
            members.append({
                "name": f"{w.server}/{w.session_id}",
                "role": "worker",
                "detail": w.description,
            })
        return web.json_response(members)

    # ── WebSocket handler ──────────────────────────────────────────────

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # Replace any existing connection
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
            self._active_channel = int(channel_id)
            await self._ws_send({"type": "channel_switched", "channel_id": self._active_channel})

        elif msg_type == "message":
            if self._active_channel is None:
                await self._ws_send({"type": "error", "text": "No channel selected"})
                return

            text = data.get("text", "").strip()
            if not text:
                return

            chat_id = self._active_channel

            async def send_reply(msg: str):
                await self._ws_send({"type": "reply", "text": msg})

            # Run in background so the WS handler can continue processing
            # messages (e.g. permission_response) while the orchestrator runs.
            asyncio.create_task(
                self._orchestrator.route_message(
                    chat_id, text, send_reply, default_direct=True,
                )
            )

        elif msg_type == "permission_response":
            request_id = data.get("request_id", "")
            approved = data.get("approved", False)
            reason = data.get("reason", "Approved" if approved else "Denied")
            ok = self._orchestrator.resolve_permission(request_id, approved=approved, reason=reason)
            if ok:
                resolution = "APPROVED" if approved else "DENIED"
                await self._ws_send({
                    "type": "escalation_resolved",
                    "request_id": request_id,
                    "resolution": resolution,
                })

        elif msg_type == "clarification_response":
            request_id = data.get("request_id", "")
            question = data.get("question", "")
            answer = data.get("answer", "")
            ok = self._orchestrator.resolve_clarification(request_id, question, answer)
            if ok:
                await self._ws_send({
                    "type": "escalation_resolved",
                    "request_id": request_id,
                    "resolution": answer,
                })

        else:
            await self._ws_send({"type": "error", "text": f"Unknown message type: {msg_type}"})

    # ── Escalation sender (wired to orchestrator) ──────────────────────

    async def _send_escalation(
        self,
        request_id: str,
        interaction_type: str,
        title: str,
        detail: str,
    ):
        """Send a permission or clarification card to the browser."""
        if interaction_type == "permission":
            await self._ws_send({
                "type": "permission_request",
                "request_id": request_id,
                "title": title,
                "detail": detail,
            })

        elif interaction_type == "clarification":
            questions = self._orchestrator.get_pending_questions(request_id)
            await self._ws_send({
                "type": "clarification_request",
                "request_id": request_id,
                "title": title,
                "detail": detail,
                "questions": questions or [],
            })

    # ── Helpers ────────────────────────────────────────────────────────

    async def _ws_send(self, data: dict):
        """Send JSON over WebSocket, guarding against closed connection."""
        if self._ws and not self._ws.closed:
            try:
                await self._ws.send_json(data)
            except (ConnectionError, RuntimeError):
                log.warning("Failed to send WebSocket message (connection closed)")
