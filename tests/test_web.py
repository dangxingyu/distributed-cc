"""Test the web chat frontend.

Verifies:
- Static file serving (index.html)
- History API endpoint (with channel param)
- Channel CRUD REST endpoints
- WebSocket connect/switch_channel/message/reply
- Permission escalation over WebSocket
- Clarification escalation over WebSocket
"""

import asyncio
import json
import tempfile
import pytest
from unittest.mock import AsyncMock, MagicMock

from aiohttp import web, WSMsgType

from src.orchestrator import Orchestrator
from src.session import SessionManager
from src.store import Store
from src.web import WebChat


async def _make_web(aiohttp_client):
    """Create a WebChat instance with mocked orchestrator, return (client, web_chat, orch, store)."""
    store = Store(tempfile.mkdtemp())
    await store.init()

    mgr = SessionManager(servers=[], default_model="haiku")
    await mgr.init()

    orch = Orchestrator(
        session_mgr=mgr,
        store=store,
        model="haiku",
        config_path="config.yaml",
    )

    web_chat = WebChat(orchestrator=orch, store=store)

    # Build the aiohttp app manually (same as WebChat.start but without runner)
    app = web.Application()
    app.router.add_get("/", web_chat._handle_index)
    app.router.add_get("/api/history", web_chat._handle_history)
    app.router.add_get("/api/channels", web_chat._handle_channels_list)
    app.router.add_post("/api/channels", web_chat._handle_channels_create)
    app.router.add_delete("/api/channels/{id}", web_chat._handle_channels_delete)
    app.router.add_get("/api/channels/{id}/members", web_chat._handle_channels_members)
    app.router.add_get("/ws", web_chat._handle_ws)

    client = await aiohttp_client(app)
    return client, web_chat, orch, store, mgr


# ── Static serving ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_index_serves_html(aiohttp_client):
    """GET / returns 200 with HTML content."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        resp = await client.get("/")
        assert resp.status == 200
        text = await resp.text()
        assert "<!DOCTYPE html>" in text
        assert "distributed-cc" in text
    finally:
        await mgr.close()
        await store.close()


# ── Channel REST API ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_channels_list_empty(aiohttp_client):
    """GET /api/channels returns [] initially."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        resp = await client.get("/api/channels")
        assert resp.status == 200
        data = await resp.json()
        assert data == []
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_create_channel(aiohttp_client):
    """POST /api/channels creates a channel, returns id+name."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        resp = await client.post("/api/channels", json={"name": "my-project"})
        assert resp.status == 200
        data = await resp.json()
        assert data["name"] == "my-project"
        assert "id" in data
        assert isinstance(data["id"], int)
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_channels_list_after_create(aiohttp_client):
    """Channels appear in list after creation."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        await client.post("/api/channels", json={"name": "alpha"})
        await client.post("/api/channels", json={"name": "beta"})

        resp = await client.get("/api/channels")
        data = await resp.json()
        assert len(data) == 2
        names = [c["name"] for c in data]
        assert "alpha" in names
        assert "beta" in names
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_delete_channel(aiohttp_client):
    """DELETE /api/channels/{id} removes the channel."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        resp = await client.post("/api/channels", json={"name": "temp"})
        ch = await resp.json()

        resp = await client.delete(f"/api/channels/{ch['id']}")
        assert resp.status == 200
        data = await resp.json()
        assert data["ok"] is True

        resp = await client.get("/api/channels")
        data = await resp.json()
        assert len(data) == 0
    finally:
        await mgr.close()
        await store.close()


# ── History API ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_history_requires_channel_param(aiohttp_client):
    """GET /api/history without channel param returns 400."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        resp = await client.get("/api/history")
        assert resp.status == 400
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_history_with_channel_param(aiohttp_client):
    """GET /api/history?channel=N returns correct messages."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        ch_id = await store.create_channel("test-ch")
        await store.add_message(ch_id, "user", "hello")
        await store.add_message(ch_id, "assistant", "orchestrator: hi")

        resp = await client.get(f"/api/history?channel={ch_id}")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 2
        assert data[0]["role"] == "user"
        assert data[0]["content"] == "hello"
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_history_empty(aiohttp_client):
    """GET /api/history with valid channel but no messages returns []."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        ch_id = await store.create_channel("empty-ch")
        resp = await client.get(f"/api/history?channel={ch_id}")
        assert resp.status == 200
        data = await resp.json()
        assert data == []
    finally:
        await mgr.close()
        await store.close()


# ── WebSocket tests ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_ws_connect(aiohttp_client):
    """WebSocket connects successfully."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        ws = await client.ws_connect("/ws")
        assert not ws.closed
        await ws.close()
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_ws_switch_channel(aiohttp_client):
    """switch_channel sets active channel and sends confirmation."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        ch_id = await store.create_channel("test-ch")

        ws = await client.ws_connect("/ws")
        await ws.send_json({"type": "switch_channel", "channel_id": ch_id})

        msg = await ws.receive_json()
        assert msg["type"] == "channel_switched"
        assert msg["channel_id"] == ch_id
        assert web_chat._active_channel == ch_id

        await ws.close()
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_ws_message_requires_channel(aiohttp_client):
    """Sending a message without switching channel first returns error."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        ws = await client.ws_connect("/ws")
        await ws.send_json({"type": "message", "text": "hello"})

        msg = await ws.receive_json()
        assert msg["type"] == "error"
        assert "No channel" in msg["text"]

        await ws.close()
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_ws_message_uses_active_channel(aiohttp_client):
    """Message is routed to the active channel."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        ch_id = await store.create_channel("routed-ch")
        called = {}

        async def mock_route(chat_id, text, send_reply, default_direct=False):
            called["chat_id"] = chat_id
            called["text"] = text
            called["default_direct"] = default_direct
            await send_reply("orchestrator: got it")

        orch.route_message = mock_route

        ws = await client.ws_connect("/ws")
        # Switch to the channel first
        await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
        await ws.receive_json()  # consume channel_switched

        await ws.send_json({"type": "message", "text": "do something"})

        msg = await ws.receive_json()
        assert msg["type"] == "reply"
        assert msg["text"] == "orchestrator: got it"

        assert called["chat_id"] == ch_id
        assert called["text"] == "do something"
        assert called["default_direct"] is True

        await ws.close()
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_ws_reply_to_client(aiohttp_client):
    """send_reply callback pushes reply over WebSocket."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        ch_id = await store.create_channel("reply-ch")

        async def mock_route(chat_id, text, send_reply, default_direct=False):
            await send_reply("first reply")
            await send_reply("second reply")

        orch.route_message = mock_route

        ws = await client.ws_connect("/ws")
        await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
        await ws.receive_json()  # consume channel_switched

        await ws.send_json({"type": "message", "text": "test"})

        msg1 = await ws.receive_json()
        msg2 = await ws.receive_json()
        assert msg1 == {"type": "reply", "text": "first reply"}
        assert msg2 == {"type": "reply", "text": "second reply"}

        await ws.close()
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_ws_permission_escalation(aiohttp_client):
    """_send_escalation(permission) sends card to client over WS."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        ws = await client.ws_connect("/ws")

        # Directly call escalation sender
        await web_chat._send_escalation(
            request_id="req123",
            interaction_type="permission",
            title="Permission: Bash",
            detail="Tool: Bash\nInput: echo hello",
        )

        msg = await ws.receive_json()
        assert msg["type"] == "permission_request"
        assert msg["request_id"] == "req123"
        assert msg["title"] == "Permission: Bash"
        assert "Bash" in msg["detail"]

        await ws.close()
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_ws_permission_resolve(aiohttp_client):
    """Client sends approval -> resolve_permission called."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        # Set up a pending permission
        future = asyncio.get_event_loop().create_future()
        orch._pending["req456"] = future

        ws = await client.ws_connect("/ws")
        await ws.send_json({
            "type": "permission_response",
            "request_id": "req456",
            "approved": True,
            "reason": "Looks good",
        })

        # Should get escalation_resolved confirmation
        msg = await ws.receive_json()
        assert msg["type"] == "escalation_resolved"
        assert msg["request_id"] == "req456"
        assert msg["resolution"] == "APPROVED"

        # Future should be resolved
        result = await asyncio.wait_for(future, timeout=1)
        assert result["approved"] is True

        await ws.close()
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_ws_clarification_escalation(aiohttp_client):
    """_send_escalation(clarification) sends card with questions to client."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        # Store pending question metadata
        questions = [{
            "question": "Which DB?",
            "header": "Database",
            "options": [
                {"label": "PostgreSQL", "description": "Relational"},
                {"label": "MongoDB", "description": "Document"},
            ],
            "multiSelect": False,
        }]
        orch._pending_meta["req789"] = {"questions": questions}

        ws = await client.ws_connect("/ws")

        await web_chat._send_escalation(
            request_id="req789",
            interaction_type="clarification",
            title="Clarification needed",
            detail="Worker needs input",
        )

        msg = await ws.receive_json()
        assert msg["type"] == "clarification_request"
        assert msg["request_id"] == "req789"
        assert len(msg["questions"]) == 1
        assert msg["questions"][0]["question"] == "Which DB?"
        assert len(msg["questions"][0]["options"]) == 2

        await ws.close()
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_ws_clarification_resolve(aiohttp_client):
    """Client sends answer -> resolve_clarification called."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        # Set up a pending clarification
        future = asyncio.get_event_loop().create_future()
        orch._pending["req012"] = future
        orch._pending_meta["req012"] = {"questions": [{"question": "Which DB?"}]}

        ws = await client.ws_connect("/ws")
        await ws.send_json({
            "type": "clarification_response",
            "request_id": "req012",
            "question": "Which DB?",
            "answer": "PostgreSQL",
        })

        # Should get escalation_resolved confirmation
        msg = await ws.receive_json()
        assert msg["type"] == "escalation_resolved"
        assert msg["request_id"] == "req012"
        assert msg["resolution"] == "PostgreSQL"

        # Future should be resolved
        result = await asyncio.wait_for(future, timeout=1)
        assert result["answers"]["Which DB?"] == "PostgreSQL"

        await ws.close()
    finally:
        await mgr.close()
        await store.close()


# ── Members API ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_channel_members(aiohttp_client):
    """GET /api/channels/{id}/members returns user, orchestrator, and workers."""
    client, web_chat, orch, store, mgr = await _make_web(aiohttp_client)
    try:
        ch_id = await store.create_channel("members-ch")

        # Initially: just user + orchestrator
        resp = await client.get(f"/api/channels/{ch_id}/members")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 2
        assert data[0] == {"name": "You", "role": "user"}
        assert data[1] == {"name": "Orchestrator", "role": "orchestrator"}

        # Add a worker
        await store.add_channel_worker(ch_id, "gpu-server", "sess-1", "/tmp", "training run")

        resp = await client.get(f"/api/channels/{ch_id}/members")
        data = await resp.json()
        assert len(data) == 3
        assert data[2]["name"] == "gpu-server/sess-1"
        assert data[2]["role"] == "worker"
        assert data[2]["detail"] == "training run"
    finally:
        await mgr.close()
        await store.close()
