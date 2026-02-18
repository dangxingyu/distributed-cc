"""Test the web chat frontend with Router architecture.

Verifies:
- Static file serving (index.html)
- History API endpoint (with channel param)
- Channel CRUD REST endpoints
- Projects API endpoint
- WebSocket connect/switch_channel/message
"""

import asyncio
import json
import tempfile
import pytest
from unittest.mock import AsyncMock, MagicMock

from aiohttp import web

from src.router import Router, RemoteOrchestrator
from src.store import Store
from src.web import WebChat


async def _make_web(aiohttp_client):
    """Create a WebChat instance with mocked Router, return (client, web_chat, router, store)."""
    store = Store(tempfile.mkdtemp())
    await store.init()

    router = Router()
    router._orchestrators = {
        "test-proj": RemoteOrchestrator(
            project_id="test-proj", name="test-server", status="idle",
        ),
    }

    web_chat = WebChat(router=router, store=store)

    app = web.Application()
    app.router.add_get("/", web_chat._handle_index)
    app.router.add_get("/api/history", web_chat._handle_history)
    app.router.add_get("/api/channels", web_chat._handle_channels_list)
    app.router.add_post("/api/channels", web_chat._handle_channels_create)
    app.router.add_delete("/api/channels/{id}", web_chat._handle_channels_delete)
    app.router.add_get("/api/channels/{id}/members", web_chat._handle_channels_members)
    app.router.add_get("/api/logs", web_chat._handle_logs)
    app.router.add_get("/api/projects", web_chat._handle_projects_list)
    app.router.add_get("/ws", web_chat._handle_ws)

    client = await aiohttp_client(app)
    return client, web_chat, router, store


# ── Static serving ─────────────────────────────────────────────────────

async def test_index_serves_html(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    resp = await client.get("/")
    assert resp.status == 200
    text = await resp.text()
    assert "<!DOCTYPE html>" in text
    assert "distributed-cc" in text
    await store.close()


# ── Channel REST API ──────────────────────────────────────────────────

async def test_channels_list_empty(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    resp = await client.get("/api/channels")
    assert resp.status == 200
    data = await resp.json()
    assert data == []
    await store.close()


async def test_create_channel(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    resp = await client.post("/api/channels", json={"name": "my-project"})
    assert resp.status == 200
    data = await resp.json()
    assert data["name"] == "my-project"
    assert "id" in data
    assert isinstance(data["id"], int)
    await store.close()


async def test_create_channel_with_project(aiohttp_client):
    """Creating a channel with project_id auto-connects it."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    resp = await client.post("/api/channels", json={
        "name": "my-project",
        "project_id": "test-proj",
    })
    assert resp.status == 200
    data = await resp.json()
    assert data["project_id"] == "test-proj"

    # Verify the channel is connected
    assert router.get_channel_project(data["id"]) == "test-proj"
    await store.close()


async def test_channels_list_after_create(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    await client.post("/api/channels", json={"name": "alpha"})
    await client.post("/api/channels", json={"name": "beta"})

    resp = await client.get("/api/channels")
    data = await resp.json()
    assert len(data) == 2
    names = [c["name"] for c in data]
    assert "alpha" in names
    assert "beta" in names
    await store.close()


async def test_delete_channel(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    resp = await client.post("/api/channels", json={"name": "temp"})
    ch = await resp.json()

    resp = await client.delete(f"/api/channels/{ch['id']}")
    assert resp.status == 200

    resp = await client.get("/api/channels")
    data = await resp.json()
    assert len(data) == 0
    await store.close()


# ── History API ────────────────────────────────────────────────────────

async def test_history_requires_channel_param(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    resp = await client.get("/api/history")
    assert resp.status == 400
    await store.close()


async def test_history_with_channel_param(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("test-ch")
    await store.add_message(ch_id, "user", "hello")
    await store.add_message(ch_id, "assistant", "hi")

    resp = await client.get(f"/api/history?channel={ch_id}")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 2
    assert data[0]["role"] == "user"
    assert data[0]["content"] == "hello"
    await store.close()


# ── Projects API ──────────────────────────────────────────────────────

async def test_projects_list(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    resp = await client.get("/api/projects")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 1
    assert data[0]["project_id"] == "test-proj"
    assert data[0]["name"] == "test-server"
    await store.close()


# ── Members API ───────────────────────────────────────────────────────

async def test_channel_members_no_project(aiohttp_client):
    """Members with no project connected: just user."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("members-ch")

    resp = await client.get(f"/api/channels/{ch_id}/members")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 1
    assert data[0] == {"name": "You", "role": "user"}
    await store.close()


async def test_channel_members_with_project(aiohttp_client):
    """Members with project connected: user + orchestrator."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("members-ch")
    await router.connect_channel(ch_id, "test-proj")

    resp = await client.get(f"/api/channels/{ch_id}/members")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "You"
    assert "Orchestrator" in data[1]["name"]
    await store.close()


# ── WebSocket tests ────────────────────────────────────────────────────

async def test_ws_connect(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ws = await client.ws_connect("/ws")
    assert not ws.closed
    await ws.close()
    await store.close()


async def test_ws_switch_channel(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("test-ch")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})

    msg = await ws.receive_json()
    assert msg["type"] == "channel_switched"
    assert msg["channel_id"] == ch_id
    assert web_chat._active_channel == ch_id

    await ws.close()
    await store.close()


async def test_ws_switch_channel_shows_project(aiohttp_client):
    """Switching to a connected channel shows project info."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("proj-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})

    msg = await ws.receive_json()
    assert msg["type"] == "channel_switched"
    assert msg["project_id"] == "test-proj"
    assert msg["project_status"] == "idle"

    await ws.close()
    await store.close()


async def test_ws_message_requires_channel(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "message", "text": "hello"})

    msg = await ws.receive_json()
    assert msg["type"] == "error"
    assert "No channel" in msg["text"]

    await ws.close()
    await store.close()


async def test_ws_message_routes_via_router(aiohttp_client):
    """Message is routed through the Router."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("routed-ch")
    await router.connect_channel(ch_id, "test-proj")

    called = {}

    async def mock_route(chat_id, text, send_reply, send_log=None):
        called["chat_id"] = chat_id
        called["text"] = text
        await send_reply("got it")

    router.route_message = mock_route

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()  # consume channel_switched

    await ws.send_json({"type": "message", "text": "do something"})

    msg = await ws.receive_json()
    assert msg["type"] == "reply"
    assert msg["text"] == "got it"

    await asyncio.sleep(0.05)
    assert called["chat_id"] == ch_id
    assert called["text"] == "do something"

    await ws.close()
    await store.close()


async def test_ws_connect_command(aiohttp_client):
    """/connect command via WebSocket routes through router and links channel."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("connect-ch")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()  # consume channel_switched

    await ws.send_json({"type": "message", "text": "/connect test-proj"})

    # The router handles /connect and calls send_reply
    msg = await ws.receive_json()
    assert msg["type"] == "reply"
    assert "test-proj" in msg["text"]

    assert router.get_channel_project(ch_id) == "test-proj"

    await ws.close()
    await store.close()


# ── Logs API ──────────────────────────────────────────────────────────

async def test_logs_api(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("log-ch")
    await store.add_log(ch_id, "tool call: Bash")

    resp = await client.get(f"/api/logs?channel={ch_id}")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 1
    assert data[0]["text"] == "tool call: Bash"
    await store.close()
