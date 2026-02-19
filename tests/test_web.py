"""Web frontend tests for multi-client/channel interaction behavior."""

import asyncio
import tempfile

import pytest
from aiohttp import web

from src.router import RemoteOrchestrator, Router
from src.store import Store
from src.web import WebChat


async def _make_web(aiohttp_client):
    store = Store(tempfile.mkdtemp())
    await store.init()

    router = Router()
    router._orchestrators = {
        "test-proj": RemoteOrchestrator(project_id="test-proj", name="test-server", status="idle"),
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


async def test_index_serves_html(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    resp = await client.get("/")
    assert resp.status == 200
    text = await resp.text()
    assert "<!DOCTYPE html>" in text
    assert "distributed-cc" in text
    await store.close()


async def test_channels_list_empty(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    resp = await client.get("/api/channels")
    assert resp.status == 200
    assert await resp.json() == []
    await store.close()


async def test_create_channel(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    resp = await client.post("/api/channels", json={"name": "my-project"})
    assert resp.status == 200
    data = await resp.json()
    assert data["name"] == "my-project"
    assert isinstance(data["id"], int)
    await store.close()


async def test_create_channel_invalid_project_rejected(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    resp = await client.post("/api/channels", json={"name": "bad", "project_id": "unknown"})
    assert resp.status == 400
    data = await resp.json()
    assert "unknown project_id" in data["error"]
    await store.close()


async def test_create_channel_with_project(aiohttp_client):
    client, _, router, store = await _make_web(aiohttp_client)
    resp = await client.post("/api/channels", json={"name": "my-project", "project_id": "test-proj"})
    assert resp.status == 200
    data = await resp.json()
    assert data["project_id"] == "test-proj"
    assert router.get_channel_project(data["id"]) == "test-proj"
    assert await store.get_channel_project(data["id"]) == "test-proj"
    await store.close()


async def test_delete_channel_clears_mapping(aiohttp_client):
    client, _, router, store = await _make_web(aiohttp_client)
    ch_resp = await client.post("/api/channels", json={"name": "temp", "project_id": "test-proj"})
    ch = await ch_resp.json()

    resp = await client.delete(f"/api/channels/{ch['id']}")
    assert resp.status == 200
    assert router.get_channel_project(ch["id"]) is None

    await store.close()


async def test_history_requires_channel_param(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    resp = await client.get("/api/history")
    assert resp.status == 400
    await store.close()


async def test_history_with_channel_param_has_ts(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("test-ch")
    await store.add_message(ch_id, "user", "hello")
    await store.add_message(ch_id, "assistant", "hi")

    resp = await client.get(f"/api/history?channel={ch_id}")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 2
    assert data[0]["role"] == "user"
    assert data[0]["content"] == "hello"
    assert data[0]["ts"] is not None
    await store.close()


async def test_projects_list(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    resp = await client.get("/api/projects")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 1
    assert data[0]["project_id"] == "test-proj"
    assert data[0]["name"] == "test-server"
    await store.close()


async def test_channel_members_no_project(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("members-ch")

    resp = await client.get(f"/api/channels/{ch_id}/members")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 2
    names = {m["name"] for m in data}
    assert "You" in names
    assert "Router" in names
    await store.close()


async def test_channel_members_with_project(aiohttp_client):
    client, _, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("members-ch")
    await router.connect_channel(ch_id, "test-proj")

    resp = await client.get(f"/api/channels/{ch_id}/members")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 4
    names = [m["name"] for m in data]
    assert any(n == "You" for n in names)
    assert any(n == "Router" for n in names)
    assert any("Orchestrator" in n for n in names)
    assert any("Worker" in n for n in names)
    await store.close()


async def test_ws_multiple_clients_can_connect(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ws1 = await client.ws_connect("/ws")
    ws2 = await client.ws_connect("/ws")

    assert not ws1.closed
    assert not ws2.closed

    await ws1.close()
    await ws2.close()
    await store.close()


async def test_ws_switch_channel(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("test-ch")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})

    msg = await ws.receive_json()
    assert msg["type"] == "channel_switched"
    assert msg["channel_id"] == ch_id

    await ws.close()
    await store.close()


async def test_ws_message_requires_channel(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "message", "text": "hello"})

    msg = await ws.receive_json()
    assert msg["type"] == "error"
    assert "No channel" in msg["text"]

    await ws.close()
    await store.close()


async def test_ws_message_broadcasts_reply_to_same_channel_viewers(aiohttp_client):
    client, _, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("routed-ch")
    await router.connect_channel(ch_id, "test-proj")

    async def mock_route(chat_id, text, send_reply, send_log=None):
        await send_reply("got it")

    router.route_message = mock_route

    ws1 = await client.ws_connect("/ws")
    ws2 = await client.ws_connect("/ws")

    await ws1.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws2.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws1.receive_json()
    await ws2.receive_json()

    await ws1.send_json({"type": "message", "text": "do something"})

    msg1 = await ws1.receive_json()
    msg2 = await ws2.receive_json()

    assert msg1["type"] == "reply"
    assert msg1["text"] == "got it"
    assert msg2["type"] == "reply"
    assert msg2["text"] == "got it"

    await ws1.close()
    await ws2.close()
    await store.close()


async def test_progress_persists_for_inactive_channel(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)

    ch_target = await store.create_channel("target")
    ch_other = await store.create_channel("other")
    await router.connect_channel(ch_target, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_other})
    await ws.receive_json()

    await web_chat._handle_progress(
        "test-proj",
        {"type": "text", "data": "background-progress", "iteration": 1, "ts": 123.0},
    )

    logs = await store.get_logs(ch_target)
    assert any("background-progress" in entry["text"] for entry in logs)

    await ws.close()
    await store.close()


async def test_orchestrator_worker_exchange_visible_in_chat_and_monitor(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("exchange-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()  # channel_switched

    await web_chat._handle_progress(
        "test-proj",
        {
            "type": "text",
            "data": "@orchestrator -> @worker: run focused tests and report",
            "iteration": 2,
            "ts": 200.0,
        },
    )

    msg1 = await asyncio.wait_for(ws.receive_json(), timeout=1)
    msg2 = await asyncio.wait_for(ws.receive_json(), timeout=1)
    msg3 = await asyncio.wait_for(ws.receive_json(), timeout=1)
    got_types = {msg1["type"], msg2["type"], msg3["type"]}
    assert "log" in got_types
    assert "reply" in got_types
    assert "channel_status" in got_types

    logs = await store.get_logs(ch_id)
    history = await store.get_recent_messages(ch_id)
    assert any("@orchestrator -> @worker" in entry["text"] for entry in logs)
    assert any(
        m["role"] == "assistant" and "@orchestrator -> @worker" in m["content"]
        for m in history
    )

    await ws.close()
    await store.close()


async def test_progress_broadcasts_channel_status(aiohttp_client):
    client, web_chat, router, store = await _make_web(aiohttp_client)

    ch_id = await store.create_channel("status-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await web_chat._handle_progress(
        "test-proj",
        {"type": "iteration", "data": "Iteration 1/20", "iteration": 1, "ts": 100.0},
    )

    first = await asyncio.wait_for(ws.receive_json(), timeout=1)
    second = await asyncio.wait_for(ws.receive_json(), timeout=1)
    types = {first["type"], second["type"]}
    assert "progress" in types
    assert "channel_status" in types

    await ws.close()
    await store.close()


async def test_logs_api(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("log-ch")
    await store.add_log(ch_id, "tool call: Bash")

    resp = await client.get(f"/api/logs?channel={ch_id}")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 1
    assert data[0]["text"] == "tool call: Bash"
    await store.close()


# ── switch_channel with project info ─────────────────────────────────


async def test_ws_switch_channel_shows_project_info(aiohttp_client):
    """Switching to a connected channel returns project_id and status."""
    client, _, router, store = await _make_web(aiohttp_client)
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


# ── Progress event types → correct WS output ─────────────────────────


async def test_progress_done_sends_chat_reply(aiohttp_client):
    """'done' progress event produces both a 'progress' and a 'reply' WS message."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("done-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()  # consume channel_switched

    await web_chat._handle_progress(
        "test-proj",
        {"type": "done", "data": "all tests pass", "iteration": 5, "ts": 100.0},
    )

    messages = []
    for _ in range(3):  # progress + reply + channel_status
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        messages.append(msg)

    types = [m["type"] for m in messages]
    assert "progress" in types
    assert "reply" in types

    reply = next(m for m in messages if m["type"] == "reply")
    assert "all tests pass" in reply["text"]

    progress = next(m for m in messages if m["type"] == "progress")
    assert progress.get("status") == "done"

    # Verify persisted as assistant message
    msgs = await store.get_recent_messages(ch_id)
    assert any("all tests pass" in m["content"] for m in msgs)

    await ws.close()
    await store.close()


async def test_progress_stuck_sends_chat_reply(aiohttp_client):
    """'stuck' progress event produces a 'Needs input' reply."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("stuck-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await web_chat._handle_progress(
        "test-proj",
        {"type": "stuck", "data": "need API key", "iteration": 3, "ts": 100.0},
    )

    messages = []
    for _ in range(3):
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        messages.append(msg)

    reply = next(m for m in messages if m["type"] == "reply")
    assert "need api key" in reply["text"].lower()

    await ws.close()
    await store.close()


async def test_progress_tool_use_goes_to_log(aiohttp_client):
    """'tool_use' event produces a log with arrow prefix, not a reply."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("tool-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await web_chat._handle_progress(
        "test-proj",
        {"type": "tool_use", "data": "Bash: ls -la", "iteration": 1, "ts": 100.0},
    )

    messages = []
    for _ in range(2):  # log + channel_status
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        messages.append(msg)

    log_msg = next(m for m in messages if m["type"] == "log")
    assert "→" in log_msg["text"]
    assert "Bash: ls -la" in log_msg["text"]

    # No reply message
    assert not any(m["type"] == "reply" for m in messages)

    await ws.close()
    await store.close()


async def test_progress_tool_error_goes_to_log(aiohttp_client):
    """'tool_error' event produces a log with [ERROR] prefix."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("err-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await web_chat._handle_progress(
        "test-proj",
        {"type": "tool_error", "data": "timeout", "iteration": 1, "ts": 100.0},
    )

    messages = []
    for _ in range(2):
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        messages.append(msg)

    log_msg = next(m for m in messages if m["type"] == "log")
    assert "[ERROR]" in log_msg["text"]
    assert "timeout" in log_msg["text"]

    await ws.close()
    await store.close()


async def test_progress_error_sends_chat_reply(aiohttp_client):
    """'error' progress event produces both a log and a chat reply."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("error-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await web_chat._handle_progress(
        "test-proj",
        {"type": "error", "data": "SDK crashed", "iteration": 3, "ts": 100.0},
    )

    messages = []
    for _ in range(4):  # progress + log + reply + channel_status
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        messages.append(msg)

    types = [m["type"] for m in messages]
    assert "progress" in types
    assert "log" in types
    assert "reply" in types

    reply = next(m for m in messages if m["type"] == "reply")
    assert "SDK crashed" in reply["text"]
    assert "@orchestrator" in reply["text"]

    progress = next(m for m in messages if m["type"] == "progress")
    assert progress.get("status") == "error"

    # Verify persisted as assistant message
    msgs = await store.get_recent_messages(ch_id)
    assert any("SDK crashed" in m["content"] for m in msgs)

    await ws.close()
    await store.close()


# ── Channel list includes project fields ─────────────────────────────


async def test_channels_list_includes_project_fields(aiohttp_client):
    """Channel list includes project_id and project_status."""
    client, _, router, store = await _make_web(aiohttp_client)
    await client.post("/api/channels", json={"name": "proj-ch", "project_id": "test-proj"})

    resp = await client.get("/api/channels")
    data = await resp.json()
    assert len(data) == 1
    assert data[0]["project_id"] == "test-proj"
    assert data[0]["project_status"] == "idle"

    await store.close()


async def test_channels_list_unconnected_status(aiohttp_client):
    """Channel without project shows 'unconnected' status."""
    client, _, _, store = await _make_web(aiohttp_client)
    await client.post("/api/channels", json={"name": "solo-ch"})

    resp = await client.get("/api/channels")
    data = await resp.json()
    assert data[0]["project_id"] is None
    assert data[0]["project_status"] == "unconnected"

    await store.close()


# ── Multi-client channel isolation ───────────────────────────────────


async def test_ws_reply_only_sent_to_same_channel_viewers(aiohttp_client):
    """Client on channel A does NOT receive replies for channel B."""
    client, _, router, store = await _make_web(aiohttp_client)
    ch_a = await store.create_channel("ch-a")
    ch_b = await store.create_channel("ch-b")
    await router.connect_channel(ch_a, "test-proj")

    async def mock_route(chat_id, text, send_reply, send_log=None):
        await send_reply("reply for A")

    router.route_message = mock_route

    ws_a = await client.ws_connect("/ws")
    ws_b = await client.ws_connect("/ws")

    await ws_a.send_json({"type": "switch_channel", "channel_id": ch_a})
    await ws_b.send_json({"type": "switch_channel", "channel_id": ch_b})
    await ws_a.receive_json()
    await ws_b.receive_json()

    await ws_a.send_json({"type": "message", "text": "test message"})

    msg_a = await asyncio.wait_for(ws_a.receive_json(), timeout=1)
    assert msg_a["type"] == "reply"
    assert msg_a["text"] == "reply for A"

    # ws_b should NOT receive the reply — use a short timeout
    try:
        msg_b = await asyncio.wait_for(ws_b.receive_json(), timeout=0.2)
        # If we get here, check it's a broadcast (channel_status) not a reply
        assert msg_b["type"] != "reply"
    except asyncio.TimeoutError:
        pass  # Expected — no message for channel B viewer

    await ws_a.close()
    await ws_b.close()
    await store.close()


# ── task_list progress event ─────────────────────────────────────────


async def test_progress_task_list_sent_to_ws(aiohttp_client):
    """task_list progress event emits a task_list WS message (no store persistence)."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("tl-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()  # channel_switched

    await web_chat._handle_progress(
        "test-proj",
        {
            "type": "task_list",
            "data": "- [x] Check reward\n- [ ] Analyze logs",
            "iteration": 2,
            "ts": 300.0,
        },
    )

    # Should get task_list + channel_status
    messages = []
    for _ in range(2):
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        messages.append(msg)

    tl_msg = next(m for m in messages if m["type"] == "task_list")
    assert "- [x] Check reward" in tl_msg["data"]
    assert "- [ ] Analyze logs" in tl_msg["data"]
    assert tl_msg["iteration"] == 2

    # Not persisted as a chat message or log
    logs = await store.get_logs(ch_id)
    history = await store.get_recent_messages(ch_id)
    assert not any("Check reward" in entry["text"] for entry in logs)
    assert not any("Check reward" in m["content"] for m in history)

    await ws.close()
    await store.close()
