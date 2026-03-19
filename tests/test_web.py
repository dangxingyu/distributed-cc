"""Web frontend tests for multi-client/channel interaction behavior."""

import asyncio
import tempfile
from unittest.mock import AsyncMock

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
        "test-proj": RemoteOrchestrator(
            project_id="test-proj",
            name="test-server",
            host="user@test-host",
            project_dir="/tmp/test-proj",
            provider="codex",
            sandbox_mode="workspace-write",
            approval_policy="never",
            status="idle",
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
    app.router.add_get("/api/setup-notes", web_chat._handle_setup_notes)
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
    assert await store.get_channel_list() == []

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


async def test_history_limit_returns_latest_messages(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("history-limit")
    await store.add_message(ch_id, "user", "m1")
    await store.add_message(ch_id, "assistant", "m2")
    await store.add_message(ch_id, "assistant", "m3")

    resp = await client.get(f"/api/history?channel={ch_id}&limit=2")
    assert resp.status == 200
    data = await resp.json()
    assert [m["content"] for m in data] == ["m2", "m3"]
    await store.close()


async def test_history_limit_invalid_rejected(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("history-limit-invalid")

    resp = await client.get(f"/api/history?channel={ch_id}&limit=0")
    assert resp.status == 400
    payload = await resp.json()
    assert "limit" in payload["error"].lower()
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


async def test_setup_notes_empty_when_no_config_md(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    resp = await client.get("/api/setup-notes")
    assert resp.status == 200
    data = await resp.json()
    assert data["notes"] == ""
    await store.close()


async def test_setup_notes_reads_config_md(aiohttp_client, tmp_path):
    (tmp_path / "config.json").write_text('{"servers": []}')
    (tmp_path / "config.md").write_text("cluster: tiger\nscheduler: slurm\n")

    store = Store(tempfile.mkdtemp())
    await store.init()
    router = Router(cwd=str(tmp_path))
    web_chat = WebChat(router=router, store=store)

    app = web.Application()
    app.router.add_get("/api/setup-notes", web_chat._handle_setup_notes)
    client = await aiohttp_client(app)

    resp = await client.get("/api/setup-notes")
    assert resp.status == 200
    data = await resp.json()
    assert "scheduler: slurm" in data["notes"]
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
    client, _, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("test-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})

    msg = await ws.receive_json()
    assert msg["type"] == "channel_switched"
    assert msg["channel_id"] == ch_id
    assert msg["project_id"] == "test-proj"
    assert msg["project"]["provider"] == "codex"
    assert msg["project"]["sandbox_mode"] == "workspace-write"
    assert msg["project"]["approval_policy"] == "never"

    await ws.close()
    await store.close()


async def test_ws_switch_channel_rejects_unknown_channel(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": 999999})

    msg = await ws.receive_json()
    assert msg["type"] == "error"
    assert "unknown channel_id" in msg["text"]

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


async def test_ws_recall_queued_message_requires_channel(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "recall_queued_message"})

    msg = await ws.receive_json()
    assert msg["type"] == "error"
    assert "No channel" in msg["text"]

    await ws.close()
    await store.close()


async def test_ws_recall_queued_message_returns_latest_for_channel(aiohttp_client):
    client, _, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("recall-ch")
    await router.connect_channel(ch_id, "test-proj")
    first_mid = await store.add_message(ch_id, "user", "first queued")
    latest_mid = await store.add_message(ch_id, "user", "latest queued")
    router._deferred_tasks["test-proj"] = [
        {"chat_id": ch_id, "text": "first queued", "message_id": first_mid, "ts": 1.0},
        {"chat_id": 999999, "text": "other channel item", "ts": 2.0},
        {"chat_id": ch_id, "text": "latest queued", "message_id": latest_mid, "ts": 3.0},
    ]

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await ws.send_json({"type": "recall_queued_message"})
    first = await asyncio.wait_for(ws.receive_json(), timeout=1)
    second = await asyncio.wait_for(ws.receive_json(), timeout=1)
    message_retracted = first if first["type"] == "message_retracted" else second
    recalled = first if first["type"] == "queue_recall" else second

    assert message_retracted["type"] == "message_retracted"
    assert message_retracted["sender"] == "user"
    assert message_retracted["text"] == "latest queued"
    assert message_retracted["message_id"] == latest_mid
    assert message_retracted["ok"] is True

    assert recalled["type"] == "queue_recall"
    assert recalled["ok"] is True
    assert recalled["retracted"] is True
    assert recalled["text"] == "latest queued"
    assert recalled["message_id"] == latest_mid

    assert [item["text"] for item in router._deferred_tasks["test-proj"]] == [
        "first queued",
        "other channel item",
    ]
    history = await store.get_recent_messages(ch_id)
    assert [m["content"] for m in history if m["role"] == "user"] == ["first queued"]

    await ws.close()
    await store.close()


async def test_ws_recall_broadcasts_retraction_to_same_channel_viewers(aiohttp_client):
    client, _, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("recall-broadcast")
    await router.connect_channel(ch_id, "test-proj")
    mid = await store.add_message(ch_id, "user", "queued from me")
    router._deferred_tasks["test-proj"] = [
        {"chat_id": ch_id, "text": "queued from me", "message_id": mid, "ts": 1.0},
    ]

    ws1 = await client.ws_connect("/ws")
    ws2 = await client.ws_connect("/ws")
    await ws1.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws2.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws1.receive_json()
    await ws2.receive_json()
    await ws1.send_json({"type": "recall_queued_message"})

    got_ws1 = []
    got_ws2 = []
    for _ in range(2):
        got_ws1.append(await asyncio.wait_for(ws1.receive_json(), timeout=1))
    got_ws2.append(await asyncio.wait_for(ws2.receive_json(), timeout=1))

    assert any(m["type"] == "message_retracted" for m in got_ws1)
    assert any(m["type"] == "queue_recall" for m in got_ws1)
    assert got_ws2[0]["type"] == "message_retracted"
    assert got_ws2[0]["text"] == "queued from me"
    assert got_ws2[0]["message_id"] == mid

    await ws1.close()
    await ws2.close()
    await store.close()


async def test_ws_restore_retracted_message_requeues_and_persists(aiohttp_client):
    client, _, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("restore")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await ws.send_json({"type": "restore_retracted_message", "text": "put it back", "message_id": "mid-x"})
    first = await asyncio.wait_for(ws.receive_json(), timeout=1)
    second = await asyncio.wait_for(ws.receive_json(), timeout=1)
    restored = first if first["type"] == "message_restored" else second
    ack = first if first["type"] == "queue_restore" else second

    assert restored["type"] == "message_restored"
    assert restored["text"] == "put it back"
    assert restored["message_id"] == "mid-x"
    assert ack["type"] == "queue_restore"
    assert ack["ok"] is True
    assert ack["queue_size"] == 1

    assert router._deferred_tasks["test-proj"][0]["text"] == "put it back"
    assert router._deferred_tasks["test-proj"][0]["message_id"] == "mid-x"
    msgs = await store.get_recent_messages(ch_id)
    assert any(m["role"] == "user" and m["content"] == "put it back" and m.get("id") == "mid-x" for m in msgs)

    await ws.close()
    await store.close()


async def test_ws_message_ack(aiohttp_client):
    client, _, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("ack-ch")
    await router.connect_channel(ch_id, "test-proj")

    async def no_op_route(chat_id, text, send_reply, send_log=None, send_typing=None):
        return

    router.route_message = no_op_route

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await ws.send_json({"type": "message", "text": "hello ack", "client_msg_id": "cmsg-1"})

    msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
    assert msg["type"] == "message_ack"
    assert msg["client_msg_id"] == "cmsg-1"
    assert isinstance(msg.get("message_id"), str) and msg["message_id"]

    history = await store.get_recent_messages(ch_id)
    assert any(m["role"] == "user" and m["content"] == "hello ack" for m in history)

    await ws.close()
    await store.close()


async def test_ws_route_failure_surfaces_to_user_and_logs(aiohttp_client):
    client, _, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("route-fail")
    await router.connect_channel(ch_id, "test-proj")

    async def failing_route(chat_id, text, send_reply, send_log=None, send_typing=None):
        raise RuntimeError("simulated route failure")

    router.route_message = failing_route

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await ws.send_json({"type": "message", "text": "trigger failure"})

    msg1 = await asyncio.wait_for(ws.receive_json(), timeout=1)
    msg2 = await asyncio.wait_for(ws.receive_json(), timeout=1)
    types = {msg1["type"], msg2["type"]}
    assert "log" in types
    assert "reply" in types

    reply = msg1 if msg1["type"] == "reply" else msg2
    assert reply["sender"] == "system"
    assert "Routing failure" in reply["text"]

    logs = await store.get_logs(ch_id)
    assert any("Routing failure" in entry["text"] for entry in logs)
    messages = await store.get_recent_messages(ch_id)
    assert any(
        m["role"] == "assistant" and m.get("sender") == "system" and "Routing failure" in m["content"]
        for m in messages
    )

    await ws.close()
    await store.close()


async def test_ws_message_broadcasts_reply_to_same_channel_viewers(aiohttp_client):
    client, _, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("routed-ch")
    await router.connect_channel(ch_id, "test-proj")

    async def mock_route(chat_id, text, send_reply, send_log=None, send_typing=None):
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


async def test_ws_typing_event_includes_token(aiohttp_client):
    client, _, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("typing-token")
    await router.connect_channel(ch_id, "test-proj")

    async def mock_route(chat_id, text, send_reply, send_log=None, send_typing=None):
        await send_typing(True, "router", "router-token-1")
        await send_typing(False, "router", "router-token-1")

    router.route_message = mock_route

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await ws.send_json({"type": "message", "text": "trigger typing"})

    typing_events = []
    for _ in range(4):
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        if msg.get("type") == "typing":
            typing_events.append(msg)
        if len(typing_events) == 2:
            break

    assert len(typing_events) == 2
    assert typing_events[0]["active"] is True
    assert typing_events[1]["active"] is False
    assert typing_events[0]["sender"] == "router"
    assert typing_events[1]["sender"] == "router"
    assert typing_events[0]["token"] == "router-token-1"
    assert typing_events[1]["token"] == "router-token-1"

    await ws.close()
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
        m["role"] == "assistant" and "@worker: run focused tests and report" in m["content"]
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


async def test_logs_limit_returns_latest_entries(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("log-limit")
    await store.add_log(ch_id, "l1")
    await store.add_log(ch_id, "l2")
    await store.add_log(ch_id, "l3")

    resp = await client.get(f"/api/logs?channel={ch_id}&limit=2")
    assert resp.status == 200
    data = await resp.json()
    assert [row["text"] for row in data] == ["l2", "l3"]
    await store.close()


async def test_logs_limit_invalid_rejected(aiohttp_client):
    client, _, _, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("log-limit-invalid")

    resp = await client.get(f"/api/logs?channel={ch_id}&limit=-1")
    assert resp.status == 400
    payload = await resp.json()
    assert "limit" in payload["error"].lower()
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


async def test_ws_switch_channel_uses_fresh_status_on_reconnect(aiohttp_client):
    """switch_channel acks fast, then pushes refreshed status asynchronously."""
    client, _, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("proj-ch")
    await router.connect_channel(ch_id, "test-proj")

    router.refresh_project_status = AsyncMock(return_value="running")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})

    first = await ws.receive_json()
    assert first["type"] == "channel_switched"
    assert first["project_id"] == "test-proj"
    assert first["project_status"] == "idle"

    second = await ws.receive_json()
    assert second["type"] == "channel_status"
    assert second["channel_id"] == ch_id
    assert second["project_status"] == "running"
    router.refresh_project_status.assert_awaited_once_with("test-proj")

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


async def test_progress_stopped_sends_chat_reply(aiohttp_client):
    """'stopped' progress event produces a stopped status and a chat reply."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("stopped-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await web_chat._handle_progress(
        "test-proj",
        {"type": "stopped", "data": "Task cancelled", "iteration": 4, "ts": 100.0},
    )

    messages = []
    for _ in range(3):  # progress + reply + channel_status
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        messages.append(msg)

    progress = next(m for m in messages if m["type"] == "progress")
    assert progress.get("status") == "stopped"
    reply = next(m for m in messages if m["type"] == "reply")
    assert "Task stopped" in reply["text"]

    history = await store.get_recent_messages(ch_id)
    assert any("Task stopped" in m["content"] for m in history)

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
    assert "Error:" in reply["text"]

    progress = next(m for m in messages if m["type"] == "progress")
    assert progress.get("status") == "error"

    # Verify persisted as assistant message
    msgs = await store.get_recent_messages(ch_id)
    assert any("SDK crashed" in m["content"] for m in msgs)

    await ws.close()
    await store.close()


# ── Orchestrator text surfaces in chat ────────────────────────────────


async def test_orchestrator_text_surfaces_in_chat(aiohttp_client):
    """[orchestrator] text events appear in chat (prefix stripped) and monitor."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("orch-text-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await web_chat._handle_progress(
        "test-proj",
        {"type": "text", "data": "[orchestrator] Let me investigate this.", "iteration": 1, "ts": 100.0},
    )

    messages = []
    for _ in range(3):  # log + reply + channel_status
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        messages.append(msg)

    types = [m["type"] for m in messages]
    assert "log" in types
    assert "reply" in types

    reply = next(m for m in messages if m["type"] == "reply")
    assert reply["text"] == "Let me investigate this."
    assert reply["sender"] == "orchestrator"

    log_msg = next(m for m in messages if m["type"] == "log")
    assert "[orchestrator]" in log_msg["text"]

    # Persisted in chat history (clean text, no prefix)
    msgs = await store.get_recent_messages(ch_id)
    assert any(m["content"] == "Let me investigate this." for m in msgs)

    await ws.close()
    await store.close()


async def test_worker_text_stays_in_monitor_only(aiohttp_client):
    """[worker] text events go to monitor log only, not chat."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("worker-text-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await web_chat._handle_progress(
        "test-proj",
        {"type": "text", "data": "[worker] Reading file parser.py...", "iteration": 1, "ts": 100.0},
    )

    messages = []
    for _ in range(2):  # log + channel_status
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        messages.append(msg)

    types = [m["type"] for m in messages]
    assert "log" in types
    assert "reply" not in types

    # Not in chat history
    msgs = await store.get_recent_messages(ch_id)
    assert not any("Reading file" in m["content"] for m in msgs)

    await ws.close()
    await store.close()


async def test_done_empty_data_no_chat_reply(aiohttp_client):
    """'done' with empty data sends progress status but no chat reply."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("done-empty-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()

    await web_chat._handle_progress(
        "test-proj",
        {"type": "done", "data": "", "iteration": 1, "ts": 100.0},
    )

    messages = []
    for _ in range(2):  # progress + channel_status
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        messages.append(msg)

    types = [m["type"] for m in messages]
    assert "progress" in types
    assert "reply" not in types

    progress = next(m for m in messages if m["type"] == "progress")
    assert progress.get("status") == "done"

    # No chat message persisted
    msgs = await store.get_recent_messages(ch_id)
    assert len(msgs) == 0

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

    async def mock_route(chat_id, text, send_reply, send_log=None, send_typing=None):
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


async def test_progress_log_update_sent_to_ws(aiohttp_client):
    """log_update progress event emits a log_update WS message."""
    client, web_chat, router, store = await _make_web(aiohttp_client)
    ch_id = await store.create_channel("log-ch")
    await router.connect_channel(ch_id, "test-proj")

    ws = await client.ws_connect("/ws")
    await ws.send_json({"type": "switch_channel", "channel_id": ch_id})
    await ws.receive_json()  # channel_switched

    await web_chat._handle_progress(
        "test-proj",
        {
            "type": "log_update",
            "data": "Hypothesis: reward hacking causing plateau",
            "iteration": 1,
            "ts": 400.0,
        },
    )

    messages = []
    for _ in range(2):
        msg = await asyncio.wait_for(ws.receive_json(), timeout=1)
        messages.append(msg)

    log_msg = next(m for m in messages if m["type"] == "log_update")
    assert "reward hacking" in log_msg["data"]
    assert log_msg["iteration"] == 1

    # Persisted to monitor logs for history/reload
    logs = await store.get_logs(ch_id)
    assert any("reward hacking" in entry["text"] for entry in logs)

    # Not persisted as a chat message
    history = await store.get_recent_messages(ch_id)
    assert not any("reward hacking" in m["content"] for m in history)

    await ws.close()
    await store.close()


async def test_progress_only_emits_for_web_channels(aiohttp_client):
    """Progress events should only be emitted to channels with source='web' or None."""
    client, web_chat, router, store = await _make_web(aiohttp_client)

    # Create a web channel and a telegram channel on the same project
    web_ch = await store.create_channel("web-chan", project_id="test-proj", source="web")
    tg_ch = await store.create_channel("tg-chan", project_id="test-proj", source="telegram")

    await router.connect_channel(web_ch, "test-proj", source="web")
    await router.connect_channel(tg_ch, "test-proj", source="telegram")

    # Fire progress event through the web handler directly
    await web_chat._handle_progress("test-proj", {
        "type": "text",
        "data": "[orchestrator] hello from progress",
        "iteration": 1,
        "ts": 1234.0,
    })

    # Web channel should have the message persisted
    web_msgs = await store.get_recent_messages(web_ch)
    assert any("hello from progress" in m["content"] for m in web_msgs)

    # Telegram channel should NOT have the message
    tg_msgs = await store.get_recent_messages(tg_ch)
    assert not any("hello from progress" in m["content"] for m in tg_msgs)

    await store.close()
