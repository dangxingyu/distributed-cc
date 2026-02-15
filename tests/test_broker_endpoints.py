"""Test broker HTTP endpoints and SessionManager ↔ Broker communication.

Starts a real broker HTTP server (with mocked Agent SDK) to verify
the HTTP interface is correct and SessionManager can talk to it.
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, TestServer

from src.session import SessionManager, ServerConfig, SessionResult


# ── Fake broker app (same routes, mocked SDK) ─────────────────────────

def create_fake_broker_app(fake_result="Task done.", fake_cost=0.05):
    """Create a broker-like aiohttp app that returns canned responses."""
    sessions = {}

    async def handle_run(request):
        data = await request.json()
        sid = data["session_id"]

        if sid in sessions and sessions[sid] == "running":
            return web.json_response(
                {"error": f"Session {sid} is already running"}, status=409
            )

        sessions[sid] = "running"
        await asyncio.sleep(0.1)  # simulate work
        sessions[sid] = "idle"

        return web.json_response({
            "session_id": f"sdk-{sid}-abc123",
            "result": fake_result,
            "cost_usd": fake_cost,
            "duration_secs": 0.1,
        })

    async def handle_health(request):
        return web.json_response({"status": "ok", "server": "test"})

    async def handle_sessions(request):
        return web.json_response([
            {"session_id": k, "status": v, "sdk_session_id": "", "started_at": 0}
            for k, v in sessions.items()
        ])

    async def handle_kill(request):
        data = await request.json()
        sid = data.get("session_id")
        if sid in sessions and sessions[sid] == "running":
            sessions[sid] = "idle"
            return web.json_response({"ok": True})
        return web.json_response({"ok": False, "reason": "No running task"})

    app = web.Application()
    app.router.add_post("/run", handle_run)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/sessions", handle_sessions)
    app.router.add_post("/kill", handle_kill)
    return app


# ── Tests ──────────────────────────────────────────────────────────────

@pytest.fixture
async def broker_server(aiohttp_server):
    app = create_fake_broker_app(fake_result="Hello from broker!")
    server = await aiohttp_server(app)
    yield server


@pytest.fixture
async def session_mgr(broker_server):
    cfg = ServerConfig(
        name="test-server",
        host=None,
        work_dir="/tmp",
        broker_port=broker_server.port,
    )
    mgr = SessionManager(servers=[cfg], default_model="haiku")
    await mgr.init()
    yield mgr
    await mgr.close()


@pytest.mark.asyncio
async def test_health_check(session_mgr):
    ok = await session_mgr.check_health("test-server")
    assert ok is True


@pytest.mark.asyncio
async def test_health_check_bad_server(session_mgr):
    ok = await session_mgr.check_health("nonexistent")
    assert ok is False


@pytest.mark.asyncio
async def test_run_task_success(session_mgr):
    result = await session_mgr.run_task("test-server", "dev", "do something")
    assert isinstance(result, SessionResult)
    assert result.is_error is False
    assert result.result_text == "Hello from broker!"
    assert result.cost_usd == 0.05
    assert result.duration_secs > 0
    print(f"Result: {result}")


@pytest.mark.asyncio
async def test_run_task_unknown_server(session_mgr):
    result = await session_mgr.run_task("nonexistent", "dev", "do something")
    assert result.is_error is True
    assert "Unknown server" in result.result_text


@pytest.mark.asyncio
async def test_cancel_task(session_mgr):
    ok = await session_mgr.cancel_task("test-server", "dev")
    # No task running, should return False
    assert ok is False


# ── Direct HTTP tests ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_broker_run_endpoint(aiohttp_client):
    app = create_fake_broker_app(fake_result="Direct test")
    client = await aiohttp_client(app)

    resp = await client.post("/run", json={
        "session_id": "test-sess",
        "prompt": "hello",
        "model": "haiku",
    })
    assert resp.status == 200
    data = await resp.json()
    assert data["result"] == "Direct test"
    assert "session_id" in data
    assert "cost_usd" in data
    assert "duration_secs" in data


@pytest.mark.asyncio
async def test_broker_health_endpoint(aiohttp_client):
    app = create_fake_broker_app()
    client = await aiohttp_client(app)

    resp = await client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_broker_sessions_endpoint(aiohttp_client):
    app = create_fake_broker_app()
    client = await aiohttp_client(app)

    # Run a task first to populate sessions
    await client.post("/run", json={
        "session_id": "s1", "prompt": "test", "model": "haiku"
    })

    resp = await client.get("/sessions")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) >= 1
    assert any(s["session_id"] == "s1" for s in data)


@pytest.mark.asyncio
async def test_broker_kill_no_task(aiohttp_client):
    app = create_fake_broker_app()
    client = await aiohttp_client(app)

    resp = await client.post("/kill", json={"session_id": "nothing"})
    assert resp.status == 200
    data = await resp.json()
    assert data["ok"] is False
