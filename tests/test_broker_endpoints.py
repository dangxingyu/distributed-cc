"""Test orchestrator daemon HTTP endpoints.

Starts a fake daemon app (same routes, mocked RALPH loop) to verify
the HTTP interface is correct.
"""

import asyncio
import json
import pytest
from aiohttp import web


# ── Fake daemon app ───────────────────────────────────────────────────

def create_fake_daemon_app():
    """Create a daemon-like aiohttp app with canned responses."""
    projects = {}
    task_states = {}
    interrupt_queues = {}

    async def handle_register(request):
        data = await request.json()
        pid = data.get("project_id")
        if not pid or not data.get("project_dir"):
            return web.json_response({"error": "project_id and project_dir required"}, status=400)
        projects[pid] = data
        return web.json_response({"ok": True, "project_id": pid})

    async def handle_task(request):
        data = await request.json()
        pid = data.get("project_id")
        task = data.get("task")
        if not pid or not task:
            return web.json_response({"error": "project_id and task required"}, status=400)
        if pid not in projects:
            return web.json_response({"error": f"Unknown project: {pid}"}, status=404)
        if pid in task_states and task_states[pid] == "running":
            return web.json_response(
                {"error": f"Project {pid} already has a running task"},
                status=409,
            )
        task_states[pid] = "running"
        return web.json_response({"ok": True, "project_id": pid, "status": "started"})

    async def handle_interrupt(request):
        data = await request.json()
        pid = data.get("project_id")
        msg = data.get("message", "")
        if pid not in interrupt_queues:
            interrupt_queues[pid] = []
        interrupt_queues[pid].append(msg)
        return web.json_response({"ok": True, "queued": True})

    async def handle_status(request):
        pid = request.query.get("project_id")
        if not pid:
            return web.json_response({
                p: {"status": s} for p, s in task_states.items()
            })
        if pid not in projects:
            return web.json_response({"error": "Unknown project"}, status=404)
        status = task_states.get(pid, "idle")
        return web.json_response({
            "project_id": pid,
            "status": status,
            "iteration": 3,
            "max_iterations": 20,
        })

    async def handle_stop(request):
        data = await request.json()
        pid = data.get("project_id")
        if pid in task_states and task_states[pid] == "running":
            task_states[pid] = "stopped"
            return web.json_response({"ok": True, "status": "stopping"})
        return web.json_response({"ok": False, "reason": "No running task"})

    async def handle_health(request):
        return web.json_response({"status": "ok", "daemon": "test", "projects": list(projects.keys())})

    app = web.Application()
    app.router.add_post("/register", handle_register)
    app.router.add_post("/task", handle_task)
    app.router.add_post("/interrupt", handle_interrupt)
    app.router.add_get("/status", handle_status)
    app.router.add_post("/stop", handle_stop)
    app.router.add_get("/health", handle_health)
    return app


# ── Tests ──────────────────────────────────────────────────────────────

@pytest.fixture
async def daemon_client(aiohttp_client):
    app = create_fake_daemon_app()
    client = await aiohttp_client(app)
    return client


async def test_health(daemon_client):
    resp = await daemon_client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "ok"


async def test_register_project(daemon_client):
    resp = await daemon_client.post("/register", json={
        "project_id": "test-proj",
        "project_dir": "/tmp/test",
        "name": "Test Project",
    })
    assert resp.status == 200
    data = await resp.json()
    assert data["ok"] is True
    assert data["project_id"] == "test-proj"


async def test_register_missing_fields(daemon_client):
    resp = await daemon_client.post("/register", json={"project_id": "test"})
    assert resp.status == 400


async def test_start_task(daemon_client):
    # Register first
    await daemon_client.post("/register", json={
        "project_id": "proj",
        "project_dir": "/tmp/proj",
    })

    resp = await daemon_client.post("/task", json={
        "project_id": "proj",
        "task": "fix the bug",
    })
    assert resp.status == 200
    data = await resp.json()
    assert data["ok"] is True
    assert data["status"] == "started"


async def test_start_task_unknown_project(daemon_client):
    resp = await daemon_client.post("/task", json={
        "project_id": "nonexistent",
        "task": "do something",
    })
    assert resp.status == 404


async def test_start_task_already_running(daemon_client):
    await daemon_client.post("/register", json={
        "project_id": "busy",
        "project_dir": "/tmp/busy",
    })
    await daemon_client.post("/task", json={
        "project_id": "busy",
        "task": "first task",
    })

    resp = await daemon_client.post("/task", json={
        "project_id": "busy",
        "task": "second task",
    })
    assert resp.status == 409


async def test_interrupt(daemon_client):
    resp = await daemon_client.post("/interrupt", json={
        "project_id": "proj",
        "message": "also check tests",
    })
    assert resp.status == 200
    data = await resp.json()
    assert data["ok"] is True
    assert data["queued"] is True


async def test_status_all(daemon_client):
    await daemon_client.post("/register", json={
        "project_id": "p1",
        "project_dir": "/tmp/p1",
    })
    resp = await daemon_client.get("/status")
    assert resp.status == 200


async def test_status_specific(daemon_client):
    await daemon_client.post("/register", json={
        "project_id": "p2",
        "project_dir": "/tmp/p2",
    })
    resp = await daemon_client.get("/status?project_id=p2")
    assert resp.status == 200
    data = await resp.json()
    assert data["project_id"] == "p2"
    assert data["status"] == "idle"


async def test_status_unknown(daemon_client):
    resp = await daemon_client.get("/status?project_id=nope")
    assert resp.status == 404


async def test_stop_running(daemon_client):
    await daemon_client.post("/register", json={
        "project_id": "stopme",
        "project_dir": "/tmp/stopme",
    })
    await daemon_client.post("/task", json={
        "project_id": "stopme",
        "task": "long task",
    })

    resp = await daemon_client.post("/stop", json={"project_id": "stopme"})
    assert resp.status == 200
    data = await resp.json()
    assert data["ok"] is True


async def test_stop_nothing_running(daemon_client):
    resp = await daemon_client.post("/stop", json={"project_id": "nothing"})
    assert resp.status == 200
    data = await resp.json()
    assert data["ok"] is False
