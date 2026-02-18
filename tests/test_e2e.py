"""End-to-end integration tests with real Claude Code.

These tests actually call Claude and cost money. Run with:
    uv run pytest tests/test_e2e.py -v -s

Skip with: pytest -k "not e2e"
"""

import asyncio
import json
import os
import tempfile
import pytest
from aiohttp import web, ClientSession, ClientTimeout

from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage
from claude_agent_sdk.types import PermissionResultAllow


def _clear_nesting_guard():
    """Remove CLAUDECODE env var to allow spawning Claude subprocesses."""
    os.environ.pop("CLAUDECODE", None)


async def _prompt_stream(text: str, done: asyncio.Event | None = None):
    """Wrap a string prompt into AsyncIterable for streaming mode.

    When `done` is provided, keeps the stream alive until the event is set.
    This prevents the SDK from closing stdin before can_use_tool control
    protocol messages are exchanged.
    """
    yield {
        "type": "user",
        "session_id": "",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
    }
    if done is not None:
        await done.wait()


# ── Daemon helpers ────────────────────────────────────────────────────


def _setup_daemon_app():
    """Create a daemon aiohttp app with all routes from orchestrator_daemon module."""
    import tools.orchestrator_daemon as daemon_mod
    app = web.Application()
    app.router.add_post("/register", daemon_mod.handle_register)
    app.router.add_post("/task", daemon_mod.handle_task)
    app.router.add_post("/interrupt", daemon_mod.handle_interrupt)
    app.router.add_get("/status", daemon_mod.handle_status)
    app.router.add_get("/stream", daemon_mod.handle_stream)
    app.router.add_post("/stop", daemon_mod.handle_stop)
    app.router.add_get("/health", daemon_mod.handle_health)
    return app


def _patch_daemon_globals(name: str, callback_url: str):
    """Patch daemon module globals and return a restore function."""
    import tools.orchestrator_daemon as daemon_mod
    orig = {
        "DAEMON_NAME": daemon_mod.DAEMON_NAME,
        "CALLBACK_URL": daemon_mod.CALLBACK_URL,
        "projects": dict(daemon_mod.projects),
        "task_states": dict(daemon_mod.task_states),
        "running_tasks": dict(daemon_mod.running_tasks),
    }
    daemon_mod.DAEMON_NAME = name
    daemon_mod.CALLBACK_URL = callback_url

    def restore():
        daemon_mod.DAEMON_NAME = orig["DAEMON_NAME"]
        daemon_mod.CALLBACK_URL = orig["CALLBACK_URL"]
        daemon_mod.projects.clear()
        daemon_mod.projects.update(orig["projects"])
        daemon_mod.task_states.clear()
        daemon_mod.task_states.update(orig["task_states"])
        daemon_mod.running_tasks.clear()
        daemon_mod.running_tasks.update(orig["running_tasks"])

    return restore


# ── T1: Agent SDK basic query ────────────────────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_sdk_basic():
    """Agent SDK query() works and returns a ResultMessage."""
    _clear_nesting_guard()
    options = ClaudeAgentOptions(
        model="haiku",
        cwd="/tmp",
    )

    got_result = False
    async for message in query(prompt="Reply with exactly: SDK_OK", options=options):
        if isinstance(message, ResultMessage):
            got_result = True
            assert message.session_id, "No session_id in ResultMessage"
            assert message.result, "No result text"
            assert "SDK_OK" in message.result, f"Unexpected: {message.result[:200]}"
            print(f"SDK result: {message.result[:100]}")
            print(f"Cost: ${message.total_cost_usd}")

    assert got_result, "Never received a ResultMessage"


# ── T2: Agent SDK session resume ─────────────────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_sdk_resume():
    """Session resume works via Agent SDK — context persists across calls."""
    _clear_nesting_guard()

    # First query: establish context
    session_id = None
    options = ClaudeAgentOptions(model="haiku", cwd="/tmp")

    async for message in query(prompt="Remember: PINEAPPLE", options=options):
        if isinstance(message, ResultMessage):
            session_id = message.session_id

    assert session_id, "No session_id from first query"

    # Second query: resume and verify context
    options2 = ClaudeAgentOptions(model="haiku", cwd="/tmp", resume=session_id)
    async for message in query(
        prompt="What fruit did I mention? Reply with just the word.",
        options=options2,
    ):
        if isinstance(message, ResultMessage):
            assert "PINEAPPLE" in message.result.upper(), f"Resume failed: {message.result}"
            print(f"Resumed OK: {message.result[:100]}")


# ── T3: Daemon health check ─────────────────────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_daemon_health():
    """Daemon health endpoint works when served."""
    _clear_nesting_guard()

    restore = _patch_daemon_globals(name="test", callback_url="http://127.0.0.1:19999")
    app = _setup_daemon_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 18300)
    await site.start()

    try:
        async with ClientSession() as http:
            async with http.get("http://127.0.0.1:18300/health") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["status"] == "ok"
                assert data["daemon"] == "test"
    finally:
        restore()
        await runner.cleanup()


# ── T4: Daemon register + task round trip ────────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_daemon_task():
    """Register a project on the daemon and start a task that runs RALPH loop."""
    _clear_nesting_guard()

    restore = _patch_daemon_globals(
        name="e2e-test",
        callback_url="http://127.0.0.1:19999",  # dummy — won't trigger escalation
    )

    app = _setup_daemon_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 18301)
    await site.start()

    try:
        async with ClientSession() as http:
            # Register project
            async with http.post(
                "http://127.0.0.1:18301/register",
                json={
                    "project_id": "e2e-test",
                    "project_dir": "/tmp",
                    "name": "E2E Test",
                },
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["ok"] is True

            # Start task
            async with http.post(
                "http://127.0.0.1:18301/task",
                json={
                    "project_id": "e2e-test",
                    "task": "Reply with exactly 'TASK_OK' and include [TASK_COMPLETE] at the end.",
                    "max_iterations": 3,
                },
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["ok"] is True
                assert data["status"] == "started"

            # Wait for completion
            for _ in range(60):
                await asyncio.sleep(2)
                async with http.get(
                    "http://127.0.0.1:18301/status?project_id=e2e-test"
                ) as resp:
                    status = await resp.json()
                    if status.get("status") in ("done", "error", "stuck"):
                        break
            else:
                pytest.fail("Task did not complete within timeout")

            print(f"Task final status: {status}")
            assert status["status"] == "done", f"Expected done, got {status['status']}: {status.get('error', '')}"

    finally:
        restore()
        await runner.cleanup()


# ── T5: Daemon interrupt flow ────────────────────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_daemon_interrupt():
    """Interrupt a running daemon task — verify interrupt is queued."""
    _clear_nesting_guard()

    restore = _patch_daemon_globals(
        name="e2e-int",
        callback_url="http://127.0.0.1:19999",
    )

    app = _setup_daemon_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 18302)
    await site.start()

    try:
        async with ClientSession() as http:
            # Register
            await http.post(
                "http://127.0.0.1:18302/register",
                json={"project_id": "int-test", "project_dir": "/tmp"},
            )

            # Start a longer task
            await http.post(
                "http://127.0.0.1:18302/task",
                json={
                    "project_id": "int-test",
                    "task": "List all files in /tmp and summarize. Include [TASK_COMPLETE] when done.",
                    "max_iterations": 5,
                },
            )

            # Wait a moment then interrupt
            await asyncio.sleep(1)
            async with http.post(
                "http://127.0.0.1:18302/interrupt",
                json={"project_id": "int-test", "message": "Also check /tmp/test if it exists"},
            ) as resp:
                data = await resp.json()
                assert data["ok"] is True
                assert data["queued"] is True

            # Wait for completion
            for _ in range(60):
                await asyncio.sleep(2)
                async with http.get(
                    "http://127.0.0.1:18302/status?project_id=int-test"
                ) as resp:
                    status = await resp.json()
                    if status.get("status") in ("done", "error", "stuck"):
                        break

            print(f"Interrupt test final status: {status}")
            # Task should finish (done or stuck, not error)
            assert status["status"] in ("done", "stuck")

    finally:
        restore()
        await runner.cleanup()


# ── T6: SDK with can_use_tool ────────────────────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_sdk_can_use_tool():
    """Agent SDK with can_use_tool callback and streaming prompt.

    Validates the critical pattern: _prompt_stream + done event + can_use_tool.
    """
    _clear_nesting_guard()

    tool_calls = []

    async def track_tools(tool_name, input_data, context=None):
        tool_calls.append(tool_name)
        return PermissionResultAllow()

    done_event = asyncio.Event()
    options = ClaudeAgentOptions(
        model="haiku",
        cwd="/tmp",
        can_use_tool=track_tools,
        allowed_tools=["Read", "Glob"],
    )

    got_result = False
    async for message in query(
        prompt=_prompt_stream("List .py files in /tmp (if any). Reply with the count.", done_event),
        options=options,
    ):
        if isinstance(message, ResultMessage):
            got_result = True
            done_event.set()
            print(f"can_use_tool result: {message.result[:200]}")
            print(f"Tools used: {tool_calls}")

    assert got_result, "Never received a ResultMessage"
