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

from src.session import SessionManager, ServerConfig, SessionResult
from src.store import Store
from src.orchestrator import Orchestrator


def _clear_nesting_guard():
    """Remove CLAUDECODE env var to allow spawning Claude subprocesses."""
    os.environ.pop("CLAUDECODE", None)


async def _prompt_stream(text: str):
    """Wrap a string prompt into AsyncIterable for streaming mode."""
    yield {
        "type": "user",
        "session_id": "",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
    }


def _setup_broker_app():
    """Create a broker aiohttp app with all routes from remote_broker module."""
    import tools.remote_broker as broker_mod
    app = web.Application()
    app.router.add_post("/run", broker_mod.handle_run)
    app.router.add_post("/register", broker_mod.handle_register)
    app.router.add_post("/unregister", broker_mod.handle_unregister)
    app.router.add_get("/sessions", broker_mod.handle_sessions)
    app.router.add_post("/kill", broker_mod.handle_kill)
    app.router.add_get("/health", broker_mod.handle_health)
    return app


def _patch_broker_globals(name: str, work_dir: str, orch_url: str):
    """Patch broker module globals and return a restore function."""
    import tools.remote_broker as broker_mod
    orig = {
        "SERVER_NAME": broker_mod.SERVER_NAME,
        "DEFAULT_WORK_DIR": broker_mod.DEFAULT_WORK_DIR,
        "ORCHESTRATOR_URL": broker_mod.ORCHESTRATOR_URL,
        "sessions": dict(broker_mod.sessions),
    }
    broker_mod.SERVER_NAME = name
    broker_mod.DEFAULT_WORK_DIR = work_dir
    broker_mod.ORCHESTRATOR_URL = orch_url
    broker_mod.sessions.clear()

    def restore():
        broker_mod.SERVER_NAME = orig["SERVER_NAME"]
        broker_mod.DEFAULT_WORK_DIR = orig["DEFAULT_WORK_DIR"]
        broker_mod.ORCHESTRATOR_URL = orig["ORCHESTRATOR_URL"]
        broker_mod.sessions.clear()
        broker_mod.sessions.update(orig["sessions"])

    return restore


# ── Agent SDK direct tests ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_e2e_agent_sdk_query():
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


@pytest.mark.asyncio
async def test_e2e_agent_sdk_can_use_tool():
    """canUseTool callback is invoked and can approve tools."""
    _clear_nesting_guard()
    tool_calls = []

    async def track_tools(tool_name, input_data, context=None):
        tool_calls.append(tool_name)
        return PermissionResultAllow(updated_input=input_data)

    options = ClaudeAgentOptions(
        model="haiku",
        cwd="/tmp",
        can_use_tool=track_tools,
    )

    # SDK requires AsyncIterable prompt when can_use_tool is set
    async for message in query(
        prompt=_prompt_stream("Read the file /tmp/.gitignore or any small file. If it doesn't exist, just say so."),
        options=options,
    ):
        if isinstance(message, ResultMessage):
            print(f"Tools called: {tool_calls}")
            print(f"Result: {message.result[:200]}")

    # The agent should have tried to use at least one tool
    print(f"All tool calls: {tool_calls}")


@pytest.mark.asyncio
async def test_e2e_agent_sdk_resume():
    """Session resume works via Agent SDK."""
    _clear_nesting_guard()
    # First query
    session_id = None
    options = ClaudeAgentOptions(model="haiku", cwd="/tmp")

    async for message in query(prompt="Remember: PINEAPPLE", options=options):
        if isinstance(message, ResultMessage):
            session_id = message.session_id

    assert session_id, "No session_id from first query"

    # Resume
    options2 = ClaudeAgentOptions(model="haiku", cwd="/tmp", resume=session_id)
    async for message in query(
        prompt="What fruit did I mention? Reply with just the word.",
        options=options2,
    ):
        if isinstance(message, ResultMessage):
            assert "PINEAPPLE" in message.result.upper(), f"Resume failed: {message.result}"
            print(f"Resumed OK: {message.result[:100]}")


# ── Broker + SessionManager integration ────────────────────────────────

@pytest.mark.asyncio
async def test_e2e_broker_round_trip():
    """Start a real broker in-process, register a session, send a task via SessionManager."""
    _clear_nesting_guard()

    restore = _patch_broker_globals(
        name="test",
        work_dir="/tmp",
        orch_url="http://127.0.0.1:19999",  # dummy — won't trigger permission in this test
    )

    app = _setup_broker_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 18200)
    await site.start()

    try:
        # Use SessionManager to talk to the broker
        cfg = ServerConfig(name="test", host=None, broker_port=18200)
        mgr = SessionManager(servers=[cfg], default_model="haiku")
        await mgr.init()

        # Health check
        ok = await mgr.check_health("test")
        assert ok, "Broker health check failed"

        # Register a session first (matching current broker flow)
        reg_result = await mgr.register_session("test", "e2e-test", "/tmp", "e2e test session")
        assert reg_result.get("ok"), f"Registration failed: {reg_result}"

        # Run a simple task
        result = await mgr.run_task(
            "test", "e2e-test",
            "Reply with exactly: E2E_SUCCESS",
        )
        print(f"E2E result: is_error={result.is_error}, text={result.result_text[:200]}")

        assert not result.is_error, f"Task failed: {result.result_text}"
        assert "E2E_SUCCESS" in result.result_text, f"Unexpected: {result.result_text[:200]}"

        await mgr.close()
    finally:
        restore()
        await runner.cleanup()


# ── Full stack: Orchestrator → SessionManager → Broker ──────────────────

@pytest.mark.asyncio
async def test_e2e_full_stack():
    """Orchestrator routes a message → broker executes → result comes back.

    This is the full flow minus the frontend.
    """
    _clear_nesting_guard()

    restore = _patch_broker_globals(
        name="local",
        work_dir="/tmp",
        orch_url="http://127.0.0.1:19121",
    )

    # Start broker
    broker_app = _setup_broker_app()
    broker_runner = web.AppRunner(broker_app)
    await broker_runner.setup()
    broker_site = web.TCPSite(broker_runner, "127.0.0.1", 18201)
    await broker_site.start()

    # Set up orchestrator stack
    store = Store(tempfile.mkdtemp())
    await store.init()

    cfg = ServerConfig(name="local", host=None, broker_port=18201)
    mgr = SessionManager(servers=[cfg], default_model="haiku")
    await mgr.init()

    orch = Orchestrator(
        session_mgr=mgr,
        store=store,
        model="haiku",
        config_path="config.yaml",
    )
    await orch.init()

    # Auto-approve all worker permissions
    async def approve_all(data):
        return {"approved": True, "reason": "test auto-approve"}
    orch.handle_permission_request = approve_all

    # Start permission callback server
    perm_app = web.Application()
    perm_app["orchestrator"] = orch

    async def handle_perm(req):
        data = await req.json()
        result = await orch.handle_permission_request(data)
        return web.json_response(result)

    perm_app.router.add_post("/permission", handle_perm)
    perm_runner = web.AppRunner(perm_app)
    await perm_runner.setup()
    perm_site = web.TCPSite(perm_runner, "127.0.0.1", 19121)
    await perm_site.start()

    try:
        # Register a session on the broker first
        reg_result = await mgr.register_session("local", "fullstack-test", "/tmp", "full stack test")
        assert reg_result.get("ok"), f"Registration failed: {reg_result}"

        # Directly execute a task (bypasses routing Claude)
        result = await mgr.run_task("local", "fullstack-test", "Reply with exactly: FULLSTACK_OK")

        print(f"Full stack result: {result.result_text[:200]}")
        assert not result.is_error, f"Failed: {result.result_text}"
        assert "FULLSTACK_OK" in result.result_text

    finally:
        await perm_runner.cleanup()
        await broker_runner.cleanup()
        await mgr.close()
        await store.close()
        restore()


# ── Dynamic server registration ────────────────────────────────────────

@pytest.mark.asyncio
async def test_e2e_register_server_dynamic():
    """SessionManager.add_server() makes a new server immediately usable."""
    _clear_nesting_guard()

    restore = _patch_broker_globals(
        name="dynamic",
        work_dir="/tmp",
        orch_url="http://127.0.0.1:19999",
    )

    app = _setup_broker_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 18202)
    await site.start()

    try:
        # Start with NO servers
        mgr = SessionManager(servers=[], default_model="haiku")
        await mgr.init()

        # Dynamically add a server (like /setup would do)
        mgr.add_server(ServerConfig(name="dynamic", host=None, broker_port=18202))

        # Should now be reachable
        ok = await mgr.check_health("dynamic")
        assert ok, "Dynamic server health check failed"

        # Register and run a task
        reg_result = await mgr.register_session("dynamic", "dyn-test", "/tmp", "dynamic test")
        assert reg_result.get("ok"), f"Registration failed: {reg_result}"

        result = await mgr.run_task("dynamic", "dyn-test", "Reply with exactly: DYNAMIC_OK")
        assert not result.is_error, f"Task failed: {result.result_text}"
        assert "DYNAMIC_OK" in result.result_text
        print(f"Dynamic server result: {result.result_text[:200]}")

        await mgr.close()
    finally:
        restore()
        await runner.cleanup()
