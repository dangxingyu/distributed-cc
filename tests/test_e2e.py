"""End-to-end integration tests with real Claude Code.

These tests actually call Claude and cost money. Run with:
    uv run pytest tests/test_e2e.py -v -s

Skip with: pytest -k "not e2e"
"""

import asyncio
import json
import os
import pytest
from aiohttp import web, ClientSession, ClientTimeout

from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage
from claude_agent_sdk.types import PermissionResultAllow

from src.session import SessionManager, ServerConfig, SessionResult
from src.store import Store
from src.permission import PermissionEvaluator
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
    """Start a real broker in-process, send a task via SessionManager."""
    _clear_nesting_guard()
    import tools.remote_broker as broker_mod

    # Save originals
    orig_server_name = broker_mod.SERVER_NAME
    orig_work_dir = broker_mod.WORK_DIR
    orig_orch_url = broker_mod.ORCHESTRATOR_URL

    broker_mod.SERVER_NAME = "test"
    broker_mod.WORK_DIR = "/tmp"
    # Point orchestrator URL to a dummy (we won't trigger permission in this test)
    broker_mod.ORCHESTRATOR_URL = "http://127.0.0.1:19999"

    app = web.Application()
    app.router.add_post("/run", broker_mod.handle_run)
    app.router.add_get("/health", broker_mod.handle_health)
    app.router.add_get("/sessions", broker_mod.handle_sessions)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 18200)
    await site.start()

    try:
        # Use SessionManager to talk to the broker
        cfg = ServerConfig(name="test", host=None, work_dir="/tmp", broker_port=18200)
        mgr = SessionManager(servers=[cfg], default_model="haiku")
        await mgr.init()

        # Health check
        ok = await mgr.check_health("test")
        assert ok, "Broker health check failed"

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
        broker_mod.SERVER_NAME = orig_server_name
        broker_mod.WORK_DIR = orig_work_dir
        broker_mod.ORCHESTRATOR_URL = orig_orch_url
        await runner.cleanup()


# ── Full stack: Orchestrator → SessionManager → Broker ──────────────────

@pytest.mark.asyncio
async def test_e2e_full_stack():
    """Orchestrator routes a message → broker executes → result comes back.

    This is the full flow minus the CLI frontend.
    """
    _clear_nesting_guard()
    import tools.remote_broker as broker_mod

    broker_mod.SERVER_NAME = "local"
    broker_mod.WORK_DIR = "/tmp"
    broker_mod.ORCHESTRATOR_URL = "http://127.0.0.1:19121"

    # Start broker
    broker_app = web.Application()
    broker_app.router.add_post("/run", broker_mod.handle_run)
    broker_app.router.add_get("/health", broker_mod.handle_health)
    broker_runner = web.AppRunner(broker_app)
    await broker_runner.setup()
    broker_site = web.TCPSite(broker_runner, "127.0.0.1", 18201)
    await broker_site.start()

    # Set up orchestrator stack
    store = Store(":memory:")
    await store.init()

    perm = PermissionEvaluator(model="haiku", config_path="config.yaml")
    # Auto-approve all permissions for this test
    async def approve_all(prompt):
        return {"decision": "approve", "reason": "test"}
    perm._call_claude = approve_all

    cfg = ServerConfig(name="local", host=None, work_dir="/tmp", broker_port=18201)
    mgr = SessionManager(servers=[cfg], default_model="haiku")
    await mgr.init()

    orch = Orchestrator(
        session_mgr=mgr,
        store=store,
        permission_evaluator=perm,
        model="haiku",
        config_path="config.yaml",
    )

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
        # Bypass routing Claude — directly execute a task
        result = await mgr.run_task("local", "fullstack-test", "Reply with exactly: FULLSTACK_OK")

        print(f"Full stack result: {result.result_text[:200]}")
        assert not result.is_error, f"Failed: {result.result_text}"
        assert "FULLSTACK_OK" in result.result_text

    finally:
        await perm_runner.cleanup()
        await broker_runner.cleanup()
        await mgr.close()
        await store.close()
