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


# ── Broker helpers ────────────────────────────────────────────────────


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


# ── Full stack helper ─────────────────────────────────────────────────


async def _setup_full_stack(broker_port: int, http_port: int):
    """Boot broker + permission/clarification HTTP server + orchestrator.

    Returns (orch, store, mgr, broker_runner, http_runner, restore, cleanup).
    """
    restore = _patch_broker_globals(
        name="local",
        work_dir="/tmp",
        orch_url=f"http://127.0.0.1:{http_port}",
    )

    # Start broker
    broker_app = _setup_broker_app()
    broker_runner = web.AppRunner(broker_app)
    await broker_runner.setup()
    broker_site = web.TCPSite(broker_runner, "127.0.0.1", broker_port)
    await broker_site.start()

    # Orchestrator stack
    store = Store(tempfile.mkdtemp())
    await store.init()

    cfg = ServerConfig(name="local", host=None, broker_port=broker_port)
    mgr = SessionManager(servers=[cfg], default_model="haiku")
    await mgr.init()

    orch = Orchestrator(
        session_mgr=mgr,
        store=store,
        model="haiku",
        config_path="config.yaml",
    )
    await orch.init()

    # HTTP callback server (permission + clarification endpoints)
    http_app = web.Application()
    http_app["orchestrator"] = orch

    async def handle_perm(req):
        data = await req.json()
        result = await orch.handle_permission_request(data)
        return web.json_response(result)

    async def handle_clarification(req):
        data = await req.json()
        result = await orch.handle_clarification_request(data)
        return web.json_response(result)

    http_app.router.add_post("/permission", handle_perm)
    http_app.router.add_post("/clarification", handle_clarification)
    http_runner = web.AppRunner(http_app)
    await http_runner.setup()
    http_site = web.TCPSite(http_runner, "127.0.0.1", http_port)
    await http_site.start()

    async def cleanup():
        await http_runner.cleanup()
        await broker_runner.cleanup()
        await mgr.close()
        await store.close()
        restore()

    return orch, store, mgr, broker_runner, http_runner, restore, cleanup


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


# ── T3: Broker round trip ────────────────────────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_broker_round_trip():
    """SessionManager -> broker /run -> real Claude -> result. No orchestrator."""
    _clear_nesting_guard()

    restore = _patch_broker_globals(
        name="test",
        work_dir="/tmp",
        orch_url="http://127.0.0.1:19999",  # dummy — won't trigger permission
    )

    app = _setup_broker_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 18200)
    await site.start()

    try:
        cfg = ServerConfig(name="test", host=None, broker_port=18200)
        mgr = SessionManager(servers=[cfg], default_model="haiku")
        await mgr.init()

        # Health check
        ok = await mgr.check_health("test")
        assert ok, "Broker health check failed"

        # Register session
        reg_result = await mgr.register_session("test", "e2e-test", "/tmp", "e2e test session")
        assert reg_result.get("ok"), f"Registration failed: {reg_result}"

        # Run a task
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


# ── T4: Worker permission callback ───────────────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_worker_permission_callback():
    """Full stack: worker triggers a tool requiring permission.

    Worker task writes a file -> broker POSTs to /permission -> orchestrator
    auto-approves -> worker completes.
    """
    _clear_nesting_guard()

    permission_requests = []

    orch, store, mgr, _, _, _, cleanup = await _setup_full_stack(
        broker_port=18210, http_port=19130,
    )

    # Track and auto-approve all permission requests
    async def tracking_handler(data):
        permission_requests.append(data)
        return {"approved": True, "reason": "test auto-approve"}

    orch.handle_permission_request = tracking_handler

    try:
        # Register a session
        reg = await mgr.register_session("local", "perm-test", "/tmp", "permission test")
        assert reg.get("ok"), f"Registration failed: {reg}"

        # Task that requires a tool permission (Write)
        result = await mgr.run_task(
            "local", "perm-test",
            "Write the text 'PERM_TEST_OK' to the file /tmp/e2e_perm_test.txt. "
            "Then read it back and reply with its contents.",
        )

        print(f"Permission test result: {result.result_text[:200]}")
        print(f"Permission requests received: {len(permission_requests)}")
        for pr in permission_requests:
            print(f"  tool={pr.get('tool_name')}, session={pr.get('session_id')}")

        assert not result.is_error, f"Task failed: {result.result_text}"
        assert "PERM_TEST_OK" in result.result_text, f"Unexpected: {result.result_text[:200]}"
        assert len(permission_requests) > 0, "No permission requests received"

    finally:
        # Clean up test file
        try:
            os.remove("/tmp/e2e_perm_test.txt")
        except FileNotFoundError:
            pass
        await cleanup()


# ── T5: Worker clarification callback ────────────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_worker_clarification_callback():
    """Full stack: worker calls AskUserQuestion -> broker POSTs /clarification
    -> orchestrator answers -> worker completes with answer.
    """
    _clear_nesting_guard()

    clarification_requests = []

    orch, store, mgr, _, _, _, cleanup = await _setup_full_stack(
        broker_port=18211, http_port=19131,
    )

    # Track and auto-answer all clarification requests
    async def tracking_clarification(data):
        clarification_requests.append(data)
        # Answer all questions with "blue"
        answers = {}
        for q in data.get("questions", []):
            answers[q.get("question", "")] = "blue"
        return {"answers": answers}

    orch.handle_clarification_request = tracking_clarification

    try:
        reg = await mgr.register_session("local", "clarify-test", "/tmp", "clarification test")
        assert reg.get("ok"), f"Registration failed: {reg}"

        # Task that forces AskUserQuestion
        result = await mgr.run_task(
            "local", "clarify-test",
            "Use the AskUserQuestion tool to ask the user: 'What is your favorite color?' "
            "with options 'red', 'blue', 'green'. "
            "After getting the answer, reply with: 'Your color is: <answer>'.",
        )

        print(f"Clarification test result: {result.result_text[:200]}")
        print(f"Clarification requests received: {len(clarification_requests)}")

        assert not result.is_error, f"Task failed: {result.result_text}"
        # The worker should have received "blue" as the answer
        assert "blue" in result.result_text.lower(), f"Answer not incorporated: {result.result_text[:200]}"
        assert len(clarification_requests) > 0, "No clarification requests received"

    finally:
        await cleanup()


# ── T6: Plan mode in worker ──────────────────────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_plan_mode_in_worker():
    """Worker enters plan mode, plans, exits, and executes.

    EnterPlanMode/ExitPlanMode are NOT blocked — worker can use them.
    """
    _clear_nesting_guard()

    orch, store, mgr, _, _, _, cleanup = await _setup_full_stack(
        broker_port=18212, http_port=19132,
    )

    # Auto-approve all permissions
    async def auto_approve(data):
        return {"approved": True, "reason": "auto-approve for plan mode test"}

    orch.handle_permission_request = auto_approve

    try:
        reg = await mgr.register_session("local", "plan-test", "/tmp", "plan mode test")
        assert reg.get("ok"), f"Registration failed: {reg}"

        # Multi-step task — worker may use plan mode
        result = await mgr.run_task(
            "local", "plan-test",
            "Create a file /tmp/e2e_plan_test.txt with the content 'step1'. "
            "Then create /tmp/e2e_plan_test2.txt with 'step2'. "
            "Finally, read both files and reply with their combined contents.",
        )

        print(f"Plan mode result: {result.result_text[:300]}")

        assert not result.is_error, f"Task failed: {result.result_text}"
        assert "step1" in result.result_text.lower(), f"step1 missing: {result.result_text[:200]}"
        assert "step2" in result.result_text.lower(), f"step2 missing: {result.result_text[:200]}"

    finally:
        for f in ["/tmp/e2e_plan_test.txt", "/tmp/e2e_plan_test2.txt"]:
            try:
                os.remove(f)
            except FileNotFoundError:
                pass
        await cleanup()


# ── T7: Dynamic server registration ─────────────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_dynamic_server_registration():
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

        # Dynamically add a server
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
