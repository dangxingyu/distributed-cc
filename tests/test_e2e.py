"""End-to-end integration tests with real Claude Code.

These tests actually call Claude and cost money. Run with:
    uv run pytest tests/test_e2e.py -v -s

Skip with: pytest -k "not e2e"
"""

import asyncio
import json
import os
import tempfile
from pathlib import Path

import pytest
import aiohttp
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
        "orchestrator_sessions": dict(daemon_mod.orchestrator_sessions),
        "worker_sessions": dict(daemon_mod.worker_sessions),
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
        daemon_mod.orchestrator_sessions.clear()
        daemon_mod.orchestrator_sessions.update(orig["orchestrator_sessions"])
        daemon_mod.worker_sessions.clear()
        daemon_mod.worker_sessions.update(orig["worker_sessions"])

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
                    "task": "Reply with 'TASK_OK' and call task_complete immediately.",
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
                    "task": "List all files in /tmp and summarize. Call task_complete when done.",
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


# ── Daemon E2E helper ─────────────────────────────────────────────────


async def _run_daemon_task(
    port: int,
    sandbox: str,
    project_id: str,
    task: str,
    max_iterations: int = 5,
    timeout_secs: int = 180,
) -> dict:
    """Spin up a daemon, register a sandbox project, run a task, return final status."""
    restore = _patch_daemon_globals(
        name=f"e2e-{project_id}",
        callback_url="http://127.0.0.1:19999",
    )
    app = _setup_daemon_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()

    base = f"http://127.0.0.1:{port}"
    try:
        async with ClientSession(timeout=ClientTimeout(total=timeout_secs + 30)) as http:
            # Register project
            async with http.post(
                f"{base}/register",
                json={"project_id": project_id, "project_dir": sandbox, "name": project_id},
            ) as resp:
                assert resp.status == 200

            # Start task
            async with http.post(
                f"{base}/task",
                json={"project_id": project_id, "task": task, "max_iterations": max_iterations},
            ) as resp:
                assert resp.status == 200

            # Wait for completion
            for _ in range(timeout_secs // 2):
                await asyncio.sleep(2)
                async with http.get(f"{base}/status?project_id={project_id}") as resp:
                    status = await resp.json()
                    if status.get("status") in ("done", "error", "stuck"):
                        return status
            pytest.fail(f"Task did not complete within {timeout_secs}s")
    finally:
        restore()
        await runner.cleanup()


# ── T7: MCP tools — assign_worker + update_task_list + task_complete ──


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_mcp_assign_worker_and_complete():
    """Orchestrator uses assign_worker to create a file, update_task_list to plan, task_complete to finish."""
    _clear_nesting_guard()

    with tempfile.TemporaryDirectory(prefix="dcc_e2e_mcp_") as sandbox:
        status = await _run_daemon_task(
            port=18310,
            sandbox=sandbox,
            project_id="mcp-worker",
            task=(
                "Do the following steps in order:\n"
                "1. Call update_task_list with a short plan (2-3 items)\n"
                "2. Call assign_worker to create a file called hello.py "
                "containing exactly: print('Hello MCP')\n"
                "3. Call task_complete with a summary"
            ),
            max_iterations=5,
            timeout_secs=180,
        )

        print(f"MCP worker test status: {status}")
        assert status["status"] == "done", (
            f"Expected done, got {status['status']}: {status.get('error', '')}"
        )

        # Verify worker created the file
        hello = Path(sandbox) / "hello.py"
        assert hello.exists(), f"Worker should have created hello.py. Files: {os.listdir(sandbox)}"
        content = hello.read_text()
        assert "Hello MCP" in content, f"Unexpected content: {content}"

        # Verify task list was created
        task_list = Path(sandbox) / "task_list.md"
        assert task_list.exists(), (
            f"Orchestrator should have called update_task_list. Files: {os.listdir(sandbox)}"
        )
        print(f"task_list.md content:\n{task_list.read_text()}")


# ── T8: MCP tools — ask_user with interrupt ───────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_mcp_ask_user_with_interrupt():
    """Orchestrator uses ask_user, daemon blocks, interrupt provides the answer."""
    _clear_nesting_guard()

    with tempfile.TemporaryDirectory(prefix="dcc_e2e_ask_") as sandbox:
        restore = _patch_daemon_globals(
            name="e2e-ask",
            callback_url="http://127.0.0.1:19999",
        )
        app = _setup_daemon_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 18311)
        await site.start()

        base = "http://127.0.0.1:18311"
        try:
            async with ClientSession(timeout=ClientTimeout(total=210)) as http:
                # Register
                await http.post(
                    f"{base}/register",
                    json={"project_id": "ask-test", "project_dir": sandbox, "name": "ask-test"},
                )

                # Start task that requires user input
                await http.post(
                    f"{base}/task",
                    json={
                        "project_id": "ask-test",
                        "task": (
                            "You need to create a config file. "
                            "First, call ask_user to ask: 'What should the project name be?' "
                            "Then assign a worker to create config.json containing "
                            '{"name": "<the name the user gave>"}. '
                            "Finally call task_complete."
                        ),
                        "max_iterations": 5,
                    },
                )

                # Wait for stuck status (ask_user blocks)
                got_stuck = False
                for _ in range(60):
                    await asyncio.sleep(2)
                    async with http.get(f"{base}/status?project_id=ask-test") as resp:
                        status = await resp.json()
                        st = status.get("status", "")
                        if st == "stuck":
                            got_stuck = True
                            print(f"Orchestrator asked: {status.get('summary', '')}")
                            break
                        if st in ("done", "error"):
                            break

                if got_stuck:
                    # Provide the answer via interrupt
                    async with http.post(
                        f"{base}/interrupt",
                        json={"project_id": "ask-test", "message": "my-awesome-project"},
                    ) as resp:
                        data = await resp.json()
                        assert data["ok"] is True

                # Wait for final completion
                for _ in range(60):
                    await asyncio.sleep(2)
                    async with http.get(f"{base}/status?project_id=ask-test") as resp:
                        status = await resp.json()
                        if status.get("status") in ("done", "error"):
                            break
                else:
                    pytest.fail("Task did not complete after interrupt")

                print(f"ask_user test final status: {status}")
                assert status["status"] == "done", (
                    f"Expected done, got {status['status']}: {status.get('error', '')}"
                )

                # Verify config.json was created
                config_path = Path(sandbox) / "config.json"
                if config_path.exists():
                    config_content = config_path.read_text()
                    print(f"config.json content: {config_content}")

        finally:
            restore()
            await runner.cleanup()


# ── T9: Full stack — WebSocket → Router → Daemon → Claude → WS back ──


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_full_stack_web_to_worker():
    """Full round trip: WS message → Router → Daemon → Orchestrator → Worker → progress events → WS.

    Verifies:
      - User message via WebSocket reaches the daemon and starts a task
      - Orchestrator assigns worker, worker creates a file
      - Progress events (iteration, log, reply, channel_status, done) flow back via WS
      - Chat messages show orchestrator↔worker exchange
      - File is actually created in the sandbox
    """
    _clear_nesting_guard()

    DAEMON_PORT = 18320
    CALLBACK_PORT = 18321
    WEB_PORT = 18322

    with tempfile.TemporaryDirectory(prefix="dcc_e2e_full_") as sandbox:
        # ── 1. Start daemon ──────────────────────────────────────────
        restore = _patch_daemon_globals(
            name="e2e-full",
            callback_url=f"http://127.0.0.1:{CALLBACK_PORT}",
        )
        daemon_app = _setup_daemon_app()
        daemon_runner = web.AppRunner(daemon_app)
        await daemon_runner.setup()
        await web.TCPSite(daemon_runner, "127.0.0.1", DAEMON_PORT).start()

        # ── 2. Set up Router + Store ─────────────────────────────────
        from src.router import Router, RemoteOrchestrator
        from src.store import Store
        from src.web import WebChat

        store = Store(tempfile.mkdtemp())
        await store.init()

        router = Router()
        await router.init()
        router._orchestrators["full-test"] = RemoteOrchestrator(
            project_id="full-test",
            name="full-test",
            broker_port=DAEMON_PORT,
            project_dir=sandbox,
        )

        # ── 3. Start callback server (daemon → router) ──────────────
        callback_app = web.Application()

        async def handle_callback_progress(request):
            data = await request.json()
            await router.ingest_progress_event(
                data.get("project_id", ""), data.get("event", {}), source="callback"
            )
            return web.json_response({"ok": True})

        callback_app.router.add_post("/progress", handle_callback_progress)
        callback_runner = web.AppRunner(callback_app)
        await callback_runner.setup()
        await web.TCPSite(callback_runner, "127.0.0.1", CALLBACK_PORT).start()

        # ── 4. Start WebChat ─────────────────────────────────────────
        web_chat = WebChat(router=router, store=store)

        web_app = web.Application()
        web_app.router.add_get("/", web_chat._handle_index)
        web_app.router.add_get("/api/history", web_chat._handle_history)
        web_app.router.add_get("/api/channels", web_chat._handle_channels_list)
        web_app.router.add_post("/api/channels", web_chat._handle_channels_create)
        web_app.router.add_delete("/api/channels/{id}", web_chat._handle_channels_delete)
        web_app.router.add_get("/api/channels/{id}/members", web_chat._handle_channels_members)
        web_app.router.add_get("/api/logs", web_chat._handle_logs)
        web_app.router.add_get("/api/projects", web_chat._handle_projects_list)
        web_app.router.add_get("/ws", web_chat._handle_ws)

        web_runner = web.AppRunner(web_app)
        await web_runner.setup()
        await web.TCPSite(web_runner, "127.0.0.1", WEB_PORT).start()

        # ── 5. Drive test via WebSocket ──────────────────────────────
        try:
            async with ClientSession() as http:
                # Create channel connected to our project
                async with http.post(
                    f"http://127.0.0.1:{WEB_PORT}/api/channels",
                    json={"name": "e2e-full", "project_id": "full-test"},
                ) as resp:
                    assert resp.status == 200
                    channel_data = await resp.json()
                    channel_id = channel_data["id"]

                # Connect WebSocket
                async with http.ws_connect(f"http://127.0.0.1:{WEB_PORT}/ws") as ws:
                    # Switch to channel
                    await ws.send_json({"type": "switch_channel", "channel_id": channel_id})
                    switch_msg = await asyncio.wait_for(ws.receive_json(), timeout=5)
                    assert switch_msg["type"] == "channel_switched"
                    assert switch_msg["project_id"] == "full-test"
                    print(f"Channel switched: {switch_msg}")

                    # Send task message
                    await ws.send_json({
                        "type": "message",
                        "text": (
                            "Create a file called greeting.txt containing exactly "
                            "'Hello from full E2E test'. Then call task_complete."
                        ),
                    })

                    # Collect WS events until done or timeout
                    ws_events = []
                    event_types = set()
                    done = False
                    error_msg = ""

                    try:
                        while not done:
                            raw = await asyncio.wait_for(ws.receive(), timeout=240)
                            if raw.type in (
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.ERROR,
                                aiohttp.WSMsgType.CLOSING,
                            ):
                                break
                            if raw.type != aiohttp.WSMsgType.TEXT:
                                continue

                            data = json.loads(raw.data)
                            ws_events.append(data)
                            event_types.add(data["type"])

                            # Print events for debugging
                            if data["type"] == "reply":
                                print(f"  [reply] {data['text'][:120]}")
                            elif data["type"] == "progress":
                                status = data.get("status", "")
                                print(f"  [progress] status={status} iter={data.get('iteration')} data={str(data.get('data',''))[:80]}")
                            elif data["type"] == "log":
                                print(f"  [log] {data['text'][:100]}")
                            elif data["type"] == "task_list":
                                print(f"  [task_list] {data['data'][:100]}")

                            if data["type"] == "progress" and data.get("status") == "done":
                                done = True
                            if data["type"] == "progress" and data.get("status") == "error":
                                error_msg = data.get("data", "")
                                done = True
                    except asyncio.TimeoutError:
                        pass

                    # ── Assertions ────────────────────────────────────
                    print(f"\nCollected {len(ws_events)} events, types: {sorted(event_types)}")

                    # Must have progress events (iteration updates + done)
                    assert "progress" in event_types, (
                        f"No progress events received. Got types: {event_types}"
                    )

                    # Must have completed successfully
                    done_events = [
                        e for e in ws_events
                        if e["type"] == "progress" and e.get("status") == "done"
                    ]
                    assert len(done_events) > 0, (
                        f"Task did not complete. Error: {error_msg}. "
                        f"Event types: {sorted(event_types)}"
                    )

                    # Must have reply messages (orchestrator↔worker exchange in chat)
                    replies = [e for e in ws_events if e["type"] == "reply"]
                    assert len(replies) > 0, (
                        f"Expected chat replies (orchestrator↔worker). Got 0. "
                        f"Event types: {sorted(event_types)}"
                    )

                    # At least one reply should show orchestrator→worker assignment
                    orch_to_worker = [
                        r for r in replies
                        if "@orchestrator" in r.get("text", "") and "@worker" in r.get("text", "")
                    ]
                    assert len(orch_to_worker) > 0, (
                        f"No orchestrator→worker assignment in chat. "
                        f"Replies: {[r['text'][:80] for r in replies]}"
                    )

                    # Must have log entries (monitor panel content)
                    logs = [e for e in ws_events if e["type"] == "log"]
                    assert len(logs) > 0, "Expected monitor log entries"

                    # Must have channel_status broadcasts (sidebar updates)
                    ch_status = [e for e in ws_events if e["type"] == "channel_status"]
                    assert len(ch_status) > 0, "Expected channel_status broadcasts"

                    # Verify file was created by the worker
                    greeting = Path(sandbox) / "greeting.txt"
                    assert greeting.exists(), (
                        f"Worker should have created greeting.txt. "
                        f"Files in sandbox: {os.listdir(sandbox)}"
                    )
                    content = greeting.read_text()
                    assert "Hello from full E2E test" in content, (
                        f"Unexpected file content: {content}"
                    )

                    # Verify history was persisted
                    history = await store.get_recent_messages(channel_id)
                    user_msgs = [m for m in history if m["role"] == "user"]
                    assistant_msgs = [m for m in history if m["role"] == "assistant"]
                    assert len(user_msgs) >= 1, "User message not persisted"
                    assert len(assistant_msgs) >= 1, "Assistant messages not persisted"

                    print(f"\nFull stack E2E passed!")
                    print(f"  Events: {len(ws_events)}")
                    print(f"  Replies: {len(replies)}")
                    print(f"  Logs: {len(logs)}")
                    print(f"  File created: {content.strip()}")
                    print(f"  History: {len(user_msgs)} user, {len(assistant_msgs)} assistant")

        finally:
            restore()
            await web_runner.cleanup()
            await callback_runner.cleanup()
            await daemon_runner.cleanup()
            await router.close()
            await store.close()
