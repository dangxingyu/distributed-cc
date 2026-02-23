"""End-to-end integration tests with real Claude Code.

These tests actually call Claude and cost money. Run with:
    uv run pytest tests/test_e2e.py -v -s

Skip with: pytest -k "not e2e"
"""

import asyncio
import json
import os
import re
import tempfile
import uuid
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


def _unique_project_id(prefix: str) -> str:
    """Generate a collision-resistant project_id for test isolation."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _base_url(port: int) -> str:
    return f"http://127.0.0.1:{port}"


async def _start_test_server(app: web.Application) -> tuple[web.AppRunner, int]:
    """Start an aiohttp test server on an ephemeral localhost port."""
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    sockets = site._server.sockets if site._server else []
    assert sockets, "Test server did not expose a bound socket"
    port = int(sockets[0].getsockname()[1])
    return runner, port


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

    state_dir_ctx = tempfile.TemporaryDirectory(prefix="dcc_e2e_state_")
    state_dir = Path(state_dir_ctx.name)
    state_dir.mkdir(parents=True, exist_ok=True)

    orig = {
        "DAEMON_NAME": daemon_mod.DAEMON_NAME,
        "CALLBACK_URL": daemon_mod.CALLBACK_URL,
        "STATE_DIR": daemon_mod.STATE_DIR,
        "projects": dict(daemon_mod.projects),
        "task_states": dict(daemon_mod.task_states),
        "running_tasks": dict(daemon_mod.running_tasks),
        "interrupt_queues": dict(daemon_mod.interrupt_queues),
        "cancel_events": dict(daemon_mod.cancel_events),
        "sse_subscribers": dict(daemon_mod.sse_subscribers),
        "orchestrator_sessions": dict(daemon_mod.orchestrator_sessions),
        "worker_sessions": dict(daemon_mod.worker_sessions),
        "orchestrator_prompt_hashes": dict(daemon_mod.orchestrator_prompt_hashes),
        "worker_prompt_hashes": dict(daemon_mod.worker_prompt_hashes),
    }
    daemon_mod.DAEMON_NAME = name
    daemon_mod.CALLBACK_URL = callback_url
    daemon_mod.STATE_DIR = state_dir
    daemon_mod.projects.clear()
    daemon_mod.task_states.clear()
    daemon_mod.running_tasks.clear()
    daemon_mod.interrupt_queues.clear()
    daemon_mod.cancel_events.clear()
    daemon_mod.sse_subscribers.clear()
    daemon_mod.orchestrator_sessions.clear()
    daemon_mod.worker_sessions.clear()
    daemon_mod.orchestrator_prompt_hashes.clear()
    daemon_mod.worker_prompt_hashes.clear()

    def restore():
        for task in list(daemon_mod.running_tasks.values()):
            if not task.done():
                task.cancel()
        daemon_mod.DAEMON_NAME = orig["DAEMON_NAME"]
        daemon_mod.CALLBACK_URL = orig["CALLBACK_URL"]
        daemon_mod.STATE_DIR = orig["STATE_DIR"]
        daemon_mod.projects.clear()
        daemon_mod.projects.update(orig["projects"])
        daemon_mod.task_states.clear()
        daemon_mod.task_states.update(orig["task_states"])
        daemon_mod.running_tasks.clear()
        daemon_mod.running_tasks.update(orig["running_tasks"])
        daemon_mod.interrupt_queues.clear()
        daemon_mod.interrupt_queues.update(orig["interrupt_queues"])
        daemon_mod.cancel_events.clear()
        daemon_mod.cancel_events.update(orig["cancel_events"])
        daemon_mod.sse_subscribers.clear()
        daemon_mod.sse_subscribers.update(orig["sse_subscribers"])
        daemon_mod.orchestrator_sessions.clear()
        daemon_mod.orchestrator_sessions.update(orig["orchestrator_sessions"])
        daemon_mod.worker_sessions.clear()
        daemon_mod.worker_sessions.update(orig["worker_sessions"])
        daemon_mod.orchestrator_prompt_hashes.clear()
        daemon_mod.orchestrator_prompt_hashes.update(orig["orchestrator_prompt_hashes"])
        daemon_mod.worker_prompt_hashes.clear()
        daemon_mod.worker_prompt_hashes.update(orig["worker_prompt_hashes"])
        state_dir_ctx.cleanup()

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
    runner, port = await _start_test_server(app)
    base = _base_url(port)

    try:
        async with ClientSession() as http:
            async with http.get(f"{base}/health") as resp:
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

    project_id = _unique_project_id("e2e-task")
    restore = _patch_daemon_globals(
        name="e2e-test",
        callback_url="http://127.0.0.1:19999",  # dummy — won't trigger escalation
    )

    app = _setup_daemon_app()
    runner, port = await _start_test_server(app)
    base = _base_url(port)

    try:
        async with ClientSession() as http:
            # Register project
            async with http.post(
                f"{base}/register",
                json={
                    "project_id": project_id,
                    "project_dir": "/tmp",
                    "name": "E2E Test",
                },
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["ok"] is True

            # Start task
            async with http.post(
                f"{base}/task",
                json={
                    "project_id": project_id,
                    "task": "Reply with 'TASK_OK' and call task_complete immediately.",
                },
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["ok"] is True
                assert data["status"] == "started"
                assert data["project_id"] == project_id

            # Verify /task default max_iterations=0 is applied when omitted.
            async with http.get(f"{base}/status?project_id={project_id}") as resp:
                status = await resp.json()
                assert status["max_iterations"] == 0

            # Wait for completion
            for _ in range(60):
                await asyncio.sleep(2)
                async with http.get(
                    f"{base}/status?project_id={project_id}"
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

    project_id = _unique_project_id("e2e-int")
    restore = _patch_daemon_globals(
        name="e2e-int",
        callback_url="http://127.0.0.1:19999",
    )

    app = _setup_daemon_app()
    runner, port = await _start_test_server(app)
    base = _base_url(port)

    try:
        async with ClientSession() as http:
            # Register
            async with http.post(
                f"{base}/register",
                json={"project_id": project_id, "project_dir": "/tmp"},
            ) as resp:
                assert resp.status == 200

            # Start a longer task
            async with http.post(
                f"{base}/task",
                json={
                    "project_id": project_id,
                    "task": "List all files in /tmp and summarize. Call task_complete when done.",
                    "max_iterations": 5,
                },
            ) as resp:
                assert resp.status == 200

            # Wait a moment then interrupt
            await asyncio.sleep(1)
            async with http.post(
                f"{base}/interrupt",
                json={
                    "project_id": project_id,
                    "message": "Also check /tmp/test if it exists",
                    "urgency": "normal",
                },
            ) as resp:
                data = await resp.json()
                assert data["ok"] is True
                assert data["queued"] is True
                assert data["urgency"] == "normal"

            # Urgent interrupts should preserve urgency metadata.
            async with http.post(
                f"{base}/interrupt",
                json={
                    "project_id": project_id,
                    "message": "Urgent: prioritize /tmp/test now",
                    "urgency": "urgent",
                },
            ) as resp:
                data = await resp.json()
                assert data["ok"] is True
                assert data["queued"] is True
                assert data["urgency"] == "urgent"

            # Wait for completion
            status = {}
            for _ in range(60):
                await asyncio.sleep(2)
                async with http.get(
                    f"{base}/status?project_id={project_id}"
                ) as resp:
                    status = await resp.json()
                    if status.get("status") in ("done", "error", "stuck"):
                        break
            else:
                pytest.fail("Interrupt test did not complete within timeout")

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
    Some SDK/model combinations may satisfy the task without invoking callback
    events; filename correctness is the hard requirement.
    """
    _clear_nesting_guard()

    with tempfile.TemporaryDirectory(prefix="dcc_e2e_tools_") as sandbox:
        expected_name = f"tool_probe_{uuid.uuid4().hex[:8]}.py"
        (Path(sandbox) / expected_name).write_text("print('probe')\n")

        tool_calls = []

        async def track_tools(tool_name, input_data, context=None):
            tool_calls.append(tool_name)
            return PermissionResultAllow()

        done_event = asyncio.Event()
        options = ClaudeAgentOptions(
            model="haiku",
            cwd=sandbox,
            can_use_tool=track_tools,
            allowed_tools=["Read", "Glob"],
        )

        got_result = False
        result_text = ""
        async for message in query(
            prompt=_prompt_stream(
                "There is exactly one .py file in the current directory. "
                "Use Glob to find it, then reply with that filename only.",
                done_event,
            ),
            options=options,
        ):
            if isinstance(message, ResultMessage):
                got_result = True
                result_text = message.result or ""
                done_event.set()
                print(f"can_use_tool result: {result_text[:200]}")
                print(f"Tools used: {tool_calls}")

        assert got_result, "Never received a ResultMessage"
        assert expected_name in result_text, (
            f"Model did not identify expected file {expected_name!r}: {result_text!r}"
        )
        if tool_calls:
            assert any("glob" in str(name).lower() for name in tool_calls), (
                f"Expected Glob tool usage. Saw: {tool_calls}"
            )


# ── Daemon E2E helper ─────────────────────────────────────────────────


async def _run_daemon_task(
    sandbox: str,
    project_id: str,
    task: str,
    max_iterations: int | None = None,
    timeout_secs: int = 180,
) -> dict:
    """Spin up a daemon, register a sandbox project, run a task, return final status."""
    restore = _patch_daemon_globals(
        name=f"e2e-{project_id}",
        callback_url="http://127.0.0.1:19999",
    )
    app = _setup_daemon_app()
    runner, port = await _start_test_server(app)

    base = _base_url(port)
    try:
        async with ClientSession(timeout=ClientTimeout(total=timeout_secs + 30)) as http:
            # Register project
            async with http.post(
                f"{base}/register",
                json={"project_id": project_id, "project_dir": sandbox, "name": project_id},
            ) as resp:
                assert resp.status == 200

            # Start task
            task_payload = {"project_id": project_id, "task": task}
            if max_iterations is not None:
                task_payload["max_iterations"] = max_iterations
            async with http.post(
                f"{base}/task",
                json=task_payload,
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


async def _start_task_and_wait(
    http: ClientSession,
    base: str,
    project_id: str,
    task: str,
    max_iterations: int = 5,
    timeout_secs: int = 180,
) -> dict:
    """Start a daemon task and wait for terminal status."""
    for _ in range(60):
        async with http.post(
            f"{base}/task",
            json={
                "project_id": project_id,
                "task": task,
                "max_iterations": max_iterations,
            },
        ) as resp:
            if resp.status == 200:
                break
            if resp.status != 409:
                body = await resp.text()
                pytest.fail(f"Unexpected /task status {resp.status}: {body}")
        await asyncio.sleep(1)
    else:
        pytest.fail("Could not start task; previous task stayed busy too long")

    status = {}
    for _ in range(timeout_secs // 2):
        await asyncio.sleep(2)
        async with http.get(f"{base}/status?project_id={project_id}") as resp:
            status = await resp.json()
            if status.get("status") in ("done", "error", "stuck"):
                return status
    pytest.fail(f"Task did not complete within {timeout_secs}s")


def _extract_json_object(text: str) -> dict:
    """Extract the first JSON object from free-form model output."""
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if "\n" in raw:
            raw = raw.split("\n", 1)[1]
    match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
    if not match:
        raise ValueError(f"No JSON object found in evaluator output: {text[:300]}")
    return json.loads(match.group(0))


async def _claude_eval_yes_no_score(
    *,
    rubric: str,
    evidence: str,
    min_score: int = 8,
) -> dict:
    """Run a second-pass Claude evaluator and assert yes/no + score contract."""
    _clear_nesting_guard()

    prompt = (
        "You are a strict software test evaluator.\n"
        "Evaluate whether the evidence satisfies the rubric.\n"
        "Return ONLY one JSON object (no markdown, no extra text):\n"
        '{"pass":"yes|no","score":0,"reason":"short reason"}\n'
        "Rules:\n"
        "- score is an integer from 0 to 10\n"
        "- pass must be yes only if evidence clearly meets rubric\n\n"
        "- use only provided evidence; do not infer hidden actions\n\n"
        f"RUBRIC:\n{rubric}\n\n"
        f"EVIDENCE:\n{evidence}\n"
    )

    options = ClaudeAgentOptions(model="haiku", cwd="/tmp")
    result_text = ""
    async for message in query(prompt=prompt, options=options):
        if isinstance(message, ResultMessage):
            result_text = message.result or ""

    parsed = _extract_json_object(result_text)
    decision = str(parsed.get("pass", "")).strip().lower()
    score = int(parsed.get("score", -1))
    assert decision in ("yes", "no"), parsed
    assert 0 <= score <= 10, parsed
    assert decision == "yes", parsed
    assert score >= min_score, parsed
    return parsed


# ── T7: MCP tools — assign_worker + update_task_list + task_complete ──


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_mcp_assign_worker_and_complete():
    """Orchestrator uses assign_worker to create a file, update_task_list to plan, task_complete to finish."""
    _clear_nesting_guard()

    with tempfile.TemporaryDirectory(prefix="dcc_e2e_mcp_") as sandbox:
        project_id = _unique_project_id("mcp-worker")
        status = await _run_daemon_task(
            sandbox=sandbox,
            project_id=project_id,
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
        project_id = _unique_project_id("ask-test")
        restore = _patch_daemon_globals(
            name="e2e-ask",
            callback_url="http://127.0.0.1:19999",
        )
        app = _setup_daemon_app()
        runner, port = await _start_test_server(app)

        base = _base_url(port)
        try:
            async with ClientSession(timeout=ClientTimeout(total=210)) as http:
                # Register
                async with http.post(
                    f"{base}/register",
                    json={"project_id": project_id, "project_dir": sandbox, "name": project_id},
                ) as resp:
                    assert resp.status == 200

                # Start task that requires user input
                async with http.post(
                    f"{base}/task",
                    json={
                        "project_id": project_id,
                        "task": (
                            "You need to create a config file. "
                            "First, call ask_user to ask: 'What should the project name be?' "
                            "Then assign a worker to create config.json containing "
                            '{"name": "<the name the user gave>"}. '
                            "Finally call task_complete."
                        ),
                        "max_iterations": 5,
                    },
                ) as resp:
                    assert resp.status == 200

                # Wait for stuck status (ask_user blocks)
                got_stuck = False
                status = {}
                for _ in range(60):
                    await asyncio.sleep(2)
                    async with http.get(f"{base}/status?project_id={project_id}") as resp:
                        status = await resp.json()
                        st = status.get("status", "")
                        if st == "stuck":
                            got_stuck = True
                            print(f"Orchestrator asked: {status.get('summary', '')}")
                            break
                        if st in ("done", "error"):
                            break

                assert got_stuck, (
                    f"Expected ask_user to block in 'stuck' state before interrupt. Last status: {status}"
                )
                assert str(status.get("summary", "")).strip(), "Stuck status should include the question summary"

                # Provide the answer via interrupt
                async with http.post(
                    f"{base}/interrupt",
                    json={"project_id": project_id, "message": "my-awesome-project"},
                ) as resp:
                    data = await resp.json()
                    assert data["ok"] is True

                # Wait for final completion
                for _ in range(60):
                    await asyncio.sleep(2)
                    async with http.get(f"{base}/status?project_id={project_id}") as resp:
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
                assert config_path.exists(), (
                    f"Expected worker to create config.json after ask_user. Files: {os.listdir(sandbox)}"
                )
                config_content = config_path.read_text()
                print(f"config.json content: {config_content}")
                parsed = json.loads(config_content)
                assert parsed.get("name") == "my-awesome-project", (
                    f"Unexpected config content: {parsed}"
                )

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

    project_id = _unique_project_id("full-test")

    with tempfile.TemporaryDirectory(prefix="dcc_e2e_full_") as sandbox:
        # ── 1. Set up Router + Store ─────────────────────────────────
        from src.router import Router, RemoteOrchestrator
        from src.store import Store
        from src.web import WebChat

        store = Store(tempfile.mkdtemp())
        await store.init()

        router = Router()
        await router.init()

        # ── 2. Start callback server (daemon → router) ──────────────
        callback_app = web.Application()

        async def handle_callback_progress(request):
            data = await request.json()
            await router.ingest_progress_event(
                data.get("project_id", ""), data.get("event", {}), source="callback"
            )
            return web.json_response({"ok": True})

        callback_app.router.add_post("/progress", handle_callback_progress)
        callback_runner, callback_port = await _start_test_server(callback_app)
        callback_url = f"{_base_url(callback_port)}/progress"

        # ── 3. Start daemon ──────────────────────────────────────────
        restore = _patch_daemon_globals(
            name="e2e-full",
            callback_url=callback_url,
        )
        daemon_app = _setup_daemon_app()
        daemon_runner, daemon_port = await _start_test_server(daemon_app)

        # Register router's remote orchestrator after daemon port is known.
        router._orchestrators[project_id] = RemoteOrchestrator(
            project_id=project_id,
            name=project_id,
            broker_port=daemon_port,
            project_dir=sandbox,
        )

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

        web_runner, web_port = await _start_test_server(web_app)
        web_base = _base_url(web_port)

        # ── 5. Drive test via WebSocket ──────────────────────────────
        try:
            async with ClientSession() as http:
                # Create channel connected to our project
                async with http.post(
                    f"{web_base}/api/channels",
                    json={"name": "e2e-full", "project_id": project_id},
                ) as resp:
                    assert resp.status == 200
                    channel_data = await resp.json()
                    channel_id = channel_data["id"]

                # Connect WebSocket
                async with http.ws_connect(f"{web_base}/ws") as ws:
                    # Switch to channel
                    await ws.send_json({"type": "switch_channel", "channel_id": channel_id})
                    switch_msg = await asyncio.wait_for(ws.receive_json(), timeout=5)
                    assert switch_msg["type"] == "channel_switched"
                    assert switch_msg["project_id"] == project_id
                    print(f"Channel switched: {switch_msg}")

                    # Send task message
                    await ws.send_json({
                        "type": "message",
                        "text": (
                            "You MUST delegate execution to worker via assign_worker exactly once. "
                            "Do not edit files directly. Ask worker to create a file called "
                            "greeting.txt containing exactly 'Hello from full E2E test'. "
                            "Then call task_complete."
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


# ── T10: Role memory injection + hot reload across sessions ───────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_role_memory_hot_reload_resets_sessions():
    """Role files in .claude/roles are injected and take effect after live updates."""
    _clear_nesting_guard()

    project_id = _unique_project_id("role-reload")
    with tempfile.TemporaryDirectory(prefix="dcc_e2e_roles_") as sandbox:
        restore = _patch_daemon_globals(
            name="e2e-roles",
            callback_url="http://127.0.0.1:19999",
        )
        app = _setup_daemon_app()
        runner, port = await _start_test_server(app)
        base = _base_url(port)

        try:
            async with ClientSession(timeout=ClientTimeout(total=300)) as http:
                async with http.post(
                    f"{base}/register",
                    json={"project_id": project_id, "project_dir": sandbox, "name": project_id},
                ) as resp:
                    assert resp.status == 200

                role_dir = Path(sandbox) / ".claude" / "roles"
                role_dir.mkdir(parents=True, exist_ok=True)
                orchestrator_role = role_dir / "orchestrator.md"
                worker_role = role_dir / "worker.md"
                report_path = Path(sandbox) / ".reports" / "iteration-1.md"

                orchestrator_role.write_text(
                    "CRITICAL TEST OVERRIDE:\n"
                    "- When you call task_complete(summary), include exact token:\n"
                    "  ROLE_ORCH_SENTINEL_A: meow~\n"
                )
                worker_role.write_text(
                    "CRITICAL TEST OVERRIDE:\n"
                    "- In every submit_report(report), include exact token:\n"
                    "  ROLE_WORKER_SENTINEL_A: meow~\n"
                )

                status1 = await _start_task_and_wait(
                    http=http,
                    base=base,
                    project_id=project_id,
                    task=(
                        "Follow exactly:\n"
                        "1) You MUST call assign_worker exactly once (do not edit files directly).\n"
                        "2) Ask worker to create role_probe_a.txt with content A.\n"
                        "3) Call task_complete with a concise summary.\n"
                    ),
                    max_iterations=4,
                    timeout_secs=180,
                )
                assert status1["status"] == "done", (
                    f"Expected done, got {status1['status']}: {status1.get('error', '')}"
                )
                assert "ROLE_ORCH_SENTINEL_A: meow~" in str(status1.get("summary", "")), status1
                assert report_path.exists(), "Expected first worker report file"
                report1 = report_path.read_text()
                assert "ROLE_WORKER_SENTINEL_A: meow~" in report1, report1
                assert (Path(sandbox) / "role_probe_a.txt").exists(), "Worker should create role_probe_a.txt"

                orchestrator_role.write_text(
                    "CRITICAL TEST OVERRIDE:\n"
                    "- When you call task_complete(summary), include exact token:\n"
                    "  ROLE_ORCH_SENTINEL_B: meow~\n"
                )
                worker_role.write_text(
                    "CRITICAL TEST OVERRIDE:\n"
                    "- In every submit_report(report), include exact token:\n"
                    "  ROLE_WORKER_SENTINEL_B: meow~\n"
                )

                status2 = await _start_task_and_wait(
                    http=http,
                    base=base,
                    project_id=project_id,
                    task=(
                        "Follow exactly:\n"
                        "1) You MUST call assign_worker exactly once (do not edit files directly).\n"
                        "2) Ask worker to create role_probe_b.txt with content B.\n"
                        "3) Call task_complete with a concise summary.\n"
                    ),
                    max_iterations=4,
                    timeout_secs=180,
                )
                assert status2["status"] == "done", (
                    f"Expected done, got {status2['status']}: {status2.get('error', '')}"
                )
                assert "ROLE_ORCH_SENTINEL_B: meow~" in str(status2.get("summary", "")), status2
                report2 = report_path.read_text()
                assert "ROLE_WORKER_SENTINEL_B: meow~" in report2, report2
                assert (Path(sandbox) / "role_probe_b.txt").exists(), "Worker should create role_probe_b.txt"
        finally:
            restore()
            await runner.cleanup()


# ── T11: MCP update_worker_config hot reload ───────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_update_worker_config_tool_hot_reload():
    """update_worker_config should affect subsequent worker turns immediately."""
    _clear_nesting_guard()
    import tools.orchestrator_daemon as daemon_mod

    project_id = _unique_project_id("worker-cfg")
    with tempfile.TemporaryDirectory(prefix="dcc_e2e_worker_cfg_") as sandbox:
        restore = _patch_daemon_globals(
            name="e2e-worker-cfg",
            callback_url="http://127.0.0.1:19999",
        )
        app = _setup_daemon_app()
        runner, port = await _start_test_server(app)
        base = _base_url(port)

        try:
            async with ClientSession(timeout=ClientTimeout(total=300)) as http:
                async with http.post(
                    f"{base}/register",
                    json={"project_id": project_id, "project_dir": sandbox, "name": project_id},
                ) as resp:
                    assert resp.status == 200

                report_path = Path(sandbox) / ".reports" / "iteration-1.md"
                role_path = Path(sandbox) / ".claude" / "roles" / "worker.md"
                state_path = daemon_mod.STATE_DIR / f"{project_id}.json"

                status1 = await _start_task_and_wait(
                    http=http,
                    base=base,
                    project_id=project_id,
                    task=(
                        "Follow exactly:\n"
                        "1) Call update_worker_config with EXACT content:\n"
                        "CFG_VERSION=A\n"
                        "TOOL_CFG_SENTINEL_A: meow~\n"
                        "2) Call assign_worker exactly once (no direct edits) to create "
                        "worker_cfg_a.txt containing A.\n"
                        "3) Call task_complete.\n"
                    ),
                    max_iterations=4,
                    timeout_secs=180,
                )
                assert status1["status"] == "done", status1
                assert report_path.exists(), "Expected first worker report file"
                report1 = report_path.read_text()
                assert (Path(sandbox) / "worker_cfg_a.txt").exists()
                assert role_path.exists(), f"Expected worker role file at {role_path}"
                role1 = role_path.read_text()
                assert "CFG_VERSION=A" in role1, role1
                assert "TOOL_CFG_SENTINEL_A: meow~" in role1, role1
                assert state_path.exists(), f"Expected persisted state file at {state_path}"
                state1 = json.loads(state_path.read_text())
                worker_hash_1 = str(state1.get("worker_prompt_hash", ""))
                worker_sid_1 = str(state1.get("worker_session_id", ""))
                assert len(worker_hash_1) == 64, state1
                assert worker_sid_1, state1

                status2 = await _start_task_and_wait(
                    http=http,
                    base=base,
                    project_id=project_id,
                    task=(
                        "Follow exactly:\n"
                        "1) Call update_worker_config with EXACT content:\n"
                        "CFG_VERSION=B\n"
                        "TOOL_CFG_SENTINEL_B: meow~\n"
                        "2) Call assign_worker exactly once (no direct edits) to create "
                        "worker_cfg_b.txt containing B.\n"
                        "3) Call task_complete.\n"
                    ),
                    max_iterations=4,
                    timeout_secs=180,
                )
                assert status2["status"] == "done", status2
                report2 = report_path.read_text()
                assert (Path(sandbox) / "worker_cfg_b.txt").exists()
                role2 = role_path.read_text()
                assert "CFG_VERSION=B" in role2, role2
                assert "TOOL_CFG_SENTINEL_B: meow~" in role2, role2
                assert "CFG_VERSION=A" not in role2, role2
                assert "TOOL_CFG_SENTINEL_A: meow~" not in role2, role2
                state2 = json.loads(state_path.read_text())
                worker_hash_2 = str(state2.get("worker_prompt_hash", ""))
                worker_sid_2 = str(state2.get("worker_session_id", ""))
                assert len(worker_hash_2) == 64, state2
                assert worker_sid_2, state2
                assert worker_hash_1 != worker_hash_2, (worker_hash_1, worker_hash_2)
                assert worker_sid_1 != worker_sid_2, (worker_sid_1, worker_sid_2)

                eval_result = await _claude_eval_yes_no_score(
                    rubric=(
                        "Check whether worker config hot reload behavior is correct:\n"
                        "1) Worker config file is updated to A on first run and B on second run.\n"
                        "2) State worker_prompt_hash changes across runs.\n"
                        "3) Worker session id changes across runs (fresh session after config change).\n"
                        "4) Both runs completed successfully."
                    ),
                    evidence=(
                        f"status1_done={status1.get('status') == 'done'}\n"
                        f"status2_done={status2.get('status') == 'done'}\n"
                        f"role1_has_A={'CFG_VERSION=A' in role1 and 'TOOL_CFG_SENTINEL_A: meow~' in role1}\n"
                        f"role2_has_B={'CFG_VERSION=B' in role2 and 'TOOL_CFG_SENTINEL_B: meow~' in role2}\n"
                        f"role2_has_A={'CFG_VERSION=A' in role2 or 'TOOL_CFG_SENTINEL_A: meow~' in role2}\n"
                        f"worker_hash_changed={worker_hash_1 != worker_hash_2}\n"
                        f"worker_sid_changed={worker_sid_1 != worker_sid_2}\n"
                        f"file_a_exists={(Path(sandbox) / 'worker_cfg_a.txt').exists()}\n"
                        f"file_b_exists={(Path(sandbox) / 'worker_cfg_b.txt').exists()}\n"
                        f"report1={report1[:800]}\n"
                        f"report2={report2[:800]}\n"
                    ),
                    min_score=9,
                )
                print(f"evaluator(worker_cfg_hot_reload): {eval_result}")
        finally:
            restore()
            await runner.cleanup()


# ── T12: Shared CLAUDE.md native load (both roles) ────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_shared_claude_md_applies_to_orchestrator_and_worker():
    """Root CLAUDE.md should be loaded natively for orchestrator and worker."""
    _clear_nesting_guard()

    project_id = _unique_project_id("shared-md")
    with tempfile.TemporaryDirectory(prefix="dcc_e2e_shared_md_") as sandbox:
        restore = _patch_daemon_globals(
            name="e2e-shared-md",
            callback_url="http://127.0.0.1:19999",
        )
        app = _setup_daemon_app()
        runner, port = await _start_test_server(app)
        base = _base_url(port)

        try:
            async with ClientSession(timeout=ClientTimeout(total=300)) as http:
                async with http.post(
                    f"{base}/register",
                    json={"project_id": project_id, "project_dir": sandbox, "name": project_id},
                ) as resp:
                    assert resp.status == 200

                (Path(sandbox) / "CLAUDE.md").write_text(
                    "# Shared test memory\n"
                    "- ORCHESTRATOR RULE: when calling task_complete(summary), include exact token "
                    "`SHARED_ORCH_SENTINEL: meow~`.\n"
                    "- WORKER RULE: when calling submit_report(report), include exact token "
                    "`SHARED_WORKER_SENTINEL: meow~`.\n"
                )

                status = await _start_task_and_wait(
                    http=http,
                    base=base,
                    project_id=project_id,
                    task=(
                        "Follow exactly:\n"
                        "1) Call assign_worker exactly once (no direct edits) to create "
                        "shared_memory_probe.txt containing SHARED_TEST.\n"
                        "2) Call task_complete.\n"
                    ),
                    max_iterations=4,
                    timeout_secs=180,
                )
                assert status["status"] == "done", status
                assert "SHARED_ORCH_SENTINEL: meow~" in str(status.get("summary", "")), status

                report_path = Path(sandbox) / ".reports" / "iteration-1.md"
                assert report_path.exists(), "Expected worker report"
                report = report_path.read_text()
                assert "SHARED_WORKER_SENTINEL: meow~" in report, report
                assert (Path(sandbox) / "shared_memory_probe.txt").exists()

                eval_result = await _claude_eval_yes_no_score(
                    rubric=(
                        "Determine whether shared root CLAUDE.md was followed by both roles:\n"
                        "1) orchestrator summary includes shared orchestrator sentinel.\n"
                        "2) worker report includes shared worker sentinel.\n"
                        "3) task completed successfully."
                    ),
                    evidence=(
                        f"status_done={status.get('status') == 'done'}\n"
                        f"summary_has_shared_orch={'SHARED_ORCH_SENTINEL: meow~' in str(status.get('summary', ''))}\n"
                        f"report_has_shared_worker={'SHARED_WORKER_SENTINEL: meow~' in report}\n"
                        f"file_exists={(Path(sandbox) / 'shared_memory_probe.txt').exists()}\n"
                        f"summary={status.get('summary', '')[:400]}\n"
                        f"report={report[:800]}\n"
                    ),
                    min_score=9,
                )
                print(f"evaluator(shared_claude_md): {eval_result}")
        finally:
            restore()
            await runner.cleanup()


# ── T13: Role isolation between orchestrator and worker ────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_role_isolation_orchestrator_vs_worker():
    """orchestrator.md and worker.md should apply to their own roles."""
    _clear_nesting_guard()

    project_id = _unique_project_id("role-isolation")
    with tempfile.TemporaryDirectory(prefix="dcc_e2e_role_iso_") as sandbox:
        restore = _patch_daemon_globals(
            name="e2e-role-iso",
            callback_url="http://127.0.0.1:19999",
        )
        app = _setup_daemon_app()
        runner, port = await _start_test_server(app)
        base = _base_url(port)

        try:
            async with ClientSession(timeout=ClientTimeout(total=300)) as http:
                async with http.post(
                    f"{base}/register",
                    json={"project_id": project_id, "project_dir": sandbox, "name": project_id},
                ) as resp:
                    assert resp.status == 200

                role_dir = Path(sandbox) / ".claude" / "roles"
                role_dir.mkdir(parents=True, exist_ok=True)
                (role_dir / "orchestrator.md").write_text(
                    "When calling task_complete(summary), include exact token: "
                    "ORCH_ONLY_SENTINEL: meow~"
                )
                (role_dir / "worker.md").write_text(
                    "In every submit_report(report), include exact token: "
                    "WORKER_ONLY_SENTINEL: meow~\n"
                    "Do NOT include ORCH_ONLY_SENTINEL in the report."
                )

                status = await _start_task_and_wait(
                    http=http,
                    base=base,
                    project_id=project_id,
                    task=(
                        "Follow exactly:\n"
                        "1) Call assign_worker exactly once (no direct edits) to create "
                        "role_isolation_probe.txt containing ROLE_ISO.\n"
                        "2) Call task_complete.\n"
                    ),
                    max_iterations=4,
                    timeout_secs=180,
                )
                assert status["status"] == "done", status
                assert "ORCH_ONLY_SENTINEL: meow~" in str(status.get("summary", "")), status

                report_path = Path(sandbox) / ".reports" / "iteration-1.md"
                assert report_path.exists(), "Expected worker report"
                report = report_path.read_text()
                assert "WORKER_ONLY_SENTINEL: meow~" in report, report
                assert "ORCH_ONLY_SENTINEL" not in report, report
                assert (Path(sandbox) / "role_isolation_probe.txt").exists()

                eval_result = await _claude_eval_yes_no_score(
                    rubric=(
                        "Assess role-isolation behavior:\n"
                        "1) Orchestrator-only sentinel appears in orchestrator summary.\n"
                        "2) Worker-only sentinel appears in worker report.\n"
                        "3) Orchestrator sentinel should not leak into worker report.\n"
                        "4) Task completed successfully."
                    ),
                    evidence=(
                        f"status_done={status.get('status') == 'done'}\n"
                        f"summary_has_orch_only={'ORCH_ONLY_SENTINEL: meow~' in str(status.get('summary', ''))}\n"
                        f"report_has_worker_only={'WORKER_ONLY_SENTINEL: meow~' in report}\n"
                        f"report_has_orch_only={'ORCH_ONLY_SENTINEL' in report}\n"
                        f"file_exists={(Path(sandbox) / 'role_isolation_probe.txt').exists()}\n"
                        f"summary={status.get('summary', '')[:400]}\n"
                        f"report={report[:800]}\n"
                    ),
                    min_score=9,
                )
                print(f"evaluator(role_isolation): {eval_result}")
        finally:
            restore()
            await runner.cleanup()


# ── T14: ask_user + urgent queue visibility in pull_user_messages ─────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_ask_user_then_pull_messages_keeps_urgent_interrupt():
    """ask_user consumes one message while urgent follow-up remains queued."""
    _clear_nesting_guard()

    project_id = _unique_project_id("ask-pull-urgent")
    with tempfile.TemporaryDirectory(prefix="dcc_e2e_ask_pull_") as sandbox:
        restore = _patch_daemon_globals(
            name="e2e-ask-pull",
            callback_url="http://127.0.0.1:19999",
        )
        app = _setup_daemon_app()
        runner, port = await _start_test_server(app)
        base = _base_url(port)

        try:
            async with ClientSession(timeout=ClientTimeout(total=300)) as http:
                async with http.post(
                    f"{base}/register",
                    json={"project_id": project_id, "project_dir": sandbox, "name": project_id},
                ) as resp:
                    assert resp.status == 200

                async with http.post(
                    f"{base}/task",
                    json={
                        "project_id": project_id,
                        "task": (
                            "You MUST follow exactly:\n"
                            "1) Call ask_user with question: 'provide answer sentinel'.\n"
                            "2) Immediately call pull_user_messages once.\n"
                            "3) Call task_complete with a summary containing:\n"
                            "   - A line `ASK_USER_ANSWER: <answer text>`\n"
                            "   - A line `PULL_MESSAGES: <tool output>`\n"
                            "Do not call assign_worker."
                        ),
                        "max_iterations": 3,
                    },
                ) as resp:
                    assert resp.status == 200

                got_stuck = False
                for _ in range(60):
                    await asyncio.sleep(2)
                    async with http.get(f"{base}/status?project_id={project_id}") as resp:
                        status = await resp.json()
                        if status.get("status") == "stuck":
                            got_stuck = True
                            break
                        if status.get("status") in ("done", "error"):
                            break
                assert got_stuck, f"Expected stuck before answering ask_user. Last: {status}"

                answer_text = "ANSWER_SENTINEL_42"
                urgent_text = "URGENT_SENTINEL_99"
                async with http.post(
                    f"{base}/interrupt",
                    json={"project_id": project_id, "message": answer_text, "urgency": "normal"},
                ) as resp:
                    assert resp.status == 200
                async with http.post(
                    f"{base}/interrupt",
                    json={"project_id": project_id, "message": urgent_text, "urgency": "urgent"},
                ) as resp:
                    assert resp.status == 200

                final = {}
                for _ in range(60):
                    await asyncio.sleep(2)
                    async with http.get(f"{base}/status?project_id={project_id}") as resp:
                        final = await resp.json()
                        if final.get("status") in ("done", "error"):
                            break
                assert final.get("status") == "done", final
                summary = str(final.get("summary", ""))
                assert "ASK_USER_ANSWER: ANSWER_SENTINEL_42" in summary, summary
                assert "URGENT_SENTINEL_99" in summary, summary
                assert "[urgent]" in summary.lower(), summary

                eval_result = await _claude_eval_yes_no_score(
                    rubric=(
                        "Evaluate ask_user + queued urgent interrupt behavior:\n"
                        "1) ask_user answer is captured in final summary.\n"
                        "2) urgent follow-up message remains visible after ask_user via pull_user_messages.\n"
                        "3) urgency metadata is reflected.\n"
                        "4) Task completed successfully."
                    ),
                    evidence=(
                        f"status_done={final.get('status') == 'done'}\n"
                        f"summary_has_answer={'ASK_USER_ANSWER: ANSWER_SENTINEL_42' in summary}\n"
                        f"summary_has_urgent_text={'URGENT_SENTINEL_99' in summary}\n"
                        f"summary_has_urgent_tag={'[urgent]' in summary.lower()}\n"
                        f"summary={summary[:800]}\n"
                    ),
                    min_score=8,
                )
                print(f"evaluator(ask_user_pull_urgent): {eval_result}")
        finally:
            restore()
            await runner.cleanup()


# ── T15: State file persists role prompt hashes ────────────────────────


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_state_persists_prompt_hashes():
    """State JSON should persist prompt hashes for role-memory reload logic."""
    _clear_nesting_guard()

    import tools.orchestrator_daemon as daemon_mod

    project_id = _unique_project_id("state-hash")
    with tempfile.TemporaryDirectory(prefix="dcc_e2e_state_hash_") as sandbox:
        restore = _patch_daemon_globals(
            name="e2e-state-hash",
            callback_url="http://127.0.0.1:19999",
        )
        app = _setup_daemon_app()
        runner, port = await _start_test_server(app)
        base = _base_url(port)

        try:
            async with ClientSession(timeout=ClientTimeout(total=300)) as http:
                async with http.post(
                    f"{base}/register",
                    json={"project_id": project_id, "project_dir": sandbox, "name": project_id},
                ) as resp:
                    assert resp.status == 200

                role_dir = Path(sandbox) / ".claude" / "roles"
                role_dir.mkdir(parents=True, exist_ok=True)
                (role_dir / "orchestrator.md").write_text("State hash probe orchestrator sentinel.")
                (role_dir / "worker.md").write_text("State hash probe worker sentinel.")

                status = await _start_task_and_wait(
                    http=http,
                    base=base,
                    project_id=project_id,
                    task=(
                        "Follow exactly:\n"
                        "1) Call assign_worker exactly once (no direct edits) to create "
                        "state_hash_probe.txt containing HASH_OK.\n"
                        "2) Call task_complete.\n"
                    ),
                    max_iterations=4,
                    timeout_secs=180,
                )
                assert status["status"] == "done", status

                state_path = daemon_mod.STATE_DIR / f"{project_id}.json"
                assert state_path.exists(), f"Missing persisted state file: {state_path}"
                state = json.loads(state_path.read_text())
                orch_hash = str(state.get("orchestrator_prompt_hash", ""))
                worker_hash = str(state.get("worker_prompt_hash", ""))
                assert len(orch_hash) == 64, state
                assert len(worker_hash) == 64, state
                assert (Path(sandbox) / "state_hash_probe.txt").exists()
        finally:
            restore()
            await runner.cleanup()
