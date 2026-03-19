import asyncio
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
from aiohttp import web, ClientSession, ClientTimeout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from dcc_runtime import RuntimeEvent, RuntimeRequest, ToolSpec
from dcc_runtime.codex_backend import run_turn as run_codex_turn


_CODEX_AUTH_AVAILABLE: bool | None = None


def _init_repo(path: str) -> None:
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)


def _unique_project_id(prefix: str) -> str:
    return f"{prefix}-{next(tempfile._get_candidate_names())}"


def _base_url(port: int) -> str:
    return f"http://127.0.0.1:{port}"


async def _start_test_server(app: web.Application) -> tuple[web.AppRunner, int]:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    sockets = site._server.sockets if site._server else []
    assert sockets, "Test server did not expose a bound socket"
    port = int(sockets[0].getsockname()[1])
    return runner, port


def _setup_daemon_app():
    import orchestrator_daemon as daemon_mod

    app = web.Application()
    app.router.add_post("/register", daemon_mod.handle_register)
    app.router.add_post("/task", daemon_mod.handle_task)
    app.router.add_post("/interrupt", daemon_mod.handle_interrupt)
    app.router.add_get("/status", daemon_mod.handle_status)
    app.router.add_get("/events", daemon_mod.handle_events)
    app.router.add_get("/stream", daemon_mod.handle_stream)
    app.router.add_post("/stop", daemon_mod.handle_stop)
    app.router.add_get("/health", daemon_mod.handle_health)
    app.on_cleanup.append(daemon_mod.on_cleanup)
    return app


def _patch_daemon_globals(name: str, callback_url: str):
    import orchestrator_daemon as daemon_mod

    state_dir_ctx = tempfile.TemporaryDirectory(prefix="dcc_codex_e2e_state_")
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
        "orchestrator_plugin_hashes": dict(daemon_mod.orchestrator_plugin_hashes),
        "worker_plugin_hashes": dict(daemon_mod.worker_plugin_hashes),
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
    daemon_mod.orchestrator_plugin_hashes.clear()
    daemon_mod.worker_plugin_hashes.clear()

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
        daemon_mod.orchestrator_plugin_hashes.clear()
        daemon_mod.orchestrator_plugin_hashes.update(orig["orchestrator_plugin_hashes"])
        daemon_mod.worker_plugin_hashes.clear()
        daemon_mod.worker_plugin_hashes.update(orig["worker_plugin_hashes"])
        state_dir_ctx.cleanup()

    return restore


async def _run_codex_daemon_task(
    sandbox: str,
    project_id: str,
    task: str,
    max_iterations: int = 4,
    timeout_secs: int = 240,
) -> dict:
    await _ensure_codex_auth()
    restore = _patch_daemon_globals(
        name=f"codex-{project_id}",
        callback_url="",
    )
    app = _setup_daemon_app()
    runner, port = await _start_test_server(app)
    base = _base_url(port)

    try:
        async with ClientSession(timeout=ClientTimeout(total=timeout_secs + 30)) as http:
            async with http.post(
                f"{base}/register",
                json={"project_id": project_id, "project_dir": sandbox, "name": project_id},
            ) as resp:
                assert resp.status == 200

            async with http.post(
                f"{base}/task",
                json={
                    "project_id": project_id,
                    "task": task,
                    "provider": "codex",
                    "model": "gpt-5.4",
                    "sandbox_mode": "workspace-write",
                    "approval_policy": "never",
                    "max_iterations": max_iterations,
                    "continuous_mode": False,
                },
            ) as resp:
                assert resp.status == 200, await resp.text()

            for _ in range(timeout_secs // 2):
                await asyncio.sleep(2)
                async with http.get(f"{base}/status?project_id={project_id}") as resp:
                    status = await resp.json()
                    if status.get("status") in ("done", "error", "stuck"):
                        return status
            pytest.fail(f"Codex daemon task did not complete within {timeout_secs}s")
    finally:
        import orchestrator_daemon as daemon_mod

        await runner.cleanup()
        pending = list(daemon_mod.running_tasks.values())
        for task in pending:
            if task and not task.done():
                task.cancel()
        for task in pending:
            if not task:
                continue
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        if daemon_mod.callback_http_session and not daemon_mod.callback_http_session.closed:
            await daemon_mod.callback_http_session.close()
        daemon_mod.callback_http_session = None
        restore()


async def _ensure_codex_auth() -> None:
    global _CODEX_AUTH_AVAILABLE
    if _CODEX_AUTH_AVAILABLE is True:
        return
    if _CODEX_AUTH_AVAILABLE is False:
        pytest.skip("Codex CLI is not authenticated in this environment")

    def _run_preflight() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--sandbox",
                "read-only",
                "-C",
                "/tmp",
                "Reply with exactly: CODEX_AUTH_OK",
            ],
            capture_output=True,
            text=True,
            timeout=90,
            env=os.environ.copy(),
        )

    proc = await asyncio.to_thread(_run_preflight)
    combined = "\n".join(part for part in (proc.stdout, proc.stderr) if part).strip()
    lowered = combined.lower()
    if proc.returncode != 0:
        _CODEX_AUTH_AVAILABLE = False
        pytest.skip(f"Codex preflight failed: {combined[:240]}")
    if "not logged in" in lowered or "login" in lowered and "codex_auth_ok" not in lowered:
        _CODEX_AUTH_AVAILABLE = False
        pytest.skip("Codex CLI is not authenticated in this environment")
    if "CODEX_AUTH_OK" not in combined:
        _CODEX_AUTH_AVAILABLE = False
        pytest.skip(f"Codex preflight returned unexpected output: {combined[:240]}")
    _CODEX_AUTH_AVAILABLE = True


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_codex_e2e_basic_runtime_turn():
    await _ensure_codex_auth()

    events: list[RuntimeEvent] = []

    async def on_event(event: RuntimeEvent) -> None:
        events.append(event)

    with tempfile.TemporaryDirectory(prefix="dcc_codex_basic_") as sandbox:
        _init_repo(sandbox)
        request = RuntimeRequest(
            prompt="Reply with exactly CODEX_RUNTIME_OK",
            project_dir=sandbox,
            source="orchestrator",
            system_prompt="You are terse. Follow the user exactly.",
            model="gpt-5.4",
            sandbox_mode="workspace-write",
            approval_policy="never",
            runtime_home_dir=str(Path(sandbox) / ".codex-home-basic"),
        )
        result = await run_codex_turn(request, on_event)

    assert result.session_id, "Expected a Codex thread/session id"
    assert "CODEX_RUNTIME_OK" in result.final_text, result.final_text
    assert any(event.type == "text" for event in events), events


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_codex_e2e_mcp_tool_bridge():
    await _ensure_codex_auth()

    events: list[RuntimeEvent] = []
    tool_calls: list[dict[str, str]] = []

    async def on_event(event: RuntimeEvent) -> None:
        events.append(event)

    async def echo_tool(args: dict[str, str]) -> dict:
        tool_calls.append(args)
        return {
            "content": [
                {
                    "type": "text",
                    "text": f"TOOL::VALUE::{args['text']}",
                }
            ]
        }

    with tempfile.TemporaryDirectory(prefix="dcc_codex_mcp_") as sandbox:
        _init_repo(sandbox)
        request = RuntimeRequest(
            prompt=(
                "Call demo_echo with text=banana exactly once. "
                "After the tool returns, reply with exactly the tool result and nothing else."
            ),
            project_dir=sandbox,
            source="orchestrator",
            system_prompt="Always use the provided MCP tools when explicitly instructed.",
            model="gpt-5.4",
            sandbox_mode="workspace-write",
            approval_policy="never",
            runtime_home_dir=str(Path(sandbox) / ".codex-home-mcp"),
            tool_specs=[
                ToolSpec(
                    name="demo_echo",
                    description="Return TOOL::VALUE::<text> for the provided text.",
                    input_schema={"text": str},
                    handler=echo_tool,
                )
            ],
        )
        result = await run_codex_turn(request, on_event)

    assert result.session_id, "Expected a Codex thread/session id"
    assert tool_calls == [{"text": "banana"}], tool_calls
    assert "TOOL::VALUE::banana" in result.final_text, result.final_text
    assert any(event.type == "tool_use" and "demo_echo" in event.data for event in events), events


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_codex_e2e_daemon_task_complete_path():
    with tempfile.TemporaryDirectory(prefix="dcc_codex_daemon_") as sandbox:
        _init_repo(sandbox)
        project_id = _unique_project_id("codex-daemon")
        status = await _run_codex_daemon_task(
            sandbox=sandbox,
            project_id=project_id,
            task=(
                "Reply with a short sentence containing CODEX_DAEMON_OK. "
                "Then call task_complete immediately with summary CODEX_DAEMON_OK."
            ),
            max_iterations=2,
            timeout_secs=180,
        )

    assert status["provider"] == "codex", status
    assert status["status"] == "done", status
    assert str(status.get("summary") or "").strip(), status


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_codex_e2e_daemon_assign_worker_path():
    with tempfile.TemporaryDirectory(prefix="dcc_codex_worker_") as sandbox:
        _init_repo(sandbox)
        project_id = _unique_project_id("codex-worker")
        probe = Path(sandbox) / "codex_worker_probe.txt"
        status = await _run_codex_daemon_task(
            sandbox=sandbox,
            project_id=project_id,
            task=(
                "Follow these steps exactly:\n"
                "1. Call assign_worker exactly once.\n"
                "2. Ask the worker to create codex_worker_probe.txt with exact content CODEX_WORKER_OK.\n"
                "3. After the worker returns, verify that file exists and contains CODEX_WORKER_OK.\n"
                "4. Call task_complete with summary CODEX_DAEMON_WORKER_OK."
            ),
            max_iterations=4,
            timeout_secs=240,
        )

        assert probe.exists(), f"Expected worker-created file at {probe}"
        assert probe.read_text().strip() == "CODEX_WORKER_OK"

    assert status["provider"] == "codex", status
    assert status["status"] == "done", status
    assert str(status.get("summary") or "").strip(), status
