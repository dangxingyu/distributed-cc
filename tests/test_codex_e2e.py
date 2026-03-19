import asyncio
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from dcc_runtime import RuntimeEvent, RuntimeRequest, ToolSpec
from dcc_runtime.codex_backend import run_turn as run_codex_turn


_CODEX_AUTH_AVAILABLE: bool | None = None


def _init_repo(path: str) -> None:
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)


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
