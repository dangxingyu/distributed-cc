"""Tests for RouterSession: init, running flag, progress callbacks, session resume."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.router_session import RouterSession, _prompt_stream


# ── Basic state ──────────────────────────────────────────────────────────


def test_router_session_init():
    """RouterSession starts with no session_id and not running."""
    s = RouterSession(cwd="/tmp")
    assert s.session_id is None
    assert s.is_running is False


def test_router_session_init_resolves_cwd():
    """cwd is resolved to an absolute path."""
    s = RouterSession(cwd=".")
    assert s._cwd.startswith("/")


def test_router_session_permission_mode_from_env(monkeypatch):
    monkeypatch.setenv("DCC_ROUTER_PERMISSION_MODE", "default")
    s = RouterSession(cwd="/tmp")
    assert s._permission_mode == "default"


# ── Running flag ─────────────────────────────────────────────────────────


async def test_router_is_running_flag():
    """is_running is True during run(), False after."""
    s = RouterSession(cwd="/tmp")

    observed_running = []

    # Mock the _run_inner to capture is_running mid-flight
    async def fake_run_inner(msg):
        observed_running.append(s.is_running)
        return "done"

    s._run_inner = fake_run_inner

    result = await s.run("hello")
    assert result == "done"
    assert observed_running == [True]
    assert s.is_running is False


async def test_router_is_running_flag_on_error():
    """is_running resets to False even if _run_inner raises."""
    s = RouterSession(cwd="/tmp")

    async def failing_run_inner(msg):
        raise RuntimeError("boom")

    s._run_inner = failing_run_inner

    with pytest.raises(RuntimeError, match="boom"):
        await s.run("hello")
    assert s.is_running is False


# ── Progress callbacks ───────────────────────────────────────────────────


async def test_router_progress_callback():
    """Progress callback receives text from AssistantMessage TextBlocks."""
    from claude_agent_sdk.types import TextBlock

    s = RouterSession(cwd="/tmp")
    received = []
    log_received = []

    async def on_progress(text):
        received.append(text)

    async def on_log(text):
        log_received.append(text)

    s.set_callbacks(progress=on_progress, log=on_log)

    # Simulate an AssistantMessage with a TextBlock
    mock_msg = MagicMock()
    mock_msg.content = [TextBlock(text="Setting up server...")]
    await s._forward_progress(mock_msg)

    assert received == ["Setting up server..."]
    assert log_received == []


async def test_router_log_callback_on_tool_use():
    """Log callback receives tool use events."""
    from claude_agent_sdk.types import ToolUseBlock

    s = RouterSession(cwd="/tmp")
    log_received = []

    async def on_log(text):
        log_received.append(text)

    s.set_callbacks(log=on_log)

    mock_msg = MagicMock()
    mock_msg.content = [ToolUseBlock(id="t1", name="Bash", input={"command": "ls"})]
    await s._forward_progress(mock_msg)

    assert len(log_received) == 1
    assert "Bash" in log_received[0]
    assert "router ->" in log_received[0]


# ── Final result dedupe ───────────────────────────────────────────────────


async def test_should_emit_final_result_false_when_same_as_last_stream_text():
    from claude_agent_sdk.types import TextBlock

    s = RouterSession(cwd="/tmp")
    mock_msg = MagicMock()
    mock_msg.content = [TextBlock(text="final summary")]
    await s._forward_progress(mock_msg)

    assert s.should_emit_final_result("final summary") is False


def test_should_emit_final_result_true_when_no_stream_text():
    s = RouterSession(cwd="/tmp")
    assert s.should_emit_final_result("final summary") is True


async def test_should_emit_final_result_true_when_different_from_last_stream_text():
    from claude_agent_sdk.types import TextBlock

    s = RouterSession(cwd="/tmp")
    mock_msg = MagicMock()
    mock_msg.content = [TextBlock(text="progress chunk")]
    await s._forward_progress(mock_msg)

    assert s.should_emit_final_result("different final summary") is True


async def test_should_emit_final_result_false_when_same_as_earlier_stream_text():
    from claude_agent_sdk.types import TextBlock

    s = RouterSession(cwd="/tmp")
    msg1 = MagicMock()
    msg1.content = [TextBlock(text="first summary")]
    await s._forward_progress(msg1)
    msg2 = MagicMock()
    msg2.content = [TextBlock(text="second summary")]
    await s._forward_progress(msg2)

    assert s.should_emit_final_result("first summary") is False


# ── Session resume ───────────────────────────────────────────────────────


async def test_router_session_resume():
    """session_id is preserved after a successful run for resuming."""
    s = RouterSession(cwd="/tmp")
    assert s.session_id is None

    # Simulate a run that sets session_id
    s._session_id = "sess-abc123"
    assert s.session_id == "sess-abc123"


# ── Prompt stream ────────────────────────────────────────────────────────


async def test_prompt_stream_yields_message():
    """_prompt_stream yields one user message then waits for done."""
    done = asyncio.Event()
    messages = []

    async def collect():
        async for msg in _prompt_stream("hello world", done):
            messages.append(msg)

    task = asyncio.create_task(collect())
    await asyncio.sleep(0.05)  # Let it yield the first message

    assert len(messages) == 1
    assert messages[0]["message"]["content"] == "hello world"
    assert messages[0]["type"] == "user"

    # Stream should still be alive (waiting on done)
    assert not task.done()

    done.set()
    await asyncio.wait_for(task, timeout=1.0)


async def test_prompt_stream_without_done():
    """_prompt_stream without done event completes after yielding."""
    messages = []
    async for msg in _prompt_stream("test"):
        messages.append(msg)
    assert len(messages) == 1
