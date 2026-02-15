"""Verify Claude Code CLI is installed and responds correctly."""

import asyncio
import json
import os
import pytest


def _clean_env():
    """Return env dict without CLAUDECODE to avoid nested-session detection."""
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    return env



@pytest.mark.asyncio
async def test_claude_cli_exists():
    """claude binary is on PATH and runs."""
    proc = await asyncio.create_subprocess_exec(
        "claude", "--version",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_clean_env(),
    )
    stdout, _ = await proc.communicate()
    assert proc.returncode == 0, "claude CLI not found or failed"
    print(f"Claude version: {stdout.decode().strip()}")


@pytest.mark.asyncio
async def test_claude_print_mode_json():
    """claude -p --output-format json returns valid JSON with 'result' key."""
    proc = await asyncio.create_subprocess_exec(
        "claude", "-p",
        "--output-format", "json",
        "--no-session-persistence",
        "--model", "haiku",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_clean_env(),
    )
    stdout, stderr = await asyncio.wait_for(
        proc.communicate(input=b"Reply with exactly: HELLO"),
        timeout=30,
    )
    assert proc.returncode == 0, f"claude -p failed: {stderr.decode()}"

    data = json.loads(stdout.decode())
    assert "result" in data, f"Missing 'result' key in response: {data.keys()}"
    assert "session_id" in data, f"Missing 'session_id' key in response: {data.keys()}"
    assert "HELLO" in data["result"], f"Unexpected result: {data['result'][:200]}"
    print(f"Result: {data['result'][:100]}")
    print(f"Session ID: {data['session_id']}")


@pytest.mark.asyncio
async def test_claude_session_resume():
    """Session can be created and resumed with --resume."""
    # Create a session
    proc = await asyncio.create_subprocess_exec(
        "claude", "-p",
        "--output-format", "json",
        "--model", "haiku",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_clean_env(),
    )
    stdout, _ = await asyncio.wait_for(
        proc.communicate(input=b"Remember this word: BANANA"),
        timeout=30,
    )
    data = json.loads(stdout.decode())
    session_id = data["session_id"]
    assert session_id, "No session_id returned"

    # Resume the session
    proc2 = await asyncio.create_subprocess_exec(
        "claude", "-p",
        "--output-format", "json",
        "--resume", session_id,
        "--model", "haiku",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_clean_env(),
    )
    stdout2, _ = await asyncio.wait_for(
        proc2.communicate(input=b"What word did I ask you to remember? Reply with just the word."),
        timeout=30,
    )
    data2 = json.loads(stdout2.decode())
    assert "BANANA" in data2["result"].upper(), f"Session resume failed, got: {data2['result'][:200]}"
    print(f"Resumed session {session_id}, got: {data2['result'][:100]}")


@pytest.mark.asyncio
async def test_claude_can_read_files():
    """Claude in -p mode can use Read tool (needed for our agentic routing)."""
    # Use absolute path — config.yaml is gitignored so Claude may not find it by relative path
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.yaml")
    prompt = f"Read the file {config_path} and tell me the server name defined in it. Reply with just the name."
    proc = await asyncio.create_subprocess_exec(
        "claude", "-p",
        "--output-format", "json",
        "--no-session-persistence",
        "--model", "haiku",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_clean_env(),
    )
    stdout, stderr = await asyncio.wait_for(
        proc.communicate(input=prompt.encode()),
        timeout=60,
    )
    assert proc.returncode == 0, f"Failed: {stderr.decode()}"
    data = json.loads(stdout.decode())
    assert "local" in data["result"].lower(), f"Claude couldn't read config.yaml: {data['result'][:200]}"
    print(f"Claude read config.yaml: {data['result'][:100]}")
