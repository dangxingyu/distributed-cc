"""Test claude -p behavior with interactive features.

These tests verify that claude -p handles plan mode, AskUserQuestion,
and permission modes correctly for our orchestrator use case.

Requires a real Claude API key — these are E2E tests that cost money.
Use `pytest tests/test_claude_print_mode.py -v` to run explicitly.
"""

import asyncio
import json
import os
import pty
import select
import subprocess
import time

import pytest


def _clean_env():
    """Remove Claude Code nesting guard env vars."""
    env = os.environ.copy()
    for key in ["CLAUDECODE", "CLAUDE_CODE_SSE_PORT", "CLAUDE_CODE_ENTRYPOINT"]:
        env.pop(key, None)
    return env


def _run_claude_pty(
    prompt: str,
    extra_args: list[str] | None = None,
    timeout_s: int = 60,
) -> dict:
    """Run claude -p with a pseudo-TTY to work around the TTY requirement.

    Returns the parsed JSON output dict, or a dict with 'error' key on failure.
    """
    master_fd, slave_fd = pty.openpty()

    cmd = [
        "claude", "-p",
        "--output-format", "json",
        "--no-session-persistence",
        "--model", "haiku",
    ]
    if extra_args:
        cmd.extend(extra_args)

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=slave_fd,
        stderr=slave_fd,
        env=_clean_env(),
        close_fds=True,
    )
    os.close(slave_fd)

    proc.stdin.write(prompt.encode() + b"\n")
    proc.stdin.close()

    output = b""
    start = time.time()
    while time.time() - start < timeout_s:
        ready, _, _ = select.select([master_fd], [], [], 1.0)
        if ready:
            try:
                data = os.read(master_fd, 8192)
                if not data:
                    break
                output += data
            except OSError:
                break
        if proc.poll() is not None:
            # Drain remaining output
            while True:
                ready, _, _ = select.select([master_fd], [], [], 0.5)
                if not ready:
                    break
                try:
                    data = os.read(master_fd, 8192)
                    if not data:
                        break
                    output += data
                except OSError:
                    break
            break
    else:
        proc.kill()
        os.close(master_fd)
        return {"error": "TIMEOUT", "raw": output.decode(errors="replace")[:500]}

    os.close(master_fd)
    proc.wait()

    text = output.decode(errors="replace")
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("{") and '"type"' in line and '"result"' in line:
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    return {"error": "NO_JSON", "raw": text[:1000]}


# ── Tests ──────────────────────────────────────────────────────────────


@pytest.mark.e2e
class TestClaudePrintMode:
    """Tests for claude -p behavior with interactive features.

    Mark with @pytest.mark.e2e — these call real Claude and cost money.
    Run with: pytest tests/test_claude_print_mode.py -v -m e2e
    """

    def test_basic_json_response(self):
        """claude -p returns valid JSON with result key."""
        data = _run_claude_pty(
            'Reply with exactly the number 42, nothing else.',
            extra_args=["--max-turns", "1"],
            timeout_s=30,
        )
        assert "error" not in data, f"Failed: {data}"
        assert "result" in data
        assert "42" in data["result"]

    def test_ask_user_question_does_not_hang(self):
        """AskUserQuestion in -p mode completes without hanging.

        The tool fires but gets no user response. Claude should
        continue/finish rather than hang waiting for input.
        """
        data = _run_claude_pty(
            "Use the AskUserQuestion tool to ask me what color I like. "
            "After using the tool, just say 'done'.",
            extra_args=["--max-turns", "3"],
            timeout_s=60,
        )
        assert "error" not in data, f"Failed: {data}"
        # Session completed — didn't hang
        assert data.get("num_turns", 0) >= 1

    def test_disallowed_tools_blocks_ask_user(self):
        """--disallowedTools prevents AskUserQuestion from being used.

        This is what our orchestrator does — forces questions through
        the JSON action protocol instead.
        """
        data = _run_claude_pty(
            "Use the AskUserQuestion tool to ask me a question. "
            "If you cannot use it, say 'TOOL_BLOCKED'.",
            extra_args=[
                "--max-turns", "2",
                "--disallowedTools", "AskUserQuestion,EnterPlanMode,ExitPlanMode",
            ],
            timeout_s=60,
        )
        assert "error" not in data, f"Failed: {data}"
        result = data.get("result", "").upper()
        # Claude should report it can't use the tool
        assert "BLOCK" in result or "UNABLE" in result or "CANNOT" in result or "DON" in result or "NOT" in result, \
            f"Expected Claude to report tool is blocked, got: {data.get('result', '')[:300]}"

    def test_system_prompt_json_forces_reply_action(self):
        """With our system prompt, Claude asks questions via JSON reply
        instead of AskUserQuestion.

        This simulates our orchestrator: system prompt forces JSON output,
        AskUserQuestion is disallowed, questions go through {"action": "reply"}.
        """
        system_prompt = (
            "You are an orchestrator. Your response MUST always be ONLY a JSON object. "
            "IMPORTANT: You do NOT have access to AskUserQuestion or EnterPlanMode. "
            "To ask the user a question, use: "
            '{"action": "reply", "text": "<your question>"}. '
            "To assign work: "
            '{"action": "assign", "server": "...", "session": "...", "prompt": "..."}.'
        )
        data = _run_claude_pty(
            "[CHANNEL WORKERS]\n  (none)\n\n"
            "[USER MESSAGE]\n"
            "Set up something on one of my servers, but I forgot to tell you which one.",
            extra_args=[
                "--max-turns", "1",
                "--system-prompt", system_prompt,
                "--disallowedTools", "AskUserQuestion,EnterPlanMode,ExitPlanMode",
            ],
            timeout_s=60,
        )
        assert "error" not in data, f"Failed: {data}"
        result_text = data.get("result", "")
        # Should be parseable JSON with action=reply
        clean = result_text.strip()
        if clean.startswith("```"):
            clean = "\n".join(clean.split("\n")[1:])
        if clean.endswith("```"):
            clean = clean.rsplit("```", 1)[0]
        parsed = json.loads(clean.strip())
        assert parsed.get("action") == "reply", f"Expected reply action, got: {parsed}"
        assert len(parsed.get("text", "")) > 0, "Reply text should not be empty"

    def test_permission_mode_plan_blocks_writes(self):
        """--permission-mode plan prevents file writes."""
        data = _run_claude_pty(
            "Try to write 'hello' to /tmp/plan_mode_test_file.txt. "
            "Report whether you succeeded or were blocked.",
            extra_args=["--max-turns", "3", "--permission-mode", "plan"],
            timeout_s=60,
        )
        assert "error" not in data, f"Failed: {data}"
        result = data.get("result", "").lower()
        # Should report inability to write
        assert any(w in result for w in ["cannot", "blocked", "not able", "unable", "restrict", "plan mode", "read-only"]), \
            f"Expected write to be blocked, got: {data.get('result', '')[:300]}"

    def test_enter_plan_mode_works(self):
        """EnterPlanMode tool works in -p mode (when not disallowed)."""
        data = _run_claude_pty(
            "Enter plan mode using the EnterPlanMode tool. "
            "After entering, describe what mode you are in.",
            extra_args=["--max-turns", "3"],
            timeout_s=60,
        )
        assert "error" not in data, f"Failed: {data}"
        result = data.get("result", "").lower()
        assert "plan" in result, f"Expected plan mode mention, got: {data.get('result', '')[:300]}"
