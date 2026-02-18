"""Probe: can a Claude agent autonomously enter/exit plan mode via the SDK?

Run: uv run python tests/probe_plan_mode.py
"""

import asyncio
import os

from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage
from claude_agent_sdk.types import (
    AssistantMessage, TextBlock, ToolUseBlock, ToolResultBlock,
    PermissionResultAllow,
)


def _clear():
    os.environ.pop("CLAUDECODE", None)


def _print_messages(message):
    """Print assistant/result messages."""
    if isinstance(message, AssistantMessage):
        for block in message.content:
            if isinstance(block, TextBlock):
                print(f"  [TEXT] {block.text[:500]}")
            elif isinstance(block, ToolUseBlock):
                print(f"  [TOOL USE] {block.name}: {str(block.input)[:300]}")
            elif isinstance(block, ToolResultBlock):
                content = block.content if isinstance(block.content, str) else str(block.content)
                err = " (ERROR)" if block.is_error else ""
                print(f"  [TOOL RESULT{err}] {content[:300]}")
    elif isinstance(message, ResultMessage):
        print(f"\n  [RESULT] session_id={message.session_id}")
        print(f"  [RESULT] cost=${message.total_cost_usd}")
        print(f"  [RESULT] text={message.result[:500] if message.result else '(empty)'}")
    return message


async def _prompt_stream(text: str):
    yield {
        "type": "user",
        "session_id": "",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
    }


async def test_self_enter_plan_mode():
    """Normal mode agent — can it call EnterPlanMode on its own?

    EnterPlanMode/ExitPlanMode NOT in disallowed_tools.
    We tell the agent to plan first, then execute.
    """
    _clear()
    print("=" * 60)
    print("TEST 1: Agent self-enters plan mode (EnterPlanMode not blocked)")
    print("=" * 60)

    tool_log = []

    async def track_tools(tool_name, input_data, context=None):
        tool_log.append(tool_name)
        print(f"  [can_use_tool] {tool_name}")
        return PermissionResultAllow(updated_input=input_data)

    options = ClaudeAgentOptions(
        model="haiku",
        cwd="/tmp",
        can_use_tool=track_tools,
        # EnterPlanMode/ExitPlanMode NOT blocked — agent can use them
        allowed_tools=["Read", "Glob", "Grep", "Write", "Bash"],
        # No disallowed_tools for plan mode tools
    )

    session_id = None
    async for message in query(
        prompt=_prompt_stream(
            "This is a complex task. Before doing anything, enter plan mode to plan your approach. "
            "Create a file /tmp/self_plan_test.txt with 'planned and executed'. "
            "Plan first, then execute after exiting plan mode."
        ),
        options=options,
    ):
        msg = _print_messages(message)
        if isinstance(msg, ResultMessage):
            session_id = msg.session_id

    print(f"\nTools that went through can_use_tool: {tool_log}")
    exists = os.path.exists("/tmp/self_plan_test.txt")
    print(f"File created? {exists}")
    if exists:
        with open("/tmp/self_plan_test.txt") as f:
            print(f"Content: {f.read()}")
        os.remove("/tmp/self_plan_test.txt")

    return session_id


async def test_resume_after_plan(session_id: str):
    """Resume a session after plan mode — does it execute?"""
    if not session_id:
        print("\nSkipping resume test — no session_id")
        return

    _clear()
    print("\n" + "=" * 60)
    print(f"TEST 2: Resume session {session_id[:12]}... after plan")
    print("=" * 60)

    tool_log = []

    async def track_tools(tool_name, input_data, context=None):
        tool_log.append(tool_name)
        print(f"  [can_use_tool] {tool_name}")
        return PermissionResultAllow(updated_input=input_data)

    options = ClaudeAgentOptions(
        model="haiku",
        cwd="/tmp",
        can_use_tool=track_tools,
        resume=session_id,
        allowed_tools=["Read", "Glob", "Grep", "Write", "Bash"],
    )

    async for message in query(
        prompt=_prompt_stream("The plan looks good. Please proceed with execution."),
        options=options,
    ):
        _print_messages(message)

    print(f"\nTools that went through can_use_tool: {tool_log}")
    exists = os.path.exists("/tmp/self_plan_test.txt")
    print(f"File created after resume? {exists}")
    if exists:
        with open("/tmp/self_plan_test.txt") as f:
            print(f"Content: {f.read()}")
        os.remove("/tmp/self_plan_test.txt")


async def test_plan_mode_blocked_vs_unblocked():
    """Compare: what happens if EnterPlanMode IS in disallowed_tools?"""
    _clear()
    print("\n" + "=" * 60)
    print("TEST 3: EnterPlanMode in disallowed_tools — agent can't plan")
    print("=" * 60)

    tool_log = []

    async def track_tools(tool_name, input_data, context=None):
        tool_log.append(tool_name)
        print(f"  [can_use_tool] {tool_name}")
        return PermissionResultAllow(updated_input=input_data)

    options = ClaudeAgentOptions(
        model="haiku",
        cwd="/tmp",
        can_use_tool=track_tools,
        allowed_tools=["Read", "Glob", "Grep", "Write", "Bash"],
        disallowed_tools=["EnterPlanMode", "ExitPlanMode"],
    )

    async for message in query(
        prompt=_prompt_stream(
            "This is a complex task. Enter plan mode to plan your approach first. "
            "Create a file /tmp/blocked_plan_test.txt with 'no plan mode'."
        ),
        options=options,
    ):
        _print_messages(message)

    print(f"\nTools that went through can_use_tool: {tool_log}")
    exists = os.path.exists("/tmp/blocked_plan_test.txt")
    print(f"File created? {exists}")
    if exists:
        with open("/tmp/blocked_plan_test.txt") as f:
            print(f"Content: {f.read()}")
        os.remove("/tmp/blocked_plan_test.txt")


async def main():
    session_id = await test_self_enter_plan_mode()
    await test_resume_after_plan(session_id)
    await test_plan_mode_blocked_vs_unblocked()


if __name__ == "__main__":
    asyncio.run(main())
