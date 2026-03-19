from __future__ import annotations

import asyncio
import json
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, create_sdk_mcp_server, query, tool
from claude_agent_sdk.types import (
    AssistantMessage,
    SystemMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
)

from .base import EventSink, RuntimeEvent, RuntimeRequest, RuntimeResult, ToolSpec


async def _prompt_stream(text: str, done: asyncio.Event | None = None):
    yield {
        "type": "user",
        "session_id": "",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
    }
    if done is not None:
        await done.wait()


async def _emit_assistant_message(
    message: AssistantMessage,
    source: str,
    on_event: EventSink,
) -> None:
    for block in message.content:
        if isinstance(block, TextBlock):
            text = block.text.strip()
            if text:
                await on_event(RuntimeEvent(type="text", data=f"[{source}] {text}"))
        elif isinstance(block, ToolUseBlock):
            tool_msg = f"{block.name}"
            if block.name in ("Bash", "Write", "Edit"):
                snippet = json.dumps(block.input, ensure_ascii=False)
                tool_msg += f": {snippet}"
            await on_event(RuntimeEvent(type="tool_use", data=f"[{source}] {tool_msg}"))
        elif isinstance(block, ToolResultBlock) and block.is_error:
            content = block.content if isinstance(block.content, str) else str(block.content or "")
            await on_event(RuntimeEvent(type="tool_error", data=f"[{source}] {content}"))


def build_sdk_server(name: str, tool_specs: list[ToolSpec]) -> dict[str, Any]:
    sdk_tools = []
    for spec in tool_specs:
        handler = spec.handler

        @tool(spec.name, spec.description, spec.input_schema)
        async def _wrapped(args, _handler=handler):
            return await _handler(args)

        sdk_tools.append(_wrapped)

    return create_sdk_mcp_server(name, tools=sdk_tools)


async def run_turn(request: RuntimeRequest, on_event: EventSink) -> RuntimeResult:
    active_model = request.session_model if request.session_id and request.session_model else request.model
    done_event = asyncio.Event()
    mcp_server_name = "worker_tools" if request.source == "worker" else "daemon"
    mcp_servers = dict(request.plugin_mcp_servers)
    if request.tool_specs:
        mcp_servers = {mcp_server_name: build_sdk_server(mcp_server_name, request.tool_specs), **mcp_servers}

    options = ClaudeAgentOptions(
        permission_mode=request.permission_mode,
        model=active_model,
        cwd=request.project_dir,
        max_turns=request.max_turns,
        setting_sources=["project"],
        mcp_servers=mcp_servers,
    )
    if request.session_id:
        options.resume = request.session_id
    else:
        options.system_prompt = request.system_prompt

    result = RuntimeResult(session_id=request.session_id)
    async for message in query(prompt=_prompt_stream(request.prompt, done_event), options=options):
        if isinstance(message, AssistantMessage):
            await _emit_assistant_message(message, request.source, on_event)
        elif isinstance(message, SystemMessage):
            await on_event(
                RuntimeEvent(
                    type="log_update",
                    data=f"[{request.source} system] {message.subtype}",
                )
            )
        elif isinstance(message, ResultMessage):
            result.final_text = message.result or ""
            result.session_id = (message.session_id or "").strip() or result.session_id
            result.saw_result = True
            done_event.set()

    done_event.set()
    return result
