from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable


ToolHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
EventSink = Callable[["RuntimeEvent"], Awaitable[None]]


@dataclass(slots=True)
class RuntimeEvent:
    type: str
    data: str


@dataclass(slots=True)
class ToolSpec:
    name: str
    description: str
    input_schema: dict[str, type]
    handler: ToolHandler


@dataclass(slots=True)
class RuntimeRequest:
    prompt: str
    project_dir: str
    source: str
    system_prompt: str = ""
    base_instructions: str = ""
    session_id: str = ""
    model: str = ""
    session_model: str = ""
    permission_mode: str = ""
    sandbox_mode: str = ""
    approval_policy: str = ""
    runtime_home_dir: str = ""
    plugin_mcp_servers: dict[str, dict[str, Any]] = field(default_factory=dict)
    tool_specs: list[ToolSpec] = field(default_factory=list)
    max_turns: int = 50


@dataclass(slots=True)
class RuntimeResult:
    session_id: str = ""
    final_text: str = ""
    saw_result: bool = False


def extract_tool_text(result: dict[str, Any]) -> str:
    content = result.get("content")
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text", "")))
            else:
                parts.append(str(item))
        text = "\n".join(part for part in parts if part)
        if text:
            return text
    if "text" in result:
        return str(result.get("text") or "")
    return str(result or "")
