from __future__ import annotations

from .base import EventSink, RuntimeRequest, RuntimeResult
from .claude_backend import run_turn as run_claude_turn
from .codex_backend import run_turn as run_codex_turn


async def run_turn(provider: str, request: RuntimeRequest, on_event: EventSink) -> RuntimeResult:
    normalized = (provider or "claude").strip().lower() or "claude"
    if normalized == "claude":
        return await run_claude_turn(request, on_event)
    if normalized == "codex":
        return await run_codex_turn(request, on_event)
    raise ValueError(f"Unsupported provider: {provider}")
