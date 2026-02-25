"""Shared progress-event interpretation rules for frontends."""

from __future__ import annotations


def log_entries_from_event(event_type: str, data_text: str, tool_use_prefix: str = "->") -> list[str]:
    """Return log lines derived from a daemon progress event."""
    if event_type == "text":
        return [data_text] if data_text else []
    if event_type == "tool_use":
        return [f"{tool_use_prefix} {data_text}"] if data_text else []
    if event_type == "tool_error":
        return [f"[ERROR] {data_text}"] if data_text else []
    if event_type == "log_update":
        return [data_text] if data_text else []
    if event_type == "error":
        return [f"[ERROR] {data_text}"] if data_text else []
    return []


def chat_messages_from_event(event_type: str, data_text: str) -> list[tuple[str, str]]:
    """Return chat messages as ``(sender, text)`` derived from a progress event."""
    if event_type == "text":
        if data_text.startswith("@orchestrator ->"):
            clean = data_text[len("@orchestrator ->") :].strip()
            return [("orchestrator", clean)] if clean else []
        if data_text.startswith("@worker ->"):
            clean = data_text[len("@worker ->") :].strip()
            return [("worker", clean)] if clean else []
        if data_text.startswith("@orchestrator"):
            clean = data_text[len("@orchestrator") :].lstrip(" :")
            return [("orchestrator", clean or data_text)]
        if data_text.startswith("@worker"):
            clean = data_text[len("@worker") :].lstrip(" :")
            return [("worker", clean or data_text)]
        if data_text.startswith("[orchestrator]"):
            clean = data_text[len("[orchestrator]") :].strip()
            return [("orchestrator", clean)] if clean else []
        return []

    if event_type == "done" and data_text:
        return [("orchestrator", f"Task complete: {data_text}")]
    if event_type == "stopped":
        return [("orchestrator", f"Task stopped: {data_text or 'stopped by user'}")]
    if event_type == "stuck" and data_text:
        return [("orchestrator", f"Needs input: {data_text}")]
    if event_type == "error" and data_text:
        return [("orchestrator", f"Error: {data_text}")]

    return []
