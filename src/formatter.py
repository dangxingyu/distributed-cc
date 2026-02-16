"""Format Claude Code output for Telegram messages."""

import re

# Telegram message limit
MAX_MESSAGE_LEN = 4096
TRUNCATION_NOTICE = "\n\n... (truncated, reply 'full' to see more)"


def escape_markdown_v2(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    special = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", text)


def format_result(result_text: str, is_error: bool = False) -> str:
    """Format a Claude session result for Telegram.

    Returns plain text (not MarkdownV2) — Telegram's plain mode is more
    forgiving with code snippets that contain special characters.
    """
    if is_error:
        text = f"[Error] {result_text}"
    else:
        text = result_text

    return truncate(text)


def format_routing_decision(server: str, session: str, prompt: str) -> str:
    """Format a routing decision notification."""
    return (
        f">> Routing to {server}/{session}\n"
        f">> Prompt: {prompt[:200]}{'...' if len(prompt) > 200 else ''}"
    )


def format_task_status(tasks: list) -> str:
    """Format a list of running tasks."""
    if not tasks:
        return "No running tasks."
    lines = ["Running tasks:"]
    for t in tasks:
        elapsed = ""
        if t.created_at:
            import time
            elapsed = f" ({int(time.time() - t.created_at)}s ago)"
        lines.append(f"  #{t.id} [{t.server_name}/{t.session_id}]{elapsed}")
    return "\n".join(lines)


def format_sessions_list(servers: list) -> str:
    """Format the list of available servers and sessions."""
    lines = ["Available sessions:"]
    for s in servers:
        for sess in s.get("sessions", []):
            host_label = s["name"]
            lines.append(f"  {host_label}/{sess['id']} - {sess.get('description', '')}")
    return "\n".join(lines)


def format_plan_created(plan) -> str:
    """Format a notification when a work plan is created."""
    lines = [f"Plan created with {len(plan.items)} task(s):"]
    for item in plan.items:
        deps = f" (after {', '.join(item.depends_on)})" if item.depends_on else ""
        lines.append(f"  [{item.id}] {item.description}{deps}")
    return "\n".join(lines)


def format_plan_summary(plan) -> str:
    """Format a summary when a work plan completes."""
    done = sum(1 for i in plan.items if i.status == "done")
    failed = sum(1 for i in plan.items if i.status == "failed")
    total = len(plan.items)

    header = f"Plan complete: {done}/{total} tasks done"
    if failed:
        header += f", {failed} failed"

    lines = [header]
    for item in plan.items:
        icon = {"done": "+", "failed": "x", "pending": "-"}.get(item.status, "?")
        summary = ""
        if item.result:
            summary = f": {item.result[:200]}{'...' if len(item.result) > 200 else ''}"
        lines.append(f"  [{icon}] {item.id} — {item.description}{summary}")
    return truncate("\n".join(lines))


def format_task_progress(item, verdict: str) -> str:
    """Format a progress update for a single work item."""
    if verdict == "done":
        summary = item.result[:300] if item.result else "(no output)"
        return truncate(f"Task {item.id} done: {summary}")
    elif verdict == "retry":
        return f"Task {item.id} retrying ({item.retries}/{item.max_retries}): {item.feedback or '(no feedback)'}"
    elif verdict == "failed":
        return f"Task {item.id} failed: {item.feedback or item.result or '(unknown error)'}"
    return f"Task {item.id}: {verdict}"


def truncate(text: str, max_len: int = MAX_MESSAGE_LEN) -> str:
    if len(text) <= max_len:
        return text
    cut = max_len - len(TRUNCATION_NOTICE)
    return text[:cut] + TRUNCATION_NOTICE
