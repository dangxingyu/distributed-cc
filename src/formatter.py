"""Format output for the channel-style interaction model.

Each project is like a Slack channel with user, orchestrator, and workers.
Messages are prefixed to show who's speaking and to whom.
"""

import re


def escape_markdown_v2(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    special = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", text)


def format_result(result_text: str, is_error: bool = False) -> str:
    """Format a Claude session result."""
    if is_error:
        return f"[Error] {result_text}"
    return result_text


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


# ── Channel-style formatters ──────────────────────────────────────────


def format_channel_orchestrator(text: str) -> str:
    """Format an orchestrator message in channel style."""
    return f"orchestrator: {text}"


def format_channel_worker_created(server: str, session_id: str, work_dir: str) -> str:
    """Format worker creation in channel style."""
    return f"orchestrator: Created worker {server}/{session_id} at {work_dir}"


def format_channel_dispatch(server: str, session: str, prompt: str) -> str:
    """Format orchestrator dispatching work to a worker."""
    return f"orchestrator -> {server}/{session}: {prompt}"


def format_channel_worker_result(item, verdict: str) -> str:
    """Format a worker result in channel style."""
    tag = f"{item.server}/{item.session}"
    if verdict == "done":
        summary = item.result or "(no output)"
        return f"{tag}: Done. {summary}"
    elif verdict == "retry":
        return f"orchestrator: @{tag} ({item.retries}/{item.max_retries}) {item.feedback or 'Please try again.'}"
    elif verdict == "retry_different":
        return f"orchestrator: @{tag} Trying a different approach."
    elif verdict == "escalate":
        return f"orchestrator: @{tag} Needs your input."
    elif verdict == "failed":
        return f"{tag}: Failed. {item.feedback or item.result or '(unknown error)'}"
    return f"{tag}: {verdict}"


def format_channel_plan_created(plan) -> str:
    """Format plan creation in channel style."""
    lines = [f"orchestrator: Planning {len(plan.items)} task(s):"]
    for item in plan.items:
        deps = f" (after {', '.join(item.depends_on)})" if item.depends_on else ""
        lines.append(f"  [{item.id}] {item.description} -> {item.server}/{item.session}{deps}")
    return "\n".join(lines)


def format_channel_plan_summary(plan) -> str:
    """Format plan completion in channel style."""
    done = sum(1 for i in plan.items if i.status == "done")
    failed = sum(1 for i in plan.items if i.status == "failed")
    total = len(plan.items)

    header = f"orchestrator: Plan complete — {done}/{total} done"
    if failed:
        header += f", {failed} failed"

    lines = [header]
    for item in plan.items:
        icon = {"done": "+", "failed": "x", "pending": "-"}.get(item.status, "?")
        result = ""
        if item.result:
            result = f": {item.result}"
        lines.append(f"  [{icon}] {item.id} — {item.description}{result}")
    return "\n".join(lines)
