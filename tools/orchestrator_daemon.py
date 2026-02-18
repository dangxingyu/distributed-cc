#!/usr/bin/env python3
"""Orchestrator daemon — runs on each remote server as a persistent autonomous agent.

Evolves from remote_broker.py. The core change: replaces one-shot /run with an
autonomous RALPH loop (Reason → Act → Learn → Plan → Hypothesize).

The daemon runs a Claude Agent SDK session that autonomously works on tasks:
  build_prompt → query() → evaluate → repeat/done

Uses the Claude Agent SDK directly for tool use (Read, Write, Bash, Grep, etc.)
instead of delegating to one-shot workers.

HTTP API:
  POST /register   — register a project (project_id, project_dir, name)
  POST /task       — start autonomous work (returns immediately, runs in background)
  POST /interrupt  — inject user message (queued for next iteration)
  GET  /status     — current status (idle/running/done/stuck/error)
  GET  /stream     — SSE stream of progress events
  POST /stop       — stop current task
  GET  /health     — health check

Deploy via: tools/deploy.sh user@host
"""

import argparse
import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from aiohttp import web, ClientSession, ClientTimeout

from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage
from claude_agent_sdk.types import (
    AssistantMessage,
    PermissionResultAllow,
    TextBlock,
    ToolUseBlock,
    ToolResultBlock,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("daemon")

DAEMON_NAME = os.environ.get("DAEMON_NAME", "unknown")
CALLBACK_URL = os.environ.get("CALLBACK_URL", "http://127.0.0.1:9120")
MAX_ITERATIONS = 20
STATE_DIR = Path.home() / ".distributed-cc" / "state"


# ── PhD Student System Prompt ─────────────────────────────────────────

PHD_STUDENT_PROMPT = """\
You are an autonomous research assistant (like a PhD student) working on a project.
You receive tasks from your advisor (the user) and work on them independently.

=== CORE PRINCIPLES ===

**AUTONOMOUS**: You don't wait for permission for routine work. Read files, run tests,
write code, check logs — just do it. Only pause when you genuinely need the user's input.

**THOROUGH**: Before making changes, understand the codebase. Read relevant files, check
existing patterns, run tests before AND after changes. Don't guess — verify.

**HONEST**: If you're stuck, say so clearly. Don't pretend to have succeeded when you haven't.
Don't keep retrying the same failing approach.

**EFFICIENT**: Do the work, report results. Don't narrate your thought process excessively.
Focus on outcomes, not process description.

**PERSISTENT**: Try alternative approaches before giving up. If one path doesn't work,
think about why and try something different. Exhaust reasonable options before escalating.

=== COMPLETION MARKERS ===

When you have COMPLETED the task successfully, end your response with:
[TASK_COMPLETE]
Summary: <brief summary of what was done>

When you are genuinely STUCK and need the user's input to proceed, end with:
[NEED_USER_INPUT]
Question: <specific question for the user>

If neither marker is present, you will receive a continuation prompt to keep working.
Always include exactly one of these markers when appropriate.

=== WORKING STYLE ===

- Start by understanding the task and relevant code
- Make a plan, then execute it
- Test your changes (run tests, verify behavior)
- If tests fail, debug and fix before declaring done
- Commit logical units of work when appropriate
- Report what you did, what worked, what didn't
"""


# ── Data Models ───────────────────────────────────────────────────────

@dataclass
class Project:
    project_id: str
    project_dir: str
    name: str = ""


@dataclass
class ProgressEvent:
    """An event streamed to SSE subscribers and the router callback."""
    type: str           # text, tool_use, tool_result, tool_error, iteration, done, stuck, error
    data: str = ""
    iteration: int = 0
    timestamp: float = field(default_factory=time.time)

    def to_sse(self) -> str:
        payload = json.dumps({
            "type": self.type,
            "data": self.data,
            "iteration": self.iteration,
            "ts": self.timestamp,
        })
        return f"data: {payload}\n\n"


@dataclass
class TaskState:
    """Tracks the state of a running task for a project."""
    task_id: str
    project_id: str
    task_text: str
    status: str = "running"     # running, done, stuck, error, stopped
    iteration: int = 0
    max_iterations: int = MAX_ITERATIONS
    sdk_session_id: str = ""
    started_at: float = field(default_factory=time.time)
    finished_at: float = 0
    summary: str = ""
    error: str = ""


# ── Daemon State ──────────────────────────────────────────────────────

projects: dict[str, Project] = {}
task_states: dict[str, TaskState] = {}      # project_id → TaskState
running_tasks: dict[str, asyncio.Task] = {} # project_id → asyncio.Task
interrupt_queues: dict[str, asyncio.Queue] = {}  # project_id → Queue[str]
cancel_events: dict[str, asyncio.Event] = {}     # project_id → Event
sse_subscribers: dict[str, list[asyncio.Queue]] = {}  # project_id → [Queue[ProgressEvent]]

# Per-project SDK session IDs (persist across tasks for context)
sdk_sessions: dict[str, str] = {}  # project_id → sdk_session_id


# ── Progress Streaming ────────────────────────────────────────────────

async def emit_progress(project_id: str, event: ProgressEvent):
    """Send progress event to all SSE subscribers and HTTP callback."""
    # SSE subscribers
    for q in sse_subscribers.get(project_id, []):
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            pass  # drop if subscriber is slow

    # HTTP callback to router
    try:
        async with ClientSession(timeout=ClientTimeout(total=5)) as http:
            await http.post(
                f"{CALLBACK_URL}/progress",
                json={
                    "project_id": project_id,
                    "daemon_name": DAEMON_NAME,
                    "event": {
                        "type": event.type,
                        "data": event.data,
                        "iteration": event.iteration,
                        "ts": event.timestamp,
                    },
                },
            )
    except Exception:
        pass  # best-effort


# ── Agent SDK Helpers ─────────────────────────────────────────────────

async def _prompt_stream(text: str, done: asyncio.Event | None = None):
    """Wrap a string prompt into an AsyncIterable for the Agent SDK.

    Required when can_use_tool is set — the SDK needs streaming mode.
    When `done` is provided, keeps the stream alive until the event is set.
    """
    yield {
        "type": "user",
        "session_id": "",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
    }
    if done is not None:
        await done.wait()


def _make_can_use_tool():
    """Create a can_use_tool callback that auto-approves everything."""

    async def can_use_tool(tool_name: str, input_data: dict, context=None):
        return PermissionResultAllow()

    return can_use_tool


# ── RALPH Loop ────────────────────────────────────────────────────────

async def run_task(project_id: str, task_text: str, max_iterations: int = MAX_ITERATIONS):
    """The autonomous RALPH loop: Reason → Act → Learn → Plan → Hypothesize.

    Runs the Agent SDK in a loop, evaluating markers after each turn.
    User interruptions are drained at iteration boundaries and injected into the prompt.
    """
    os.environ.pop("CLAUDECODE", None)

    project = projects.get(project_id)
    if not project:
        log.error(f"Unknown project: {project_id}")
        return

    task_id = uuid.uuid4().hex[:12]
    state = TaskState(
        task_id=task_id,
        project_id=project_id,
        task_text=task_text,
        max_iterations=max_iterations,
    )
    task_states[project_id] = state

    # Ensure interrupt queue and cancel event exist
    if project_id not in interrupt_queues:
        interrupt_queues[project_id] = asyncio.Queue()
    if project_id not in cancel_events:
        cancel_events[project_id] = asyncio.Event()
    cancel_events[project_id].clear()

    session_id = sdk_sessions.get(project_id)
    feedback = ""

    await emit_progress(project_id, ProgressEvent(
        type="iteration", data=f"Starting task: {task_text[:200]}", iteration=0,
    ))

    try:
        while state.iteration < max_iterations:
            # Check for cancellation
            if cancel_events[project_id].is_set():
                state.status = "stopped"
                state.summary = "Stopped by user"
                state.finished_at = time.time()
                await emit_progress(project_id, ProgressEvent(
                    type="done", data="Task stopped by user",
                    iteration=state.iteration,
                ))
                break

            state.iteration += 1

            # Drain pending interruptions
            user_msgs = _drain_interruptions(project_id)

            # Build prompt
            prompt = _build_prompt(task_text, feedback, user_msgs, state.iteration)

            await emit_progress(project_id, ProgressEvent(
                type="iteration",
                data=f"Iteration {state.iteration}/{max_iterations}",
                iteration=state.iteration,
            ))

            # One Agent SDK turn
            options = ClaudeAgentOptions(
                can_use_tool=_make_can_use_tool(),
                model="claude-opus-4-6",
                cwd=project.project_dir,
                allowed_tools=[
                    "Read", "Write", "Edit", "Bash", "Glob", "Grep",
                    "WebSearch", "WebFetch", "NotebookEdit",
                ],
                sandbox={"enabled": False},
            )

            if session_id:
                options.resume = session_id
            else:
                options.system_prompt = PHD_STUDENT_PROMPT

            result_text = ""
            done_event = asyncio.Event()

            try:
                async for message in query(
                    prompt=_prompt_stream(prompt, done_event),
                    options=options,
                ):
                    if isinstance(message, AssistantMessage):
                        await _forward_assistant_message(project_id, message, state.iteration)
                    elif isinstance(message, ResultMessage):
                        result_text = message.result or ""
                        session_id = message.session_id
                        sdk_sessions[project_id] = session_id
                        done_event.set()
            except Exception as e:
                log.exception(f"Agent SDK error on iteration {state.iteration}: {e}")
                state.status = "error"
                state.error = str(e)
                state.finished_at = time.time()
                await emit_progress(project_id, ProgressEvent(
                    type="error", data=f"SDK error: {e}",
                    iteration=state.iteration,
                ))
                break

            # Save state
            _save_state(state, session_id)

            # Evaluate result (zero-cost: marker-based)
            if "[TASK_COMPLETE]" in result_text:
                state.status = "done"
                state.summary = _extract_after_marker(result_text, "[TASK_COMPLETE]")
                state.finished_at = time.time()
                await emit_progress(project_id, ProgressEvent(
                    type="done", data=state.summary,
                    iteration=state.iteration,
                ))
                break

            if "[NEED_USER_INPUT]" in result_text:
                state.status = "stuck"
                question = _extract_after_marker(result_text, "[NEED_USER_INPUT]")
                state.summary = f"Needs input: {question}"
                await emit_progress(project_id, ProgressEvent(
                    type="stuck", data=question,
                    iteration=state.iteration,
                ))
                # Wait for an interruption (user message)
                try:
                    user_input = await asyncio.wait_for(
                        interrupt_queues[project_id].get(),
                        timeout=600,  # 10 min timeout
                    )
                    state.status = "running"
                    feedback = f"User responded: {user_input}\n\nContinue working on the task."
                except asyncio.TimeoutError:
                    state.status = "stuck"
                    state.finished_at = time.time()
                    await emit_progress(project_id, ProgressEvent(
                        type="stuck", data="No user response (timeout)",
                        iteration=state.iteration,
                    ))
                    break
            else:
                # No marker — continue working
                feedback = "Continue working toward the task goal."

        else:
            # Max iterations reached
            state.status = "done"
            state.summary = f"Reached max iterations ({max_iterations})"
            state.finished_at = time.time()
            await emit_progress(project_id, ProgressEvent(
                type="done", data=state.summary,
                iteration=state.iteration,
            ))

    except asyncio.CancelledError:
        state.status = "stopped"
        state.summary = "Task cancelled"
        state.finished_at = time.time()
        await emit_progress(project_id, ProgressEvent(
            type="done", data="Task cancelled",
            iteration=state.iteration,
        ))
    except Exception as e:
        log.exception(f"RALPH loop error: {e}")
        state.status = "error"
        state.error = str(e)
        state.finished_at = time.time()
        await emit_progress(project_id, ProgressEvent(
            type="error", data=str(e),
            iteration=state.iteration,
        ))
    finally:
        running_tasks.pop(project_id, None)
        _save_state(state, session_id)


def _build_prompt(
    task_text: str,
    feedback: str,
    user_msgs: list[str],
    iteration: int,
) -> str:
    """Build the prompt for an iteration of the RALPH loop."""
    parts = []

    if iteration == 1:
        parts.append(f"[TASK]\n{task_text}")
    else:
        parts.append(f"[CONTINUATION — iteration {iteration}]")
        if feedback:
            parts.append(f"\n{feedback}")

    if user_msgs:
        parts.append("\n[USER INTERRUPTIONS]")
        for msg in user_msgs:
            parts.append(f"- {msg}")
        parts.append("")

    if iteration > 1 and not user_msgs:
        parts.append(
            "\nRemember: emit [TASK_COMPLETE] when done, or [NEED_USER_INPUT] if stuck."
        )

    return "\n".join(parts)


def _drain_interruptions(project_id: str) -> list[str]:
    """Drain all pending interruption messages for a project."""
    queue = interrupt_queues.get(project_id)
    if not queue:
        return []
    msgs = []
    while not queue.empty():
        try:
            msgs.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            break
    return msgs


def _extract_after_marker(text: str, marker: str) -> str:
    """Extract text after a marker line."""
    idx = text.find(marker)
    if idx < 0:
        return ""
    rest = text[idx + len(marker):].strip()
    # Take the first meaningful line
    for line in rest.split("\n"):
        line = line.strip()
        if line and not line.startswith("["):
            # Strip common prefixes like "Summary: " or "Question: "
            for prefix in ("Summary:", "Question:"):
                if line.startswith(prefix):
                    line = line[len(prefix):].strip()
            return line
    return rest[:200] if rest else ""


async def _forward_assistant_message(
    project_id: str, message: AssistantMessage, iteration: int,
):
    """Forward intermediate assistant output as progress events."""
    for block in message.content:
        if isinstance(block, TextBlock):
            text = block.text.strip()
            if text:
                await emit_progress(project_id, ProgressEvent(
                    type="text", data=text[:2000],
                    iteration=iteration,
                ))
        elif isinstance(block, ToolUseBlock):
            tool_msg = f"{block.name}"
            if block.name in ("Bash", "Write", "Edit"):
                snippet = json.dumps(block.input, ensure_ascii=False)[:300]
                tool_msg += f": {snippet}"
            await emit_progress(project_id, ProgressEvent(
                type="tool_use", data=tool_msg,
                iteration=iteration,
            ))
        elif isinstance(block, ToolResultBlock):
            if block.is_error:
                content = block.content if isinstance(block.content, str) else str(block.content or "")
                await emit_progress(project_id, ProgressEvent(
                    type="tool_error", data=content[:500],
                    iteration=iteration,
                ))


# ── State Persistence ─────────────────────────────────────────────────

def _save_state(state: TaskState, session_id: str = ""):
    """Persist task state to disk for crash recovery."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    path = STATE_DIR / f"{state.project_id}.json"
    data = {
        "task_id": state.task_id,
        "project_id": state.project_id,
        "task_text": state.task_text,
        "status": state.status,
        "iteration": state.iteration,
        "max_iterations": state.max_iterations,
        "sdk_session_id": session_id,
        "started_at": state.started_at,
        "finished_at": state.finished_at,
        "summary": state.summary,
        "error": state.error,
    }
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, str(path))


def _load_state(project_id: str) -> dict | None:
    """Load persisted state for a project."""
    path = STATE_DIR / f"{project_id}.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


# ── HTTP Handlers ─────────────────────────────────────────────────────

async def handle_register(request: web.Request) -> web.Response:
    """POST /register — register a project."""
    data = await request.json()
    project_id = data.get("project_id")
    project_dir = data.get("project_dir")
    if not project_id or not project_dir:
        return web.json_response(
            {"error": "project_id and project_dir required"}, status=400
        )

    name = data.get("name", project_id)
    projects[project_id] = Project(
        project_id=project_id,
        project_dir=project_dir,
        name=name,
    )
    log.info(f"Registered project {project_id}: {project_dir}")
    return web.json_response({"ok": True, "project_id": project_id})


async def handle_task(request: web.Request) -> web.Response:
    """POST /task — start autonomous work on a task. Returns immediately."""
    data = await request.json()
    project_id = data.get("project_id")
    task_text = data.get("task")
    max_iter = data.get("max_iterations", MAX_ITERATIONS)

    if not project_id or not task_text:
        return web.json_response(
            {"error": "project_id and task required"}, status=400
        )

    if project_id not in projects:
        return web.json_response(
            {"error": f"Unknown project: {project_id}. Register first."}, status=404
        )

    # Check if already running
    if project_id in running_tasks and not running_tasks[project_id].done():
        return web.json_response(
            {"error": f"Project {project_id} already has a running task. Use /interrupt or /stop first."},
            status=409,
        )

    # Start RALPH loop in background
    task = asyncio.create_task(run_task(project_id, task_text, max_iter))
    running_tasks[project_id] = task

    return web.json_response({
        "ok": True,
        "project_id": project_id,
        "status": "started",
    })


async def handle_interrupt(request: web.Request) -> web.Response:
    """POST /interrupt — inject a user message into the running task."""
    data = await request.json()
    project_id = data.get("project_id")
    message = data.get("message", "")

    if not project_id:
        return web.json_response({"error": "project_id required"}, status=400)

    if project_id not in interrupt_queues:
        interrupt_queues[project_id] = asyncio.Queue()
    interrupt_queues[project_id].put_nowait(message)

    log.info(f"Interrupt queued for {project_id}: {message[:100]}")
    return web.json_response({"ok": True, "queued": True})


async def handle_status(request: web.Request) -> web.Response:
    """GET /status — current status of a project's task."""
    project_id = request.query.get("project_id")
    if not project_id:
        # Return all project statuses
        result = {}
        for pid, proj in projects.items():
            state = task_states.get(pid)
            result[pid] = {
                "name": proj.name,
                "project_dir": proj.project_dir,
                "status": state.status if state else "idle",
                "iteration": state.iteration if state else 0,
                "max_iterations": state.max_iterations if state else MAX_ITERATIONS,
                "summary": state.summary if state else "",
            }
        return web.json_response(result)

    state = task_states.get(project_id)
    if not state:
        if project_id in projects:
            return web.json_response({"status": "idle", "project_id": project_id})
        return web.json_response({"error": "Unknown project"}, status=404)

    return web.json_response({
        "project_id": project_id,
        "task_id": state.task_id,
        "status": state.status,
        "iteration": state.iteration,
        "max_iterations": state.max_iterations,
        "task_text": state.task_text[:500],
        "summary": state.summary,
        "error": state.error,
        "started_at": state.started_at,
        "finished_at": state.finished_at,
    })


async def handle_stream(request: web.Request) -> web.StreamResponse:
    """GET /stream — SSE stream of progress events for a project."""
    project_id = request.query.get("project_id")
    if not project_id:
        return web.json_response({"error": "project_id required"}, status=400)

    response = web.StreamResponse(
        status=200,
        reason="OK",
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
    await response.prepare(request)

    # Subscribe
    queue: asyncio.Queue[ProgressEvent] = asyncio.Queue(maxsize=100)
    if project_id not in sse_subscribers:
        sse_subscribers[project_id] = []
    sse_subscribers[project_id].append(queue)

    try:
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=30)
                await response.write(event.to_sse().encode())
            except asyncio.TimeoutError:
                # Send keepalive
                await response.write(b": keepalive\n\n")
            except (ConnectionError, ConnectionResetError):
                break
    finally:
        sse_subscribers.get(project_id, []).remove(queue) if queue in sse_subscribers.get(project_id, []) else None

    return response


async def handle_stop(request: web.Request) -> web.Response:
    """POST /stop — stop a running task."""
    data = await request.json()
    project_id = data.get("project_id")

    if not project_id:
        return web.json_response({"error": "project_id required"}, status=400)

    # Signal cancellation
    if project_id in cancel_events:
        cancel_events[project_id].set()

    # Cancel the asyncio task
    task = running_tasks.get(project_id)
    if task and not task.done():
        task.cancel()
        return web.json_response({"ok": True, "status": "stopping"})

    return web.json_response({"ok": False, "reason": "No running task"})


async def handle_health(request: web.Request) -> web.Response:
    """GET /health — health check."""
    return web.json_response({
        "status": "ok",
        "daemon": DAEMON_NAME,
        "projects": list(projects.keys()),
        "running": [pid for pid, t in running_tasks.items() if not t.done()],
    })


# ── Utility ───────────────────────────────────────────────────────────

def _safe_serialize(obj) -> dict:
    """Best-effort JSON-safe conversion."""
    try:
        json.dumps(obj)
        return obj
    except (TypeError, ValueError):
        return {"raw": str(obj)}


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Orchestrator daemon")
    parser.add_argument("--port", type=int, default=8200)
    parser.add_argument("--name", default=os.environ.get("DAEMON_NAME", "unknown"))
    parser.add_argument("--callback-url", default=os.environ.get("CALLBACK_URL", "http://127.0.0.1:9120"))
    args = parser.parse_args()

    global DAEMON_NAME, CALLBACK_URL
    DAEMON_NAME = args.name
    CALLBACK_URL = args.callback_url
    os.environ["DAEMON_NAME"] = args.name

    log.info(f"Daemon starting: name={args.name}, port={args.port}, callback={args.callback_url}")

    app = web.Application()
    app.router.add_post("/register", handle_register)
    app.router.add_post("/task", handle_task)
    app.router.add_post("/interrupt", handle_interrupt)
    app.router.add_get("/status", handle_status)
    app.router.add_get("/stream", handle_stream)
    app.router.add_post("/stop", handle_stop)
    app.router.add_get("/health", handle_health)

    web.run_app(app, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
