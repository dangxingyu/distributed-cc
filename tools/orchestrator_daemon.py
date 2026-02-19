#!/usr/bin/env python3
"""Orchestrator daemon — runs on each remote server as a persistent autonomous agent.

This daemon now runs a split-channel architecture per project:
- Orchestrator channel: planning, decomposition, verification, completion decisions
- Worker channel: execution of orchestrator assignments

HTTP API:
  POST /register   — register a project (project_id, project_dir, name)
  POST /task       — start autonomous work (returns immediately, runs in background)
  POST /interrupt  — inject user message (queued for next iteration)
  GET  /status     — current status (idle/running/done/stuck/error)
  GET  /stream     — SSE stream of progress events
  POST /stop       — stop current task
  GET  /health     — health check
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

from aiohttp import ClientSession, ClientTimeout, web

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query
from claude_agent_sdk.types import (
    AssistantMessage,
    PermissionResultAllow,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
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


# -- split-role prompts ------------------------------------------------

ORCHESTRATOR_PROMPT = """\
You are the ORCHESTRATOR (PhD student role). You do planning, decomposition,
verification, and decision making. You do NOT execute implementation work directly.

You also maintain a task list — your research plan for the overall task.
Include a [TASK_LIST] block in your response to update it:

[TASK_LIST]
- [x] Completed item
- [ ] Pending item
- [ ] Another pending item
[/TASK_LIST]

Update this every turn — mark items done, add new ones as you learn more.
This is your PhD-level research plan, not micro-implementation steps.

At the end of every response, choose exactly one:

1) [ASSIGN_WORKER]
WorkerTask: <clear executable instruction>

2) [TASK_COMPLETE]
Summary: <why the overall user task is complete>

3) [NEED_USER_INPUT]
Question: <specific blocking question>

Rules:
- Use worker reports as evidence.
- If evidence is insufficient, assign a refined worker task.
- Do not output multiple terminal markers.
"""


WORKER_PROMPT = """\
You are a WORKER agent. Execute the orchestrator assignment end-to-end.
Focus on concrete actions and evidence.

End with:
[WORKER_REPORT]
Summary: <what you changed/checked and outcomes>

If blocked, still use [WORKER_REPORT] and describe blocker + attempts.
Do not decide overall user-task completion.
"""


# -- data models -------------------------------------------------------


@dataclass
class Project:
    project_id: str
    project_dir: str
    name: str = ""


@dataclass
class ProgressEvent:
    """An event streamed to SSE subscribers and the router callback."""

    type: str
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    data: str = ""
    iteration: int = 0
    timestamp: float = field(default_factory=time.time)

    def to_sse(self) -> str:
        payload = json.dumps(
            {
                "event_id": self.event_id,
                "type": self.type,
                "data": self.data,
                "iteration": self.iteration,
                "ts": self.timestamp,
            }
        )
        return f"data: {payload}\n\n"


@dataclass
class TaskState:
    """Tracks the state of a running task for a project."""

    task_id: str
    project_id: str
    task_text: str
    status: str = "running"  # running, done, stuck, error, stopped
    iteration: int = 0
    max_iterations: int = MAX_ITERATIONS
    sdk_session_id: str = ""  # backward-compat alias of orchestrator_session_id
    orchestrator_session_id: str = ""
    worker_session_id: str = ""
    started_at: float = field(default_factory=time.time)
    finished_at: float = 0
    summary: str = ""
    error: str = ""


# -- daemon state ------------------------------------------------------

projects: dict[str, Project] = {}
task_states: dict[str, TaskState] = {}
running_tasks: dict[str, asyncio.Task] = {}
interrupt_queues: dict[str, asyncio.Queue] = {}
cancel_events: dict[str, asyncio.Event] = {}
sse_subscribers: dict[str, list[asyncio.Queue]] = {}

orchestrator_sessions: dict[str, str] = {}  # project_id -> orchestrator session
worker_sessions: dict[str, str] = {}  # project_id -> worker session


# -- progress streaming ------------------------------------------------


async def emit_progress(project_id: str, event: ProgressEvent):
    """Send progress event to SSE subscribers and HTTP callback."""
    for q in sse_subscribers.get(project_id, []):
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            pass

    try:
        async with ClientSession(timeout=ClientTimeout(total=5)) as http:
            await http.post(
                f"{CALLBACK_URL}/progress",
                json={
                    "project_id": project_id,
                    "daemon_name": DAEMON_NAME,
                    "event": {
                        "event_id": event.event_id,
                        "type": event.type,
                        "data": event.data,
                        "iteration": event.iteration,
                        "ts": event.timestamp,
                    },
                },
            )
    except Exception:
        pass


# -- agent sdk helpers -------------------------------------------------


async def _prompt_stream(text: str, done: asyncio.Event | None = None):
    """Wrap a string prompt into an AsyncIterable for Agent SDK streaming mode."""
    yield {
        "type": "user",
        "session_id": "",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
    }
    if done is not None:
        await done.wait()


def _make_can_use_tool():
    """Auto-approve tools in daemon-owned sessions."""

    async def can_use_tool(tool_name: str, input_data: dict, context=None):
        return PermissionResultAllow()

    return can_use_tool


# -- ralph split loop --------------------------------------------------


async def _run_worker_turn(
    project_id: str,
    assignment: str,
    iteration: int,
    worker_session_id: str = "",
) -> tuple[str, str]:
    """Execute one worker assignment in an independent worker session."""
    project = projects.get(project_id)
    if not project:
        return "Worker failed: unknown project.", worker_session_id

    options = ClaudeAgentOptions(
        can_use_tool=_make_can_use_tool(),
        model="claude-opus-4-6",
        cwd=project.project_dir,
        allowed_tools=[
            "Read",
            "Write",
            "Edit",
            "Bash",
            "Glob",
            "Grep",
            "WebSearch",
            "WebFetch",
            "NotebookEdit",
        ],
        sandbox={"enabled": False},
    )

    if worker_session_id:
        options.resume = worker_session_id
    else:
        options.system_prompt = WORKER_PROMPT

    prompt = (
        "[WORKER_TASK]\n"
        f"{assignment}\n\n"
        "Execute this assignment and provide evidence. End with [WORKER_REPORT]."
    )

    result_text = ""
    done_event = asyncio.Event()

    try:
        async for message in query(prompt=_prompt_stream(prompt, done_event), options=options):
            if isinstance(message, AssistantMessage):
                await _forward_assistant_message(
                    project_id, message, iteration, source="worker"
                )
            elif isinstance(message, ResultMessage):
                result_text = message.result or ""
                worker_session_id = message.session_id
                done_event.set()
    except Exception as e:
        log.exception("Worker SDK error on iteration %s: %s", iteration, e)
        await emit_progress(
            project_id,
            ProgressEvent(
                type="tool_error",
                data=f"[worker] SDK error: {e}",
                iteration=iteration,
            ),
        )
        return f"Worker failed: {e}", worker_session_id

    report = _extract_after_marker(result_text, "[WORKER_REPORT]")
    if not report:
        report = result_text.strip()[:4000] or "Worker returned empty report."

    return report, worker_session_id


async def run_task(project_id: str, task_text: str, max_iterations: int = MAX_ITERATIONS):
    """Supervisor RALPH loop with split channels: orchestrator + worker."""
    os.environ.pop("CLAUDECODE", None)

    project = projects.get(project_id)
    if not project:
        log.error("Unknown project: %s", project_id)
        return

    task_id = uuid.uuid4().hex[:12]
    state = TaskState(
        task_id=task_id,
        project_id=project_id,
        task_text=task_text,
        max_iterations=max_iterations,
    )
    task_states[project_id] = state

    if project_id not in interrupt_queues:
        interrupt_queues[project_id] = asyncio.Queue()
    if project_id not in cancel_events:
        cancel_events[project_id] = asyncio.Event()
    cancel_events[project_id].clear()

    orchestrator_session_id = orchestrator_sessions.get(project_id, "")
    worker_session_id = worker_sessions.get(project_id, "")
    feedback = ""
    worker_report = ""

    await emit_progress(
        project_id,
        ProgressEvent(type="iteration", data=f"Starting task: {task_text[:200]}", iteration=0),
    )

    try:
        while state.iteration < max_iterations:
            if cancel_events[project_id].is_set():
                state.status = "stopped"
                state.summary = "Stopped by user"
                state.finished_at = time.time()
                await emit_progress(
                    project_id,
                    ProgressEvent(type="done", data="Task stopped by user", iteration=state.iteration),
                )
                break

            state.iteration += 1
            user_msgs = _drain_interruptions(project_id)

            prompt = _build_prompt(
                task_text=task_text,
                feedback=feedback,
                user_msgs=user_msgs,
                iteration=state.iteration,
                worker_report=worker_report,
                task_list=_load_task_list(project_id),
            )
            # Reset after consuming so stale reports don't leak into future iterations
            worker_report = ""
            feedback = ""

            await emit_progress(
                project_id,
                ProgressEvent(
                    type="iteration",
                    data=f"Orchestrator iteration {state.iteration}/{max_iterations}",
                    iteration=state.iteration,
                ),
            )

            options = ClaudeAgentOptions(
                can_use_tool=_make_can_use_tool(),
                model="claude-opus-4-6",
                cwd=project.project_dir,
                allowed_tools=["Read", "Glob", "Grep", "WebSearch", "WebFetch"],
                sandbox={"enabled": False},
            )

            if orchestrator_session_id:
                options.resume = orchestrator_session_id
            else:
                options.system_prompt = ORCHESTRATOR_PROMPT

            result_text = ""
            done_event = asyncio.Event()

            try:
                async for message in query(prompt=_prompt_stream(prompt, done_event), options=options):
                    if isinstance(message, AssistantMessage):
                        await _forward_assistant_message(
                            project_id, message, state.iteration, source="orchestrator"
                        )
                    elif isinstance(message, ResultMessage):
                        result_text = message.result or ""
                        orchestrator_session_id = message.session_id
                        orchestrator_sessions[project_id] = orchestrator_session_id
                        done_event.set()
            except Exception as e:
                log.exception("Orchestrator SDK error on iteration %s: %s", state.iteration, e)
                state.status = "error"
                state.error = str(e)
                state.finished_at = time.time()
                await emit_progress(
                    project_id,
                    ProgressEvent(
                        type="error",
                        data=f"Orchestrator SDK error: {e}",
                        iteration=state.iteration,
                    ),
                )
                break

            state.orchestrator_session_id = orchestrator_session_id
            state.worker_session_id = worker_session_id
            state.sdk_session_id = orchestrator_session_id

            # Extract and persist task list if present
            task_list_content = _extract_task_list(result_text)
            if task_list_content is not None:
                _save_task_list(project_id, task_list_content)
                await emit_progress(
                    project_id,
                    ProgressEvent(
                        type="task_list",
                        data=task_list_content,
                        iteration=state.iteration,
                    ),
                )

            _save_state(
                state,
                orchestrator_session_id=orchestrator_session_id,
                worker_session_id=worker_session_id,
            )

            if "[TASK_COMPLETE]" in result_text:
                state.status = "done"
                state.summary = _extract_after_marker(result_text, "[TASK_COMPLETE]")
                state.finished_at = time.time()
                await emit_progress(
                    project_id,
                    ProgressEvent(type="done", data=state.summary, iteration=state.iteration),
                )
                break

            if "[NEED_USER_INPUT]" in result_text:
                state.status = "stuck"
                question = _extract_after_marker(result_text, "[NEED_USER_INPUT]")
                state.summary = f"Needs input: {question}"
                await emit_progress(
                    project_id,
                    ProgressEvent(type="stuck", data=question, iteration=state.iteration),
                )
                try:
                    user_input = await asyncio.wait_for(interrupt_queues[project_id].get(), timeout=600)
                    state.status = "running"
                    feedback = f"User responded: {user_input}"
                    continue
                except asyncio.TimeoutError:
                    state.status = "stuck"
                    state.finished_at = time.time()
                    await emit_progress(
                        project_id,
                        ProgressEvent(
                            type="stuck",
                            data="No user response (timeout)",
                            iteration=state.iteration,
                        ),
                    )
                    break

            if "[ASSIGN_WORKER]" in result_text:
                assignment = _extract_after_marker(result_text, "[ASSIGN_WORKER]")
                if not assignment:
                    feedback = (
                        "You returned [ASSIGN_WORKER] without a concrete assignment. "
                        "Provide a specific WorkerTask."
                    )
                    continue

                await emit_progress(
                    project_id,
                    ProgressEvent(
                        type="tool_use",
                        data=f"[orchestrator -> worker] {assignment[:400]}",
                        iteration=state.iteration,
                    ),
                )
                await emit_progress(
                    project_id,
                    ProgressEvent(
                        type="text",
                        data=f"@orchestrator -> @worker: {assignment[:1500]}",
                        iteration=state.iteration,
                    ),
                )

                worker_report, worker_session_id = await _run_worker_turn(
                    project_id=project_id,
                    assignment=assignment,
                    iteration=state.iteration,
                    worker_session_id=worker_session_id,
                )
                worker_sessions[project_id] = worker_session_id
                state.worker_session_id = worker_session_id

                await emit_progress(
                    project_id,
                    ProgressEvent(
                        type="text",
                        data=f"@worker -> @orchestrator: {worker_report[:1800]}",
                        iteration=state.iteration,
                    ),
                )

                feedback = (
                    "Worker report received (see [LATEST_WORKER_REPORT] below). "
                    "Verify this against the original goal. Then either assign another worker task, "
                    "ask user input, or declare completion."
                )
                _save_state(
                    state,
                    orchestrator_session_id=orchestrator_session_id,
                    worker_session_id=worker_session_id,
                )
                continue

            feedback = (
                "No valid terminal marker found. End with exactly one of: "
                "[ASSIGN_WORKER], [TASK_COMPLETE], [NEED_USER_INPUT]."
            )

        else:
            state.status = "done"
            state.summary = f"Reached max iterations ({max_iterations})"
            state.finished_at = time.time()
            await emit_progress(
                project_id,
                ProgressEvent(type="done", data=state.summary, iteration=state.iteration),
            )

    except asyncio.CancelledError:
        state.status = "stopped"
        state.summary = "Task cancelled"
        state.finished_at = time.time()
        await emit_progress(
            project_id,
            ProgressEvent(type="done", data="Task cancelled", iteration=state.iteration),
        )
    except Exception as e:
        log.exception("Supervisor RALPH loop error: %s", e)
        state.status = "error"
        state.error = str(e)
        state.finished_at = time.time()
        await emit_progress(
            project_id,
            ProgressEvent(type="error", data=str(e), iteration=state.iteration),
        )
    finally:
        running_tasks.pop(project_id, None)
        _save_state(
            state,
            orchestrator_session_id=orchestrator_session_id,
            worker_session_id=worker_session_id,
        )


def _build_prompt(
    task_text: str,
    feedback: str,
    user_msgs: list[str],
    iteration: int,
    worker_report: str = "",
    task_list: str = "",
) -> str:
    """Build orchestrator prompt for an iteration."""
    parts = []

    if task_list:
        parts.append(f"[CURRENT_TASK_LIST]\n{task_list}")

    if iteration == 1:
        parts.append(f"[TASK]\n{task_text}")
    else:
        parts.append(f"[CONTINUATION — iteration {iteration}]")

    if feedback:
        parts.append(f"\n[SUPERVISOR_FEEDBACK]\n{feedback}")

    if worker_report:
        parts.append(f"\n[LATEST_WORKER_REPORT]\n{worker_report}")

    if user_msgs:
        parts.append("\n[USER INTERRUPTIONS]")
        for msg in user_msgs:
            parts.append(f"- {msg}")

    parts.append(
        "\nChoose exactly one marker this turn: [ASSIGN_WORKER], [TASK_COMPLETE], or [NEED_USER_INPUT]."
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
    """Extract all text after a marker until the next marker or end of text.

    Strips optional label prefixes (Summary:, Question:, WorkerTask:) from
    the first line, then captures everything until a line starting with '['.
    """
    idx = text.find(marker)
    if idx < 0:
        return ""

    rest = text[idx + len(marker) :].strip()
    if not rest:
        return ""

    lines = rest.split("\n")
    result_lines = []
    for line in lines:
        stripped = line.strip()
        # Stop at the next marker (e.g. [TASK_COMPLETE], [ASSIGN_WORKER])
        if stripped.startswith("[") and stripped.endswith("]") and len(stripped) > 2:
            break
        if not result_lines:
            # Strip optional label prefix on first content line
            for prefix in ("Summary:", "Question:", "WorkerTask:"):
                if stripped.startswith(prefix):
                    stripped = stripped[len(prefix) :].strip()
                    break
        result_lines.append(stripped)

    # Drop leading/trailing empty lines
    while result_lines and not result_lines[0]:
        result_lines.pop(0)
    while result_lines and not result_lines[-1]:
        result_lines.pop()

    return "\n".join(result_lines) if result_lines else ""


def _extract_task_list(text: str) -> str | None:
    """Extract content between [TASK_LIST] and [/TASK_LIST]."""
    start = text.find("[TASK_LIST]")
    end = text.find("[/TASK_LIST]")
    if start < 0 or end < 0 or end <= start:
        return None
    return text[start + len("[TASK_LIST]") : end].strip()


def _save_task_list(project_id: str, content: str):
    """Write task list to {project_dir}/.task_list.md."""
    project = projects.get(project_id)
    if not project:
        return
    path = Path(project.project_dir) / ".task_list.md"
    path.write_text(content)


def _load_task_list(project_id: str) -> str:
    """Read task list from {project_dir}/.task_list.md."""
    project = projects.get(project_id)
    if not project:
        return ""
    path = Path(project.project_dir) / ".task_list.md"
    if not path.exists():
        return ""
    return path.read_text().strip()


async def _forward_assistant_message(
    project_id: str,
    message: AssistantMessage,
    iteration: int,
    source: str,
):
    """Forward intermediate assistant output as progress events."""
    for block in message.content:
        if isinstance(block, TextBlock):
            text = block.text.strip()
            if text:
                await emit_progress(
                    project_id,
                    ProgressEvent(
                        type="text",
                        data=f"[{source}] {text[:2000]}",
                        iteration=iteration,
                    ),
                )
        elif isinstance(block, ToolUseBlock):
            tool_msg = f"{block.name}"
            if block.name in ("Bash", "Write", "Edit"):
                snippet = json.dumps(block.input, ensure_ascii=False)[:300]
                tool_msg += f": {snippet}"
            await emit_progress(
                project_id,
                ProgressEvent(
                    type="tool_use",
                    data=f"[{source}] {tool_msg}",
                    iteration=iteration,
                ),
            )
        elif isinstance(block, ToolResultBlock):
            if block.is_error:
                content = block.content if isinstance(block.content, str) else str(block.content or "")
                await emit_progress(
                    project_id,
                    ProgressEvent(
                        type="tool_error",
                        data=f"[{source}] {content[:500]}",
                        iteration=iteration,
                    ),
                )


# -- state persistence -------------------------------------------------


def _save_state(
    state: TaskState,
    orchestrator_session_id: str = "",
    worker_session_id: str = "",
):
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
        "sdk_session_id": orchestrator_session_id,
        "orchestrator_session_id": orchestrator_session_id,
        "worker_session_id": worker_session_id,
        "started_at": state.started_at,
        "finished_at": state.finished_at,
        "summary": state.summary,
        "error": state.error,
        "task_list": _load_task_list(state.project_id),
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


# -- http handlers -----------------------------------------------------


async def handle_register(request: web.Request) -> web.Response:
    """POST /register — register a project."""
    data = await request.json()
    project_id = data.get("project_id")
    project_dir = data.get("project_dir")
    if not project_id or not project_dir:
        return web.json_response({"error": "project_id and project_dir required"}, status=400)

    name = data.get("name", project_id)
    projects[project_id] = Project(project_id=project_id, project_dir=project_dir, name=name)
    log.info("Registered project %s: %s", project_id, project_dir)
    return web.json_response({"ok": True, "project_id": project_id})


async def handle_task(request: web.Request) -> web.Response:
    """POST /task — start autonomous work on a task. Returns immediately."""
    data = await request.json()
    project_id = data.get("project_id")
    task_text = data.get("task")
    max_iter = data.get("max_iterations", MAX_ITERATIONS)

    if not project_id or not task_text:
        return web.json_response({"error": "project_id and task required"}, status=400)

    if project_id not in projects:
        return web.json_response(
            {"error": f"Unknown project: {project_id}. Register first."}, status=404
        )

    if project_id in running_tasks and not running_tasks[project_id].done():
        return web.json_response(
            {
                "error": (
                    f"Project {project_id} already has a running task. "
                    "Use /interrupt or /stop first."
                )
            },
            status=409,
        )

    task = asyncio.create_task(run_task(project_id, task_text, max_iter))
    running_tasks[project_id] = task

    return web.json_response({"ok": True, "project_id": project_id, "status": "started"})


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

    log.info("Interrupt queued for %s: %s", project_id, message[:100])
    return web.json_response({"ok": True, "queued": True})


async def handle_status(request: web.Request) -> web.Response:
    """GET /status — current status of a project's task."""
    project_id = request.query.get("project_id")
    if not project_id:
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

    return web.json_response(
        {
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
        }
    )


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
                await response.write(b": keepalive\n\n")
            except (ConnectionError, ConnectionResetError):
                break
    finally:
        subscribers = sse_subscribers.get(project_id, [])
        if queue in subscribers:
            subscribers.remove(queue)

    return response


async def handle_stop(request: web.Request) -> web.Response:
    """POST /stop — stop a running task."""
    data = await request.json()
    project_id = data.get("project_id")

    if not project_id:
        return web.json_response({"error": "project_id required"}, status=400)

    if project_id in cancel_events:
        cancel_events[project_id].set()

    task = running_tasks.get(project_id)
    if task and not task.done():
        task.cancel()
        return web.json_response({"ok": True, "status": "stopping"})

    return web.json_response({"ok": False, "reason": "No running task"})


async def handle_health(request: web.Request) -> web.Response:
    """GET /health — health check."""
    return web.json_response(
        {
            "status": "ok",
            "daemon": DAEMON_NAME,
            "projects": list(projects.keys()),
            "running": [pid for pid, t in running_tasks.items() if not t.done()],
        }
    )


# -- utility -----------------------------------------------------------


def _safe_serialize(obj) -> dict:
    """Best-effort JSON-safe conversion."""
    try:
        json.dumps(obj)
        return obj
    except (TypeError, ValueError):
        return {"raw": str(obj)}


# -- main --------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Orchestrator daemon")
    parser.add_argument("--port", type=int, default=8200)
    parser.add_argument("--name", default=os.environ.get("DAEMON_NAME", "unknown"))
    parser.add_argument(
        "--callback-url",
        default=os.environ.get("CALLBACK_URL", "http://127.0.0.1:9120"),
    )
    args = parser.parse_args()

    global DAEMON_NAME, CALLBACK_URL
    DAEMON_NAME = args.name
    CALLBACK_URL = args.callback_url
    os.environ["DAEMON_NAME"] = args.name

    log.info(
        "Daemon starting: name=%s, port=%s, callback=%s",
        args.name,
        args.port,
        args.callback_url,
    )

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
