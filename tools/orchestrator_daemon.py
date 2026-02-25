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
import hashlib
import json
import logging
import os
import time
import traceback
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from aiohttp import ClientSession, ClientTimeout, web

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ResultMessage,
    create_sdk_mcp_server,
    query,
    tool,
)
from claude_agent_sdk.types import (
    AssistantMessage,
    SystemMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("daemon")


def _install_sdk_event_compat() -> None:
    """Treat unknown CLI *_event payloads as SystemMessage to avoid stream aborts."""
    try:
        import claude_agent_sdk._internal.client as sdk_client_internal
        import claude_agent_sdk._internal.message_parser as sdk_message_parser
    except Exception:
        return

    parse_message_fn = getattr(sdk_message_parser, "parse_message", None)
    if not callable(parse_message_fn):
        return
    if getattr(parse_message_fn, "_dcc_event_compat", False):
        return

    def parse_message_compat(data):
        msg_type = data.get("type") if isinstance(data, dict) else None
        if isinstance(msg_type, str) and msg_type.endswith("_event"):
            return SystemMessage(subtype=msg_type, data=data)
        return parse_message_fn(data)

    setattr(parse_message_compat, "_dcc_event_compat", True)
    sdk_message_parser.parse_message = parse_message_compat
    if hasattr(sdk_client_internal, "parse_message"):
        sdk_client_internal.parse_message = parse_message_compat


_install_sdk_event_compat()


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.environ.get(name)
    try:
        value = int(raw) if raw is not None else default
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


VALID_PERMISSION_MODES = {"default", "acceptEdits", "plan", "bypassPermissions"}


def _normalize_permission_mode(value: str | None, default: str = "bypassPermissions") -> str:
    candidate = str(value or "").strip()
    if candidate in VALID_PERMISSION_MODES:
        return candidate
    return default


DAEMON_NAME = os.environ.get("DAEMON_NAME", "unknown")
CALLBACK_URL = os.environ.get("CALLBACK_URL", "http://127.0.0.1:9120")
DEFAULT_ORCHESTRATOR_MODEL = os.environ.get("ORCHESTRATOR_MODEL", "claude-opus-4-6")
DEFAULT_SESSION_MODEL = os.environ.get("ORCHESTRATOR_SESSION_MODEL", DEFAULT_ORCHESTRATOR_MODEL)
DEFAULT_PERMISSION_MODE = _normalize_permission_mode(
    os.environ.get("DCC_PERMISSION_MODE", "bypassPermissions"),
    default="bypassPermissions",
)
DEBUG_FLOW = _env_flag("DCC_DEBUG_FLOW")
EVENT_HISTORY_MAX = _env_int("EVENT_HISTORY_MAX", 2000, minimum=1)
MAX_ITERATIONS = 0
STATE_DIR = Path.home() / ".distributed-cc" / "state"
INTERRUPT_QUEUE_MAX = 100
HEARTBEAT_ENABLED = os.environ.get("HEARTBEAT_ENABLED", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
HEARTBEAT_INTERVAL_SECONDS = _env_int("HEARTBEAT_INTERVAL_SECONDS", 45, minimum=10)
HEARTBEAT_IDLE_SECONDS = _env_int("HEARTBEAT_IDLE_SECONDS", 180, minimum=30)
HEARTBEAT_GPU_HINT = os.environ.get("HEARTBEAT_GPU_HINT", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
HEARTBEAT_GPU_IDLE_UTIL = _env_int("HEARTBEAT_GPU_IDLE_UTIL", 10, minimum=0)
HEARTBEAT_GPU_IDLE_MEMORY_MB = _env_int("HEARTBEAT_GPU_IDLE_MEMORY_MB", 2048, minimum=0)
CONTINUOUS_MODE_DEFAULT = os.environ.get("CONTINUOUS_MODE", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
ROLE_CONFIG_DIR = ".claude/roles"
ORCHESTRATOR_ROLE_FILE = f"{ROLE_CONFIG_DIR}/orchestrator.md"
WORKER_ROLE_FILE = f"{ROLE_CONFIG_DIR}/worker.md"


# -- split-role prompts ------------------------------------------------

ORCH_IDENTITY = """\
You are the ORCHESTRATOR — a PhD-student-level autonomous researcher.
You receive direction from the professor (user), then independently plan,
investigate, decompose tasks, assign workers, review output, and drive to completion.
"""

ORCH_TOOLS = """\
## Your tools

Besides the standard Read/Glob/Grep/WebSearch/WebFetch for investigation, you have:

- **assign_worker(task)** — send a concrete assignment to your worker agent.
  This is your primary execution path for substantial implementation/investigation work.
  The worker has full tool access (Edit, Write, Bash, etc). Returns their report.
- **task_complete(summary)** — mark the overall task as done.
- **ask_user(question)** — ask the professor a blocking question (use sparingly).
- **pull_user_messages()** — fetch queued user guidance/urgent interruptions when useful.
- **update_task_list(content)** — update your research plan (task_list.md).
- **append_log(entry)** — append an entry to your research log (LOG.md).
- **update_worker_config(content)** — update standing worker instructions (.claude/roles/worker.md).
"""

ORCH_WORKFLOW = """\
## Workflow

1. Start: Read task_list.md and LOG.md (if they exist) to resume context.
2. Investigate: Read files, search the codebase, understand the problem.
3. Plan: Call update_task_list with a research-level plan.
4. Execute: Delegate concrete execution to worker by default.
   Use direct implementation only for trivial unblockers (< 2 minutes, e.g., a one-line fix
   or a quick file read to unblock yourself).
5. Verify: NEVER accept a worker report at face value. Verify at least one key claim
   with concrete evidence (read the diff, run the test, check the artifact) before
   marking progress on that item.
6. Iterate: Refine based on evidence until the goal is met.
7. Complete: Call task_complete with a summary.
"""

ORCH_EXECUTION_POLICY = """\
## Execution Policy

VERY IMPORTANT: You are a planner and verifier. Workers are your hands.

**Anti-patterns — do NOT do these:**
- Don't "just quickly fix" something yourself when a worker should do it.
  If you wrote code or ran a command that changes project state, you skipped delegation.
- Don't skip verification. "Worker says tests pass" is NOT evidence. Read the test output.
- Don't do work AND assign it. If you already made the change, don't also ask a worker to
  make the same change.
- Don't assign vague tasks. "Look into this" is not an assignment. Provide objective,
  acceptance criteria, and relevant context.

**Your job in each cycle:**
1. Decide what needs to happen next (planning)
2. Write a concrete worker assignment OR do a trivial unblock (< 2 min)
3. Verify the result with evidence
4. Update task_list and log
"""

ORCH_COMMUNICATION = """\
## Communication

**User messages:**
- Pull queued user messages periodically so urgent direction is integrated quickly.
- When the user sends guidance, integrate it into your plan — don't just acknowledge.

**ask_user discipline:**
- Use ONLY for genuine blocking decisions or information not in the codebase.
- Anti-patterns: don't ask "should I proceed?", don't ask for status confirmation,
  don't ask when you can figure it out yourself.
- Format: one precise question per call. Include context for why you're blocked.

**Permission denial fallback:**
- If a worker reports permission denied or tool access errors, investigate alternative
  approaches before escalating to the user.

**Writing style:**
- Talk in direct prose. Do not write self-addressed chat text like "@orchestrator ...".
- Be concise — state findings and decisions, not narration of what you're about to do.
"""

ORCH_LOG_GUIDANCE = """\
## Research log (LOG.md)

Your log is your lab notebook — the narrative record of an investigation.
Call append_log at turning points, not on every action.

**Log when it matters:**
- A hypothesis forms or is tested ("suspect reward hacking — reward distribution is bimodal")
- Evidence changes your understanding ("worker confirmed: 72% of outputs exploit length bonus")
- You choose between approaches and WHY ("normalized reward > length penalty: no extra hyperparameter")
- A dead end worth remembering ("lr warmup had no effect — loss curve identical, ruling out optimization issue")
- A milestone is reached ("fix verified: loss drops to 0.3 by step 8k, reward distribution unimodal")

**A great entry captures WHY, not just WHAT:**
- Not "ran tests" → "tests revealed the reward model gives max score to 73% of outputs — confirms reward hacking"
- Not "fixed the bug" → "root cause: gradient clipping at 0.1 starved updates after step 5k. Relaxed to 1.0"
- Not "moving to next task" → "approach A ruled out (no effect on loss curve). Pivoting to reward shaping"

The log should read as a story — someone picking up where you left off can trace
your reasoning, see what you tried, and understand why you made each decision.
"""

ORCH_RULES = """\
## Rules

- Investigate before assigning work — don't delegate blindly.
- For Python commands, prefer uv-managed execution (`uv run ...`) or an explicit venv.
- Keep task_list at PhD-level granularity (experiments, milestones), not micro-steps.
- Only update worker config (.claude/roles/worker.md) when conventions genuinely change.
"""

ORCHESTRATOR_PROMPT = (
    ORCH_IDENTITY
    + "\n"
    + ORCH_TOOLS
    + "\n"
    + ORCH_WORKFLOW
    + "\n"
    + ORCH_EXECUTION_POLICY
    + "\n"
    + ORCH_COMMUNICATION
    + "\n"
    + ORCH_LOG_GUIDANCE
    + "\n"
    + ORCH_RULES
)


WORKER_IDENTITY = """\
You are a WORKER agent. Execute the orchestrator assignment end-to-end.
Focus on concrete actions and evidence.
Do not decide overall user-task completion — just execute your assignment and report results.
"""

WORKER_ENV = """\
## Execution environment

- Prefer uv-managed execution (`uv run ...`) or a project venv.
- Avoid bare global `python`/`pip` when a uv/venv path is available.
"""

WORKER_REPORT_CONTRACT = """\
## Report contract

When finished, call the **submit_report** tool with a structured report.
Your report goes directly to the orchestrator for review and verification.

**Required sections:**

1. **Assignment restatement**: One-line summary of what you were asked to do.
2. **What was done**: Specific actions taken, files modified, commands run.
3. **Results & Evidence**: Test output (paste actual output), verification results,
   key findings. Include file paths, line numbers, test counts, error messages.
4. **Acceptance criteria status**: For each goal in your assignment, state DONE or
   NOT DONE with evidence.
5. **Open issues** (if any): Blockers, partial results, questions for orchestrator.

**Anti-patterns — do NOT do these:**
- Don't say "tests pass" without pasting the actual test output.
- Don't say "file updated" without stating the file path and what changed.
- Don't omit error details — paste the traceback, not "an error occurred".

If blocked, still submit a report describing the blocker and what you attempted.
"""

WORKER_PROMPT = WORKER_IDENTITY + "\n" + WORKER_ENV + "\n" + WORKER_REPORT_CONTRACT


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
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
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
    model: str = DEFAULT_ORCHESTRATOR_MODEL
    session_model: str = DEFAULT_SESSION_MODEL
    permission_mode: str = DEFAULT_PERMISSION_MODE
    continuous_mode: bool = CONTINUOUS_MODE_DEFAULT
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
event_history: dict[str, deque] = {}

orchestrator_sessions: dict[str, str] = {}  # project_id -> orchestrator session
worker_sessions: dict[str, str] = {}  # project_id -> worker session
orchestrator_prompt_hashes: dict[str, str] = {}  # project_id -> orchestrator prompt hash
worker_prompt_hashes: dict[str, str] = {}  # project_id -> worker prompt hash
current_worker_tasks: dict[str, asyncio.Task] = {}  # project_id -> active worker task
callback_http_session: ClientSession | None = None
callback_http_lock = asyncio.Lock()
project_last_progress_ts: dict[str, float] = {}
project_last_heartbeat_nudge_ts: dict[str, float] = {}
heartbeat_tasks: dict[str, asyncio.Task] = {}


def _ensure_interrupt_queue(project_id: str) -> asyncio.Queue:
    """Create or fetch a bounded interrupt queue for a project."""
    queue = interrupt_queues.get(project_id)
    if queue is None:
        queue = asyncio.Queue(maxsize=INTERRUPT_QUEUE_MAX)
        interrupt_queues[project_id] = queue
    return queue


def _interrupt_payload_text(payload) -> str:
    """Normalize interrupt payloads from old/new queue formats to text."""
    if isinstance(payload, str):
        return payload.strip()
    if isinstance(payload, dict):
        return str(payload.get("text", "")).strip()
    return str(payload or "").strip()


def _normalize_urgency(raw: str) -> str:
    value = str(raw or "normal").strip().lower()
    return value if value in ("normal", "urgent") else "normal"


def _parse_bool(value, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return default


def _hash_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _compose_role_prompt(project_dir: str, role: str) -> tuple[str, str]:
    """Compose base prompt + optional role-specific memory file."""
    if role == "orchestrator":
        base_prompt = ORCHESTRATOR_PROMPT
        role_path = Path(project_dir) / ORCHESTRATOR_ROLE_FILE
    elif role == "worker":
        base_prompt = WORKER_PROMPT
        role_path = Path(project_dir) / WORKER_ROLE_FILE
    else:
        raise ValueError(f"Unknown role: {role}")

    prompt = base_prompt
    if role_path.exists():
        try:
            role_notes = role_path.read_text().strip()
        except Exception as e:
            log.warning("Failed reading role memory %s: %s", role_path, e)
            role_notes = ""
        if role_notes:
            relative_path = role_path.relative_to(project_dir)
            prompt = (
                f"{base_prompt.rstrip()}\n\n"
                f"## Role Memory ({relative_path.as_posix()})\n\n"
                f"{role_notes}\n"
            )

    return prompt, _hash_text(prompt)


def _interrupt_payload_meta(payload) -> dict:
    if isinstance(payload, dict):
        text = str(payload.get("text", "")).strip()
        urgency = _normalize_urgency(payload.get("urgency", "normal"))
        kind = str(payload.get("kind", "user_message")).strip() or "user_message"
        ts = payload.get("ts")
        if not isinstance(ts, (int, float)):
            ts = time.time()
        return {"text": text, "urgency": urgency, "kind": kind, "ts": float(ts)}
    text = _interrupt_payload_text(payload)
    return {"text": text, "urgency": "normal", "kind": "user_message", "ts": time.time()}


def _enqueue_interrupt(
    project_id: str,
    message: str,
    kind: str = "user_message",
    urgency: str = "normal",
) -> int:
    """Append a typed interrupt payload; bounded by queue size."""
    queue = _ensure_interrupt_queue(project_id)
    payload = {
        "kind": kind,
        "text": message,
        "urgency": _normalize_urgency(urgency),
        "ts": time.time(),
    }

    if queue.full():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass

    queue.put_nowait(payload)
    return queue.qsize()


async def _wait_for_interrupt_text(project_id: str, timeout: float) -> str:
    """Wait for the next non-empty USER message text.

    Non-user queue payloads (e.g., heartbeat nudges) are preserved and restored.
    """
    queue = _ensure_interrupt_queue(project_id)
    deadline = time.monotonic() + timeout
    deferred_payloads: list[dict] = []
    restored = False
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError
            payload = await asyncio.wait_for(queue.get(), timeout=remaining)
            meta = _interrupt_payload_meta(payload)
            if not meta["text"]:
                continue
            if meta["kind"] == "user_message":
                for deferred in deferred_payloads:
                    if queue.full():
                        try:
                            queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    queue.put_nowait(deferred)
                restored = True
                return meta["text"]
            deferred_payloads.append(meta)
    finally:
        if not restored:
            for deferred in deferred_payloads:
                if queue.full():
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                queue.put_nowait(deferred)


# -- progress streaming ------------------------------------------------


async def _get_callback_http_session() -> ClientSession:
    """Return a shared aiohttp session for callback POSTs."""
    global callback_http_session
    async with callback_http_lock:
        if callback_http_session is None or callback_http_session.closed:
            callback_http_session = ClientSession(timeout=ClientTimeout(total=5))
        return callback_http_session


def _event_payload(event: ProgressEvent) -> dict:
    return {
        "event_id": event.event_id,
        "type": event.type,
        "data": event.data,
        "iteration": event.iteration,
        "ts": event.timestamp,
    }


def _record_event(project_id: str, payload: dict):
    history = event_history.get(project_id)
    if history is None:
        history = deque(maxlen=EVENT_HISTORY_MAX)
        event_history[project_id] = history
    history.append(payload)


async def emit_progress(project_id: str, event: ProgressEvent):
    """Send progress event to SSE subscribers and HTTP callback."""
    payload = _event_payload(event)
    _record_event(project_id, payload)
    project_last_progress_ts[project_id] = event.timestamp

    if DEBUG_FLOW:
        log.info(
            "[flow/daemon] emit project=%s event_id=%s type=%s iter=%s data_len=%s",
            project_id,
            event.event_id,
            event.type,
            event.iteration,
            len(event.data or ""),
        )

    for q in sse_subscribers.get(project_id, []):
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            if DEBUG_FLOW:
                log.warning(
                    "[flow/daemon] sse queue full project=%s event_id=%s type=%s",
                    project_id,
                    event.event_id,
                    event.type,
                )
            pass

    try:
        http = await _get_callback_http_session()
        async with http.post(
            f"{CALLBACK_URL}/progress",
            json={
                "project_id": project_id,
                "daemon_name": DAEMON_NAME,
                "event": payload,
            },
        ) as resp:
            if resp.status >= 400:
                await resp.read()
                log.debug(
                    "Progress callback failed: status=%s project_id=%s event=%s",
                    resp.status,
                    project_id,
                    event.type,
                )
    except Exception:
        if DEBUG_FLOW:
            log.exception(
                "[flow/daemon] callback post failed project=%s event_id=%s type=%s",
                project_id,
                event.event_id,
                event.type,
            )
        pass


def _format_queued_messages_for_prompt(payloads: list[dict]) -> str:
    """Render queued user/system messages as prompt context."""
    if not payloads:
        return ""
    lines = [
        "[QUEUED MESSAGES]",
        "Integrate these messages before deciding your next action:",
    ]
    for idx, item in enumerate(payloads, start=1):
        kind = item.get("kind", "user_message")
        urgency = item.get("urgency", "normal")
        label = urgency if kind == "user_message" else f"{kind}/{urgency}"
        lines.append(f"{idx}. [{label}] {item.get('text', '')}")
    return "\n".join(lines)


async def _gpu_idle_hint(project_dir: str) -> str:
    """Best-effort GPU utilization hint for heartbeat nudges."""
    if not HEARTBEAT_GPU_HINT:
        return ""

    try:
        proc = await asyncio.create_subprocess_exec(
            "nvidia-smi",
            "--query-gpu=index,utilization.gpu,memory.used,memory.total",
            "--format=csv,noheader,nounits",
            cwd=project_dir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        return ""
    except Exception:
        return ""

    try:
        stdout, _stderr = await asyncio.wait_for(proc.communicate(), timeout=5)
    except asyncio.TimeoutError:
        proc.kill()
        try:
            await proc.communicate()
        except Exception:
            pass
        return ""
    except Exception:
        return ""

    if proc.returncode != 0:
        return ""

    idle_cards: list[str] = []
    for raw_line in stdout.decode("utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 4:
            continue
        try:
            gpu_idx = parts[0]
            util = int(parts[1])
            mem_used = int(parts[2])
            mem_total = int(parts[3])
        except ValueError:
            continue
        if util <= HEARTBEAT_GPU_IDLE_UTIL and mem_used <= HEARTBEAT_GPU_IDLE_MEMORY_MB:
            idle_cards.append(f"GPU{gpu_idx} (util={util}%, mem={mem_used}/{mem_total} MiB)")

    if not idle_cards:
        return ""

    return (
        "GPU heartbeat hint: detected likely-idle cards: "
        + "; ".join(idle_cards)
        + ". If beneficial, schedule GPU-bound worker tasks."
    )


async def _maybe_enqueue_heartbeat_nudge(project_id: str, state: TaskState) -> bool:
    """Inject a system nudge when the orchestrator has been idle too long."""
    if state.status != "running":
        return False

    now = time.time()
    last_progress = project_last_progress_ts.get(project_id, state.started_at)
    idle_for = now - last_progress
    if idle_for < HEARTBEAT_IDLE_SECONDS:
        return False

    cooldown = max(HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_IDLE_SECONDS // 2)
    last_nudge = project_last_heartbeat_nudge_ts.get(project_id, 0.0)
    if now - last_nudge < cooldown:
        return False

    idle_minutes = max(1, int(idle_for // 60))
    lines = [
        f"Heartbeat: no visible progress for ~{idle_minutes} minute(s).",
        "Keep moving autonomously: investigate, delegate concrete execution to worker, and verify evidence.",
        "If blocked on a true decision, call ask_user with one precise question.",
    ]
    if state.iteration > 0 and idle_for >= 600:
        lines.append(
            "Task hygiene: read task_list.md, pull user messages, "
            "and ensure your plan is current before continuing."
        )
    project = projects.get(project_id)
    if project:
        gpu_hint = await _gpu_idle_hint(project.project_dir)
        if gpu_hint:
            lines.append(gpu_hint)

    nudge_text = "\n".join(lines)
    _enqueue_interrupt(project_id, nudge_text, kind="system_nudge", urgency="normal")
    project_last_heartbeat_nudge_ts[project_id] = now
    await emit_progress(
        project_id,
        ProgressEvent(
            type="log_update",
            data=f"[heartbeat] queued nudge after {int(idle_for)}s idle",
            iteration=state.iteration,
        ),
    )
    return True


async def _heartbeat_loop(project_id: str, state: TaskState):
    """Periodic heartbeat to nudge orchestrator when progress stalls."""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
        if state.status != "running":
            break
        if cancel_events.get(project_id, asyncio.Event()).is_set():
            break
        try:
            await _maybe_enqueue_heartbeat_nudge(project_id, state)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("Heartbeat loop error for %s: %s", project_id, e)


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


# -- worker execution --------------------------------------------------


def _create_worker_tools(project_id: str, iteration: int, captured_report: list):
    """Create MCP server with the worker's submit_report tool.

    The submit_report tool writes a structured report to .reports/iteration-N.md
    and captures the content via the `captured_report` list so the daemon can
    return it to the orchestrator as the assign_worker result.
    """
    project = projects.get(project_id)
    reports_dir = Path(project.project_dir) / ".reports" if project else Path("/tmp/.reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    @tool(
        "submit_report",
        "Submit your work report when you've completed the assignment. "
        "This report goes directly to the orchestrator for verification.\n\n"
        "**Required 5-section structure:**\n"
        "1. **Assignment restatement**: One-line summary of what you were asked to do.\n"
        "2. **What was done**: Specific actions, files modified, commands run.\n"
        "3. **Results & Evidence**: Paste actual test output, diffs, verification results. "
        "Include file paths, line numbers, test counts, error messages.\n"
        "4. **Acceptance criteria status**: For each goal, state DONE or NOT DONE with evidence.\n"
        "5. **Open issues** (if any): Blockers, partial results, questions.\n\n"
        "**Anti-patterns:**\n"
        "- Don't say 'tests pass' without pasting actual test output.\n"
        "- Don't say 'file updated' without stating the path and what changed.\n"
        "- Don't omit error details — paste the traceback.",
        {"report": str},
    )
    async def submit_report(args):
        report_text = args["report"]
        report_path = reports_dir / f"iteration-{iteration}.md"
        report_path.write_text(report_text)
        captured_report.append(report_text)
        return {"content": [{"type": "text", "text":
            f"Report submitted and saved to .reports/iteration-{iteration}.md"}]}

    return create_sdk_mcp_server(
        "worker_tools",
        tools=[submit_report],
    )


async def _run_worker_turn(
    project_id: str,
    assignment: str,
    iteration: int,
    worker_session_id: str = "",
    model: str = DEFAULT_SESSION_MODEL,
    permission_mode: str = DEFAULT_PERMISSION_MODE,
) -> tuple[str, str]:
    """Execute one worker assignment in an independent worker session."""
    project = projects.get(project_id)
    if not project:
        return "Worker failed: unknown project.", worker_session_id

    # Capture report content via closure
    captured_report: list[str] = []
    worker_mcp = _create_worker_tools(project_id, iteration, captured_report)
    worker_prompt, worker_prompt_hash = _compose_role_prompt(project.project_dir, "worker")
    previous_hash = worker_prompt_hashes.get(project_id)

    # Role memory changed; start a fresh worker session so new instructions load.
    if previous_hash and previous_hash != worker_prompt_hash and worker_session_id:
        log.info(
            "Worker role memory changed for %s; resetting worker session",
            project_id,
        )
        worker_session_id = ""
        worker_sessions.pop(project_id, None)
    worker_prompt_hashes[project_id] = worker_prompt_hash

    options = ClaudeAgentOptions(
        permission_mode=permission_mode,
        model=model,
        cwd=project.project_dir,
        max_turns=50,
        setting_sources=["project"],  # loads CLAUDE.md from project dir natively
        mcp_servers={"worker_tools": worker_mcp},
    )

    if worker_session_id:
        options.resume = worker_session_id
    else:
        options.system_prompt = worker_prompt

    result_text = ""
    done_event = asyncio.Event()

    try:
        async for message in query(prompt=_prompt_stream(assignment, done_event), options=options):
            if isinstance(message, AssistantMessage):
                await _forward_assistant_message(
                    project_id, message, iteration, source="worker"
                )
            elif isinstance(message, SystemMessage):
                await emit_progress(
                    project_id,
                    ProgressEvent(
                        type="log_update",
                        data=f"[worker system] {message.subtype}",
                        iteration=iteration,
                    ),
                )
            elif isinstance(message, ResultMessage):
                result_text = message.result or ""
                worker_session_id = message.session_id
                done_event.set()
    except Exception as e:
        done_event.set()
        log.exception("Worker SDK error on iteration %s: %s", iteration, e)
        tb = traceback.format_exc()
        await emit_progress(
            project_id,
            ProgressEvent(
                type="tool_error",
                data=f"[worker] SDK error: {e}",
                iteration=iteration,
            ),
        )
        details = f"\nTraceback:\n{tb}" if tb else ""
        return f"Worker failed: {e}{details}", worker_session_id

    # Use the report submitted via MCP tool; fall back to session result
    if captured_report:
        report = captured_report[-1]
    else:
        report = result_text.strip() or "Worker returned without submitting a report."

    return report, worker_session_id


# -- orchestrator MCP tools -------------------------------------------


def _create_orchestrator_tools(project_id: str, state: TaskState):
    """Create in-process MCP server with orchestrator control tools.

    These tools replace the old text-marker protocol ([ASSIGN_WORKER], etc).
    The orchestrator calls them naturally as tool-use, and the daemon handles
    the side effects (running workers, emitting events, writing files).
    """

    @tool(
        "assign_worker",
        "Send a concrete task to your worker agent for execution. "
        "The worker has full tool access (Edit, Write, Bash, etc). "
        "Returns the worker's report when done. "
        "Each call counts toward the iteration limit.\n\n"
        "**When to use:** ANY task involving file edits, running commands, multi-step "
        "implementation, or investigation that requires tool access. This is your "
        "default execution path.\n\n"
        "**When NOT to use:** Don't assign work you already did yourself. Don't assign "
        "vague tasks like 'look into this'.\n\n"
        "**Required assignment structure:**\n"
        "- Objective: what the worker must accomplish\n"
        "- Acceptance criteria: how to verify success (tests to pass, output to produce)\n"
        "- Context: relevant file paths, prior findings, constraints",
        {"task": str},
    )
    async def assign_worker(args):
        state.iteration += 1

        if state.max_iterations > 0 and state.iteration > state.max_iterations:
            return {
                "content": [{"type": "text", "text":
                    f"Worker assignment limit ({state.max_iterations}) reached. "
                    "Call task_complete to summarize progress, or ask_user for guidance."}],
                "is_error": True,
            }

        if cancel_events.get(project_id, asyncio.Event()).is_set():
            return {
                "content": [{"type": "text", "text": "Task has been cancelled by user."}],
                "is_error": True,
            }

        task_desc = args["task"]

        # Drain any pending user interrupts
        interrupts = _drain_interrupt_payloads(project_id)

        await emit_progress(
            project_id,
            ProgressEvent(
                type="iteration",
                data=(
                    f"Worker assignment {state.iteration}/{state.max_iterations}"
                    if state.max_iterations > 0
                    else f"Worker assignment {state.iteration} (no cap)"
                ),
                iteration=state.iteration,
            ),
        )
        await emit_progress(
            project_id,
            ProgressEvent(
                type="tool_use",
                data=f"[orchestrator -> worker] {task_desc}",
                iteration=state.iteration,
            ),
        )
        await emit_progress(
            project_id,
            ProgressEvent(
                type="text",
                data=f"@orchestrator -> @worker: {task_desc}",
                iteration=state.iteration,
            ),
        )

        worker_sid = worker_sessions.get(project_id, "")
        try:
            worker_task = asyncio.create_task(
                _run_worker_turn(
                    project_id=project_id,
                    assignment=task_desc,
                    iteration=state.iteration,
                    worker_session_id=worker_sid,
                    model=state.session_model or state.model,
                    permission_mode=state.permission_mode,
                )
            )
            current_worker_tasks[project_id] = worker_task
            report, new_sid = await worker_task
        except Exception as e:
            log.exception("Worker turn failed: %s", e)
            report = f"Worker failed with error: {e}"
            new_sid = worker_sid
        finally:
            existing = current_worker_tasks.get(project_id)
            if existing and existing.done():
                current_worker_tasks.pop(project_id, None)

        worker_sessions[project_id] = new_sid
        state.worker_session_id = new_sid

        await emit_progress(
            project_id,
            ProgressEvent(
                type="text",
                data=f"@worker -> @orchestrator: {report}",
                iteration=state.iteration,
            ),
        )

        _save_state(
            state,
            orchestrator_session_id=orchestrator_sessions.get(project_id, ""),
            worker_session_id=new_sid,
        )

        # Append any pending user interrupts to the report
        result_text = report
        if interrupts:
            result_text += "\n\n[QUEUED MESSAGES]\n"
            for item in interrupts:
                kind = item.get("kind", "user_message")
                urgency = item.get("urgency", "normal")
                label = urgency if kind == "user_message" else f"{kind}/{urgency}"
                result_text += f"- ({label}) {item['text']}\n"

        result_text += (
            "\n\n[VERIFICATION REMINDER] The above is a worker CLAIM. "
            "Verify at least one key claim before marking progress."
        )

        return {"content": [{"type": "text", "text": result_text}]}

    @tool(
        "task_complete",
        "Mark the overall user task as complete. "
        "Call this when all goals are achieved.\n\n"
        "**When NOT to use:** Don't call if the last worker report is unverified. "
        "Don't call prematurely — ensure all acceptance criteria are met with evidence.\n\n"
        "**Before calling:** Pull user messages first (call pull_user_messages) to check "
        "for any last-minute guidance or course corrections. "
        "If user messages are queued, integrate them before completion.",
        {"summary": str},
    )
    async def task_complete(args):
        state.status = "done"
        state.summary = args["summary"]
        state.finished_at = time.time()

        await emit_progress(
            project_id,
            ProgressEvent(type="done", data=state.summary, iteration=state.iteration),
        )

        _save_state(
            state,
            orchestrator_session_id=orchestrator_sessions.get(project_id, ""),
            worker_session_id=worker_sessions.get(project_id, ""),
        )

        return {"content": [{"type": "text", "text": f"Task marked complete: {args['summary']}"}]}

    @tool(
        "pull_user_messages",
        "Fetch queued user messages with urgency metadata. "
        "Use this periodically to integrate non-urgent guidance and urgent interruptions.\n\n"
        "**When to use:** Before planning your next step, before calling task_complete, "
        "after receiving a worker report, and whenever you suspect the user may have "
        "sent guidance.",
        {},
    )
    async def pull_user_messages(_args):
        pending = _drain_interrupt_payloads(project_id)
        if not pending:
            return {"content": [{"type": "text", "text": "No queued user messages."}]}

        lines = []
        for idx, item in enumerate(pending, start=1):
            lines.append(f"{idx}. [{item['kind']}:{item['urgency']}] {item['text']}")

        return {
            "content": [
                {
                    "type": "text",
                    "text": "Queued user messages:\n" + "\n".join(lines),
                }
            ]
        }

    @tool(
        "ask_user",
        "Ask the professor/user a blocking question. "
        "Use sparingly — only for genuine decisions or information "
        "that cannot be found in the codebase. "
        "Blocks until the user responds (up to 10 minutes).\n\n"
        "**Anti-patterns — do NOT ask these:**\n"
        "- 'Should I proceed?' — just proceed.\n"
        "- 'Can you confirm the status?' — check it yourself.\n"
        "- 'Is this correct?' — verify it with evidence.\n"
        "- Multiple questions in one call — ask ONE precise question.\n\n"
        "**Good format:** State the decision point, the options you see, and why "
        "you can't decide without input. Example: 'The training config supports both "
        "fp16 and bf16. The GPU is A100 (supports both). Which precision do you prefer "
        "for this experiment?'",
        {"question": str},
    )
    async def ask_user(args):
        question = args["question"]

        state.status = "stuck"
        state.summary = f"Needs input: {question}"
        _save_state(
            state,
            orchestrator_session_id=orchestrator_sessions.get(project_id, ""),
            worker_session_id=worker_sessions.get(project_id, ""),
        )

        await emit_progress(
            project_id,
            ProgressEvent(type="stuck", data=question, iteration=state.iteration),
        )

        try:
            answer = await _wait_for_interrupt_text(project_id, timeout=600)
            state.status = "running"
            state.summary = ""
            _save_state(
                state,
                orchestrator_session_id=orchestrator_sessions.get(project_id, ""),
                worker_session_id=worker_sessions.get(project_id, ""),
            )
            return {"content": [{"type": "text", "text": f"User responded: {answer}"}]}
        except asyncio.TimeoutError:
            state.status = "running"
            state.summary = ""
            _save_state(
                state,
                orchestrator_session_id=orchestrator_sessions.get(project_id, ""),
                worker_session_id=worker_sessions.get(project_id, ""),
            )
            return {"content": [{"type": "text", "text":
                "No user response after 10 minutes. "
                "Proceed with your best judgment or call task_complete with current progress."}]}

    @tool(
        "update_task_list",
        "Update your research plan (task_list.md). "
        "Use markdown checkboxes. PhD-level granularity: "
        "experiments, investigations, milestones — not micro-implementation steps.\n\n"
        "**Anti-pattern:** Don't create a new task list every iteration. Read the existing "
        "one, update check marks and add/remove items as needed.",
        {"content": str},
    )
    async def update_task_list(args):
        _save_task_list(project_id, args["content"])

        await emit_progress(
            project_id,
            ProgressEvent(
                type="task_list",
                data=args["content"],
                iteration=state.iteration,
            ),
        )

        return {"content": [{"type": "text", "text": "Task list updated."}]}

    @tool(
        "append_log",
        "Append an entry to your research log (LOG.md). "
        "Your lab notebook — record hypotheses, findings, decisions, and pivots. "
        "Write at turning points, not on every action.\n\n"
        "**Anti-pattern:** Don't log routine actions ('assigned worker', 'read file'). "
        "Log insights, evidence, decisions, and pivots.",
        {"entry": str},
    )
    async def append_log(args):
        log_path = _append_log(project_id, args["entry"])

        await emit_progress(
            project_id,
            ProgressEvent(
                type="log_update",
                data=args["entry"],
                iteration=state.iteration,
            ),
        )

        return {"content": [{"type": "text", "text": f"Log entry appended to {log_path}"}]}

    @tool(
        "update_worker_config",
        "Update standing instructions for your worker (.claude/roles/worker.md). "
        "The worker loads this role memory at the start of every assignment. "
        "This is separate from the project's root CLAUDE.md (which you should not overwrite). "
        "Use for: learned conventions, file locations, environment quirks, "
        "tool preferences. Only update when something genuinely changes.\n\n"
        "**Anti-pattern:** Don't update on every iteration. Only update when you discover "
        "a convention or constraint the worker should know for ALL future assignments.",
        {"content": str},
    )
    async def update_worker_config(args):
        project = projects.get(project_id)
        if not project:
            return {
                "content": [{"type": "text", "text": "Error: unknown project."}],
                "is_error": True,
            }

        role_path = Path(project.project_dir) / WORKER_ROLE_FILE
        role_path.parent.mkdir(parents=True, exist_ok=True)
        role_path.write_text(args["content"])

        # Force fresh worker session so new role memory is loaded immediately.
        worker_prompt_hashes.pop(project_id, None)
        if worker_sessions.pop(project_id, None):
            state.worker_session_id = ""
            _save_state(
                state,
                orchestrator_session_id=orchestrator_sessions.get(project_id, ""),
                worker_session_id="",
            )

        return {"content": [{"type": "text", "text":
            "Worker instructions (.claude/roles/worker.md) updated."}]}

    return create_sdk_mcp_server(
        "daemon",
        tools=[
            assign_worker,
            task_complete,
            pull_user_messages,
            ask_user,
            update_task_list,
            append_log,
            update_worker_config,
        ],
    )


# -- main task runner --------------------------------------------------


def _build_orchestrator_options(
    project_dir: str,
    mcp_server,
    max_iterations: int,
    session_id: str,
    system_prompt: str,
    model: str,
    session_model: str,
    permission_mode: str,
) -> ClaudeAgentOptions:
    if max_iterations > 0:
        max_turns = max(8, max_iterations * 8)
    else:
        # Unlimited worker assignments still need a practical per-query turn cap.
        max_turns = 160

    active_model = session_model if session_id and session_model else model
    options = ClaudeAgentOptions(
        permission_mode=permission_mode,
        model=active_model,
        cwd=project_dir,
        setting_sources=["project"],  # load shared CLAUDE.md natively
        mcp_servers={"daemon": mcp_server},
        max_turns=max_turns,
    )
    if session_id:
        options.resume = session_id
    else:
        options.system_prompt = system_prompt
    return options


async def run_task(
    project_id: str,
    task_text: str,
    max_iterations: int = MAX_ITERATIONS,
    continuous_mode: bool = CONTINUOUS_MODE_DEFAULT,
    model: str = DEFAULT_ORCHESTRATOR_MODEL,
    session_model: str = DEFAULT_SESSION_MODEL,
    permission_mode: str = DEFAULT_PERMISSION_MODE,
):
    """Run autonomous task with MCP tool-driven orchestrator."""
    os.environ.pop("CLAUDECODE", None)

    project = projects.get(project_id)
    if not project:
        log.error("Unknown project: %s", project_id)
        return

    task_id = uuid.uuid4().hex
    state = TaskState(
        task_id=task_id,
        project_id=project_id,
        task_text=task_text,
        max_iterations=max_iterations,
        continuous_mode=continuous_mode,
        model=model,
        session_model=session_model or model,
        permission_mode=_normalize_permission_mode(permission_mode, default=DEFAULT_PERMISSION_MODE),
    )
    task_states[project_id] = state

    _ensure_interrupt_queue(project_id)
    if project_id not in cancel_events:
        cancel_events[project_id] = asyncio.Event()
    cancel_events[project_id].clear()
    project_last_progress_ts[project_id] = time.time()

    orchestrator_session_id = orchestrator_sessions.get(project_id, "")

    # Create MCP tools bound to this project/task state
    mcp_server = _create_orchestrator_tools(project_id, state)

    await emit_progress(
        project_id,
        ProgressEvent(type="iteration", data=f"Starting task: {task_text}", iteration=0),
    )

    prompt = f"[TASK]\n{task_text}"
    if HEARTBEAT_ENABLED:
        hb_task = asyncio.create_task(_heartbeat_loop(project_id, state))
        heartbeat_tasks[project_id] = hb_task

    try:
        next_prompt = prompt
        while True:
            orchestrator_prompt, orchestrator_prompt_hash = _compose_role_prompt(
                project.project_dir,
                "orchestrator",
            )
            previous_hash = orchestrator_prompt_hashes.get(project_id)
            if previous_hash and previous_hash != orchestrator_prompt_hash and orchestrator_session_id:
                log.info(
                    "Orchestrator role memory changed for %s; resetting orchestrator session",
                    project_id,
                )
                orchestrator_session_id = ""
                orchestrator_sessions.pop(project_id, None)
                state.orchestrator_session_id = ""
                state.sdk_session_id = ""
            orchestrator_prompt_hashes[project_id] = orchestrator_prompt_hash

            done_event = asyncio.Event()
            options = _build_orchestrator_options(
                project_dir=project.project_dir,
                mcp_server=mcp_server,
                max_iterations=max_iterations,
                session_id=orchestrator_session_id,
                system_prompt=orchestrator_prompt,
                model=state.model,
                session_model=state.session_model,
                permission_mode=state.permission_mode,
            )
            saw_result = False
            prompt_for_turn = next_prompt
            pending_messages = _drain_interrupt_payloads(project_id)
            if pending_messages:
                prompt_for_turn = (
                    f"{next_prompt}\n\n{_format_queued_messages_for_prompt(pending_messages)}"
                )

            async for message in query(
                prompt=_prompt_stream(prompt_for_turn, done_event), options=options
            ):
                if isinstance(message, AssistantMessage):
                    await _forward_assistant_message(
                        project_id, message, state.iteration, source="orchestrator"
                    )
                elif isinstance(message, SystemMessage):
                    await emit_progress(
                        project_id,
                        ProgressEvent(
                            type="log_update",
                            data=f"[orchestrator system] {message.subtype}",
                            iteration=state.iteration,
                        ),
                    )
                elif isinstance(message, ResultMessage):
                    sid = (message.session_id or "").strip()
                    if sid:
                        orchestrator_session_id = sid
                        orchestrator_sessions[project_id] = sid
                        state.orchestrator_session_id = sid
                        state.sdk_session_id = sid
                    saw_result = True
                    done_event.set()

            if state.status != "running":
                break

            if not continuous_mode:
                state.status = "done"
                state.summary = "Orchestrator session ended naturally"
                state.finished_at = time.time()
                # Empty data so web layer only shows progress status, not a chat message
                # (only explicit task_complete summaries should appear in chat)
                await emit_progress(
                    project_id,
                    ProgressEvent(type="done", data="", iteration=state.iteration),
                )
                break

            if not saw_result:
                log.warning(
                    "Orchestrator query ended without ResultMessage for %s; continuing in continuous mode",
                    project_id,
                )

            await emit_progress(
                project_id,
                ProgressEvent(
                    type="iteration",
                    data="Continuing autonomously from task_list and current context",
                    iteration=state.iteration,
                ),
            )
            _save_state(
                state,
                orchestrator_session_id=orchestrator_session_id,
                worker_session_id=worker_sessions.get(project_id, ""),
            )

            next_prompt = (
                "[CONTINUE]\n"
                "No new user instruction. Before continuing:\n"
                "1. Read task_list.md to see current progress and next items.\n"
                "2. Pull user messages (call pull_user_messages) for any guidance.\n"
                "3. Continue with the next unchecked item in task_list.\n"
                "Keep driving progress autonomously."
            )
            await asyncio.sleep(0.2)

    except asyncio.CancelledError:
        state.status = "stopped"
        state.summary = "Task cancelled"
        state.finished_at = time.time()
        await emit_progress(
            project_id,
            ProgressEvent(type="stopped", data="Task cancelled", iteration=state.iteration),
        )
    except Exception as e:
        log.exception("Orchestrator error: %s", e)
        state.status = "error"
        state.error = str(e)
        state.finished_at = time.time()
        await emit_progress(
            project_id,
            ProgressEvent(type="error", data=str(e), iteration=state.iteration),
        )
    finally:
        hb_task = heartbeat_tasks.pop(project_id, None)
        if hb_task and not hb_task.done():
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        running_tasks.pop(project_id, None)
        project_last_progress_ts.pop(project_id, None)
        project_last_heartbeat_nudge_ts.pop(project_id, None)
        _save_state(
            state,
            orchestrator_session_id=orchestrator_session_id,
            worker_session_id=worker_sessions.get(project_id, ""),
        )


def _drain_interrupt_payloads(project_id: str) -> list[dict]:
    """Drain all pending interruption payloads for a project."""
    queue = interrupt_queues.get(project_id)
    if not queue:
        return []

    payloads: list[dict] = []
    while not queue.empty():
        try:
            payload = queue.get_nowait()
            meta = _interrupt_payload_meta(payload)
            if meta["text"]:
                payloads.append(meta)
        except asyncio.QueueEmpty:
            break
    return payloads


def _drain_interruptions(project_id: str) -> list[str]:
    """Drain pending interruptions and return text-only messages."""
    return [item["text"] for item in _drain_interrupt_payloads(project_id)]




def _save_task_list(project_id: str, content: str):
    """Write task list to task_list.md in project root."""
    project = projects.get(project_id)
    if not project:
        return
    path = Path(project.project_dir) / "task_list.md"
    path.write_text(content)


def _append_log(project_id: str, entry: str) -> str:
    """Append a timestamped entry to LOG.md in project root. Returns the file path."""
    project = projects.get(project_id)
    if not project:
        return ""
    path = Path(project.project_dir) / "LOG.md"
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"\n---\n**[{timestamp}]**\n\n{entry.strip()}\n"
    with open(path, "a") as f:
        f.write(formatted)
    return str(path)




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
                        data=f"[{source}] {text}",
                        iteration=iteration,
                    ),
                )
        elif isinstance(block, ToolUseBlock):
            tool_msg = f"{block.name}"
            if block.name in ("Bash", "Write", "Edit"):
                snippet = json.dumps(block.input, ensure_ascii=False)
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
                        data=f"[{source}] {content}",
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
        "model": state.model,
        "session_model": state.session_model,
        "permission_mode": state.permission_mode,
        "continuous_mode": state.continuous_mode,
        "sdk_session_id": orchestrator_session_id,
        "orchestrator_session_id": orchestrator_session_id,
        "worker_session_id": worker_session_id,
        "orchestrator_prompt_hash": orchestrator_prompt_hashes.get(state.project_id, ""),
        "worker_prompt_hash": worker_prompt_hashes.get(state.project_id, ""),
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


def _hydrate_sessions_from_state(project_id: str) -> bool:
    """Restore orchestrator/worker session IDs and prompt hashes from persisted state."""
    data = _load_state(project_id)
    if not data:
        return False

    orch_sid = str(
        data.get("orchestrator_session_id")
        or data.get("sdk_session_id")
        or ""
    ).strip()
    worker_sid = str(data.get("worker_session_id") or "").strip()

    if orch_sid:
        orchestrator_sessions[project_id] = orch_sid
    if worker_sid:
        worker_sessions[project_id] = worker_sid
    orch_prompt_hash = str(data.get("orchestrator_prompt_hash") or "").strip()
    worker_prompt_hash = str(data.get("worker_prompt_hash") or "").strip()
    if orch_prompt_hash:
        orchestrator_prompt_hashes[project_id] = orch_prompt_hash
    if worker_prompt_hash:
        worker_prompt_hashes[project_id] = worker_prompt_hash
    return bool(orch_sid or worker_sid)


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
    if _hydrate_sessions_from_state(project_id):
        log.info("Restored persisted session IDs for %s", project_id)
    log.info("Registered project %s: %s", project_id, project_dir)
    return web.json_response({"ok": True, "project_id": project_id})


async def handle_task(request: web.Request) -> web.Response:
    """POST /task — start autonomous work on a task. Returns immediately."""
    data = await request.json()
    project_id = data.get("project_id")
    task_text = data.get("task")
    max_iter_raw = data.get("max_iterations", MAX_ITERATIONS)
    continuous_mode = _parse_bool(data.get("continuous_mode"), CONTINUOUS_MODE_DEFAULT)
    model = str(data.get("model") or DEFAULT_ORCHESTRATOR_MODEL).strip() or DEFAULT_ORCHESTRATOR_MODEL
    session_model = str(data.get("session_model") or model).strip() or model
    permission_mode = _normalize_permission_mode(
        data.get("permission_mode"),
        default=DEFAULT_PERMISSION_MODE,
    )

    if not project_id or not task_text:
        return web.json_response({"error": "project_id and task required"}, status=400)

    try:
        max_iter = int(max_iter_raw)
    except (TypeError, ValueError):
        return web.json_response({"error": "max_iterations must be an integer"}, status=400)
    if max_iter < 0:
        return web.json_response({"error": "max_iterations must be >= 0"}, status=400)

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

    task = asyncio.create_task(
        run_task(
            project_id,
            task_text,
            max_iter,
            continuous_mode=continuous_mode,
            model=model,
            session_model=session_model,
            permission_mode=permission_mode,
        )
    )
    running_tasks[project_id] = task

    return web.json_response(
        {
            "ok": True,
            "project_id": project_id,
            "status": "started",
            "continuous_mode": continuous_mode,
            "model": model,
            "session_model": session_model,
            "permission_mode": permission_mode,
        }
    )


async def handle_interrupt(request: web.Request) -> web.Response:
    """POST /interrupt — inject a user message into the running task."""
    data = await request.json()
    project_id = data.get("project_id")
    message = str(data.get("message", "")).strip()
    urgency = _normalize_urgency(data.get("urgency", "normal"))

    if not project_id:
        return web.json_response({"error": "project_id required"}, status=400)
    if project_id not in projects:
        return web.json_response({"error": "Unknown project"}, status=404)
    if not message:
        return web.json_response({"error": "message required"}, status=400)

    qsize = _enqueue_interrupt(project_id, message, urgency=urgency)

    log.info(
        "Interrupt queued for %s urgency=%s (size=%s): %s",
        project_id,
        urgency,
        qsize,
        message,
    )
    return web.json_response({"ok": True, "queued": True, "urgency": urgency, "queue_size": qsize})


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
                "model": state.model if state else DEFAULT_ORCHESTRATOR_MODEL,
                "session_model": state.session_model if state else DEFAULT_SESSION_MODEL,
                "permission_mode": state.permission_mode if state else DEFAULT_PERMISSION_MODE,
                "continuous_mode": state.continuous_mode if state else CONTINUOUS_MODE_DEFAULT,
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
            "model": state.model,
            "session_model": state.session_model,
            "permission_mode": state.permission_mode,
            "continuous_mode": state.continuous_mode,
            "task_text": state.task_text,
            "summary": state.summary,
            "error": state.error,
            "started_at": state.started_at,
            "finished_at": state.finished_at,
        }
    )


async def handle_events(request: web.Request) -> web.Response:
    """GET /events — replay buffered progress events for a project."""
    project_id = request.query.get("project_id")
    if not project_id:
        return web.json_response({"error": "project_id required"}, status=400)

    history = list(event_history.get(project_id, []))
    if not history:
        if project_id in projects:
            return web.json_response({"project_id": project_id, "events": [], "truncated": False})
        return web.json_response({"error": "Unknown project"}, status=404)

    after_event_id = str(request.query.get("after_event_id", "")).strip()
    limit_raw = request.query.get("limit")
    try:
        limit = int(limit_raw) if limit_raw is not None else EVENT_HISTORY_MAX
    except (TypeError, ValueError):
        return web.json_response({"error": "limit must be an integer"}, status=400)
    limit = max(1, min(limit, EVENT_HISTORY_MAX))

    start_idx = 0
    truncated = False
    if after_event_id:
        found = False
        for idx, event in enumerate(history):
            if event.get("event_id") == after_event_id:
                start_idx = idx + 1
                found = True
                break
        if not found:
            truncated = True
            start_idx = max(0, len(history) - limit)

    events = history[start_idx:]
    if len(events) > limit:
        truncated = True
        events = events[-limit:]

    return web.json_response(
        {
            "project_id": project_id,
            "events": events,
            "truncated": truncated,
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

    worker_task = current_worker_tasks.get(project_id)
    if worker_task and not worker_task.done():
        try:
            # Graceful drain: let active worker turn finish briefly before cancellation.
            await asyncio.wait_for(asyncio.shield(worker_task), timeout=10)
        except asyncio.TimeoutError:
            log.info("Stop drain timed out for %s; cancelling orchestrator task", project_id)
        except Exception as e:
            log.debug("Stop drain error for %s: %s", project_id, e)

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


# -- app lifecycle -----------------------------------------------------


async def on_cleanup(_app: web.Application):
    global callback_http_session
    if callback_http_session and not callback_http_session.closed:
        await callback_http_session.close()
    callback_http_session = None


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
    app.router.add_get("/events", handle_events)
    app.router.add_get("/stream", handle_stream)
    app.router.add_post("/stop", handle_stop)
    app.router.add_get("/health", handle_health)
    app.on_cleanup.append(on_cleanup)

    web.run_app(app, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
