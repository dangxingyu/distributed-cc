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
import re
import time
import traceback
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from aiohttp import ClientSession, ClientTimeout, web

from claude_agent_sdk.types import (
    AssistantMessage,
    SystemMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
)

from dcc_runtime import RuntimeEvent, RuntimeRequest, ToolSpec
from dcc_runtime.claude_backend import build_sdk_server
from dcc_runtime.factory import run_turn as run_runtime_turn

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
DEFAULT_PROVIDER = str(os.environ.get("DCC_PROVIDER", "claude") or "claude").strip().lower() or "claude"
DEFAULT_ORCHESTRATOR_MODEL = os.environ.get("ORCHESTRATOR_MODEL", "claude-opus-4-6")
DEFAULT_SESSION_MODEL = os.environ.get("ORCHESTRATOR_SESSION_MODEL", DEFAULT_ORCHESTRATOR_MODEL)
DEFAULT_PERMISSION_MODE = _normalize_permission_mode(
    os.environ.get("DCC_PERMISSION_MODE", "bypassPermissions"),
    default="bypassPermissions",
)
DEFAULT_CODEX_SANDBOX_MODE = str(
    os.environ.get("DCC_CODEX_SANDBOX_MODE", "workspace-write") or "workspace-write"
).strip() or "workspace-write"
DEFAULT_CODEX_APPROVAL_POLICY = str(
    os.environ.get("DCC_CODEX_APPROVAL_POLICY", "never") or "never"
).strip() or "never"
DEBUG_FLOW = _env_flag("DCC_DEBUG_FLOW")
EVENT_HISTORY_MAX = _env_int("EVENT_HISTORY_MAX", 2000, minimum=1)
MAX_ITERATIONS = 0
STATE_DIR = Path.home() / ".distributed-cc" / "state"
INTERRUPT_QUEUE_MAX = 100
HEARTBEAT_GPU_HINT = os.environ.get("HEARTBEAT_GPU_HINT", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
HEARTBEAT_GPU_IDLE_UTIL = _env_int("HEARTBEAT_GPU_IDLE_UTIL", 10, minimum=0)
HEARTBEAT_GPU_IDLE_MEMORY_MB = _env_int("HEARTBEAT_GPU_IDLE_MEMORY_MB", 2048, minimum=0)
STANDBY_HEARTBEAT_ENABLED = os.environ.get("STANDBY_HEARTBEAT_ENABLED", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
STANDBY_HEARTBEAT_SECONDS = _env_int("STANDBY_HEARTBEAT_SECONDS", 1800, minimum=60)
STANDBY_WAKE_MAX_ITERATIONS = _env_int("STANDBY_WAKE_MAX_ITERATIONS", 1, minimum=1)
STANDBY_WAKEUP_MARKER = "[STANDBY HEARTBEAT WAKEUP]"
CONTINUOUS_MODE_DEFAULT = os.environ.get("CONTINUOUS_MODE", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
ROLE_CONFIG_DIR = ".claude/roles"
ORCHESTRATOR_ROLE_FILE = f"{ROLE_CONFIG_DIR}/orchestrator.md"
WORKER_ROLE_FILE = f"{ROLE_CONFIG_DIR}/worker.md"
MCP_CONFIG_DIR = ".claude/mcp"
ORCHESTRATOR_MCP_FILE = f"{MCP_CONFIG_DIR}/orchestrator.json"
WORKER_MCP_FILE = f"{MCP_CONFIG_DIR}/worker.json"


# -- split-role prompts ------------------------------------------------

ORCH_IDENTITY = """\
You are the ORCHESTRATOR — a PhD-student-level autonomous researcher.
The user is your advisor (professor). They give high-level direction and
occasional course corrections — they are NOT controlling you step by step.
You independently plan, investigate, decompose tasks, assign workers,
review output, and drive to completion.

The advisor may be juggling many projects simultaneously and cannot track your
progress in detail. YOU are the state keeper. Your task_list.md and LOG.md are
the source of truth for this project's status. Keep them current so that anyone
(including yourself after a session restart) can pick up exactly where you left off.

When the advisor is silent, keep working. Their silence means "carry on".
Only stop when all goals are met (call task_complete), when you intentionally
enter standby (call stay_idle), or when you hit a genuine decision that
requires their input (call ask_user).
"""

ORCH_TOOLS = """\
## Your tools

Besides the standard Read/Glob/Grep/WebSearch/WebFetch for investigation, you have:

- **assign_worker(task)** — send a concrete assignment to your worker agent.
  This is your primary execution path for substantial implementation/investigation work.
  The worker has full tool access (Edit, Write, Bash, etc). Returns their report.
- **stay_idle(reason)** — enter standby when triage finds no meaningful action right now.
  This ends the current run cleanly and lets heartbeat/user messages wake you later.
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
7. Complete: Call task_complete with a summary when done, or call stay_idle(reason)
   when there is no meaningful next action yet.
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

**Advisor model:**
- The user is an advisor, not a controller. They send occasional guidance,
  course corrections, or new priorities — not step-by-step instructions.
- When the user sends guidance, integrate it into your plan immediately.
- When no guidance arrives, that means "keep going". Never stall waiting for input.
- Pull queued user messages periodically so urgent direction is integrated quickly.

**ask_user discipline:**
- Use ONLY for genuine blocking decisions or information not in the codebase.
- Anti-patterns: don't ask "should I proceed?", don't ask for status confirmation,
  don't ask when you can figure it out yourself, don't ask for approval.
- Format: one precise question per call. Include context for why you're blocked.
- The advisor is busy — respect their attention. If you can make a reasonable
  decision yourself, do it and note it in the log.

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
    provider: str = DEFAULT_PROVIDER
    model: str = DEFAULT_ORCHESTRATOR_MODEL
    session_model: str = DEFAULT_SESSION_MODEL
    permission_mode: str = DEFAULT_PERMISSION_MODE
    sandbox_mode: str = DEFAULT_CODEX_SANDBOX_MODE
    approval_policy: str = DEFAULT_CODEX_APPROVAL_POLICY
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
orchestrator_plugin_hashes: dict[str, str] = {}  # project_id -> orchestrator MCP plugin hash
worker_plugin_hashes: dict[str, str] = {}  # project_id -> worker MCP plugin hash
current_worker_tasks: dict[str, asyncio.Task] = {}  # project_id -> active worker task
callback_http_session: ClientSession | None = None
callback_http_lock = asyncio.Lock()
project_last_standby_wake_ts: dict[str, float] = {}
standby_heartbeat_tasks: dict[str, asyncio.Task] = {}
project_last_standby_signal_hash: dict[str, str] = {}
project_last_standby_resting_hash: dict[str, str] = {}


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


def _normalize_provider(raw: str | None, default: str = DEFAULT_PROVIDER) -> str:
    value = str(raw or default or "claude").strip().lower()
    return value if value in ("claude", "codex") else default


def _normalize_codex_sandbox_mode(raw: str | None, permission_mode: str = "") -> str:
    value = str(raw or "").strip()
    if value in ("read-only", "workspace-write", "danger-full-access"):
        return value
    permission = _normalize_permission_mode(permission_mode, default=DEFAULT_PERMISSION_MODE)
    if permission == "bypassPermissions":
        return "danger-full-access"
    return DEFAULT_CODEX_SANDBOX_MODE


def _normalize_codex_approval_policy(raw: str | None) -> str:
    value = str(raw or "").strip()
    return value if value in ("untrusted", "on-failure", "on-request", "never") else DEFAULT_CODEX_APPROVAL_POLICY


def _shared_codex_instructions(project_dir: str) -> str:
    path = Path(project_dir) / "CLAUDE.md"
    if not path.exists():
        return ""
    try:
        content = path.read_text().strip()
    except Exception as e:
        log.warning("Failed reading shared project instructions %s: %s", path, e)
        return ""
    if not content:
        return ""
    return (
        f"## Shared Project Instructions ({path.name})\n\n"
        f"{content}\n"
    )


def _compose_runtime_prompt(project_dir: str, role: str, provider: str) -> tuple[str, str]:
    prompt, prompt_hash = _compose_role_prompt(project_dir, role)
    if _normalize_provider(provider) != "codex":
        return prompt, prompt_hash
    shared = _shared_codex_instructions(project_dir)
    if not shared:
        return prompt, prompt_hash
    combined = f"{prompt.rstrip()}\n\n{shared}"
    return combined, _hash_text(combined)


def _hash_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _is_standby_resting_text(text: str) -> bool:
    lowered = " ".join(text.lower().split())
    markers = (
        "decision: stay resting",
        "staying resting",
        "stay resting",
        "nothing to act on",
        "nothing to do",
        "no pending work",
        "all deliverables shipped",
        "project is complete",
    )
    return any(marker in lowered for marker in markers)


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


def _normalize_mcp_servers_payload(raw) -> dict:
    if not isinstance(raw, dict):
        return {}

    if isinstance(raw.get("mcp_servers"), dict):
        payload = raw["mcp_servers"]
    elif isinstance(raw.get("servers"), dict):
        payload = raw["servers"]
    else:
        payload = raw

    normalized: dict[str, dict] = {}
    for name, cfg in payload.items():
        key = str(name or "").strip()
        if not key:
            continue
        if not isinstance(cfg, dict):
            continue
        normalized[key] = cfg
    return normalized


def _load_role_mcp_servers(project_dir: str, role: str) -> tuple[dict, str]:
    """Load role-specific MCP server declarations from project files."""
    if role == "orchestrator":
        path = Path(project_dir) / ORCHESTRATOR_MCP_FILE
    elif role == "worker":
        path = Path(project_dir) / WORKER_MCP_FILE
    else:
        raise ValueError(f"Unknown role for MCP config: {role}")

    if not path.exists():
        return {}, _hash_text("{}")

    try:
        raw_text = path.read_text()
    except Exception as e:
        log.warning("Failed reading MCP config %s: %s", path, e)
        return {}, _hash_text("{}")

    try:
        data = json.loads(raw_text)
    except Exception as e:
        log.warning("Invalid MCP config JSON %s: %s", path, e)
        return {}, _hash_text("{}")

    servers = _normalize_mcp_servers_payload(data)
    servers_hash = _hash_text(json.dumps(servers, sort_keys=True))
    return servers, servers_hash


def _merge_mcp_servers(base_servers: dict, extra_servers: dict, reserved_names: set[str]) -> dict:
    merged = dict(base_servers)
    for name, cfg in extra_servers.items():
        if name in reserved_names:
            log.warning("Ignoring role MCP config server '%s' (reserved name)", name)
            continue
        merged[name] = cfg
    return merged


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


def _task_list_has_unchecked_items(project_id: str) -> bool:
    """Return True when task_list.md contains unchecked checkbox items."""
    has_unchecked, _signal = _task_list_unchecked_state(project_id)
    return has_unchecked


def _task_list_unchecked_state(project_id: str) -> tuple[bool, str]:
    """Return task-list unchecked status and a stable signal hash."""
    project = projects.get(project_id)
    if not project:
        return False, ""

    path = Path(project.project_dir) / "task_list.md"
    if not path.exists():
        return False, ""

    try:
        content = path.read_text()
    except Exception:
        return False, ""

    unchecked = re.compile(r"^\s*[-*]\s*\[\s\]\s+")
    unchecked_lines = [line.strip() for line in content.splitlines() if unchecked.match(line)]
    if not unchecked_lines:
        return False, ""

    return True, _hash_text("\n".join(unchecked_lines))


def _queued_user_messages(project_id: str) -> list[dict]:
    """Snapshot queued user messages without draining the interrupt queue."""
    queue = interrupt_queues.get(project_id)
    if not queue or queue.empty():
        return []

    try:
        pending = list(queue._queue)  # type: ignore[attr-defined]
    except Exception:
        # Fallback: best-effort when queue internals are unavailable.
        return [{"kind": "user_message", "urgency": "normal", "text": "__queued__", "ts": 0.0}] * queue.qsize()

    messages: list[dict] = []
    for payload in pending:
        meta = _interrupt_payload_meta(payload)
        if meta.get("kind") == "user_message" and meta.get("text"):
            messages.append(meta)
    return messages


def _queued_user_message_count(project_id: str) -> int:
    """Count queued user messages without draining the interrupt queue."""
    return len(_queued_user_messages(project_id))


def _build_standby_wakeup_prompt(reasons: list[str], gpu_hint: str) -> str:
    lines = [
        STANDBY_WAKEUP_MARKER,
        "PhDLoop heartbeat woke you from rest (default cadence: ~30 minutes).",
        "You were resting after a completed milestone.",
        "Purpose: prevent stagnation, NOT generate busywork.",
    ]
    if reasons:
        lines.append("Wake reason(s): " + "; ".join(reasons))
    lines.extend(
        [
            "Run a lightweight triage pass:",
            "1. Read task_list.md and LOG.md to restore state.",
            "2. Pull queued advisor messages and integrate any guidance.",
            "3. Check for redundant resource usage (stale processes, duplicate jobs, avoidable re-runs).",
            "4. Decide if there is ONE meaningful next action with clear expected value right now.",
            "5. If yes: execute surgically (usually via assign_worker), then verify evidence.",
            "6. If no: call stay_idle with a brief reason and end this wake cycle.",
            "Hard rule: do not invent speculative tasks just to appear active.",
            "Hard rule: do not repeat expensive actions unless new evidence justifies rerunning.",
        ]
    )
    if gpu_hint:
        lines.append(gpu_hint)
    return "\n".join(lines)


async def _maybe_start_standby_wakeup(project_id: str) -> bool:
    """Wake a resting orchestrator only when there is a meaningful signal."""
    if not STANDBY_HEARTBEAT_ENABLED:
        return False

    project = projects.get(project_id)
    if not project:
        return False

    running = running_tasks.get(project_id)
    if running and not running.done():
        return False

    state = task_states.get(project_id)
    if not state:
        return False
    if state.status in ("running", "stopped"):
        return False

    now = time.time()
    last_wake = project_last_standby_wake_ts.get(project_id, 0.0)
    if now - last_wake < STANDBY_HEARTBEAT_SECONDS:
        return False

    queued_messages = _queued_user_messages(project_id)
    queued_user_messages = len(queued_messages)
    has_unchecked_items, unchecked_signal = _task_list_unchecked_state(project_id)
    if queued_user_messages <= 0 and not has_unchecked_items:
        return False

    signal_payload = {
        "queued": [
            {
                "text": item.get("text", ""),
                "urgency": item.get("urgency", "normal"),
                "ts": float(item.get("ts", 0.0)),
            }
            for item in queued_messages
        ],
        "task_list_unchecked_signal": unchecked_signal,
    }
    signal_hash = _hash_text(json.dumps(signal_payload, sort_keys=True, ensure_ascii=False))
    if project_last_standby_signal_hash.get(project_id) == signal_hash:
        return False

    reasons: list[str] = []
    if queued_user_messages > 0:
        reasons.append(f"{queued_user_messages} queued advisor message(s)")
    if has_unchecked_items:
        reasons.append("unchecked items in task_list.md")

    gpu_hint = await _gpu_idle_hint(project.project_dir) if has_unchecked_items else ""
    wake_prompt = _build_standby_wakeup_prompt(reasons, gpu_hint)
    project_last_standby_wake_ts[project_id] = now
    project_last_standby_signal_hash[project_id] = signal_hash
    project_last_standby_resting_hash.pop(project_id, None)

    await emit_progress(
        project_id,
        ProgressEvent(
            type="log_update",
            data=f"[heartbeat] standby wake triggered: {'; '.join(reasons)}",
            iteration=state.iteration,
        ),
    )

    task = asyncio.create_task(
        run_task(
            project_id=project_id,
            task_text=wake_prompt,
            max_iterations=STANDBY_WAKE_MAX_ITERATIONS,
            continuous_mode=False,
            provider=state.provider or DEFAULT_PROVIDER,
            model=state.model or DEFAULT_ORCHESTRATOR_MODEL,
            session_model=state.session_model or state.model or DEFAULT_SESSION_MODEL,
            permission_mode=state.permission_mode or DEFAULT_PERMISSION_MODE,
            sandbox_mode=state.sandbox_mode or DEFAULT_CODEX_SANDBOX_MODE,
            approval_policy=state.approval_policy or DEFAULT_CODEX_APPROVAL_POLICY,
        )
    )
    running_tasks[project_id] = task
    return True


async def _standby_heartbeat_loop(project_id: str):
    """Periodic heartbeat that wakes resting orchestrators only on useful signals."""
    while True:
        await asyncio.sleep(STANDBY_HEARTBEAT_SECONDS)
        if project_id not in projects:
            break
        try:
            await _maybe_start_standby_wakeup(project_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("Standby heartbeat error for %s: %s", project_id, e)


def _ensure_standby_heartbeat_task(project_id: str):
    if not STANDBY_HEARTBEAT_ENABLED:
        return
    task = standby_heartbeat_tasks.get(project_id)
    if task and not task.done():
        return
    standby_heartbeat_tasks[project_id] = asyncio.create_task(
        _standby_heartbeat_loop(project_id)
    )


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


def _build_worker_tool_specs(project_id: str, iteration: int, captured_report: list) -> list[ToolSpec]:
    """Build worker tool specifications for runtime adapters."""
    project = projects.get(project_id)
    reports_dir = Path(project.project_dir) / ".reports" if project else Path("/tmp/.reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    async def submit_report(args):
        report_text = args["report"]
        report_path = reports_dir / f"iteration-{iteration}.md"
        report_path.write_text(report_text)
        captured_report.append(report_text)
        return {
            "content": [
                {
                    "type": "text",
                    "text": f"Report submitted and saved to .reports/iteration-{iteration}.md",
                }
            ]
        }

    return [
        ToolSpec(
            name="submit_report",
            description="""Submit your work report when you've completed the assignment. This report goes directly to the orchestrator for verification.

**Required 5-section structure:**
1. **Assignment restatement**: One-line summary of what you were asked to do.
2. **What was done**: Specific actions, files modified, commands run.
3. **Results & Evidence**: Paste actual test output, diffs, verification results. Include file paths, line numbers, test counts, error messages.
4. **Acceptance criteria status**: For each goal, state DONE or NOT DONE with evidence.
5. **Open issues** (if any): Blockers, partial results, questions.

**Anti-patterns:**
- Don't say 'tests pass' without pasting actual test output.
- Don't say 'file updated' without stating the path and what changed.
- Don't omit error details — paste the traceback.""",
            input_schema={"report": str},
            handler=submit_report,
        )
    ]


def _create_worker_tools(project_id: str, iteration: int, captured_report: list):
    """Backward-compatible Claude SDK MCP wrapper used in tests."""
    return build_sdk_server(
        "worker_tools",
        _build_worker_tool_specs(project_id, iteration, captured_report),
    )


async def _forward_runtime_event(
    project_id: str,
    event: RuntimeEvent,
    iteration: int,
    source: str,
    standby_wakeup: bool = False,
) -> None:
    if event.type == "text":
        text = str(event.data or "").strip()
        source_prefix = f"[{source}]"
        clean_text = text[len(source_prefix) :].strip() if text.startswith(source_prefix) else text
        if standby_wakeup and _is_standby_resting_text(clean_text):
            resting_hash = _hash_text(" ".join(clean_text.lower().split()))
            if project_last_standby_resting_hash.get(project_id) == resting_hash:
                return
            project_last_standby_resting_hash[project_id] = resting_hash
            await emit_progress(
                project_id,
                ProgressEvent(type="log_update", data=text, iteration=iteration),
            )
            return

    await emit_progress(
        project_id,
        ProgressEvent(type=event.type, data=event.data, iteration=iteration),
    )


async def _run_worker_turn(
    project_id: str,
    assignment: str,
    iteration: int,
    worker_session_id: str = "",
    provider: str = DEFAULT_PROVIDER,
    model: str = DEFAULT_SESSION_MODEL,
    permission_mode: str = DEFAULT_PERMISSION_MODE,
    sandbox_mode: str = DEFAULT_CODEX_SANDBOX_MODE,
    approval_policy: str = DEFAULT_CODEX_APPROVAL_POLICY,
) -> tuple[str, str]:
    """Execute one worker assignment in an independent worker session."""
    project = projects.get(project_id)
    if not project:
        return "Worker failed: unknown project.", worker_session_id

    captured_report: list[str] = []
    tool_specs = _build_worker_tool_specs(project_id, iteration, captured_report)
    worker_prompt, worker_prompt_hash = _compose_runtime_prompt(project.project_dir, "worker", provider)
    worker_plugin_servers, worker_plugin_hash = _load_role_mcp_servers(project.project_dir, "worker")
    previous_hash = worker_prompt_hashes.get(project_id)
    previous_plugin_hash = worker_plugin_hashes.get(project_id)

    if previous_hash and previous_hash != worker_prompt_hash and worker_session_id:
        log.info("Worker role memory changed for %s; resetting worker session", project_id)
        worker_session_id = ""
        worker_sessions.pop(project_id, None)
    if previous_plugin_hash and previous_plugin_hash != worker_plugin_hash and worker_session_id:
        log.info("Worker MCP plugin config changed for %s; resetting worker session", project_id)
        worker_session_id = ""
        worker_sessions.pop(project_id, None)
    worker_prompt_hashes[project_id] = worker_prompt_hash
    worker_plugin_hashes[project_id] = worker_plugin_hash

    request = RuntimeRequest(
        prompt=assignment,
        project_dir=project.project_dir,
        source="worker",
        system_prompt=worker_prompt,
        session_id=worker_session_id,
        model=model,
        session_model=model,
        permission_mode=permission_mode,
        sandbox_mode=sandbox_mode,
        approval_policy=approval_policy,
        plugin_mcp_servers=worker_plugin_servers,
        tool_specs=tool_specs,
        max_turns=50,
    )

    try:
        result = await run_runtime_turn(
            provider,
            request,
            lambda event: _forward_runtime_event(project_id, event, iteration, "worker"),
        )
    except Exception as e:
        log.exception("Worker runtime error on iteration %s: %s", iteration, e)
        tb = traceback.format_exc()
        await emit_progress(
            project_id,
            ProgressEvent(
                type="tool_error",
                data=f"[worker] runtime error: {e}",
                iteration=iteration,
            ),
        )
        details = f"\nTraceback:\n{tb}" if tb else ""
        return f"Worker failed: {e}{details}", worker_session_id

    worker_session_id = result.session_id or worker_session_id
    if captured_report:
        report = captured_report[-1]
    else:
        report = result.final_text.strip() or "Worker returned without submitting a report."
    return report, worker_session_id


# -- orchestrator MCP tools -------------------------------------------


def _build_orchestrator_tool_specs(project_id: str, state: TaskState) -> list[ToolSpec]:
    """Build provider-neutral orchestrator control tools."""

    async def assign_worker(args):
        state.iteration += 1

        if state.max_iterations > 0 and state.iteration > state.max_iterations:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": (
                            f"Worker assignment limit ({state.max_iterations}) reached. "
                            "Call task_complete to summarize progress, or ask_user for guidance."
                        ),
                    }
                ],
                "is_error": True,
            }

        if cancel_events.get(project_id, asyncio.Event()).is_set():
            return {
                "content": [{"type": "text", "text": "Task has been cancelled by user."}],
                "is_error": True,
            }

        task_desc = args["task"]
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
                    provider=state.provider,
                    model=state.session_model or state.model,
                    permission_mode=state.permission_mode,
                    sandbox_mode=state.sandbox_mode,
                    approval_policy=state.approval_policy,
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

    async def stay_idle(args):
        reason = str(args.get("reason", "")).strip() or "No meaningful next action right now."
        state.status = "done"
        state.summary = f"Standby: {reason}"
        state.finished_at = time.time()

        await emit_progress(
            project_id,
            ProgressEvent(
                type="log_update",
                data=f"[orchestrator] Entering standby: {reason}",
                iteration=state.iteration,
            ),
        )
        await emit_progress(
            project_id,
            ProgressEvent(type="done", data="", iteration=state.iteration),
        )

        _save_state(
            state,
            orchestrator_session_id=orchestrator_sessions.get(project_id, ""),
            worker_session_id=worker_sessions.get(project_id, ""),
        )
        return {
            "content": [
                {
                    "type": "text",
                    "text": (
                        "Entered standby. I will resume when new advisor guidance arrives "
                        "or heartbeat detects meaningful signals."
                    ),
                }
            ]
        }

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

    async def pull_user_messages(_args):
        pending = _drain_interrupt_payloads(project_id)
        if not pending:
            return {"content": [{"type": "text", "text": "No queued user messages."}]}

        lines = []
        for idx, item in enumerate(pending, start=1):
            kind = str(item.get("kind", "user_message"))
            urgency = str(item.get("urgency", "normal"))
            text = str(item.get("text", ""))
            tag = urgency if kind == "user_message" else f"{kind}:{urgency}"
            lines.append(f"{idx}. [{tag}] {text}")

        return {
            "content": [{"type": "text", "text": "Queued user messages:\n" + "\n".join(lines)}]
        }

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
            return {
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "No user response after 10 minutes. "
                            "Proceed with your best judgment or call task_complete with current progress."
                        ),
                    }
                ]
            }

    async def update_task_list(args):
        _save_task_list(project_id, args["content"])
        await emit_progress(
            project_id,
            ProgressEvent(type="task_list", data=args["content"], iteration=state.iteration),
        )
        return {"content": [{"type": "text", "text": "Task list updated."}]}

    async def append_log(args):
        log_path = _append_log(project_id, args["entry"])
        await emit_progress(
            project_id,
            ProgressEvent(type="log_update", data=args["entry"], iteration=state.iteration),
        )
        return {"content": [{"type": "text", "text": f"Log entry appended to {log_path}"}]}

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

        worker_prompt_hashes.pop(project_id, None)
        if worker_sessions.pop(project_id, None):
            state.worker_session_id = ""
            _save_state(
                state,
                orchestrator_session_id=orchestrator_sessions.get(project_id, ""),
                worker_session_id="",
            )

        return {
            "content": [
                {
                    "type": "text",
                    "text": "Worker instructions (.claude/roles/worker.md) updated.",
                }
            ]
        }

    return [
        ToolSpec(
            name="assign_worker",
            description="""Send a concrete task to your worker agent for execution. The worker has full tool access (Edit, Write, Bash, etc). Returns the worker's report when done. Each call counts toward the iteration limit.

**When to use:** ANY task involving file edits, running commands, multi-step implementation, or investigation that requires tool access. This is your default execution path.

**When NOT to use:** Don't assign work you already did yourself. Don't assign vague tasks like 'look into this'.

**Required assignment structure:**
- Objective: what the worker must accomplish
- Acceptance criteria: how to verify success (tests to pass, output to produce)
- Context: relevant file paths, prior findings, constraints""",
            input_schema={"task": str},
            handler=assign_worker,
        ),
        ToolSpec(
            name="stay_idle",
            description="""Enter standby when triage finds no meaningful action right now. This cleanly ends the current run and waits for future wake signals (user guidance or heartbeat signals). Use this instead of repeatedly narrating that you're resting.

**When to use:** After checking task_list, queued user messages, and resource state, if there is no high-value next action now.

**Required input:** A concise reason so the advisor can understand why you chose standby.""",
            input_schema={"reason": str},
            handler=stay_idle,
        ),
        ToolSpec(
            name="task_complete",
            description="""Mark the overall user task as complete. Call this when all goals are achieved.

**When NOT to use:** Don't call if the last worker report is unverified. Don't call prematurely — ensure all acceptance criteria are met with evidence.

**Before calling:** Pull user messages first (call pull_user_messages) to check for any last-minute guidance or course corrections. If user messages are queued, integrate them before completion.""",
            input_schema={"summary": str},
            handler=task_complete,
        ),
        ToolSpec(
            name="pull_user_messages",
            description="""Fetch queued user messages with urgency metadata. Use this periodically to integrate non-urgent guidance and urgent interruptions.

**When to use:** Before planning your next step, before calling task_complete, after receiving a worker report, and whenever you suspect the user may have sent guidance.""",
            input_schema={},
            handler=pull_user_messages,
        ),
        ToolSpec(
            name="ask_user",
            description="""Ask the advisor a blocking question. This PAUSES your work until they respond (up to 10 minutes). The advisor is busy across many projects — respect their attention. If you can make a reasonable decision yourself, do it and log it.

**When to use:** Only for genuine blocking decisions where you lack the information or authority to choose (e.g., conflicting requirements, resource allocation preferences, ambiguous goals).

**Anti-patterns — do NOT ask these:**
- 'Should I proceed?' — just proceed.
- 'Can you confirm the status?' — check it yourself.
- 'Is this correct?' — verify it with evidence.
- 'I've completed X, what's next?' — check your task_list.
- Multiple questions in one call — ask ONE precise question.

**Good format:** State the decision point, the options you see, and why you can't decide without input. Example: 'The training config supports both fp16 and bf16. The GPU is A100 (supports both). Which precision do you prefer for this experiment?'""",
            input_schema={"question": str},
            handler=ask_user,
        ),
        ToolSpec(
            name="update_task_list",
            description="""Update your research plan (task_list.md). Use markdown checkboxes. PhD-level granularity: experiments, investigations, milestones — not micro-implementation steps.

**Anti-pattern:** Don't create a new task list every iteration. Read the existing one, update check marks and add/remove items as needed.""",
            input_schema={"content": str},
            handler=update_task_list,
        ),
        ToolSpec(
            name="append_log",
            description="""Append an entry to your research log (LOG.md). Your lab notebook — record hypotheses, findings, decisions, and pivots. Write at turning points, not on every action.

**Anti-pattern:** Don't log routine actions ('assigned worker', 'read file'). Log insights, evidence, decisions, and pivots.""",
            input_schema={"entry": str},
            handler=append_log,
        ),
        ToolSpec(
            name="update_worker_config",
            description="""Update standing instructions for your worker (.claude/roles/worker.md). The worker loads this role memory at the start of every assignment. This is separate from the project's root CLAUDE.md (which you should not overwrite). Use for: learned conventions, file locations, environment quirks, tool preferences. Only update when something genuinely changes.

**Anti-pattern:** Don't update on every iteration. Only update when you discover a convention or constraint the worker should know for ALL future assignments.""",
            input_schema={"content": str},
            handler=update_worker_config,
        ),
    ]



def _create_orchestrator_tools(project_id: str, state: TaskState):
    """Backward-compatible Claude SDK MCP wrapper used in tests."""
    return build_sdk_server("daemon", _build_orchestrator_tool_specs(project_id, state))


# -- main task runner --------------------------------------------------


def _orchestrator_max_turns(max_iterations: int) -> int:
    if max_iterations > 0:
        return max(8, max_iterations * 8)
    return 160


async def run_task(
    project_id: str,
    task_text: str,
    max_iterations: int = MAX_ITERATIONS,
    continuous_mode: bool = CONTINUOUS_MODE_DEFAULT,
    provider: str = DEFAULT_PROVIDER,
    model: str = DEFAULT_ORCHESTRATOR_MODEL,
    session_model: str = DEFAULT_SESSION_MODEL,
    permission_mode: str = DEFAULT_PERMISSION_MODE,
    sandbox_mode: str = DEFAULT_CODEX_SANDBOX_MODE,
    approval_policy: str = DEFAULT_CODEX_APPROVAL_POLICY,
):
    """Run autonomous task with tool-driven orchestrator."""
    os.environ.pop("CLAUDECODE", None)

    project = projects.get(project_id)
    if not project:
        log.error("Unknown project: %s", project_id)
        return

    normalized_provider = _normalize_provider(provider)
    normalized_permission_mode = _normalize_permission_mode(
        permission_mode,
        default=DEFAULT_PERMISSION_MODE,
    )
    normalized_sandbox_mode = _normalize_codex_sandbox_mode(
        sandbox_mode,
        permission_mode=normalized_permission_mode,
    )
    normalized_approval_policy = _normalize_codex_approval_policy(approval_policy)

    task_id = uuid.uuid4().hex
    state = TaskState(
        task_id=task_id,
        project_id=project_id,
        task_text=task_text,
        max_iterations=max_iterations,
        continuous_mode=continuous_mode,
        provider=normalized_provider,
        model=model,
        session_model=session_model or model,
        permission_mode=normalized_permission_mode,
        sandbox_mode=normalized_sandbox_mode,
        approval_policy=normalized_approval_policy,
    )
    task_states[project_id] = state

    _ensure_interrupt_queue(project_id)
    if project_id not in cancel_events:
        cancel_events[project_id] = asyncio.Event()
    cancel_events[project_id].clear()

    orchestrator_session_id = orchestrator_sessions.get(project_id, "")
    tool_specs = _build_orchestrator_tool_specs(project_id, state)

    await emit_progress(
        project_id,
        ProgressEvent(type="iteration", data=f"Starting task: {task_text}", iteration=0),
    )

    prompt = f"[TASK]\n{task_text}"
    is_standby_wakeup = task_text.startswith(STANDBY_WAKEUP_MARKER)

    try:
        next_prompt = prompt
        while True:
            orchestrator_prompt, orchestrator_prompt_hash = _compose_runtime_prompt(
                project.project_dir,
                "orchestrator",
                state.provider,
            )
            orchestrator_plugin_servers, orchestrator_plugin_hash = _load_role_mcp_servers(
                project.project_dir,
                "orchestrator",
            )
            previous_hash = orchestrator_prompt_hashes.get(project_id)
            previous_plugin_hash = orchestrator_plugin_hashes.get(project_id)
            if previous_hash and previous_hash != orchestrator_prompt_hash and orchestrator_session_id:
                log.info(
                    "Orchestrator role memory changed for %s; resetting orchestrator session",
                    project_id,
                )
                orchestrator_session_id = ""
                orchestrator_sessions.pop(project_id, None)
                state.orchestrator_session_id = ""
                state.sdk_session_id = ""
            if (
                previous_plugin_hash
                and previous_plugin_hash != orchestrator_plugin_hash
                and orchestrator_session_id
            ):
                log.info(
                    "Orchestrator MCP plugin config changed for %s; resetting orchestrator session",
                    project_id,
                )
                orchestrator_session_id = ""
                orchestrator_sessions.pop(project_id, None)
                state.orchestrator_session_id = ""
                state.sdk_session_id = ""
            orchestrator_prompt_hashes[project_id] = orchestrator_prompt_hash
            orchestrator_plugin_hashes[project_id] = orchestrator_plugin_hash

            prompt_for_turn = next_prompt
            pending_messages = _drain_interrupt_payloads(project_id)
            if pending_messages:
                prompt_for_turn = (
                    f"{next_prompt}\n\n{_format_queued_messages_for_prompt(pending_messages)}"
                )

            request = RuntimeRequest(
                prompt=prompt_for_turn,
                project_dir=project.project_dir,
                source="orchestrator",
                system_prompt=orchestrator_prompt,
                session_id=orchestrator_session_id,
                model=state.model,
                session_model=state.session_model,
                permission_mode=state.permission_mode,
                sandbox_mode=state.sandbox_mode,
                approval_policy=state.approval_policy,
                plugin_mcp_servers=orchestrator_plugin_servers,
                tool_specs=tool_specs,
                max_turns=_orchestrator_max_turns(max_iterations),
            )

            result = await run_runtime_turn(
                state.provider,
                request,
                lambda event: _forward_runtime_event(
                    project_id,
                    event,
                    state.iteration,
                    "orchestrator",
                    standby_wakeup=is_standby_wakeup,
                ),
            )
            sid = (result.session_id or "").strip()
            if sid:
                orchestrator_session_id = sid
                orchestrator_sessions[project_id] = sid
                state.orchestrator_session_id = sid
                state.sdk_session_id = sid

            if state.status != "running":
                break

            if not continuous_mode:
                state.status = "done"
                state.summary = "Orchestrator session ended naturally"
                state.finished_at = time.time()
                await emit_progress(
                    project_id,
                    ProgressEvent(type="done", data="", iteration=state.iteration),
                )
                break

            if not result.saw_result:
                log.warning(
                    "Orchestrator turn ended without terminal result for %s; continuing in continuous mode",
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
                "Advisor silence means 'carry on'. You own this project's progress.\n"
                "1. Read task_list.md — what's the next unchecked item?\n"
                "2. Pull user messages — any new guidance to integrate?\n"
                "3. Drive the next item to completion: assign worker, verify, update task_list.\n"
                "If all items are done, call task_complete. If no meaningful action exists now, call stay_idle."
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
        running_tasks.pop(project_id, None)
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
    standby_wakeup: bool = False,
):
    """Forward intermediate assistant output as progress events."""

    for block in message.content:
        if isinstance(block, TextBlock):
            text = block.text.strip()
            if text:
                if standby_wakeup and _is_standby_resting_text(text):
                    resting_hash = _hash_text(" ".join(text.lower().split()))
                    if project_last_standby_resting_hash.get(project_id) == resting_hash:
                        continue
                    project_last_standby_resting_hash[project_id] = resting_hash
                    await emit_progress(
                        project_id,
                        ProgressEvent(
                            type="log_update",
                            data=f"[{source}] {text}",
                            iteration=iteration,
                        ),
                    )
                    continue
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
        "provider": state.provider,
        "model": state.model,
        "session_model": state.session_model,
        "permission_mode": state.permission_mode,
        "sandbox_mode": state.sandbox_mode,
        "approval_policy": state.approval_policy,
        "continuous_mode": state.continuous_mode,
        "sdk_session_id": orchestrator_session_id,
        "orchestrator_session_id": orchestrator_session_id,
        "worker_session_id": worker_session_id,
        "orchestrator_prompt_hash": orchestrator_prompt_hashes.get(state.project_id, ""),
        "worker_prompt_hash": worker_prompt_hashes.get(state.project_id, ""),
        "orchestrator_plugin_hash": orchestrator_plugin_hashes.get(state.project_id, ""),
        "worker_plugin_hash": worker_plugin_hashes.get(state.project_id, ""),
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
    """Restore orchestrator/worker session IDs and prompt/plugin hashes from persisted state."""
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
    orch_plugin_hash = str(data.get("orchestrator_plugin_hash") or "").strip()
    worker_plugin_hash = str(data.get("worker_plugin_hash") or "").strip()
    if orch_plugin_hash:
        orchestrator_plugin_hashes[project_id] = orch_plugin_hash
    if worker_plugin_hash:
        worker_plugin_hashes[project_id] = worker_plugin_hash
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
    _ensure_standby_heartbeat_task(project_id)
    log.info("Registered project %s: %s", project_id, project_dir)
    return web.json_response({"ok": True, "project_id": project_id})


async def handle_task(request: web.Request) -> web.Response:
    """POST /task — start autonomous work on a task. Returns immediately."""
    data = await request.json()
    project_id = data.get("project_id")
    task_text = data.get("task")
    max_iter_raw = data.get("max_iterations", MAX_ITERATIONS)
    continuous_mode = _parse_bool(data.get("continuous_mode"), CONTINUOUS_MODE_DEFAULT)
    provider = _normalize_provider(data.get("provider"), default=DEFAULT_PROVIDER)
    model = str(data.get("model") or DEFAULT_ORCHESTRATOR_MODEL).strip() or DEFAULT_ORCHESTRATOR_MODEL
    session_model = str(data.get("session_model") or model).strip() or model
    permission_mode = _normalize_permission_mode(
        data.get("permission_mode"),
        default=DEFAULT_PERMISSION_MODE,
    )
    sandbox_mode = _normalize_codex_sandbox_mode(
        data.get("sandbox_mode"),
        permission_mode=permission_mode,
    )
    approval_policy = _normalize_codex_approval_policy(data.get("approval_policy"))

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
            provider=provider,
            model=model,
            session_model=session_model,
            permission_mode=permission_mode,
            sandbox_mode=sandbox_mode,
            approval_policy=approval_policy,
        )
    )
    running_tasks[project_id] = task

    return web.json_response(
        {
            "ok": True,
            "project_id": project_id,
            "status": "started",
            "continuous_mode": continuous_mode,
            "provider": provider,
            "model": model,
            "session_model": session_model,
            "permission_mode": permission_mode,
            "sandbox_mode": sandbox_mode,
            "approval_policy": approval_policy,
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
                "provider": state.provider if state else DEFAULT_PROVIDER,
                "model": state.model if state else DEFAULT_ORCHESTRATOR_MODEL,
                "session_model": state.session_model if state else DEFAULT_SESSION_MODEL,
                "permission_mode": state.permission_mode if state else DEFAULT_PERMISSION_MODE,
                "sandbox_mode": state.sandbox_mode if state else DEFAULT_CODEX_SANDBOX_MODE,
                "approval_policy": state.approval_policy if state else DEFAULT_CODEX_APPROVAL_POLICY,
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
            "provider": state.provider,
            "model": state.model,
            "session_model": state.session_model,
            "permission_mode": state.permission_mode,
            "sandbox_mode": state.sandbox_mode,
            "approval_policy": state.approval_policy,
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
    all_bg_tasks = list(standby_heartbeat_tasks.values())
    for task in all_bg_tasks:
        if task and not task.done():
            task.cancel()
    for task in all_bg_tasks:
        if not task:
            continue
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    standby_heartbeat_tasks.clear()
    project_last_standby_signal_hash.clear()
    project_last_standby_resting_hash.clear()

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
