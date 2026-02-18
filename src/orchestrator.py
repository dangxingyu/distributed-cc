"""Orchestrator: the brain that manages distributed Claude Code sessions.

Uses a single persistent Claude session per chat (via Agent SDK `query()`)
to assign tasks, evaluate worker results, and accumulate context — like a
PhD student managing research across multiple workers.

Each chat is a project channel. Workers are created on-demand when the
orchestrator needs them (via create_worker → broker /register). The
orchestrator session handles both user messages (assign/create_worker)
and worker results (verdicts).

Permission and clarification requests from remote workers are routed through
the orchestrator session, which has full task context. The orchestrator
decides approve/deny/escalate; escalations go to the human via Telegram/CLI.

The orchestrator's own Claude session uses `can_use_tool` callbacks for:
- AskUserQuestion → routed through the channel to the user
- Auto-approved tools (Read, Glob, Grep, etc.) → allowed immediately
- Everything else → escalated directly to human (no Claude intermediary)
"""

import asyncio
import json
import logging
import os
import re
import uuid

from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage
from claude_agent_sdk.types import (
    AssistantMessage,
    PermissionResultAllow,
    PermissionResultDeny,
    TextBlock,
    ToolUseBlock,
    ToolResultBlock,
)

from .session import SessionManager, SessionResult
from .store import Store, TaskStatus, ChannelWorker
from .models import WorkItem, WorkPlan
from .formatter import (
    format_task_status,
    format_channel_orchestrator,
    format_channel_dispatch,
    format_channel_worker_result,
    format_channel_worker_created,
    format_channel_plan_created,
    format_channel_plan_summary,
)

log = logging.getLogger(__name__)

# Safe tools that are auto-approved for worker permission requests
WORKER_SAFE_TOOLS = {"Read", "Grep", "Glob", "WebSearch", "WebFetch", "Explore"}

# Message routing patterns
_ORCH_PREFIX_RE = re.compile(r"^@orchestrator\s+", re.IGNORECASE)
_STOP_CMD_RE = re.compile(r"^(?:@orchestrator\s+)?/stop\s*$", re.IGNORECASE)


async def _prompt_stream(text: str):
    """Wrap a string prompt into an AsyncIterable for the Agent SDK.

    Required when can_use_tool is set — the SDK needs streaming mode.
    """
    yield {
        "type": "user",
        "session_id": "",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
    }


def _format_questions(questions: list[dict]) -> str:
    """Format AskUserQuestion questions for display to the user."""
    lines = []
    for i, q in enumerate(questions, 1):
        opts = q.get("options", [])
        opt_labels = ", ".join(o.get("label", "?") for o in opts)
        lines.append(f"{i}. {q.get('question', '?')}")
        if opt_labels:
            lines.append(f"   Options: {opt_labels}")
    return "\n".join(lines)


ORCHESTRATOR_SYSTEM_PROMPT = """\
You are an orchestrator managing distributed Claude Code sessions across remote servers.
You act like a PhD student managing a research project — you receive direction from the \
user (professor), assign work to Claude Code workers, evaluate their reports, and decide \
next steps autonomously. If the professor doesn't respond, figure it out yourself.

You are in a project channel. Each channel is one project.

SETUP: At the start of a conversation, read `{config_path}` for available servers. Also \
check for `config.md` and `setup.md` in the same directory for extra context.

=== RESPONSE FORMAT ===

You have two modes:

**Tool mode** — Use your tools (Bash, Read, etc.) freely for /setup and any investigation \
you need (checking server status, reading files, etc.). When done, emit a SINGLE JSON action.

**JSON mode** — For all decisions (assign, reply, verdict, etc.), respond with EXACTLY ONE \
JSON object. No preamble, no commentary, no markdown fences — just the JSON.

CRITICAL: Never output multiple JSON objects. Never wrap JSON in text. If you used tools, \
your final text output must be the single JSON action and nothing else.

You CAN use EnterPlanMode for complex tasks that need planning before execution. \
You CAN use AskUserQuestion for structured questions (multiple-choice, preferences). \
For simple questions, {{"action": "reply", "text": "your question"}} works too.

=== CHANNEL WORKERS ===

Each message starts with a [CHANNEL WORKERS] block listing workers attached to this channel. \
Workers are your implementation hands — each is a Claude Code session on a remote server.

When you create_worker, it is AUTOMATICALLY attached to this channel. You don't need a \
separate step to "pull it in" — it immediately appears in [CHANNEL WORKERS].

=== ACTIONS ===

**Reply** — answer questions, give status, ask the user something (supports markdown):
{{"action": "reply", "text": "<markdown text>"}}

**Create worker** — spin up a new Claude Code session (auto-attaches to this channel):
{{"action": "create_worker", "server": "<server_name>", "work_dir": "<absolute path>", "description": "<brief>"}}
You need server name + work_dir. Ask via reply if not obvious.

**Assign task** — send work to an existing worker:
{{"action": "assign", "server": "<server>", "session": "<session_id>", "prompt": "<detailed prompt>"}}
The session must match a worker from [CHANNEL WORKERS].

**Plan** — multiple independent tasks across workers (rare — only for large parallel workstreams):
{{"action": "plan", "tasks": [
    {{"id": "t1", "description": "...", "server": "...", "session": "...", "prompt": "...", "depends_on": []}},
    {{"id": "t2", "description": "...", "server": "...", "session": "...", "prompt": "...", "depends_on": ["t1"]}}
]}}

**Register server** — make a server available (usually after /setup):
{{"action": "register_server", "name": "<name>", "host": "<user@host>", "broker_port": <port>}}

**Show tasks**: {{"action": "show_tasks"}}

=== ASSIGNING RULES ===

- Default to "assign". Workers are capable — give them the full task, don't micromanage.
- NEVER decompose a single task into subtasks. One assign = one complete job.
- "plan" is ONLY for genuinely parallel workstreams across different workers.
- Write clear, detailed prompts. Include relevant context from config/setup docs.

=== WORKER RESULTS ===

[WORKER RESULT] messages report what a worker produced. Evaluate and respond:

{{"action": "verdict", "task_id": "<id>", "status": "done", "summary": "<for the user>"}}
{{"action": "verdict", "task_id": "<id>", "status": "retry", "feedback": "<actionable feedback>"}}
{{"action": "verdict", "task_id": "<id>", "status": "failed", "summary": "<what went wrong>"}}
{{"action": "verdict", "task_id": "<id>", "status": "retry_different", "new_prompt": "<new approach>"}}
{{"action": "verdict", "task_id": "<id>", "status": "escalate", "question": "<question for user>"}}

Optional on "done": "suggestions": "<follow-up ideas>", \
"new_tasks": [{{...task objects...}}]

Rules: Focus on the CORE OBJECTIVE. Don't nitpick. Retry only if the main goal was missed. \
Escalate only when you genuinely need the user's judgment.

=== PERMISSION & CLARIFICATION REQUESTS ===

[PERMISSION REQUEST] — a worker wants to use a tool. Decide:
{{"action": "permission_decision", "approved": true, "reason": "..."}}
{{"action": "permission_decision", "approved": false, "reason": "..."}}
{{"action": "permission_decision", "escalate": true, "reason": "..."}}

Approve if aligned with the task. Deny if destructive/off-task. Escalate if ambiguous.

[PERMISSION REQUEST (FORCED)] — no escalate option, you must decide. Lean towards approve \
for normal development work.

[CLARIFICATION REQUEST] — a worker has a question:
{{"action": "clarification_answer", "answers": {{"<question>": "<answer>"}}, "reason": "..."}}
{{"action": "clarification_answer", "escalate": true, "reason": "..."}}

[CLARIFICATION REQUEST (FORCED)] — you must answer, no escalate.

=== CHANNEL NOTES ===

[CHANNEL NOTES] are ambient observations from the user — preferences, reminders. Not \
direct requests. Acknowledge naturally within your response.

=== STOP REQUEST ===

[STOP REQUESTED] — halt work and summarize what was in progress.
{{"action": "reply", "text": "<summary of what was stopped>"}}

=== /setup ===

Set up server connections. Use Bash to SSH in, install broker, start it, open tunnels, \
and verify. Read `{config_path}` and `config.md` for server details.

For each remote server:
1. Check/install broker: `ssh <host> 'test -f ~/.distributed-cc/remote_broker.py && echo ok || \
curl -fsSL https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools/install-broker.sh | bash'`
2. Start in tmux if not running: \
`ssh <host> "tmux new-session -d -s dcc-broker '~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/remote_broker.py --port 8200 --name <name>'"`
3. SSH tunnel (skip if host is null/local): \
`ssh -fN -L <local_port>:localhost:8200 -R 9120:localhost:9120 <host>` \
Port allocation: 8201, 8202, etc.
4. Verify: `curl -sf http://localhost:<local_port>/health`
5. Register: {{"action": "register_server", "name": "...", "host": "...", "broker_port": ...}}

Then summarize with {{"action": "reply", "text": "..."}}.
"""


class Orchestrator:
    def __init__(
        self,
        session_mgr: SessionManager,
        store: Store,
        model: str = "claude-opus-4-6",
        config_path: str = "config.yaml",
        orch_config: dict | None = None,
    ):
        self._session_mgr = session_mgr
        self._store = store
        self._model = model
        self._config_path = config_path
        self._send_telegram: callable = None
        self._system_prompt = ORCHESTRATOR_SYSTEM_PROMPT.format(config_path=config_path)
        # Per-chat orchestrator session IDs for --resume
        self._orchestrator_sessions: dict[int, str] = {}

        # Tool permission config (from config.yaml orchestrator.permissions)
        perm_cfg = (orch_config or {}).get("permissions", {})
        self._auto_approve_tools: set[str] = set(
            perm_cfg.get("auto_approve", ["Read", "Glob", "Grep", "WebSearch", "WebFetch", "Bash", "Write", "Edit"])
        )
        self._denied_tools: set[str] = set(perm_cfg.get("deny", []))

        # AskUserQuestion routing: store send_reply per chat during _send_to_orchestrator
        self._active_reply_fns: dict[int, callable] = {}
        # Pending answers: chat_id -> asyncio.Future for user responses to orchestrator questions
        self._pending_answers: dict[int, asyncio.Future] = {}

        # Escalation plumbing (absorbed from PermissionEvaluator)
        # request_id -> asyncio.Future for pending human decisions
        self._pending: dict[str, asyncio.Future] = {}
        # request_id -> question metadata (for clarification buttons)
        self._pending_meta: dict[str, dict] = {}

        # Reverse index: (server, session_id) → chat_id
        self._worker_to_chat: dict[tuple[str, str], int] = {}

        # Per-chat session locks to prevent concurrent orchestrator access
        self._session_locks: dict[int, asyncio.Lock] = {}

        # Per-chat message queues and queue processor tasks
        self._message_queues: dict[int, asyncio.Queue] = {}
        self._queue_tasks: dict[int, asyncio.Task] = {}

        # Per-chat active orchestrator tasks (for /stop cancellation)
        self._active_tasks: dict[int, set[asyncio.Task]] = {}

        # Per-chat status callbacks and state
        self._status_callbacks: dict[int, callable] = {}
        self._chat_status: dict[int, str] = {}  # "busy" | "idle"

    async def init(self):
        """Initialize: populate worker-to-chat reverse index from store."""
        for chat_id in await self._store.get_all_channel_ids():
            workers = await self._store.get_channel_workers(chat_id)
            for w in workers:
                self._worker_to_chat[(w.server, w.session_id)] = chat_id

    def set_send_telegram(self, fn):
        """Inject the Telegram send function (set by Bot after wiring)."""
        self._send_telegram = fn

    def set_status_callback(self, chat_id: int, callback: callable):
        """Register a status callback for a chat (called with 'busy'/'idle')."""
        self._status_callbacks[chat_id] = callback

    def remove_status_callback(self, chat_id: int):
        self._status_callbacks.pop(chat_id, None)

    async def _set_status(self, chat_id: int, status: str):
        """Update chat status and notify via callback."""
        if self._chat_status.get(chat_id) == status:
            return  # no-op if unchanged
        self._chat_status[chat_id] = status
        cb = self._status_callbacks.get(chat_id)
        if cb:
            try:
                await cb(status)
                log.info(f"Chat {chat_id} status → {status}")
            except Exception:
                log.warning(f"Status callback failed for chat {chat_id}", exc_info=True)
        else:
            log.info(f"Chat {chat_id} status → {status} (no callback registered)")

    # ── Human resolution (moved from PermissionEvaluator) ──────────────

    def resolve_permission(self, request_id: str, approved: bool, reason: str = "") -> bool:
        """Resolve a pending permission escalation."""
        future = self._pending.pop(request_id, None)
        self._pending_meta.pop(request_id, None)
        if future and not future.done():
            future.set_result({"approved": approved, "reason": reason})
            return True
        return False

    def resolve_clarification(self, request_id: str, question: str, answer: str) -> bool:
        """Resolve a pending clarification with a specific answer."""
        future = self._pending.pop(request_id, None)
        self._pending_meta.pop(request_id, None)
        if future and not future.done():
            answers = {question: answer}
            future.set_result({"answers": answers})
            return True
        return False

    def get_pending_questions(self, request_id: str) -> list[dict] | None:
        """Get the questions metadata for a pending clarification."""
        meta = self._pending_meta.get(request_id)
        return meta["questions"] if meta else None

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    # ── Message routing ─────────────────────────────────────────────────

    async def route_message(
        self,
        chat_id: int,
        raw_text: str,
        send_reply: callable,
        default_direct: bool = False,
        send_log: callable = None,
    ):
        """Route an incoming message based on prefix.

        - @orchestrator /stop  → cancel running tasks
        - @orchestrator <text> → direct message (queued if busy)
        - No prefix + default_direct → direct message
        - No prefix + not default_direct → channel note
        """
        stripped = raw_text.strip()

        # Check if this answers a pending orchestrator question (avoid deadlock:
        # the queue processor is blocked awaiting the future, so the answer would
        # never be dequeued — resolve it directly here instead).
        pending = self._pending_answers.get(chat_id)
        if pending and not pending.done():
            pending.set_result(stripped)
            return

        # @orchestrator /stop
        if _STOP_CMD_RE.match(stripped):
            await self._handle_stop(chat_id, send_reply)
            return

        # @orchestrator <text>
        m = _ORCH_PREFIX_RE.match(stripped)
        if m:
            text = stripped[m.end():]
            await self._enqueue_direct_message(chat_id, text, send_reply, send_log)
            return

        # No prefix
        if default_direct:
            await self._enqueue_direct_message(chat_id, stripped, send_reply, send_log)
        else:
            await self._store.add_note(chat_id, stripped)
            await send_reply("(noted)")

    async def _enqueue_direct_message(
        self, chat_id: int, text: str, send_reply: callable,
        send_log: callable = None,
    ):
        """Queue a direct message for the orchestrator. Ack if lock is held."""
        if chat_id not in self._message_queues:
            self._message_queues[chat_id] = asyncio.Queue()

        self._message_queues[chat_id].put_nowait((text, send_reply, send_log))

        # If the session lock is currently held, ack that we queued it
        lock = self._get_session_lock(chat_id)
        if lock.locked():
            await send_reply("(queued)")

        self._ensure_queue_processor(chat_id)

    def _ensure_queue_processor(self, chat_id: int):
        """Start the queue processor task if not already running."""
        task = self._queue_tasks.get(chat_id)
        if task is None or task.done():
            t = asyncio.create_task(self._process_queue(chat_id))
            self._queue_tasks[chat_id] = t
            self._track_task(chat_id, t)

    async def _process_queue(self, chat_id: int):
        """Drain the message queue, sending each to handle_message."""
        queue = self._message_queues.get(chat_id)
        if queue is None:
            return
        await self._set_status(chat_id, "busy")
        try:
            while not queue.empty():
                text, send_reply, send_log = queue.get_nowait()
                await self.handle_message(chat_id, text, send_reply, send_log)
        finally:
            await self._set_status(chat_id, "idle")

    def _track_task(self, chat_id: int, task: asyncio.Task):
        """Register an active task for cancellation via /stop."""
        if chat_id not in self._active_tasks:
            self._active_tasks[chat_id] = set()
        self._active_tasks[chat_id].add(task)
        task.add_done_callback(lambda t: self._active_tasks.get(chat_id, set()).discard(t))

    async def _handle_stop(self, chat_id: int, send_reply: callable):
        """Cancel running worker tasks AND orchestrator background tasks."""
        await self._store.add_message(chat_id, "user", "/stop")

        # Cancel worker tasks
        tasks = await self._store.get_running_tasks(chat_id)
        cancelled_workers = 0
        for t in tasks:
            ok = await self._session_mgr.cancel_task(t.server_name, t.session_id)
            if ok:
                cancelled_workers += 1
                await self._store.finish_task(t.id, TaskStatus.FAILED, "Stopped by user")

        # Cancel orchestrator background tasks (e.g. /setup, pending queries)
        active = self._active_tasks.get(chat_id, set())
        cancelled_orch = 0
        for t in list(active):
            if not t.done():
                t.cancel()
                cancelled_orch += 1
        active.clear()

        # Cancel pending AskUserQuestion futures (avoid stale captures)
        pending = self._pending_answers.pop(chat_id, None)
        if pending and not pending.done():
            pending.cancel()

        # Drain the message queue
        queue = self._message_queues.get(chat_id)
        if queue:
            while not queue.empty():
                queue.get_nowait()

        parts = []
        if cancelled_workers or tasks:
            parts.append(f"{cancelled_workers}/{len(tasks)} worker tasks")
        if cancelled_orch:
            parts.append(f"{cancelled_orch} orchestrator operations")
        summary = ", ".join(parts) if parts else "nothing running"
        await send_reply(f"(stop: cancelled {summary})")

        # Notify orchestrator session (fire-and-forget)
        asyncio.create_task(
            self._send_to_orchestrator(chat_id, "[STOP REQUESTED]\nThe user has stopped all running tasks.")
        )

    # ── User message handling ──────────────────────────────────────────

    async def handle_message(
        self,
        chat_id: int,
        user_text: str,
        send_reply: callable,
        send_log: callable = None,
    ):
        """Process a user message end-to-end."""
        # Check if this message is an answer to a pending orchestrator question
        pending = self._pending_answers.get(chat_id)
        if pending and not pending.done():
            pending.set_result(user_text)
            return

        await self._store.add_message(chat_id, "user", user_text)

        # Prepend channel workers context
        workers = await self._store.get_channel_workers(chat_id)
        context = self._format_channel_workers(workers)
        augmented_text = f"{context}\n[USER MESSAGE]\n{user_text}"

        # Store send_reply so the can_use_tool callback can route questions
        self._active_reply_fns[chat_id] = send_reply
        try:
            decision = await self._send_to_orchestrator(
                chat_id, augmented_text, send_reply, send_log
            )
        finally:
            self._active_reply_fns.pop(chat_id, None)

        if decision is None:
            await send_reply("Sorry, I couldn't understand that. Try again?")
            return

        await self._handle_decision(chat_id, user_text, decision, send_reply, send_log)

    async def _reply(self, chat_id: int, send_reply: callable, text: str):
        """Send a reply to the user AND persist it to the store."""
        await self._store.add_message(chat_id, "assistant", text)
        await send_reply(text)

    async def _handle_decision(
        self,
        chat_id: int,
        user_text: str,
        decision: dict,
        send_reply: callable,
        send_log: callable = None,
    ):
        """Dispatch on orchestrator decision. May recurse for create_worker follow-ups."""
        action = decision.get("action")

        if action == "reply":
            text = decision.get("text", "...")
            await self._reply(chat_id, send_reply, format_channel_orchestrator(text))

        elif action == "show_tasks":
            tasks = await self._store.get_running_tasks(chat_id)
            await self._reply(chat_id, send_reply, format_task_status(tasks))

        elif action == "create_worker":
            await self._handle_create_worker(chat_id, user_text, decision, send_reply, send_log)

        elif action == "assign":
            server = decision.get("server", "")
            session = decision.get("session", "")
            prompt = decision.get("prompt", user_text)

            plan = WorkPlan(
                id=uuid.uuid4().hex[:8],
                chat_id=chat_id,
                user_message=user_text,
                items=[WorkItem(
                    id="t1",
                    description=user_text[:200],
                    server=server,
                    session=session,
                    prompt=prompt,
                )],
            )
            await self._reply(chat_id, send_reply, format_channel_dispatch(server, session, prompt))
            t = asyncio.create_task(self._execute_plan(plan, send_reply, send_log))
            self._track_task(chat_id, t)

        elif action == "plan":
            tasks_data = decision.get("tasks", [])
            if not tasks_data:
                await self._reply(chat_id, send_reply, format_channel_orchestrator("Plan has no tasks."))
                return

            plan = WorkPlan(
                id=uuid.uuid4().hex[:8],
                chat_id=chat_id,
                user_message=user_text,
                items=[
                    WorkItem(
                        id=t["id"],
                        description=t.get("description", t.get("prompt", "")[:200]),
                        server=t["server"],
                        session=t["session"],
                        prompt=t["prompt"],
                        depends_on=t.get("depends_on", []),
                    )
                    for t in tasks_data
                ],
            )
            await self._reply(chat_id, send_reply, format_channel_plan_created(plan))
            t = asyncio.create_task(self._execute_plan(plan, send_reply, send_log))
            self._track_task(chat_id, t)

        elif action == "register_server":
            await self._handle_register_server(chat_id, decision, send_reply)

        else:
            await self._reply(chat_id, send_reply, format_channel_orchestrator(f"Unknown action: {action}"))

    async def _handle_create_worker(
        self,
        chat_id: int,
        user_text: str,
        decision: dict,
        send_reply: callable,
        send_log: callable = None,
    ):
        """Handle create_worker action: register on broker, store in DB, confirm to orchestrator."""
        server_name = decision.get("server", "")
        work_dir = decision.get("work_dir", "")
        description = decision.get("description", "")

        if not server_name or not work_dir:
            await self._reply(chat_id, send_reply, format_channel_orchestrator("Cannot create worker: missing server or work_dir."))
            return

        # Generate session_id from work_dir basename
        session_id = work_dir.rstrip("/").split("/")[-1]

        # Register on broker
        result = await self._session_mgr.register_session(server_name, session_id, work_dir, description)
        if not result.get("ok"):
            error = result.get("error", "Unknown error")
            await self._reply(chat_id, send_reply, format_channel_orchestrator(f"Failed to create worker: {error}"))
            return

        # Store in DB
        await self._store.add_channel_worker(chat_id, server_name, session_id, work_dir, description)
        # Update reverse index
        self._worker_to_chat[(server_name, session_id)] = chat_id
        await self._reply(chat_id, send_reply, format_channel_worker_created(server_name, session_id, work_dir))

        # Confirm back to orchestrator session so it can continue
        workers = await self._store.get_channel_workers(chat_id)
        confirmation = (
            f"[WORKER CREATED] {server_name}/{session_id} at {work_dir} — ready\n\n"
            f"{self._format_channel_workers(workers)}\n"
            f"Continue with the user's original request."
        )
        follow_up = await self._send_to_orchestrator(chat_id, confirmation, send_reply, send_log)
        if follow_up:
            await self._handle_decision(chat_id, user_text, follow_up, send_reply, send_log)

    async def _handle_register_server(
        self, chat_id: int, decision: dict, send_reply: callable,
    ):
        """Handle register_server action: dynamically add a server to SessionManager."""
        name = decision.get("name", "")
        host = decision.get("host")
        broker_port = decision.get("broker_port", 8200)

        if not name:
            await self._reply(chat_id, send_reply, format_channel_orchestrator("Cannot register server: missing name."))
            return

        from .session import ServerConfig
        config = ServerConfig(
            name=name,
            host=host,
            broker_port=broker_port,
        )
        self._session_mgr.add_server(config)

        # Verify health
        healthy = await self._session_mgr.check_health(name)
        status = "healthy" if healthy else "unreachable"
        await self._reply(chat_id, send_reply, format_channel_orchestrator(
            f"Server `{name}` registered (broker port {broker_port}, {status}). Ready for workers."
        ))

    # ── Plan execution ─────────────────────────────────────────────────

    async def _execute_plan(
        self, plan: WorkPlan, send_reply: callable, send_log: callable = None,
    ):
        """Execute a work plan: run items respecting dependencies, send
        each result to the orchestrator session for evaluation."""
        try:
            while True:
                # Find items ready to run
                done_ids = {i.id for i in plan.items if i.status == "done"}
                failed_ids = {i.id for i in plan.items if i.status == "failed"}

                ready = [
                    i for i in plan.items
                    if i.status == "pending"
                    and all(d in done_ids for d in i.depends_on)
                ]

                # Check if we're stuck (deps on failed items)
                stuck = [
                    i for i in plan.items
                    if i.status == "pending"
                    and any(d in failed_ids for d in i.depends_on)
                ]
                for item in stuck:
                    item.status = "failed"
                    item.feedback = "Dependency failed"

                if not ready:
                    running = [i for i in plan.items if i.status == "running"]
                    if not running:
                        break
                    await asyncio.sleep(1)
                    continue

                # Execute ready items sequentially
                for item in ready:
                    await self._execute_item(plan, item, send_reply, send_log)

            # All done — send summary
            all_done = all(i.status == "done" for i in plan.items)
            plan.status = "completed" if all_done else "failed"

            summary = format_channel_plan_summary(plan)
            await self._reply(plan.chat_id, send_reply, summary)

        except Exception as e:
            log.exception(f"Plan {plan.id} failed: {e}")
            plan.status = "failed"
            await self._reply(plan.chat_id, send_reply, f"Plan execution failed: {e}")

    async def _execute_item(
        self, plan: WorkPlan, item: WorkItem, send_reply: callable,
        send_log: callable = None,
    ):
        """Execute a single work item: run worker, send result to orchestrator, handle verdict."""
        item.status = "running"

        task_id = await self._store.create_task(
            plan.chat_id, item.description, item.server, item.session, item.prompt
        )

        # Build prompt with upstream context and retry feedback
        upstream_context = self._build_upstream_context(plan, item)
        prompt = item.prompt
        if upstream_context:
            prompt = upstream_context + prompt
        if item.feedback:
            prompt = (
                f"PREVIOUS ATTEMPT FEEDBACK: {item.feedback}\n\n"
                f"Please address the feedback above and complete the task:\n\n"
                f"{prompt}"
            )

        try:
            result: SessionResult = await self._session_mgr.run_task(
                item.server, item.session, prompt, task_id=task_id
            )

            if result.is_error:
                await self._store.finish_task(task_id, TaskStatus.FAILED, result.result_text)
                item.status = "failed"
                item.result = result.result_text
                await self._reply(plan.chat_id, send_reply, format_channel_worker_result(item, "failed"))
                return

            await self._store.finish_task(task_id, TaskStatus.DONE, result.result_text)
            item.result = result.result_text

            # Send result to orchestrator session for evaluation
            worker_report = self._format_worker_result(item, plan, is_error=False)
            verdict = await self._send_to_orchestrator(plan.chat_id, worker_report, send_reply, send_log)

            if verdict is None or verdict.get("action") != "verdict":
                # Orchestrator didn't return a valid verdict — assume done
                log.warning("No valid verdict from orchestrator, assuming done")
                item.status = "done"
                await self._reply(plan.chat_id, send_reply, format_channel_worker_result(item, "done"))
                return

            status = verdict.get("status", "done")

            if status == "done":
                item.status = "done"
                cost_info = ""
                if result.cost_usd:
                    cost_info += f"\n(cost: ${result.cost_usd:.4f})"
                if result.duration_secs:
                    cost_info += f" ({result.duration_secs:.0f}s)"
                await self._reply(plan.chat_id, send_reply, format_channel_worker_result(item, "done") + cost_info)
                # Handle suggestions
                suggestions = verdict.get("suggestions", "")
                if suggestions:
                    await self._reply(plan.chat_id, send_reply, format_channel_orchestrator(
                        f"Suggested next steps: {suggestions}"
                    ))

            elif status == "retry" and item.retries < item.max_retries:
                item.retries += 1
                item.feedback = verdict.get("feedback", "Please try again")
                item.status = "pending"
                await self._reply(plan.chat_id, send_reply, format_channel_worker_result(item, "retry"))

            elif status == "retry_different" and item.approach_changes < item.max_approach_changes:
                item.approach_changes += 1
                new_prompt = verdict.get("new_prompt")
                if new_prompt:
                    item.prompt = new_prompt
                    item.retries = 0
                    item.feedback = None
                    item.status = "pending"
                    await self._reply(plan.chat_id, send_reply, format_channel_worker_result(item, "retry_different"))
                else:
                    item.status = "failed"
                    item.feedback = verdict.get("summary", "No new approach provided")
                    await self._reply(plan.chat_id, send_reply, format_channel_worker_result(item, "failed"))

            elif status == "escalate":
                question = verdict.get("question", "Need your input on this task.")
                await self._reply(plan.chat_id, send_reply, format_channel_orchestrator(
                    f"Task {item.id} needs your input: {question}"
                ))
                item.status = "failed"
                item.feedback = f"Escalated: {question}"

            else:
                # failed, or retry/approach_change exhausted
                item.status = "failed"
                item.feedback = verdict.get("summary", verdict.get("feedback", "Task failed"))
                await self._reply(plan.chat_id, send_reply, format_channel_worker_result(item, "failed"))

            # Add any new tasks discovered
            for new_task in verdict.get("new_tasks", []):
                if any(i.id == new_task["id"] for i in plan.items):
                    continue
                plan.items.append(WorkItem(
                    id=new_task["id"],
                    description=new_task.get("description", new_task.get("prompt", "")[:200]),
                    server=new_task["server"],
                    session=new_task["session"],
                    prompt=new_task["prompt"],
                    depends_on=new_task.get("depends_on", []),
                ))

        except Exception as e:
            log.exception(f"Work item {item.id} failed: {e}")
            await self._store.finish_task(task_id, TaskStatus.FAILED, str(e))
            item.status = "failed"
            item.result = str(e)
            await self._reply(plan.chat_id, send_reply, format_channel_worker_result(item, "failed"))

    # ── Orchestrator session (Agent SDK) ──────────────────────────────

    def _get_session_lock(self, chat_id: int) -> asyncio.Lock:
        """Get or create a per-chat session lock."""
        if chat_id not in self._session_locks:
            self._session_locks[chat_id] = asyncio.Lock()
        return self._session_locks[chat_id]

    async def _send_to_orchestrator(
        self, chat_id: int, text: str, send_reply: callable = None,
        send_log: callable = None,
    ) -> dict | None:
        """Send a message to the orchestrator Claude session and parse the JSON response.

        Uses Agent SDK query() with can_use_tool for permission handling
        and AskUserQuestion routing. Resumes the existing session for this
        chat, or starts a new one with the system prompt.

        Serialized per-chat via session lock. Injects unchecked notes before sending.

        If send_reply is provided, intermediate text and tool activity from the
        orchestrator's Claude session will be forwarded to the user in real-time.
        If send_log is provided, intermediate output goes to the monitor panel
        instead of the chat (falling back to send_reply when send_log is None).
        """
        async with self._get_session_lock(chat_id):
            # Inject unchecked channel notes
            notes = await self._store.get_unchecked_notes(chat_id)
            if notes:
                note_lines = "\n".join(f"- {n['content']}" for n in notes)
                text = f"[CHANNEL NOTES]\n{note_lines}\n\n{text}"
                await self._store.mark_notes_checked(chat_id)

            return await self._send_to_orchestrator_unlocked(
                chat_id, text, send_reply, send_log
            )

    async def _send_to_orchestrator_unlocked(
        self, chat_id: int, text: str, send_reply: callable = None,
        send_log: callable = None,
    ) -> dict | None:
        """Inner implementation of _send_to_orchestrator (no lock)."""
        # Clear nested-session guard so SDK can spawn claude subprocess
        os.environ.pop("CLAUDECODE", None)

        options = ClaudeAgentOptions(
            can_use_tool=self._make_can_use_tool(chat_id),
            model=self._model,
            cwd=os.path.dirname(os.path.abspath(self._config_path)),
            # Auto-approve common tools at the CLI level (bypasses permission
            # prompt entirely — faster than round-tripping through can_use_tool).
            # AskUserQuestion is NOT listed so it still goes through can_use_tool.
            allowed_tools=list(self._auto_approve_tools),
            # Block tools the orchestrator shouldn't use (uses JSON actions instead)
            disallowed_tools=[],
            # Disable sandbox so orchestrator can use SSH, curl, etc.
            sandbox={"enabled": False},
        )

        session_id = self._orchestrator_sessions.get(chat_id)
        if session_id:
            options.resume = session_id
        else:
            options.system_prompt = self._system_prompt

        try:
            result_text = ""
            async for message in query(
                prompt=_prompt_stream(text), options=options
            ):
                if isinstance(message, AssistantMessage):
                    await self._forward_assistant_message(
                        chat_id, message, send_reply, send_log
                    )
                elif isinstance(message, ResultMessage):
                    result_text = message.result or ""
                    self._orchestrator_sessions[chat_id] = message.session_id

            # Parse JSON — strip markdown fences if present
            clean = result_text.strip()
            if clean.startswith("```"):
                # Handle ```json or ``` fence
                first_newline = clean.index("\n") if "\n" in clean else len(clean)
                clean = clean[first_newline + 1:]
            if clean.endswith("```"):
                clean = clean.rsplit("```", 1)[0]
            clean = clean.strip()

            try:
                return json.loads(clean)
            except json.JSONDecodeError:
                # Try to extract embedded JSON action from preamble text
                # e.g. "Some commentary...\n{"action": "reply", ...}"
                idx = clean.find('{"action"')
                if idx > 0:
                    try:
                        return json.loads(clean[idx:])
                    except json.JSONDecodeError:
                        pass
                # Model returned plain text instead of JSON — treat as a reply
                log.warning("Orchestrator returned plain text, wrapping as reply: %s", result_text[:200])
                return {"action": "reply", "text": result_text.strip()}
        except Exception as e:
            log.exception("Orchestrator error: %s", e)
            return None

    async def _forward_assistant_message(
        self, chat_id: int, message: AssistantMessage,
        send_reply: callable = None, send_log: callable = None,
    ):
        """Extract and forward useful content from an AssistantMessage.

        Forwards non-JSON text blocks and tool use blocks as orchestrator
        activity. When send_log is provided, output goes to the monitor
        panel (ephemeral, not persisted). Otherwise falls back to send_reply.
        """
        log_fn = send_log or send_reply
        if log_fn is None:
            return

        for block in message.content:
            if isinstance(block, TextBlock):
                text = block.text.strip()
                if not text:
                    continue
                # Skip if this looks like a raw JSON action (will be parsed later)
                if text.startswith("{") and text.endswith("}"):
                    try:
                        json.loads(text)
                        continue  # valid JSON action — handled by caller
                    except json.JSONDecodeError:
                        pass
                # Strip JSON code fences (model sometimes wraps action in commentary)
                # e.g. "Some text...\n```json\n{...}\n```"
                if '{"action"' in text:
                    # Extract only the text before the JSON block
                    for marker in ("```json", "```", '{"action"'):
                        idx = text.find(marker)
                        if idx >= 0:
                            text = text[:idx].strip()
                            break
                    if not text:
                        continue
                # Forward non-JSON text as orchestrator commentary
                formatted = format_channel_orchestrator(text)
                await log_fn(formatted)

            elif isinstance(block, ToolUseBlock):
                # Show tool activity as a system-style message
                tool_msg = f"orchestrator -> {block.name}"
                if block.name in ("Bash", "Write", "Edit"):
                    # Include a snippet of the input for context
                    snippet = json.dumps(block.input, ensure_ascii=False)[:200]
                    tool_msg += f": {snippet}"
                await log_fn(tool_msg)

            elif isinstance(block, ToolResultBlock):
                # Only show errors in monitor (success results are too noisy)
                if block.is_error:
                    content = block.content if isinstance(block.content, str) else str(block.content or "")
                    await log_fn(f"[ERROR] {content[:500]}")

    def _make_can_use_tool(self, chat_id: int):
        """Create a can_use_tool callback for the orchestrator session.

        Handles:
        - AskUserQuestion → route through channel to user
        - Auto-approved tools → allow immediately
        - Denied tools → deny immediately
        - Everything else → escalate directly to human (no Claude intermediary)
        """

        async def can_use_tool(tool_name: str, input_data: dict, context=None):
            # AskUserQuestion → route through channel
            if tool_name == "AskUserQuestion":
                log.info("[can_use_tool] %s → routing question", tool_name)
                return await self._handle_orchestrator_question(chat_id, input_data)

            # Config-based auto-approve
            if tool_name in self._auto_approve_tools:
                log.info("[can_use_tool] %s → auto-approve", tool_name)
                return PermissionResultAllow()

            # Config-based deny
            if tool_name in self._denied_tools:
                log.info("[can_use_tool] %s → deny (config)", tool_name)
                return PermissionResultDeny(
                    message=f"Tool {tool_name} not allowed for orchestrator"
                )

            # Escalate directly to human (the orchestrator IS the Claude session)
            log.info("[can_use_tool] %s → escalate to human", tool_name)
            return await self._escalate_orchestrator_permission(
                chat_id, tool_name, input_data
            )

        return can_use_tool

    async def _handle_orchestrator_question(
        self, chat_id: int, input_data: dict
    ) -> PermissionResultAllow | PermissionResultDeny:
        """Route AskUserQuestion from orchestrator session to the user via channel.

        Sends the question to the user, waits for their response, and returns
        it as a PermissionResultAllow with updated_input containing the answer.
        """
        send_reply = self._active_reply_fns.get(chat_id)
        if not send_reply:
            return PermissionResultDeny(message="No active channel for questions")

        questions = input_data.get("questions", [])
        question_text = _format_questions(questions)
        msg = format_channel_orchestrator(f"Question:\n{question_text}")
        await self._store.add_message(chat_id, "assistant", msg)
        await send_reply(msg)

        # Wait for user response via handle_message resolving the future
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_answers[chat_id] = future

        try:
            answer = await asyncio.wait_for(future, timeout=300)
            # Build answers dict mapping question text → user's answer
            answers = {}
            for q in questions:
                answers[q.get("question", "")] = answer
            return PermissionResultAllow(updated_input={
                "questions": questions,
                "answers": answers,
            })
        except asyncio.TimeoutError:
            timeout_msg = format_channel_orchestrator("(No response received, continuing without answer)")
            await self._store.add_message(chat_id, "assistant", timeout_msg)
            await send_reply(timeout_msg)
            return PermissionResultDeny(message="No answer from user (timeout)")
        finally:
            self._pending_answers.pop(chat_id, None)

    async def _escalate_orchestrator_permission(
        self, chat_id: int, tool_name: str, tool_input: dict,
    ) -> PermissionResultAllow | PermissionResultDeny:
        """Escalate the orchestrator's own tool use directly to human.

        No Claude intermediary — the orchestrator IS the Claude session.
        Timeout → deny (conservative).
        """
        request_id = uuid.uuid4().hex[:12]
        future = asyncio.get_event_loop().create_future()
        self._pending[request_id] = future

        detail = (
            f"Orchestrator (chat {chat_id})\n"
            f"Tool: {tool_name}\n"
            f"Input: {json.dumps(tool_input, ensure_ascii=False)[:500]}"
        )

        if self._send_telegram:
            await self._send_telegram(
                request_id, "permission", f"Permission: {tool_name}", detail
            )

        try:
            result = await asyncio.wait_for(future, timeout=300)
            if result.get("approved"):
                return PermissionResultAllow()
            return PermissionResultDeny(
                message=result.get("reason", "Denied by user")
            )
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            return PermissionResultDeny(
                message="No response from user (timeout)"
            )

    # ── Broker callbacks (called by HTTP endpoints in main.py) ─────────

    async def handle_permission_request(self, data: dict) -> dict:
        """Handle /permission callback from remote broker.

        Routes through the orchestrator session which has full task context.
        """
        server = data.get("server_name", "unknown")
        session = data.get("session_id", "unknown")
        tool = data.get("tool_name", "unknown")
        tool_input = data.get("tool_input", {})

        # Fast-path: auto-approve safe tools
        if tool in WORKER_SAFE_TOOLS:
            return {"approved": True, "reason": f"Auto-approved: {tool}"}

        # Find chat_id for this worker
        chat_id = self._worker_to_chat.get((server, session))
        if chat_id is None:
            log.warning(f"No chat for worker {server}/{session}, auto-denying")
            return {"approved": False, "reason": "Unknown worker"}

        # Ask orchestrator session (which knows the task context)
        msg = (
            f"[PERMISSION REQUEST]\n"
            f"Worker: {server}/{session}\n"
            f"Tool: {tool}\n"
            f"Input: {json.dumps(tool_input, ensure_ascii=False)[:2000]}"
        )
        decision = await self._send_to_orchestrator(chat_id, msg)

        if decision is None:
            return {"approved": False, "reason": "Orchestrator error"}

        if decision.get("action") != "permission_decision":
            log.warning(
                "Unexpected orchestrator response for permission: %s",
                decision.get("action"),
            )
            return {"approved": False, "reason": "Unexpected orchestrator response"}

        if decision.get("escalate"):
            return await self._escalate_permission(
                server, session, tool, tool_input,
                decision.get("reason", "Orchestrator unsure"),
            )

        return {
            "approved": decision.get("approved", False),
            "reason": decision.get("reason", ""),
        }

    async def handle_clarification_request(self, data: dict) -> dict:
        """Handle /clarification callback from remote broker (AskUserQuestion).

        Routes through the orchestrator session which has full task context.
        """
        server = data.get("server_name", "unknown")
        session = data.get("session_id", "unknown")
        questions = data.get("questions", [])

        # Find chat_id for this worker
        chat_id = self._worker_to_chat.get((server, session))
        if chat_id is None:
            log.warning(f"No chat for worker {server}/{session}")
            return {"answers": None, "reason": "Unknown worker"}

        # Format questions for orchestrator
        q_lines = []
        for i, q in enumerate(questions, 1):
            opts = ", ".join(o.get("label", "?") for o in q.get("options", []))
            q_lines.append(
                f"Q{i} [{q.get('header', '')}]: {q['question']}\n    Options: {opts}"
            )
        questions_formatted = "\n".join(q_lines)

        msg = (
            f"[CLARIFICATION REQUEST]\n"
            f"Worker: {server}/{session}\n\n"
            f"{questions_formatted}"
        )
        decision = await self._send_to_orchestrator(chat_id, msg)

        if decision is None:
            return {"answers": None, "reason": "Orchestrator error"}

        if decision.get("action") != "clarification_answer":
            log.warning(
                "Unexpected orchestrator response for clarification: %s",
                decision.get("action"),
            )
            return {"answers": None, "reason": "Unexpected orchestrator response"}

        if decision.get("escalate"):
            return await self._escalate_clarification_to_human(
                server, session, questions,
                decision.get("reason", "Orchestrator unsure"),
            )

        return {"answers": decision.get("answers", {})}

    # ── Escalation to human ────────────────────────────────────────────

    async def _escalate_permission(
        self,
        server: str,
        session: str,
        tool: str,
        tool_input: dict,
        reason: str,
    ) -> dict:
        """Escalate worker permission to human. Forced fallback on timeout."""
        request_id = uuid.uuid4().hex[:12]
        future = asyncio.get_event_loop().create_future()
        self._pending[request_id] = future

        detail = (
            f"Server: {server}/{session}\n"
            f"Tool: {tool}\n"
            f"Input: {json.dumps(tool_input, ensure_ascii=False)[:500]}\n"
            f"Reason: {reason}"
        )

        if self._send_telegram:
            await self._send_telegram(
                request_id, "permission", f"Permission: {tool}", detail
            )

        try:
            result = await asyncio.wait_for(future, timeout=300)
            return result
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            log.info(
                f"Permission timeout for {server}/{session}/{tool}, "
                f"falling back to forced orchestrator decision"
            )
            return await self._forced_permission_decision(
                server, session, tool, tool_input
            )

    async def _escalate_clarification_to_human(
        self,
        server: str,
        session: str,
        questions: list[dict],
        reason: str,
    ) -> dict:
        """Escalate worker clarification to human. Forced fallback on timeout."""
        request_id = uuid.uuid4().hex[:12]
        future = asyncio.get_event_loop().create_future()
        self._pending[request_id] = future
        self._pending_meta[request_id] = {"questions": questions}

        detail_lines = [f"Server: {server}/{session}", f"Reason: {reason}", ""]
        for q in questions:
            detail_lines.append(f"  {q['question']}")

        if self._send_telegram:
            await self._send_telegram(
                request_id, "clarification",
                "Clarification needed", "\n".join(detail_lines),
            )

        try:
            result = await asyncio.wait_for(future, timeout=300)
            self._pending_meta.pop(request_id, None)
            return result
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            self._pending_meta.pop(request_id, None)
            log.info(
                f"Clarification timeout for {server}/{session}, "
                f"falling back to forced orchestrator decision"
            )
            return await self._forced_clarification_decision(
                server, session, questions
            )

    # ── Forced fallback (human didn't respond) ────────────────────────

    async def _forced_permission_decision(
        self, server: str, session: str, tool: str, tool_input: dict,
    ) -> dict:
        """Send [PERMISSION REQUEST (FORCED)] to orchestrator — no escalate option."""
        chat_id = self._worker_to_chat.get((server, session))
        if chat_id is None:
            return {"approved": False, "reason": "Unknown worker"}

        msg = (
            f"[PERMISSION REQUEST (FORCED)]\n"
            f"Worker: {server}/{session}\n"
            f"Tool: {tool}\n"
            f"Input: {json.dumps(tool_input, ensure_ascii=False)[:2000]}\n\n"
            f"Human did not respond in time. You MUST approve or deny — no escalate."
        )
        decision = await self._send_to_orchestrator(chat_id, msg)

        if decision and decision.get("approved"):
            return {
                "approved": True,
                "reason": f"[fallback] {decision.get('reason', '')}",
            }
        reason = (
            decision.get("reason", "Forced deny after timeout")
            if decision
            else "Orchestrator error"
        )
        return {"approved": False, "reason": f"[fallback] {reason}"}

    async def _forced_clarification_decision(
        self, server: str, session: str, questions: list[dict],
    ) -> dict:
        """Send [CLARIFICATION REQUEST (FORCED)] to orchestrator — must answer."""
        chat_id = self._worker_to_chat.get((server, session))
        if chat_id is None:
            return {"answers": None, "reason": "Unknown worker"}

        q_lines = []
        for i, q in enumerate(questions, 1):
            opts = ", ".join(o.get("label", "?") for o in q.get("options", []))
            q_lines.append(
                f"Q{i} [{q.get('header', '')}]: {q['question']}\n    Options: {opts}"
            )

        msg = (
            f"[CLARIFICATION REQUEST (FORCED)]\n"
            f"Worker: {server}/{session}\n\n"
            f"{chr(10).join(q_lines)}\n\n"
            f"Human did not respond in time. You MUST answer — no escalate."
        )
        decision = await self._send_to_orchestrator(chat_id, msg)

        if decision and decision.get("answers"):
            return {"answers": decision["answers"]}
        return {"answers": None, "reason": "Orchestrator could not answer"}

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _format_channel_workers(workers: list[ChannelWorker]) -> str:
        """Format channel workers for injection into orchestrator messages."""
        if not workers:
            return "[CHANNEL WORKERS]\n  (none)"
        lines = ["[CHANNEL WORKERS]"]
        for w in workers:
            desc = f" — {w.description}" if w.description else ""
            lines.append(f"  {w.server}/{w.session_id} (work_dir: {w.work_dir}){desc}")
        return "\n".join(lines)

    def _format_worker_result(self, item: WorkItem, plan: WorkPlan, is_error: bool) -> str:
        """Format a worker result for the orchestrator session."""
        result_text = item.result or "(no output)"
        if len(result_text) > 2000:
            result_text = result_text[:2000] + "... (truncated)"

        plan_context = self._format_plan_context(plan, item.id)

        return (
            f"[WORKER RESULT]\n"
            f"Task: {item.id} — {item.description}\n"
            f"Server: {item.server}/{item.session}\n"
            f"Status: {'error' if is_error else 'completed'}\n"
            f"Attempt: {item.retries + 1}\n\n"
            f"Plan context:\n{plan_context}\n\n"
            f"Output:\n{result_text}"
        )

    def _build_upstream_context(self, plan: WorkPlan, item: WorkItem) -> str:
        """Collect results from upstream (depends_on) tasks, truncated."""
        if not item.depends_on:
            return ""
        parts = []
        for dep_id in item.depends_on:
            dep = next((i for i in plan.items if i.id == dep_id), None)
            if dep and dep.result:
                truncated = dep.result[:1000]
                if len(dep.result) > 1000:
                    truncated += "... (truncated)"
                parts.append(
                    f"[{dep.id}] {dep.description}:\n{truncated}"
                )
        if not parts:
            return ""
        return "CONTEXT FROM UPSTREAM TASKS:\n\n" + "\n\n".join(parts) + "\n\n---\n\n"

    def _format_plan_context(self, plan: WorkPlan, current_item_id: str) -> str:
        """Format plan context: completed/pending/current tasks."""
        lines = []
        for item in plan.items:
            if item.id == current_item_id:
                status_label = "CURRENT"
            elif item.status == "done":
                status_label = "COMPLETED"
                result_preview = (item.result or "")[:300]
                if result_preview:
                    lines.append(f"  [{item.id}] ({status_label}) {item.description}: {result_preview}")
                    continue
            elif item.status == "failed":
                status_label = "FAILED"
            else:
                status_label = "PENDING"
            lines.append(f"  [{item.id}] ({status_label}) {item.description}")
        return "\n".join(lines)
