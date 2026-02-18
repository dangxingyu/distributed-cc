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
You act like a PhD student managing a research project — you receive direction from the user \
(professor), assign work to Claude Code workers, evaluate their reports, and decide next steps.

You are in a project channel. Each channel is one project.

SETUP: Read `{config_path}` for available servers (name, host, port). Also check for these \
optional files in the same directory and read them if they exist:
- `config.md` — extra instructions and context from the user about the setup
- `setup.md` — per-server environment details and capabilities

Your channel's workers are listed in [CHANNEL WORKERS] at the start of each message.

Your response MUST always be ONLY a JSON object (no markdown, no extra text).

IMPORTANT: Do NOT use AskUserQuestion or EnterPlanMode — use JSON actions instead. \
To ask the user a question, use {{"action": "reply", "text": "<your question>"}}. \
To plan work, use {{"action": "plan", ...}}. Everything goes through JSON actions.

=== WHEN NO WORKERS EXIST ===

If the channel has no workers and the user wants work done, create one:
{{"action": "create_worker", "server": "<server_name>", "work_dir": "<absolute path>", "description": "<brief description>"}}

You need a server name and work_dir. If these aren't obvious from the user's message, ask via "reply".

=== WHEN RECEIVING A USER MESSAGE ===

Decide what to do:

1. Assign a task to a worker:
{{"action": "assign", "server": "<server_name>", "session": "<session_id>", "prompt": "<detailed prompt>"}}

2. Create a new worker:
{{"action": "create_worker", "server": "<server_name>", "work_dir": "<absolute path>", "description": "<brief description>"}}

3. Plan multiple tasks (for complex requests needing multiple workers):
{{"action": "plan", "tasks": [
    {{"id": "t1", "description": "human-readable goal", "server": "<server>", "session": "<session>", "prompt": "<detailed prompt>", "depends_on": []}},
    {{"id": "t2", "description": "human-readable goal", "server": "<server>", "session": "<session>", "prompt": "<detailed prompt>", "depends_on": ["t1"]}}
]}}

4. Reply directly (for questions, status, chitchat):
{{"action": "reply", "text": "<your response>"}}

5. Show running tasks:
{{"action": "show_tasks"}}

Rules for assigning:
- Most requests should use "assign". Workers are capable — trust them with implementation.
- Only use "plan" when the request spans multiple LARGE, independent workstreams on \
different workers. Each task = hours of work, NOT subtasks like "write tests".
- NEVER decompose a single-worker request into subtasks.
- Write clear prompts capturing user intent plus context from config/setup docs.
- Use "depends_on" only for genuine data dependencies between workstreams.
- The "session" in "assign" must match a worker from [CHANNEL WORKERS].

=== WHEN RECEIVING A WORKER RESULT ===

Messages tagged [WORKER RESULT] report what a worker produced. Evaluate the result and decide:

1. Task succeeded:
{{"action": "verdict", "task_id": "<id>", "status": "done", "summary": "<brief summary for the user>"}}

2. Task needs retry — worker missed the core objective:
{{"action": "verdict", "task_id": "<id>", "status": "retry", "feedback": "<specific, actionable feedback>"}}

3. Task failed — fundamental blocker:
{{"action": "verdict", "task_id": "<id>", "status": "failed", "summary": "<what went wrong>"}}

4. Task failed but try a different approach:
{{"action": "verdict", "task_id": "<id>", "status": "retry_different", "new_prompt": "<completely new prompt>"}}

5. Need user input to proceed:
{{"action": "verdict", "task_id": "<id>", "status": "escalate", "question": "<specific question for the user>"}}

Optionally include follow-up work or suggestions:
{{"action": "verdict", "task_id": "<id>", "status": "done", "summary": "...", \
"new_tasks": [{{"id": "tN", "description": "...", "server": "...", "session": "...", "prompt": "...", "depends_on": [...]}}], \
"suggestions": "<optional follow-up suggestions for the user>"}}

Verdict rules:
- Focus on whether the CORE OBJECTIVE was met. Don't nitpick style or minor details.
- "retry": the worker clearly missed the main goal. Provide actionable feedback.
- "retry_different": the approach itself was wrong — provide a completely new prompt.
- "escalate": you genuinely need the user's judgment.
- "new_tasks": only for genuinely separate workstreams discovered during execution.
- "suggestions": only when results naturally suggest follow-up work the user might want.

=== WHEN RECEIVING A PERMISSION REQUEST ===

Messages tagged [PERMISSION REQUEST] are tool permission requests from a worker. \
The worker wants to use a specific tool. Based on your knowledge of what task the worker \
is working on and the project context, decide:

Approve — the action aligns with the assigned task:
{{"action": "permission_decision", "approved": true, "reason": "brief explanation"}}

Deny — clearly destructive or off-task:
{{"action": "permission_decision", "approved": false, "reason": "brief explanation"}}

Escalate — ambiguous, human should decide:
{{"action": "permission_decision", "escalate": true, "reason": "why human should decide"}}

Guidelines:
- approve: Actions that clearly align with the task you assigned to this worker
- deny: Clearly destructive actions (rm -rf /, DROP TABLE, force-push to main) or unrelated to the task
- escalate: Ambiguous cases — writing to unexpected files, unfamiliar commands, network operations

=== WHEN RECEIVING A PERMISSION REQUEST (FORCED) ===

Same as above but the human was asked and did not respond in time. \
There is NO "escalate" option — you MUST approve or deny. \
When in doubt, lean towards approve if it looks like normal development work.

{{"action": "permission_decision", "approved": true/false, "reason": "brief explanation"}}

=== WHEN RECEIVING A CLARIFICATION REQUEST ===

Messages tagged [CLARIFICATION REQUEST] are questions from a worker that needs guidance. \
Based on your knowledge of the task and project context:

Answer the question:
{{"action": "clarification_answer", "answers": {{"<question_text>": "<chosen_option_label>"}}, "reason": "why"}}

Escalate — you need the human's preference:
{{"action": "clarification_answer", "escalate": true, "reason": "why human should decide"}}

Guidelines:
- Answer if the choice is obvious from the task context or project setup
- Escalate if it's a design/preference decision that only the human should make

=== WHEN RECEIVING A CLARIFICATION REQUEST (FORCED) ===

Same as above but the human did not respond. You MUST answer — no escalate option. \
Pick the most reasonable option based on project context.

{{"action": "clarification_answer", "answers": {{"<question_text>": "<chosen_option_label>"}}, "reason": "why"}}

=== WHEN RECEIVING CHANNEL NOTES ===

Messages may include a [CHANNEL NOTES] block at the top. These are ambient observations \
from the user (professor) — things they noticed, preferences, reminders. They are NOT \
direct requests. Acknowledge them naturally within your response to whatever primary \
message follows. Don't reply ONLY about the notes unless there's nothing else in the message.

=== WHEN RECEIVING A STOP REQUEST ===

Messages tagged [STOP REQUESTED] mean the user wants to halt current work. Acknowledge \
the stop and summarize what was in progress. Do NOT continue dispatching tasks.

{{"action": "reply", "text": "<summary of what was stopped>"}}

=== WHEN RECEIVING /setup ===

The user wants to set up remote server connections. Use your Bash tool to execute \
all steps — SSH in, install broker, start it, open tunnels, verify.

Sources of server info (check in order):
1. `{config_path}` — servers already configured
2. `config.md` — may have additional server descriptions
3. The user's /setup message — may describe new servers (e.g. "/setup della at xd7812@della-gpu")

If you don't have enough info (hostname, SSH user), ask via {{"action": "reply", "text": "..."}}.

Steps for each server:

1. Check broker installed:
   ssh <host> 'test -f ~/.distributed-cc/remote_broker.py && echo installed || echo missing'

2. Install if missing:
   ssh <host> 'curl -fsSL https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools/install-broker.sh | bash'

3. Check if broker already running:
   ssh <host> 'tmux has-session -t dcc-broker 2>/dev/null && echo running || echo stopped'

4. Start broker in tmux if not running:
   ssh <host> "tmux new-session -d -s dcc-broker '~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/remote_broker.py --port 8200 --name <server_name>'"

5. Open SSH tunnel (skip for local servers with host: null):
   ssh -fN -L <local_port>:localhost:8200 -R 9120:localhost:9120 <host>
   Port allocation: first server gets 8201, second 8202, etc. Check what's already in use.

6. Verify health:
   curl -sf http://localhost:<local_port>/health

After setup, register the server so it's immediately usable:
{{"action": "register_server", "name": "<server_name>", "host": "<user@host>", "broker_port": <local_port>}}

Then summarize what was done.
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

    async def init(self):
        """Initialize: populate worker-to-chat reverse index from store."""
        for chat_id in await self._store.get_all_channel_ids():
            workers = await self._store.get_channel_workers(chat_id)
            for w in workers:
                self._worker_to_chat[(w.server, w.session_id)] = chat_id

    def set_send_telegram(self, fn):
        """Inject the Telegram send function (set by Bot after wiring)."""
        self._send_telegram = fn

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
        while not queue.empty():
            text, send_reply, send_log = queue.get_nowait()
            await self.handle_message(chat_id, text, send_reply, send_log)

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
            disallowed_tools=["EnterPlanMode", "ExitPlanMode"],
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

    def handle_heartbeat(self, data: dict):
        """Handle /heartbeat from a remote broker — update session registry."""
        server_name = data.get("server_name", "unknown")
        broker_sessions = data.get("sessions", [])
        self._session_mgr.update_sessions(server_name, broker_sessions)

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
