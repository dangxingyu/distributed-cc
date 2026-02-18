"""Orchestrator: the brain that manages distributed Claude Code sessions.

Uses a single persistent Claude session per chat (via Agent SDK `query()`)
to assign tasks, evaluate worker results, and accumulate context — like a
PhD student managing research across multiple workers.

Each chat is a project channel. Workers are created on-demand when the
orchestrator needs them (via create_worker → broker /register). The
orchestrator session handles both user messages (assign/create_worker)
and worker results (verdicts).

The orchestrator's own Claude session uses `can_use_tool` callbacks for:
- AskUserQuestion → routed through the channel to the user
- Auto-approved tools (Read, Glob, Grep, etc.) → allowed immediately
- Everything else → PermissionEvaluator (Claude judgment → escalate to human)

Remote workers use the same pattern via the broker's canUseTool → HTTP
callbacks to this orchestrator's /permission and /clarification endpoints.
"""

import asyncio
import json
import logging
import os
import uuid

from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage
from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny

from .session import SessionManager, SessionResult
from .store import Store, TaskStatus, ChannelWorker
from .models import WorkItem, WorkPlan
from .permission import PermissionEvaluator
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
"""


class Orchestrator:
    def __init__(
        self,
        session_mgr: SessionManager,
        store: Store,
        permission_evaluator: PermissionEvaluator,
        model: str = "claude-opus-4-6",
        config_path: str = "config.yaml",
        orch_config: dict | None = None,
    ):
        self._session_mgr = session_mgr
        self._store = store
        self._permission = permission_evaluator
        self._model = model
        self._config_path = config_path
        self._send_telegram: callable = None
        self._system_prompt = ORCHESTRATOR_SYSTEM_PROMPT.format(config_path=config_path)
        # Per-chat orchestrator session IDs for --resume
        self._orchestrator_sessions: dict[int, str] = {}

        # Tool permission config (from config.yaml orchestrator.permissions)
        perm_cfg = (orch_config or {}).get("permissions", {})
        self._auto_approve_tools: set[str] = set(
            perm_cfg.get("auto_approve", ["Read", "Glob", "Grep", "WebSearch", "WebFetch"])
        )
        self._denied_tools: set[str] = set(perm_cfg.get("deny", []))

        # AskUserQuestion routing: store send_reply per chat during _send_to_orchestrator
        self._active_reply_fns: dict[int, callable] = {}
        # Pending answers: chat_id -> asyncio.Future for user responses to orchestrator questions
        self._pending_answers: dict[int, asyncio.Future] = {}

    def set_send_telegram(self, fn):
        """Inject the Telegram send function (set by Bot after wiring)."""
        self._send_telegram = fn

    # ── User message handling ──────────────────────────────────────────

    async def handle_message(
        self,
        chat_id: int,
        user_text: str,
        send_reply: callable,
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
            decision = await self._send_to_orchestrator(chat_id, augmented_text)
        finally:
            self._active_reply_fns.pop(chat_id, None)

        if decision is None:
            await send_reply("Sorry, I couldn't understand that. Try again?")
            return

        await self._handle_decision(chat_id, user_text, decision, send_reply)

    async def _handle_decision(
        self,
        chat_id: int,
        user_text: str,
        decision: dict,
        send_reply: callable,
    ):
        """Dispatch on orchestrator decision. May recurse for create_worker follow-ups."""
        action = decision.get("action")

        if action == "reply":
            text = decision.get("text", "...")
            await self._store.add_message(chat_id, "assistant", text)
            await send_reply(format_channel_orchestrator(text))

        elif action == "show_tasks":
            tasks = await self._store.get_running_tasks(chat_id)
            await send_reply(format_task_status(tasks))

        elif action == "create_worker":
            await self._handle_create_worker(chat_id, user_text, decision, send_reply)

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
            await send_reply(format_channel_dispatch(server, session, prompt))
            asyncio.create_task(self._execute_plan(plan, send_reply))

        elif action == "plan":
            tasks_data = decision.get("tasks", [])
            if not tasks_data:
                await send_reply(format_channel_orchestrator("Plan has no tasks."))
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
            await send_reply(format_channel_plan_created(plan))
            asyncio.create_task(self._execute_plan(plan, send_reply))

        else:
            await send_reply(format_channel_orchestrator(f"Unknown action: {action}"))

    async def _handle_create_worker(
        self,
        chat_id: int,
        user_text: str,
        decision: dict,
        send_reply: callable,
    ):
        """Handle create_worker action: register on broker, store in DB, confirm to orchestrator."""
        server_name = decision.get("server", "")
        work_dir = decision.get("work_dir", "")
        description = decision.get("description", "")

        if not server_name or not work_dir:
            await send_reply(format_channel_orchestrator("Cannot create worker: missing server or work_dir."))
            return

        # Generate session_id from work_dir basename
        session_id = work_dir.rstrip("/").split("/")[-1]

        # Register on broker
        result = await self._session_mgr.register_session(server_name, session_id, work_dir, description)
        if not result.get("ok"):
            error = result.get("error", "Unknown error")
            await send_reply(format_channel_orchestrator(f"Failed to create worker: {error}"))
            return

        # Store in DB
        await self._store.add_channel_worker(chat_id, server_name, session_id, work_dir, description)
        await send_reply(format_channel_worker_created(server_name, session_id, work_dir))

        # Confirm back to orchestrator session so it can continue
        workers = await self._store.get_channel_workers(chat_id)
        confirmation = (
            f"[WORKER CREATED] {server_name}/{session_id} at {work_dir} — ready\n\n"
            f"{self._format_channel_workers(workers)}\n"
            f"Continue with the user's original request."
        )
        follow_up = await self._send_to_orchestrator(chat_id, confirmation)
        if follow_up:
            await self._handle_decision(chat_id, user_text, follow_up, send_reply)

    # ── Plan execution ─────────────────────────────────────────────────

    async def _execute_plan(self, plan: WorkPlan, send_reply: callable):
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
                    await self._execute_item(plan, item, send_reply)

            # All done — send summary
            all_done = all(i.status == "done" for i in plan.items)
            plan.status = "completed" if all_done else "failed"

            summary = format_channel_plan_summary(plan)
            await self._store.add_message(plan.chat_id, "assistant", summary)
            await send_reply(summary)

        except Exception as e:
            log.exception(f"Plan {plan.id} failed: {e}")
            plan.status = "failed"
            await send_reply(f"Plan execution failed: {e}")

    async def _execute_item(
        self, plan: WorkPlan, item: WorkItem, send_reply: callable,
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
                await send_reply(format_channel_worker_result(item, "failed"))
                return

            await self._store.finish_task(task_id, TaskStatus.DONE, result.result_text)
            item.result = result.result_text

            # Send result to orchestrator session for evaluation
            worker_report = self._format_worker_result(item, plan, is_error=False)
            verdict = await self._send_to_orchestrator(plan.chat_id, worker_report)

            if verdict is None or verdict.get("action") != "verdict":
                # Orchestrator didn't return a valid verdict — assume done
                log.warning("No valid verdict from orchestrator, assuming done")
                item.status = "done"
                await send_reply(format_channel_worker_result(item, "done"))
                return

            status = verdict.get("status", "done")

            if status == "done":
                item.status = "done"
                cost_info = ""
                if result.cost_usd:
                    cost_info += f"\n(cost: ${result.cost_usd:.4f})"
                if result.duration_secs:
                    cost_info += f" ({result.duration_secs:.0f}s)"
                await send_reply(format_channel_worker_result(item, "done") + cost_info)
                # Handle suggestions
                suggestions = verdict.get("suggestions", "")
                if suggestions:
                    await send_reply(format_channel_orchestrator(
                        f"Suggested next steps: {suggestions}"
                    ))

            elif status == "retry" and item.retries < item.max_retries:
                item.retries += 1
                item.feedback = verdict.get("feedback", "Please try again")
                item.status = "pending"
                await send_reply(format_channel_worker_result(item, "retry"))

            elif status == "retry_different" and item.approach_changes < item.max_approach_changes:
                item.approach_changes += 1
                new_prompt = verdict.get("new_prompt")
                if new_prompt:
                    item.prompt = new_prompt
                    item.retries = 0
                    item.feedback = None
                    item.status = "pending"
                    await send_reply(format_channel_worker_result(item, "retry_different"))
                else:
                    item.status = "failed"
                    item.feedback = verdict.get("summary", "No new approach provided")
                    await send_reply(format_channel_worker_result(item, "failed"))

            elif status == "escalate":
                question = verdict.get("question", "Need your input on this task.")
                await send_reply(format_channel_orchestrator(
                    f"Task {item.id} needs your input: {question}"
                ))
                item.status = "failed"
                item.feedback = f"Escalated: {question}"

            else:
                # failed, or retry/approach_change exhausted
                item.status = "failed"
                item.feedback = verdict.get("summary", verdict.get("feedback", "Task failed"))
                await send_reply(format_channel_worker_result(item, "failed"))

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
            await send_reply(format_channel_worker_result(item, "failed"))

    # ── Orchestrator session (Agent SDK) ──────────────────────────────

    async def _send_to_orchestrator(self, chat_id: int, text: str) -> dict | None:
        """Send a message to the orchestrator Claude session and parse the JSON response.

        Uses Agent SDK query() with can_use_tool for permission handling
        and AskUserQuestion routing. Resumes the existing session for this
        chat, or starts a new one with the system prompt.
        """
        # Clear nested-session guard so SDK can spawn claude subprocess
        os.environ.pop("CLAUDECODE", None)

        options = ClaudeAgentOptions(
            can_use_tool=self._make_can_use_tool(chat_id),
            model=self._model,
            cwd=os.path.dirname(os.path.abspath(self._config_path)),
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
                if isinstance(message, ResultMessage):
                    result_text = message.result or ""
                    self._orchestrator_sessions[chat_id] = message.session_id

            # Parse JSON — Agent SDK returns clean text, no markdown fences needed
            clean = result_text.strip()
            if clean.startswith("```"):
                clean = "\n".join(clean.split("\n")[1:])
            if clean.endswith("```"):
                clean = clean.rsplit("```", 1)[0]
            return json.loads(clean.strip())

        except json.JSONDecodeError as e:
            log.error("Orchestrator JSON parse error: %s (raw: %s)", e, result_text[:200])
            return None
        except Exception as e:
            log.exception("Orchestrator error: %s", e)
            return None

    def _make_can_use_tool(self, chat_id: int):
        """Create a can_use_tool callback for the orchestrator session.

        Handles:
        - AskUserQuestion → route through channel to user
        - Auto-approved tools → allow immediately
        - Denied tools → deny immediately
        - Everything else → PermissionEvaluator (Claude judgment → escalate)
        """

        async def can_use_tool(tool_name: str, input_data: dict, context=None):
            # AskUserQuestion → route through channel
            if tool_name == "AskUserQuestion":
                return await self._handle_orchestrator_question(chat_id, input_data)

            # Config-based auto-approve
            if tool_name in self._auto_approve_tools:
                return PermissionResultAllow()

            # Config-based deny
            if tool_name in self._denied_tools:
                return PermissionResultDeny(
                    message=f"Tool {tool_name} not allowed for orchestrator"
                )

            # Default: use PermissionEvaluator (same pipeline as workers)
            result = await self._permission.evaluate_permission(
                server_name="orchestrator",
                session_id=f"orch-{chat_id}",
                tool_name=tool_name,
                tool_input=input_data,
                send_escalation=self._make_escalation_sender(),
            )
            if result.get("approved"):
                return PermissionResultAllow()
            return PermissionResultDeny(
                message=result.get("reason", "Denied by permission evaluator")
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
        await send_reply(format_channel_orchestrator(f"Question:\n{question_text}"))

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
            await send_reply(format_channel_orchestrator(
                "(No response received, continuing without answer)"
            ))
            return PermissionResultDeny(message="No answer from user (timeout)")
        finally:
            self._pending_answers.pop(chat_id, None)

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

    # ── Broker callbacks (called by HTTP endpoints in main.py) ─────────

    async def handle_permission_request(self, data: dict) -> dict:
        """Handle /permission callback from remote broker."""
        return await self._permission.evaluate_permission(
            server_name=data.get("server_name", "unknown"),
            session_id=data.get("session_id", "unknown"),
            tool_name=data.get("tool_name", "unknown"),
            tool_input=data.get("tool_input", {}),
            send_escalation=self._make_escalation_sender(),
        )

    async def handle_clarification_request(self, data: dict) -> dict:
        """Handle /clarification callback from remote broker (AskUserQuestion)."""
        return await self._permission.evaluate_clarification(
            server_name=data.get("server_name", "unknown"),
            session_id=data.get("session_id", "unknown"),
            questions=data.get("questions", []),
            send_escalation=self._make_escalation_sender(),
        )

    def handle_heartbeat(self, data: dict):
        """Handle /heartbeat from a remote broker — update session registry."""
        server_name = data.get("server_name", "unknown")
        broker_sessions = data.get("sessions", [])
        self._session_mgr.update_sessions(server_name, broker_sessions)

    def _make_escalation_sender(self):
        async def send_escalation(request_id, interaction_type, title, detail):
            if self._send_telegram:
                await self._send_telegram(request_id, interaction_type, title, detail)
        return send_escalation
