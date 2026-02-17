"""Orchestrator: the brain that manages distributed Claude Code sessions.

Uses a single persistent Claude session per chat (via `claude -p --resume`)
to route tasks, evaluate worker results, and accumulate context — like a
PhD student managing research across multiple workers.

The orchestrator session handles both user messages (routing decisions) and
worker results (verdicts), eliminating the need for separate verification,
reflection, or suggestion pipelines.

Permission and clarification are handled in real-time by the Agent SDK's
canUseTool callback in the remote broker, which calls back to this
orchestrator's HTTP endpoints (/permission, /clarification).
"""

import asyncio
import json
import logging
import uuid

from .session import SessionManager, SessionResult
from .store import Store, TaskStatus
from .models import WorkItem, WorkPlan
from .permission import PermissionEvaluator
from .formatter import (
    format_result,
    format_task_status,
    format_channel_orchestrator,
    format_channel_dispatch,
    format_channel_worker_result,
    format_channel_plan_created,
    format_channel_plan_summary,
)

log = logging.getLogger(__name__)

ORCHESTRATOR_SYSTEM_PROMPT = """\
You are an orchestrator managing distributed Claude Code sessions across remote servers.
You act like a PhD student managing a research project — you receive direction from the user \
(professor), assign work to Claude Code workers, evaluate their reports, and decide next steps.

SETUP: First, read the config file at `{config_path}` to understand available servers and sessions. \
Also check for these optional files in the same directory and read them if they exist:
- `config.md` — extra instructions and context from the user about the setup
- `setup.md` — per-server environment details and capabilities

Your response MUST always be ONLY a JSON object (no markdown, no extra text).

=== WHEN RECEIVING A USER MESSAGE ===

Decide what to do:

1. Route a single task:
{{"action": "route", "server": "<server_name>", "session": "<session_id>", "prompt": "<detailed prompt>"}}

2. Plan multiple tasks (for complex requests spanning multiple servers/sessions):
{{"action": "plan", "tasks": [
    {{"id": "t1", "description": "human-readable goal", "server": "<server>", "session": "<session>", "prompt": "<detailed prompt>", "depends_on": []}},
    {{"id": "t2", "description": "human-readable goal", "server": "<server>", "session": "<session>", "prompt": "<detailed prompt>", "depends_on": ["t1"]}}
]}}

3. Reply directly (for questions, status, chitchat):
{{"action": "reply", "text": "<your response>"}}

4. Show running tasks:
{{"action": "show_tasks"}}

Rules for routing:
- Most requests should use "route". Workers are capable — trust them with implementation.
- Only use "plan" when the request spans multiple LARGE, independent workstreams on \
different servers/sessions. Each task = hours of work, NOT subtasks like "write tests".
- NEVER decompose a single-session request into subtasks.
- Write clear prompts capturing user intent plus context from config/setup docs.
- Use "depends_on" only for genuine data dependencies between workstreams.
- If ambiguous which session, ask via "reply".

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
        await self._store.add_message(chat_id, "user", user_text)

        decision = await self._send_to_orchestrator(chat_id, user_text)
        if decision is None:
            await send_reply("Sorry, I couldn't understand that. Try again?")
            return

        action = decision.get("action")

        if action == "reply":
            text = decision.get("text", "...")
            await self._store.add_message(chat_id, "assistant", text)
            await send_reply(format_channel_orchestrator(text))

        elif action == "show_tasks":
            tasks = await self._store.get_running_tasks(chat_id)
            await send_reply(format_task_status(tasks))

        elif action == "route":
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

    # ── Orchestrator session ───────────────────────────────────────────

    async def _send_to_orchestrator(self, chat_id: int, text: str) -> dict | None:
        """Send a message to the orchestrator Claude session and parse the JSON response.

        Used for both user messages (routing decisions) and worker result
        feedback (verdicts). Resumes the existing session for this chat,
        or starts a new one with the system prompt.
        """
        cmd = [
            "claude", "-p",
            "--output-format", "json",
            "--model", self._model,
        ]

        session_id = self._orchestrator_sessions.get(chat_id)
        if session_id:
            cmd.extend(["--resume", session_id])
        else:
            cmd.extend(["--system-prompt", self._system_prompt])

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(input=text.encode()), timeout=120
            )
            if proc.returncode != 0:
                log.error("Orchestrator claude failed: %s", stderr.decode())
                return None

            outer = json.loads(stdout.decode())

            # Capture session ID so future calls resume this conversation
            if outer.get("session_id"):
                self._orchestrator_sessions[chat_id] = outer["session_id"]

            result_text = outer.get("result", "")
            clean = result_text.strip()
            if clean.startswith("```"):
                clean = "\n".join(clean.split("\n")[1:])
            if clean.endswith("```"):
                clean = clean.rsplit("```", 1)[0]
            return json.loads(clean.strip())

        except (json.JSONDecodeError, asyncio.TimeoutError) as e:
            log.error("Orchestrator parse error: %s", e)
            return None
        except Exception as e:
            log.exception("Orchestrator error: %s", e)
            return None

    # ── Helpers ─────────────────────────────────────────────────────────

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
