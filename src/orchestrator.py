"""Orchestrator: the brain that routes user messages to Claude Code sessions.

Uses Claude (via `claude -p`) to understand user intent and decide
which server/session should handle the request.

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
    format_routing_decision,
    format_task_status,
    format_plan_created,
    format_plan_summary,
    format_task_progress,
)

log = logging.getLogger(__name__)

ROUTER_SYSTEM_PROMPT = """\
You are an orchestrator managing distributed Claude Code sessions across remote servers.

IMPORTANT: First, read the config file at `{config_path}` to understand the available servers \
and sessions. Also check for these optional files in the same directory and read them if they exist:
- `config.md` — extra instructions and context from the user about the setup
- `setup.md` — per-server environment details and capabilities

Based on the config and the user's message, decide what to do.

Your final response MUST be ONLY a JSON object (no markdown, no extra text):

1. Route a single task to a remote session:
{{"action": "route", "server": "<server_name>", "session": "<session_id>", "prompt": "<detailed prompt>"}}

2. Plan multiple tasks (for complex requests that need decomposition):
{{"action": "plan", "tasks": [
    {{"id": "t1", "description": "human-readable goal", "server": "<server>", "session": "<session>", "prompt": "<detailed prompt>", "depends_on": []}},
    {{"id": "t2", "description": "human-readable goal", "server": "<server>", "session": "<session>", "prompt": "<detailed prompt>", "depends_on": ["t1"]}}
]}}

3. Answer directly (for simple questions, status checks, or chitchat):
{{"action": "reply", "text": "<your response>"}}

4. Show running tasks:
{{"action": "show_tasks"}}

Rules:
- Most requests should use "route". The worker is a capable Claude Code agent — trust it to \
handle implementation details, debugging, testing, etc. within a single task.
- Only use "plan" when the request genuinely spans multiple LARGE, independent workstreams \
that belong on different servers/sessions (e.g. "implement the backend auth on server A and \
the frontend login page on server B"). Think of each task as something a good engineer could \
spend hours on — NOT fine-grained subtasks like "write tests" or "update docs".
- NEVER decompose a single-session request into subtasks. If the whole job runs on one \
server/session, use "route" and let the worker figure out the steps.
- When routing or planning, write clear, detailed prompts that capture the user's intent. \
Include relevant context from conversation history and what you learned from config/setup docs.
- Use "depends_on" only when one workstream genuinely cannot start without the other's output.
- If the user asks about a specific project/server, route to the matching session.
- If ambiguous which session to use, ask the user to clarify via "reply".
- For greetings, status questions, or meta-questions, use "reply".
- If the user asks to list sessions, read the config and reply with a formatted list directly.
"""

VERIFY_PROMPT = """\
You are reviewing the output of a remote Claude Code session.

Task: {description}
Server: {server}/{session}

Result:
{result_text}

Give a high-level verdict on whether the task goal was achieved. Do NOT nitpick style, \
minor details, or things the worker could easily fix themselves. Focus on whether the \
core objective was met.

Your response MUST be ONLY a JSON object:
{{"verdict": "<done|retry|failed>", "reason": "...", "new_tasks": []}}

- "done": The main goal was achieved. Provide a brief summary in "reason".
- "retry": The worker clearly missed the core objective or left major functionality \
broken/unimplemented. Provide specific, actionable feedback in "reason".
- "failed": Fundamental blocker prevents completion (e.g. missing access, wrong server). \
Explain why in "reason".
- "new_tasks": Only include if the result reveals a genuinely separate workstream that \
was not part of the original request. Do NOT use this to decompose the current task.
  Each: {{"id": "tN", "description": "...", "server": "...", "session": "...", "prompt": "...", "depends_on": [...]}}
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
        self._system_prompt = ROUTER_SYSTEM_PROMPT.format(config_path=config_path)
        # Per-chat router session IDs for --resume (Claude maintains its own context)
        self._router_sessions: dict[int, str] = {}

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

        decision = await self._get_routing_decision(chat_id, user_text)
        if decision is None:
            await send_reply("Sorry, I couldn't understand that. Try again?")
            return

        action = decision.get("action")

        if action == "reply":
            text = decision.get("text", "...")
            await self._store.add_message(chat_id, "assistant", text)
            await send_reply(text)

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
            await send_reply(format_routing_decision(server, session, prompt))
            asyncio.create_task(self._execute_plan(plan, send_reply))

        elif action == "plan":
            tasks_data = decision.get("tasks", [])
            if not tasks_data:
                await send_reply("Plan has no tasks.")
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
            await send_reply(format_plan_created(plan))
            asyncio.create_task(self._execute_plan(plan, send_reply))

        else:
            await send_reply(f"Unknown action: {action}")

    # ── Plan execution ─────────────────────────────────────────────────

    async def _execute_plan(self, plan: WorkPlan, send_reply: callable):
        """Execute a work plan: run items respecting dependencies, verify each."""
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
                    # No more items to run — check if anything is still running
                    running = [i for i in plan.items if i.status == "running"]
                    if not running:
                        break
                    # Wait for running items to finish (shouldn't happen in this loop)
                    await asyncio.sleep(1)
                    continue

                # Execute ready items (sequentially to avoid overwhelming brokers)
                for item in ready:
                    await self._execute_item(plan, item, send_reply)

            # All done — send summary
            all_done = all(i.status == "done" for i in plan.items)
            plan.status = "completed" if all_done else "failed"

            summary = format_plan_summary(plan)
            await self._store.add_message(plan.chat_id, "assistant", summary)
            await send_reply(summary)

        except Exception as e:
            log.exception(f"Plan {plan.id} failed: {e}")
            plan.status = "failed"
            await send_reply(f"Plan execution failed: {e}")

    async def _execute_item(
        self, plan: WorkPlan, item: WorkItem, send_reply: callable,
    ):
        """Execute a single work item with verification and retry loop."""
        item.status = "running"

        task_id = await self._store.create_task(
            plan.chat_id, item.description, item.server, item.session, item.prompt
        )

        prompt = item.prompt
        if item.feedback:
            prompt = (
                f"PREVIOUS ATTEMPT FEEDBACK: {item.feedback}\n\n"
                f"Please address the feedback above and complete the task:\n\n"
                f"{item.prompt}"
            )

        try:
            result: SessionResult = await self._session_mgr.run_task(
                item.server, item.session, prompt, task_id=task_id
            )

            if result.is_error:
                await self._store.finish_task(task_id, TaskStatus.FAILED, result.result_text)
                item.status = "failed"
                item.result = result.result_text
                await send_reply(format_task_progress(item, "failed"))
                return

            await self._store.finish_task(task_id, TaskStatus.DONE, result.result_text)
            item.result = result.result_text

            # Verify the result
            verdict = await self._verify_result(item)

            if verdict["verdict"] == "done":
                item.status = "done"
                cost_info = ""
                if result.cost_usd:
                    cost_info += f"\n(cost: ${result.cost_usd:.4f})"
                if result.duration_secs:
                    cost_info += f" ({result.duration_secs:.0f}s)"
                await send_reply(format_task_progress(item, "done") + cost_info)

            elif verdict["verdict"] == "retry" and item.retries < item.max_retries:
                item.retries += 1
                item.feedback = verdict.get("reason", "Please try again")
                item.status = "pending"  # Will be picked up again in the loop
                await send_reply(format_task_progress(item, "retry"))

            else:
                item.status = "failed"
                item.feedback = verdict.get("reason", "Verification failed")
                await send_reply(format_task_progress(item, "failed"))

            # Add any new tasks discovered during verification
            for new_task in verdict.get("new_tasks", []):
                if any(i.id == new_task["id"] for i in plan.items):
                    continue  # Skip duplicates
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
            await send_reply(format_task_progress(item, "failed"))

    # ── Verification ───────────────────────────────────────────────────

    async def _verify_result(self, item: WorkItem) -> dict:
        """Call Claude to verify a task result. Returns verdict dict."""
        verify_prompt = VERIFY_PROMPT.format(
            description=item.description,
            server=item.server,
            session=item.session,
            result_text=item.result or "(no output)",
        )

        cmd = [
            "claude", "-p",
            "--output-format", "json",
            "--model", self._model,
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(input=verify_prompt.encode()), timeout=30
            )
            if proc.returncode != 0:
                log.error(f"Verify claude failed: {stderr.decode()}")
                return {"verdict": "done", "reason": "Verification unavailable", "new_tasks": []}

            outer = json.loads(stdout.decode())
            result_text = outer.get("result", "")
            clean = result_text.strip()
            if clean.startswith("```"):
                clean = "\n".join(clean.split("\n")[1:])
            if clean.endswith("```"):
                clean = clean.rsplit("```", 1)[0]
            parsed = json.loads(clean.strip())
            # Ensure required fields
            parsed.setdefault("verdict", "done")
            parsed.setdefault("reason", "")
            parsed.setdefault("new_tasks", [])
            return parsed

        except (json.JSONDecodeError, asyncio.TimeoutError) as e:
            log.error(f"Verify parse error: {e}")
            # If verification fails to parse, assume done (don't block on verification bugs)
            return {"verdict": "done", "reason": "Verification parse error", "new_tasks": []}
        except Exception as e:
            log.error(f"Verify error: {e}")
            return {"verdict": "done", "reason": f"Verification error: {e}", "new_tasks": []}

    # ── Broker callbacks (called by HTTP endpoints in main.py) ─────────

    async def handle_permission_request(self, data: dict) -> dict:
        """Handle /permission callback from remote broker.

        The agent proactively asks for permission before certain tool calls.
        """
        return await self._permission.evaluate_permission(
            server_name=data.get("server_name", "unknown"),
            session_id=data.get("session_id", "unknown"),
            tool_name=data.get("tool_name", "unknown"),
            tool_input=data.get("tool_input", {}),
            send_escalation=self._make_escalation_sender(),
        )

    async def handle_clarification_request(self, data: dict) -> dict:
        """Handle /clarification callback from remote broker (AskUserQuestion).

        The agent proactively asks a clarifying question.
        """
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

    # ── Routing ────────────────────────────────────────────────────────

    async def _get_routing_decision(self, chat_id: int, user_text: str) -> dict | None:
        """Let the router Claude decide. It maintains its own session via --resume,
        so it naturally remembers conversation history, config it read, etc."""
        cmd = [
            "claude", "-p",
            "--output-format", "json",
            "--model", self._model,
        ]

        # Resume existing router session or start fresh with system prompt
        session_id = self._router_sessions.get(chat_id)
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
                proc.communicate(input=user_text.encode()), timeout=60
            )
            if proc.returncode != 0:
                log.error(f"Router claude failed: {stderr.decode()}")
                return None

            outer = json.loads(stdout.decode())

            # Capture session ID so future calls resume this conversation
            if outer.get("session_id"):
                self._router_sessions[chat_id] = outer["session_id"]

            result_text = outer.get("result", "")
            clean = result_text.strip()
            if clean.startswith("```"):
                clean = "\n".join(clean.split("\n")[1:])
            if clean.endswith("```"):
                clean = clean.rsplit("```", 1)[0]
            return json.loads(clean.strip())

        except (json.JSONDecodeError, asyncio.TimeoutError) as e:
            log.error(f"Router parse error: {e}")
            return None
        except Exception as e:
            log.exception(f"Router error: {e}")
            return None
