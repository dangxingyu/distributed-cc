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

from .session import SessionManager, SessionResult
from .store import Store, TaskStatus
from .permission import PermissionEvaluator
from .formatter import format_result, format_routing_decision, format_task_status

log = logging.getLogger(__name__)

ROUTER_SYSTEM_PROMPT = """\
You are an orchestrator managing distributed Claude Code sessions across remote servers.

IMPORTANT: First, read the config file at `{config_path}` to understand the available servers \
and sessions. Also check for these optional files in the same directory and read them if they exist:
- `config.md` — extra instructions and context from the user about the setup
- `setup.md` — per-server environment details and capabilities

Based on the config and the user's message, decide what to do.

Your final response MUST be ONLY a JSON object (no markdown, no extra text):

1. Route a task to a remote session:
{{"action": "route", "server": "<server_name>", "session": "<session_id>", "prompt": "<detailed prompt to send>"}}

2. Answer directly (for simple questions, status checks, or chitchat):
{{"action": "reply", "text": "<your response>"}}

3. Show running tasks:
{{"action": "show_tasks"}}

Rules:
- When routing, write a clear, detailed prompt that captures the user's intent. \
Include relevant context from conversation history and what you learned from config/setup docs.
- If the user asks about a specific project/server, route to the matching session.
- If ambiguous which session to use, ask the user to clarify via "reply".
- For greetings, status questions, or meta-questions, use "reply".
- If the user asks to list sessions, read the config and reply with a formatted list directly.
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

            await send_reply(format_routing_decision(server, session, prompt))

            task_id = await self._store.create_task(
                chat_id, user_text, server, session, prompt
            )
            asyncio.create_task(
                self._execute_and_reply(task_id, chat_id, server, session, prompt, send_reply)
            )
        else:
            await send_reply(f"Unknown action: {action}")

    # ── Task execution ─────────────────────────────────────────────────

    async def _execute_and_reply(
        self,
        task_id: int,
        chat_id: int,
        server_name: str,
        session_id: str,
        prompt: str,
        send_reply: callable,
    ):
        """Run a remote task and send the result back.

        Permission and clarification are handled in real-time by the broker's
        canUseTool callback → HTTP → this orchestrator's endpoints.
        No post-hoc evaluation needed.
        """
        try:
            result: SessionResult = await self._session_mgr.run_task(
                server_name, session_id, prompt, task_id=task_id
            )
            status = TaskStatus.FAILED if result.is_error else TaskStatus.DONE
            await self._store.finish_task(task_id, status, result.result_text)

            reply = format_result(result.result_text, is_error=result.is_error)
            if result.cost_usd:
                reply += f"\n(cost: ${result.cost_usd:.4f})"
            if result.duration_secs:
                reply += f" ({result.duration_secs:.0f}s)"
            await self._store.add_message(chat_id, "assistant", reply)
            await send_reply(reply)

        except Exception as e:
            log.exception(f"Task #{task_id} failed: {e}")
            await self._store.finish_task(task_id, TaskStatus.FAILED, str(e))
            await send_reply(f"Task #{task_id} failed: {e}")

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
