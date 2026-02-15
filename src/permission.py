"""Permission and clarification evaluation.

Handles two types of callbacks from remote brokers:
1. Permission requests — broker's canUseTool fires for Bash/Write/etc.
2. Clarification requests — broker's canUseTool fires for AskUserQuestion

Both follow the same pattern:
  fast-path auto-decide → Claude evaluates → if unsure → escalate to human
"""

import asyncio
import json
import logging
import uuid

log = logging.getLogger(__name__)

# ── Prompts ──────────────────────────────────────────────────────────────

PERMISSION_EVAL_PROMPT = """\
You are a security evaluator for a distributed Claude Code system.
A remote Claude Code session wants to perform the following action:

Server: {server_name}
Session: {session_id}
Tool: {tool_name}
Input: {tool_input}

Read `{config_path}`, `config.md`, and `setup.md` (if they exist) to understand the project context, \
what each server/session is supposed to do, and what operations are expected.

Then decide whether to allow this action. Your final response MUST be ONLY a JSON object:
{{"decision": "approve" | "deny" | "escalate", "reason": "brief explanation"}}

Guidelines:
- approve: Safe read-only operations, running tests, linting, and actions that align with the session's stated purpose
- deny: Clearly destructive actions (rm -rf /, DROP TABLE, modifying /etc, force-pushing to main)
- escalate: Anything ambiguous — writing to files you're unsure about, running unfamiliar commands, network operations, installing packages
"""

CLARIFICATION_EVAL_PROMPT = """\
You are an orchestrator managing a remote Claude Code session.
The session is asking a clarifying question:

Server: {server_name}
Session: {session_id}

{questions_formatted}

Read `{config_path}`, `config.md`, and `setup.md` (if they exist) to understand the project context \
and what each server/session is working on.

Can you confidently answer based on that context? Your final response MUST be ONLY a JSON object:
{{"can_answer": true | false, "answers": {{"<question_text>": "<chosen_option_label>"}}, "reason": "why you chose this or why you need human input"}}

Guidelines:
- If the answer is obvious from the project context, answer it.
- If it's a design/preference decision, set can_answer=false to ask the human.
- Always pick from the provided option labels exactly.
"""


class PermissionEvaluator:
    """Evaluates permission and clarification requests.

    Uses Claude for judgment, escalates to Telegram when unsure.
    """

    def __init__(self, model: str = "claude-opus-4-6", config_path: str = "config.yaml"):
        self._model = model
        self._config_path = config_path
        # request_id -> asyncio.Future for pending human decisions
        self._pending: dict[str, asyncio.Future] = {}
        # request_id -> question metadata (for clarification buttons)
        self._pending_meta: dict[str, dict] = {}

    # ── Permission evaluation ──────────────────────────────────────────

    async def evaluate_permission(
        self,
        server_name: str,
        session_id: str,
        tool_name: str,
        tool_input: dict,
        send_escalation: callable,
    ) -> dict:
        """Returns: {"approved": bool, "reason": str}"""
        # Fast path: auto-approve safe tools
        safe_tools = {"Read", "Grep", "Glob", "WebSearch", "WebFetch", "Explore"}
        if tool_name in safe_tools:
            return {"approved": True, "reason": f"Auto-approved: {tool_name}"}

        # Claude evaluates (reads config for context)
        prompt = PERMISSION_EVAL_PROMPT.format(
            server_name=server_name,
            session_id=session_id,
            tool_name=tool_name,
            tool_input=json.dumps(tool_input, ensure_ascii=False)[:2000],
            config_path=self._config_path,
        )
        decision = await self._call_claude(prompt)
        if decision is None:
            decision = {"decision": "escalate", "reason": "Evaluator error"}

        if decision["decision"] == "approve":
            return {"approved": True, "reason": decision.get("reason", "")}
        elif decision["decision"] == "deny":
            return {"approved": False, "reason": decision.get("reason", "")}
        else:
            return await self._escalate(
                interaction_type="permission",
                title=f"Permission: {tool_name}",
                detail=f"Server: {server_name}/{session_id}\nTool: {tool_name}\nInput: {json.dumps(tool_input, ensure_ascii=False)[:500]}",
                send_escalation=send_escalation,
            )

    # ── Clarification evaluation (AskUserQuestion) ─────────────────────

    async def evaluate_clarification(
        self,
        server_name: str,
        session_id: str,
        questions: list[dict],
        send_escalation: callable,
    ) -> dict:
        """Evaluate AskUserQuestion from a remote session.

        Returns: {"answers": {"question": "answer", ...}} or
                 {"answers": None, "reason": "..."}
        """
        # Format questions for Claude
        q_lines = []
        for i, q in enumerate(questions, 1):
            opts = ", ".join(o.get("label", "?") for o in q.get("options", []))
            q_lines.append(f"Q{i} [{q.get('header', '')}]: {q['question']}\n    Options: {opts}")
        questions_formatted = "\n".join(q_lines)

        prompt = CLARIFICATION_EVAL_PROMPT.format(
            server_name=server_name,
            session_id=session_id,
            questions_formatted=questions_formatted,
            config_path=self._config_path,
        )
        assessment = await self._call_claude(prompt)

        if assessment and assessment.get("can_answer"):
            return {"answers": assessment["answers"]}

        # Escalate to human — send questions with option buttons
        return await self._escalate_clarification(
            server_name=server_name,
            session_id=session_id,
            questions=questions,
            reason=assessment.get("reason", "Needs human input") if assessment else "Evaluator error",
            send_escalation=send_escalation,
        )

    async def _escalate_clarification(
        self,
        server_name: str,
        session_id: str,
        questions: list[dict],
        reason: str,
        send_escalation: callable,
    ) -> dict:
        """Escalate AskUserQuestion to Telegram with option buttons."""
        request_id = uuid.uuid4().hex[:12]
        future = asyncio.get_event_loop().create_future()
        self._pending[request_id] = future
        self._pending_meta[request_id] = {"questions": questions}

        # Build detail text
        detail_lines = [f"Server: {server_name}/{session_id}", f"Reason: {reason}", ""]
        for q in questions:
            detail_lines.append(f"  {q['question']}")

        await send_escalation(
            request_id,
            "clarification",
            "Clarification needed",
            "\n".join(detail_lines),
        )

        try:
            result = await asyncio.wait_for(future, timeout=300)
            self._pending_meta.pop(request_id, None)
            return result
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            self._pending_meta.pop(request_id, None)
            return {"answers": None, "reason": "Timed out waiting for human"}

    # ── Human resolution ───────────────────────────────────────────────

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
        meta = self._pending_meta.pop(request_id, None)
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

    # ── Claude helper ──────────────────────────────────────────────────

    async def _call_claude(self, prompt: str) -> dict | None:
        try:
            proc = await asyncio.create_subprocess_exec(
                "claude", "-p",
                "--output-format", "json",
                "--model", self._model,
                "--no-session-persistence",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(input=prompt.encode()), timeout=120
            )
            if proc.returncode != 0:
                log.error(f"Claude eval failed: {stderr.decode()}")
                return None

            outer = json.loads(stdout.decode())
            text = outer.get("result", "").strip()
            if text.startswith("```"):
                text = "\n".join(text.split("\n")[1:])
            if text.endswith("```"):
                text = text.rsplit("```", 1)[0]
            return json.loads(text.strip())
        except Exception as e:
            log.exception(f"Claude eval error: {e}")
            return None

    # ── Generic escalation (permission) ────────────────────────────────

    async def _escalate(
        self,
        interaction_type: str,
        title: str,
        detail: str,
        send_escalation: callable,
    ) -> dict:
        request_id = uuid.uuid4().hex[:12]
        future = asyncio.get_event_loop().create_future()
        self._pending[request_id] = future

        await send_escalation(request_id, interaction_type, title, detail)

        try:
            result = await asyncio.wait_for(future, timeout=300)
            return result
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            return {"approved": False, "reason": "Timed out"}
