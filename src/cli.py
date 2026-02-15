"""CLI frontend — talk to the orchestrator directly from your terminal.

Replaces Telegram for local development. Permission and clarification
escalations appear inline in the terminal.
"""

import asyncio
import logging
import sys

from .orchestrator import Orchestrator
from .permission import PermissionEvaluator

log = logging.getLogger(__name__)


class CLI:
    def __init__(self, orchestrator: Orchestrator, permission_evaluator: PermissionEvaluator):
        self._orchestrator = orchestrator
        self._permission = permission_evaluator
        self._chat_id = 0  # Dummy chat_id for CLI mode
        # Queue for permission/clarification prompts that interrupt the REPL
        self._interrupt_queue: asyncio.Queue = asyncio.Queue()

        self._orchestrator.set_send_telegram(self._send_escalation)

    async def run(self):
        """Main REPL loop."""
        print("Claude Code Orchestrator (CLI mode)")
        print("Type a message to route to a remote session.")
        print("Commands: /sessions, /tasks, /quit\n")

        # Run the input loop and interrupt handler concurrently
        await asyncio.gather(
            self._input_loop(),
            self._interrupt_handler(),
        )

    async def _input_loop(self):
        loop = asyncio.get_event_loop()
        while True:
            try:
                line = await loop.run_in_executor(None, self._read_input)
            except (EOFError, KeyboardInterrupt):
                break

            line = line.strip()
            if not line:
                continue
            if line == "/quit":
                break

            async def send_reply(msg: str):
                print(f"\n\033[36m{msg}\033[0m\n")  # Cyan for responses

            await self._orchestrator.handle_message(self._chat_id, line, send_reply)

    def _read_input(self) -> str:
        try:
            return input("\033[33myou>\033[0m ")  # Yellow prompt
        except EOFError:
            raise

    # ── Escalation handling ────────────────────────────────────────────

    async def _send_escalation(
        self,
        request_id: str,
        interaction_type: str,
        title: str,
        detail: str,
    ):
        """Queue an escalation for the interrupt handler."""
        await self._interrupt_queue.put({
            "request_id": request_id,
            "type": interaction_type,
            "title": title,
            "detail": detail,
        })

    async def _interrupt_handler(self):
        """Handle permission/clarification prompts from the queue."""
        loop = asyncio.get_event_loop()
        while True:
            item = await self._interrupt_queue.get()
            request_id = item["request_id"]
            itype = item["type"]

            if itype == "permission":
                print(f"\n\033[31m[PERMISSION] {item['title']}\033[0m")
                print(f"{item['detail']}")
                answer = await loop.run_in_executor(
                    None, lambda: input("\033[31mApprove? (y/n): \033[0m")
                )
                approved = answer.strip().lower() in ("y", "yes")
                self._permission.resolve_permission(
                    request_id, approved=approved,
                    reason="Approved by user" if approved else "Denied by user",
                )
                print(f">> {'APPROVED' if approved else 'DENIED'}\n")

            elif itype == "clarification":
                questions = self._permission.get_pending_questions(request_id)
                print(f"\n\033[35m[CLARIFICATION] {item['title']}\033[0m")
                print(f"{item['detail']}")

                if questions and len(questions) > 0:
                    q = questions[0]
                    options = q.get("options", [])
                    for i, opt in enumerate(options):
                        desc = f" — {opt['description']}" if opt.get("description") else ""
                        print(f"  [{i}] {opt['label']}{desc}")
                    choice = await loop.run_in_executor(
                        None, lambda: input("\033[35mChoose (number or type answer): \033[0m")
                    )
                    choice = choice.strip()
                    try:
                        idx = int(choice)
                        answer = options[idx]["label"]
                    except (ValueError, IndexError):
                        answer = choice  # Free-text answer

                    self._permission.resolve_clarification(request_id, q["question"], answer)
                    print(f">> {answer}\n")
                else:
                    answer = await loop.run_in_executor(
                        None, lambda: input("\033[35mYour answer: \033[0m")
                    )
                    self._permission.resolve_clarification(request_id, "", answer.strip())
                    print(f">> {answer.strip()}\n")
