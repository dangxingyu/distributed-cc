"""Telegram bot gateway.

Receives messages from Telegram, forwards to the orchestrator,
and sends replies back to the user.

Handles escalation UI:
  - Permission: Approve / Deny inline buttons
  - Clarification (AskUserQuestion): option buttons matching Claude's choices
"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from .orchestrator import Orchestrator
from .permission import PermissionEvaluator

log = logging.getLogger(__name__)

# Callback data prefixes
CB_PERM_APPROVE = "pa:"       # pa:<request_id>
CB_PERM_DENY = "pd:"          # pd:<request_id>
CB_CLAR_ANSWER = "ca:"        # ca:<request_id>:<option_index>


class Bot:
    def __init__(
        self,
        token: str,
        allowed_users: list[int],
        orchestrator: Orchestrator,
        permission_evaluator: PermissionEvaluator,
    ):
        self._token = token
        self._allowed_users = set(allowed_users)
        self._orchestrator = orchestrator
        self._permission = permission_evaluator
        self._app: Application | None = None

        # Wire the Telegram escalation sender
        self._orchestrator.set_send_telegram(self._send_escalation)

    async def start(self):
        self._app = Application.builder().token(self._token).build()
        self._app.add_handler(CommandHandler("start", self._cmd_start))
        self._app.add_handler(CommandHandler("sessions", self._cmd_sessions))
        self._app.add_handler(CommandHandler("tasks", self._cmd_tasks))
        self._app.add_handler(CommandHandler("cancel", self._cmd_cancel))
        self._app.add_handler(CallbackQueryHandler(self._on_callback))
        self._app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._on_message))

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling()
        log.info("Telegram bot started.")

    async def stop(self):
        if self._app:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()

    # ── Escalation sender ──────────────────────────────────────────────

    async def _send_escalation(
        self,
        request_id: str,
        interaction_type: str,
        title: str,
        detail: str,
    ):
        """Send a permission or clarification prompt to Telegram."""
        chat_id = next(iter(self._allowed_users), None)
        if not self._app or chat_id is None:
            log.error("No chat_id for escalation")
            return

        if interaction_type == "permission":
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Approve", callback_data=f"{CB_PERM_APPROVE}{request_id}"),
                    InlineKeyboardButton("Deny", callback_data=f"{CB_PERM_DENY}{request_id}"),
                ]
            ])
            text = f"[Permission]\n{title}\n\n{detail}"
            await self._app.bot.send_message(chat_id=chat_id, text=text[:4096], reply_markup=keyboard)

        elif interaction_type == "clarification":
            # Build option buttons from the actual AskUserQuestion options
            questions = self._permission.get_pending_questions(request_id)
            if questions and len(questions) > 0:
                q = questions[0]  # Handle first question; multi-question support can be added
                buttons = []
                for i, opt in enumerate(q.get("options", [])):
                    label = opt.get("label", f"Option {i}")
                    # Callback data: ca:<request_id>:<option_index>
                    buttons.append(
                        InlineKeyboardButton(label, callback_data=f"{CB_CLAR_ANSWER}{request_id}:{i}")
                    )
                # Arrange buttons in rows of 2
                rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
                keyboard = InlineKeyboardMarkup(rows)
                text = f"[Clarification]\n{q['question']}\n\n{detail}"
            else:
                keyboard = None
                text = f"[Clarification]\n{title}\n\n{detail}"

            await self._app.bot.send_message(chat_id=chat_id, text=text[:4096], reply_markup=keyboard)

    # ── Callback handler ───────────────────────────────────────────────

    async def _on_callback(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        if not self._check_auth_user(query.from_user.id):
            await query.answer("Unauthorized.")
            return

        data = query.data

        # Permission: Approve
        if data.startswith(CB_PERM_APPROVE):
            request_id = data[len(CB_PERM_APPROVE):]
            ok = self._permission.resolve_permission(request_id, approved=True, reason="Approved")
            await query.answer("Approved" if ok else "Expired")
            await query.edit_message_text(f"{query.message.text}\n\n>> APPROVED")

        # Permission: Deny
        elif data.startswith(CB_PERM_DENY):
            request_id = data[len(CB_PERM_DENY):]
            ok = self._permission.resolve_permission(request_id, approved=False, reason="Denied")
            await query.answer("Denied" if ok else "Expired")
            await query.edit_message_text(f"{query.message.text}\n\n>> DENIED")

        # Clarification: option selected
        elif data.startswith(CB_CLAR_ANSWER):
            rest = data[len(CB_CLAR_ANSWER):]
            parts = rest.split(":", 1)
            if len(parts) != 2:
                await query.answer("Invalid")
                return

            request_id, opt_idx_str = parts
            try:
                opt_idx = int(opt_idx_str)
            except ValueError:
                await query.answer("Invalid")
                return

            questions = self._permission.get_pending_questions(request_id)
            if not questions:
                await query.answer("Expired")
                return

            q = questions[0]
            options = q.get("options", [])
            if opt_idx >= len(options):
                await query.answer("Invalid option")
                return

            chosen_label = options[opt_idx]["label"]
            ok = self._permission.resolve_clarification(request_id, q["question"], chosen_label)
            await query.answer(f"Selected: {chosen_label}" if ok else "Expired")
            await query.edit_message_text(f"{query.message.text}\n\n>> {chosen_label}")

        else:
            await query.answer("Unknown")

    # ── Message handler ────────────────────────────────────────────────

    async def _on_message(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return

        chat_id = update.effective_chat.id
        text = update.message.text

        async def send_reply(msg: str):
            while msg:
                chunk = msg[:4096]
                msg = msg[4096:]
                await ctx.bot.send_message(chat_id=chat_id, text=chunk)

        await self._orchestrator.handle_message(chat_id, text, send_reply)

    # ── Commands ───────────────────────────────────────────────────────

    async def _cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return
        await update.message.reply_text(
            "Claude Code Orchestrator ready.\n\n"
            "Send me a message to route to a remote session.\n"
            "/sessions - list sessions\n"
            "/tasks - running tasks\n"
            "/cancel <id> - cancel task"
        )

    async def _cmd_sessions(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._check_auth(update):
            return
        await self._orchestrator.handle_message(
            update.effective_chat.id, "/sessions",
            lambda text: update.message.reply_text(text),
        )

    async def _cmd_tasks(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._check_auth(update):
            return
        await self._orchestrator.handle_message(
            update.effective_chat.id, "/tasks",
            lambda text: update.message.reply_text(text),
        )

    async def _cmd_cancel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._check_auth(update):
            return
        if not ctx.args:
            await update.message.reply_text("Usage: /cancel <task_id>")
            return
        # TODO: cancel needs server_name + session_id, not just task_id
        await update.message.reply_text("Cancel not yet implemented for broker mode.")

    # ── Auth ───────────────────────────────────────────────────────────

    def _check_auth(self, update: Update) -> bool:
        return self._check_auth_user(update.effective_user.id)

    def _check_auth_user(self, user_id: int) -> bool:
        if user_id not in self._allowed_users:
            log.warning(f"Unauthorized user: {user_id}")
            return False
        return True
