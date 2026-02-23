"""Telegram chat frontend.

This adapter reuses Router + Store and replaces browser WebSocket UI with a
Telegram bot group/direct-chat interface.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import aiohttp

from .router import Router
from .progress_rules import chat_messages_from_event, log_entries_from_event
from .store import Store

log = logging.getLogger(__name__)


class TelegramChat:
    def __init__(
        self,
        router: Router,
        store: Store,
        token: str,
        api_base: str = "https://api.telegram.org",
        drop_pending_updates: bool = True,
    ):
        self._router = router
        self._store = store
        self._token = token.strip()
        self._api_base = api_base.rstrip("/")
        self._drop_pending_updates = drop_pending_updates

        self._http: aiohttp.ClientSession | None = None
        self._poll_task: asyncio.Task | None = None
        self._routing_tasks: set[asyncio.Task] = set()
        self._bot_username: str = ""
        self._next_update_offset: int | None = None
        self._typing_last_sent: dict[int, float] = {}

        # Wire callbacks
        self._router.set_progress_callback(self._handle_progress)
        self._router.set_mapping_persist_callback(self._persist_channel_mapping)

    async def start(self):
        if not self._token:
            raise ValueError("Telegram token is required")

        await self._hydrate_channel_mappings()
        self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45))

        await self._load_bot_profile()
        if self._drop_pending_updates:
            await self._prime_update_offset()

        self._poll_task = asyncio.create_task(self._poll_loop())
        log.info("Telegram bot frontend started%s", f" as @{self._bot_username}" if self._bot_username else "")

    async def stop(self):
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

        for task in list(self._routing_tasks):
            task.cancel()
        if self._routing_tasks:
            await asyncio.gather(*self._routing_tasks, return_exceptions=True)
        self._routing_tasks.clear()

        if self._http:
            await self._http.close()
            self._http = None

    async def _hydrate_channel_mappings(self):
        mapping = await self._store.get_channel_project_map()
        for chat_id, project_id in mapping.items():
            self._router.hydrate_channel_mapping(chat_id, project_id)

    async def _persist_channel_mapping(self, chat_id: int, project_id: str | None):
        await self._store.set_channel_project(chat_id, project_id)

    def _base_url(self) -> str:
        return f"{self._api_base}/bot{self._token}"

    async def _api_get(self, method: str, params: dict | None = None, timeout: int = 45):
        if not self._http:
            raise RuntimeError("Telegram frontend not started")
        url = f"{self._base_url()}/{method}"
        async with self._http.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Telegram API {method} failed ({resp.status}): {text}")
            try:
                data = json.loads(text)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Telegram API {method} returned non-JSON: {text}") from e
            if not data.get("ok"):
                raise RuntimeError(f"Telegram API {method} error: {data}")
            return data.get("result")

    async def _api_post(self, method: str, payload: dict | None = None, timeout: int = 45):
        if not self._http:
            raise RuntimeError("Telegram frontend not started")
        url = f"{self._base_url()}/{method}"
        async with self._http.post(url, json=payload or {}, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Telegram API {method} failed ({resp.status}): {text}")
            try:
                data = json.loads(text)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Telegram API {method} returned non-JSON: {text}") from e
            if not data.get("ok"):
                raise RuntimeError(f"Telegram API {method} error: {data}")
            return data.get("result")

    async def _load_bot_profile(self):
        try:
            result = await self._api_get("getMe")
            self._bot_username = (result or {}).get("username", "")
        except Exception as e:
            log.warning("Failed to fetch Telegram bot profile: %s", e)
            self._bot_username = ""

    async def _prime_update_offset(self):
        """Drop pending backlog on startup to avoid replaying stale chat history."""
        try:
            updates = await self._api_get("getUpdates", params={"timeout": 0, "limit": 100})
        except Exception as e:
            log.warning("Failed to read pending Telegram updates: %s", e)
            return

        if not updates:
            return
        ids = [int(u.get("update_id", 0)) for u in updates if isinstance(u.get("update_id"), int)]
        newest_id = max(ids) if ids else 0
        if newest_id > 0:
            self._next_update_offset = newest_id + 1
            log.info("Dropped %d pending Telegram update(s)", len(updates))

    async def _poll_loop(self):
        while True:
            params: dict[str, object] = {"timeout": 25, "allowed_updates": json.dumps(["message"])}
            if self._next_update_offset is not None:
                params["offset"] = self._next_update_offset

            try:
                updates = await self._api_get("getUpdates", params=params, timeout=35)
                for update in updates or []:
                    update_id = update.get("update_id")
                    if isinstance(update_id, int):
                        self._next_update_offset = update_id + 1
                    await self._handle_update(update)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("Telegram polling error: %s", e)
                await asyncio.sleep(2)

    async def _handle_update(self, update: dict):
        message = update.get("message")
        if not isinstance(message, dict):
            return

        chat = message.get("chat", {})
        chat_id = chat.get("id")
        if not isinstance(chat_id, int):
            return

        text_raw = (message.get("text") or "").strip()
        if not text_raw:
            return

        sender = message.get("from", {}) or {}
        if sender.get("is_bot"):
            return

        text = self._normalize_incoming_text(text_raw, self._bot_username)
        if not text:
            return

        cmd = text.split(None, 1)[0].lower()
        if cmd in ("/start", "/help"):
            await self._send_text(chat_id, self._help_text())
            return

        sender_name = self._sender_label(sender)
        await self._store.add_message(chat_id, "user", text, sender=sender_name)

        async def send_reply(msg: str, sender: str = "system"):
            await self._store.add_message(chat_id, "assistant", msg, sender=sender)
            await self._send_text(chat_id, self._format_assistant_text(msg, sender))

        async def send_log(msg: str):
            await self._store.add_log(chat_id, msg)
            await self._send_text(chat_id, f"[log] {msg}")

        async def send_typing(active: bool, sender: str = "router"):
            if active:
                await self._send_typing(chat_id)

        task = asyncio.create_task(
            self._route_message(chat_id, text, send_reply, send_log, send_typing)
        )
        self._routing_tasks.add(task)
        task.add_done_callback(self._routing_tasks.discard)

    async def _route_message(
        self,
        chat_id: int,
        text: str,
        send_reply: callable,
        send_log: callable,
        send_typing: callable,
    ):
        try:
            await self._router.route_message(chat_id, text, send_reply, send_log, send_typing)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Routing failed for Telegram chat %s", chat_id)
            line = f"[ERROR] Routing failure: {e}"
            await self._store.add_log(chat_id, line)
            await self._send_text(chat_id, line)
            await self._store.add_message(chat_id, "assistant", f"Routing failure: {e}", sender="system")
            await self._send_text(chat_id, self._format_assistant_text(f"Routing failure: {e}", "system"))

    def _help_text(self) -> str:
        return (
            "Telegram interface is active.\n"
            "Use these commands:\n"
            "/connect <project-id>\n"
            "/connect\n"
            "/status\n"
            "/stop\n"
            "/setup <user@host>\n"
            "/setup-project <workdir or instruction>\n"
            "/setup_project <workdir or instruction> (Telegram-friendly alias)\n"
            "@router <message>\n"
            "@orchestrator <message>"
        )

    def _sender_label(self, sender: dict) -> str:
        username = sender.get("username")
        if username:
            return f"@{username}"
        first = sender.get("first_name", "").strip()
        last = sender.get("last_name", "").strip()
        name = f"{first} {last}".strip()
        return name or "user"

    def _format_assistant_text(self, msg: str, sender: str) -> str:
        clean_sender = (sender or "assistant").strip().lower()
        if clean_sender in ("assistant", ""):
            return msg
        return f"[{clean_sender}] {msg}"

    def _normalize_incoming_text(self, text: str, bot_username: str) -> str:
        stripped = text.strip()
        if not stripped:
            return ""

        parts = stripped.split(None, 1)
        head = parts[0]
        tail = parts[1] if len(parts) > 1 else ""

        if head.startswith("/"):
            cmd = head
            if "@" in head:
                base_cmd, mention = head.split("@", 1)
                if bot_username and mention.lower() != bot_username.lower():
                    return ""
                cmd = base_cmd

            cmd_lower = cmd.lower()
            if cmd_lower == "/setup_project":
                cmd = "/setup-project"

            stripped = f"{cmd} {tail}".strip()

        return stripped

    async def _send_typing(self, chat_id: int):
        now = time.monotonic()
        last = self._typing_last_sent.get(chat_id, 0.0)
        if now - last < 2.0:
            return
        self._typing_last_sent[chat_id] = now
        try:
            await self._api_post("sendChatAction", {"chat_id": chat_id, "action": "typing"})
        except Exception as e:
            log.debug("Failed to send typing to %s: %s", chat_id, e)

    async def _send_text(self, chat_id: int, text: str):
        for chunk in self._split_text(text):
            try:
                await self._api_post("sendMessage", {"chat_id": chat_id, "text": chunk})
            except Exception as e:
                log.warning("Failed to send Telegram message to %s: %s", chat_id, e)
                break

    def _split_text(self, text: str, max_len: int = 3800) -> list[str]:
        if len(text) <= max_len:
            return [text]
        chunks: list[str] = []
        remaining = text
        while remaining:
            if len(remaining) <= max_len:
                chunks.append(remaining)
                break
            cut = remaining.rfind("\n", 0, max_len)
            if cut <= 0:
                cut = max_len
            chunks.append(remaining[:cut])
            remaining = remaining[cut:].lstrip("\n")
        return chunks

    # -- Progress callback --------------------------------------------

    async def _handle_progress(self, project_id: str, event: dict):
        chat_ids = self._router.get_channels_for_project(project_id)
        if not chat_ids:
            return

        event_type = event.get("type", "")
        data_text = event.get("data", "")
        iteration = event.get("iteration", 0)
        ts = event.get("ts")

        for chat_id in chat_ids:
            await self._persist_and_emit_progress(chat_id, project_id, event_type, data_text, iteration, ts)

    async def _persist_and_emit_progress(
        self,
        chat_id: int,
        project_id: str,
        event_type: str,
        data_text: str,
        iteration: int,
        ts: float | None,
    ):
        for line in log_entries_from_event(event_type, data_text, tool_use_prefix="->"):
            await self._store.add_log(chat_id, line)
            if line.startswith("[ERROR]"):
                await self._send_text(chat_id, line)

        for sender, text in chat_messages_from_event(event_type, data_text):
            await self._store.add_message(chat_id, "assistant", text, sender=sender)
            await self._send_text(chat_id, self._format_assistant_text(text, sender))
