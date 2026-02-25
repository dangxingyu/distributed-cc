"""JSON file persistence for conversation history and channel management.

Simplified for the new router architecture: no task/worker tracking
(that's handled by the remote daemon). Keeps messages, logs, notes,
and channel-project mappings.
"""

import asyncio
import json
import os
import time


class Store:
    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        self._channels_dir = os.path.join(data_dir, "channels")
        self._channel_locks: dict[int, asyncio.Lock] = {}
        # Serializes channel-id allocation and directory-level operations.
        self._channels_meta_lock = asyncio.Lock()

    async def init(self):
        os.makedirs(self._channels_dir, exist_ok=True)

    async def close(self):
        pass

    def _channel_path(self, chat_id: int) -> str:
        return os.path.join(self._channels_dir, f"{chat_id}.json")

    def _default_channel_data(self, chat_id: int, name: str = "") -> dict:
        return {
            "meta": {"name": name or f"Channel {chat_id}", "project_id": None},
            "messages": [],
            "notes": [],
            "logs": [],
        }

    def _get_channel_lock(self, chat_id: int) -> asyncio.Lock:
        lock = self._channel_locks.get(chat_id)
        if lock is None:
            lock = asyncio.Lock()
            self._channel_locks[chat_id] = lock
        return lock

    def _list_channel_ids_unlocked(self) -> list[int]:
        ids = []
        for fname in os.listdir(self._channels_dir):
            if fname.endswith(".json"):
                try:
                    ids.append(int(fname[:-5]))
                except ValueError:
                    pass
        return ids

    def _load(self, chat_id: int) -> dict:
        path = self._channel_path(chat_id)
        if not os.path.exists(path):
            return self._default_channel_data(chat_id)
        with open(path, "r") as f:
            data = json.load(f)
        # Backward compat
        for key in ("notes", "logs"):
            if key not in data:
                data[key] = []
        if "meta" not in data:
            data["meta"] = {"name": f"Channel {chat_id}", "project_id": None}
        else:
            meta = data["meta"]
            if "name" not in meta or not meta["name"]:
                meta["name"] = f"Channel {chat_id}"
            if "project_id" not in meta:
                meta["project_id"] = None
        return data

    def _save(self, chat_id: int, data: dict):
        path = self._channel_path(chat_id)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)

    # ── messages ──

    async def add_message(self, chat_id: int, role: str, content: str, sender: str = ""):
        async with self._get_channel_lock(chat_id):
            entry = {"role": role, "content": content, "ts": time.time()}
            if sender:
                entry["sender"] = sender
            data = self._load(chat_id)
            data["messages"].append(entry)
            self._save(chat_id, data)

    async def get_recent_messages(self, chat_id: int) -> list[dict]:
        async with self._get_channel_lock(chat_id):
            data = self._load(chat_id)
            messages = []
            for m in data["messages"]:
                msg = {
                    "role": m.get("role", ""),
                    "content": m.get("content", ""),
                    "ts": m.get("ts"),
                }
                if m.get("sender"):
                    msg["sender"] = m["sender"]
                messages.append(msg)
            return messages

    # ── channel management ──

    async def create_channel(self, name: str, project_id: str | None = None) -> int:
        """Create a new channel with auto-incremented ID. Returns the chat_id."""
        async with self._channels_meta_lock:
            existing = self._list_channel_ids_unlocked()
            chat_id = max(existing, default=0) + 1
            data = self._default_channel_data(chat_id, name=name)
            data["meta"]["project_id"] = project_id or None
            self._save(chat_id, data)
            self._get_channel_lock(chat_id)
            return chat_id

    async def get_channel_list(self) -> list[dict]:
        """Return list of {id, name} for all channels, sorted by id."""
        async with self._channels_meta_lock:
            result = []
            for chat_id in self._list_channel_ids_unlocked():
                data = self._load(chat_id)
                name = data.get("meta", {}).get("name", "") or f"Channel {chat_id}"
                result.append({"id": chat_id, "name": name})
            result.sort(key=lambda c: c["id"])
            return result

    async def get_all_channel_ids(self) -> list[int]:
        async with self._channels_meta_lock:
            return self._list_channel_ids_unlocked()

    async def delete_channel(self, chat_id: int):
        async with self._channels_meta_lock:
            async with self._get_channel_lock(chat_id):
                path = self._channel_path(chat_id)
                if os.path.exists(path):
                    os.remove(path)
            self._channel_locks.pop(chat_id, None)

    async def set_channel_project(self, chat_id: int, project_id: str | None):
        async with self._get_channel_lock(chat_id):
            data = self._load(chat_id)
            data.setdefault("meta", {})
            data["meta"]["project_id"] = project_id or None
            if "name" not in data["meta"] or not data["meta"]["name"]:
                data["meta"]["name"] = f"Channel {chat_id}"
            self._save(chat_id, data)

    async def get_channel_project(self, chat_id: int) -> str | None:
        async with self._get_channel_lock(chat_id):
            data = self._load(chat_id)
            project_id = data.get("meta", {}).get("project_id")
            return project_id or None

    async def get_channel_project_map(self) -> dict[int, str]:
        mapping: dict[int, str] = {}
        for chat_id in await self.get_all_channel_ids():
            project_id = await self.get_channel_project(chat_id)
            if project_id:
                mapping[chat_id] = project_id
        return mapping

    # ── logs (monitor) ──

    async def add_log(self, chat_id: int, text: str):
        async with self._get_channel_lock(chat_id):
            data = self._load(chat_id)
            data["logs"].append({"text": text, "ts": time.time()})
            self._save(chat_id, data)

    async def get_logs(self, chat_id: int) -> list[dict]:
        async with self._get_channel_lock(chat_id):
            data = self._load(chat_id)
            return data.get("logs", [])

    # ── notes ──

    async def add_note(self, chat_id: int, content: str):
        async with self._get_channel_lock(chat_id):
            data = self._load(chat_id)
            data["notes"].append({"content": content, "ts": time.time(), "checked": False})
            self._save(chat_id, data)

    async def get_unchecked_notes(self, chat_id: int) -> list[dict]:
        async with self._get_channel_lock(chat_id):
            data = self._load(chat_id)
            return [n for n in data["notes"] if not n.get("checked")]

    async def mark_notes_checked(self, chat_id: int):
        async with self._get_channel_lock(chat_id):
            data = self._load(chat_id)
            changed = False
            for n in data["notes"]:
                if not n.get("checked"):
                    n["checked"] = True
                    changed = True
            if changed:
                self._save(chat_id, data)
