"""JSON file persistence for conversation history and channel management.

Simplified for the new router architecture: no task/worker tracking
(that's handled by the remote daemon). Keeps messages, logs, notes,
and channel-project mappings.
"""

import json
import os
import time
from pathlib import Path


class Store:
    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        self._channels_dir = os.path.join(data_dir, "channels")

    async def init(self):
        os.makedirs(self._channels_dir, exist_ok=True)

    async def close(self):
        pass

    def _channel_path(self, chat_id: int) -> str:
        return os.path.join(self._channels_dir, f"{chat_id}.json")

    def _load(self, chat_id: int) -> dict:
        path = self._channel_path(chat_id)
        if not os.path.exists(path):
            return {
                "meta": {"name": ""},
                "messages": [],
                "notes": [],
                "logs": [],
            }
        with open(path, "r") as f:
            data = json.load(f)
        # Backward compat
        for key in ("notes", "logs"):
            if key not in data:
                data[key] = []
        if "meta" not in data:
            data["meta"] = {"name": f"Channel {chat_id}"}
        return data

    def _save(self, chat_id: int, data: dict):
        path = self._channel_path(chat_id)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)

    # ── messages ──

    async def add_message(self, chat_id: int, role: str, content: str):
        data = self._load(chat_id)
        data["messages"].append({"role": role, "content": content, "ts": time.time()})
        self._save(chat_id, data)

    async def get_recent_messages(self, chat_id: int) -> list[dict]:
        data = self._load(chat_id)
        return [{"role": m["role"], "content": m["content"]} for m in data["messages"]]

    # ── channel management ──

    async def create_channel(self, name: str) -> int:
        """Create a new channel with auto-incremented ID. Returns the chat_id."""
        existing = await self.get_all_channel_ids()
        chat_id = max(existing, default=0) + 1
        data = {
            "meta": {"name": name},
            "messages": [],
            "notes": [],
            "logs": [],
        }
        self._save(chat_id, data)
        return chat_id

    async def get_channel_list(self) -> list[dict]:
        """Return list of {id, name} for all channels, sorted by id."""
        result = []
        for chat_id in await self.get_all_channel_ids():
            data = self._load(chat_id)
            name = data.get("meta", {}).get("name", "") or f"Channel {chat_id}"
            result.append({"id": chat_id, "name": name})
        result.sort(key=lambda c: c["id"])
        return result

    async def get_all_channel_ids(self) -> list[int]:
        ids = []
        for fname in os.listdir(self._channels_dir):
            if fname.endswith(".json"):
                try:
                    ids.append(int(fname[:-5]))
                except ValueError:
                    pass
        return ids

    async def delete_channel(self, chat_id: int):
        path = self._channel_path(chat_id)
        if os.path.exists(path):
            os.remove(path)

    # ── logs (monitor) ──

    async def add_log(self, chat_id: int, text: str):
        data = self._load(chat_id)
        data["logs"].append({"text": text, "ts": time.time()})
        self._save(chat_id, data)

    async def get_logs(self, chat_id: int) -> list[dict]:
        data = self._load(chat_id)
        return data.get("logs", [])

    # ── notes ──

    async def add_note(self, chat_id: int, content: str):
        data = self._load(chat_id)
        data["notes"].append({"content": content, "ts": time.time(), "checked": False})
        self._save(chat_id, data)

    async def get_unchecked_notes(self, chat_id: int) -> list[dict]:
        data = self._load(chat_id)
        return [n for n in data["notes"] if not n.get("checked")]

    async def mark_notes_checked(self, chat_id: int):
        data = self._load(chat_id)
        changed = False
        for n in data["notes"]:
            if not n.get("checked"):
                n["checked"] = True
                changed = True
        if changed:
            self._save(chat_id, data)
