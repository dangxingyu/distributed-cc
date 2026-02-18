"""JSON file persistence for conversation history and task state."""

import json
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


@dataclass
class ChannelWorker:
    server: str
    session_id: str
    work_dir: str
    description: str


@dataclass
class Task:
    id: int
    telegram_chat_id: int
    user_message: str
    server_name: str
    session_id: str
    prompt_sent: str
    status: TaskStatus
    result: str | None = None
    created_at: float = 0
    finished_at: float | None = None


_EMPTY_CHANNEL = {
    "meta": {"name": ""},
    "messages": [], "workers": [], "tasks": [], "notes": [], "next_task_id": 1,
}


class Store:
    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        self._channels_dir = os.path.join(data_dir, "channels")
        self._next_task_id = 1
        self._task_to_chat: dict[int, int] = {}

    async def init(self):
        os.makedirs(self._channels_dir, exist_ok=True)
        # Scan existing channel files to rebuild in-memory state
        for fname in os.listdir(self._channels_dir):
            if not fname.endswith(".json"):
                continue
            chat_id = int(fname[:-5])
            data = self._load(chat_id)
            for t in data.get("tasks", []):
                tid = t["id"]
                self._task_to_chat[tid] = chat_id
                if tid >= self._next_task_id:
                    self._next_task_id = tid + 1

    async def close(self):
        pass

    def _channel_path(self, chat_id: int) -> str:
        return os.path.join(self._channels_dir, f"{chat_id}.json")

    def _load(self, chat_id: int) -> dict:
        path = self._channel_path(chat_id)
        if not os.path.exists(path):
            return {
                "meta": {"name": ""},
                "messages": [], "workers": [], "tasks": [], "notes": [], "next_task_id": 1,
            }
        with open(path, "r") as f:
            data = json.load(f)
        # Backward compat: old channels may lack these fields
        if "notes" not in data:
            data["notes"] = []
        if "meta" not in data:
            data["meta"] = {"name": f"Channel {chat_id}"}
        return data

    def _save(self, chat_id: int, data: dict):
        path = self._channel_path(chat_id)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)

    # ---- messages ----

    async def add_message(self, chat_id: int, role: str, content: str):
        data = self._load(chat_id)
        data["messages"].append({"role": role, "content": content, "ts": time.time()})
        self._save(chat_id, data)

    async def get_recent_messages(self, chat_id: int) -> list[dict]:
        data = self._load(chat_id)
        return [{"role": m["role"], "content": m["content"]} for m in data["messages"]]

    # ---- tasks ----

    async def create_task(
        self, chat_id: int, user_message: str, server_name: str, session_id: str, prompt_sent: str
    ) -> int:
        data = self._load(chat_id)
        task_id = self._next_task_id
        self._next_task_id += 1
        data["tasks"].append({
            "id": task_id,
            "chat_id": chat_id,
            "user_message": user_message,
            "server_name": server_name,
            "session_id": session_id,
            "prompt_sent": prompt_sent,
            "status": TaskStatus.RUNNING.value,
            "result": None,
            "created_at": time.time(),
            "finished_at": None,
        })
        self._task_to_chat[task_id] = chat_id
        self._save(chat_id, data)
        return task_id

    async def finish_task(self, task_id: int, status: TaskStatus, result: str):
        chat_id = self._task_to_chat.get(task_id)
        if chat_id is None:
            return
        data = self._load(chat_id)
        for t in data["tasks"]:
            if t["id"] == task_id:
                t["status"] = status.value
                t["result"] = result
                t["finished_at"] = time.time()
                break
        self._save(chat_id, data)

    async def get_running_tasks(self, chat_id: int) -> list[Task]:
        data = self._load(chat_id)
        tasks = []
        for t in data["tasks"]:
            if t.get("status") == TaskStatus.RUNNING.value:
                tasks.append(Task(
                    id=t["id"],
                    telegram_chat_id=t.get("chat_id", chat_id),
                    user_message=t["user_message"],
                    server_name=t["server_name"],
                    session_id=t["session_id"],
                    prompt_sent=t["prompt_sent"],
                    status=TaskStatus(t["status"]),
                    result=t.get("result"),
                    created_at=t.get("created_at", 0),
                    finished_at=t.get("finished_at"),
                ))
        return tasks

    # ---- channel workers ----

    async def add_channel_worker(
        self, chat_id: int, server: str, session_id: str, work_dir: str, description: str = ""
    ):
        data = self._load(chat_id)
        # Upsert: replace existing worker with same (server, session_id)
        data["workers"] = [
            w for w in data["workers"]
            if not (w["server"] == server and w["session_id"] == session_id)
        ]
        data["workers"].append({
            "server": server,
            "session_id": session_id,
            "work_dir": work_dir,
            "description": description,
            "created_at": time.time(),
        })
        self._save(chat_id, data)

    async def get_channel_workers(self, chat_id: int) -> list[ChannelWorker]:
        data = self._load(chat_id)
        return [
            ChannelWorker(
                server=w["server"],
                session_id=w["session_id"],
                work_dir=w["work_dir"],
                description=w.get("description", ""),
            )
            for w in sorted(data["workers"], key=lambda w: w.get("created_at", 0))
        ]

    async def get_all_channel_ids(self) -> list[int]:
        """Return all chat_ids that have channel files."""
        ids = []
        for fname in os.listdir(self._channels_dir):
            if fname.endswith(".json"):
                ids.append(int(fname[:-5]))
        return ids

    # ---- channel management ----

    async def create_channel(self, name: str) -> int:
        """Create a new channel with auto-incremented ID. Returns the chat_id."""
        existing = await self.get_all_channel_ids()
        chat_id = max(existing, default=0) + 1
        data = {
            "meta": {"name": name},
            "messages": [], "workers": [], "tasks": [], "notes": [], "next_task_id": 1,
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

    async def set_channel_name(self, chat_id: int, name: str):
        """Update the name of a channel."""
        data = self._load(chat_id)
        data.setdefault("meta", {})["name"] = name
        self._save(chat_id, data)

    async def delete_channel(self, chat_id: int):
        """Delete a channel's JSON file."""
        path = self._channel_path(chat_id)
        if os.path.exists(path):
            os.remove(path)

    async def remove_channel_worker(self, chat_id: int, server: str, session_id: str):
        data = self._load(chat_id)
        data["workers"] = [
            w for w in data["workers"]
            if not (w["server"] == server and w["session_id"] == session_id)
        ]
        self._save(chat_id, data)

    # ---- logs (monitor) ----

    async def add_log(self, chat_id: int, text: str):
        data = self._load(chat_id)
        if "logs" not in data:
            data["logs"] = []
        data["logs"].append({"text": text, "ts": time.time()})
        self._save(chat_id, data)

    async def get_logs(self, chat_id: int) -> list[dict]:
        data = self._load(chat_id)
        return data.get("logs", [])

    # ---- notes ----

    async def add_note(self, chat_id: int, content: str):
        """Append a channel note (unchecked by default)."""
        data = self._load(chat_id)
        data["notes"].append({"content": content, "ts": time.time(), "checked": False})
        self._save(chat_id, data)

    async def get_unchecked_notes(self, chat_id: int) -> list[dict]:
        """Return all unchecked notes for a channel."""
        data = self._load(chat_id)
        return [n for n in data["notes"] if not n.get("checked")]

    async def mark_notes_checked(self, chat_id: int):
        """Mark all unchecked notes as checked."""
        data = self._load(chat_id)
        changed = False
        for n in data["notes"]:
            if not n.get("checked"):
                n["checked"] = True
                changed = True
        if changed:
            self._save(chat_id, data)
