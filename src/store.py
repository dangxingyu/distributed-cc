"""SQLite persistence for conversation history and task state."""

import json
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import aiosqlite


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


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


SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    role TEXT NOT NULL,          -- 'user' or 'assistant'
    content TEXT NOT NULL,
    ts REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    user_message TEXT NOT NULL,
    server_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    prompt_sent TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    result TEXT,
    created_at REAL NOT NULL,
    finished_at REAL
);
"""


class Store:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init(self):
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA)
        await self._db.commit()

    async def close(self):
        if self._db:
            await self._db.close()

    # ---- messages ----

    async def add_message(self, chat_id: int, role: str, content: str):
        await self._db.execute(
            "INSERT INTO messages (chat_id, role, content, ts) VALUES (?, ?, ?, ?)",
            (chat_id, role, content, time.time()),
        )
        await self._db.commit()

    async def get_recent_messages(self, chat_id: int, limit: int = 20) -> list[dict]:
        cursor = await self._db.execute(
            "SELECT role, content FROM messages WHERE chat_id = ? ORDER BY ts DESC LIMIT ?",
            (chat_id, limit),
        )
        rows = await cursor.fetchall()
        return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]

    # ---- tasks ----

    async def create_task(
        self, chat_id: int, user_message: str, server_name: str, session_id: str, prompt_sent: str
    ) -> int:
        cursor = await self._db.execute(
            "INSERT INTO tasks (chat_id, user_message, server_name, session_id, prompt_sent, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (chat_id, user_message, server_name, session_id, prompt_sent, TaskStatus.RUNNING, time.time()),
        )
        await self._db.commit()
        return cursor.lastrowid

    async def finish_task(self, task_id: int, status: TaskStatus, result: str):
        await self._db.execute(
            "UPDATE tasks SET status = ?, result = ?, finished_at = ? WHERE id = ?",
            (status, result, time.time(), task_id),
        )
        await self._db.commit()

    async def get_running_tasks(self, chat_id: int) -> list[Task]:
        cursor = await self._db.execute(
            "SELECT * FROM tasks WHERE chat_id = ? AND status = ? ORDER BY created_at DESC",
            (chat_id, TaskStatus.RUNNING),
        )
        rows = await cursor.fetchall()
        return [self._row_to_task(r) for r in rows]

    @staticmethod
    def _row_to_task(row) -> Task:
        return Task(
            id=row["id"],
            telegram_chat_id=row["chat_id"],
            user_message=row["user_message"],
            server_name=row["server_name"],
            session_id=row["session_id"],
            prompt_sent=row["prompt_sent"],
            status=TaskStatus(row["status"]),
            result=row["result"],
            created_at=row["created_at"],
            finished_at=row["finished_at"],
        )
