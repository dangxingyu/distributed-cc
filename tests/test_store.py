"""Test SQLite store — messages and tasks."""

import asyncio
import os
import pytest
from src.store import Store, TaskStatus


@pytest.fixture
async def store(tmp_path):
    db_path = str(tmp_path / "test.db")
    s = Store(db_path)
    await s.init()
    yield s
    await s.close()


@pytest.mark.asyncio
async def test_store_init(store):
    """Store creates tables without error."""
    assert store is not None


@pytest.mark.asyncio
async def test_add_and_get_messages(store):
    await store.add_message(1, "user", "hello")
    await store.add_message(1, "assistant", "hi there")
    await store.add_message(1, "user", "how are you")

    msgs = await store.get_recent_messages(1, limit=10)
    assert len(msgs) == 3
    assert msgs[0]["role"] == "user"
    assert msgs[0]["content"] == "hello"
    assert msgs[1]["role"] == "assistant"
    assert msgs[2]["content"] == "how are you"


@pytest.mark.asyncio
async def test_messages_per_chat(store):
    """Messages are isolated per chat_id."""
    await store.add_message(1, "user", "chat 1")
    await store.add_message(2, "user", "chat 2")

    msgs1 = await store.get_recent_messages(1)
    msgs2 = await store.get_recent_messages(2)
    assert len(msgs1) == 1
    assert len(msgs2) == 1
    assert msgs1[0]["content"] == "chat 1"
    assert msgs2[0]["content"] == "chat 2"


@pytest.mark.asyncio
async def test_message_limit(store):
    for i in range(30):
        await store.add_message(1, "user", f"msg {i}")

    msgs = await store.get_recent_messages(1, limit=5)
    assert len(msgs) == 5
    # Should be the 5 most recent
    assert msgs[-1]["content"] == "msg 29"


@pytest.mark.asyncio
async def test_task_lifecycle(store):
    """Create → running → done."""
    task_id = await store.create_task(
        chat_id=1,
        user_message="fix the bug",
        server_name="local",
        session_id="dev",
        prompt_sent="please fix the bug in auth.py",
    )
    assert task_id > 0

    running = await store.get_running_tasks(1)
    assert len(running) == 1
    assert running[0].server_name == "local"
    assert running[0].session_id == "dev"
    assert running[0].status == TaskStatus.RUNNING

    await store.finish_task(task_id, TaskStatus.DONE, "Fixed!")
    running = await store.get_running_tasks(1)
    assert len(running) == 0


@pytest.mark.asyncio
async def test_task_failed(store):
    task_id = await store.create_task(1, "test", "srv", "sess", "prompt")
    await store.finish_task(task_id, TaskStatus.FAILED, "timeout")
    running = await store.get_running_tasks(1)
    assert len(running) == 0
