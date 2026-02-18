"""Test JSON file store — messages, tasks, and channel workers."""

import asyncio
import os
import pytest
from src.store import Store, TaskStatus, ChannelWorker


@pytest.fixture
async def store(tmp_path):
    s = Store(str(tmp_path / "data"))
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


# ---- channel workers ----


@pytest.mark.asyncio
async def test_channel_worker_round_trip(store):
    """Add a worker and retrieve it."""
    await store.add_channel_worker(1, "server-a", "ml-pipeline", "/home/user/ml-pipeline", "ML training")
    workers = await store.get_channel_workers(1)
    assert len(workers) == 1
    assert workers[0].server == "server-a"
    assert workers[0].session_id == "ml-pipeline"
    assert workers[0].work_dir == "/home/user/ml-pipeline"
    assert workers[0].description == "ML training"


@pytest.mark.asyncio
async def test_channel_workers_per_chat(store):
    """Workers are isolated per chat_id."""
    await store.add_channel_worker(1, "server-a", "proj-1", "/home/user/proj-1")
    await store.add_channel_worker(2, "server-b", "proj-2", "/home/user/proj-2")

    w1 = await store.get_channel_workers(1)
    w2 = await store.get_channel_workers(2)
    assert len(w1) == 1
    assert len(w2) == 1
    assert w1[0].session_id == "proj-1"
    assert w2[0].session_id == "proj-2"


@pytest.mark.asyncio
async def test_channel_worker_upsert(store):
    """INSERT OR REPLACE updates description on conflict."""
    await store.add_channel_worker(1, "srv", "sess", "/dir", "old")
    await store.add_channel_worker(1, "srv", "sess", "/dir", "new")
    workers = await store.get_channel_workers(1)
    assert len(workers) == 1
    assert workers[0].description == "new"


@pytest.mark.asyncio
async def test_remove_channel_worker(store):
    await store.add_channel_worker(1, "srv", "sess", "/dir")
    await store.remove_channel_worker(1, "srv", "sess")
    workers = await store.get_channel_workers(1)
    assert len(workers) == 0


@pytest.mark.asyncio
async def test_channel_workers_empty(store):
    workers = await store.get_channel_workers(999)
    assert workers == []


# ---- JSON backend specific ----


@pytest.mark.asyncio
async def test_task_ids_unique_across_channels(store):
    """Task IDs are globally unique, not per-channel."""
    t1 = await store.create_task(1, "task a", "srv", "sess", "prompt a")
    t2 = await store.create_task(2, "task b", "srv", "sess", "prompt b")
    t3 = await store.create_task(1, "task c", "srv", "sess", "prompt c")
    assert t1 != t2 != t3
    assert len({t1, t2, t3}) == 3


@pytest.mark.asyncio
async def test_finish_task_cross_channel(store):
    """finish_task finds the right channel via in-memory mapping."""
    t1 = await store.create_task(1, "msg", "srv", "sess", "prompt")
    t2 = await store.create_task(2, "msg", "srv", "sess", "prompt")

    await store.finish_task(t1, TaskStatus.DONE, "result 1")
    await store.finish_task(t2, TaskStatus.FAILED, "result 2")

    assert await store.get_running_tasks(1) == []
    assert await store.get_running_tasks(2) == []


@pytest.mark.asyncio
async def test_finish_task_unknown_id(store):
    """finish_task with nonexistent ID is a no-op."""
    await store.finish_task(9999, TaskStatus.DONE, "no crash")


@pytest.mark.asyncio
async def test_persistence_across_instances(tmp_path):
    """Data survives closing and reopening the store."""
    data_dir = str(tmp_path / "data")

    s1 = Store(data_dir)
    await s1.init()
    await s1.add_message(1, "user", "hello")
    task_id = await s1.create_task(1, "msg", "srv", "sess", "prompt")
    await s1.add_channel_worker(1, "srv", "sess", "/dir", "desc")
    await s1.close()

    s2 = Store(data_dir)
    await s2.init()

    msgs = await s2.get_recent_messages(1)
    assert len(msgs) == 1
    assert msgs[0]["content"] == "hello"

    running = await s2.get_running_tasks(1)
    assert len(running) == 1
    assert running[0].id == task_id

    workers = await s2.get_channel_workers(1)
    assert len(workers) == 1
    assert workers[0].description == "desc"

    # Task ID counter continues from where it left off
    t2 = await s2.create_task(1, "msg2", "srv", "sess", "prompt2")
    assert t2 > task_id

    await s2.close()


@pytest.mark.asyncio
async def test_task_id_continues_after_restart(tmp_path):
    """Task IDs don't collide after a restart."""
    data_dir = str(tmp_path / "data")

    s1 = Store(data_dir)
    await s1.init()
    t1 = await s1.create_task(1, "a", "s", "s", "p")
    t2 = await s1.create_task(1, "b", "s", "s", "p")
    await s1.close()

    s2 = Store(data_dir)
    await s2.init()
    t3 = await s2.create_task(1, "c", "s", "s", "p")
    assert t3 > t2

    await s2.close()


@pytest.mark.asyncio
async def test_get_recent_messages_empty(store):
    """Querying messages for a channel with no data returns empty list."""
    msgs = await store.get_recent_messages(999)
    assert msgs == []


@pytest.mark.asyncio
async def test_get_running_tasks_empty(store):
    """Querying tasks for a channel with no data returns empty list."""
    tasks = await store.get_running_tasks(999)
    assert tasks == []


@pytest.mark.asyncio
async def test_json_file_structure(store, tmp_path):
    """Verify the on-disk JSON has the expected shape."""
    await store.add_message(42, "user", "hi")
    await store.create_task(42, "msg", "srv", "sess", "prompt")
    await store.add_channel_worker(42, "srv", "sess", "/dir", "desc")

    import json
    path = tmp_path / "data" / "channels" / "42.json"
    assert path.exists()

    with open(path) as f:
        data = json.load(f)

    assert "messages" in data
    assert "tasks" in data
    assert "workers" in data
    assert len(data["messages"]) == 1
    assert len(data["tasks"]) == 1
    assert len(data["workers"]) == 1
    assert data["messages"][0]["content"] == "hi"
    assert data["tasks"][0]["server_name"] == "srv"
    assert data["workers"][0]["description"] == "desc"
