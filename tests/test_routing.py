"""Test message routing: @orchestrator prefix, channel notes, /stop, queue ack, note injection."""

import asyncio
import tempfile
import pytest
from unittest.mock import AsyncMock, patch

from src.orchestrator import Orchestrator
from src.session import SessionManager, ServerConfig
from src.store import Store


async def _make_orchestrator(workers=None, servers=None):
    """Create an Orchestrator with mocked _send_to_orchestrator for testing."""
    store = Store(tempfile.mkdtemp())
    await store.init()

    server_configs = servers or []
    mgr = SessionManager(servers=server_configs, default_model="haiku")
    await mgr.init()

    orch = Orchestrator(
        session_mgr=mgr,
        store=store,
        model="haiku",
        cwd=".",
    )

    if workers:
        for server, session, chat_id in workers:
            orch._worker_to_chat[(server, session)] = chat_id

    return orch, mgr, store


# ── Routing tests ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_route_direct_message():
    """@orchestrator prefix → handle_message called with stripped text."""
    orch, mgr, store = await _make_orchestrator()
    try:
        calls = []

        async def mock_handle(chat_id, text, send_reply, send_log=None):
            calls.append((chat_id, text))
            await send_reply("ok")

        orch.handle_message = mock_handle
        send_reply = AsyncMock()

        await orch.route_message(1, "@orchestrator do the thing", send_reply)
        # Let the queue processor run
        await asyncio.sleep(0.05)

        assert len(calls) == 1
        assert calls[0] == (1, "do the thing")
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_route_channel_note():
    """No prefix (default_direct=False) → stored as note, '(noted)' ack."""
    orch, mgr, store = await _make_orchestrator()
    try:
        send_reply = AsyncMock()

        await orch.route_message(1, "I noticed the tests are flaky", send_reply)

        send_reply.assert_called_once_with("(noted)")
        notes = await store.get_unchecked_notes(1)
        assert len(notes) == 1
        assert notes[0]["content"] == "I noticed the tests are flaky"
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_route_stop():
    """@orchestrator /stop → cancel_task called on running tasks."""
    orch, mgr, store = await _make_orchestrator(
        workers=[("local", "dev", 1)],
        servers=[ServerConfig(name="local", host=None, broker_port=9999)],
    )
    try:
        # Create a running task
        task_id = await store.create_task(1, "do stuff", "local", "dev", "prompt")

        # Mock cancel_task
        mgr.cancel_task = AsyncMock(return_value=True)
        # Mock _send_to_orchestrator to avoid real SDK call
        orch._send_to_orchestrator = AsyncMock(return_value={"action": "reply", "text": "stopped"})

        send_reply = AsyncMock()

        await orch.route_message(1, "@orchestrator /stop", send_reply)
        await asyncio.sleep(0.05)

        mgr.cancel_task.assert_called_once_with("local", "dev")
        # Should have acked with cancellation count
        send_reply.assert_called_once()
        assert "cancelled 1/1" in send_reply.call_args[0][0]
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_route_default_direct():
    """default_direct=True → unprefixed message goes to handle_message."""
    orch, mgr, store = await _make_orchestrator()
    try:
        calls = []

        async def mock_handle(chat_id, text, send_reply, send_log=None):
            calls.append((chat_id, text))

        orch.handle_message = mock_handle
        send_reply = AsyncMock()

        await orch.route_message(1, "just do it", send_reply, default_direct=True)
        await asyncio.sleep(0.05)

        assert len(calls) == 1
        assert calls[0] == (1, "just do it")
    finally:
        await mgr.close()
        await store.close()


# ── Notes CRUD tests ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_note_crud():
    """add_note, get_unchecked_notes, mark_notes_checked lifecycle."""
    store = Store(tempfile.mkdtemp())
    await store.init()
    try:
        await store.add_note(1, "note A")
        await store.add_note(1, "note B")

        notes = await store.get_unchecked_notes(1)
        assert len(notes) == 2
        assert notes[0]["content"] == "note A"
        assert notes[1]["content"] == "note B"

        await store.mark_notes_checked(1)
        notes = await store.get_unchecked_notes(1)
        assert len(notes) == 0

        # Add a new one after checking
        await store.add_note(1, "note C")
        notes = await store.get_unchecked_notes(1)
        assert len(notes) == 1
        assert notes[0]["content"] == "note C"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_notes_per_channel():
    """Notes are isolated between channels."""
    store = Store(tempfile.mkdtemp())
    await store.init()
    try:
        await store.add_note(1, "channel 1 note")
        await store.add_note(2, "channel 2 note")

        n1 = await store.get_unchecked_notes(1)
        n2 = await store.get_unchecked_notes(2)
        assert len(n1) == 1
        assert len(n2) == 1
        assert n1[0]["content"] == "channel 1 note"
        assert n2[0]["content"] == "channel 2 note"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_notes_backward_compat(tmp_path):
    """Old channel files without 'notes' key still work."""
    import json, os

    data_dir = str(tmp_path / "data")
    channels_dir = os.path.join(data_dir, "channels")
    os.makedirs(channels_dir)

    # Write an old-format channel file (no "notes" key)
    old_data = {"messages": [{"role": "user", "content": "hi", "ts": 1.0}], "workers": [], "tasks": []}
    with open(os.path.join(channels_dir, "1.json"), "w") as f:
        json.dump(old_data, f)

    store = Store(data_dir)
    await store.init()
    try:
        # Should not crash — backward compat adds empty notes
        notes = await store.get_unchecked_notes(1)
        assert notes == []

        # Can add notes to migrated channel
        await store.add_note(1, "new note")
        notes = await store.get_unchecked_notes(1)
        assert len(notes) == 1
    finally:
        await store.close()


# ── Queue ack test ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_queue_ack_when_busy():
    """When session lock is held, direct messages get '(queued)' ack."""
    orch, mgr, store = await _make_orchestrator()
    try:
        send_reply = AsyncMock()

        # Acquire the lock to simulate busy orchestrator
        lock = orch._get_session_lock(1)
        await lock.acquire()

        # Don't set up handle_message mock — we just want the ack behavior
        # The queue processor will try to call handle_message but we'll release the lock eventually
        original_handle = orch.handle_message

        handle_called = asyncio.Event()

        async def mock_handle(chat_id, text, send_fn, send_log=None):
            handle_called.set()

        orch.handle_message = mock_handle

        await orch.route_message(1, "@orchestrator hello", send_reply)

        # Should have received "(queued)" ack
        send_reply.assert_called_once_with("(queued)")

        # Release the lock so processor can run
        lock.release()
        await asyncio.sleep(0.05)

        assert handle_called.is_set()
    finally:
        await mgr.close()
        await store.close()


# ── Note injection tests ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_note_injection_in_send():
    """Notes are prepended to orchestrator message text."""
    orch, mgr, store = await _make_orchestrator()
    try:
        # Add some notes
        await store.add_note(1, "remember to use pytest")
        await store.add_note(1, "prefer async over sync")

        captured_text = []

        async def mock_unlocked(chat_id, text, send_reply=None, send_log=None):
            captured_text.append(text)
            return {"action": "reply", "text": "ok"}

        orch._send_to_orchestrator_unlocked = mock_unlocked

        await orch._send_to_orchestrator(1, "Hello orchestrator")

        assert len(captured_text) == 1
        assert "[CHANNEL NOTES]" in captured_text[0]
        assert "remember to use pytest" in captured_text[0]
        assert "prefer async over sync" in captured_text[0]
        assert "Hello orchestrator" in captured_text[0]

        # Notes should now be checked
        remaining = await store.get_unchecked_notes(1)
        assert len(remaining) == 0
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_note_injection_noop_when_empty():
    """No notes → text passed through unchanged."""
    orch, mgr, store = await _make_orchestrator()
    try:
        captured_text = []

        async def mock_unlocked(chat_id, text, send_reply=None, send_log=None):
            captured_text.append(text)
            return {"action": "reply", "text": "ok"}

        orch._send_to_orchestrator_unlocked = mock_unlocked

        await orch._send_to_orchestrator(1, "Hello orchestrator")

        assert len(captured_text) == 1
        assert captured_text[0] == "Hello orchestrator"
        assert "[CHANNEL NOTES]" not in captured_text[0]
    finally:
        await mgr.close()
        await store.close()
