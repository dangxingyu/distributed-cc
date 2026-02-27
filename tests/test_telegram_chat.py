"""Telegram frontend adapter tests (no real Telegram network calls)."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from src.router import RemoteOrchestrator, Router
from src.store import Store
from src.telegram_chat import TelegramChat


@pytest.fixture
async def telegram_ctx(tmp_path):
    store = Store(str(tmp_path / "data"))
    await store.init()

    router = Router()
    router._orchestrators = {
        "proj-a": RemoteOrchestrator(project_id="proj-a", name="server-a", status="idle"),
    }

    chat = TelegramChat(router=router, store=store, token="dummy-token")
    yield chat, router, store
    await store.close()


@pytest.mark.asyncio
async def test_normalize_command_with_bot_mention(telegram_ctx):
    chat, _, _ = telegram_ctx
    text = chat._normalize_incoming_text("/connect@MyBot proj-a", "MyBot")
    assert text == "/connect proj-a"


@pytest.mark.asyncio
async def test_normalize_setup_project_alias(telegram_ctx):
    chat, _, _ = telegram_ctx
    text = chat._normalize_incoming_text("/setup_project /home/ubuntu/repo", "mybot")
    assert text == "/setup-project /home/ubuntu/repo"


@pytest.mark.asyncio
async def test_normalize_upgrade_check_alias(telegram_ctx):
    chat, _, _ = telegram_ctx
    text = chat._normalize_incoming_text("/upgrade_check ftgs", "mybot")
    assert text == "/upgrade-check ftgs"


@pytest.mark.asyncio
async def test_normalize_other_bot_command_is_ignored(telegram_ctx):
    chat, _, _ = telegram_ctx
    text = chat._normalize_incoming_text("/connect@OtherBot proj-a", "MyBot")
    assert text == ""


@pytest.mark.asyncio
async def test_split_text_chunks(telegram_ctx):
    chat, _, _ = telegram_ctx
    long_text = "a" * 8000
    chunks = chat._split_text(long_text, max_len=3800)
    assert len(chunks) == 3
    assert sum(len(c) for c in chunks) == len(long_text)
    assert max(len(c) for c in chunks) <= 3800


@pytest.mark.asyncio
async def test_progress_orchestrator_text_is_persisted_and_sent(telegram_ctx):
    chat, router, store = telegram_ctx
    await router.connect_channel(12345, "proj-a")

    chat._send_text = AsyncMock()

    await chat._persist_and_emit_progress(
        chat_id=12345,
        project_id="proj-a",
        event_type="text",
        data_text="[orchestrator] Please rerun ablation with larger batch size.",
        iteration=3,
        ts=None,
    )

    messages = await store.get_recent_messages(12345)
    assert len(messages) == 1
    assert messages[0]["role"] == "assistant"
    assert messages[0]["sender"] == "orchestrator"
    assert "rerun ablation" in messages[0]["content"].lower()

    chat._send_text.assert_awaited_once()
    sent = chat._send_text.await_args.args[1]
    assert "[orchestrator]" in sent.lower()


@pytest.mark.asyncio
async def test_tool_use_progress_only_goes_to_log(telegram_ctx):
    chat, router, store = telegram_ctx
    await router.connect_channel(67890, "proj-a")

    chat._send_text = AsyncMock()

    await chat._persist_and_emit_progress(
        chat_id=67890,
        project_id="proj-a",
        event_type="tool_use",
        data_text="Bash: pytest -q",
        iteration=5,
        ts=None,
    )

    logs = await store.get_logs(67890)
    assert len(logs) == 1
    assert "pytest -q" in logs[0]["text"]
    chat._send_text.assert_not_called()


@pytest.mark.asyncio
async def test_progress_only_emits_for_telegram_channels(telegram_ctx):
    """Progress events should only be emitted to channels with source='telegram'."""
    chat, router, store = telegram_ctx

    # Create a web channel and a telegram channel on the same project
    web_ch = await store.create_channel("web-chan", project_id="proj-a", source="web")
    tg_ch = await store.create_channel("tg-chan", project_id="proj-a", source="telegram")

    await router.connect_channel(web_ch, "proj-a", source="web")
    await router.connect_channel(tg_ch, "proj-a", source="telegram")

    chat._send_text = AsyncMock()

    await chat._handle_progress("proj-a", {
        "type": "text",
        "data": "[orchestrator] hello from progress",
        "iteration": 1,
        "ts": 1234.0,
    })

    # Telegram channel should have the message persisted
    tg_msgs = await store.get_recent_messages(tg_ch)
    assert any("hello from progress" in m["content"] for m in tg_msgs)

    # Web channel should NOT have the message
    web_msgs = await store.get_recent_messages(web_ch)
    assert not any("hello from progress" in m["content"] for m in web_msgs)


@pytest.mark.asyncio
async def test_ensure_channel_exists_sets_source(telegram_ctx):
    """_ensure_channel_exists tags chat_id with telegram source."""
    chat, router, store = telegram_ctx

    assert router.get_channel_source(99999) is None
    await chat._ensure_channel_exists(99999, "Test Group")
    assert router.get_channel_source(99999) == "telegram"
    assert await store.get_channel_source(99999) == "telegram"


@pytest.mark.asyncio
async def test_handle_update_persists_source_for_new_chat(telegram_ctx):
    chat, router, store = telegram_ctx
    chat._send_text = AsyncMock()
    chat._route_message = AsyncMock()

    update = {
        "message": {
            "chat": {"id": 12345, "type": "private", "first_name": "Alice"},
            "text": "hello",
            "from": {"id": 1, "is_bot": False, "first_name": "Alice"},
        }
    }
    await chat._handle_update(update)
    await asyncio.sleep(0)

    assert router.get_channel_source(12345) == "telegram"
    assert await store.get_channel_source(12345) == "telegram"
