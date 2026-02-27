"""Integration tests for simultaneous web + telegram frontends.

Verifies that when both frontends are registered on the same router,
progress events are delivered to each frontend's channels exactly once
with no cross-delivery or double-writes.
"""

import tempfile

import pytest
from unittest.mock import AsyncMock

from src.router import RemoteOrchestrator, Router
from src.store import Store
from src.web import WebChat
from src.telegram_chat import TelegramChat


@pytest.fixture
async def multi_ctx(tmp_path):
    store = Store(str(tmp_path / "data"))
    await store.init()

    router = Router()
    router._orchestrators = {
        "shared-proj": RemoteOrchestrator(
            project_id="shared-proj", name="test-server", status="running"
        ),
    }

    web_chat = WebChat(router=router, store=store)
    tg_chat = TelegramChat(router=router, store=store, token="dummy-token")
    # Don't call start() — just verify listener wiring

    yield router, store, web_chat, tg_chat

    await web_chat.stop()
    await tg_chat.stop()
    await store.close()


async def test_both_frontends_register_listeners(multi_ctx):
    """Both frontends register their own progress and mapping persist listeners."""
    router, store, web_chat, tg_chat = multi_ctx

    assert web_chat._handle_progress in router._progress_listeners
    assert tg_chat._handle_progress in router._progress_listeners
    assert len(router._progress_listeners) == 2

    assert web_chat._persist_channel_mapping in router._mapping_persist_listeners
    assert tg_chat._persist_channel_mapping in router._mapping_persist_listeners
    assert len(router._mapping_persist_listeners) == 2


async def test_progress_no_cross_delivery(multi_ctx):
    """A progress event is delivered only to each frontend's own channels."""
    router, store, web_chat, tg_chat = multi_ctx

    # Create one channel per frontend on the same project
    web_ch = await store.create_channel("web-chan", project_id="shared-proj", source="web")
    tg_ch = await store.create_channel("tg-chan", project_id="shared-proj", source="telegram")

    await router.connect_channel(web_ch, "shared-proj", source="web")
    await router.connect_channel(tg_ch, "shared-proj", source="telegram")

    # Mock sends for telegram
    tg_chat._send_text = AsyncMock()

    # Fire a single progress event through the router (both listeners get called)
    event = {
        "event_id": "multi-evt-1",
        "type": "text",
        "data": "[orchestrator] multi-frontend test",
        "iteration": 1,
        "ts": 1000.0,
    }
    await router.ingest_progress_event("shared-proj", event, source="sse")

    # Web channel has the message
    web_msgs = await store.get_recent_messages(web_ch)
    web_progress = [m for m in web_msgs if "multi-frontend test" in m["content"]]
    assert len(web_progress) == 1, f"Expected 1 web message, got {len(web_progress)}"

    # Telegram channel has the message (exactly once)
    tg_msgs = await store.get_recent_messages(tg_ch)
    tg_progress = [m for m in tg_msgs if "multi-frontend test" in m["content"]]
    assert len(tg_progress) == 1, f"Expected 1 telegram message, got {len(tg_progress)}"


async def test_stop_removes_listeners(multi_ctx):
    """Stopping a frontend removes its listeners from the router."""
    router, store, web_chat, tg_chat = multi_ctx

    assert len(router._progress_listeners) == 2
    await web_chat.stop()
    assert web_chat._handle_progress not in router._progress_listeners
    assert tg_chat._handle_progress in router._progress_listeners
    assert len(router._progress_listeners) == 1


async def test_legacy_channels_go_to_web_only(multi_ctx):
    """Legacy channels (source=None) are handled by web, not telegram."""
    router, store, web_chat, tg_chat = multi_ctx

    legacy_ch = await store.create_channel("old-chan", project_id="shared-proj")  # source=None
    await router.connect_channel(legacy_ch, "shared-proj")
    # No source set — legacy

    tg_chat._send_text = AsyncMock()

    event = {
        "event_id": "legacy-evt-1",
        "type": "text",
        "data": "[orchestrator] legacy message",
        "iteration": 1,
        "ts": 2000.0,
    }
    await router.ingest_progress_event("shared-proj", event, source="sse")

    # Web should have it (source=None included for web)
    msgs = await store.get_recent_messages(legacy_ch)
    web_count = sum(1 for m in msgs if "legacy message" in m["content"])
    assert web_count == 1

    # Telegram _send_text should NOT have been called for this channel
    # (telegram only handles source="telegram")
    for call_args in tg_chat._send_text.call_args_list:
        assert call_args.args[0] != legacy_ch, "Telegram should not send to legacy channel"


async def test_hydration_does_not_reassign_legacy_to_telegram(multi_ctx):
    """When both frontends hydrate, legacy channels stay web-owned."""
    router, store, web_chat, tg_chat = multi_ctx

    legacy_ch = await store.create_channel("legacy", project_id="shared-proj")  # source=None

    await web_chat._hydrate_channel_mappings()
    assert router.get_channel_source(legacy_ch) == "web"

    await tg_chat._hydrate_channel_mappings()
    assert router.get_channel_source(legacy_ch) == "web"
