"""Router routing and ingestion tests.

Covers:
- slash command routing (including @orchestrator-prefixed commands)
- urgency semantics: @orchestrator = urgent interrupt, non-mention while running = deferred
- progress ingestion dedupe (event_id + fallback signature)
- deferred queue autostart on terminal events
- /connect command via route_message
- router session routing
- config loading (dual schema) and reload
- channel/project mapping helpers
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.router import RemoteOrchestrator, Router


def _make_router(orchestrators=None):
    router = Router()
    if orchestrators:
        for o in orchestrators:
            router._orchestrators[o.project_id] = o
    return router


async def test_route_no_project_connected_goes_to_router():
    """Plain message with no project connected routes to router (sysadmin)."""
    router = _make_router([RemoteOrchestrator(project_id="test", name="test-srv")])
    send_reply = AsyncMock()

    with patch.object(router, "_handle_router_message", new_callable=AsyncMock) as mock_router:
        await router.route_message(1, "hello", send_reply)
        mock_router.assert_called_once_with(1, "hello", send_reply, None, None)


async def test_route_idle_starts_task():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="idle")])
    router._channel_project[1] = "myproj"

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    send_log = AsyncMock()
    await router.route_message(1, "fix the bug", send_reply, send_log)

    mock_http.post.assert_called_once()
    call_args = mock_http.post.call_args
    assert "/task" in call_args[0][0]
    payload = call_args[1]["json"]
    assert payload["project_id"] == "myproj"
    assert payload["task"] == "fix the bug"


async def test_route_idle_starts_task_with_model_overrides():
    router = _make_router([
        RemoteOrchestrator(
            project_id="myproj",
            name="srv",
            status="idle",
            model="claude-opus-4-6",
            session_model="claude-sonnet-4-6",
            permission_mode="default",
        )
    ])
    router._channel_project[1] = "myproj"

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    await router.route_message(1, "fix the bug", send_reply)

    mock_http.post.assert_called_once()
    payload = mock_http.post.call_args[1]["json"]
    assert payload["project_id"] == "myproj"
    assert payload["task"] == "fix the bug"
    assert payload["model"] == "claude-opus-4-6"
    assert payload["session_model"] == "claude-sonnet-4-6"
    assert payload["permission_mode"] == "default"


async def test_route_running_non_mention_is_deferred():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="running")])
    router._channel_project[1] = "myproj"

    mock_http = MagicMock()
    mock_http.post = MagicMock()
    router._http = mock_http

    send_reply = AsyncMock()
    await router.route_message(1, "also check tests", send_reply)

    # No interrupt request for non-@orchestrator message
    mock_http.post.assert_not_called()
    assert len(router._deferred_tasks["myproj"]) == 1
    send_reply.assert_called_once()
    assert "queued as next task" in send_reply.call_args[0][0].lower()


async def test_route_running_orchestrator_mention_interrupts():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="running")])
    router._channel_project[1] = "myproj"

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    await router.route_message(1, "@orchestrator also check tests", send_reply)

    mock_http.post.assert_called_once()
    call_args = mock_http.post.call_args
    assert "/interrupt" in call_args[0][0]
    payload = call_args[1]["json"]
    assert payload["message"] == "also check tests"
    assert payload["urgency"] == "urgent"


async def test_route_stuck_sends_interrupt():
    """When orchestrator is stuck (ask_user), user messages route as interrupts."""
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="stuck")])
    router._channel_project[1] = "myproj"

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True, "queued": True})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    await router.route_message(1, "use the staging API key", send_reply)

    mock_http.post.assert_called_once()
    call_args = mock_http.post.call_args
    assert "/interrupt" in call_args[0][0]
    payload = call_args[1]["json"]
    assert payload["message"] == "use the staging API key"
    assert payload["urgency"] == "normal"


async def test_route_stop_command():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="running")])
    router._channel_project[1] = "myproj"

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True, "status": "stopping"})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    await router.route_message(1, "/stop", send_reply)

    mock_http.post.assert_called_once()
    assert "/stop" in mock_http.post.call_args[0][0]
    send_reply.assert_called_once()
    assert "stopping" in send_reply.call_args[0][0].lower()


async def test_route_status_command():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="running")])
    router._channel_project[1] = "myproj"
    router._deferred_tasks["myproj"] = [{"chat_id": 1, "text": "a", "ts": 1.0}]

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(
        return_value={
            "status": "running",
            "iteration": 3,
            "max_iterations": 20,
            "summary": "",
        }
    )
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.get = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    await router.route_message(1, "/status", send_reply)

    send_reply.assert_called_once()
    reply = send_reply.call_args[0][0]
    assert "running" in reply.lower()
    assert "3/20" in reply
    assert "queued tasks" in reply.lower()


async def test_route_queue_requires_connected_project():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="running")])
    send_reply = AsyncMock()

    await router.route_message(1, "/queue", send_reply)

    send_reply.assert_called_once()
    assert "no project connected" in send_reply.call_args[0][0].lower()


async def test_route_queue_list_shows_entries():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="running")])
    router._channel_project[1] = "myproj"
    router._deferred_tasks["myproj"] = [
        {"chat_id": 1, "text": "run ablation A", "ts": 1.0},
        {"chat_id": 2, "text": "run ablation B", "ts": 2.0, "retries": 1},
    ]
    send_reply = AsyncMock()

    await router.route_message(1, "/queue", send_reply)

    send_reply.assert_called_once()
    reply = send_reply.call_args[0][0]
    assert "queue for `myproj` (2)" in reply.lower()
    assert "1. run ablation A".lower() in reply.lower()
    assert "2. run ablation B".lower() in reply.lower()
    assert "retry:1" in reply.lower()


async def test_route_queue_edit_updates_entry():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="running")])
    router._channel_project[1] = "myproj"
    router._deferred_tasks["myproj"] = [
        {"chat_id": 1, "text": "old text", "ts": 1.0},
    ]
    send_reply = AsyncMock()

    await router.route_message(1, "/queue edit 1 new text", send_reply)

    assert router._deferred_tasks["myproj"][0]["text"] == "new text"
    send_reply.assert_called_once()
    assert "updated queued task #1" in send_reply.call_args[0][0].lower()


async def test_route_queue_delete_removes_entry():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="running")])
    router._channel_project[1] = "myproj"
    router._deferred_tasks["myproj"] = [
        {"chat_id": 1, "text": "first", "ts": 1.0},
        {"chat_id": 1, "text": "second", "ts": 2.0},
    ]
    send_reply = AsyncMock()

    await router.route_message(1, "/queue delete 1", send_reply)

    assert [t["text"] for t in router._deferred_tasks["myproj"]] == ["second"]
    send_reply.assert_called_once()
    assert "removed queued task #1" in send_reply.call_args[0][0].lower()


async def test_route_queue_move_reorders_entries():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="running")])
    router._channel_project[1] = "myproj"
    router._deferred_tasks["myproj"] = [
        {"chat_id": 1, "text": "first", "ts": 1.0},
        {"chat_id": 1, "text": "second", "ts": 2.0},
        {"chat_id": 1, "text": "third", "ts": 3.0},
    ]
    send_reply = AsyncMock()

    await router.route_message(1, "/queue move 3 1", send_reply)

    assert [t["text"] for t in router._deferred_tasks["myproj"]] == ["third", "first", "second"]
    send_reply.assert_called_once()
    assert "moved queued task #3 -> #1" in send_reply.call_args[0][0].lower()


async def test_route_queue_clear_empties_queue():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="running")])
    router._channel_project[1] = "myproj"
    router._deferred_tasks["myproj"] = [
        {"chat_id": 1, "text": "first", "ts": 1.0},
        {"chat_id": 1, "text": "second", "ts": 2.0},
    ]
    send_reply = AsyncMock()

    await router.route_message(1, "/queue clear", send_reply)

    assert router._deferred_tasks["myproj"] == []
    send_reply.assert_called_once()
    assert "cleared 2 queued task(s)" in send_reply.call_args[0][0].lower()


async def test_at_orchestrator_status_command_is_normalized():
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="running")])
    router._channel_project[1] = "myproj"

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(
        return_value={
            "status": "running",
            "iteration": 1,
            "max_iterations": 20,
            "summary": "",
        }
    )
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.get = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    await router.route_message(1, "@orchestrator /status", send_reply)

    mock_http.get.assert_called_once()
    send_reply.assert_called_once()


async def test_connect_channel():
    router = _make_router([RemoteOrchestrator(project_id="proj-a", name="server-a")])
    ok = await router.connect_channel(1, "proj-a")
    assert ok
    assert router.get_channel_project(1) == "proj-a"


async def test_disconnect_channel():
    router = _make_router([RemoteOrchestrator(project_id="proj-a", name="server-a")])
    await router.connect_channel(1, "proj-a")
    await router.disconnect_channel(1)
    assert router.get_channel_project(1) is None


async def test_connect_unknown_project():
    router = _make_router([])
    ok = await router.connect_channel(1, "nonexistent")
    assert not ok


async def test_get_project_status():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="running")])
    assert router.get_project_status("proj") == "running"
    assert router.get_project_status("unknown") == "unknown"


async def test_route_to_disconnected_daemon():
    """Message to unreachable daemon fails with helpful error."""
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="disconnected")])
    router._channel_project[1] = "proj"

    # Mock HTTP to simulate unreachable daemon
    mock_resp = AsyncMock()
    mock_resp.status = 502
    mock_resp.text = AsyncMock(return_value="bad gateway")
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    await router.route_message(1, "do something", send_reply)
    assert "cannot reach" in send_reply.call_args[0][0].lower()


async def test_check_health_requires_orchestrator_daemon_signature():
    """Health check should require status=ok plus non-empty daemon field."""
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="idle")])

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value={"status": "ok", "daemon": "della-gpu"})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.get = MagicMock(return_value=mock_resp)
    router._http = mock_http

    ok = await router.check_health("proj")
    assert ok is True


async def test_check_health_rejects_legacy_payload_without_daemon():
    """Legacy/foreign payloads should not be treated as healthy daemon."""
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="idle")])

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value={"status": "ok", "server": "legacy"})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.get = MagicMock(return_value=mock_resp)
    router._http = mock_http

    ok = await router.check_health("proj")
    assert ok is False


async def test_ingest_progress_event_dedupes_event_id():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="idle")])

    seen = []

    async def on_progress(project_id, event):
        seen.append((project_id, event.get("event_id")))

    router.add_progress_listener(on_progress)

    event = {"event_id": "evt-1", "type": "iteration", "data": "Iteration 1/20", "iteration": 1}
    accepted_1 = await router.ingest_progress_event("proj", event, source="sse")
    accepted_2 = await router.ingest_progress_event("proj", event, source="callback")

    assert accepted_1 is True
    assert accepted_2 is False
    assert len(seen) == 1


async def test_ingest_progress_event_fallback_signature_dedupe():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="idle")])

    seen = []

    async def on_progress(project_id, event):
        seen.append(project_id)

    router.add_progress_listener(on_progress)

    event = {"type": "done", "data": "ok", "iteration": 2, "ts": 123.0}
    accepted_1 = await router.ingest_progress_event("proj", dict(event), source="sse")
    accepted_2 = await router.ingest_progress_event("proj", dict(event), source="callback")

    assert accepted_1 is True
    assert accepted_2 is False
    assert len(seen) == 1


async def test_terminal_event_starts_deferred_task():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="running")])
    router._deferred_tasks["proj"] = [{"chat_id": 1, "text": "next task", "ts": 1.0}]

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    await router.ingest_progress_event(
        "proj",
        {"event_id": "evt-done", "type": "done", "data": "finished", "iteration": 3},
        source="sse",
    )

    # done event should trigger deferred task startup
    assert mock_http.post.called
    call_url = mock_http.post.call_args[0][0]
    assert "/task" in call_url
    payload = mock_http.post.call_args[1]["json"]
    assert payload["task"] == "next task"


# ── /connect command via route_message ────────────────────────────────


async def test_connect_command_via_route_message():
    """/connect proj-a via route_message links channel and replies."""
    router = _make_router([RemoteOrchestrator(project_id="proj-a", name="server-a")])

    send_reply = AsyncMock()
    await router.route_message(1, "/connect proj-a", send_reply)

    assert router.get_channel_project(1) == "proj-a"
    send_reply.assert_called_once()
    assert "proj-a" in send_reply.call_args[0][0]


async def test_connect_command_no_arg_shows_current():
    """/connect with no arg shows current connection."""
    router = _make_router([RemoteOrchestrator(project_id="proj-a", name="server-a")])
    router._channel_project[1] = "proj-a"

    send_reply = AsyncMock()
    await router.route_message(1, "/connect", send_reply)

    send_reply.assert_called_once()
    assert "proj-a" in send_reply.call_args[0][0]


async def test_connect_command_no_arg_no_project():
    """/connect with no arg and no connection shows available."""
    router = _make_router([RemoteOrchestrator(project_id="proj-a", name="server-a")])

    send_reply = AsyncMock()
    await router.route_message(1, "/connect", send_reply)

    send_reply.assert_called_once()
    reply = send_reply.call_args[0][0]
    assert "not connected" in reply.lower()
    assert "proj-a" in reply


async def test_connect_command_unknown_project_via_route():
    """/connect unknown-proj shows error with available list."""
    router = _make_router([RemoteOrchestrator(project_id="proj-a", name="server-a")])

    send_reply = AsyncMock()
    await router.route_message(1, "/connect unknown-proj", send_reply)

    assert router.get_channel_project(1) is None
    send_reply.assert_called_once()
    assert "unknown" in send_reply.call_args[0][0].lower()
    assert "proj-a" in send_reply.call_args[0][0]


async def test_connect_command_reloads_config_before_lookup(tmp_path):
    """/connect should pick up projects newly written to config.json."""
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "servers": [
                    {
                        "name": "proj-new",
                        "host": "user@host",
                        "work_dir": "/tmp/proj-new",
                        "broker_port": 8201,
                    }
                ]
            }
        )
    )
    router = Router(cwd=str(tmp_path))

    send_reply = AsyncMock()
    await router.route_message(1, "/connect proj-new", send_reply)

    assert router.get_channel_project(1) == "proj-new"
    send_reply.assert_called_once()
    assert "connected" in send_reply.call_args[0][0].lower()
    assert "shared across channels" in send_reply.call_args[0][0].lower()


async def test_connect_unknown_project_while_router_task_running(tmp_path):
    """/connect unknown while setup is running should explain likely sync timing."""
    (tmp_path / "config.json").write_text(json.dumps({"servers": []}))
    router = Router(cwd=str(tmp_path))
    send_reply = AsyncMock()

    pending = asyncio.create_task(asyncio.sleep(10))
    router._router_tasks[1] = pending
    try:
        await router.route_message(1, "/connect not-yet", send_reply)
    finally:
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending

    send_reply.assert_called_once()
    reply = send_reply.call_args[0][0].lower()
    assert "still running" in reply
    assert "retry" in reply


async def test_connect_command_fails_fast_when_daemon_unreachable():
    """/connect should fail immediately if daemon health check fails."""
    router = _make_router([RemoteOrchestrator(project_id="proj-a", name="server-a")])
    router._http = object()
    router.check_health = AsyncMock(return_value=False)
    router._ensure_registered = AsyncMock(return_value=True)

    send_reply = AsyncMock()
    await router.route_message(1, "/connect proj-a", send_reply)

    assert router.get_channel_project(1) is None
    send_reply.assert_called_once()
    reply = send_reply.call_args[0][0].lower()
    assert "cannot connect" in reply
    assert "unreachable" in reply
    router._ensure_registered.assert_not_called()


async def test_connect_command_fails_when_registration_fails():
    """/connect should fail if daemon is reachable but register fails."""
    router = _make_router([RemoteOrchestrator(project_id="proj-a", name="server-a")])
    router._http = object()
    router.check_health = AsyncMock(return_value=True)
    router._ensure_registered = AsyncMock(return_value=False)

    send_reply = AsyncMock()
    await router.route_message(1, "/connect proj-a", send_reply)

    assert router.get_channel_project(1) is None
    send_reply.assert_called_once()
    reply = send_reply.call_args[0][0].lower()
    assert "cannot connect" in reply
    assert "registration failed" in reply
    router._ensure_registered.assert_awaited_once()


async def test_at_orchestrator_connect_command():
    """@orchestrator /connect proj-a normalizes to /connect."""
    router = _make_router([RemoteOrchestrator(project_id="proj-a", name="server-a")])

    send_reply = AsyncMock()
    await router.route_message(1, "@orchestrator /connect proj-a", send_reply)

    assert router.get_channel_project(1) == "proj-a"


# ── @orchestrator on idle starts task (not interrupt) ─────────────────


async def test_at_orchestrator_on_idle_starts_task():
    """@orchestrator message when idle starts a task, not an interrupt."""
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="idle")])
    router._channel_project[1] = "myproj"

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    send_log = AsyncMock()
    await router.route_message(1, "@orchestrator fix the tests", send_reply, send_log)

    mock_http.post.assert_called_once()
    assert "/task" in mock_http.post.call_args[0][0]
    payload = mock_http.post.call_args[1]["json"]
    assert payload["task"] == "fix the tests"


async def test_empty_at_orchestrator_message():
    """@orchestrator with no body returns error."""
    router = _make_router([RemoteOrchestrator(project_id="myproj", name="srv", status="idle")])
    router._channel_project[1] = "myproj"

    send_reply = AsyncMock()
    await router.route_message(1, "@orchestrator", send_reply)

    send_reply.assert_called_once()
    assert "empty" in send_reply.call_args[0][0].lower()


# ── Setup mode routing ────────────────────────────────────────────────


async def test_setup_command_routes_to_router():
    """/setup routes to _handle_router_message."""
    router = _make_router([])

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch.object(router, "_handle_router_message", new_callable=AsyncMock) as mock_setup:
        await router.route_message(1, "/setup user@server", send_reply, send_log)
        mock_setup.assert_called_once_with(1, "/setup user@server", send_reply, send_log, None)


async def test_setup_project_command_routes_to_router():
    """/setup-project routes to _handle_router_message."""
    router = _make_router([])

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch.object(router, "_handle_router_message", new_callable=AsyncMock) as mock_setup:
        await router.route_message(1, "/setup-project /home/ubuntu/proj-a", send_reply, send_log)
        mock_setup.assert_called_once_with(
            1,
            "/setup-project /home/ubuntu/proj-a",
            send_reply,
            send_log,
            None,
        )


async def test_doctor_command_routes_to_router():
    """/doctor routes to _handle_router_message."""
    router = _make_router([])

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch.object(router, "_handle_router_message", new_callable=AsyncMock) as mock_router:
        await router.route_message(1, "/doctor ftgs", send_reply, send_log)
        mock_router.assert_called_once_with(1, "/doctor ftgs", send_reply, send_log, None)


async def test_upgrade_check_command_routes_to_router():
    """/upgrade-check routes to _handle_router_message."""
    router = _make_router([])

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch.object(router, "_handle_router_message", new_callable=AsyncMock) as mock_router:
        await router.route_message(1, "/upgrade-check ftgs", send_reply, send_log)
        mock_router.assert_called_once_with(1, "/upgrade-check ftgs", send_reply, send_log, None)


async def test_orchestrator_plugin_command_routes_to_router():
    """/orchestrator_plugin routes to _handle_router_message."""
    router = _make_router([])

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch.object(router, "_handle_router_message", new_callable=AsyncMock) as mock_router:
        await router.route_message(1, "/orchestrator_plugin add filesystem mcp", send_reply, send_log)
        mock_router.assert_called_once_with(
            1,
            "/orchestrator_plugin add filesystem mcp",
            send_reply,
            send_log,
            None,
        )


async def test_worker_plugin_command_routes_to_router():
    """/worker_plugin routes to _handle_router_message."""
    router = _make_router([])

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch.object(router, "_handle_router_message", new_callable=AsyncMock) as mock_router:
        await router.route_message(1, "/worker_plugin add memory mcp", send_reply, send_log)
        mock_router.assert_called_once_with(
            1,
            "/worker_plugin add memory mcp",
            send_reply,
            send_log,
            None,
        )


async def test_setup_project_prompt_enforces_workdir_and_health_gates(tmp_path):
    """Injected /setup-project template requires work_dir + writability + health checks."""
    (tmp_path / "config.json").write_text('{"servers": []}')
    router = Router(cwd=str(tmp_path))

    captured_prompt = {"text": ""}

    class FakeRouterSession:
        def __init__(self, cwd="."):
            self.cwd = cwd

        def set_callbacks(self, progress=None, log=None):
            return None

        async def run(self, prompt: str) -> str:
            captured_prompt["text"] = prompt
            return "ok"

        def should_emit_final_result(self, result_text: str) -> bool:
            return True

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch("src.router.RouterSession", FakeRouterSession):
        await router._handle_router_message(
            1,
            "/setup-project /blue/yanjun.li/xd7812.princeton/anti-finetuning",
            send_reply,
            send_log,
            None,
        )
        task = router._router_tasks[1]
        await task

    prompt = captured_prompt["text"]
    assert "Resolve one concrete absolute work_dir" in prompt
    assert "If no concrete work_dir is derivable" in prompt
    assert "Ensure work_dir exists and is writable" in prompt
    assert "Ensure work_dir/CLAUDE.md exists" in prompt
    assert "NEVER overwrite whole file" in prompt
    assert "Verify daemon health" in prompt
    assert "Response format" in prompt
    assert "READY: project_id, work_dir, host, broker_port" in prompt


async def test_doctor_prompt_includes_channel_context(tmp_path):
    (tmp_path / "config.json").write_text('{"servers": []}')
    router = Router(cwd=str(tmp_path))
    router._orchestrators["ftgs"] = RemoteOrchestrator(
        project_id="ftgs",
        name="della-gpu",
        host="user@host",
        broker_port=8203,
        project_dir="/scratch/ftgs",
        status="disconnected",
    )
    router._channel_project[1] = "ftgs"
    router._record_channel_context(1, "daemon connect failed on ftgs")
    router._record_channel_context(1, "please inspect tunnel and remote process")

    captured_prompt = {"text": ""}

    class FakeRouterSession:
        def __init__(self, cwd="."):
            self.cwd = cwd

        def set_callbacks(self, progress=None, log=None):
            return None

        async def run(self, prompt: str) -> str:
            captured_prompt["text"] = prompt
            return "ok"

        def should_emit_final_result(self, result_text: str) -> bool:
            return True

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch("src.router.RouterSession", FakeRouterSession):
        await router._handle_router_message(
            1,
            "/doctor verify communication path",
            send_reply,
            send_log,
            None,
        )
        task = router._router_tasks[1]
        await task

    prompt = captured_prompt["text"]
    assert "DOCTOR MODE (/doctor)" in prompt
    assert "connected_project: ftgs" in prompt
    assert "user_hint: verify communication path" in prompt
    assert "daemon connect failed on ftgs" in prompt
    assert "tools/doctor.py --project <project_id>" in prompt
    assert "ROOT CAUSE" in prompt


async def test_upgrade_check_prompt_requires_confirmation(tmp_path):
    (tmp_path / "config.json").write_text('{"servers": []}')
    router = Router(cwd=str(tmp_path))
    router._orchestrators["ftgs"] = RemoteOrchestrator(
        project_id="ftgs",
        name="della-gpu",
        host="user@host",
        broker_port=8203,
        project_dir="/scratch/ftgs",
        status="idle",
    )
    router._channel_project[1] = "ftgs"
    router._record_channel_context(1, "please verify remote daemon version against github main")

    captured_prompt = {"text": ""}

    class FakeRouterSession:
        def __init__(self, cwd="."):
            self.cwd = cwd

        def set_callbacks(self, progress=None, log=None):
            return None

        async def run(self, prompt: str) -> str:
            captured_prompt["text"] = prompt
            return "ok"

        def should_emit_final_result(self, result_text: str) -> bool:
            return True

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch("src.router.RouterSession", FakeRouterSession):
        await router._handle_router_message(
            1,
            "/upgrade-check ftgs",
            send_reply,
            send_log,
            None,
        )
        task = router._router_tasks[1]
        await task

    prompt = captured_prompt["text"]
    assert "UPGRADE CHECK MODE (/upgrade-check)" in prompt
    assert "connected_project: ftgs" in prompt
    assert "raw.githubusercontent.com" in prompt
    assert "shasum -a 256 tools/orchestrator_daemon.py" in prompt
    assert "Do NOT upgrade automatically" in prompt
    assert "Proceed with upgrade now? (yes/no)" in prompt


async def test_orchestrator_plugin_prompt_includes_role_and_target_file(tmp_path):
    (tmp_path / "config.json").write_text('{"servers": []}')
    router = Router(cwd=str(tmp_path))
    router._orchestrators["ftgs"] = RemoteOrchestrator(
        project_id="ftgs",
        name="della-gpu",
        host="user@host",
        broker_port=8203,
        project_dir="/scratch/ftgs",
        status="idle",
    )
    router._channel_project[1] = "ftgs"
    captured_prompt = {"text": ""}

    class FakeRouterSession:
        def __init__(self, cwd="."):
            self.cwd = cwd

        def set_callbacks(self, progress=None, log=None):
            return None

        async def run(self, prompt: str) -> str:
            captured_prompt["text"] = prompt
            return "ok"

        def should_emit_final_result(self, result_text: str) -> bool:
            return True

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch("src.router.RouterSession", FakeRouterSession):
        await router._handle_router_message(
            1,
            "/orchestrator_plugin add filesystem and memory servers",
            send_reply,
            send_log,
            None,
        )
        task = router._router_tasks[1]
        await task

    prompt = captured_prompt["text"]
    assert "MCP PLUGIN SETUP MODE (/orchestrator_plugin)" in prompt
    assert "target_role: orchestrator" in prompt
    assert "target_plugin_file: .claude/mcp/orchestrator.json" in prompt
    assert "connected_project: ftgs" in prompt
    assert "Canonical schema" in prompt
    assert "activation behavior" in prompt.lower()


async def test_setup_server_prompt_is_machine_only(tmp_path):
    (tmp_path / "config.json").write_text('{"servers": []}')
    router = Router(cwd=str(tmp_path))

    captured_prompt = {"text": ""}

    class FakeRouterSession:
        def __init__(self, cwd="."):
            self.cwd = cwd

        def set_callbacks(self, progress=None, log=None):
            return None

        async def run(self, prompt: str) -> str:
            captured_prompt["text"] = prompt
            return "ok"

        def should_emit_final_result(self, result_text: str) -> bool:
            return True

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch("src.router.RouterSession", FakeRouterSession):
        await router._handle_router_message(
            1,
            "/setup user@server",
            send_reply,
            send_log,
            None,
        )
        task = router._router_tasks[1]
        await task

    prompt = captured_prompt["text"]
    assert "machine connectivity only" in prompt.lower()
    assert "do not create or modify project/work_dir entries" in prompt.lower()
    assert "do not create or edit work_dir/claude.md" in prompt.lower()


async def test_setup_machine_scope_guard_reverts_project_changes(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "machines": [{"name": "m1", "host": "user@h1", "broker_port": 8201}],
                "projects": [{"project_id": "p1", "machine": "m1", "work_dir": "/work/p1"}],
            }
        )
    )
    router = Router(cwd=str(tmp_path))

    class FakeRouterSession:
        def __init__(self, cwd="."):
            self.cwd = cwd

        def set_callbacks(self, progress=None, log=None):
            return None

        async def run(self, prompt: str) -> str:
            cfg = json.loads(config_path.read_text())
            cfg["projects"] = [{"project_id": "bad", "machine": "m1", "work_dir": "/tmp/bad"}]
            config_path.write_text(json.dumps(cfg))
            return "setup complete"

        def should_emit_final_result(self, result_text: str) -> bool:
            return True

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch("src.router.RouterSession", FakeRouterSession):
        await router._handle_router_message(
            1,
            "/setup user@server",
            send_reply,
            send_log,
            None,
        )
        await router._router_tasks[1]

    restored = json.loads(config_path.read_text())
    assert restored["projects"] == [{"project_id": "p1", "machine": "m1", "work_dir": "/work/p1"}]
    msg = send_reply.call_args[0][0].lower()
    assert "scope guard blocked" in msg
    assert "/setup-project" in msg


async def test_setup_project_scope_guard_reverts_machine_changes(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "machines": [{"name": "m1", "host": "user@h1", "broker_port": 8201}],
                "projects": [{"project_id": "p1", "machine": "m1", "work_dir": "/work/p1"}],
            }
        )
    )
    router = Router(cwd=str(tmp_path))

    class FakeRouterSession:
        def __init__(self, cwd="."):
            self.cwd = cwd

        def set_callbacks(self, progress=None, log=None):
            return None

        async def run(self, prompt: str) -> str:
            cfg = json.loads(config_path.read_text())
            cfg["machines"] = [{"name": "m2", "host": "user@h2", "broker_port": 8301}]
            config_path.write_text(json.dumps(cfg))
            return "project setup complete"

        def should_emit_final_result(self, result_text: str) -> bool:
            return True

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch("src.router.RouterSession", FakeRouterSession):
        await router._handle_router_message(
            1,
            "/setup-project /work/p1",
            send_reply,
            send_log,
            None,
        )
        await router._router_tasks[1]

    restored = json.loads(config_path.read_text())
    assert restored["machines"] == [{"name": "m1", "host": "user@h1", "broker_port": 8201}]
    msg = send_reply.call_args[0][0].lower()
    assert "scope guard blocked" in msg
    assert "/setup-project" in msg


async def test_setup_machine_scope_guard_blocks_project_fields_when_config_missing(tmp_path):
    config_path = tmp_path / "config.json"
    router = Router(cwd=str(tmp_path))

    class FakeRouterSession:
        def __init__(self, cwd="."):
            self.cwd = cwd

        def set_callbacks(self, progress=None, log=None):
            return None

        async def run(self, prompt: str) -> str:
            config_path.write_text(
                json.dumps(
                    {
                        "machines": [{"name": "m1", "host": "user@h1", "broker_port": 8201}],
                        "projects": [{"project_id": "bad", "machine": "m1", "work_dir": "/tmp/bad"}],
                    }
                )
            )
            return "setup complete"

        def should_emit_final_result(self, result_text: str) -> bool:
            return True

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch("src.router.RouterSession", FakeRouterSession):
        await router._handle_router_message(
            1,
            "/setup user@server",
            send_reply,
            send_log,
            None,
        )
        await router._router_tasks[1]

    assert not config_path.exists()
    msg = send_reply.call_args[0][0].lower()
    assert "scope guard blocked" in msg


def test_parse_setup_command_health_default():
    router = Router()
    parsed = router._parse_setup_command("/setup")
    assert parsed["mode"] == "health"


def test_parse_setup_command_auto_tunnel_default():
    router = Router()
    parsed = router._parse_setup_command("/setup user@server")
    assert parsed["mode"] == "setup"
    assert parsed["host"] == "user@server"
    assert parsed["auto_tunnel"] is True


def test_parse_setup_command_manual_tunnel():
    router = Router()
    parsed = router._parse_setup_command("/setup user@server --manual-tunnel")
    assert parsed["mode"] == "setup"
    assert parsed["host"] == "user@server"
    assert parsed["auto_tunnel"] is False


def test_parse_setup_command_invalid_flag():
    router = Router()
    parsed = router._parse_setup_command("/setup user@server --oops")
    assert parsed["mode"] == "error"
    assert "unknown /setup flag" in str(parsed["error"]).lower()


def test_parse_setup_command_health_conflict():
    router = Router()
    parsed = router._parse_setup_command("/setup user@server --health")
    assert parsed["mode"] == "error"
    assert "cannot be combined" in str(parsed["error"]).lower()


def test_parse_setup_project_command_basic():
    router = Router()
    parsed = router._parse_setup_project_command("/setup-project /home/ubuntu/proj-a")
    assert parsed["mode"] == "setup_project"
    assert parsed["instruction"] == "/home/ubuntu/proj-a"


def test_parse_setup_project_command_missing_instruction():
    router = Router()
    parsed = router._parse_setup_project_command("/setup-project")
    assert parsed["mode"] == "error"
    assert "missing instruction" in str(parsed["error"]).lower()


def test_parse_plugin_setup_command_basic():
    router = Router()
    parsed = router._parse_plugin_setup_command("/worker_plugin add memory")
    assert parsed["mode"] == "plugin_setup"
    assert parsed["role"] == "worker"
    assert parsed["instruction"] == "add memory"


def test_parse_plugin_setup_command_dash_alias():
    router = Router()
    parsed = router._parse_plugin_setup_command("/orchestrator-plugin add filesystem")
    assert parsed["mode"] == "plugin_setup"
    assert parsed["role"] == "orchestrator"
    assert parsed["instruction"] == "add filesystem"


def test_parse_plugin_setup_command_missing_instruction():
    router = Router()
    parsed = router._parse_plugin_setup_command("/worker_plugin")
    assert parsed["mode"] == "error"
    assert "missing instruction" in str(parsed["error"]).lower()


async def test_connect_works_from_any_state():
    """/connect works regardless of prior channel state."""
    router = _make_router([
        RemoteOrchestrator(project_id="proj", name="srv"),
    ])

    send_reply = AsyncMock()
    await router.route_message(1, "/connect proj", send_reply)

    assert router.get_channel_project(1) == "proj"


# ── Config loading ────────────────────────────────────────────────────


async def test_load_config_servers_schema(tmp_path):
    """_load_config reads legacy 'servers' schema."""
    config = {
        "servers": [
            {"name": "srv-a", "work_dir": "/tmp", "broker_port": 8201},
        ]
    }
    (tmp_path / "config.json").write_text(json.dumps(config))

    router = Router(cwd=str(tmp_path))
    router._load_config()

    assert "srv-a" in router._orchestrators
    orch = router._orchestrators["srv-a"]
    assert orch.project_dir == "/tmp"
    assert orch.broker_port == 8201


async def test_load_config_servers_schema_with_orchestrator_defaults(tmp_path):
    config = {
        "orchestrator": {
            "model": "claude-opus-4-6",
            "session_model": "claude-sonnet-4-6",
            "permission_mode": "acceptEdits",
        },
        "servers": [
            {"name": "srv-a", "work_dir": "/tmp/a"},
            {"name": "srv-b", "work_dir": "/tmp/b", "model": "claude-haiku-4-5"},
        ],
    }
    (tmp_path / "config.json").write_text(json.dumps(config))

    router = Router(cwd=str(tmp_path))
    router._load_config()

    orch_a = router._orchestrators["srv-a"]
    assert orch_a.model == "claude-opus-4-6"
    assert orch_a.session_model == "claude-sonnet-4-6"
    assert orch_a.permission_mode == "acceptEdits"

    orch_b = router._orchestrators["srv-b"]
    assert orch_b.model == "claude-haiku-4-5"
    assert orch_b.session_model == "claude-sonnet-4-6"
    assert orch_b.permission_mode == "acceptEdits"


async def test_load_config_orchestrators_schema(tmp_path):
    """_load_config reads new 'orchestrators' schema."""
    config = {
        "orchestrators": [
            {"project_id": "my-proj", "name": "my-name", "project_dir": "/work"},
        ]
    }
    (tmp_path / "config.json").write_text(json.dumps(config))

    router = Router(cwd=str(tmp_path))
    router._load_config()

    assert "my-proj" in router._orchestrators
    orch = router._orchestrators["my-proj"]
    assert orch.name == "my-name"
    assert orch.project_dir == "/work"


async def test_load_config_orchestrators_takes_precedence(tmp_path):
    """When both schemas present, 'orchestrators' wins."""
    config = {
        "orchestrators": [
            {"project_id": "new", "name": "new-srv"},
        ],
        "servers": [
            {"name": "old-srv", "work_dir": "/tmp"},
        ],
    }
    (tmp_path / "config.json").write_text(json.dumps(config))

    router = Router(cwd=str(tmp_path))
    router._load_config()

    assert "new" in router._orchestrators
    assert "old-srv" not in router._orchestrators


async def test_load_config_projects_schema_resolves_machine_and_server(tmp_path):
    """_load_config reads split machines/projects schema and backfills servers."""
    config = {
        "orchestrator": {
            "model": "claude-opus-4-6",
            "session_model": "claude-sonnet-4-6",
            "permission_mode": "acceptEdits",
        },
        "machines": [
            {"name": "della-gpu", "host": "user@della", "broker_port": 8203},
        ],
        "servers": [
            {"name": "local", "host": None, "work_dir": "/tmp/local", "broker_port": 8200},
        ],
        "projects": [
            {"project_id": "ftgs", "machine": "della-gpu", "work_dir": "/scratch/ftgs"},
            {"project_id": "local-proj", "server": "local", "work_dir": "/tmp/local-proj"},
        ],
    }
    (tmp_path / "config.json").write_text(json.dumps(config))

    router = Router(cwd=str(tmp_path))
    router._load_config()

    ftgs = router._orchestrators["ftgs"]
    assert ftgs.host == "user@della"
    assert ftgs.broker_port == 8203
    assert ftgs.project_dir == "/scratch/ftgs"
    assert ftgs.model == "claude-opus-4-6"
    assert ftgs.session_model == "claude-sonnet-4-6"
    assert ftgs.permission_mode == "acceptEdits"

    local_proj = router._orchestrators["local-proj"]
    assert local_proj.host is None
    assert local_proj.broker_port == 8200
    assert local_proj.project_dir == "/tmp/local-proj"

    # Server remains directly connectable unless shadowed by a project_id.
    assert "local" in router._orchestrators


async def test_load_config_missing_file(tmp_path):
    """Missing config.json is handled gracefully."""
    router = Router(cwd=str(tmp_path))
    router._load_config()
    assert len(router._orchestrators) == 0


# ── reload_config ─────────────────────────────────────────────────────


async def test_reload_config_adds_new_projects(tmp_path):
    config = {"servers": [{"name": "new-srv", "work_dir": "/tmp"}]}
    (tmp_path / "config.json").write_text(json.dumps(config))

    router = Router(cwd=str(tmp_path))
    assert len(router._orchestrators) == 0

    with patch.object(router, "_register_project", new_callable=AsyncMock):
        router.reload_config()

    assert "new-srv" in router._orchestrators


async def test_reload_config_removes_old_projects(tmp_path):
    config = {"servers": [{"name": "old-srv", "work_dir": "/tmp"}]}
    (tmp_path / "config.json").write_text(json.dumps(config))
    router = Router(cwd=str(tmp_path))
    router._load_config()
    assert "old-srv" in router._orchestrators

    mock_task = MagicMock()
    router._sse_tasks["old-srv"] = mock_task

    (tmp_path / "config.json").write_text('{"servers": []}')
    router.reload_config()

    assert "old-srv" not in router._orchestrators
    mock_task.cancel.assert_called_once()


# ── Channel/project mapping helpers ──────────────────────────────────


async def test_hydrate_channel_mapping():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv")])
    ok = router.hydrate_channel_mapping(1, "proj")
    assert ok
    assert router.get_channel_project(1) == "proj"


async def test_hydrate_channel_mapping_unknown_project():
    router = _make_router([])
    ok = router.hydrate_channel_mapping(1, "nonexistent")
    assert not ok
    assert router.get_channel_project(1) is None


async def test_get_channels_for_project():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv")])
    router._channel_project[1] = "proj"
    router._channel_project[2] = "proj"
    router._channel_project[3] = "other"

    channels = router.get_channels_for_project("proj")
    assert set(channels) == {1, 2}


async def test_get_channels_for_project_empty():
    router = _make_router([])
    assert router.get_channels_for_project("proj") == []


# ── Ingestion updates orch.status ────────────────────────────────────


async def test_ingest_done_sets_status_done():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="running")])
    await router.ingest_progress_event("proj", {"event_id": "e1", "type": "done", "data": "ok"})
    assert router._orchestrators["proj"].status == "done"


async def test_ingest_stopped_sets_status_stopped():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="running")])
    await router.ingest_progress_event("proj", {"event_id": "e1", "type": "stopped", "data": "cancelled"})
    assert router._orchestrators["proj"].status == "stopped"


async def test_ingest_stuck_sets_status_stuck():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="running")])
    await router.ingest_progress_event("proj", {"event_id": "e1", "type": "stuck", "data": "help"})
    assert router._orchestrators["proj"].status == "stuck"


async def test_ingest_error_sets_status_error():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="running")])
    await router.ingest_progress_event("proj", {"event_id": "e1", "type": "error", "data": "bad"})
    assert router._orchestrators["proj"].status == "error"


async def test_ingest_iteration_sets_status_running():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="idle")])
    await router.ingest_progress_event("proj", {"event_id": "e1", "type": "iteration", "data": "1/20", "iteration": 1})
    assert router._orchestrators["proj"].status == "running"


async def test_sync_daemon_status_replays_missed_events():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="disconnected")])
    orch = router._orchestrators["proj"]
    router._last_event_id["proj"] = "e1"

    status_resp = AsyncMock()
    status_resp.status = 200
    status_resp.json = AsyncMock(return_value={"status": "running"})
    status_resp.__aenter__ = AsyncMock(return_value=status_resp)
    status_resp.__aexit__ = AsyncMock(return_value=None)

    events_resp = AsyncMock()
    events_resp.status = 200
    events_resp.json = AsyncMock(
        return_value={
            "events": [
                {
                    "event_id": "e2",
                    "type": "text",
                    "data": "[orchestrator] recovered progress",
                    "iteration": 2,
                    "ts": 100.0,
                }
            ],
            "truncated": False,
        }
    )
    events_resp.__aenter__ = AsyncMock(return_value=events_resp)
    events_resp.__aexit__ = AsyncMock(return_value=None)

    mock_http = MagicMock()
    mock_http.get = MagicMock(side_effect=[status_resp, events_resp])

    router.ingest_progress_event = AsyncMock(return_value=True)

    await router._sync_daemon_status(orch, mock_http)

    assert orch.status == "running"
    assert mock_http.get.call_count == 2
    router.ingest_progress_event.assert_awaited_once_with(
        "proj",
        {
            "event_id": "e2",
            "type": "text",
            "data": "[orchestrator] recovered progress",
            "iteration": 2,
            "ts": 100.0,
        },
        source="replay",
    )


async def test_sync_daemon_status_skips_replay_without_cursor():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="disconnected")])
    orch = router._orchestrators["proj"]

    status_resp = AsyncMock()
    status_resp.status = 200
    status_resp.json = AsyncMock(return_value={"status": "idle"})
    status_resp.__aenter__ = AsyncMock(return_value=status_resp)
    status_resp.__aexit__ = AsyncMock(return_value=None)

    mock_http = MagicMock()
    mock_http.get = MagicMock(return_value=status_resp)

    router.ingest_progress_event = AsyncMock(return_value=True)

    await router._sync_daemon_status(orch, mock_http)

    assert mock_http.get.call_count == 1
    router.ingest_progress_event.assert_not_awaited()


# ── deferred task triggers ────────────────────────────────────────────


async def test_stuck_event_does_not_start_deferred_task():
    """'stuck' means ask_user is pending — don't start deferred tasks."""
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="running")])
    router._deferred_tasks["proj"] = [{"chat_id": 1, "text": "queued task", "ts": 1.0}]

    mock_http = MagicMock()
    router._http = mock_http

    await router.ingest_progress_event(
        "proj",
        {"event_id": "e-stuck", "type": "stuck", "data": "need help"},
        source="sse",
    )

    # Should NOT have tried to start a deferred task
    assert not mock_http.post.called
    # Deferred queue should still have the task
    assert len(router._deferred_tasks["proj"]) == 1


async def test_error_event_starts_deferred_task():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="running")])
    router._deferred_tasks["proj"] = [{"chat_id": 1, "text": "retry task", "ts": 1.0}]

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    await router.ingest_progress_event(
        "proj",
        {"event_id": "e-err", "type": "error", "data": "crash"},
        source="callback",
    )

    assert mock_http.post.called
    assert mock_http.post.call_args[1]["json"]["task"] == "retry task"


async def test_deferred_task_start_failure_keeps_queue_head():
    """Failed starts should keep the task queued for retry."""
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="done")])
    router._deferred_tasks["proj"] = [{"chat_id": 1, "text": "queued task", "ts": 1.0}]

    mock_resp = AsyncMock()
    mock_resp.status = 503
    mock_resp.json = AsyncMock(return_value={"error": "daemon unavailable"})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    await router._maybe_start_deferred_task("proj")

    assert len(router._deferred_tasks["proj"]) == 1
    assert router._deferred_tasks["proj"][0]["text"] == "queued task"
    assert router._deferred_tasks["proj"][0]["retries"] == 1
    assert "proj" not in router._deferred_retry_tasks


async def test_deferred_task_retryable_start_failure_auto_retries():
    """409/'already running' should retry queued start with backoff."""
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="done")])
    router._deferred_tasks["proj"] = [{"chat_id": 1, "text": "queued task", "ts": 1.0}]

    router._start_task_request = AsyncMock(
        side_effect=[
            (False, "Project proj already has a running task. Use /interrupt or /stop first."),
            (True, ""),
        ]
    )
    router._deferred_retry_delay = lambda retries: 0.01

    progress_events = []

    async def on_progress(project_id, event):
        progress_events.append(event)

    router.add_progress_listener(on_progress)

    await router._maybe_start_deferred_task("proj")
    await asyncio.sleep(0.05)

    assert router._start_task_request.await_count >= 2
    assert len(router._deferred_tasks["proj"]) == 0
    assert any("Starting queued task:" in str(e.get("data", "")) for e in progress_events)
    assert not any("Failed to start queued task:" in str(e.get("data", "")) for e in progress_events)


# ── Mapping persistence callback ─────────────────────────────────────


async def test_connect_channel_calls_persist_callback():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv")])
    persisted = {}

    async def persist(chat_id, project_id):
        persisted[chat_id] = project_id

    router.add_mapping_persist_listener(persist)
    await router.connect_channel(1, "proj")

    assert persisted[1] == "proj"


async def test_disconnect_channel_calls_persist_with_none():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv")])
    router._channel_project[1] = "proj"
    persisted = {}

    async def persist(chat_id, project_id):
        persisted[chat_id] = project_id

    router.add_mapping_persist_listener(persist)
    await router.disconnect_channel(1)

    assert persisted[1] is None


# ── Deferred task notification ────────────────────────────────────────


async def test_deferred_task_notifies_via_progress_callback():
    """When a deferred task starts, a progress event is emitted."""
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="done")])
    router._channel_project[1] = "proj"
    router._deferred_tasks["proj"] = [{"chat_id": 1, "text": "run the tests", "ts": 1.0}]

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    progress_events = []

    async def on_progress(project_id, event):
        progress_events.append(event)

    router.add_progress_listener(on_progress)

    await router._maybe_start_deferred_task("proj")

    # Should have emitted a text event about starting the queued task
    assert any(
        "run the tests" in e.get("data", "") and "@orchestrator" in e.get("data", "")
        for e in progress_events
    )


# ── Per-channel router sessions ────────────────────────────────────────


async def test_router_sessions_are_per_channel():
    """Each channel gets its own router session."""
    router = _make_router([])

    with patch.object(router, "_handle_router_message", new_callable=AsyncMock) as mock_setup:
        send_reply_1 = AsyncMock()
        send_reply_2 = AsyncMock()
        await router.route_message(1, "/setup server-a", send_reply_1)
        await router.route_message(2, "/setup server-b", send_reply_2)

        assert mock_setup.call_count == 2
        # Each call has the correct chat_id
        assert mock_setup.call_args_list[0][0][0] == 1
        assert mock_setup.call_args_list[1][0][0] == 2


async def test_router_typing_token_emitted_for_start_and_stop(tmp_path):
    (tmp_path / "config.json").write_text('{"servers": []}')
    router = Router(cwd=str(tmp_path))

    class FakeRouterSession:
        def __init__(self, cwd="."):
            self.cwd = cwd

        def set_callbacks(self, progress=None, log=None):
            return None

        async def run(self, prompt: str) -> str:
            return "done"

        def should_emit_final_result(self, result_text: str) -> bool:
            return False

    typing_events = []

    async def send_typing(active: bool, sender: str = "router", token: str | None = None):
        typing_events.append((active, sender, token))

    with patch("src.router.RouterSession", FakeRouterSession):
        await router._handle_router_message(1, "/setup", AsyncMock(), AsyncMock(), send_typing)
        await router._router_tasks[1]

    assert len(typing_events) == 2
    assert typing_events[0][0] is True
    assert typing_events[1][0] is False
    assert typing_events[0][1] == "router"
    assert typing_events[1][1] == "router"
    assert typing_events[0][2] == typing_events[1][2]
    assert isinstance(typing_events[0][2], str) and typing_events[0][2].startswith("router-")


async def test_router_typing_compat_with_two_arg_callback(tmp_path):
    (tmp_path / "config.json").write_text('{"servers": []}')
    router = Router(cwd=str(tmp_path))

    class FakeRouterSession:
        def __init__(self, cwd="."):
            self.cwd = cwd

        def set_callbacks(self, progress=None, log=None):
            return None

        async def run(self, prompt: str) -> str:
            return "done"

        def should_emit_final_result(self, result_text: str) -> bool:
            return False

    typing_events = []

    async def send_typing(active: bool, sender: str = "router"):
        typing_events.append((active, sender))

    with patch("src.router.RouterSession", FakeRouterSession):
        await router._handle_router_message(1, "/setup", AsyncMock(), AsyncMock(), send_typing)
        await router._router_tasks[1]

    assert typing_events == [(True, "router"), (False, "router")]


# ── Multi-listener fan-out and source cache ──────────────────────────


async def test_multi_listener_fan_out():
    """Multiple progress listeners all receive the same event."""
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="idle")])

    seen_a = []
    seen_b = []

    async def listener_a(project_id, event):
        seen_a.append(event.get("event_id"))

    async def listener_b(project_id, event):
        seen_b.append(event.get("event_id"))

    router.add_progress_listener(listener_a)
    router.add_progress_listener(listener_b)

    event = {"event_id": "multi-1", "type": "iteration", "data": "Iteration 1/5", "iteration": 1}
    await router.ingest_progress_event("proj", event, source="sse")

    assert seen_a == ["multi-1"]
    assert seen_b == ["multi-1"]


async def test_listener_error_isolation():
    """A failing listener does not prevent other listeners from running."""
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="idle")])

    seen = []

    async def bad_listener(project_id, event):
        raise RuntimeError("boom")

    async def good_listener(project_id, event):
        seen.append(event.get("event_id"))

    router.add_progress_listener(bad_listener)
    router.add_progress_listener(good_listener)

    event = {"event_id": "iso-1", "type": "iteration", "data": "Iteration 1/5", "iteration": 1}
    await router.ingest_progress_event("proj", event, source="sse")

    assert seen == ["iso-1"]


async def test_channel_source_tracking():
    """set/get_channel_source works correctly."""
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv")])

    assert router.get_channel_source(1) is None

    router.set_channel_source(1, "web")
    assert router.get_channel_source(1) == "web"

    router.set_channel_source(1, "telegram")
    assert router.get_channel_source(1) == "telegram"


async def test_connect_channel_sets_source():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv")])
    await router.connect_channel(1, "proj", source="web")
    assert router.get_channel_source(1) == "web"


async def test_disconnect_channel_clears_source():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv")])
    await router.connect_channel(1, "proj", source="web")
    await router.disconnect_channel(1)
    assert router.get_channel_source(1) is None


async def test_hydrate_with_source():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv")])
    ok = router.hydrate_channel_mapping(1, "proj", source="telegram")
    assert ok is True
    assert router.get_channel_source(1) == "telegram"


async def test_remove_progress_listener():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="idle")])

    seen = []

    async def listener(project_id, event):
        seen.append(1)

    router.add_progress_listener(listener)
    event = {"event_id": "rm-1", "type": "iteration", "data": "x", "iteration": 1}
    await router.ingest_progress_event("proj", event, source="sse")
    assert len(seen) == 1

    router.remove_progress_listener(listener)
    event2 = {"event_id": "rm-2", "type": "iteration", "data": "y", "iteration": 2}
    await router.ingest_progress_event("proj", event2, source="sse")
    assert len(seen) == 1  # listener was removed


async def test_multi_mapping_persist_listeners():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv")])

    persisted_a = {}
    persisted_b = {}

    async def persist_a(chat_id, project_id):
        persisted_a[chat_id] = project_id

    async def persist_b(chat_id, project_id):
        persisted_b[chat_id] = project_id

    router.add_mapping_persist_listener(persist_a)
    router.add_mapping_persist_listener(persist_b)

    await router.connect_channel(1, "proj")
    assert persisted_a[1] == "proj"
    assert persisted_b[1] == "proj"
