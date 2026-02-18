"""Router routing and ingestion tests.

Covers:
- slash command routing (including @orchestrator-prefixed commands)
- urgency semantics: @orchestrator = urgent interrupt, non-mention while running = deferred
- progress ingestion dedupe (event_id + fallback signature)
- deferred queue autostart on terminal events
- /connect command via route_message
- setup mode routing
- config loading (dual schema) and reload
- channel/project mapping helpers
"""

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


async def test_route_no_project_connected():
    router = _make_router([RemoteOrchestrator(project_id="test", name="test-srv")])
    send_reply = AsyncMock()
    await router.route_message(1, "hello", send_reply)
    send_reply.assert_called_once()
    assert "connect" in send_reply.call_args[0][0].lower()


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


async def test_channel_status_callback():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="idle")])
    router._channel_project[1] = "proj"

    events = []

    async def cb(event):
        events.append(event)

    router.set_channel_status_callback(1, cb)
    await router._update_channel_status(1, "iteration", "Iteration 1/20", 1)
    assert len(events) == 1
    assert events[0]["type"] == "busy"

    router.remove_channel_status_callback(1)
    await router._update_channel_status(1, "done", "Complete", 5)
    assert len(events) == 1


async def test_route_to_disconnected_daemon():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="disconnected")])
    router._channel_project[1] = "proj"

    send_reply = AsyncMock()
    await router.route_message(1, "do something", send_reply)
    assert "disconnected" in send_reply.call_args[0][0].lower()


async def test_ingest_progress_event_dedupes_event_id():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="idle")])

    seen = []

    async def on_progress(project_id, event):
        seen.append((project_id, event.get("event_id")))

    router.set_progress_callback(on_progress)

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

    router.set_progress_callback(on_progress)

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


async def test_setup_command_enters_setup_mode():
    """/setup enters setup mode and routes to _handle_setup."""
    router = _make_router([])

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch.object(router, "_handle_setup", new_callable=AsyncMock) as mock_setup:
        await router.route_message(1, "/setup user@server", send_reply, send_log)
        mock_setup.assert_called_once_with(1, "/setup user@server", send_reply, send_log)
        assert 1 in router._setup_channels


async def test_setup_mode_follow_up_routes_to_setup():
    """Follow-up messages in setup mode route to setup handler."""
    router = _make_router([
        RemoteOrchestrator(project_id="proj", name="srv", status="idle"),
    ])
    router._channel_project[1] = "proj"
    router._setup_channels.add(1)

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch.object(router, "_handle_setup", new_callable=AsyncMock) as mock_setup:
        await router.route_message(1, "this machine uses conda", send_reply, send_log)
        mock_setup.assert_called_once()
        assert mock_setup.call_args[0][1] == "this machine uses conda"


async def test_setup_mode_exit_on_done():
    """/done exits setup mode."""
    router = _make_router([])
    router._setup_channels.add(1)

    send_reply = AsyncMock()
    await router.route_message(1, "/done", send_reply)

    assert 1 not in router._setup_channels
    send_reply.assert_called_once()
    assert "exited" in send_reply.call_args[0][0].lower()


async def test_setup_mode_exit_on_connect():
    """/connect exits setup mode and proceeds with connect handling."""
    router = _make_router([
        RemoteOrchestrator(project_id="proj", name="srv"),
    ])
    router._setup_channels.add(1)

    send_reply = AsyncMock()
    await router.route_message(1, "/connect proj", send_reply)

    assert 1 not in router._setup_channels
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


# ── stuck/error also trigger deferred tasks ──────────────────────────


async def test_stuck_event_starts_deferred_task():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv", status="running")])
    router._deferred_tasks["proj"] = [{"chat_id": 1, "text": "queued task", "ts": 1.0}]

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    await router.ingest_progress_event(
        "proj",
        {"event_id": "e-stuck", "type": "stuck", "data": "need help"},
        source="sse",
    )

    assert mock_http.post.called
    assert "/task" in mock_http.post.call_args[0][0]
    assert mock_http.post.call_args[1]["json"]["task"] == "queued task"


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


# ── Mapping persistence callback ─────────────────────────────────────


async def test_connect_channel_calls_persist_callback():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv")])
    persisted = {}

    async def persist(chat_id, project_id):
        persisted[chat_id] = project_id

    router.set_mapping_persist_callback(persist)
    await router.connect_channel(1, "proj")

    assert persisted[1] == "proj"


async def test_disconnect_channel_calls_persist_with_none():
    router = _make_router([RemoteOrchestrator(project_id="proj", name="srv")])
    router._channel_project[1] = "proj"
    persisted = {}

    async def persist(chat_id, project_id):
        persisted[chat_id] = project_id

    router.set_mapping_persist_callback(persist)
    await router.disconnect_channel(1)

    assert persisted[1] is None
