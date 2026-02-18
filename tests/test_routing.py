"""Test Router message routing: /connect, /stop, /status, @mention, setup mode."""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.router import Router, RemoteOrchestrator


def _make_router(orchestrators=None):
    """Create a Router with test orchestrators (no real HTTP client)."""
    router = Router()
    if orchestrators:
        for o in orchestrators:
            router._orchestrators[o.project_id] = o
    return router


# ── Routing tests ──────────────────────────────────────────────────────


async def test_route_no_project_connected():
    """Message to channel with no project → error message."""
    router = _make_router([
        RemoteOrchestrator(project_id="test", name="test-srv"),
    ])
    send_reply = AsyncMock()
    await router.route_message(1, "hello", send_reply)
    send_reply.assert_called_once()
    assert "connect" in send_reply.call_args[0][0].lower()


async def test_route_idle_starts_task():
    """Message to idle project → POST /task."""
    router = _make_router([
        RemoteOrchestrator(project_id="myproj", name="srv", status="idle"),
    ])
    router._channel_project[1] = "myproj"

    # Mock HTTP client
    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True})
    mock_resp.status = 200
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    send_log = AsyncMock()
    await router.route_message(1, "fix the bug", send_reply, send_log)

    # Verify POST was made
    mock_http.post.assert_called_once()
    call_args = mock_http.post.call_args
    assert "/task" in call_args[0][0]
    payload = call_args[1]["json"]
    assert payload["project_id"] == "myproj"
    assert payload["task"] == "fix the bug"


async def test_route_running_sends_interrupt():
    """Message to running project → POST /interrupt."""
    router = _make_router([
        RemoteOrchestrator(project_id="myproj", name="srv", status="running"),
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
    await router.route_message(1, "also check tests", send_reply)

    mock_http.post.assert_called_once()
    call_args = mock_http.post.call_args
    assert "/interrupt" in call_args[0][0]
    payload = call_args[1]["json"]
    assert payload["message"] == "also check tests"


async def test_route_stop_command():
    """"/stop" → POST /stop to daemon."""
    router = _make_router([
        RemoteOrchestrator(project_id="myproj", name="srv", status="running"),
    ])
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
    """"/status" → GET /status from daemon."""
    router = _make_router([
        RemoteOrchestrator(project_id="myproj", name="srv", status="running"),
    ])
    router._channel_project[1] = "myproj"

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={
        "status": "running",
        "iteration": 3,
        "max_iterations": 20,
        "summary": "",
    })
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


# ── Connect tests ─────────────────────────────────────────────────────


async def test_connect_channel():
    router = _make_router([
        RemoteOrchestrator(project_id="proj-a", name="server-a"),
    ])
    ok = await router.connect_channel(1, "proj-a")
    assert ok
    assert router.get_channel_project(1) == "proj-a"


async def test_connect_unknown_project():
    router = _make_router([])
    ok = await router.connect_channel(1, "nonexistent")
    assert not ok


async def test_get_project_status():
    router = _make_router([
        RemoteOrchestrator(project_id="proj", name="srv", status="running"),
    ])
    assert router.get_project_status("proj") == "running"
    assert router.get_project_status("unknown") == "unknown"


# ── Status callbacks ──────────────────────────────────────────────────


async def test_channel_status_callback():
    router = _make_router([
        RemoteOrchestrator(project_id="proj", name="srv", status="idle"),
    ])
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
    assert len(events) == 1  # no new events after removal


# ── Disconnected daemon ───────────────────────────────────────────────


async def test_route_to_disconnected_daemon():
    router = _make_router([
        RemoteOrchestrator(project_id="proj", name="srv", status="disconnected"),
    ])
    router._channel_project[1] = "proj"

    send_reply = AsyncMock()
    await router.route_message(1, "do something", send_reply)
    assert "disconnected" in send_reply.call_args[0][0].lower()


# ── @mention routing ──────────────────────────────────────────────────


async def test_mention_routes_to_named_server():
    """@name message routes to that specific server."""
    router = _make_router([
        RemoteOrchestrator(project_id="h100", name="h100", status="idle"),
    ])
    # No channel-project connection needed for @mention

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    send_log = AsyncMock()
    await router.route_message(1, "@h100 run the tests", send_reply, send_log)

    mock_http.post.assert_called_once()
    call_args = mock_http.post.call_args
    assert "/task" in call_args[0][0]
    payload = call_args[1]["json"]
    assert payload["project_id"] == "h100"
    assert payload["task"] == "run the tests"


async def test_mention_unknown_server_falls_through():
    """@unknown-name falls through to normal routing (not recognized as mention)."""
    router = _make_router([
        RemoteOrchestrator(project_id="h100", name="h100"),
    ])

    send_reply = AsyncMock()
    await router.route_message(1, "@nonexistent do stuff", send_reply)
    # Not a known name, so _parse_mention returns None → normal routing → no project
    send_reply.assert_called_once()
    assert "connect" in send_reply.call_args[0][0].lower()
    assert "h100" in send_reply.call_args[0][0]


async def test_mention_interrupts_running_server():
    """@name message to a running server sends interrupt."""
    router = _make_router([
        RemoteOrchestrator(project_id="h100", name="h100", status="running"),
    ])

    mock_resp = AsyncMock()
    mock_resp.json = AsyncMock(return_value={"ok": True})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock()

    mock_http = MagicMock()
    mock_http.post = MagicMock(return_value=mock_resp)
    router._http = mock_http

    send_reply = AsyncMock()
    await router.route_message(1, "@h100 stop and check this instead", send_reply)

    mock_http.post.assert_called_once()
    assert "/interrupt" in mock_http.post.call_args[0][0]


async def test_parse_mention():
    """_parse_mention correctly extracts @name."""
    router = _make_router([
        RemoteOrchestrator(project_id="h100", name="h100"),
    ])

    target, body = router._parse_mention("@h100 do something")
    assert target == "h100"
    assert body == "do something"

    target, body = router._parse_mention("@unknown do something")
    assert target is None

    target, body = router._parse_mention("normal message")
    assert target is None


# ── Setup mode routing ───────────────────────────────────────────────────


async def test_setup_command_no_project_needed():
    """/setup works without a connected project — enters setup mode."""
    router = _make_router([])  # No orchestrators

    send_reply = AsyncMock()
    send_log = AsyncMock()

    with patch.object(router, "_handle_setup", new_callable=AsyncMock) as mock_setup:
        await router.route_message(1, "/setup user@server", send_reply, send_log)
        mock_setup.assert_called_once_with(1, "/setup user@server", send_reply, send_log)
        assert 1 in router._setup_channels


async def test_setup_mode_routing():
    """Follow-up messages in setup mode route to setup handler, not daemon."""
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
        # Verify it was routed to setup, not to _start_task
        assert mock_setup.call_args[0][1] == "this machine uses conda"


async def test_setup_mode_exit_on_done():
    """/done exits setup mode and does not route to daemon."""
    router = _make_router([])
    router._setup_channels.add(1)

    send_reply = AsyncMock()
    await router.route_message(1, "/done", send_reply)

    assert 1 not in router._setup_channels
    send_reply.assert_called_once()
    assert "exited" in send_reply.call_args[0][0].lower()


async def test_setup_mode_exit_on_connect():
    """/connect exits setup mode and proceeds with normal connect handling."""
    router = _make_router([
        RemoteOrchestrator(project_id="proj", name="srv"),
    ])
    router._setup_channels.add(1)

    send_reply = AsyncMock()
    await router.route_message(1, "/connect proj", send_reply)

    # Should have exited setup mode
    assert 1 not in router._setup_channels


# ── Reload config ────────────────────────────────────────────────────────


async def test_reload_config_adds_new_projects(tmp_path):
    """reload_config picks up new projects from config.json."""
    config = {
        "servers": [
            {"name": "new-srv", "work_dir": "/tmp"},
        ]
    }
    (tmp_path / "config.json").write_text(json.dumps(config))

    router = Router(cwd=str(tmp_path))
    assert len(router._orchestrators) == 0

    with patch.object(router, "_register_project", new_callable=AsyncMock):
        router.reload_config()

    assert "new-srv" in router._orchestrators


async def test_reload_config_removes_old_projects(tmp_path):
    """reload_config cancels SSE tasks for removed projects."""
    # Start with one project
    config = {
        "servers": [
            {"name": "old-srv", "work_dir": "/tmp"},
        ]
    }
    (tmp_path / "config.json").write_text(json.dumps(config))
    router = Router(cwd=str(tmp_path))
    router._load_config()
    assert "old-srv" in router._orchestrators

    # Add a mock SSE task
    mock_task = MagicMock()
    router._sse_tasks["old-srv"] = mock_task

    # Now empty the config
    (tmp_path / "config.json").write_text('{"servers": []}')
    router.reload_config()

    assert "old-srv" not in router._orchestrators
    mock_task.cancel.assert_called_once()
