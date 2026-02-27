"""Tests for orchestrator daemon helper functions.

Covers _create_orchestrator_tools and _create_worker_tools (MCP tool factories),
and report file I/O.
"""

import asyncio
import sys
import os
import json
from collections import deque

import pytest

# Add tools/ to path so we can import daemon helpers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))


# ── _create_orchestrator_tools ────────────────────────────────────────


def test_create_orchestrator_tools_returns_server():
    """Factory returns an MCP server config with the expected tools."""
    from orchestrator_daemon import _create_orchestrator_tools, TaskState

    state = TaskState(task_id="t1", project_id="p1", task_text="test")
    server = _create_orchestrator_tools("p1", state)

    # Should be a dict-like McpSdkServerConfig with type="sdk"
    assert server["type"] == "sdk"
    assert server["name"] == "daemon"
    # The instance should have our tools registered
    mcp_instance = server["instance"]
    assert mcp_instance is not None


# ── _create_worker_tools ─────────────────────────────────────────────


def test_create_worker_tools_returns_server(tmp_path):
    """Factory returns an MCP server config with submit_report tool."""
    from orchestrator_daemon import _create_worker_tools, projects, Project

    projects["p_test"] = Project(
        project_id="p_test", project_dir=str(tmp_path), name="test"
    )
    try:
        captured = []
        server = _create_worker_tools("p_test", iteration=3, captured_report=captured)

        assert server["type"] == "sdk"
        assert server["name"] == "worker_tools"
        assert server["instance"] is not None
    finally:
        projects.pop("p_test", None)


def test_create_worker_tools_creates_reports_dir(tmp_path):
    """Worker tools factory creates .reports/ directory."""
    from orchestrator_daemon import _create_worker_tools, projects, Project

    projects["p_test2"] = Project(
        project_id="p_test2", project_dir=str(tmp_path), name="test"
    )
    try:
        captured = []
        _create_worker_tools("p_test2", iteration=1, captured_report=captured)
        assert (tmp_path / ".reports").is_dir()
    finally:
        projects.pop("p_test2", None)


def test_submit_report_writes_file_and_captures(tmp_path):
    """Report file is written to .reports/iteration-N.md correctly."""
    from pathlib import Path

    reports_dir = tmp_path / ".reports"
    reports_dir.mkdir()

    report_text = "## What was done\nFixed bug in parser.py\n## Results\nAll 23 tests pass."
    report_path = reports_dir / "iteration-5.md"
    report_path.write_text(report_text)

    # Verify file structure
    assert report_path.exists()
    assert report_path.read_text() == report_text

    # Verify captured_report pattern (used by closure in _create_worker_tools)
    captured = []
    captured.append(report_text)
    assert len(captured) == 1
    assert captured[0] == report_text


# ── _append_log ──────────────────────────────────────────────────────


def test_append_log_creates_file(tmp_path):
    """First append_log creates LOG.md with a timestamped entry."""
    from orchestrator_daemon import _append_log, projects, Project

    projects["log_test"] = Project(
        project_id="log_test", project_dir=str(tmp_path), name="test"
    )
    try:
        _append_log("log_test", "Hypothesis: reward hacking causing plateau")
        log_path = tmp_path / "LOG.md"
        assert log_path.exists()
        content = log_path.read_text()
        assert "reward hacking causing plateau" in content
        assert "---" in content
    finally:
        projects.pop("log_test", None)


def test_append_log_appends_multiple_entries(tmp_path):
    """Multiple append_log calls accumulate entries chronologically."""
    from orchestrator_daemon import _append_log, projects, Project

    projects["log_test2"] = Project(
        project_id="log_test2", project_dir=str(tmp_path), name="test"
    )
    try:
        _append_log("log_test2", "First entry: starting investigation")
        _append_log("log_test2", "Second entry: found the root cause")
        content = (tmp_path / "LOG.md").read_text()
        # Both entries present in order
        first_pos = content.index("First entry")
        second_pos = content.index("Second entry")
        assert first_pos < second_pos
        # Two separators
        assert content.count("---") == 2
    finally:
        projects.pop("log_test2", None)


def test_append_log_unknown_project():
    """append_log returns empty string for unknown project."""
    from orchestrator_daemon import _append_log
    assert _append_log("nonexistent", "test") == ""


def test_compose_role_prompt_appends_role_memory(tmp_path):
    from orchestrator_daemon import _compose_role_prompt, WORKER_PROMPT, WORKER_ROLE_FILE

    baseline_prompt, baseline_hash = _compose_role_prompt(str(tmp_path), "worker")
    assert baseline_prompt == WORKER_PROMPT

    role_path = tmp_path / WORKER_ROLE_FILE
    role_path.parent.mkdir(parents=True, exist_ok=True)
    role_path.write_text("Always append ROLE_WORKER_SENTINEL: meow~ in reports.")

    prompt, prompt_hash = _compose_role_prompt(str(tmp_path), "worker")
    assert "Role Memory (.claude/roles/worker.md)" in prompt
    assert "ROLE_WORKER_SENTINEL: meow~" in prompt
    assert prompt_hash != baseline_hash


def test_load_role_mcp_servers_supports_mcp_servers_schema(tmp_path):
    from orchestrator_daemon import (
        _load_role_mcp_servers,
        ORCHESTRATOR_MCP_FILE,
    )

    cfg_path = tmp_path / ORCHESTRATOR_MCP_FILE
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        json.dumps(
            {
                "mcp_servers": {
                    "filesystem": {
                        "command": "npx",
                        "args": ["-y", "@modelcontextprotocol/server-filesystem", "."],
                    }
                }
            }
        )
    )

    servers, cfg_hash = _load_role_mcp_servers(str(tmp_path), "orchestrator")
    assert "filesystem" in servers
    assert servers["filesystem"]["command"] == "npx"
    assert isinstance(cfg_hash, str) and cfg_hash


def test_load_role_mcp_servers_supports_direct_mapping_schema(tmp_path):
    from orchestrator_daemon import (
        _load_role_mcp_servers,
        WORKER_MCP_FILE,
    )

    cfg_path = tmp_path / WORKER_MCP_FILE
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        json.dumps(
            {
                "memory": {
                    "command": "npx",
                    "args": ["-y", "@modelcontextprotocol/server-memory"],
                }
            }
        )
    )

    servers, cfg_hash = _load_role_mcp_servers(str(tmp_path), "worker")
    assert "memory" in servers
    assert servers["memory"]["command"] == "npx"
    assert isinstance(cfg_hash, str) and cfg_hash


def test_merge_mcp_servers_skips_reserved_names():
    from orchestrator_daemon import _merge_mcp_servers

    merged = _merge_mcp_servers(
        base_servers={"daemon": {"type": "sdk", "name": "daemon"}},
        extra_servers={
            "daemon": {"command": "echo", "args": ["oops"]},
            "filesystem": {"command": "npx", "args": ["-y", "@modelcontextprotocol/server-filesystem", "."]},
        },
        reserved_names={"daemon"},
    )
    assert "daemon" in merged
    assert merged["daemon"]["type"] == "sdk"
    assert "filesystem" in merged


def test_interrupt_queue_is_bounded_and_typed():
    """Interrupt queue should be typed and bounded, dropping oldest when full."""
    from orchestrator_daemon import (
        INTERRUPT_QUEUE_MAX,
        _ensure_interrupt_queue,
        _enqueue_interrupt,
        _interrupt_payload_text,
        interrupt_queues,
    )

    project_id = "queue-test"
    interrupt_queues.pop(project_id, None)
    _ensure_interrupt_queue(project_id)

    for i in range(INTERRUPT_QUEUE_MAX + 5):
        _enqueue_interrupt(project_id, f"msg-{i}")

    queue = interrupt_queues[project_id]
    assert queue.qsize() == INTERRUPT_QUEUE_MAX

    oldest = queue.get_nowait()
    assert isinstance(oldest, dict)
    assert oldest["kind"] == "user_message"
    assert oldest["urgency"] == "normal"
    assert _interrupt_payload_text(oldest) == "msg-5"

    interrupt_queues.pop(project_id, None)


def test_enqueue_interrupt_stores_urgency():
    from orchestrator_daemon import (
        _ensure_interrupt_queue,
        _enqueue_interrupt,
        interrupt_queues,
    )

    project_id = "urgency-test"
    interrupt_queues.pop(project_id, None)
    _ensure_interrupt_queue(project_id)

    _enqueue_interrupt(project_id, "normal msg")
    _enqueue_interrupt(project_id, "urgent msg", urgency="urgent")
    _enqueue_interrupt(project_id, "bad urgency", urgency="unexpected")

    queue = interrupt_queues[project_id]
    first = queue.get_nowait()
    second = queue.get_nowait()
    third = queue.get_nowait()

    assert first["urgency"] == "normal"
    assert second["urgency"] == "urgent"
    assert third["urgency"] == "normal"

    interrupt_queues.pop(project_id, None)


@pytest.mark.asyncio
async def test_wait_for_interrupt_text_skips_empty_payloads():
    """wait_for_interrupt_text should ignore empty payloads and return first text."""
    from orchestrator_daemon import (
        _ensure_interrupt_queue,
        _wait_for_interrupt_text,
        interrupt_queues,
    )

    project_id = "queue-test-async"
    interrupt_queues.pop(project_id, None)
    queue = _ensure_interrupt_queue(project_id)
    queue.put_nowait({"kind": "user_message", "text": "   ", "ts": 0})
    queue.put_nowait({"kind": "user_message", "text": "hello", "ts": 0})

    result = await _wait_for_interrupt_text(project_id, timeout=1)
    assert result == "hello"

    interrupt_queues.pop(project_id, None)


@pytest.mark.asyncio
async def test_wait_for_interrupt_text_preserves_non_user_payloads():
    from orchestrator_daemon import (
        _ensure_interrupt_queue,
        _wait_for_interrupt_text,
        interrupt_queues,
    )

    project_id = "queue-preserve-non-user"
    interrupt_queues.pop(project_id, None)
    queue = _ensure_interrupt_queue(project_id)
    queue.put_nowait({"kind": "system_nudge", "text": "heartbeat", "ts": 0})
    queue.put_nowait({"kind": "user_message", "text": "human answer", "ts": 0})

    result = await _wait_for_interrupt_text(project_id, timeout=1)
    assert result == "human answer"

    remaining = queue.get_nowait()
    assert remaining["kind"] == "system_nudge"
    assert remaining["text"] == "heartbeat"
    interrupt_queues.pop(project_id, None)


def test_task_list_has_unchecked_items(tmp_path):
    from orchestrator_daemon import _task_list_has_unchecked_items, projects, Project

    project_id = "task-list-unchecked"
    projects[project_id] = Project(project_id=project_id, project_dir=str(tmp_path), name="x")
    try:
        (tmp_path / "task_list.md").write_text("- [x] done\n- [ ] next item\n")
        assert _task_list_has_unchecked_items(project_id) is True

        (tmp_path / "task_list.md").write_text("- [x] done\n- [x] done2\n")
        assert _task_list_has_unchecked_items(project_id) is False
    finally:
        projects.pop(project_id, None)


@pytest.mark.asyncio
async def test_maybe_start_standby_wakeup_requires_meaningful_signal(tmp_path, monkeypatch):
    import orchestrator_daemon as daemon_mod
    from orchestrator_daemon import (
        _maybe_start_standby_wakeup,
        Project,
        TaskState,
        projects,
        running_tasks,
        task_states,
        project_last_standby_wake_ts,
    )

    project_id = "standby-no-signal"
    projects[project_id] = Project(project_id=project_id, project_dir=str(tmp_path), name="x")
    task_states[project_id] = TaskState(
        task_id="t-standby-no-signal",
        project_id=project_id,
        task_text="done work",
        status="done",
    )

    called = {"run_task": False}

    async def fake_run_task(*_args, **_kwargs):
        called["run_task"] = True

    async def fake_emit_progress(*_args, **_kwargs):
        return None

    monkeypatch.setattr(daemon_mod, "run_task", fake_run_task)
    monkeypatch.setattr(daemon_mod, "emit_progress", fake_emit_progress)
    monkeypatch.setattr(daemon_mod, "STANDBY_HEARTBEAT_ENABLED", True)

    try:
        started = await _maybe_start_standby_wakeup(project_id)
        assert started is False
        assert called["run_task"] is False
    finally:
        projects.pop(project_id, None)
        task_states.pop(project_id, None)
        running_tasks.pop(project_id, None)
        project_last_standby_wake_ts.pop(project_id, None)


@pytest.mark.asyncio
async def test_maybe_start_standby_wakeup_starts_when_unchecked_items_exist(
    tmp_path, monkeypatch
):
    import orchestrator_daemon as daemon_mod
    from orchestrator_daemon import (
        STANDBY_WAKE_MAX_ITERATIONS,
        _maybe_start_standby_wakeup,
        Project,
        TaskState,
        projects,
        running_tasks,
        task_states,
        project_last_standby_wake_ts,
    )

    project_id = "standby-starts"
    projects[project_id] = Project(project_id=project_id, project_dir=str(tmp_path), name="x")
    task_states[project_id] = TaskState(
        task_id="t-standby-starts",
        project_id=project_id,
        task_text="done work",
        status="done",
        model="model-a",
        session_model="model-b",
        permission_mode="default",
    )
    (tmp_path / "task_list.md").write_text("- [ ] resume experiment\n")

    called = {}

    async def fake_run_task(
        project_id: str,
        task_text: str,
        max_iterations: int = 0,
        continuous_mode: bool = True,
        model: str = "",
        session_model: str = "",
        permission_mode: str = "",
    ):
        called["project_id"] = project_id
        called["task_text"] = task_text
        called["max_iterations"] = max_iterations
        called["continuous_mode"] = continuous_mode
        called["model"] = model
        called["session_model"] = session_model
        called["permission_mode"] = permission_mode

    async def fake_emit_progress(*_args, **_kwargs):
        return None

    async def fake_gpu_hint(_project_dir: str) -> str:
        return ""

    monkeypatch.setattr(daemon_mod, "run_task", fake_run_task)
    monkeypatch.setattr(daemon_mod, "emit_progress", fake_emit_progress)
    monkeypatch.setattr(daemon_mod, "_gpu_idle_hint", fake_gpu_hint)
    monkeypatch.setattr(daemon_mod, "STANDBY_HEARTBEAT_ENABLED", True)

    try:
        started = await _maybe_start_standby_wakeup(project_id)
        assert started is True
        task = running_tasks.get(project_id)
        assert task is not None
        await task

        assert called["project_id"] == project_id
        assert called["max_iterations"] == STANDBY_WAKE_MAX_ITERATIONS
        assert called["continuous_mode"] is False
        assert called["model"] == "model-a"
        assert called["session_model"] == "model-b"
        assert called["permission_mode"] == "default"
        assert "[STANDBY HEARTBEAT WAKEUP]" in called["task_text"]
    finally:
        projects.pop(project_id, None)
        task_states.pop(project_id, None)
        running_tasks.pop(project_id, None)
        project_last_standby_wake_ts.pop(project_id, None)


def test_parse_bool_helper():
    from orchestrator_daemon import _parse_bool

    assert _parse_bool(True, default=False) is True
    assert _parse_bool("true", default=False) is True
    assert _parse_bool("YES", default=False) is True
    assert _parse_bool("0", default=True) is False
    assert _parse_bool("off", default=True) is False
    assert _parse_bool("unexpected", default=True) is True


def test_normalize_permission_mode_helper():
    from orchestrator_daemon import _normalize_permission_mode

    assert _normalize_permission_mode("default") == "default"
    assert _normalize_permission_mode("acceptEdits") == "acceptEdits"
    assert _normalize_permission_mode("not-a-mode") == "bypassPermissions"
    assert _normalize_permission_mode("", default="plan") == "plan"


def test_hydrate_sessions_from_state(tmp_path):
    from orchestrator_daemon import (
        _hydrate_sessions_from_state,
        orchestrator_plugin_hashes,
        orchestrator_sessions,
        orchestrator_prompt_hashes,
        worker_plugin_hashes,
        worker_sessions,
        worker_prompt_hashes,
    )
    import orchestrator_daemon as daemon_mod

    project_id = "hydrate-state-test"
    state_file = tmp_path / f"{project_id}.json"
    state_file.write_text(
        json.dumps(
            {
                "orchestrator_session_id": "orch-sid",
                "worker_session_id": "worker-sid",
                "sdk_session_id": "orch-sid",
                "orchestrator_prompt_hash": "orch-hash",
                "worker_prompt_hash": "worker-hash",
                "orchestrator_plugin_hash": "orch-plugin-hash",
                "worker_plugin_hash": "worker-plugin-hash",
            }
        )
    )

    old_state_dir = daemon_mod.STATE_DIR
    daemon_mod.STATE_DIR = tmp_path
    orchestrator_sessions.pop(project_id, None)
    worker_sessions.pop(project_id, None)
    orchestrator_prompt_hashes.pop(project_id, None)
    worker_prompt_hashes.pop(project_id, None)
    orchestrator_plugin_hashes.pop(project_id, None)
    worker_plugin_hashes.pop(project_id, None)
    try:
        assert _hydrate_sessions_from_state(project_id) is True
        assert orchestrator_sessions[project_id] == "orch-sid"
        assert worker_sessions[project_id] == "worker-sid"
        assert orchestrator_prompt_hashes[project_id] == "orch-hash"
        assert worker_prompt_hashes[project_id] == "worker-hash"
        assert orchestrator_plugin_hashes[project_id] == "orch-plugin-hash"
        assert worker_plugin_hashes[project_id] == "worker-plugin-hash"
    finally:
        daemon_mod.STATE_DIR = old_state_dir
        orchestrator_sessions.pop(project_id, None)
        worker_sessions.pop(project_id, None)
        orchestrator_prompt_hashes.pop(project_id, None)
        worker_prompt_hashes.pop(project_id, None)
        orchestrator_plugin_hashes.pop(project_id, None)
        worker_plugin_hashes.pop(project_id, None)


@pytest.mark.asyncio
async def test_handle_interrupt_returns_urgency():
    from orchestrator_daemon import handle_interrupt, projects, Project

    project_id = "interrupt-urgency-test"
    projects[project_id] = Project(project_id=project_id, project_dir="/tmp", name="x")

    class _Req:
        async def json(self):
            return {"project_id": project_id, "message": "hello", "urgency": "urgent"}

    try:
        resp = await handle_interrupt(_Req())
        assert resp.status == 200
        payload = json.loads(resp.text)
        assert payload["urgency"] == "urgent"
        assert payload["queued"] is True
    finally:
        projects.pop(project_id, None)


@pytest.mark.asyncio
async def test_handle_events_replays_after_cursor():
    from orchestrator_daemon import handle_events, event_history, projects, Project

    project_id = "events-replay-test"
    projects[project_id] = Project(project_id=project_id, project_dir="/tmp", name="x")
    event_history[project_id] = deque(
        [
            {"event_id": "e1", "type": "iteration", "data": "start", "iteration": 1, "ts": 1.0},
            {"event_id": "e2", "type": "text", "data": "next", "iteration": 2, "ts": 2.0},
        ],
        maxlen=50,
    )

    class _Req:
        query = {"project_id": project_id, "after_event_id": "e1"}

    try:
        resp = await handle_events(_Req())
        assert resp.status == 200
        payload = json.loads(resp.text)
        assert payload["truncated"] is False
        assert [e["event_id"] for e in payload["events"]] == ["e2"]
    finally:
        projects.pop(project_id, None)
        event_history.pop(project_id, None)


@pytest.mark.asyncio
async def test_handle_events_marks_truncated_when_cursor_missing():
    from orchestrator_daemon import handle_events, event_history, projects, Project

    project_id = "events-truncated-test"
    projects[project_id] = Project(project_id=project_id, project_dir="/tmp", name="x")
    event_history[project_id] = deque(
        [
            {"event_id": "e10", "type": "iteration", "data": "old", "iteration": 10, "ts": 10.0},
            {"event_id": "e11", "type": "text", "data": "new", "iteration": 11, "ts": 11.0},
        ],
        maxlen=50,
    )

    class _Req:
        query = {"project_id": project_id, "after_event_id": "missing-cursor", "limit": "1"}

    try:
        resp = await handle_events(_Req())
        assert resp.status == 200
        payload = json.loads(resp.text)
        assert payload["truncated"] is True
        assert len(payload["events"]) == 1
        assert payload["events"][0]["event_id"] == "e11"
    finally:
        projects.pop(project_id, None)
        event_history.pop(project_id, None)
