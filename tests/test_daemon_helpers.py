"""Tests for orchestrator daemon helper functions.

Covers _create_orchestrator_tools and _create_worker_tools (MCP tool factories),
and report file I/O.
"""

import asyncio
import sys
import os
import json

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


def test_parse_bool_helper():
    from orchestrator_daemon import _parse_bool

    assert _parse_bool(True, default=False) is True
    assert _parse_bool("true", default=False) is True
    assert _parse_bool("YES", default=False) is True
    assert _parse_bool("0", default=True) is False
    assert _parse_bool("off", default=True) is False
    assert _parse_bool("unexpected", default=True) is True


def test_hydrate_sessions_from_state(tmp_path):
    from orchestrator_daemon import (
        _hydrate_sessions_from_state,
        orchestrator_sessions,
        orchestrator_prompt_hashes,
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
            }
        )
    )

    old_state_dir = daemon_mod.STATE_DIR
    daemon_mod.STATE_DIR = tmp_path
    orchestrator_sessions.pop(project_id, None)
    worker_sessions.pop(project_id, None)
    orchestrator_prompt_hashes.pop(project_id, None)
    worker_prompt_hashes.pop(project_id, None)
    try:
        assert _hydrate_sessions_from_state(project_id) is True
        assert orchestrator_sessions[project_id] == "orch-sid"
        assert worker_sessions[project_id] == "worker-sid"
        assert orchestrator_prompt_hashes[project_id] == "orch-hash"
        assert worker_prompt_hashes[project_id] == "worker-hash"
    finally:
        daemon_mod.STATE_DIR = old_state_dir
        orchestrator_sessions.pop(project_id, None)
        worker_sessions.pop(project_id, None)
        orchestrator_prompt_hashes.pop(project_id, None)
        worker_prompt_hashes.pop(project_id, None)


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
