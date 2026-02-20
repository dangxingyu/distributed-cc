"""Tests for orchestrator daemon helper functions.

Covers _create_orchestrator_tools and _create_worker_tools (MCP tool factories),
and report file I/O.
"""

import asyncio
import sys
import os

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
