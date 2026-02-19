"""Tests for orchestrator daemon helper functions.

Covers _extract_after_marker (still used for worker [WORKER_REPORT] extraction)
and _create_orchestrator_tools (MCP tool factory).
"""

import sys
import os

# Add tools/ to path so we can import daemon helpers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from orchestrator_daemon import _extract_after_marker


# ── _extract_after_marker ─────────────────────────────────────────────


def test_extract_single_line():
    text = "blah\n[TASK_COMPLETE]\nSummary: all done"
    assert _extract_after_marker(text, "[TASK_COMPLETE]") == "all done"


def test_extract_multi_line():
    text = (
        "thinking...\n"
        "[ASSIGN_WORKER]\n"
        "WorkerTask: Do these things:\n"
        "1. Read the config file\n"
        "2. Fix the bug in function Y\n"
        "3. Run the test suite\n"
    )
    result = _extract_after_marker(text, "[ASSIGN_WORKER]")
    assert "Do these things:" in result
    assert "1. Read the config file" in result
    assert "2. Fix the bug in function Y" in result
    assert "3. Run the test suite" in result


def test_extract_stops_at_next_marker():
    text = (
        "[ASSIGN_WORKER]\n"
        "WorkerTask: check the logs\n"
        "Look for error patterns\n"
        "[TASK_COMPLETE]\n"
        "Summary: should not be included"
    )
    result = _extract_after_marker(text, "[ASSIGN_WORKER]")
    assert "check the logs" in result
    assert "Look for error patterns" in result
    assert "should not be included" not in result


def test_extract_missing_marker():
    text = "no markers here"
    assert _extract_after_marker(text, "[TASK_COMPLETE]") == ""


def test_extract_empty_after_marker():
    text = "[TASK_COMPLETE]"
    assert _extract_after_marker(text, "[TASK_COMPLETE]") == ""


def test_extract_strips_question_prefix():
    text = "[NEED_USER_INPUT]\nQuestion: What API key should I use?"
    assert _extract_after_marker(text, "[NEED_USER_INPUT]") == "What API key should I use?"


def test_extract_worker_report():
    text = (
        "I checked the file and found the issue.\n"
        "[WORKER_REPORT]\n"
        "Summary: Fixed the off-by-one error in parser.py line 42.\n"
        "Tests now pass (23/23).\n"
        "No regressions found."
    )
    result = _extract_after_marker(text, "[WORKER_REPORT]")
    assert "Fixed the off-by-one error" in result
    assert "Tests now pass" in result
    assert "No regressions found" in result


def test_extract_preserves_indented_content():
    text = (
        "[ASSIGN_WORKER]\n"
        "WorkerTask: Edit the config:\n"
        "  - Set debug = true\n"
        "  - Set log_level = verbose\n"
    )
    result = _extract_after_marker(text, "[ASSIGN_WORKER]")
    assert "Edit the config:" in result
    assert "- Set debug = true" in result
    assert "- Set log_level = verbose" in result


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
