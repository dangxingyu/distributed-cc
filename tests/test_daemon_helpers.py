"""Tests for orchestrator daemon helper functions.

Covers _extract_after_marker and _build_prompt.
"""

import sys
import os

# Add tools/ to path so we can import daemon helpers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from orchestrator_daemon import _extract_after_marker, _extract_task_list, _build_prompt


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


# ── _build_prompt ─────────────────────────────────────────────────────


def test_build_prompt_first_iteration():
    prompt = _build_prompt(
        task_text="fix the bug",
        feedback="",
        user_msgs=[],
        iteration=1,
        worker_report="",
    )
    assert "[TASK]" in prompt
    assert "fix the bug" in prompt
    assert "[CONTINUATION" not in prompt


def test_build_prompt_continuation():
    prompt = _build_prompt(
        task_text="fix the bug",
        feedback="some feedback",
        user_msgs=[],
        iteration=3,
        worker_report="",
    )
    assert "[CONTINUATION" in prompt
    assert "iteration 3" in prompt
    assert "[SUPERVISOR_FEEDBACK]" in prompt
    assert "some feedback" in prompt


def test_build_prompt_with_worker_report():
    prompt = _build_prompt(
        task_text="fix the bug",
        feedback="",
        user_msgs=[],
        iteration=2,
        worker_report="Fixed parser.py",
    )
    assert "[LATEST_WORKER_REPORT]" in prompt
    assert "Fixed parser.py" in prompt


def test_build_prompt_with_user_interruptions():
    prompt = _build_prompt(
        task_text="fix the bug",
        feedback="",
        user_msgs=["also check tests", "use pytest"],
        iteration=2,
        worker_report="",
    )
    assert "[USER INTERRUPTIONS]" in prompt
    assert "also check tests" in prompt
    assert "use pytest" in prompt


def test_build_prompt_no_duplicate_worker_report():
    """Worker report should NOT appear in feedback anymore."""
    prompt = _build_prompt(
        task_text="fix the bug",
        feedback="Worker report received (see [LATEST_WORKER_REPORT] below). Verify this.",
        user_msgs=[],
        iteration=2,
        worker_report="Fixed parser.py line 42",
    )
    # Count occurrences of the report content
    count = prompt.count("Fixed parser.py line 42")
    assert count == 1, f"Worker report appeared {count} times, expected 1"


# ── _extract_task_list ────────────────────────────────────────────────


def test_extract_task_list_basic():
    text = (
        "Let me plan this.\n"
        "[TASK_LIST]\n"
        "- [ ] Check reward function\n"
        "- [ ] Analyze logs\n"
        "[/TASK_LIST]\n"
        "[ASSIGN_WORKER]\nWorkerTask: do something"
    )
    result = _extract_task_list(text)
    assert result == "- [ ] Check reward function\n- [ ] Analyze logs"


def test_extract_task_list_missing_markers():
    assert _extract_task_list("no markers here") is None
    assert _extract_task_list("[TASK_LIST] only open") is None
    assert _extract_task_list("[/TASK_LIST] only close") is None


def test_extract_task_list_with_checked_items():
    text = (
        "[TASK_LIST]\n"
        "- [x] Completed item\n"
        "- [ ] Pending item\n"
        "- [ ] Another pending\n"
        "[/TASK_LIST]"
    )
    result = _extract_task_list(text)
    assert "- [x] Completed item" in result
    assert "- [ ] Pending item" in result
    assert "- [ ] Another pending" in result


def test_extract_task_list_reversed_markers():
    """End marker before start marker returns None."""
    text = "[/TASK_LIST]\nstuff\n[TASK_LIST]"
    assert _extract_task_list(text) is None


# ── _build_prompt with task_list ──────────────────────────────────────


def test_build_prompt_with_task_list():
    prompt = _build_prompt(
        task_text="investigate reward hacking",
        feedback="",
        user_msgs=[],
        iteration=2,
        task_list="- [x] Check reward function\n- [ ] Analyze logs",
    )
    assert "[CURRENT_TASK_LIST]" in prompt
    assert "- [x] Check reward function" in prompt
    assert "- [ ] Analyze logs" in prompt
    # Task list should appear before CONTINUATION
    tl_pos = prompt.find("[CURRENT_TASK_LIST]")
    cont_pos = prompt.find("[CONTINUATION")
    assert tl_pos < cont_pos


def test_build_prompt_without_task_list():
    prompt = _build_prompt(
        task_text="fix the bug",
        feedback="",
        user_msgs=[],
        iteration=1,
    )
    assert "[CURRENT_TASK_LIST]" not in prompt
