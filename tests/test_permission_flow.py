"""Test the permission and clarification callback flow.

Verifies that:
- Worker safe tools are auto-approved (fast path)
- Orchestrator-routed permission approve/deny/escalate works
- Clarification answered by orchestrator vs escalated works
- Human resolution plumbing (pending_count, resolve, get_pending_questions)
- HTTP callback integration
"""

import asyncio
import json
import pytest
import tempfile
from unittest.mock import AsyncMock, patch
from aiohttp import web

from src.orchestrator import Orchestrator
from src.session import SessionManager, ServerConfig
from src.store import Store


async def _make_orchestrator(workers=None):
    """Create an Orchestrator with mocked _send_to_orchestrator for testing."""
    store = Store(tempfile.mkdtemp())
    await store.init()

    mgr = SessionManager(servers=[], default_model="haiku")
    await mgr.init()

    orch = Orchestrator(
        session_mgr=mgr,
        store=store,
        model="haiku",
        cwd=".",
    )

    # Register workers in the reverse index
    if workers:
        for server, session, chat_id in workers:
            orch._worker_to_chat[(server, session)] = chat_id

    return orch, mgr, store


# ── Fast-path permission tests ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_auto_approve_safe_tools():
    """Read, Grep, Glob etc. should be auto-approved without calling orchestrator."""
    orch, mgr, store = await _make_orchestrator()
    try:
        for tool in ["Read", "Grep", "Glob", "WebSearch", "WebFetch", "Explore"]:
            result = await orch.handle_permission_request({
                "server_name": "local",
                "session_id": "dev",
                "tool_name": tool,
                "tool_input": {"file_path": "/tmp/test.py"},
            })
            assert result["approved"] is True, f"{tool} should be auto-approved"
            assert "Auto-approved" in result["reason"]
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_unknown_worker_denied():
    """Permission request from unknown worker should be denied."""
    orch, mgr, store = await _make_orchestrator()
    try:
        result = await orch.handle_permission_request({
            "server_name": "unknown",
            "session_id": "unknown",
            "tool_name": "Bash",
            "tool_input": {"command": "echo hello"},
        })
        assert result["approved"] is False
        assert "Unknown worker" in result["reason"]
    finally:
        await mgr.close()
        await store.close()


# ── Orchestrator-routed permission tests ───────────────────────────────

@pytest.mark.asyncio
async def test_orchestrator_approves_permission():
    """Orchestrator session approves a permission request."""
    orch, mgr, store = await _make_orchestrator(
        workers=[("local", "dev", 1)]
    )
    try:
        # Mock _send_to_orchestrator to return approve
        async def mock_send(chat_id, text):
            assert "[PERMISSION REQUEST]" in text
            assert "Bash" in text
            return {
                "action": "permission_decision",
                "approved": True,
                "reason": "Aligns with task",
            }
        orch._send_to_orchestrator = mock_send

        result = await orch.handle_permission_request({
            "server_name": "local",
            "session_id": "dev",
            "tool_name": "Bash",
            "tool_input": {"command": "npm install"},
        })
        assert result["approved"] is True
        assert result["reason"] == "Aligns with task"
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_orchestrator_denies_permission():
    """Orchestrator session denies a permission request."""
    orch, mgr, store = await _make_orchestrator(
        workers=[("local", "dev", 1)]
    )
    try:
        async def mock_send(chat_id, text):
            return {
                "action": "permission_decision",
                "approved": False,
                "reason": "Destructive action",
            }
        orch._send_to_orchestrator = mock_send

        result = await orch.handle_permission_request({
            "server_name": "local",
            "session_id": "dev",
            "tool_name": "Bash",
            "tool_input": {"command": "rm -rf /"},
        })
        assert result["approved"] is False
        assert "Destructive" in result["reason"]
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_orchestrator_escalates_permission():
    """Orchestrator escalates → human approves → resolved."""
    orch, mgr, store = await _make_orchestrator(
        workers=[("local", "dev", 1)]
    )
    try:
        escalated = {}

        async def mock_send(chat_id, text):
            if "[PERMISSION REQUEST]" in text and "(FORCED)" not in text:
                return {
                    "action": "permission_decision",
                    "escalate": True,
                    "reason": "Ambiguous",
                }
            # Shouldn't reach forced fallback in this test
            return {"action": "permission_decision", "approved": True, "reason": "forced"}

        orch._send_to_orchestrator = mock_send

        # Wire escalation sender to capture and auto-approve
        async def capture_escalation(request_id, interaction_type, title, detail):
            escalated["request_id"] = request_id
            escalated["type"] = interaction_type
            await asyncio.sleep(0.05)
            orch.resolve_permission(request_id, approved=True, reason="User approved")

        orch.set_send_telegram(capture_escalation)

        result = await orch.handle_permission_request({
            "server_name": "local",
            "session_id": "dev",
            "tool_name": "Bash",
            "tool_input": {"command": "npm install"},
        })
        assert result["approved"] is True
        assert escalated["type"] == "permission"
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_orchestrator_escalates_permission_denied():
    """Orchestrator escalates → human denies."""
    orch, mgr, store = await _make_orchestrator(
        workers=[("local", "dev", 1)]
    )
    try:
        async def mock_send(chat_id, text):
            return {
                "action": "permission_decision",
                "escalate": True,
                "reason": "Ambiguous",
            }
        orch._send_to_orchestrator = mock_send

        async def deny_escalation(request_id, interaction_type, title, detail):
            await asyncio.sleep(0.05)
            orch.resolve_permission(request_id, approved=False, reason="Too risky")

        orch.set_send_telegram(deny_escalation)

        result = await orch.handle_permission_request({
            "server_name": "local",
            "session_id": "dev",
            "tool_name": "Bash",
            "tool_input": {"command": "rm -rf /tmp/stuff"},
        })
        assert result["approved"] is False
    finally:
        await mgr.close()
        await store.close()


# ── Clarification tests ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_orchestrator_answers_clarification():
    """Orchestrator answers a clarification question directly."""
    orch, mgr, store = await _make_orchestrator(
        workers=[("local", "dev", 1)]
    )
    try:
        async def mock_send(chat_id, text):
            assert "[CLARIFICATION REQUEST]" in text
            return {
                "action": "clarification_answer",
                "answers": {"Which database should we use?": "PostgreSQL"},
                "reason": "Config specifies PostgreSQL",
            }
        orch._send_to_orchestrator = mock_send

        result = await orch.handle_clarification_request({
            "server_name": "local",
            "session_id": "dev",
            "questions": [{
                "question": "Which database should we use?",
                "header": "Database",
                "options": [
                    {"label": "PostgreSQL", "description": "Relational DB"},
                    {"label": "MongoDB", "description": "Document DB"},
                ],
                "multiSelect": False,
            }],
        })
        assert result["answers"]["Which database should we use?"] == "PostgreSQL"
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_orchestrator_escalates_clarification():
    """Orchestrator escalates clarification → human answers."""
    orch, mgr, store = await _make_orchestrator(
        workers=[("local", "dev", 1)]
    )
    try:
        questions = [{
            "question": "Which database should we use?",
            "header": "Database",
            "options": [
                {"label": "PostgreSQL", "description": "Relational DB"},
                {"label": "MongoDB", "description": "Document DB"},
            ],
            "multiSelect": False,
        }]

        async def mock_send(chat_id, text):
            return {
                "action": "clarification_answer",
                "escalate": True,
                "reason": "Design decision",
            }
        orch._send_to_orchestrator = mock_send

        async def answer_escalation(request_id, interaction_type, title, detail):
            assert interaction_type == "clarification"
            await asyncio.sleep(0.05)
            orch.resolve_clarification(
                request_id,
                question="Which database should we use?",
                answer="PostgreSQL",
            )

        orch.set_send_telegram(answer_escalation)

        result = await orch.handle_clarification_request({
            "server_name": "local",
            "session_id": "dev",
            "questions": questions,
        })
        assert result["answers"]["Which database should we use?"] == "PostgreSQL"
    finally:
        await mgr.close()
        await store.close()


# ── Resolution plumbing tests ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_pending_count():
    orch, mgr, store = await _make_orchestrator()
    try:
        assert orch.pending_count == 0
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_get_pending_questions_none():
    orch, mgr, store = await _make_orchestrator()
    try:
        assert orch.get_pending_questions("nonexistent") is None
    finally:
        await mgr.close()
        await store.close()


@pytest.mark.asyncio
async def test_resolve_permission_expired():
    """Resolving a non-existent request returns False."""
    orch, mgr, store = await _make_orchestrator()
    try:
        ok = orch.resolve_permission("nonexistent", approved=True)
        assert ok is False
    finally:
        await mgr.close()
        await store.close()


# ── HTTP callback integration ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_http_permission_callback(aiohttp_client):
    """Simulate broker calling POST /permission on the orchestrator."""
    orch, mgr, store = await _make_orchestrator(
        workers=[("local", "dev", 1)]
    )

    # Mock orchestrator session to approve non-safe tools
    async def mock_send(chat_id, text):
        return {
            "action": "permission_decision",
            "approved": True,
            "reason": "Looks safe",
        }
    orch._send_to_orchestrator = mock_send

    # Build HTTP app like main.py does
    app = web.Application()
    app["orchestrator"] = orch

    async def handle_permission(request):
        data = await request.json()
        result = await orch.handle_permission_request(data)
        return web.json_response(result)

    async def handle_clarification(request):
        data = await request.json()
        result = await orch.handle_clarification_request(data)
        return web.json_response(result)

    app.router.add_post("/permission", handle_permission)
    app.router.add_post("/clarification", handle_clarification)

    client = await aiohttp_client(app)

    try:
        # Simulate broker calling /permission for a Read tool (fast-path)
        resp = await client.post("/permission", json={
            "server_name": "local",
            "session_id": "dev",
            "tool_name": "Read",
            "tool_input": {"file_path": "/tmp/test.py"},
        })
        assert resp.status == 200
        data = await resp.json()
        assert data["approved"] is True

        # Simulate broker calling /permission for Bash (orchestrator evaluates)
        resp = await client.post("/permission", json={
            "server_name": "local",
            "session_id": "dev",
            "tool_name": "Bash",
            "tool_input": {"command": "echo hello"},
        })
        assert resp.status == 200
        data = await resp.json()
        assert data["approved"] is True  # mock returns approve
    finally:
        await mgr.close()
        await store.close()
