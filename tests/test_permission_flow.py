"""Test the permission and clarification callback flow.

Verifies the HTTP callback server (main.py) can receive broker callbacks
and the PermissionEvaluator's escalation/resolution mechanism works.
"""

import asyncio
import pytest
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase

from src.permission import PermissionEvaluator


@pytest.fixture
def evaluator():
    return PermissionEvaluator(model="haiku", config_path="config.yaml")


# ── Fast-path permission tests ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_auto_approve_safe_tools(evaluator):
    """Read, Grep, Glob etc. should be auto-approved without calling Claude."""
    for tool in ["Read", "Grep", "Glob", "WebSearch", "WebFetch", "Explore"]:
        result = await evaluator.evaluate_permission(
            server_name="local",
            session_id="dev",
            tool_name=tool,
            tool_input={"file_path": "/tmp/test.py"},
            send_escalation=AsyncNoop(),
        )
        assert result["approved"] is True, f"{tool} should be auto-approved"
        assert "Auto-approved" in result["reason"]


# ── Escalation mechanism tests ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_permission_escalation_approve():
    """Permission escalation → human approves → future resolves."""
    evaluator = PermissionEvaluator(model="haiku", config_path="config.yaml")
    escalated = {}

    async def capture_escalation(request_id, interaction_type, title, detail):
        escalated["request_id"] = request_id
        escalated["type"] = interaction_type
        # Simulate human approving after a short delay
        await asyncio.sleep(0.1)
        evaluator.resolve_permission(request_id, approved=True, reason="User approved")

    # Force escalation by making _call_claude return "escalate"
    async def mock_call_claude(prompt):
        return {"decision": "escalate", "reason": "unsure"}

    evaluator._call_claude = mock_call_claude

    result = await evaluator.evaluate_permission(
        server_name="local",
        session_id="dev",
        tool_name="Bash",
        tool_input={"command": "npm install"},
        send_escalation=capture_escalation,
    )
    assert result["approved"] is True
    assert escalated["type"] == "permission"


@pytest.mark.asyncio
async def test_permission_escalation_deny():
    """Permission escalation → human denies."""
    evaluator = PermissionEvaluator(model="haiku", config_path="config.yaml")

    async def deny_escalation(request_id, interaction_type, title, detail):
        await asyncio.sleep(0.1)
        evaluator.resolve_permission(request_id, approved=False, reason="Too risky")

    async def mock_call_claude(prompt):
        return {"decision": "escalate", "reason": "unsure"}

    evaluator._call_claude = mock_call_claude

    result = await evaluator.evaluate_permission(
        server_name="local",
        session_id="dev",
        tool_name="Bash",
        tool_input={"command": "rm -rf /tmp/stuff"},
        send_escalation=deny_escalation,
    )
    assert result["approved"] is False


@pytest.mark.asyncio
async def test_clarification_escalation():
    """Clarification escalation → human picks an option."""
    evaluator = PermissionEvaluator(model="haiku", config_path="config.yaml")

    questions = [{
        "question": "Which database should we use?",
        "header": "Database",
        "options": [
            {"label": "PostgreSQL", "description": "Relational DB"},
            {"label": "MongoDB", "description": "Document DB"},
        ],
        "multiSelect": False,
    }]

    async def answer_escalation(request_id, interaction_type, title, detail):
        assert interaction_type == "clarification"
        await asyncio.sleep(0.1)
        # Human picks PostgreSQL
        evaluator.resolve_clarification(
            request_id,
            question="Which database should we use?",
            answer="PostgreSQL",
        )

    # Make Claude say it can't answer
    async def mock_call_claude(prompt):
        return {"can_answer": False, "reason": "Design decision"}

    evaluator._call_claude = mock_call_claude

    result = await evaluator.evaluate_clarification(
        server_name="local",
        session_id="dev",
        questions=questions,
        send_escalation=answer_escalation,
    )
    assert result["answers"] is not None
    assert result["answers"]["Which database should we use?"] == "PostgreSQL"


@pytest.mark.asyncio
async def test_pending_count(evaluator):
    assert evaluator.pending_count == 0


@pytest.mark.asyncio
async def test_get_pending_questions_none(evaluator):
    assert evaluator.get_pending_questions("nonexistent") is None


# ── HTTP callback integration ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_http_permission_callback(aiohttp_client):
    """Simulate broker calling POST /permission on the orchestrator."""
    from src.orchestrator import Orchestrator
    from src.session import SessionManager, ServerConfig
    from src.store import Store

    store = Store(":memory:")
    await store.init()

    mgr = SessionManager(servers=[], default_model="haiku")
    await mgr.init()

    perm = PermissionEvaluator(model="haiku", config_path="config.yaml")

    # Mock Claude to auto-approve via evaluation
    async def mock_call_claude(prompt):
        return {"decision": "approve", "reason": "Looks safe"}
    perm._call_claude = mock_call_claude

    orch = Orchestrator(
        session_mgr=mgr,
        store=store,
        permission_evaluator=perm,
        model="haiku",
        config_path="config.yaml",
    )

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

    # Simulate broker calling /permission for Bash (Claude evaluates)
    resp = await client.post("/permission", json={
        "server_name": "local",
        "session_id": "dev",
        "tool_name": "Bash",
        "tool_input": {"command": "echo hello"},
    })
    assert resp.status == 200
    data = await resp.json()
    assert data["approved"] is True  # mock returns approve

    await mgr.close()
    await store.close()


# ── Helpers ────────────────────────────────────────────────────────────

class AsyncNoop:
    """Callable that does nothing, for send_escalation placeholder."""
    async def __call__(self, *args, **kwargs):
        pass
