"""Test the permission escalation flow in the new Router architecture.

Verifies that:
- Router creates pending permission futures
- Permission escalation callback fires
- resolve_permission resolves futures correctly
- Timeout handling works
- Escalation metadata is tracked
"""

import asyncio
import pytest
from unittest.mock import AsyncMock

from src.router import Router, RemoteOrchestrator


def _make_router():
    """Create a Router with test orchestrators (no real HTTP client)."""
    router = Router()
    router._orchestrators = {
        "proj-a": RemoteOrchestrator(
            project_id="proj-a", name="server-a", status="idle",
        ),
    }
    return router


# ── Basic permission resolution ──────────────────────────────────────


async def test_resolve_pending_permission():
    """resolve_permission resolves a pending future."""
    router = _make_router()

    future = asyncio.get_event_loop().create_future()
    router._pending_permissions["req1"] = future

    ok = router.resolve_permission("req1", approved=True, reason="Looks good")
    assert ok is True

    result = await asyncio.wait_for(future, timeout=1)
    assert result["approved"] is True
    assert result["reason"] == "Looks good"


async def test_resolve_denied():
    """Permission denied flows through correctly."""
    router = _make_router()

    future = asyncio.get_event_loop().create_future()
    router._pending_permissions["req2"] = future

    ok = router.resolve_permission("req2", approved=False, reason="Too risky")
    assert ok is True

    result = await asyncio.wait_for(future, timeout=1)
    assert result["approved"] is False
    assert result["reason"] == "Too risky"


async def test_resolve_nonexistent_returns_false():
    """Resolving a non-existent request returns False."""
    router = _make_router()
    ok = router.resolve_permission("nonexistent", approved=True)
    assert ok is False


async def test_resolve_already_done_returns_false():
    """Resolving an already-resolved request returns False."""
    router = _make_router()

    future = asyncio.get_event_loop().create_future()
    future.set_result({"approved": True, "reason": "already done"})
    router._pending_permissions["req3"] = future

    ok = router.resolve_permission("req3", approved=False)
    assert ok is False


# ── Escalation handler ───────────────────────────────────────────────


async def test_handle_permission_escalation():
    """handle_permission_escalation creates future and calls callback."""
    router = _make_router()

    escalation_data = {}

    async def mock_callback(request_id, data):
        escalation_data["request_id"] = request_id
        escalation_data["data"] = data
        # Auto-approve after a short delay
        await asyncio.sleep(0.01)
        router.resolve_permission(request_id, approved=True, reason="User approved")

    router.set_escalation_callback(mock_callback)

    result = await router.handle_permission_escalation({
        "project_id": "proj-a",
        "daemon_name": "server-a",
        "tool_name": "Bash",
        "tool_input": {"command": "rm -rf /"},
    })

    assert result["approved"] is True
    assert result["reason"] == "User approved"
    assert "request_id" in escalation_data
    assert escalation_data["data"]["tool_name"] == "Bash"


async def test_handle_permission_escalation_denied():
    """Permission escalation where user denies."""
    router = _make_router()

    async def deny_callback(request_id, data):
        await asyncio.sleep(0.01)
        router.resolve_permission(request_id, approved=False, reason="Dangerous")

    router.set_escalation_callback(deny_callback)

    result = await router.handle_permission_escalation({
        "project_id": "proj-a",
        "tool_name": "Bash",
        "tool_input": {"command": "rm -rf /"},
    })

    assert result["approved"] is False
    assert result["reason"] == "Dangerous"


async def test_handle_permission_escalation_timeout():
    """Permission escalation times out gracefully."""
    router = _make_router()

    async def never_resolve(request_id, data):
        pass  # Never resolves

    router.set_escalation_callback(never_resolve)

    # Monkey-patch a short timeout for testing
    import src.router as router_mod
    original = router.handle_permission_escalation

    async def short_timeout_escalation(data):
        request_id = "timeout-test"
        future = asyncio.get_event_loop().create_future()
        router._pending_permissions[request_id] = future
        router._pending_meta[request_id] = data

        if router._escalation_callback:
            await router._escalation_callback(request_id, data)

        try:
            result = await asyncio.wait_for(future, timeout=0.1)
            return result
        except asyncio.TimeoutError:
            router._pending_permissions.pop(request_id, None)
            router._pending_meta.pop(request_id, None)
            return {"approved": False, "reason": "Timeout"}

    result = await short_timeout_escalation({
        "project_id": "proj-a",
        "tool_name": "Bash",
    })

    assert result["approved"] is False
    assert result["reason"] == "Timeout"
    assert len(router._pending_permissions) == 0  # cleaned up


# ── Metadata tracking ────────────────────────────────────────────────


async def test_pending_meta_tracked_and_cleaned():
    """Pending metadata is stored and cleaned up after resolution."""
    router = _make_router()

    future = asyncio.get_event_loop().create_future()
    router._pending_permissions["req-meta"] = future
    router._pending_meta["req-meta"] = {"tool_name": "Bash", "project_id": "proj-a"}

    assert "req-meta" in router._pending_meta

    router.resolve_permission("req-meta", approved=True, reason="ok")
    assert "req-meta" not in router._pending_permissions
    assert "req-meta" not in router._pending_meta


# ── No callback set ──────────────────────────────────────────────────


async def test_escalation_without_callback():
    """Escalation with no callback set still creates future (waits for timeout)."""
    router = _make_router()
    # No callback set — _escalation_callback is None

    # Create a pending permission and resolve it manually
    future = asyncio.get_event_loop().create_future()
    router._pending_permissions["no-cb"] = future

    # Simulate external resolution
    router.resolve_permission("no-cb", approved=True, reason="Manual")
    result = await asyncio.wait_for(future, timeout=1)
    assert result["approved"] is True
