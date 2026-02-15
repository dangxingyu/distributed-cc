#!/usr/bin/env python3
"""Remote broker — runs on each server as a persistent daemon.

Uses the Claude Agent SDK to execute tasks. The agent runs with full
autonomy (no turn/budget caps). When it proactively asks for permission
or clarification, those are forwarded to the orchestrator via HTTP
(reachable through SSH reverse tunnel).

Usage:
  python3 remote_broker.py --port 8200 --work-dir /path/to/project

Environment:
  ORCH_URL     — orchestrator callback URL (default: http://127.0.0.1:9120)
  SERVER_NAME  — this server's name in config (default: from --name flag)

Deploy via: tools/deploy.sh user@host
"""

import argparse
import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass

from aiohttp import web, ClientSession, ClientTimeout

from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage
from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("broker")

ORCHESTRATOR_URL = os.environ.get("ORCH_URL", "http://127.0.0.1:9120")
SERVER_NAME = os.environ.get("SERVER_NAME", "unknown")
WORK_DIR = "."


# ── Session tracking ───────────────────────────────────────────────────

@dataclass
class RunningSession:
    session_id: str             # logical session name (e.g. "dev")
    sdk_session_id: str = ""    # actual Claude session ID for --resume
    started_at: float = 0
    status: str = "idle"


sessions: dict[str, RunningSession] = {}


# ── canUseTool callback ───────────────────────────────────────────────

def make_can_use_tool(session_id: str, http: ClientSession):
    """Create a canUseTool callback bound to a specific session.

    This is the agent's own permission/clarification mechanism:
    - AskUserQuestion → forwarded as clarification
    - Other tool calls → forwarded as permission request
    Both are the agent proactively pausing to ask.
    """

    async def can_use_tool(tool_name: str, input_data: dict, context=None):
        if tool_name == "AskUserQuestion":
            return await _handle_clarification(session_id, input_data, http)
        return await _handle_permission(session_id, tool_name, input_data, http)

    return can_use_tool


async def _handle_permission(session_id: str, tool_name: str, input_data: dict, http: ClientSession):
    """Forward tool permission request to orchestrator."""
    try:
        async with http.post(
            f"{ORCHESTRATOR_URL}/permission",
            json={
                "server_name": SERVER_NAME,
                "session_id": session_id,
                "tool_name": tool_name,
                "tool_input": _safe_serialize(input_data),
            },
            timeout=ClientTimeout(total=300),
        ) as resp:
            result = await resp.json()
            if result.get("approved"):
                return PermissionResultAllow(updated_input=input_data)
            return PermissionResultDeny(
                message=result.get("reason", "Denied by orchestrator")
            )
    except Exception as e:
        log.error(f"Permission callback failed: {e}")
        return PermissionResultDeny(message=f"Cannot reach orchestrator: {e}")


async def _handle_clarification(session_id: str, input_data: dict, http: ClientSession):
    """Forward AskUserQuestion to orchestrator, return with answers."""
    try:
        async with http.post(
            f"{ORCHESTRATOR_URL}/clarification",
            json={
                "server_name": SERVER_NAME,
                "session_id": session_id,
                "questions": input_data.get("questions", []),
            },
            timeout=ClientTimeout(total=300),
        ) as resp:
            result = await resp.json()

            if result.get("answers"):
                return PermissionResultAllow(updated_input={
                    "questions": input_data.get("questions", []),
                    "answers": result["answers"],
                })
            return PermissionResultDeny(
                message=result.get("reason", "Clarification declined")
            )
    except Exception as e:
        log.error(f"Clarification callback failed: {e}")
        return PermissionResultDeny(message=f"Cannot reach orchestrator: {e}")


# ── Agent SDK runner ───────────────────────────────────────────────────

async def _prompt_stream(prompt: str):
    """Wrap a string prompt into an AsyncIterable for streaming mode.

    The SDK requires AsyncIterable when can_use_tool is set.
    """
    yield {
        "type": "user",
        "session_id": "",
        "message": {"role": "user", "content": prompt},
        "parent_tool_use_id": None,
    }


async def run_agent_task(
    session_id: str,
    prompt: str,
    model: str = "claude-opus-4-6",
) -> dict:
    """Run an Agent SDK query and return the result.

    The agent runs with full autonomy — no turn limits, no budget caps.
    It stops when the task is done or when it asks for clarification.
    Uses --resume to maintain session context across calls.
    """
    # Clear nested-session guard so SDK can spawn claude subprocess
    os.environ.pop("CLAUDECODE", None)

    http = ClientSession(timeout=ClientTimeout(total=None))
    try:
        # Check if we have a previous SDK session to resume
        sess = sessions.get(session_id)
        sdk_session_id = sess.sdk_session_id if sess and sess.sdk_session_id else None

        options = ClaudeAgentOptions(
            can_use_tool=make_can_use_tool(session_id, http),
            model=model,
            cwd=WORK_DIR,
        )
        if sdk_session_id:
            options.resume = sdk_session_id

        result_text = ""
        final_session_id = ""
        cost_usd = 0.0

        # SDK requires AsyncIterable prompt when can_use_tool is set
        async for message in query(prompt=_prompt_stream(prompt), options=options):
            if isinstance(message, ResultMessage):
                result_text = message.result or ""
                final_session_id = message.session_id
                cost_usd = message.total_cost_usd or 0.0

        # Save SDK session ID for future --resume
        if final_session_id and sess:
            sess.sdk_session_id = final_session_id

        return {
            "session_id": final_session_id or session_id,
            "result": result_text,
            "cost_usd": cost_usd,
        }
    finally:
        await http.close()


# ── HTTP handlers ──────────────────────────────────────────────────────

async def handle_run(request: web.Request) -> web.Response:
    """POST /run — execute a prompt in a Claude Code session."""
    data = await request.json()
    session_id = data["session_id"]
    prompt = data["prompt"]

    # Track the session
    sess = sessions.setdefault(session_id, RunningSession(session_id=session_id))
    if sess.status == "running":
        return web.json_response(
            {"error": f"Session {session_id} is already running a task"},
            status=409,
        )

    sess.status = "running"
    sess.started_at = time.time()

    try:
        result = await run_agent_task(
            session_id=session_id,
            prompt=prompt,
            model=data.get("model", "claude-opus-4-6"),
        )
        result["duration_secs"] = time.time() - sess.started_at
        sess.status = "idle"
        return web.json_response(result)

    except asyncio.CancelledError:
        sess.status = "idle"
        return web.json_response({"error": "Task cancelled"}, status=499)
    except Exception as e:
        log.exception(f"Task failed for session {session_id}: {e}")
        sess.status = "idle"
        return web.json_response({"error": str(e)}, status=500)


async def handle_sessions(request: web.Request) -> web.Response:
    """GET /sessions — list tracked sessions."""
    result = []
    for s in sessions.values():
        result.append({
            "session_id": s.session_id,
            "sdk_session_id": s.sdk_session_id,
            "status": s.status,
            "started_at": s.started_at,
        })
    return web.json_response(result)


async def handle_kill(request: web.Request) -> web.Response:
    """POST /kill — cancel a running task."""
    data = await request.json()
    session_id = data.get("session_id")
    sess = sessions.get(session_id)
    if sess and sess.status == "running":
        # TODO: need ClaudeSDKClient.interrupt() for proper cancellation
        sess.status = "idle"
        return web.json_response({"ok": True, "note": "marked idle, task may still run"})
    return web.json_response({"ok": False, "reason": "No running task found"})


async def handle_health(request: web.Request) -> web.Response:
    """GET /health — health check."""
    return web.json_response({"status": "ok", "server": SERVER_NAME})


# ── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Claude Code remote broker")
    parser.add_argument("--port", type=int, default=8200)
    parser.add_argument("--work-dir", default=".")
    parser.add_argument("--name", default=os.environ.get("SERVER_NAME", "unknown"))
    args = parser.parse_args()

    global SERVER_NAME, WORK_DIR
    SERVER_NAME = args.name
    WORK_DIR = os.path.abspath(args.work_dir)
    os.environ["SERVER_NAME"] = args.name

    os.chdir(WORK_DIR)
    log.info(f"Broker starting: server={args.name}, work_dir={WORK_DIR}, port={args.port}")

    app = web.Application()
    app.router.add_post("/run", handle_run)
    app.router.add_get("/sessions", handle_sessions)
    app.router.add_post("/kill", handle_kill)
    app.router.add_get("/health", handle_health)

    web.run_app(app, host="127.0.0.1", port=args.port)


def _safe_serialize(obj) -> dict:
    """Best-effort JSON-safe conversion."""
    try:
        json.dumps(obj)
        return obj
    except (TypeError, ValueError):
        return {"raw": str(obj)}


if __name__ == "__main__":
    main()
