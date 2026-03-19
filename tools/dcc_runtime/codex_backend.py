from __future__ import annotations

import asyncio
import contextlib
import json
import shutil
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
from mcp.server.fastmcp import FastMCP

from .base import EventSink, RuntimeEvent, RuntimeRequest, RuntimeResult, ToolSpec, extract_tool_text


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(sock.getsockname()[1])


def _toml_literal(value: Any) -> str:
    if value is None:
        return '""'
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_literal(item) for item in value) + "]"
    if isinstance(value, dict):
        parts = [f"{key} = {_toml_literal(val)}" for key, val in value.items()]
        return "{" + ", ".join(parts) + "}"
    return json.dumps(str(value), ensure_ascii=False)


def build_codex_override_args(plugin_mcp_servers: dict[str, dict[str, Any]]) -> list[str]:
    overrides: list[str] = []
    servers = dict(plugin_mcp_servers)

    for name, cfg in servers.items():
        if not isinstance(cfg, dict):
            continue
        prefix = f"mcp_servers.{name}"
        for key, value in cfg.items():
            if value is None:
                continue
            overrides.append(f"{prefix}.{key}={_toml_literal(value)}")
    return overrides


async def _wait_for_http_ready(url: str, timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=1.0)) as resp:
                    if resp.status < 500:
                        return
            except Exception:
                pass
            if asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError(f"Timed out waiting for {url}")
            await asyncio.sleep(0.1)


async def _terminate_process(proc: asyncio.subprocess.Process | None) -> str:
    if proc is None:
        return ""
    if proc.returncode is None:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
    stderr = ""
    if proc.stderr is not None:
        with contextlib.suppress(Exception):
            stderr = (await proc.stderr.read()).decode("utf-8", errors="replace")
    return stderr.strip()


def _build_fastmcp_tool(spec: ToolSpec):
    async def _call(payload: dict[str, Any]):
        result = await spec.handler(payload)
        text = extract_tool_text(result)
        if result.get("is_error"):
            raise RuntimeError(text or f"{spec.name} failed")
        return text

    namespace = {"_call": _call, "str": str, "int": int, "float": float, "bool": bool}
    params = ", ".join(f"{name}: {kind.__name__}" for name, kind in spec.input_schema.items())
    call_args = ", ".join(f'"{name}": {name}' for name in spec.input_schema)
    if params:
        src = (
            f"async def {spec.name}({params}):\n"
            f"    return await _call({{{call_args}}})\n"
        )
    else:
        src = f"async def {spec.name}():\n    return await _call({{}})\n"
    exec(src, namespace)
    fn = namespace[spec.name]
    fn.__name__ = spec.name
    fn.__doc__ = spec.description
    return fn


@dataclass(slots=True)
class _CodexTurnTracker:
    source: str
    on_event: EventSink
    final_text: str = ""
    turn_status: str = ""
    turn_error: str = ""
    agent_messages: dict[str, str] = field(default_factory=dict)

    async def handle(self, message: dict[str, Any]) -> None:
        method = str(message.get("method") or "")
        params = message.get("params") or {}

        if method == "item/agentMessage/delta":
            item_id = str(params.get("itemId") or "")
            delta = str(params.get("delta") or "")
            if item_id and delta:
                self.agent_messages[item_id] = self.agent_messages.get(item_id, "") + delta
            return

        if method == "item/started":
            item = params.get("item") or {}
            item_type = str(item.get("type") or "")
            if item_type == "mcpToolCall":
                tool = str(item.get("tool") or "")
                server = str(item.get("server") or "")
                args = item.get("arguments")
                prefix = f"{server}.{tool}" if server else tool
                data = f"[{self.source}] {prefix}"
                if args not in ({}, None):
                    data += f": {json.dumps(args, ensure_ascii=False)}"
                await self.on_event(RuntimeEvent(type="tool_use", data=data))
            return

        if method == "item/completed":
            item = params.get("item") or {}
            item_type = str(item.get("type") or "")
            if item_type == "agentMessage":
                text = str(item.get("text") or self.agent_messages.get(str(item.get("id") or ""), "")).strip()
                if text:
                    self.final_text = text
                    await self.on_event(RuntimeEvent(type="text", data=f"[{self.source}] {text}"))
            elif item_type == "mcpToolCall":
                error = item.get("error")
                if error:
                    await self.on_event(
                        RuntimeEvent(type="tool_error", data=f"[{self.source}] {error}")
                    )
            return

        if method == "error":
            error = params.get("error") or {}
            message_text = str(error.get("message") or "").strip()
            if not message_text:
                return
            if params.get("willRetry"):
                await self.on_event(
                    RuntimeEvent(type="log_update", data=f"[{self.source} system] {message_text}")
                )
            else:
                self.turn_error = message_text
                await self.on_event(
                    RuntimeEvent(type="tool_error", data=f"[{self.source}] {message_text}")
                )
            return

        if method == "turn/completed":
            turn = params.get("turn") or {}
            self.turn_status = str(turn.get("status") or "")
            error = turn.get("error") or {}
            if isinstance(error, dict):
                self.turn_error = str(error.get("message") or self.turn_error or "")
            elif error:
                self.turn_error = str(error)


async def _recv_json(ws: aiohttp.ClientWebSocketResponse) -> dict[str, Any]:
    while True:
        msg = await ws.receive()
        if msg.type == aiohttp.WSMsgType.TEXT:
            return json.loads(msg.data)
        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
            raise RuntimeError("Codex app-server closed the websocket")
        if msg.type == aiohttp.WSMsgType.ERROR:
            raise RuntimeError(f"Codex websocket error: {ws.exception()}")


async def _send_json(ws: aiohttp.ClientWebSocketResponse, payload: dict[str, Any]) -> None:
    await ws.send_str(json.dumps(payload, ensure_ascii=False))


async def _request_response(
    ws: aiohttp.ClientWebSocketResponse,
    request_id: int,
    method: str,
    params: dict[str, Any],
    tracker: _CodexTurnTracker | None = None,
) -> dict[str, Any]:
    await _send_json(ws, {"id": request_id, "method": method, "params": params})
    while True:
        message = await _recv_json(ws)
        if tracker is not None and "method" in message:
            await tracker.handle(message)
        if message.get("id") == request_id:
            return message


async def run_turn(request: RuntimeRequest, on_event: EventSink) -> RuntimeResult:
    if not shutil.which("codex"):
        raise RuntimeError("Codex CLI is not installed or not on PATH")

    ws_port = _find_free_port()
    mcp_port = _find_free_port() if request.tool_specs else None
    mcp_task: asyncio.Task | None = None
    proc: asyncio.subprocess.Process | None = None

    if request.tool_specs:
        daemon_mcp = FastMCP(
            name=f"dcc-{request.source}",
            host="127.0.0.1",
            port=mcp_port,
            log_level="ERROR",
        )
        for spec in request.tool_specs:
            daemon_mcp.add_tool(
                _build_fastmcp_tool(spec),
                name=spec.name,
                description=spec.description,
            )
        mcp_task = asyncio.create_task(daemon_mcp.run_streamable_http_async())
        await asyncio.sleep(0.3)

    daemon_mcp_name = "worker_tools" if request.source == "worker" else "daemon"
    daemon_mcp_url = f"http://127.0.0.1:{mcp_port}/mcp" if mcp_port is not None else None
    cmd = ["codex", "app-server", "--listen", f"ws://127.0.0.1:{ws_port}"]
    daemon_override_servers = dict(request.plugin_mcp_servers)
    if daemon_mcp_url:
        daemon_override_servers[daemon_mcp_name] = {"url": daemon_mcp_url}
    for override in build_codex_override_args(daemon_override_servers):
        cmd.extend(["-c", override])

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=request.project_dir,
        )
        await _wait_for_http_ready(f"http://127.0.0.1:{ws_port}/readyz")

        tracker = _CodexTurnTracker(source=request.source, on_event=on_event)
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{ws_port}") as ws:
                init_resp = await _request_response(
                    ws,
                    request_id=1,
                    method="initialize",
                    params={"clientInfo": {"name": "distributed-cc", "version": "0.1"}},
                )
                if "result" not in init_resp:
                    raise RuntimeError(f"Codex initialize failed: {init_resp}")

                active_model = request.session_model if request.session_id and request.session_model else request.model
                thread_params: dict[str, Any] = {
                    "threadId": request.session_id,
                    "cwd": request.project_dir,
                    "developerInstructions": request.system_prompt,
                    "approvalPolicy": request.approval_policy,
                    "sandbox": request.sandbox_mode,
                }
                if active_model:
                    thread_params["model"] = active_model

                if request.session_id:
                    thread_resp = await _request_response(
                        ws,
                        request_id=2,
                        method="thread/resume",
                        params=thread_params,
                        tracker=tracker,
                    )
                else:
                    thread_params.pop("threadId", None)
                    thread_resp = await _request_response(
                        ws,
                        request_id=2,
                        method="thread/start",
                        params=thread_params,
                        tracker=tracker,
                    )

                try:
                    thread_id = str(thread_resp["result"]["thread"]["id"])
                except Exception as exc:
                    raise RuntimeError(f"Codex thread start failed: {thread_resp}") from exc

                turn_resp = await _request_response(
                    ws,
                    request_id=3,
                    method="turn/start",
                    params={
                        "threadId": thread_id,
                        "input": [{"type": "text", "text": request.prompt}],
                    },
                    tracker=tracker,
                )
                if "result" not in turn_resp:
                    raise RuntimeError(f"Codex turn start failed: {turn_resp}")

                while True:
                    message = await _recv_json(ws)
                    if "method" in message:
                        await tracker.handle(message)
                    if message.get("method") == "turn/completed":
                        break

        if tracker.turn_status and tracker.turn_status != "completed":
            raise RuntimeError(tracker.turn_error or f"Codex turn failed with status={tracker.turn_status}")
        return RuntimeResult(session_id=thread_id, final_text=tracker.final_text, saw_result=True)
    finally:
        stderr_text = await _terminate_process(proc)
        if mcp_task is not None:
            mcp_task.cancel()
            with contextlib.suppress(BaseException):
                await mcp_task
        if proc is not None and proc.returncode not in (0, None) and stderr_text:
            last_line = stderr_text.strip().splitlines()[-1]
            if last_line and "Address already in use" not in last_line:
                await on_event(RuntimeEvent(type="log_update", data=f"[{request.source} system] codex app-server: {last_line}"))
