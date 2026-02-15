"""Remote (and local) Claude Code session manager.

Makes HTTP calls to remote brokers (tools/remote_broker.py) which use
the Agent SDK to run tasks. Brokers are reachable via SSH port forwarding.

Sessions can be:
  - Pre-configured in config.yaml (static)
  - Registered dynamically via broker heartbeats (dynamic)
"""

import asyncio
import json
import logging
from dataclasses import dataclass

import aiohttp

log = logging.getLogger(__name__)


@dataclass
class SessionResult:
    session_id: str
    result_text: str
    is_error: bool = False
    cost_usd: float = 0.0
    duration_secs: float = 0.0


@dataclass
class ServerConfig:
    name: str
    host: str | None          # SSH destination (null = local)
    broker_port: int = 8200   # Local port forwarded to remote broker
    ssh_options: str = ""
    work_dir: str = ""        # optional server-level fallback


@dataclass
class RemoteSession:
    """A session registered with a remote broker."""
    session_id: str
    work_dir: str
    description: str = ""
    status: str = "idle"


class SessionManager:
    """Manages Claude Code sessions via remote brokers."""

    def __init__(
        self,
        servers: list[ServerConfig],
        default_model: str = "claude-opus-4-6",
    ):
        self._servers = {s.name: s for s in servers}
        self._model = default_model
        self._http: aiohttp.ClientSession | None = None
        # Dynamic sessions reported by broker heartbeats
        # key: server_name → {session_id → RemoteSession}
        self._remote_sessions: dict[str, dict[str, RemoteSession]] = {}

    async def init(self):
        self._http = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None)  # no timeout — agent decides when to stop
        )

    async def close(self):
        if self._http:
            await self._http.close()

    def get_server(self, name: str) -> ServerConfig | None:
        return self._servers.get(name)

    def list_servers(self) -> list[ServerConfig]:
        return list(self._servers.values())

    def _broker_url(self, server: ServerConfig) -> str:
        return f"http://127.0.0.1:{server.broker_port}"

    # ── Dynamic session management ────────────────────────────────────

    def update_sessions(self, server_name: str, sessions: list[dict]):
        """Update session registry from broker heartbeat."""
        self._remote_sessions[server_name] = {
            s["session_id"]: RemoteSession(
                session_id=s["session_id"],
                work_dir=s["work_dir"],
                description=s.get("description", ""),
                status=s.get("status", "idle"),
            )
            for s in sessions
        }
        log.info(
            f"Updated sessions for {server_name}: "
            f"{[s['session_id'] for s in sessions]}"
        )

    def list_sessions(self) -> dict[str, list[RemoteSession]]:
        """All known sessions across all servers."""
        return {
            name: list(sess.values())
            for name, sess in self._remote_sessions.items()
        }

    def get_session(self, server_name: str, session_id: str) -> RemoteSession | None:
        """Look up a specific remote session."""
        server_sessions = self._remote_sessions.get(server_name, {})
        return server_sessions.get(session_id)

    # ── Task execution ────────────────────────────────────────────────

    async def run_task(
        self,
        server_name: str,
        session_id: str,
        prompt: str,
        task_id: int | None = None,
    ) -> SessionResult:
        """Execute a prompt via the remote broker."""
        server = self._servers.get(server_name)
        if not server:
            return SessionResult(
                session_id=session_id,
                result_text=f"Unknown server: {server_name}",
                is_error=True,
            )

        url = f"{self._broker_url(server)}/run"
        payload = {
            "session_id": session_id,
            "prompt": prompt,
            "model": self._model,
        }

        # Include work_dir from session registry or server config
        remote_sess = self.get_session(server_name, session_id)
        if remote_sess:
            payload["work_dir"] = remote_sess.work_dir
        elif server.work_dir:
            payload["work_dir"] = server.work_dir

        log.info(f"Running task on {server_name}/{session_id}")

        try:
            async with self._http.post(url, json=payload) as resp:
                if resp.status == 409:
                    return SessionResult(
                        session_id=session_id,
                        result_text=f"Session {session_id} is busy (already running a task)",
                        is_error=True,
                    )
                if resp.status >= 400:
                    body = await resp.text()
                    return SessionResult(
                        session_id=session_id,
                        result_text=f"Broker error (HTTP {resp.status}): {body[:2000]}",
                        is_error=True,
                    )

                data = await resp.json()

                if "error" in data:
                    return SessionResult(
                        session_id=data.get("session_id", session_id),
                        result_text=data["error"],
                        is_error=True,
                    )

                return SessionResult(
                    session_id=data.get("session_id", session_id),
                    result_text=data.get("result", ""),
                    cost_usd=data.get("cost_usd", 0),
                    duration_secs=data.get("duration_secs", 0),
                )

        except aiohttp.ClientError as e:
            log.error(f"Cannot reach broker for {server_name}: {e}")
            return SessionResult(
                session_id=session_id,
                result_text=f"Cannot reach broker for {server_name} (port {server.broker_port}): {e}",
                is_error=True,
            )
        except asyncio.TimeoutError:
            return SessionResult(
                session_id=session_id,
                result_text="Task timed out (HTTP level)",
                is_error=True,
            )

    async def cancel_task(self, server_name: str, session_id: str) -> bool:
        """Cancel a running task on a remote broker."""
        server = self._servers.get(server_name)
        if not server:
            return False
        try:
            url = f"{self._broker_url(server)}/kill"
            async with self._http.post(url, json={"session_id": session_id}) as resp:
                data = await resp.json()
                return data.get("ok", False)
        except Exception:
            return False

    async def check_health(self, server_name: str) -> bool:
        """Check if a remote broker is reachable."""
        server = self._servers.get(server_name)
        if not server:
            return False
        try:
            url = f"{self._broker_url(server)}/health"
            async with self._http.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                return resp.status == 200
        except Exception:
            return False
