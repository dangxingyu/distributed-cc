#!/usr/bin/env python3
"""Systematic communication/setup diagnostics for local router <-> remote daemons.

Checks:
1) Endpoint health signature on each configured broker_port.
2) Project registration + status probe for each configured project.

Exit code:
  0 = all checks passed
  1 = one or more checks failed
  2 = invalid config / no targets
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp


@dataclass
class EndpointTarget:
    broker_port: int
    labels: set[str] = field(default_factory=set)


@dataclass
class ProjectTarget:
    project_id: str
    broker_port: int
    project_dir: str
    name: str
    source: str


@dataclass
class CheckResult:
    ok: bool
    target: str
    detail: str


def _as_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _add_endpoint(endpoint_map: dict[int, EndpointTarget], broker_port: int, label: str) -> None:
    target = endpoint_map.setdefault(broker_port, EndpointTarget(broker_port=broker_port))
    if label:
        target.labels.add(label)


def _collect_targets(cfg: dict[str, Any]) -> tuple[list[EndpointTarget], list[ProjectTarget]]:
    endpoint_map: dict[int, EndpointTarget] = {}
    projects: list[ProjectTarget] = []

    machines = cfg.get("machines", []) or []
    servers = cfg.get("servers", []) or []
    projects_cfg = cfg.get("projects", []) or []
    orchestrators = cfg.get("orchestrators", []) or []

    # Explicit orchestrator schema (highest precedence)
    if orchestrators:
        for o in orchestrators:
            project_id = str(o.get("project_id", "")).strip()
            if not project_id:
                continue
            broker_port = _as_int(o.get("broker_port", 8200), 8200)
            project_dir = str(o.get("project_dir", "")).strip()
            name = str(o.get("name", project_id)).strip() or project_id
            _add_endpoint(endpoint_map, broker_port, f"orchestrator:{project_id}")
            projects.append(
                ProjectTarget(
                    project_id=project_id,
                    broker_port=broker_port,
                    project_dir=project_dir,
                    name=name,
                    source="orchestrators",
                )
            )
        return list(endpoint_map.values()), projects

    machines_by_name: dict[str, dict[str, Any]] = {}
    for m in machines:
        machine_name = str(m.get("name", "")).strip()
        if not machine_name:
            continue
        machines_by_name[machine_name] = m
        _add_endpoint(
            endpoint_map,
            _as_int(m.get("broker_port", 8200), 8200),
            f"machine:{machine_name}",
        )

    servers_by_name: dict[str, dict[str, Any]] = {}
    for s in servers:
        server_name = str(s.get("name", "")).strip()
        if not server_name:
            continue
        servers_by_name[server_name] = s
        _add_endpoint(
            endpoint_map,
            _as_int(s.get("broker_port", 8200), 8200),
            f"server:{server_name}",
        )

    # Split schema projects
    for p in projects_cfg:
        project_id = str(p.get("project_id", "")).strip()
        if not project_id:
            continue
        machine_name = str(p.get("machine", "")).strip()
        server_name = str(p.get("server", "")).strip()
        base = machines_by_name.get(machine_name) or servers_by_name.get(server_name) or {}

        broker_port = _as_int(p.get("broker_port", base.get("broker_port", 8200)), 8200)
        project_dir = str(
            p.get(
                "work_dir",
                p.get("project_dir", base.get("work_dir", base.get("project_dir", ""))),
            )
        ).strip()
        name = str(p.get("name", project_id)).strip() or project_id
        _add_endpoint(endpoint_map, broker_port, f"project:{project_id}")
        projects.append(
            ProjectTarget(
                project_id=project_id,
                broker_port=broker_port,
                project_dir=project_dir,
                name=name,
                source="projects",
            )
        )

    # Legacy servers as projects when split schema projects are absent.
    if not projects_cfg:
        for s in servers:
            project_id = str(s.get("project_id", s.get("name", ""))).strip()
            if not project_id:
                continue
            broker_port = _as_int(s.get("broker_port", 8200), 8200)
            project_dir = str(s.get("work_dir", s.get("project_dir", ""))).strip()
            name = str(s.get("name", project_id)).strip() or project_id
            projects.append(
                ProjectTarget(
                    project_id=project_id,
                    broker_port=broker_port,
                    project_dir=project_dir,
                    name=name,
                    source="servers",
                )
            )

    return list(endpoint_map.values()), projects


def _validate_health_payload(payload: Any) -> tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "health payload is not JSON object"
    status = str(payload.get("status", "")).strip().lower()
    daemon_name = str(payload.get("daemon", "")).strip()
    if status != "ok":
        return False, f"status={payload.get('status')!r}"
    if not daemon_name:
        if "server" in payload:
            return False, "missing `daemon` (looks like legacy/foreign service)"
        return False, "missing `daemon`"
    return True, daemon_name


async def _check_endpoint_health(
    http: aiohttp.ClientSession,
    endpoint: EndpointTarget,
    timeout_seconds: float,
) -> CheckResult:
    labels = ", ".join(sorted(endpoint.labels)) or f"port:{endpoint.broker_port}"
    target = f":{endpoint.broker_port} [{labels}]"
    url = f"http://127.0.0.1:{endpoint.broker_port}/health"
    try:
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=timeout_seconds)) as resp:
            body = await resp.text()
            if resp.status != 200:
                return CheckResult(False, target, f"HTTP {resp.status}: {body[:160]}")
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                return CheckResult(False, target, "invalid JSON payload")
            ok, detail = _validate_health_payload(payload)
            if not ok:
                return CheckResult(False, target, detail)
            return CheckResult(True, target, f"daemon={detail}")
    except Exception as e:
        return CheckResult(False, target, f"request failed: {e}")


async def _check_project_registration(
    http: aiohttp.ClientSession,
    project: ProjectTarget,
    timeout_seconds: float,
) -> CheckResult:
    target = f"{project.project_id} (:{project.broker_port}, {project.source})"
    if not project.project_dir:
        return CheckResult(False, target, "missing project_dir/work_dir in config")

    base_url = f"http://127.0.0.1:{project.broker_port}"
    register_payload = {
        "project_id": project.project_id,
        "project_dir": project.project_dir,
        "name": project.name or project.project_id,
    }
    try:
        async with http.post(
            f"{base_url}/register",
            json=register_payload,
            timeout=aiohttp.ClientTimeout(total=timeout_seconds),
        ) as resp:
            body = await resp.text()
            if resp.status != 200:
                return CheckResult(False, target, f"/register HTTP {resp.status}: {body[:160]}")
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                return CheckResult(False, target, "/register returned non-JSON")
            if not isinstance(data, dict) or not data.get("ok"):
                return CheckResult(False, target, f"/register rejected: {data}")
    except Exception as e:
        return CheckResult(False, target, f"/register request failed: {e}")

    try:
        async with http.get(
            f"{base_url}/status",
            params={"project_id": project.project_id},
            timeout=aiohttp.ClientTimeout(total=timeout_seconds),
        ) as resp:
            body = await resp.text()
            if resp.status != 200:
                return CheckResult(False, target, f"/status HTTP {resp.status}: {body[:160]}")
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                return CheckResult(False, target, "/status returned non-JSON")
            if not isinstance(data, dict):
                return CheckResult(False, target, "/status payload is not object")
            status = str(data.get("status", "")).strip()
            if not status:
                return CheckResult(False, target, f"/status missing status: {data}")
            return CheckResult(True, target, f"registered, status={status}")
    except Exception as e:
        return CheckResult(False, target, f"/status request failed: {e}")


def _print_results(title: str, results: list[CheckResult]) -> None:
    print(f"\n== {title} ==")
    if not results:
        print("  (none)")
        return
    for r in results:
        prefix = "PASS" if r.ok else "FAIL"
        print(f"[{prefix}] {r.target} :: {r.detail}")


async def _run(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 2

    try:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Failed to read config: {e}", file=sys.stderr)
        return 2

    endpoints, projects = _collect_targets(cfg)
    if args.project:
        requested = {p.strip() for p in args.project if p.strip()}
        projects = [p for p in projects if p.project_id in requested]

    if not endpoints and not projects:
        print("No endpoints/projects found in config.", file=sys.stderr)
        return 2

    timeout_seconds = float(args.timeout)

    async with aiohttp.ClientSession() as http:
        endpoint_list = sorted(endpoints, key=lambda e: e.broker_port)
        endpoint_results = await asyncio.gather(
            *[
                _check_endpoint_health(http, endpoint, timeout_seconds)
                for endpoint in endpoint_list
            ]
        )
        healthy_ports = {
            endpoint.broker_port: result.ok
            for endpoint, result in zip(endpoint_list, endpoint_results)
        }

        project_results = await asyncio.gather(
            *[
                _check_project_registration(http, p, timeout_seconds)
                if healthy_ports.get(p.broker_port, False)
                else asyncio.sleep(0, result=CheckResult(False, f"{p.project_id} (:{p.broker_port}, {p.source})", "skipped: endpoint health failed"))
                for p in sorted(projects, key=lambda p: (p.broker_port, p.project_id))
            ]
        )

    _print_results("Endpoint Health", list(endpoint_results))
    _print_results("Project Register/Status", list(project_results))

    failed = [r for r in list(endpoint_results) + list(project_results) if not r.ok]
    if failed:
        print(f"\nSummary: {len(failed)} check(s) failed.")
        return 1

    print("\nSummary: all checks passed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose daemon/tunnel/project communication health.")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    parser.add_argument(
        "--project",
        action="append",
        default=[],
        help="Optional project_id filter (repeatable)",
    )
    parser.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout per check (seconds)")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
