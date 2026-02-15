#!/usr/bin/env python3
"""Register/unregister Claude Code sessions with the local broker.

Run this from a project directory to tell the broker about it.

Usage:
  broker-session start                    # register cwd with auto-generated name
  broker-session start --name my-project  # register with custom name
  broker-session start --desc "ML research"
  broker-session stop                     # unregister cwd's session
  broker-session stop --name my-project   # unregister by name
  broker-session list                     # show all registered sessions
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error


def _broker_url(port: int) -> str:
    return f"http://127.0.0.1:{port}"


def _post(url: str, data: dict) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError as e:
        print(f"Error: Cannot reach broker at {url}: {e}", file=sys.stderr)
        sys.exit(1)


def _get(url: str) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError as e:
        print(f"Error: Cannot reach broker at {url}: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_start(args):
    work_dir = os.path.abspath(".")
    session_id = args.name or os.path.basename(work_dir)
    description = args.desc or ""

    result = _post(f"{_broker_url(args.port)}/register", {
        "session_id": session_id,
        "work_dir": work_dir,
        "description": description,
    })

    if result.get("ok"):
        print(f"Registered session '{session_id}' at {work_dir}")
    else:
        print(f"Failed: {result}", file=sys.stderr)
        sys.exit(1)


def cmd_stop(args):
    work_dir = os.path.abspath(".")
    session_id = args.name or os.path.basename(work_dir)

    result = _post(f"{_broker_url(args.port)}/unregister", {
        "session_id": session_id,
    })

    if result.get("ok"):
        print(f"Unregistered session '{session_id}'")
    else:
        print(f"Failed: {result.get('reason', 'unknown')}", file=sys.stderr)
        sys.exit(1)


def cmd_list(args):
    sessions = _get(f"{_broker_url(args.port)}/sessions")
    if not sessions:
        print("No sessions registered.")
        return

    for s in sessions:
        status = s.get("status", "?")
        sid = s["session_id"]
        wd = s.get("work_dir", "?")
        desc = s.get("description", "")
        line = f"  {sid:20s}  [{status:7s}]  {wd}"
        if desc:
            line += f"  ({desc})"
        print(line)


def main():
    parser = argparse.ArgumentParser(
        description="Register sessions with the local broker",
        prog="broker-session",
    )
    parser.add_argument("--port", type=int, default=8200, help="Broker port")
    sub = parser.add_subparsers(dest="command")

    start_p = sub.add_parser("start", help="Register current directory as a session")
    start_p.add_argument("--name", default="", help="Session name (default: directory basename)")
    start_p.add_argument("--desc", default="", help="Description")

    stop_p = sub.add_parser("stop", help="Unregister a session")
    stop_p.add_argument("--name", default="", help="Session name (default: directory basename)")

    sub.add_parser("list", help="List registered sessions")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    {"start": cmd_start, "stop": cmd_stop, "list": cmd_list}[args.command](args)


if __name__ == "__main__":
    main()
