"""Entry point: wire everything together and run.

Supports three frontend modes:
  --web       Web chat UI on localhost (default)
  --cli       Terminal REPL
  --telegram  Telegram bot (requires TELEGRAM_TOKEN env var)

Priority: --web (default) > --telegram > --cli.
All modes start the HTTP callback server for broker permission/clarification.

The orchestrator reads config.json from the working directory on init
to discover servers, model preferences, and tool permissions.
"""

import argparse
import asyncio
import logging
import os
import signal
import sys

from aiohttp import web

from .store import Store
from .session import SessionManager
from .orchestrator import Orchestrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


# ── HTTP handlers for broker callbacks ─────────────────────────────────

async def handle_permission(request: web.Request) -> web.Response:
    orchestrator: Orchestrator = request.app["orchestrator"]
    try:
        data = await request.json()
        log.info(f"Permission: {data.get('tool_name')} from {data.get('server_name')}/{data.get('session_id')}")
        result = await orchestrator.handle_permission_request(data)
        return web.json_response(result)
    except Exception as e:
        log.exception(f"Permission error: {e}")
        return web.json_response({"approved": False, "reason": str(e)}, status=500)


async def handle_clarification(request: web.Request) -> web.Response:
    orchestrator: Orchestrator = request.app["orchestrator"]
    try:
        data = await request.json()
        log.info(f"Clarification from {data.get('server_name')}/{data.get('session_id')}")
        result = await orchestrator.handle_clarification_request(data)
        return web.json_response(result)
    except Exception as e:
        log.exception(f"Clarification error: {e}")
        return web.json_response({"answers": None, "reason": str(e)}, status=500)



async def start_http_server(app: web.Application, port: int):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    log.info(f"Callback HTTP server on 127.0.0.1:{port}")
    return runner


# ── Main ───────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Claude Code Orchestrator")
    parser.add_argument("--web", action="store_true", default=True, help="Use web chat frontend (default)")
    parser.add_argument("--cli", action="store_true", help="Use CLI REPL frontend")
    parser.add_argument("--telegram", action="store_true", help="Use Telegram bot frontend")
    parser.add_argument("--http-port", type=int, default=9120, help="Callback HTTP server port")
    parser.add_argument("--web-port", type=int, default=8080, help="Web chat frontend port")
    parser.add_argument("--web-host", default="127.0.0.1", help="Web chat frontend bind address")
    parser.add_argument("--data-dir", default="./data", help="Data directory for persistence")
    args = parser.parse_args()

    # Priority: --cli/--telegram override web default
    use_web = not args.cli and not args.telegram
    use_telegram = args.telegram

    # Store
    store = Store(args.data_dir)
    await store.init()

    # Session manager (starts empty — orchestrator registers servers from config.json)
    session_mgr = SessionManager(servers=[])
    await session_mgr.init()

    # Orchestrator (reads config.json from cwd for servers, model, permissions)
    orchestrator = Orchestrator(
        session_mgr=session_mgr,
        store=store,
    )
    await orchestrator.init()

    # HTTP server for broker callbacks (always runs)
    http_app = web.Application()
    http_app["orchestrator"] = orchestrator
    http_app.router.add_post("/permission", handle_permission)
    http_app.router.add_post("/clarification", handle_clarification)
    http_runner = await start_http_server(http_app, args.http_port)

    # Frontend
    frontend = None
    if use_web:
        from .web import WebChat
        frontend = WebChat(
            orchestrator=orchestrator,
            store=store,
            host=args.web_host,
            port=args.web_port,
        )
        await frontend.start()
        log.info("Running with Web frontend. Ctrl+C to stop.")

        # Wait for signal
        stop_event = asyncio.Event()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        await stop_event.wait()
        await frontend.stop()
    elif use_telegram:
        from .bot import Bot
        token = os.environ.get("TELEGRAM_TOKEN", "")
        allowed = os.environ.get("TELEGRAM_ALLOWED_USERS", "")
        if not token:
            log.error("Telegram mode requires TELEGRAM_TOKEN env var")
            sys.exit(1)
        allowed_users = [int(u) for u in allowed.split(",") if u.strip()]
        frontend = Bot(
            token=token,
            allowed_users=allowed_users,
            orchestrator=orchestrator,
        )
        await frontend.start()
        log.info("Running with Telegram frontend. Ctrl+C to stop.")

        # Wait for signal
        stop_event = asyncio.Event()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        await stop_event.wait()
        await frontend.stop()
    else:
        from .cli import CLI
        frontend = CLI(orchestrator=orchestrator)
        try:
            await frontend.run()
        except (KeyboardInterrupt, EOFError):
            pass

    # Cleanup
    await http_runner.cleanup()
    await session_mgr.close()
    await store.close()
    log.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
