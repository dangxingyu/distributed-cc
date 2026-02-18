"""Entry point: wire Router + WebChat + callback server.

The router is a thin relay between the web UI and remote orchestrator daemons.
It reads config.json for project/daemon configuration.

Usage:
  python -m src.main [--web-port 8080] [--http-port 9120] [--data-dir ./data]
"""

import argparse
import asyncio
import logging
import signal
import sys

from aiohttp import web

from .store import Store
from .router import Router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


# ── HTTP handlers for daemon callbacks ────────────────────────────────

async def handle_progress(request: web.Request) -> web.Response:
    """POST /progress — daemon sends progress events."""
    # This is handled by the SSE listener in the router, but daemons can also
    # push events directly via HTTP callback for reliability.
    router: Router = request.app["router"]
    try:
        data = await request.json()
        project_id = data.get("project_id", "")
        event = data.get("event", {})
        if router._progress_callback:
            await router._progress_callback(project_id, event)
        return web.json_response({"ok": True})
    except Exception as e:
        log.exception(f"Progress callback error: {e}")
        return web.json_response({"ok": False}, status=500)


async def start_callback_server(app: web.Application, port: int):
    """Start the HTTP callback server for daemon → router communication."""
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    log.info(f"Callback HTTP server on 127.0.0.1:{port}")
    return runner


# ── Main ──────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Distributed Claude Code — Router")
    parser.add_argument("--http-port", type=int, default=9120, help="Callback HTTP server port")
    parser.add_argument("--web-port", type=int, default=8080, help="Web chat frontend port")
    parser.add_argument("--web-host", default="127.0.0.1", help="Web chat frontend bind address")
    parser.add_argument("--data-dir", default="./data", help="Data directory for persistence")
    args = parser.parse_args()

    # Store
    store = Store(args.data_dir)
    await store.init()

    # Router
    router = Router()
    await router.init()

    # Callback HTTP server (daemon → router)
    http_app = web.Application()
    http_app["router"] = router
    http_app.router.add_post("/progress", handle_progress)
    http_runner = await start_callback_server(http_app, args.http_port)

    # Web frontend
    from .web import WebChat
    frontend = WebChat(
        router=router,
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

    # Cleanup
    await frontend.stop()
    await http_runner.cleanup()
    await router.close()
    await store.close()
    log.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
