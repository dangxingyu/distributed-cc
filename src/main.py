"""Entry point: wire Router + user frontend + callback server.

The router is a thin relay between the web UI and remote orchestrator daemons.
It reads config.json for project/daemon configuration.

Usage:
  python -m src.main [--frontend web|telegram] [--web-port 8080] [--http-port 9120] [--data-dir ./data]
"""

import argparse
import asyncio
import logging
import os
import signal

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
    router: Router = request.app["router"]
    try:
        data = await request.json()
        project_id = data.get("project_id", "")
        event = data.get("event", {})
        await router.ingest_progress_event(project_id, event, source="callback")
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
    parser = argparse.ArgumentParser(description="distributed-cc — Router")
    parser.add_argument(
        "--frontend",
        choices=["web", "telegram", "both"],
        default="web",
        help="User-facing frontend (both = web + telegram simultaneously)",
    )
    parser.add_argument("--http-port", type=int, default=9120, help="Callback HTTP server port")
    parser.add_argument("--web-port", type=int, default=8080, help="Web chat frontend port")
    parser.add_argument("--web-host", default="127.0.0.1", help="Web chat frontend bind address")
    parser.add_argument(
        "--telegram-token",
        default=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        help="Telegram bot token (or set TELEGRAM_BOT_TOKEN)",
    )
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

    # User frontend(s)
    frontends = []

    try:
        if args.frontend in ("web", "both"):
            from .web import WebChat

            web_frontend = WebChat(
                router=router,
                store=store,
                host=args.web_host,
                port=args.web_port,
            )
            frontends.append(web_frontend)
            await web_frontend.start()
            log.info("Web frontend on http://%s:%s", args.web_host, args.web_port)

        if args.frontend in ("telegram", "both"):
            from .telegram_chat import TelegramChat

            tg_frontend = TelegramChat(
                router=router,
                store=store,
                token=args.telegram_token,
            )
            frontends.append(tg_frontend)
            await tg_frontend.start()
            log.info("Telegram frontend started.")

        log.info("Running with %s frontend(s). Ctrl+C to stop.", args.frontend)

        # Wait for signal
        stop_event = asyncio.Event()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        await stop_event.wait()
    finally:
        for frontend in reversed(frontends):
            try:
                await frontend.stop()
            except Exception:
                log.warning("Frontend shutdown failed", exc_info=True)

        await http_runner.cleanup()
        await router.close()
        await store.close()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
