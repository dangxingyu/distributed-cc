"""Entry point: wire everything together and run.

Supports three frontend modes:
  --web       Web chat UI on localhost (default)
  --cli       Terminal REPL
  --telegram  Telegram bot (requires token in config)

Priority: --web (default) > --telegram > --cli.
All modes start the HTTP callback server for broker permission/clarification.
"""

import argparse
import asyncio
import logging
import signal
import sys

import yaml
from aiohttp import web

from .store import Store
from .session import SessionManager, ServerConfig
from .orchestrator import Orchestrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def build_server_configs(cfg: dict) -> list[ServerConfig]:
    servers = []
    for s in cfg.get("servers", []):
        servers.append(ServerConfig(
            name=s["name"],
            host=s.get("host"),
            broker_port=s.get("broker_port", 8200),
            ssh_options=s.get("ssh_options", ""),
            work_dir=s.get("work_dir", ""),
        ))
    return servers


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
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--web", action="store_true", default=True, help="Use web chat frontend (default)")
    parser.add_argument("--cli", action="store_true", help="Use CLI REPL frontend")
    parser.add_argument("--telegram", action="store_true", help="Use Telegram bot frontend")
    args = parser.parse_args()

    # Priority: --cli/--telegram override web default
    use_web = not args.cli and not args.telegram
    use_telegram = args.telegram

    cfg = load_config(args.config)

    # Store
    store = Store(cfg.get("data", {}).get("dir", "./data"))
    await store.init()

    # Session manager
    orch_cfg = cfg.get("orchestrator", {})
    server_configs = build_server_configs(cfg)
    session_mgr = SessionManager(
        servers=server_configs,
        default_model=orch_cfg.get("session_model", "claude-opus-4-6"),
    )
    await session_mgr.init()

    # Orchestrator
    orchestrator = Orchestrator(
        session_mgr=session_mgr,
        store=store,
        model=orch_cfg.get("model", "claude-opus-4-6"),
        config_path=args.config,
        orch_config=orch_cfg,
    )
    await orchestrator.init()

    # HTTP server for broker callbacks (always runs)
    # Support both old "permission.port" and new "http.port" config keys
    http_cfg = cfg.get("http", cfg.get("permission", {}))
    http_app = web.Application()
    http_app["orchestrator"] = orchestrator
    http_app.router.add_post("/permission", handle_permission)
    http_app.router.add_post("/clarification", handle_clarification)
    http_runner = await start_http_server(http_app, http_cfg.get("port", 9120))

    # Frontend
    frontend = None
    if use_web:
        from .web import WebChat
        web_cfg = cfg.get("web", {})
        frontend = WebChat(
            orchestrator=orchestrator,
            store=store,
            host=web_cfg.get("host", "127.0.0.1"),
            port=web_cfg.get("port", 8080),
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
        tg_cfg = cfg.get("telegram", {})
        if not tg_cfg.get("token"):
            log.error("Telegram mode requires telegram.token in config")
            sys.exit(1)
        frontend = Bot(
            token=tg_cfg["token"],
            allowed_users=tg_cfg.get("allowed_users", []),
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
