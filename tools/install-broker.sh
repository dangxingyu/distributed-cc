#!/usr/bin/env bash
# Install the distributed-cc orchestrator daemon on any server.
#
# One-liner:
#   curl -fsSL https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools/install-broker.sh | bash
#
# What it does:
#   1. Creates ~/.distributed-cc/
#   2. Downloads orchestrator_daemon.py plus runtime helper modules
#   3. Installs uv (if not present)
#   4. Creates a venv with dependencies (claude-agent-sdk, aiohttp, mcp)
#
# After install:
#   1. Start the daemon (once per server):
#      ~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/orchestrator_daemon.py --port 8200 --name my-server
#   2. Set up SSH tunnels from the router machine:
#      ssh -N -L <local-port>:localhost:8200 -R 9120:localhost:9120 user@this-server

set -euo pipefail

INSTALL_DIR="$HOME/.distributed-cc"
VENV_DIR="$INSTALL_DIR/.venv"
BASE_URL="https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools"

echo "Installing distributed-cc daemon to $INSTALL_DIR ..."

mkdir -p "$INSTALL_DIR/dcc_runtime"
curl -fsSL "$BASE_URL/orchestrator_daemon.py" -o "$INSTALL_DIR/orchestrator_daemon.py"
for module in __init__.py base.py claude_backend.py codex_backend.py factory.py; do
    curl -fsSL "$BASE_URL/dcc_runtime/$module" -o "$INSTALL_DIR/dcc_runtime/$module"
done

echo "Installing uv (if not present) ..."
if ! command -v uv &>/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source "$HOME/.local/bin/env" 2>/dev/null || true
fi

echo "Setting up venv and installing dependencies ..."
uv venv "$VENV_DIR"
uv pip install --python "$VENV_DIR/bin/python3" claude-agent-sdk aiohttp mcp

echo ""
echo "Done! Daemon installed at: $INSTALL_DIR/"
echo ""
echo "Next steps:"
echo "  1. Install/auth the agent runtime you plan to use on this server:"
echo "     - Claude backend: Claude Code CLI + auth"
echo "     - Codex backend: Codex CLI + auth (ensure 'codex app-server' is on PATH)"
echo ""
echo "  2. Start the daemon (once per server, e.g. in tmux):"
echo "     $VENV_DIR/bin/python3 $INSTALL_DIR/orchestrator_daemon.py --port 8200 --name <server-name>"
echo ""
echo "  3. On the router machine, set up SSH tunnels:"
echo "     ssh -N -L <local-port>:localhost:8200 -R 9120:localhost:9120 user@this-server"
