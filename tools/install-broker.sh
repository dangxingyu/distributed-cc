#!/usr/bin/env bash
# Install the distributed-cc remote broker on any server.
#
# One-liner:
#   curl -fsSL https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools/install-broker.sh | bash
#
# What it does:
#   1. Creates ~/.distributed-cc/
#   2. Downloads remote_broker.py and broker_session.py
#   3. Installs uv (if not present)
#   4. Creates a venv with dependencies (claude-agent-sdk, aiohttp)
#
# After install:
#   1. Start the broker (once per server):
#      ~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/remote_broker.py --port 8200 --name my-server
#   2. Register sessions from project dirs:
#      cd /path/to/project && ~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/broker_session.py start

set -euo pipefail

INSTALL_DIR="$HOME/.distributed-cc"
VENV_DIR="$INSTALL_DIR/.venv"
BASE_URL="https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools"

echo "Installing distributed-cc broker to $INSTALL_DIR ..."

mkdir -p "$INSTALL_DIR"
curl -fsSL "$BASE_URL/remote_broker.py" -o "$INSTALL_DIR/remote_broker.py"
curl -fsSL "$BASE_URL/broker_session.py" -o "$INSTALL_DIR/broker_session.py"

echo "Installing uv (if not present) ..."
if ! command -v uv &>/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source "$HOME/.local/bin/env" 2>/dev/null || true
fi

echo "Setting up venv and installing dependencies ..."
uv venv "$VENV_DIR"
uv pip install --python "$VENV_DIR/bin/python3" claude-agent-sdk aiohttp

echo ""
echo "Done! Broker installed at: $INSTALL_DIR/"
echo ""
echo "Next steps:"
echo "  1. Make sure Claude Code CLI is installed and authenticated on this server"
echo ""
echo "  2. Start the broker (once per server, e.g. in tmux):"
echo "     $VENV_DIR/bin/python3 $INSTALL_DIR/remote_broker.py --port 8200 --name <server-name>"
echo ""
echo "  3. Register sessions from project directories:"
echo "     cd /path/to/your/project"
echo "     $VENV_DIR/bin/python3 $INSTALL_DIR/broker_session.py start"
echo ""
echo "  4. On the orchestrator machine, set up SSH tunnels:"
echo "     ssh -N -L <local-port>:localhost:8200 -R 9120:localhost:9120 user@this-server"
