#!/usr/bin/env bash
# Install the distributed-cc remote broker on any server.
#
# One-liner:
#   curl -fsSL https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools/install-broker.sh | bash
#
# What it does:
#   1. Creates ~/.distributed-cc/
#   2. Downloads remote_broker.py
#   3. Installs uv (if not present)
#   4. Creates a venv with dependencies (claude-agent-sdk, aiohttp)
#
# After install, start the broker:
#   cd /path/to/your/project
#   ~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/remote_broker.py --port 8200 --name my-server --work-dir .

set -euo pipefail

INSTALL_DIR="$HOME/.distributed-cc"
VENV_DIR="$INSTALL_DIR/.venv"
BROKER_URL="https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools/remote_broker.py"

echo "Installing distributed-cc broker to $INSTALL_DIR ..."

mkdir -p "$INSTALL_DIR"
curl -fsSL "$BROKER_URL" -o "$INSTALL_DIR/remote_broker.py"

echo "Installing uv (if not present) ..."
if ! command -v uv &>/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source "$HOME/.local/bin/env" 2>/dev/null || true
fi

echo "Setting up venv and installing dependencies ..."
uv venv "$VENV_DIR"
uv pip install --python "$VENV_DIR/bin/python3" claude-agent-sdk aiohttp

echo ""
echo "Done! Broker installed at: $INSTALL_DIR/remote_broker.py"
echo ""
echo "Next steps:"
echo "  1. Make sure Claude Code CLI is installed and authenticated on this server"
echo "  2. Start the broker:"
echo "     cd /path/to/your/project"
echo "     $VENV_DIR/bin/python3 $INSTALL_DIR/remote_broker.py --port 8200 --name <server-name> --work-dir ."
echo ""
echo "  3. On the orchestrator machine, set up SSH tunnels:"
echo "     ssh -N -L <local-port>:localhost:8200 -R 9120:localhost:9120 user@this-server"
