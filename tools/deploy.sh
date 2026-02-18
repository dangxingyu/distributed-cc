#!/usr/bin/env bash
# Deploy orchestrator daemon to a remote server.
# Usage: ./tools/deploy.sh user@host [daemon-name]
#
# This copies the daemon script, installs uv, and sets up a venv with dependencies.
# After deploying:
#   1. Start the daemon (once per server):
#      ssh user@host "~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/orchestrator_daemon.py --port 8200 --name server-a"
#   2. Set up SSH tunnels from your laptop:
#      ssh -N -L 8201:localhost:8200 -R 9120:localhost:9120 user@host

set -euo pipefail

REMOTE="$1"
DAEMON_NAME="${2:-unknown}"
REMOTE_DIR="~/.distributed-cc"

echo "Deploying orchestrator daemon to $REMOTE:$REMOTE_DIR ..."

ssh "$REMOTE" "mkdir -p $REMOTE_DIR"
scp tools/orchestrator_daemon.py "$REMOTE:$REMOTE_DIR/orchestrator_daemon.py"

# Keep remote_broker.py for backward compat during migration
if [ -f tools/remote_broker.py ]; then
    scp tools/remote_broker.py "$REMOTE:$REMOTE_DIR/remote_broker.py"
fi

echo "Installing uv (if not present) ..."
ssh "$REMOTE" "command -v uv >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/install.sh | sh"

echo "Setting up venv and installing dependencies ..."
ssh "$REMOTE" "source \$HOME/.local/bin/env 2>/dev/null; uv venv $REMOTE_DIR/.venv && uv pip install --python $REMOTE_DIR/.venv/bin/python3 claude-agent-sdk aiohttp"

echo ""
echo "Done. Next steps on remote:"
echo "  1. Start daemon (once per server, e.g. in tmux):"
echo "     $REMOTE_DIR/.venv/bin/python3 $REMOTE_DIR/orchestrator_daemon.py --port 8200 --name $DAEMON_NAME"
echo ""
echo "  2. Set up SSH tunnels from your laptop:"
echo "     ssh -N -L <local_port>:localhost:8200 -R 9120:localhost:9120 $REMOTE"
