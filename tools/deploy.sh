#!/usr/bin/env bash
# Deploy remote broker to a server.
# Usage: ./tools/deploy.sh user@host [server-name]
#
# Alternative: the remote user can self-install with:
#   curl -fsSL https://raw.githubusercontent.com/dangxingyu/distributed-cc/main/tools/install-broker.sh | bash
#
# This copies the broker script and sets up a self-contained venv with dependencies.
# After deploying, start the broker on the remote:
#   ssh user@host "cd /path/to/project && ~/.distributed-cc/.venv/bin/python3 ~/.distributed-cc/remote_broker.py --port 8200 --name server-a --work-dir ."

set -euo pipefail

REMOTE="$1"
SERVER_NAME="${2:-unknown}"
REMOTE_DIR="~/.distributed-cc"

echo "Deploying remote broker to $REMOTE:$REMOTE_DIR ..."

ssh "$REMOTE" "mkdir -p $REMOTE_DIR"
scp tools/remote_broker.py "$REMOTE:$REMOTE_DIR/remote_broker.py"

echo "Setting up venv and installing dependencies ..."
ssh "$REMOTE" "python3 -m venv $REMOTE_DIR/.venv && $REMOTE_DIR/.venv/bin/pip install --upgrade pip claude-agent-sdk aiohttp"

echo ""
echo "Done. Start broker on remote:"
echo "  ssh $REMOTE \"cd /path/to/project && $REMOTE_DIR/.venv/bin/python3 $REMOTE_DIR/remote_broker.py --port 8200 --name $SERVER_NAME --work-dir .\""
