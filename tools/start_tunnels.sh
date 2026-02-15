#!/usr/bin/env bash
# Set up SSH tunnels to remote brokers.
#
# Each server needs two tunnels:
#   -L <local_port>:localhost:8200   → access remote broker from local
#   -R 9120:localhost:9120           → remote broker calls back to local orchestrator
#
# Usage: ./tools/start_tunnels.sh
# Or manually: ssh -N -L 8201:localhost:8200 -R 9120:localhost:9120 user@server-a

set -euo pipefail

ORCH_PORT=9120

# ── Define your servers here ──────────────────────────────────
# Format: local_port:ssh_destination
TUNNELS=(
    "8201:user@server-a.example.com"
    "8202:user@server-b.example.com"
)
# ──────────────────────────────────────────────────────────────

PIDS=()

cleanup() {
    echo "Stopping tunnels..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    exit 0
}
trap cleanup SIGINT SIGTERM

for entry in "${TUNNELS[@]}"; do
    LOCAL_PORT="${entry%%:*}"
    SSH_HOST="${entry#*:}"

    echo "Tunnel: localhost:$LOCAL_PORT → $SSH_HOST:8200 (+ reverse :$ORCH_PORT)"
    ssh -N \
        -L "$LOCAL_PORT:localhost:8200" \
        -R "$ORCH_PORT:localhost:$ORCH_PORT" \
        -o ServerAliveInterval=30 \
        -o ServerAliveCountMax=3 \
        -o ExitOnForwardFailure=yes \
        "$SSH_HOST" &
    PIDS+=($!)
done

echo ""
echo "All tunnels started. Press Ctrl+C to stop."
echo "PIDs: ${PIDS[*]}"
wait
