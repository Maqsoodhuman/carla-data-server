#!/usr/bin/env bash
# 3. Start the data server against CARLA.
set -u
REPO="${REPO:-/home/maqsood/Documents/carla-data-server/carla-data-server}"
PORT="${PORT:-2000}"

pkill -f "server/server.py" 2>/dev/null
sleep 2
cd "$REPO" || exit 1
exec venv/bin/python server/server.py \
    --host 0.0.0.0 --port 8765 \
    --carla-host localhost --carla-port "$PORT" \
    --tick-rate 20
