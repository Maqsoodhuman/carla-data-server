#!/usr/bin/env bash
# 3. Start the data server against CARLA. Frees port 8765 first.
set -u
REPO="${REPO:-/home/maqsood/Documents/carla-data-server/carla-data-server}"
PORT="${PORT:-2000}"
WS_PORT="${WS_PORT:-8765}"

# Kill whatever holds the websocket port, however it was started.
OLD=$(ss -ltnp 2>/dev/null | grep ":$WS_PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
if [ -n "${OLD:-}" ]; then
    echo "stopping existing server (pid $OLD)"
    kill -9 "$OLD" 2>/dev/null
fi
pkill -9 -f "server/server.py" 2>/dev/null
sleep 2

cd "$REPO" || exit 1
echo "starting data server on ws://0.0.0.0:$WS_PORT  (CARLA $PORT)"
exec venv/bin/python server/server.py \
    --host 0.0.0.0 --port "$WS_PORT" \
    --carla-host localhost --carla-port "$PORT" \
    --tick-rate 20
