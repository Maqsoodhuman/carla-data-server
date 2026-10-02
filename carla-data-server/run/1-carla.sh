#!/usr/bin/env bash
# 1. Start CARLA on the NVIDIA GPU, with a window you can see.
#    OFFSCREEN=1 ./1-carla.sh   -> headless instead
set -u
CARLA_DIR="${CARLA_DIR:-/home/maqsood/Documents/Cavas_Lab/CARLA_ff009c8a3-dirty}"
PORT="${PORT:-2000}"
QUALITY="${QUALITY:-Epic}"
RESX="${RESX:-1280}"
RESY="${RESY:-720}"

pkill -9 -f CarlaUE4 2>/dev/null
sleep 3

if [ "${OFFSCREEN:-0}" = "1" ]; then
    MODE="-RenderOffScreen"
else
    MODE="-windowed -ResX=$RESX -ResY=$RESY"
fi

cd "$CARLA_DIR" || exit 1
__NV_PRIME_RENDER_OFFLOAD=1 \
__GLX_VENDOR_LIBRARY_NAME=nvidia \
__VK_LAYER_NV_optimus=NVIDIA_only \
./CarlaUE4.sh -carla-rpc-port="$PORT" -quality-level="$QUALITY" $MODE &

echo "starting CARLA on port $PORT ..."
until ss -ltn 2>/dev/null | grep -q ":$PORT"; do sleep 3; done
echo "CARLA up on port $PORT"
nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader
