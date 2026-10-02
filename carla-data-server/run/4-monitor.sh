#!/usr/bin/env bash
# 4. Watch CPU and GPU while CARLA runs. Ctrl+C to stop.
set -u
while true; do
    clear
    echo "=== GPU ==============================================="
    nvidia-smi --query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw \
               --format=csv,noheader 2>/dev/null
    echo
    echo "processes on the GPU:"
    nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader 2>/dev/null
    nvidia-smi 2>/dev/null | awk '/Processes:/{f=1} f&&/MiB/{print "  "$0}' | tail -4
    echo
    echo "=== CPU ==============================================="
    echo "load:$(uptime | sed 's/.*load average://') across $(nproc) cores"
    echo
    ps -eo pcpu,pmem,comm --sort=-pcpu --no-headers 2>/dev/null | head -5 \
        | awk '{printf "  %5s%% cpu  %5s%% mem  %s\n", $1, $2, $3}'
    echo
    echo "=== CARLA ============================================="
    if ss -ltn 2>/dev/null | grep -q ':2000'; then echo "  port 2000: UP"; else echo "  port 2000: down"; fi
    if ss -ltn 2>/dev/null | grep -q ':8765'; then echo "  port 8765: UP (data server)"; else echo "  port 8765: down"; fi
    sleep 2
done
