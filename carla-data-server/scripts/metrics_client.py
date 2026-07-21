"""
Metrics Client
==============
Connects to the data server and measures real-time performance metrics:
  - Network latency (server wall_time vs client receive time)
  - Tick rate (actual Hz received)
  - Message size (bytes per world_state)
  - Actor counts (vehicles, pedestrians, traffic lights)
  - Jitter (variation in inter-message timing)

Usage:
    python3 scripts/metrics_client.py --server ws://128.205.222.211:8765
    python3 scripts/metrics_client.py --server ws://localhost:8765

Prints a summary line every second. Ctrl+C to stop and see final averages.
"""

import argparse
import asyncio
import json
import logging
import time
import sys
import os
import statistics

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "client"))

import websockets
import websockets.exceptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [metrics] %(message)s",
)
log = logging.getLogger("metrics")


class MetricsCollector:
    def __init__(self):
        self.latencies = []
        self.intervals = []
        self.msg_sizes = []
        self.tick_counts = []
        self.vehicle_counts = []
        self.ped_counts = []
        self.light_counts = []
        self.last_recv_time = None
        self.start_time = time.time()

        # Per-second window
        self._window_latencies = []
        self._window_intervals = []
        self._window_sizes = []
        self._window_start = time.monotonic()
        self._window_ticks = 0

    def record(self, raw_msg, state):
        now = time.time()
        now_mono = time.monotonic()

        # Latency
        server_wall = state.get("wall_time", now)
        latency_ms = (now - server_wall) * 1000
        self.latencies.append(latency_ms)
        self._window_latencies.append(latency_ms)

        # Inter-message interval
        if self.last_recv_time is not None:
            interval_ms = (now_mono - self.last_recv_time) * 1000
            self.intervals.append(interval_ms)
            self._window_intervals.append(interval_ms)
        self.last_recv_time = now_mono

        # Message size
        size_bytes = len(raw_msg.encode('utf-8')) if isinstance(raw_msg, str) else len(raw_msg)
        self.msg_sizes.append(size_bytes)
        self._window_sizes.append(size_bytes)

        # Actor counts
        n_vehicles = len(state.get("vehicles", []))
        n_peds = len(state.get("pedestrians", []))
        n_lights = len(state.get("traffic_lights", []))
        self.vehicle_counts.append(n_vehicles)
        self.ped_counts.append(n_peds)
        self.light_counts.append(n_lights)

        # Tick counting
        self._window_ticks += 1

        # Print summary every second
        elapsed = now_mono - self._window_start
        if elapsed >= 1.0:
            self._print_window(state.get("tick", 0), n_vehicles, n_peds, n_lights)
            self._reset_window(now_mono)

    def _print_window(self, tick, vehicles, peds, lights):
        # Latency stats
        if self._window_latencies:
            lat_avg = statistics.mean(self._window_latencies)
            lat_min = min(self._window_latencies)
            lat_max = max(self._window_latencies)
        else:
            lat_avg = lat_min = lat_max = 0

        # Jitter (stddev of inter-message intervals)
        if len(self._window_intervals) > 1:
            jitter = statistics.stdev(self._window_intervals)
        else:
            jitter = 0

        # Effective Hz
        hz = self._window_ticks

        # Avg message size
        avg_size = statistics.mean(self._window_sizes) if self._window_sizes else 0
        bandwidth_kbps = (sum(self._window_sizes) * 8) / 1000  # kbps

        log.info(
            "tick=%d | %d Hz | lat: %.1f/%.1f/%.1f ms (avg/min/max) | "
            "jitter: %.1f ms | msg: %.0f B | bw: %.0f kbps | "
            "actors: %d veh, %d ped, %d light",
            tick, hz,
            lat_avg, lat_min, lat_max,
            jitter,
            avg_size, bandwidth_kbps,
            vehicles, peds, lights,
        )

    def _reset_window(self, now_mono):
        self._window_latencies = []
        self._window_intervals = []
        self._window_sizes = []
        self._window_start = now_mono
        self._window_ticks = 0

    def print_final_summary(self):
        duration = time.time() - self.start_time
        total_msgs = len(self.latencies)

        if total_msgs == 0:
            log.info("No messages received")
            return

        log.info("=" * 70)
        log.info("FINAL SUMMARY (%.0f seconds, %d messages)", duration, total_msgs)
        log.info("=" * 70)
        log.info("  Effective tick rate:  %.1f Hz", total_msgs / duration)
        log.info("  Latency (avg):       %.1f ms", statistics.mean(self.latencies))
        log.info("  Latency (min):       %.1f ms", min(self.latencies))
        log.info("  Latency (max):       %.1f ms", max(self.latencies))
        log.info("  Latency (median):    %.1f ms", statistics.median(self.latencies))
        if len(self.latencies) > 1:
            log.info("  Latency (stddev):    %.1f ms", statistics.stdev(self.latencies))
        if len(self.intervals) > 1:
            log.info("  Jitter (stddev):     %.1f ms", statistics.stdev(self.intervals))
            log.info("  Interval (avg):      %.1f ms", statistics.mean(self.intervals))
        log.info("  Msg size (avg):      %.0f bytes", statistics.mean(self.msg_sizes))
        log.info("  Msg size (max):      %.0f bytes", max(self.msg_sizes))
        total_bytes = sum(self.msg_sizes)
        log.info("  Total data received: %.1f KB", total_bytes / 1024)
        log.info("  Avg bandwidth:       %.0f kbps", (total_bytes * 8) / (duration * 1000))
        if self.vehicle_counts:
            log.info("  Vehicles (avg):      %.0f", statistics.mean(self.vehicle_counts))
            log.info("  Vehicles (max):      %d", max(self.vehicle_counts))
        log.info("=" * 70)


async def run_metrics(server_url):
    collector = MetricsCollector()

    while True:
        try:
            async with websockets.connect(
                server_url,
                max_size=10 * 1024 * 1024,
                ping_interval=10,
                ping_timeout=20,
            ) as ws:
                # Wait for welcome
                raw = await ws.recv()
                welcome = json.loads(raw)
                log.info("Connected as %s", welcome.get("client_id", "unknown"))

                # Subscribe to everything
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "payload": {"topics": ["vehicles", "pedestrians", "traffic_lights", "sensors"]}
                }))

                # Keep-alive: send a ping every 3 seconds so the server's
                # silence-timeout janitor doesn't evict us
                async def keep_alive():
                    while True:
                        await asyncio.sleep(3)
                        try:
                            await ws.send(json.dumps({
                                "type": "ping",
                                "payload": {"client_ts": time.time()}
                            }))
                        except Exception:
                            return

                ping_task = asyncio.create_task(keep_alive())

                try:
                    # Consume messages
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        msg_type = msg.get("type")

                        if msg_type == "world_state":
                            collector.record(raw, msg)
                        # Silently ignore acks, pong, welcome, etc.
                finally:
                    ping_task.cancel()

        except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError,
                OSError, asyncio.TimeoutError) as e:
            log.warning("Connection lost: %s - reconnecting in 3s", e)
            await asyncio.sleep(3)
        except KeyboardInterrupt:
            break

    collector.print_final_summary()


def main():
    parser = argparse.ArgumentParser(description="CARLA Data Server Metrics Client")
    parser.add_argument("--server", default="ws://localhost:8765",
                        help="WebSocket URL of the data server")
    args = parser.parse_args()

    log.info("Connecting to %s ...", args.server)
    log.info("Press Ctrl+C to stop and see final summary")
    log.info("")

    try:
        asyncio.run(run_metrics(args.server))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
