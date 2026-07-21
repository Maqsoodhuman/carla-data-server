"""
CARLA Data Client  (patched + ping / peer-left)
================================================
Base WebSocket client that receives world state from the CARLA Data Server
and renders / processes the scene locally.

Extend CARLAClient and override on_world_state() for your use case:
  - AutowareClient  -> publish to ROS2 topics
  - MRAgentClient   -> forward to Unity via shared memory / UDP
  - ManualClient    -> pygame render loop
  - SpectatorClient -> read-only display

What changed in this revision
-----------------------------
  * Background ping task: sends {type: "ping", payload: {client_ts}} every
    PING_INTERVAL seconds. Server echoes back via the ack channel with both
    client_ts (preserved) and server_ts (server's wall clock at handling).
  * RTT and one-way latency tracking using a bounded deque (avg/min/max).
    See get_latency_stats(). Mirrors the pattern from peer Telemetry classes.
  * Ping doubles as heartbeat - if the client is alive, the server sees
    inbound traffic at least every PING_INTERVAL, well under any silence
    timeout.
  * on_peer_left(client_id, owned_actor_ids) override hook for handling
    client_left broadcasts from the server.

Patches kept from previous revision
-----------------------------------
  * Catches asyncio.TimeoutError in the reconnect loop.
  * Catches all websockets exceptions, not just ConnectionClosed.
  * _enqueue() uses the running loop captured at startup (Python 3.10+ safe).
  * Bounded reconnect backoff with attempt logging.
  * Cleaner shutdown when Ctrl+C arrives.
  * asyncio.wait(FIRST_COMPLETED) to avoid sender hang on close.

Usage
-----
  python client.py --server ws://192.168.1.10:8765 --role spectator
  python client.py --server ws://192.168.1.10:8765 --role manual --subscribe vehicles pedestrians
"""

import argparse
import asyncio
import json
import logging
import threading
import time
from collections import deque
from typing import Callable, List, Optional

import websockets
import websockets.exceptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("carla-client")


RECONNECT_EXCEPTIONS = (
    websockets.exceptions.ConnectionClosed,
    websockets.exceptions.InvalidHandshake,
    websockets.exceptions.WebSocketException,
    ConnectionRefusedError,
    ConnectionResetError,
    asyncio.TimeoutError,
    OSError,
)

# Heartbeat / latency
PING_INTERVAL = 2.0       # seconds between pings
LATENCY_BUFFER_SIZE = 100  # rolling window for stats


# ──────────────────────────────────────────────────────────────────────────────
#  Base Client
# ──────────────────────────────────────────────────────────────────────────────

class CARLAClient:
    """
    Base class for all CARLA Data Server clients.

    Subclass and override:
        on_world_state(state: dict)          - tick world data
        on_ack(ack: dict)                    - command acknowledgements
        on_connected(client_id: str)         - after handshake
        on_peer_left(peer_id, owned_ids)     - when another client disconnects
    """

    def __init__(self, server_url: str, subscriptions: List[str], role: str = "spectator"):
        self.server_url = server_url
        self.subscriptions = subscriptions
        self.role = role

        self.client_id: Optional[str] = None
        self._ws = None
        self._send_queue: Optional[asyncio.Queue] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = True

        self._reconnect_delay = 3.0
        self._max_reconnect_delay = 30.0
        self._attempt = 0

        self._tick_count = 0
        self._last_tick_time = time.monotonic()
        self._fps = 0.0

        # Latency state
        self._latency_lock = threading.Lock()
        self._rtt_buffer: deque = deque(maxlen=LATENCY_BUFFER_SIZE)
        self._last_rtt_ms: float = float("nan")

    # ── override these ────────────────────────────────────────────────────────

    def on_connected(self, client_id: str):
        log.info("Connected as client_id=%s role=%s subs=%s",
                 client_id, self.role, self.subscriptions)

    def on_world_state(self, state: dict):
        self._tick_count += 1
        now = time.monotonic()
        dt = now - self._last_tick_time
        if dt >= 1.0:
            self._fps = self._tick_count / dt
            stats = self.get_latency_stats()
            log.info(
                "tick=%d fps=%.1f vehicles=%d peds=%d lights=%d "
                "rtt(ms) avg=%.1f min=%.1f max=%.1f",
                state.get("tick", 0),
                self._fps,
                len(state.get("vehicles", [])),
                len(state.get("pedestrians", [])),
                len(state.get("traffic_lights", [])),
                stats["avg_rtt_ms"], stats["min_rtt_ms"], stats["max_rtt_ms"],
            )
            self._tick_count = 0
            self._last_tick_time = now

    def on_ack(self, ack: dict):
        log.info("ACK status=%s actor_id=%s msg=%s",
                 ack.get("status"), ack.get("actor_id"), ack.get("message"))

    def on_peer_left(self, peer_id: str, owned_actor_ids: List[int]):
        """Another client disconnected. Default: log it. Override to react."""
        log.info("[peer-left] %s (owned actors=%s)", peer_id, owned_actor_ids)

    # ── latency stats ────────────────────────────────────────────────────────

    def get_latency_stats(self) -> dict:
        """Return current avg/min/max RTT in ms. NaN when no samples yet."""
        with self._latency_lock:
            if not self._rtt_buffer:
                nan = float("nan")
                return {"samples": 0, "last_rtt_ms": nan,
                        "avg_rtt_ms": nan, "min_rtt_ms": nan, "max_rtt_ms": nan,
                        "avg_one_way_ms": nan}
            samples = list(self._rtt_buffer)
        avg = sum(samples) / len(samples)
        return {
            "samples": len(samples),
            "last_rtt_ms": self._last_rtt_ms,
            "avg_rtt_ms": avg,
            "min_rtt_ms": min(samples),
            "max_rtt_ms": max(samples),
            "avg_one_way_ms": avg / 2.0,
        }

    # ── commands ──────────────────────────────────────────────────────────────

    def send_ego_control(self, throttle: float = 0, steer: float = 0,
                         brake: float = 0, hand_brake: bool = False, reverse: bool = False):
        self._enqueue({
            "type": "ego_control",
            "payload": {
                "throttle": throttle,
                "steer": steer,
                "brake": brake,
                "hand_brake": hand_brake,
                "reverse": reverse,
            }
        })

    def send_spawn(self, blueprint_id: str, location: dict, rotation: dict,
                   autopilot: bool = False):
        self._enqueue({
            "type": "spawn",
            "payload": {
                "blueprint_id": blueprint_id,
                "transform": {"location": location, "rotation": rotation},
                "autopilot": autopilot,
            }
        })

    def send_spawn_at_index(self, blueprint_id: str, spawn_point_index: int,
                            autopilot: bool = False):
        self._enqueue({
            "type": "spawn",
            "payload": {
                "blueprint_id": blueprint_id,
                "spawn_point_index": spawn_point_index,
                "autopilot": autopilot,
            }
        })

    def send_list_spawn_points(self):
        self._enqueue({
            "type": "list_spawn_points",
            "payload": {},
        })

    def send_destroy(self, actor_id: int):
        self._enqueue({
            "type": "destroy",
            "payload": {"actor_id": actor_id}
        })

    def send_ping(self):
        self._enqueue({
            "type": "ping",
            "payload": {"client_ts": time.time()},
        })

    def disconnect(self):
        self._running = False

    def _enqueue(self, msg: dict):
        if self._loop is None or self._send_queue is None:
            log.warning("_enqueue called before client is connected")
            return
        try:
            self._loop.call_soon_threadsafe(
                self._send_queue.put_nowait, json.dumps(msg)
            )
        except RuntimeError:
            log.warning("_enqueue: event loop is closed")

    # ── internal async loop ───────────────────────────────────────────────────

    async def _run(self):
        self._loop = asyncio.get_running_loop()
        self._send_queue = asyncio.Queue()

        while self._running:
            self._attempt += 1
            try:
                async with websockets.connect(
                    self.server_url,
                    max_size=10 * 1024 * 1024,
                    ping_interval=20,
                    ping_timeout=10,
                    open_timeout=10,
                ) as ws:
                    self._ws = ws
                    self._attempt = 0
                    self._reconnect_delay = 3.0
                    log.info("Connected to %s", self.server_url)

                    recv_task = asyncio.create_task(self._handshake_and_receive(ws))
                    send_task = asyncio.create_task(self._send_loop(ws))
                    ping_task = asyncio.create_task(self._ping_loop())
                    try:
                        done, pending = await asyncio.wait(
                            [recv_task, send_task, ping_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        for t in pending:
                            t.cancel()
                        for t in done:
                            exc = t.exception()
                            if exc and not isinstance(exc, asyncio.CancelledError):
                                raise exc
                    except RECONNECT_EXCEPTIONS as e:
                        log.warning("Connection dropped: %s", e)

            except RECONNECT_EXCEPTIONS as e:
                if not self._running:
                    break
                log.warning(
                    "Connection attempt %d failed: %s - retrying in %.0fs",
                    self._attempt, type(e).__name__, self._reconnect_delay,
                )
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 1.5, self._max_reconnect_delay
                )

            except Exception as e:
                log.exception("Unexpected error in client loop: %s", e)
                if not self._running:
                    break
                await asyncio.sleep(self._reconnect_delay)

        log.info("Client stopped")

    async def _handshake_and_receive(self, ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("Received invalid JSON")
                continue

            msg_type = msg.get("type")

            if msg_type == "welcome":
                self.client_id = msg["client_id"]
                self.on_connected(self.client_id)
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "payload": {"topics": self.subscriptions}
                }))

            elif msg_type == "world_state":
                try:
                    self.on_world_state(msg)
                except Exception:
                    log.exception("Error in on_world_state")

            elif msg_type == "ack":
                # Special-case ping acks: parse and update RTT, do not propagate
                # to on_ack so subclasses don't see ping noise.
                if ack_is_ping(msg):
                    self._handle_ping_ack(msg)
                    continue
                try:
                    self.on_ack(msg)
                except Exception:
                    log.exception("Error in on_ack")

            elif msg_type == "client_left":
                try:
                    self.on_peer_left(
                        msg.get("client_id", ""),
                        msg.get("owned_actor_ids", []) or [],
                    )
                except Exception:
                    log.exception("Error in on_peer_left")

    def _handle_ping_ack(self, ack: dict):
        try:
            payload = json.loads(ack.get("message", "") or "{}")
            client_ts = float(payload.get("client_ts", 0.0))
        except (json.JSONDecodeError, TypeError, ValueError):
            return
        if client_ts <= 0.0:
            return
        rtt_ms = (time.time() - client_ts) * 1000.0
        with self._latency_lock:
            self._last_rtt_ms = rtt_ms
            self._rtt_buffer.append(rtt_ms)

    async def _send_loop(self, ws):
        try:
            while True:
                msg = await self._send_queue.get()
                await ws.send(msg)
        except (websockets.exceptions.ConnectionClosed, asyncio.CancelledError):
            return

    async def _ping_loop(self):
        """Periodic app-level ping; doubles as the heartbeat the server expects."""
        try:
            # Small initial delay so the welcome handshake completes first
            await asyncio.sleep(0.5)
            while True:
                self.send_ping()
                await asyncio.sleep(PING_INTERVAL)
        except asyncio.CancelledError:
            return

    def run(self):
        try:
            asyncio.run(self._run())
        except KeyboardInterrupt:
            log.info("Interrupted - shutting down")
            self._running = False

    def run_in_thread(self) -> threading.Thread:
        t = threading.Thread(target=self.run, name=f"CARLAClient-{self.role}", daemon=True)
        t.start()
        return t


# ──────────────────────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────────────────────

def ack_is_ping(ack: dict) -> bool:
    """An ack belongs to a ping if its 'command' field equals 'ping'."""
    return ack.get("command") == "ping"


# ──────────────────────────────────────────────────────────────────────────────
#  Spectator
# ──────────────────────────────────────────────────────────────────────────────

class SpectatorClient(CARLAClient):
    def __init__(self, server_url: str):
        super().__init__(
            server_url,
            subscriptions=["vehicles", "pedestrians", "traffic_lights"],
        )

    def on_world_state(self, state: dict):
        super().on_world_state(state)


# ──────────────────────────────────────────────────────────────────────────────
#  Manual control
# ──────────────────────────────────────────────────────────────────────────────

class ManualControlClient(CARLAClient):
    """
    Phase 3 hardcoded test sequence:
      1. List spawn points
      2. Spawn ego at spawn point #0 (or --spawn-index)
      3. Drive forward 5s
      4. Turn left while throttling for 2s
      5. Brake hard for 2s
      6. Destroy ego and disconnect
    """

    def __init__(self, server_url: str, spawn_index: int = 0):
        super().__init__(
            server_url,
            subscriptions=["vehicles", "pedestrians", "traffic_lights"],
            role="manual",
        )
        self._state = "INIT"
        self._spawn_index = spawn_index
        self._spawn_points: List[dict] = []
        self._drive_task: Optional[asyncio.Task] = None
        self._last_telemetry = 0.0

    def on_world_state(self, state: dict):
        now = time.monotonic()
        if now - self._last_telemetry < 1.0:
            return
        self._last_telemetry = now

        ego = next((v for v in state.get("vehicles", []) if v.get("is_ego")), None)
        if ego is None:
            return
        loc = ego["transform"]["location"]
        vel = ego["velocity"]
        speed_ms = (vel["x"] ** 2 + vel["y"] ** 2 + vel["z"] ** 2) ** 0.5
        stats = self.get_latency_stats()
        log.info("[manual] ego pos=(%.1f, %.1f) yaw=%.0f speed=%.1f m/s "
                 "(%.0f km/h) rtt=%.1fms",
                 loc["x"], loc["y"], ego["transform"]["rotation"]["yaw"],
                 speed_ms, speed_ms * 3.6, stats["avg_rtt_ms"])

    def on_connected(self, client_id: str):
        super().on_connected(client_id)
        self._state = "LISTING"
        log.info("[manual] Asking server for valid spawn points...")
        self.send_list_spawn_points()

    def on_ack(self, ack: dict):
        cmd_type = ack.get("command", "")

        if cmd_type not in ("list_spawn_points", "spawn"):
            return

        if cmd_type == "list_spawn_points" and self._state == "LISTING":
            try:
                payload = json.loads(ack.get("message", "") or "{}")
                self._spawn_points = payload.get("spawn_points", [])
            except json.JSONDecodeError:
                self._spawn_points = []

            if not self._spawn_points:
                log.error("[manual] Server returned no spawn points - aborting")
                self.disconnect()
                return

            log.info("[manual] Got %d spawn points, picking index %d",
                     len(self._spawn_points), self._spawn_index)

            if self._spawn_index >= len(self._spawn_points):
                log.error("[manual] spawn_index %d out of range (max %d)",
                          self._spawn_index, len(self._spawn_points) - 1)
                self.disconnect()
                return

            self._state = "SPAWNING"
            self.send_spawn_at_index(
                blueprint_id="vehicle.tesla.model3",
                spawn_point_index=self._spawn_index,
                autopilot=False,
            )
            return

        if cmd_type == "spawn" and self._state == "SPAWNING":
            if ack.get("status") == "ok" and ack.get("actor_id"):
                actor_id = ack["actor_id"]
                log.info("[manual] Ego spawned: actor_id=%d at spawn point %d",
                         actor_id, self._spawn_index)
                self._state = "DRIVING"
                if self._loop:
                    self._drive_task = self._loop.create_task(self._drive_sequence(actor_id))
            else:
                log.error("[manual] Spawn failed: %s - aborting", ack.get("message"))
                self.disconnect()
            return

    async def _drive_sequence(self, actor_id: int):
        log.info("[manual] === Drive sequence start ===")

        log.info("[manual] (A) throttle 0.6 forward for 5s")
        for _ in range(100):
            self.send_ego_control(throttle=0.6, steer=0.0, brake=0.0)
            await asyncio.sleep(0.05)

        log.info("[manual] (B) throttle 0.4 + steer left for 2s")
        for _ in range(40):
            self.send_ego_control(throttle=0.4, steer=-0.4, brake=0.0)
            await asyncio.sleep(0.05)

        log.info("[manual] (C) full brake for 2s")
        for _ in range(40):
            self.send_ego_control(throttle=0.0, steer=0.0, brake=1.0)
            await asyncio.sleep(0.05)

        log.info("[manual] (D) destroying ego actor_id=%d", actor_id)
        self.send_destroy(actor_id)
        await asyncio.sleep(1.0)

        log.info("[manual] === Drive sequence done - disconnecting ===")
        self._state = "DONE"
        self.disconnect()

    def send_keyboard_control(self, keys):
        throttle = 1.0 if keys.get("up") else 0.0
        brake    = 1.0 if keys.get("down") else 0.0
        steer    = -0.5 if keys.get("left") else (0.5 if keys.get("right") else 0.0)
        self.send_ego_control(throttle=throttle, steer=steer, brake=brake)


# ──────────────────────────────────────────────────────────────────────────────
#  MR Agent
# ──────────────────────────────────────────────────────────────────────────────

class MRAgentClient(CARLAClient):
    def __init__(self, server_url: str, on_state_callback: Optional[Callable] = None):
        super().__init__(
            server_url,
            subscriptions=["vehicles", "pedestrians", "traffic_lights", "sensors"],
            role="mr_agent",
        )
        self._callback = on_state_callback

    def on_world_state(self, state: dict):
        super().on_world_state(state)
        if self._callback:
            self._callback(state)


# ──────────────────────────────────────────────────────────────────────────────
#  CLI
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CARLA Data Client")
    parser.add_argument("--server", default="ws://localhost:8765")
    parser.add_argument("--role", choices=["spectator", "manual", "mr_agent", "autoware"],
                        default="spectator")
    parser.add_argument("--subscribe", nargs="+",
                        default=["vehicles", "pedestrians", "traffic_lights"],
                        choices=["vehicles", "pedestrians", "traffic_lights", "sensors"])
    parser.add_argument("--spawn-index", type=int, default=0)
    args = parser.parse_args()

    if args.role == "spectator":
        client = SpectatorClient(args.server)
    elif args.role == "manual":
        client = ManualControlClient(args.server, spawn_index=args.spawn_index)
    elif args.role == "mr_agent":
        client = MRAgentClient(args.server)
    else:
        client = CARLAClient(args.server, subscriptions=args.subscribe, role=args.role)

    client.run()


if __name__ == "__main__":
    main()
