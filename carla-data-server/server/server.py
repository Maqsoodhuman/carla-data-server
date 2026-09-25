"""
CARLA Data Server
=================

What it is
----------
The hub of a hub-and-spoke topology: one process owns the only connection to
CARLA and mirrors world state to any number of clients over WebSocket. Clients
never talk to CARLA directly, or to each other. With no CARLA PythonAPI
installed it runs in STUB mode on synthetic actors, so the pipeline is testable
without a simulator.

What it does
------------
Each tick: advance CARLA in sync mode, snapshot the world (vehicles,
pedestrians, traffic lights, latest sensor frame), fan it out to subscribers as
a JSON world_state. Inbound it accepts ego_control, spawn, destroy, subscribe,
list_spawn_points, ping and spawn_sensor, and evicts clients that go silent.

How it works
------------
  TickLoopThread   - only writer to CARLA. Drains commands, ticks, snapshots.
  BroadcastThread  - filters each snapshot per client, caches the JSON per
                     unique (subscription, ego) key, fills per-client queues.
  SensorBuffer     - not a thread; sensor callbacks write here, tick loop reads.

Asyncio side: a sender and receiver coroutine per client (the receiver
validates against VALID_COMMANDS and queues - there is no separate command
processor), plus janitor (evicts silent clients) and peer_event_fanout
(broadcasts client_left).

Queues are the only path between these pieces. Every asyncio queue is bounded
and drops oldest, so a slow client cannot stall the tick loop.

Liveness is app-level, not WebSocket ping: any inbound frame bumps last_seen,
clients ping every 2s, janitor evicts past --silence-timeout. Eviction destroys
the client's ego and broadcasts client_left with its actor ids; a clean
disconnect does the same. ping is echoed with both timestamps for RTT.

Usage
-----
  python server.py --host 0.0.0.0 --port 8765 \
                   --carla-host localhost --carla-port 2000 \
                   --tick-rate 20 --silence-timeout 5
"""

import argparse
import asyncio
import json
import logging
import math
import os
import queue
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import websockets
from websockets.server import WebSocketServerProtocol

# Shared wire-protocol registry (type strings, topic/command sets) - lives in
# the sibling client/ directory alongside client.py. See docs/wire-protocol.md.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "client"))
import wire  # noqa: E402

# CARLA import (graceful stub if not installed)
try:
    import carla
    CARLA_AVAILABLE = True
except ImportError:
    CARLA_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
)
log = logging.getLogger("carla-server")
if not CARLA_AVAILABLE:
    log.warning("CARLA PythonAPI not found - running in STUB mode")

# Defaults (overridable via CLI)
DEFAULT_SILENCE_TIMEOUT = 5.0   # seconds without inbound traffic before eviction
DEFAULT_TRAFFIC_RATE_DIVISOR = 1  # refresh pedestrians/traffic_lights every Nth tick
JANITOR_INTERVAL = 1.0          # how often the janitor scans
CLIENT_QUEUE_MAX = 10


# ──────────────────────────────────────────────────────────────────────────────
#  Data structures
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ClientSession:
    id: str
    ws: WebSocketServerProtocol
    subscriptions: Set[str] = field(default_factory=lambda: set(wire.VALID_TOPICS))
    send_queue: asyncio.Queue = None
    ego_actor_id: Optional[int] = None
    role: str = "spectator"
    last_seen: float = field(default_factory=time.monotonic)
    last_control: Optional[dict] = None  # persisted ego control, reapplied every tick
    owned_sensor_ids: List[int] = field(default_factory=list)


@dataclass
class Command:
    client_id: str
    type: str
    payload: dict


# ──────────────────────────────────────────────────────────────────────────────
#  Sensor buffer
# ──────────────────────────────────────────────────────────────────────────────

class SensorBuffer:
    def __init__(self):
        self._lock = threading.Lock()
        self._latest: Dict[int, dict] = {}
        self._dirty: Set[int] = set()

    def update(self, sensor_id: int, frame: dict):
        with self._lock:
            self._latest[sensor_id] = frame
            self._dirty.add(sensor_id)

    def drain(self) -> List[dict]:
        """Return only sensors with a frame that arrived since the last
        drain, instead of every active sensor's last-known frame - a tick
        with no fresh camera frame sends no stale JPEG at all."""
        with self._lock:
            out = [self._latest[sid] for sid in self._dirty if sid in self._latest]
            self._dirty.clear()
            return out

    def remove(self, sensor_id: int):
        with self._lock:
            self._latest.pop(sensor_id, None)
            self._dirty.discard(sensor_id)


# ──────────────────────────────────────────────────────────────────────────────
#  CARLA connection / stub
# ──────────────────────────────────────────────────────────────────────────────

class CarlaConnection:
    def __init__(self, host: str, port: int, tick_rate: float, sensor_buffer: SensorBuffer,
                 observe_only: bool = False):
        self.host = host
        self.port = port
        self.tick_rate = tick_rate
        self.sensor_buffer = sensor_buffer
        # Observe-only: another process owns the simulation clock (Autoware's
        # bridge, a dedicated time master). We must not set synchronous mode
        # and must never call world.tick(), or two clients advance one world.
        self.observe_only = observe_only
        self.client = None
        self.world = None
        self._original_settings = None
        self._lock = threading.Lock()
        self._sensors: Dict[int, "carla.Sensor"] = {}
        self._actor_cache: Dict[int, "carla.Actor"] = {}  # id -> direct reference

    def connect(self):
        if not CARLA_AVAILABLE:
            log.info("STUB: pretending to connect to CARLA")
            return
        self.client = carla.Client(self.host, self.port)
        self.client.set_timeout(10.0)
        self.world = self.client.get_world()
        self._original_settings = self.world.get_settings()
        if self.observe_only:
            current = self.world.get_settings()
            log.info("Connected to CARLA at %s:%d (observe-only; clock owned "
                     "elsewhere, world is %s)", self.host, self.port,
                     "synchronous" if current.synchronous_mode else "asynchronous")
            return
        settings = self.world.get_settings()
        settings.synchronous_mode = True
        settings.fixed_delta_seconds = 1.0 / self.tick_rate
        self.world.apply_settings(settings)
        log.info("Connected to CARLA at %s:%d (sync mode @ %.1f Hz)",
                 self.host, self.port, self.tick_rate)

    def disconnect(self):
        if not CARLA_AVAILABLE or self.world is None:
            return
        try:
            with self._lock:
                for sid, sensor in list(self._sensors.items()):
                    try:
                        sensor.stop()
                        sensor.destroy()
                    except Exception:
                        pass
                self._sensors.clear()
                if self.observe_only:
                    # We never changed the settings, and the process that owns
                    # the clock may have changed them since we connected.
                    # Pushing our stale snapshot back would disrupt it.
                    log.info("CARLA settings left untouched (observe-only)")
                elif self._original_settings is not None:
                    self.world.apply_settings(self._original_settings)
                else:
                    s = self.world.get_settings()
                    s.synchronous_mode = False
                    s.fixed_delta_seconds = None
                    self.world.apply_settings(s)
            log.info("CARLA settings restored")
        except Exception as e:
            log.warning("CARLA cleanup error: %s", e)

    def apply_and_tick(self, controls: list):
        """Apply controls and tick in a single lock hold.
        controls is a list of (actor_id, control_dict) tuples.
        Uses cached actor references instead of world.get_actor()
        because get_actor() returns objects that don't properly
        apply controls in sync mode (CARLA custom build quirk)."""
        if not CARLA_AVAILABLE:
            return
        with self._lock:
            for actor_id, control in controls:
                actor = self._actor_cache.get(actor_id)
                if actor is None:
                    continue
                vc = carla.VehicleControl(
                    throttle=float(control.get("throttle", 0)),
                    steer=float(control.get("steer", 0)),
                    brake=float(control.get("brake", 0)),
                    hand_brake=bool(control.get("hand_brake", False)),
                    reverse=bool(control.get("reverse", False)),
                )
                try:
                    actor.apply_control(vc)
                except Exception as e:
                    # Actor was likely destroyed out-of-band; drop it from the
                    # cache so we stop trying to drive it every tick.
                    log.warning("apply_control failed for actor %d, evicting: %s",
                                actor_id, e)
                    self._actor_cache.pop(actor_id, None)
            # Controls are still applied when observing - setting an actor's
            # control does not advance the world. Only the tick is withheld.
            if not self.observe_only:
                self.world.tick()

    def tick(self):
        if not CARLA_AVAILABLE or self.observe_only:
            return
        with self._lock:
            self.world.tick()

    def world_clock(self):
        """The simulation's own frame and elapsed seconds.

        When another process owns the clock our local cycle count says nothing
        about how far the world has advanced, so the published tick and
        timestamp must come from the simulator itself.
        """
        if not CARLA_AVAILABLE:
            return None
        with self._lock:
            stamp = self.world.get_snapshot().timestamp
        return int(stamp.frame), float(stamp.elapsed_seconds)

    def snapshot(self, tick: int, timestamp: float, refresh_traffic: bool = True) -> dict:
        """Build one world_state snapshot. When refresh_traffic is False,
        pedestrians and traffic_lights are skipped entirely (including their
        actor.get_transform()/get_velocity()/get_state() calls) - the caller
        (TickLoopThread) is expected to reuse its last cached copy of those
        two lists. Vehicles are always refreshed every tick regardless."""
        if not CARLA_AVAILABLE:
            return _stub_world_state(tick, timestamp)

        with self._lock:
            actors = self.world.get_actors()
            vehicles, pedestrians, traffic_lights = [], [], []

            for actor in actors:
                if actor.type_id.startswith("vehicle."):
                    t = actor.get_transform()
                    transform = {
                        "location": {"x": t.location.x, "y": t.location.y, "z": t.location.z},
                        "rotation": {"pitch": t.rotation.pitch, "yaw": t.rotation.yaw, "roll": t.rotation.roll},
                    }
                    v = actor.get_velocity()
                    av = actor.get_angular_velocity()
                    vehicles.append({
                        "id": actor.id,
                        "type_id": actor.type_id,
                        # Who spawned this vehicle and why: CARLA's own tag,
                        # e.g. "ego_vehicle" for an Autoware-driven car or
                        # "hero" for a manual one. Empty for background traffic.
                        "role_name": actor.attributes.get("role_name", ""),
                        "transform": transform,
                        "velocity": {"x": v.x, "y": v.y, "z": v.z},
                        "angular_vel": {"x": av.x, "y": av.y, "z": av.z},
                        "is_ego": False,
                    })
                elif refresh_traffic and actor.type_id.startswith("walker."):
                    t = actor.get_transform()
                    transform = {
                        "location": {"x": t.location.x, "y": t.location.y, "z": t.location.z},
                        "rotation": {"pitch": t.rotation.pitch, "yaw": t.rotation.yaw, "roll": t.rotation.roll},
                    }
                    v = actor.get_velocity()
                    pedestrians.append({
                        "id": actor.id,
                        "type_id": actor.type_id,
                        "transform": transform,
                        "velocity": {"x": v.x, "y": v.y, "z": v.z},
                    })
                elif refresh_traffic and actor.type_id.startswith("traffic.traffic_light"):
                    t = actor.get_transform()
                    transform = {
                        "location": {"x": t.location.x, "y": t.location.y, "z": t.location.z},
                        "rotation": {"pitch": t.rotation.pitch, "yaw": t.rotation.yaw, "roll": t.rotation.roll},
                    }
                    state_map = {
                        carla.TrafficLightState.Red:    "Red",
                        carla.TrafficLightState.Yellow: "Yellow",
                        carla.TrafficLightState.Green:  "Green",
                        carla.TrafficLightState.Off:    "Off",
                    }
                    traffic_lights.append({
                        "id": actor.id,
                        "transform": transform,
                        "state": state_map.get(actor.get_state(), "Off"),
                        "elapsed": actor.get_elapsed_time(),
                    })

        sensors = self.sensor_buffer.drain()

        return {
            "type": wire.MSG_WORLD_STATE,
            "tick": tick,
            "timestamp": timestamp,
            "wall_time": time.time(),
            "vehicles": vehicles,
            "pedestrians": pedestrians,
            "traffic_lights": traffic_lights,
            "sensors": sensors,
        }

    def spawn_actor(self, blueprint_id: str, transform: Optional[dict],
                    autopilot: bool, spawn_point_index: Optional[int] = None) -> Optional[int]:
        if not CARLA_AVAILABLE:
            return int(time.time() * 1000) % 100000
        with self._lock:
            bp_lib = self.world.get_blueprint_library()
            bp = bp_lib.find(blueprint_id)
            if bp is None:
                return None

            if spawn_point_index is not None:
                spawn_points = self.world.get_map().get_spawn_points()
                if not spawn_points:
                    return None
                if spawn_point_index < 0 or spawn_point_index >= len(spawn_points):
                    return None
                t = spawn_points[spawn_point_index]
            elif transform is not None:
                loc = transform["location"]
                rot = transform["rotation"]
                t = carla.Transform(
                    carla.Location(x=loc["x"], y=loc["y"], z=loc["z"]),
                    carla.Rotation(pitch=rot["pitch"], yaw=rot["yaw"], roll=rot["roll"]),
                )
            else:
                return None

            actor = self.world.try_spawn_actor(bp, t)
            if actor is None:
                return None
            self._actor_cache[actor.id] = actor  # cache direct reference
            if autopilot and actor.type_id.startswith("vehicle."):
                actor.set_autopilot(True)
            if actor.type_id.startswith("sensor."):
                self._attach_sensor_listener(actor)
            return actor.id

    def list_spawn_points(self) -> list:
        if not CARLA_AVAILABLE:
            return [
                {"index": 0, "location": {"x": 0.0, "y": 0.0, "z": 0.5},
                 "rotation": {"pitch": 0.0, "yaw": 0.0, "roll": 0.0}},
                {"index": 1, "location": {"x": 10.0, "y": 0.0, "z": 0.5},
                 "rotation": {"pitch": 0.0, "yaw": 90.0, "roll": 0.0}},
            ]
        with self._lock:
            spawn_points = self.world.get_map().get_spawn_points()
            return [
                {
                    "index": i,
                    "location": {"x": sp.location.x, "y": sp.location.y, "z": sp.location.z},
                    "rotation": {"pitch": sp.rotation.pitch, "yaw": sp.rotation.yaw, "roll": sp.rotation.roll},
                }
                for i, sp in enumerate(spawn_points)
            ]

    def destroy_actor(self, actor_id: int) -> bool:
        if not CARLA_AVAILABLE:
            return True
        with self._lock:
            actor = self._actor_cache.pop(actor_id, None)
            if actor is None:
                actor = self.world.get_actor(actor_id)
            if actor is None:
                return False
            if actor_id in self._sensors:
                try:
                    self._sensors[actor_id].stop()
                except Exception:
                    pass
                self._sensors.pop(actor_id, None)
                self.sensor_buffer.remove(actor_id)
            actor.destroy()
            return True

    def spawn_sensor(self, parent_id: int, sensor_bp_name: str,
                     transform_dict: dict, attributes: dict = None) -> Optional[int]:
        """Spawn a sensor attached to a parent actor."""
        if not CARLA_AVAILABLE:
            return int(time.time() * 1000) % 100000
        with self._lock:
            parent = self.world.get_actor(parent_id)
            if parent is None:
                return None
            bp_lib = self.world.get_blueprint_library()
            try:
                bp = bp_lib.find(sensor_bp_name)
            except Exception:
                return None
            # Apply attributes (image_size_x, image_size_y, fov, etc.)
            if attributes:
                for k, v in attributes.items():
                    if bp.has_attribute(k):
                        bp.set_attribute(k, str(v))
            loc = transform_dict.get("location", {})
            rot = transform_dict.get("rotation", {})
            t = carla.Transform(
                carla.Location(x=loc.get("x", 0), y=loc.get("y", 0), z=loc.get("z", 2.5)),
                carla.Rotation(pitch=rot.get("pitch", -15), yaw=rot.get("yaw", 0), roll=rot.get("roll", 0)),
            )
            sensor = self.world.spawn_actor(bp, t, attach_to=parent)
            if sensor is None:
                return None
            self._attach_sensor_listener(sensor)
            return sensor.id

    def _attach_sensor_listener(self, sensor):
        sid = sensor.id
        type_id = sensor.type_id
        buf = self.sensor_buffer

        def _on_data(data):
            sensor_type = type_id.split(".")[-1]
            frame_dict = {
                "actor_id": sid,
                "sensor_type": sensor_type,
                "frame": int(getattr(data, "frame", 0)),
                "timestamp": float(getattr(data, "timestamp", 0.0)),
            }

            # Camera: convert to JPEG bytes
            if "camera" in type_id and hasattr(data, "raw_data"):
                try:
                    import numpy as np
                    array = np.frombuffer(data.raw_data, dtype=np.uint8)
                    array = array.reshape((data.height, data.width, 4))[:, :, :3]  # BGRA -> BGR
                    # Encode as JPEG
                    import io
                    from PIL import Image
                    img = Image.fromarray(array[:, :, ::-1])  # BGR -> RGB
                    jpeg_buf = io.BytesIO()
                    img.save(jpeg_buf, format="JPEG", quality=60)
                    frame_dict["encoding"] = "jpeg"
                    frame_dict["data"] = jpeg_buf.getvalue()
                    frame_dict["width"] = data.width
                    frame_dict["height"] = data.height
                except Exception as e:
                    frame_dict["encoding"] = "meta"
                    frame_dict["data"] = b""
            else:
                frame_dict["encoding"] = "meta"
                frame_dict["data"] = b""

            buf.update(sid, frame_dict)

        sensor.listen(_on_data)
        self._sensors[sid] = sensor


# ──────────────────────────────────────────────────────────────────────────────
#  Stub world state
# ──────────────────────────────────────────────────────────────────────────────

def _stub_world_state(tick: int, timestamp: float) -> dict:
    t = timestamp
    return {
        "type": wire.MSG_WORLD_STATE,
        "tick": tick,
        "timestamp": timestamp,
        "wall_time": time.time(),
        "vehicles": [{
            "id": 1,
            "type_id": "vehicle.tesla.model3",
            "role_name": "",
            "transform": {
                "location": {"x": math.sin(t) * 20, "y": math.cos(t) * 20, "z": 0.5},
                "rotation": {"pitch": 0, "yaw": math.degrees(t) % 360, "roll": 0},
            },
            "velocity": {"x": math.cos(t) * 5, "y": -math.sin(t) * 5, "z": 0},
            "angular_vel": {"x": 0, "y": 0, "z": 0},
            "is_ego": False,
        }],
        "pedestrians": [{
            "id": 100,
            "type_id": "walker.pedestrian.0001",
            "transform": {
                "location": {"x": 5 + math.sin(t * 0.5), "y": 3, "z": 0},
                "rotation": {"pitch": 0, "yaw": 0, "roll": 0},
            },
            "velocity": {"x": 1.0, "y": 0, "z": 0},
        }],
        "traffic_lights": [{
            "id": 200,
            "transform": {"location": {"x": 30, "y": 0, "z": 3},
                          "rotation": {"pitch": 0, "yaw": 0, "roll": 0}},
            "state": ["Red", "Yellow", "Green"][int(t / 5) % 3],
            "elapsed": t % 5,
        }],
        "sensors": [],
    }


# ──────────────────────────────────────────────────────────────────────────────
#  Shared server state
# ──────────────────────────────────────────────────────────────────────────────

class ServerState:
    def __init__(self, silence_timeout: float = DEFAULT_SILENCE_TIMEOUT):
        self.clients: Dict[str, ClientSession] = {}
        self.client_tasks: Dict[str, list] = {}
        self.clients_lock = threading.Lock()

        self.command_queue: "queue.Queue[Command]" = queue.Queue()
        self.broadcast_queue: "queue.Queue[dict]" = queue.Queue(maxsize=5)

        self.running = threading.Event()
        self.running.set()

        self.loop: Optional[asyncio.AbstractEventLoop] = None

        self.silence_timeout = silence_timeout

        # Carries client_left messages from any thread to the asyncio loop.
        # The fan-out coroutine drains this and broadcasts.
        self.peer_event_queue: Optional[asyncio.Queue] = None


# ──────────────────────────────────────────────────────────────────────────────
#  TickLoop
# ──────────────────────────────────────────────────────────────────────────────

class TickLoopThread(threading.Thread):
    def __init__(self, carla_conn: CarlaConnection, state: ServerState, tick_rate: float,
                 traffic_rate_divisor: int = DEFAULT_TRAFFIC_RATE_DIVISOR):
        super().__init__(name="TickLoop", daemon=True)
        self.carla = carla_conn
        self.state = state
        self.tick_rate = tick_rate
        self.tick_interval = 1.0 / tick_rate
        self.traffic_rate_divisor = max(1, traffic_rate_divisor)
        self._tick = 0
        self._sim_time = 0.0
        self._cached_traffic = {"pedestrians": [], "traffic_lights": []}

    def run(self):
        log.info("TickLoop started at %.1f Hz", self.tick_rate)
        while self.state.running.is_set():
            t0 = time.monotonic()

            self._drain_commands()

            # A transient CARLA error or a destroyed actor must not kill this
            # daemon thread — that would silently freeze all broadcasts while
            # clients stay connected. Log and continue to the next tick.
            try:
                # Apply controls and tick in a single lock hold, matching
                # how CARLA's manual_control.py does it (apply then tick
                # with no gap between them)
                with self.state.clients_lock:
                    controls_to_apply = [
                        (session.ego_actor_id, session.last_control)
                        for session in self.state.clients.values()
                        if session.ego_actor_id and session.last_control
                    ]
                self.carla.apply_and_tick(controls_to_apply)
                if self.carla.observe_only:
                    # Someone else advances the world, so take its clock rather
                    # than counting our own cycles, which would drift from it.
                    clock = self.carla.world_clock()
                    if clock is not None:
                        self._tick, self._sim_time = clock
                    else:
                        self._tick += 1
                        self._sim_time += self.tick_interval
                else:
                    self._tick += 1
                    self._sim_time += self.tick_interval

                refresh_traffic = (self._tick % self.traffic_rate_divisor == 0)
                world_state = self.carla.snapshot(self._tick, self._sim_time, refresh_traffic)
                if refresh_traffic:
                    self._cached_traffic = {
                        "pedestrians": world_state["pedestrians"],
                        "traffic_lights": world_state["traffic_lights"],
                    }
                else:
                    world_state["pedestrians"] = self._cached_traffic["pedestrians"]
                    world_state["traffic_lights"] = self._cached_traffic["traffic_lights"]

                try:
                    self.state.broadcast_queue.put_nowait(world_state)
                except queue.Full:
                    try:
                        self.state.broadcast_queue.get_nowait()
                    except queue.Empty:
                        pass
                    self.state.broadcast_queue.put_nowait(world_state)
            except Exception as e:
                log.exception("TickLoop iteration error: %s", e)

            elapsed = time.monotonic() - t0
            sleep_time = self.tick_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            elif elapsed > self.tick_interval * 1.5:
                log.warning("TickLoop overrun: %.1f ms", elapsed * 1000)

    def _drain_commands(self):
        # Collect all commands, but for ego_control, only keep the last per client
        pending = []
        last_ego_control = {}  # client_id -> (cmd, ack)

        while True:
            try:
                cmd: Command = self.state.command_queue.get_nowait()
            except queue.Empty:
                break

            ack = {"type": wire.MSG_ACK, "client_id": cmd.client_id,
                   "command": cmd.type,
                   "status": "ok", "message": "", "actor_id": 0}

            if cmd.type == "ego_control":
                last_ego_control[cmd.client_id] = (cmd, ack)
            else:
                pending.append((cmd, ack))

        # Process non-ego commands first
        for cmd, ack in pending:
            try:
                self._apply_command(cmd, ack)
            except Exception as e:
                log.exception("Command error: %s", e)
                ack.update({"status": "failed", "message": str(e)})
            self._send_ack(cmd.client_id, ack)

        # Then apply only the last ego_control per client
        for client_id, (cmd, ack) in last_ego_control.items():
            try:
                self._apply_command(cmd, ack)
            except Exception as e:
                log.exception("Command error: %s", e)
                ack.update({"status": "failed", "message": str(e)})
            self._send_ack(cmd.client_id, ack)

    def _apply_command(self, cmd: Command, ack: dict):
        if cmd.type == "ego_control":
            with self.state.clients_lock:
                session = self.state.clients.get(cmd.client_id)
            if session and session.ego_actor_id:
                # Only update last_control here. The actual apply_control
                # happens in apply_and_tick, once per tick, right before world.tick()
                session.last_control = cmd.payload
                log.debug("SERVER RX: ego=%d thr=%.1f brk=%.1f str=%.3f",
                         session.ego_actor_id,
                         cmd.payload.get("throttle", 0),
                         cmd.payload.get("brake", 0),
                         cmd.payload.get("steer", 0))
            else:
                ack.update({"status": "failed", "message": "no ego actor assigned"})

        elif cmd.type == "spawn":
            spawn_point_index = cmd.payload.get("spawn_point_index")
            transform = cmd.payload.get("transform")
            if spawn_point_index is None and transform is None:
                transform = {
                    "location": {"x": 0, "y": 0, "z": 2},
                    "rotation": {"pitch": 0, "yaw": 0, "roll": 0},
                }
            actor_id = self.carla.spawn_actor(
                cmd.payload.get("blueprint_id", "vehicle.tesla.model3"),
                transform,
                cmd.payload.get("autopilot", False),
                spawn_point_index=spawn_point_index,
            )
            if actor_id:
                ack["actor_id"] = actor_id
                with self.state.clients_lock:
                    session = self.state.clients.get(cmd.client_id)
                    if session and session.ego_actor_id is None:
                        session.ego_actor_id = actor_id
            else:
                ack.update({"status": "failed", "message": "spawn failed"})

        elif cmd.type == "destroy":
            actor_id = cmd.payload.get("actor_id")
            ok = self.carla.destroy_actor(int(actor_id))
            if not ok:
                ack.update({"status": "failed", "message": f"actor {actor_id} not found"})

        elif cmd.type == "subscribe":
            topics = set(cmd.payload.get("topics", list(wire.VALID_TOPICS)))
            invalid = topics - wire.VALID_TOPICS
            topics &= wire.VALID_TOPICS
            with self.state.clients_lock:
                session = self.state.clients.get(cmd.client_id)
                if session:
                    session.subscriptions = topics
            if invalid:
                ack.update({"message": f"unknown topics ignored: {sorted(invalid)}"})

        elif cmd.type == "list_spawn_points":
            points = self.carla.list_spawn_points()
            ack["message"] = json.dumps({"spawn_points": points, "count": len(points)})

        elif cmd.type == "ping":
            ack["message"] = json.dumps({
                "client_ts": cmd.payload.get("client_ts", 0.0),
                "server_ts": time.time(),
            })

        elif cmd.type == "spawn_sensor":
            # Spawn a sensor attached to the client's ego vehicle
            parent_id = cmd.payload.get("parent_actor_id")
            if parent_id is None:
                with self.state.clients_lock:
                    session = self.state.clients.get(cmd.client_id)
                if session and session.ego_actor_id:
                    parent_id = session.ego_actor_id
            if parent_id is None:
                ack.update({"status": "failed", "message": "no parent actor"})
            else:
                sensor_bp = cmd.payload.get("sensor_type", "sensor.camera.rgb")
                sensor_attrs = cmd.payload.get("attributes", {})
                transform_dict = cmd.payload.get("transform", {
                    "location": {"x": -5.5, "y": 0.0, "z": 2.5},
                    "rotation": {"pitch": -15.0, "yaw": 0.0, "roll": 0.0},
                })
                sensor_id = self.carla.spawn_sensor(
                    parent_id, sensor_bp, transform_dict, sensor_attrs
                )
                if sensor_id:
                    ack["actor_id"] = sensor_id
                    with self.state.clients_lock:
                        session = self.state.clients.get(cmd.client_id)
                        if session:
                            session.owned_sensor_ids.append(sensor_id)
                else:
                    ack.update({"status": "failed", "message": "sensor spawn failed"})

    def _send_ack(self, client_id: str, ack: dict):
        with self.state.clients_lock:
            session = self.state.clients.get(client_id)
        if session and self.state.loop and session.send_queue is not None:
            try:
                asyncio.run_coroutine_threadsafe(
                    _enqueue_drop_oldest(session.send_queue, json.dumps(ack)),
                    self.state.loop,
                )
            except RuntimeError:
                pass


# ──────────────────────────────────────────────────────────────────────────────
#  Broadcast
# ──────────────────────────────────────────────────────────────────────────────

class BroadcastThread(threading.Thread):
    def __init__(self, state: ServerState):
        super().__init__(name="Broadcast", daemon=True)
        self.state = state

    def run(self):
        log.info("BroadcastThread started")
        while self.state.running.is_set():
            try:
                world_state = self.state.broadcast_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            with self.state.clients_lock:
                sessions = list(self.state.clients.values())

            cache: Dict[tuple, str] = {}

            for session in sessions:
                key = (frozenset(session.subscriptions), session.ego_actor_id)
                msg = cache.get(key)
                if msg is None:
                    filtered = self._filter(world_state, session.subscriptions, session.ego_actor_id)
                    msg = json.dumps(filtered)
                    cache[key] = msg

                if self.state.loop and session.send_queue is not None:
                    try:
                        asyncio.run_coroutine_threadsafe(
                            _enqueue_drop_oldest(session.send_queue, msg),
                            self.state.loop,
                        )
                    except RuntimeError:
                        pass

    @staticmethod
    def _filter(ws: dict, subs: Set[str], ego_actor_id: Optional[int]) -> dict:
        out = {
            "type": ws["type"],
            "tick": ws["tick"],
            "timestamp": ws["timestamp"],
            "wall_time": ws["wall_time"],
        }
        if "vehicles" in subs:
            vehicles = ws.get("vehicles", [])
            if ego_actor_id is not None:
                vehicles = [
                    {**v, "is_ego": v["id"] == ego_actor_id}
                    for v in vehicles
                ]
            out["vehicles"] = vehicles
        if "pedestrians" in subs:
            out["pedestrians"] = ws.get("pedestrians", [])
        if "traffic_lights" in subs:
            out["traffic_lights"] = ws.get("traffic_lights", [])
        if "sensors" in subs:
            import base64
            sensors_out = []
            for s in ws.get("sensors", []):
                s_copy = dict(s)
                # Convert binary data to base64 string for JSON transport
                if isinstance(s_copy.get("data"), bytes) and len(s_copy["data"]) > 0:
                    s_copy["data"] = base64.b64encode(s_copy["data"]).decode("ascii")
                elif isinstance(s_copy.get("data"), bytes):
                    s_copy["data"] = ""
                sensors_out.append(s_copy)
            out["sensors"] = sensors_out
        return out


# ──────────────────────────────────────────────────────────────────────────────
#  Bounded send_queue helper
# ──────────────────────────────────────────────────────────────────────────────

async def _enqueue_drop_oldest(q: asyncio.Queue, msg: str):
    if q.full():
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            pass
    q.put_nowait(msg)


# ──────────────────────────────────────────────────────────────────────────────
#  Per-client handler
# ──────────────────────────────────────────────────────────────────────────────

async def handle_client(ws: WebSocketServerProtocol, state: ServerState,
                         carla_conn: CarlaConnection):
    client_id = str(uuid.uuid4())[:8]
    session = ClientSession(
        id=client_id,
        ws=ws,
        send_queue=asyncio.Queue(maxsize=CLIENT_QUEUE_MAX),
    )

    with state.clients_lock:
        state.clients[client_id] = session

    log.info("Client connected: %s from %s", client_id, ws.remote_address)

    await ws.send(json.dumps({
        "type": wire.MSG_WELCOME,
        "client_id": client_id,
        "valid_topics": sorted(wire.VALID_TOPICS),
        "valid_commands": sorted(wire.VALID_COMMANDS),
        "silence_timeout": state.silence_timeout,
    }))

    async def sender():
        try:
            while True:
                msg = await session.send_queue.get()
                await ws.send(msg)
        except (websockets.exceptions.ConnectionClosed, asyncio.CancelledError):
            return

    async def receiver():
        try:
            async for raw in ws:
                # Bump heartbeat on every inbound frame, even malformed ones.
                # The intent is "client process is alive and talking to us".
                session.last_seen = time.monotonic()
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    log.warning("Invalid JSON from client %s", client_id)
                    continue
                cmd_type = msg.get("type")
                if cmd_type not in wire.VALID_COMMANDS:
                    log.warning("Unknown command from %s: %s", client_id, cmd_type)
                    continue
                state.command_queue.put(Command(
                    client_id=client_id,
                    type=cmd_type,
                    payload=msg.get("payload", {}),
                ))
        except (websockets.exceptions.ConnectionClosed, asyncio.CancelledError):
            return

    tasks = [asyncio.create_task(sender()), asyncio.create_task(receiver())]
    with state.clients_lock:
        state.client_tasks[client_id] = tasks

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        # Determine ownership BEFORE removing the session, so we can both
        # destroy the ego actor (if appropriate) and tell other clients.
        owned_actor_ids: List[int] = []
        owned_sensor_ids: List[int] = []
        with state.clients_lock:
            sess = state.clients.pop(client_id, None)
            state.client_tasks.pop(client_id, None)
            if sess and sess.ego_actor_id is not None:
                owned_actor_ids.append(sess.ego_actor_id)
            if sess:
                owned_sensor_ids.extend(sess.owned_sensor_ids)

        # Best-effort: destroy the client's sensors first, then its ego actor,
        # on disconnect. Without this, an evicted/abruptly-closed client leaves
        # its camera's last frame in SensorBuffer, rebroadcast to peers forever.
        # CARLA calls happen on whichever thread; CarlaConnection's lock
        # serializes them safely.
        for sid in owned_sensor_ids:
            try:
                carla_conn.destroy_actor(int(sid))
            except Exception as e:
                log.warning("Failed to destroy sensor %d on disconnect: %s", sid, e)
        for aid in owned_actor_ids:
            try:
                carla_conn.destroy_actor(int(aid))
            except Exception as e:
                log.warning("Failed to destroy ego actor %d on disconnect: %s", aid, e)

        # Tell remaining clients
        if state.peer_event_queue is not None:
            try:
                state.peer_event_queue.put_nowait({
                    "type": wire.MSG_CLIENT_LEFT,
                    "client_id": client_id,
                    "owned_actor_ids": owned_actor_ids,
                    "wall_time": time.time(),
                })
            except asyncio.QueueFull:
                pass

        log.info("Client disconnected: %s (owned=%s)", client_id, owned_actor_ids)


# ──────────────────────────────────────────────────────────────────────────────
#  Janitor: evict silent clients
# ──────────────────────────────────────────────────────────────────────────────

async def janitor(state: ServerState):
    """Periodically scan sessions and force-close ones past silence timeout."""
    log.info("Janitor started (silence_timeout=%.1fs)", state.silence_timeout)
    while state.running.is_set():
        try:
            await asyncio.sleep(JANITOR_INTERVAL)
        except asyncio.CancelledError:
            return

        now = time.monotonic()
        stale: List[ClientSession] = []
        with state.clients_lock:
            for sess in state.clients.values():
                if now - sess.last_seen > state.silence_timeout:
                    stale.append(sess)

        for sess in stale:
            log.warning("Evicting silent client %s (silent for %.1fs)",
                        sess.id, now - sess.last_seen)
            try:
                # Closing the websocket causes handle_client's gather() to
                # return, which runs the cleanup path (ego destroy + client_left).
                await sess.ws.close(code=4408, reason="silence timeout")
            except Exception:
                pass


# ──────────────────────────────────────────────────────────────────────────────
#  Peer-event fan-out: drain peer_event_queue and broadcast
# ──────────────────────────────────────────────────────────────────────────────

async def peer_event_fanout(state: ServerState):
    """Take peer events (e.g. client_left) and send to all remaining clients."""
    while state.running.is_set():
        try:
            event = await state.peer_event_queue.get()
        except asyncio.CancelledError:
            return

        msg = json.dumps(event)
        with state.clients_lock:
            sessions = list(state.clients.values())

        for sess in sessions:
            if sess.send_queue is not None:
                try:
                    await _enqueue_drop_oldest(sess.send_queue, msg)
                except Exception:
                    pass


# ──────────────────────────────────────────────────────────────────────────────
#  Main
# ──────────────────────────────────────────────────────────────────────────────

async def ws_server(host: str, port: int, state: ServerState,
                     carla_conn: CarlaConnection):
    state.loop = asyncio.get_running_loop()
    state.peer_event_queue = asyncio.Queue(maxsize=128)

    stop = state.loop.create_future()

    def _stop_from_signal(signame: str):
        log.info("Signal %s received, shutting down...", signame)
        state.running.clear()
        if not stop.done():
            stop.set_result(None)

    for signame in ("SIGINT", "SIGTERM"):
        try:
            state.loop.add_signal_handler(
                getattr(signal, signame),
                _stop_from_signal,
                signame,
            )
        except NotImplementedError:
            signal.signal(getattr(signal, signame),
                          lambda s, f, n=signame: _stop_from_signal(n))

    janitor_task = asyncio.create_task(janitor(state))
    fanout_task = asyncio.create_task(peer_event_fanout(state))

    async with websockets.serve(
        lambda ws: handle_client(ws, state, carla_conn),
        host, port,
        max_size=10 * 1024 * 1024,
    ):
        log.info("WebSocket server listening on ws://%s:%d", host, port)
        await stop
        log.info("WebSocket server shutting down")

        with state.clients_lock:
            all_tasks = [t for tasks in state.client_tasks.values() for t in tasks]
            sessions = list(state.clients.values())

        for t in all_tasks:
            t.cancel()

        for session in sessions:
            try:
                await session.ws.close(code=1001, reason="server shutdown")
            except Exception:
                pass

        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        janitor_task.cancel()
        fanout_task.cancel()
        await asyncio.gather(janitor_task, fanout_task, return_exceptions=True)


def main():
    parser = argparse.ArgumentParser(description="CARLA Data Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--carla-host", default="localhost")
    parser.add_argument("--carla-port", type=int, default=2000)
    parser.add_argument("--tick-rate", type=float, default=20.0)
    parser.add_argument("--silence-timeout", type=float,
                        default=DEFAULT_SILENCE_TIMEOUT,
                        help="Seconds without inbound traffic before evicting a client")
    parser.add_argument("--observe", action="store_true",
                        help="do not own the simulation clock: skip synchronous-mode "
                             "setup and never call world.tick(). Use when another "
                             "process advances the world (an Autoware bridge, a "
                             "dedicated time master). tick and timestamp are then "
                             "read from the simulator instead of counted locally.")
    parser.add_argument("--traffic-rate-divisor", type=int,
                        default=DEFAULT_TRAFFIC_RATE_DIVISOR,
                        help="Refresh pedestrians/traffic_lights every Nth tick instead of "
                             "every tick (default 1 = every tick, unchanged behavior). "
                             "Vehicles always refresh every tick regardless.")
    args = parser.parse_args()

    state = ServerState(silence_timeout=args.silence_timeout)
    sensor_buffer = SensorBuffer()
    carla_conn = CarlaConnection(args.carla_host, args.carla_port, args.tick_rate,
                                 sensor_buffer, observe_only=args.observe)
    carla_conn.connect()

    threads = [
        TickLoopThread(carla_conn, state, args.tick_rate,
                       traffic_rate_divisor=args.traffic_rate_divisor),
        BroadcastThread(state),
    ]
    for t in threads:
        t.start()

    try:
        asyncio.run(ws_server(args.host, args.port, state, carla_conn))
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt - shutting down")
    finally:
        state.running.clear()
        for t in threads:
            t.join(timeout=2.0)
        carla_conn.disconnect()
        log.info("Server stopped cleanly")


if __name__ == "__main__":
    main()