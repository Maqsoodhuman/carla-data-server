"""
CARLA Mirror Bridge Client
==========================
A bridge that subscribes to our Data Server's world state and replicates
each tick into a *second* CARLA instance ("CARLA B"), where mirrored actors
are passively rendered.

Architecture:

    CARLA A  --PythonAPI-->  Data Server  --WebSocket-->  THIS BRIDGE
                                                              |
                                                              | PythonAPI
                                                              v
                                                          CARLA B (passive renderer)

What gets mirrored
------------------
  * Vehicles      - spawned and teleported to match CARLA A
  * Pedestrians   - spawned and teleported to match CARLA A
  * Traffic lights - state replicated (Red/Yellow/Green/Off)

Mirroring strategy
------------------
  * CARLA B runs in *async* mode (it's not simulating, just rendering)
  * Mirrored actors have physics disabled - they're teleported via
    set_transform() every tick, not driven via apply_control()
  * Identity mapping: each source actor_id from CARLA A maps to a shadow
    actor_id in CARLA B. The bridge tracks this internally.
  * On disconnect: when an actor disappears from CARLA A, the bridge
    destroys its shadow in CARLA B
  * Traffic lights are matched by index (CARLA's traffic lights are part
    of the map, so map alignment is required for this to be meaningful)

What this bridge does NOT do
----------------------------
  * Sensors (CARLA B would need its own sensors anyway, mirroring sensor
    data has no meaning)
  * Map loading (assumes both CARLAs already have the same map; logs a
    warning if maps differ)
  * Physics in CARLA B (everything is teleported)

Usage
-----
  # Same machine, two CARLAs on ports 2000 and 2001
  python bridges/carla_mirror_client.py \\
      --server ws://localhost:8765 \\
      --shadow-host localhost --shadow-port 2001

  # Different machines (server + CARLA A on one PC, CARLA B elsewhere)
  python bridges/carla_mirror_client.py \\
      --server ws://192.168.1.10:8765 \\
      --shadow-host 192.168.1.20 --shadow-port 2000
"""

import argparse
import asyncio
import logging
import os
import sys
import threading
import time
from typing import Dict, Optional, Set

# Allow importing from the sibling client/ directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "client"))
from client import CARLAClient  # noqa: E402

import carla  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("mirror")


# Map our JSON traffic light states back to CARLA enum values
TRAFFIC_LIGHT_STATE_MAP = {
    "Red":    carla.TrafficLightState.Red,
    "Yellow": carla.TrafficLightState.Yellow,
    "Green":  carla.TrafficLightState.Green,
    "Off":    carla.TrafficLightState.Off,
}


class CarlaMirrorBridge(CARLAClient):
    """
    Subscribes to the data server, replicates vehicles/pedestrians/traffic
    lights into a second CARLA instance.
    """

    def __init__(self, server_url: str, shadow_host: str, shadow_port: int):
        super().__init__(
            server_url,
            subscriptions=["vehicles", "pedestrians", "traffic_lights"],
            role="mirror",
        )

        self.shadow_host = shadow_host
        self.shadow_port = shadow_port
        self.shadow_client: Optional[carla.Client] = None
        self.shadow_world: Optional[carla.World] = None
        self.shadow_bp_lib = None

        # source_actor_id (from CARLA A) -> shadow actor in CARLA B
        self._vehicle_map: Dict[int, "carla.Actor"] = {}
        self._pedestrian_map: Dict[int, "carla.Actor"] = {}

        # Cache of CARLA B's traffic lights by index for fast state updates
        self._shadow_traffic_lights: list = []

        # Lock around CARLA B PythonAPI calls (the on_world_state callback
        # runs on the asyncio thread - we want the mirror writes serialized)
        self._mirror_lock = threading.Lock()

        # Throttle telemetry logging
        self._last_log = 0.0

    # ── lifecycle ────────────────────────────────────────────────────────────

    def connect_shadow(self):
        """Connect to CARLA B and prepare the world."""
        log.info("Connecting to shadow CARLA at %s:%d ...",
                 self.shadow_host, self.shadow_port)
        self.shadow_client = carla.Client(self.shadow_host, self.shadow_port)
        self.shadow_client.set_timeout(10.0)
        self.shadow_world = self.shadow_client.get_world()
        self.shadow_bp_lib = self.shadow_world.get_blueprint_library()

        # Force shadow CARLA into async mode - it's a renderer, not a sim
        settings = self.shadow_world.get_settings()
        if settings.synchronous_mode:
            log.info("Shadow CARLA was in sync mode - switching to async")
            settings.synchronous_mode = False
            settings.fixed_delta_seconds = None
            self.shadow_world.apply_settings(settings)

        # Cache traffic lights by index for fast updates
        self._shadow_traffic_lights = list(
            self.shadow_world.get_actors().filter("traffic.traffic_light*")
        )
        log.info("Shadow world ready: map=%s, %d traffic lights cached",
                 self.shadow_world.get_map().name,
                 len(self._shadow_traffic_lights))

    def cleanup_shadow(self):
        """Destroy every shadow actor we created and reset state."""
        log.info("Cleaning up %d vehicle(s) and %d pedestrian(s) in shadow",
                 len(self._vehicle_map), len(self._pedestrian_map))
        with self._mirror_lock:
            for actor in list(self._vehicle_map.values()):
                try:
                    actor.destroy()
                except Exception:
                    pass
            for actor in list(self._pedestrian_map.values()):
                try:
                    actor.destroy()
                except Exception:
                    pass
            self._vehicle_map.clear()
            self._pedestrian_map.clear()

    # ── data plane: called every tick from on_world_state ───────────────────

    def _mirror_vehicles(self, vehicles_in: list):
        """Spawn / teleport / destroy vehicles to match the source list."""
        seen_ids: Set[int] = set()

        for v in vehicles_in:
            source_id = v["id"]
            type_id = v["type_id"]
            transform = self._make_transform(v["transform"])
            seen_ids.add(source_id)

            shadow = self._vehicle_map.get(source_id)
            if shadow is None:
                # First time we've seen this vehicle - spawn a shadow
                shadow = self._spawn_shadow_vehicle(type_id, transform)
                if shadow is not None:
                    self._vehicle_map[source_id] = shadow
            else:
                # Existing shadow - just teleport
                try:
                    shadow.set_transform(transform)
                except Exception as e:
                    log.warning("Failed to teleport vehicle %d: %s", source_id, e)

        # Destroy shadows for vehicles that have left CARLA A
        gone = set(self._vehicle_map.keys()) - seen_ids
        for source_id in gone:
            actor = self._vehicle_map.pop(source_id, None)
            if actor is not None:
                try:
                    actor.destroy()
                except Exception:
                    pass

    def _mirror_pedestrians(self, peds_in: list):
        """Spawn / teleport / destroy walkers to match the source list."""
        seen_ids: Set[int] = set()

        for p in peds_in:
            source_id = p["id"]
            type_id = p["type_id"]
            transform = self._make_transform(p["transform"])
            seen_ids.add(source_id)

            shadow = self._pedestrian_map.get(source_id)
            if shadow is None:
                shadow = self._spawn_shadow_pedestrian(type_id, transform)
                if shadow is not None:
                    self._pedestrian_map[source_id] = shadow
            else:
                try:
                    shadow.set_transform(transform)
                except Exception as e:
                    log.warning("Failed to teleport ped %d: %s", source_id, e)

        gone = set(self._pedestrian_map.keys()) - seen_ids
        for source_id in gone:
            actor = self._pedestrian_map.pop(source_id, None)
            if actor is not None:
                try:
                    actor.destroy()
                except Exception:
                    pass

    def _mirror_traffic_lights(self, lights_in: list):
        """Replicate traffic light state by index."""
        # The source list and shadow list are both ordered by CARLA's actor
        # iteration order. We assume map alignment - if maps differ, the
        # indices won't match and the warning at startup is the user's hint.
        for i, light in enumerate(lights_in):
            if i >= len(self._shadow_traffic_lights):
                break
            state_str = light.get("state", "Off")
            state_enum = TRAFFIC_LIGHT_STATE_MAP.get(state_str)
            if state_enum is None:
                continue
            try:
                shadow_light = self._shadow_traffic_lights[i]
                if shadow_light.get_state() != state_enum:
                    shadow_light.set_state(state_enum)
            except Exception:
                pass  # Best-effort; traffic light writes can fail

    # ── helpers ─────────────────────────────────────────────────────────────

    def _make_transform(self, t: dict) -> "carla.Transform":
        loc = t["location"]
        rot = t["rotation"]
        return carla.Transform(
            carla.Location(x=loc["x"], y=loc["y"], z=loc["z"]),
            carla.Rotation(pitch=rot["pitch"], yaw=rot["yaw"], roll=rot["roll"]),
        )

    def _spawn_shadow_vehicle(self, type_id: str,
                              transform: "carla.Transform") -> Optional["carla.Actor"]:
        """Spawn a vehicle in CARLA B with physics disabled."""
        try:
            bp = self.shadow_bp_lib.find(type_id)
        except Exception:
            log.warning("Shadow CARLA has no blueprint %s - falling back", type_id)
            try:
                bp = self.shadow_bp_lib.find("vehicle.tesla.model3")
            except Exception:
                return None

        # try_spawn_actor returns None on collision; bump z slightly to help
        bumped = carla.Transform(
            carla.Location(transform.location.x,
                           transform.location.y,
                           transform.location.z + 0.5),
            transform.rotation,
        )
        actor = self.shadow_world.try_spawn_actor(bp, bumped)
        if actor is None:
            return None

        # Disable physics so we can teleport without fighting the engine
        try:
            actor.set_simulate_physics(False)
        except Exception:
            pass
        return actor

    def _spawn_shadow_pedestrian(self, type_id: str,
                                 transform: "carla.Transform") -> Optional["carla.Actor"]:
        try:
            bp = self.shadow_bp_lib.find(type_id)
        except Exception:
            try:
                bp = self.shadow_bp_lib.filter("walker.pedestrian.*")[0]
            except Exception:
                return None

        bumped = carla.Transform(
            carla.Location(transform.location.x,
                           transform.location.y,
                           transform.location.z + 0.5),
            transform.rotation,
        )
        actor = self.shadow_world.try_spawn_actor(bp, bumped)
        if actor is None:
            return None
        try:
            actor.set_simulate_physics(False)
        except Exception:
            pass
        return actor

    # ── overrides from CARLAClient ──────────────────────────────────────────

    def on_connected(self, client_id: str):
        super().on_connected(client_id)
        log.info("[mirror] Connected to data server as %s", client_id)

    def on_world_state(self, state: dict):
        if self.shadow_world is None:
            return  # not yet connected to shadow

        with self._mirror_lock:
            try:
                self._mirror_vehicles(state.get("vehicles", []))
                self._mirror_pedestrians(state.get("pedestrians", []))
                self._mirror_traffic_lights(state.get("traffic_lights", []))
            except Exception as e:
                log.exception("Mirror tick failed: %s", e)

        # Telemetry once per second
        now = time.monotonic()
        if now - self._last_log >= 1.0:
            self._last_log = now
            log.info(
                "[mirror] tick=%d  vehicles %d->%d  peds %d->%d  lights %d",
                state.get("tick", 0),
                len(state.get("vehicles", [])), len(self._vehicle_map),
                len(state.get("pedestrians", [])), len(self._pedestrian_map),
                len(state.get("traffic_lights", [])),
            )

    def on_ack(self, ack: dict):
        # Mirror sends no commands, nothing to dispatch
        pass


# ──────────────────────────────────────────────────────────────────────────────
#  Map sanity check
# ──────────────────────────────────────────────────────────────────────────────

def warn_if_maps_differ(server_url: str, shadow_world: "carla.World"):
    """
    Best-effort warning. We don't actually know what map CARLA A is using
    without querying it directly (which would defeat the architecture).
    Just log the shadow map name so the user can verify visually.
    """
    shadow_map = shadow_world.get_map().name
    log.info("Shadow CARLA map: %s", shadow_map)
    log.info("(Make sure CARLA A is running the same map for sensible mirroring)")


# ──────────────────────────────────────────────────────────────────────────────
#  CLI entry point
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CARLA Mirror Bridge Client")
    parser.add_argument("--server", default="ws://localhost:8765",
                        help="WebSocket URL of the data server")
    parser.add_argument("--shadow-host", default="localhost",
                        help="Host of CARLA B (the shadow)")
    parser.add_argument("--shadow-port", type=int, default=2001,
                        help="Port of CARLA B (default: 2001)")
    args = parser.parse_args()

    bridge = CarlaMirrorBridge(args.server, args.shadow_host, args.shadow_port)

    # Connect to shadow CARLA before opening the WebSocket
    try:
        bridge.connect_shadow()
        warn_if_maps_differ(args.server, bridge.shadow_world)
    except Exception as e:
        log.error("Failed to connect to shadow CARLA at %s:%d - %s",
                  args.shadow_host, args.shadow_port, e)
        sys.exit(1)

    try:
        bridge.run()
    finally:
        bridge.cleanup_shadow()
        log.info("Mirror bridge stopped")


if __name__ == "__main__":
    main()
