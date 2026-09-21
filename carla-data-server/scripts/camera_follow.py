"""
Spectator Camera Follower
=========================

What it is
----------
The viewing role: subscribes to the data server's world_state and points a
*local* CARLA's spectator camera at one of the actors in it. The equivalent
of UB-DigitalTwin's `camera-follow` role, which watches the simulation
without participating in it.

Typical use is alongside the mirror bridge: `carla_mirror_client.py`
replicates the authoritative world into a shadow CARLA, and this points that
shadow's camera at the car you care about, so the shadow is actually watchable.

What it does
------------
Each world_state, picks a target (an explicit actor id, the first vehicle
flagged is_ego, or just the first vehicle) and places the spectator behind
and above it. No commands are ever sent to the server - this role is
read-only by design.

Coordinates are used as-is: world_state carries CARLA world coordinates and
the spectator lives in a CARLA world, so nothing is converted here (unlike
the Unity bridge, which must).

Usage:
    python3 scripts/camera_follow.py --server ws://localhost:8765 \\
        --carla-host localhost --carla-port 2003
    python3 scripts/camera_follow.py --follow-id 42 --duration 30
"""

import argparse
import logging
import math
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "client"))
from client import CARLAClient  # noqa: E402

import carla  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("camera-follow")


class CameraFollower(CARLAClient):
    def __init__(self, server_url, carla_host, carla_port, follow_id=None,
                 distance=8.0, height=4.0, pitch=-15.0, update_hz=20.0):
        super().__init__(server_url, subscriptions=["vehicles"], role="camera_follow")
        self.carla_host = carla_host
        self.carla_port = carla_port
        self.follow_id = follow_id
        self.distance = distance
        self.height = height
        self.pitch = pitch
        self._min_interval = 1.0 / update_hz if update_hz > 0 else 0.0

        self._spectator = None
        self._lock = threading.Lock()
        self._last_write = 0.0
        self.updates = 0
        self.target_id = None

    def connect_viewer(self):
        client = carla.Client(self.carla_host, self.carla_port)
        client.set_timeout(10.0)
        self._spectator = client.get_world().get_spectator()
        log.info("viewing CARLA at %s:%d", self.carla_host, self.carla_port)

    def _pick_target(self, vehicles):
        if self.follow_id is not None:
            return next((v for v in vehicles if v.get("id") == self.follow_id), None)
        return next((v for v in vehicles if v.get("is_ego")), None) or (
            vehicles[0] if vehicles else None)

    def on_world_state(self, state):
        if self._spectator is None:
            return
        # Rate-limit writes: the feed can outpace what a spectator needs.
        now = time.monotonic()
        if now - self._last_write < self._min_interval:
            return

        target = self._pick_target(state.get("vehicles", []))
        if target is None:
            return
        transform = target.get("transform", {})
        loc = transform.get("location", {})
        yaw = transform.get("rotation", {}).get("yaw", 0.0)

        # Sit behind the car along its own heading, raised and angled down.
        radians = math.radians(yaw)
        camera = carla.Transform(
            carla.Location(x=loc.get("x", 0.0) - self.distance * math.cos(radians),
                           y=loc.get("y", 0.0) - self.distance * math.sin(radians),
                           z=loc.get("z", 0.0) + self.height),
            carla.Rotation(pitch=self.pitch, yaw=yaw, roll=0.0),
        )
        try:
            self._spectator.set_transform(camera)
        except RuntimeError as exc:
            log.warning("could not move the spectator: %s", exc)
            return
        with self._lock:
            self._last_write = now
            self.updates += 1
            self.target_id = target.get("id")


def main():
    parser = argparse.ArgumentParser(description="Follow an actor with a local "
                                                 "CARLA spectator camera")
    parser.add_argument("--server", default="ws://localhost:8765")
    parser.add_argument("--carla-host", default="localhost",
                        help="the CARLA whose spectator is moved (often a shadow sim)")
    parser.add_argument("--carla-port", type=int, default=2000)
    parser.add_argument("--follow-id", type=int,
                        help="actor id to follow (default: the ego, else any vehicle)")
    parser.add_argument("--distance", type=float, default=8.0)
    parser.add_argument("--height", type=float, default=4.0)
    parser.add_argument("--pitch", type=float, default=-15.0)
    parser.add_argument("--update-hz", type=float, default=20.0)
    parser.add_argument("--duration", type=float,
                        help="stop after this many seconds (default: run until Ctrl+C)")
    args = parser.parse_args()

    follower = CameraFollower(args.server, args.carla_host, args.carla_port,
                              follow_id=args.follow_id, distance=args.distance,
                              height=args.height, pitch=args.pitch,
                              update_hz=args.update_hz)
    follower.connect_viewer()
    thread = follower.run_in_thread()
    try:
        if args.duration:
            time.sleep(args.duration)
        else:
            while thread.is_alive():
                time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        follower.disconnect()
        thread.join(timeout=5)
    log.info("followed actor %s with %d camera updates", follower.target_id,
             follower.updates)


if __name__ == "__main__":
    main()
