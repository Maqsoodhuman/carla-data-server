"""
WebSocket-to-Redis Telemetry Bridge
===================================

What it is
----------
The adapter that lets UB-DigitalTwin's existing Redis clients run unmodified
against this server. They speak Redis pub/sub in the envelope defined by
UB-DigitalTwin's `docs/telemetry-protocol.md` (reference implementation:
`CARLA/UB-API/ub-telemetry/`); this server speaks JSON over WebSocket. Rather
than forking every one of their roles onto `CARLAClient`, this bridge
translates once, so `multi_traffic_renderer`, `multi_agent_renderer` and the
rest keep working with our server standing in for their Redis hub.

    CARLA -> data server --WebSocket--> THIS BRIDGE --Redis pub/sub--> their clients

What it publishes
-----------------
Type 2 (`traffic`) - a batch of every vehicle the server owns, at a fixed
declared rate, plus type 1 (`destroy`) on clean shutdown so consumers tear
down what we spawned.

It deliberately does NOT publish:
  * type 0 (`telemetry`) - that is a participant announcing its own car. This
    bridge is a relay, not a participant; every vehicle is already in type 2.
  * type 3 (`ego`) - the mixed-reality relay. `ws_to_udp_bridge.py` already
    covers the Unity direction.

Known translation gaps (our wire protocol carries no equivalent)
----------------------------------------------------------------
  * `role_name` is emitted as "" for every vehicle. Their traffic renderer
    uses it to skip `hero`/`external_ego` - cars belonging to other
    participants. Here the server is authoritative for every actor including
    client egos, so there is nothing to exclude; consumers see the whole world.
  * `color` is not in `world_state`, so a single `--color` is applied to all.

`server_timestamp` carries our simulation clock (`world_state.timestamp`),
which is what their interpolator wants - not wall clock. See
`docs/wire-protocol.md` for why.

Config resolution matches theirs exactly (env var, then telemetry.conf beside
the script / in the cwd / beside this file, then the built-in default), so
this drops into an existing UB-DigitalTwin deployment without new settings.

Usage:
    python3 bridges/ws_to_redis_bridge.py --server ws://localhost:8765
    UB_REDIS_HOST=10.0.0.5 python3 bridges/ws_to_redis_bridge.py --publish-hz 20
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "client"))
from client import CARLAClient  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("ws-redis-bridge")

# UB-DigitalTwin's canonical message types (Telemetry.MESSAGE_TYPES).
MESSAGE_TYPES = {"telemetry": 0, "destroy": 1, "traffic": 2, "ego": 3}

CONFIG_FILE = "telemetry.conf"
ENV_CONFIG_PATH = "UB_TELEMETRY_CONFIG"
DEFAULTS = {"host": "localhost", "port": 6390,
            "password": "password", "channel": "carla:telemetry"}
DEFAULT_COLOR = "255,255,255"


# ── config, resolved the way their Telemetry base class does ────────────────

def _find_config_file():
    explicit = os.environ.get(ENV_CONFIG_PATH)
    if explicit:
        return explicit
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), CONFIG_FILE),
        os.path.join(os.getcwd(), CONFIG_FILE),
    ]
    return next((c for c in candidates if os.path.exists(c)), None)


def load_redis_config():
    """Environment variable, then telemetry.conf, then the built-in default."""
    config = {}
    path = _find_config_file()
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                config = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("ignoring invalid telemetry config '%s': %s", path, exc)

    def value(env_name, key):
        if env_name in os.environ:
            return os.environ[env_name]
        return config.get(key, DEFAULTS[key])

    port_raw = value("UB_REDIS_PORT", "port")
    try:
        port = int(port_raw)
    except (TypeError, ValueError):
        log.warning("invalid Redis port %r, using %s", port_raw, DEFAULTS["port"])
        port = DEFAULTS["port"]

    return {
        "host": value("UB_REDIS_HOST", "host"),
        "port": port,
        "password": value("UB_REDIS_PASSWORD", "password"),
        "channel": value("UB_REDIS_CHANNEL", "channel"),
        "source": path or "defaults/env only",
    }


# ── translation (pure - no Redis, no network) ───────────────────────────────

def build_traffic_payload(state: dict, color: str = DEFAULT_COLOR) -> dict:
    """Turn one of our world_state messages into a type 2 traffic payload.

    Their renderer requires `id`, `location` and `blueprint` per vehicle (it
    skips any entry missing the latter two) and reads `yaw`, `role_name` and
    `color`. Vehicle ids are strings in their protocol; ours are ints.
    """
    vehicles = []
    for v in state.get("vehicles", []):
        transform = v.get("transform") or {}
        location = transform.get("location")
        blueprint = v.get("type_id")
        if location is None or blueprint is None:
            continue  # unusable to their renderer; drop rather than half-send
        vehicles.append({
            "id": str(v.get("id")),
            # Passed through from world_state. Their renderer uses this to skip
            # cars owned by other participants (hero, external_ego).
            "role_name": v.get("role_name", ""),
            "blueprint": blueprint,
            "color": color,
            "location": {"x": location.get("x"), "y": location.get("y"),
                         "z": location.get("z")},
            "yaw": (transform.get("rotation") or {}).get("yaw", 0.0),
            "server_timestamp": state.get("timestamp"),
            "server_frame": state.get("tick"),
        })
    return {
        "vehicles": vehicles,
        # Simulation time, not wall clock: this is what their interpolator
        # keys on, and it survives clock skew between machines.
        "server_timestamp": state.get("timestamp"),
        "server_frame": state.get("tick"),
    }


def create_message(payload: dict, publisher_id: str, message_type: int) -> str:
    """Their exact envelope: payload first, then id/type/timestamp on top."""
    return json.dumps({**payload, "id": publisher_id, "type": message_type,
                       "timestamp": time.time()})


# ── the bridge ───────────────────────────────────────────────────────────────

class WsToRedisBridge(CARLAClient):
    def __init__(self, server_url, redis_config, publish_hz=20.0,
                 color=DEFAULT_COLOR):
        super().__init__(server_url, subscriptions=["vehicles"], role="redis_bridge")
        # Their protocol requires a process-unique id: publishers ignore their
        # own messages and use them to estimate latency, so a duplicate id
        # corrupts another participant's measurements.
        self.publisher_id = str(uuid.uuid1())
        self.config = redis_config
        self.color = color
        self.min_interval = 1.0 / publish_hz if publish_hz > 0 else 0.0

        self._redis = None
        self._last_publish = 0.0
        self.published = 0
        self.skipped = 0
        self._last_log = 0.0

    def connect_redis(self):
        try:
            import redis
        except ImportError:
            raise SystemExit(
                "the redis package is required for this bridge: pip install redis\n"
                "(only this bridge needs it; the rest of the repo does not)")
        cfg = self.config
        self._redis = redis.Redis(host=cfg["host"], port=cfg["port"],
                                  password=cfg["password"] or None)
        self._redis.ping()
        log.info("publishing to redis %s:%s channel %r (config: %s) as id=%s",
                 cfg["host"], cfg["port"], cfg["channel"], cfg["source"],
                 self.publisher_id)

    def on_world_state(self, state):
        # Publish at a fixed declared rate, not as fast as the feed turns -
        # their channel is shared with participants across a WAN.
        now = time.monotonic()
        if now - self._last_publish < self.min_interval:
            self.skipped += 1
            return
        self._last_publish = now

        payload = build_traffic_payload(state, color=self.color)
        message = create_message(payload, self.publisher_id, MESSAGE_TYPES["traffic"])
        try:
            self._redis.publish(self.config["channel"], message)
        except Exception as exc:  # a Redis hiccup must not kill the bridge
            log.warning("publish failed: %s", exc)
            return
        self.published += 1

        if now - self._last_log >= 5.0:
            self._last_log = now
            log.info("tick=%s vehicles=%d published=%d (rate-limited %d)",
                     state.get("tick"), len(payload["vehicles"]),
                     self.published, self.skipped)

    def send_destroy(self):
        """Tell consumers we are leaving so they tear down our vehicles."""
        if self._redis is None:
            return
        try:
            self._redis.publish(
                self.config["channel"],
                create_message({}, self.publisher_id, MESSAGE_TYPES["destroy"]))
            log.info("sent destroy for id=%s", self.publisher_id)
        except Exception as exc:
            log.warning("could not send destroy: %s", exc)


def main():
    parser = argparse.ArgumentParser(
        description="Republish this server's world_state onto UB-DigitalTwin's "
                    "Redis telemetry channel")
    parser.add_argument("--server", default="ws://localhost:8765",
                        help="WebSocket URL of the CARLA Data Server")
    parser.add_argument("--publish-hz", type=float, default=20.0,
                        help="fixed publish rate onto the Redis channel")
    parser.add_argument("--color", default=DEFAULT_COLOR,
                        help="color for every vehicle (world_state carries none)")
    args = parser.parse_args()

    bridge = WsToRedisBridge(args.server, load_redis_config(),
                             publish_hz=args.publish_hz, color=args.color)
    bridge.connect_redis()
    thread = bridge.run_in_thread()

    def _stop(signum, frame):
        bridge.disconnect()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        while thread.is_alive():
            thread.join(timeout=0.5)
    except KeyboardInterrupt:
        bridge.disconnect()
    finally:
        bridge.send_destroy()
    log.info("bridge stopped after %d published messages", bridge.published)


if __name__ == "__main__":
    main()
