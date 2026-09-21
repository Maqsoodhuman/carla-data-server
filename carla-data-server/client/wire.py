"""
Wire protocol registry
=======================
Single source of truth for the JSON-over-WebSocket message types and topic/
command sets used by server.py and every consumer (client.py, bridges/,
scripts/). See docs/wire-protocol.md for the full contract.

Import this instead of hardcoding type strings, so a future protocol change
only needs to touch one place.
"""

import json

# Server -> client message types
MSG_WELCOME = "welcome"
MSG_WORLD_STATE = "world_state"
MSG_ACK = "ack"
MSG_CLIENT_LEFT = "client_left"
SERVER_MSG_TYPES = {MSG_WELCOME, MSG_WORLD_STATE, MSG_ACK, MSG_CLIENT_LEFT}

# Client -> server command types
CMD_EGO_CONTROL = "ego_control"
CMD_SPAWN = "spawn"
CMD_DESTROY = "destroy"
CMD_SUBSCRIBE = "subscribe"
CMD_LIST_SPAWN_POINTS = "list_spawn_points"
CMD_PING = "ping"
CMD_SPAWN_SENSOR = "spawn_sensor"
VALID_COMMANDS = {
    CMD_EGO_CONTROL, CMD_SPAWN, CMD_DESTROY, CMD_SUBSCRIBE,
    CMD_LIST_SPAWN_POINTS, CMD_PING, CMD_SPAWN_SENSOR,
}

VALID_TOPICS = {"vehicles", "pedestrians", "traffic_lights", "sensors"}

# Required top-level fields per server->client message type, used by
# validate_message(). Unknown types are not invalid - see validate_message.
REQUIRED_FIELDS = {
    MSG_WORLD_STATE: ("tick", "timestamp", "wall_time"),
    MSG_WELCOME: ("client_id",),
    MSG_ACK: ("command", "status"),
    MSG_CLIENT_LEFT: ("client_id",),
}


def parse_frame(raw):
    """Parse a raw WebSocket text frame. Returns None on invalid JSON instead
    of raising, so a malformed frame never kills a receive loop."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def validate_message(msg) -> bool:
    """True if msg is well-formed enough to dispatch.

    Unknown types are NOT invalid - a consumer must ignore types it doesn't
    handle rather than reject them, so the protocol can grow without a
    synchronized upgrade of every client. Only a KNOWN type missing its
    required fields is invalid.
    """
    if not isinstance(msg, dict):
        return False
    required = REQUIRED_FIELDS.get(msg.get("type"))
    return required is None or all(f in msg for f in required)


def make_subscribe(topics):
    return {"type": CMD_SUBSCRIBE, "payload": {"topics": list(topics)}}


def make_ping(client_ts: float):
    return {"type": CMD_PING, "payload": {"client_ts": client_ts}}
