"""The WebSocket -> Redis translation.

These assert against what UB-DigitalTwin's own consumer actually requires
(`ub_telemetry/multi_traffic_renderer.py`): per vehicle it does `v_msg["id"]`
unguarded, skips any entry without both `location` and `blueprint`, and reads
`yaw`, `role_name` and `color`. Top level it reads `vehicles` and
`server_timestamp`. The envelope is `{**payload, id, type, timestamp}`.

No Redis and no server needed - the translation is deliberately pure.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bridges"))

from ws_to_redis_bridge import (  # noqa: E402
    DEFAULT_COLOR, MESSAGE_TYPES, build_traffic_payload, create_message,
    load_redis_config)


def _world_state(**overrides):
    state = {
        "type": "world_state", "tick": 1234, "timestamp": 61.7,
        "wall_time": 1758345600.123,
        "vehicles": [
            {"id": 42, "type_id": "vehicle.audi.a2",
             "transform": {"location": {"x": 40.1, "y": 12.0, "z": 0.3},
                           "rotation": {"pitch": 0.0, "yaw": 180.0, "roll": 0.0}},
             "velocity": {"x": 1.0, "y": 0.0, "z": 0.0}, "is_ego": False},
        ],
        "pedestrians": [], "traffic_lights": [], "sensors": [],
    }
    state.update(overrides)
    return state


# ── per-vehicle shape their renderer requires ────────────────────────────────

def test_vehicle_carries_every_field_the_renderer_reads():
    vehicle = build_traffic_payload(_world_state())["vehicles"][0]
    for field in ("id", "location", "blueprint", "yaw", "role_name", "color"):
        assert field in vehicle, f"their renderer reads {field}"
    assert vehicle["location"] == {"x": 40.1, "y": 12.0, "z": 0.3}
    assert vehicle["blueprint"] == "vehicle.audi.a2"
    assert vehicle["yaw"] == 180.0


def test_vehicle_ids_are_strings():
    # Their protocol uses string ids ("1423"); ours are ints, and their
    # renderer uses the value as a dict key, so the types must not diverge.
    assert build_traffic_payload(_world_state())["vehicles"][0]["id"] == "42"


def test_role_name_is_present_but_empty():
    # We have no role_name; theirs uses it to skip hero/external_ego. Empty
    # means "nothing excluded", which is right when our server owns every actor.
    assert build_traffic_payload(_world_state())["vehicles"][0]["role_name"] == ""


def test_color_defaults_and_is_overridable():
    assert build_traffic_payload(_world_state())["vehicles"][0]["color"] == DEFAULT_COLOR
    custom = build_traffic_payload(_world_state(), color="255,0,0")
    assert custom["vehicles"][0]["color"] == "255,0,0"


@pytest.mark.parametrize("broken", [
    {"id": 1, "type_id": "vehicle.a"},                      # no transform
    {"id": 2, "transform": {"location": {"x": 1, "y": 2, "z": 3}}},  # no blueprint
    {"id": 3, "type_id": "vehicle.c", "transform": {}},     # no location
])
def test_unusable_vehicles_are_dropped_not_half_sent(broken):
    # Their renderer silently skips entries missing location/blueprint, so
    # sending them is pure noise on a shared channel.
    payload = build_traffic_payload(_world_state(vehicles=[broken]))
    assert payload["vehicles"] == []


def test_all_vehicles_are_relayed():
    state = _world_state(vehicles=[
        {"id": i, "type_id": "vehicle.x",
         "transform": {"location": {"x": i, "y": 0, "z": 0},
                       "rotation": {"yaw": 0.0}}} for i in range(5)])
    assert len(build_traffic_payload(state)["vehicles"]) == 5


# ── timing fields their interpolator keys on ────────────────────────────────

def test_server_timestamp_is_simulation_time_not_wall_clock():
    # Their _sample_timestamp interpolates on server_timestamp; wall clock
    # would be meaningless across two machines with unsynced clocks.
    state = _world_state()
    payload = build_traffic_payload(state)
    assert payload["server_timestamp"] == state["timestamp"] == 61.7
    assert payload["server_timestamp"] != state["wall_time"]
    assert payload["server_frame"] == state["tick"] == 1234


def test_each_vehicle_also_carries_the_server_clock():
    vehicle = build_traffic_payload(_world_state())["vehicles"][0]
    assert vehicle["server_timestamp"] == 61.7
    assert vehicle["server_frame"] == 1234


# ── the envelope ─────────────────────────────────────────────────────────────

def test_envelope_matches_their_create_message():
    msg = json.loads(create_message({"vehicles": []}, "pub-1",
                                    MESSAGE_TYPES["traffic"]))
    assert msg["id"] == "pub-1"
    assert msg["type"] == 2
    assert isinstance(msg["timestamp"], float)
    assert msg["vehicles"] == []


def test_envelope_fields_win_over_payload_fields():
    # Theirs is {**message, "id":..., "type":..., "timestamp":...} - payload
    # first, envelope last - so a colliding payload key must not shadow them.
    msg = json.loads(create_message({"id": "payload", "type": 99}, "pub-1", 2))
    assert msg["id"] == "pub-1" and msg["type"] == 2


def test_destroy_message_has_no_payload():
    msg = json.loads(create_message({}, "pub-1", MESSAGE_TYPES["destroy"]))
    assert msg["type"] == 1
    assert set(msg) == {"id", "type", "timestamp"}


def test_message_type_numbers_match_theirs():
    assert MESSAGE_TYPES == {"telemetry": 0, "destroy": 1, "traffic": 2, "ego": 3}


def test_a_full_traffic_message_round_trips_as_json():
    msg = json.loads(create_message(build_traffic_payload(_world_state()),
                                    "pub-1", MESSAGE_TYPES["traffic"]))
    assert msg["type"] == MESSAGE_TYPES["traffic"]
    assert msg["vehicles"][0]["id"] == "42"
    assert msg["server_frame"] == 1234


# ── config resolution, which must match theirs ──────────────────────────────

def test_config_defaults_match_their_telemetry_class(monkeypatch, tmp_path):
    for var in ("UB_REDIS_HOST", "UB_REDIS_PORT", "UB_REDIS_PASSWORD",
                "UB_REDIS_CHANNEL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("UB_TELEMETRY_CONFIG", str(tmp_path / "missing.conf"))
    cfg = load_redis_config()
    assert (cfg["host"], cfg["port"], cfg["channel"]) == (
        "localhost", 6390, "carla:telemetry")


def test_environment_overrides_the_config_file(monkeypatch, tmp_path):
    conf = tmp_path / "telemetry.conf"
    conf.write_text(json.dumps({"host": "from-file", "port": 1111,
                                "password": "p", "channel": "from-file"}))
    monkeypatch.setenv("UB_TELEMETRY_CONFIG", str(conf))
    monkeypatch.setenv("UB_REDIS_HOST", "from-env")
    cfg = load_redis_config()
    assert cfg["host"] == "from-env", "env must win"
    assert cfg["channel"] == "from-file", "file used where env is absent"
    assert cfg["port"] == 1111


def test_invalid_port_falls_back_instead_of_crashing(monkeypatch, tmp_path):
    monkeypatch.setenv("UB_TELEMETRY_CONFIG", str(tmp_path / "missing.conf"))
    monkeypatch.setenv("UB_REDIS_PORT", "not-a-port")
    assert load_redis_config()["port"] == 6390
