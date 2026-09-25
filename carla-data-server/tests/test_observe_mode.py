"""Observe-only mode and the role_name field.

Observe-only exists so the server can share a simulator with something that
already owns the clock (an Autoware bridge, a dedicated time master). Two
clients advancing one synchronous world double-step it, so the rule is
absolute: when observing, never tick and never touch the world settings.
"""

import types

import pytest

import server
from server import CarlaConnection, SensorBuffer


def _fake_carla(world):
    """Stand-in for the carla module. Needed because these tests run on a
    machine without the PythonAPI, so `server.carla` does not exist."""
    mod = types.SimpleNamespace()
    mod.VehicleControl = lambda **kw: kw
    mod.Client = lambda host, port: types.SimpleNamespace(
        set_timeout=lambda t: None, get_world=lambda: world)
    return mod


class _FakeSettings:
    def __init__(self):
        self.synchronous_mode = False
        self.fixed_delta_seconds = None


class _FakeTimestamp:
    def __init__(self, frame, elapsed):
        self.frame = frame
        self.elapsed_seconds = elapsed


class _FakeSnapshot:
    def __init__(self, frame, elapsed):
        self.timestamp = _FakeTimestamp(frame, elapsed)


class _FakeWorld:
    """Records everything the server does to the simulator."""

    def __init__(self):
        self.ticks = 0
        self.applied_settings = []
        self.settings = _FakeSettings()
        self.frame = 500
        self.elapsed = 25.0

    def get_settings(self):
        return self.settings

    def apply_settings(self, settings):
        self.applied_settings.append(settings)

    def tick(self):
        self.ticks += 1
        self.frame += 1
        self.elapsed += 0.05

    def get_snapshot(self):
        return _FakeSnapshot(self.frame, self.elapsed)


def _conn(observe_only, world=None):
    conn = CarlaConnection("h", 2000, 20.0, SensorBuffer(), observe_only=observe_only)
    conn.world = world or _FakeWorld()
    return conn


# ── the rule: an observer never advances the world ───────────────────────────

def test_observer_never_ticks(monkeypatch):
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    conn = _conn(True)
    conn.tick()
    conn.apply_and_tick([])
    assert conn.world.ticks == 0, "an observer must never advance the world"


def test_owner_still_ticks(monkeypatch):
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    conn = _conn(False)
    conn.tick()
    conn.apply_and_tick([])
    assert conn.world.ticks == 2


def test_observer_still_applies_controls(monkeypatch):
    # Setting an actor's control does not advance the world, so a client can
    # still drive while another process keeps time.
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    applied = []

    class _Actor:
        def apply_control(self, control):
            applied.append(control)

    conn = _conn(True)
    conn._actor_cache[7] = _Actor()
    monkeypatch.setattr(server, "carla", _fake_carla(conn.world), raising=False)
    conn.apply_and_tick([(7, {"throttle": 0.5})])
    assert len(applied) == 1
    assert conn.world.ticks == 0


def test_observer_leaves_world_settings_alone(monkeypatch):
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    world = _FakeWorld()
    monkeypatch.setattr(server, "carla", _fake_carla(world), raising=False)
    conn = CarlaConnection("h", 2000, 20.0, SensorBuffer(), observe_only=True)
    conn.connect()
    assert world.applied_settings == [], "observer must not reconfigure the world"
    assert world.settings.synchronous_mode is False


def test_owner_configures_synchronous_mode(monkeypatch):
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    world = _FakeWorld()
    monkeypatch.setattr(server, "carla", _fake_carla(world), raising=False)
    conn = CarlaConnection("h", 2000, 20.0, SensorBuffer(), observe_only=False)
    conn.connect()
    assert len(world.applied_settings) == 1
    assert world.applied_settings[0].synchronous_mode is True
    assert world.applied_settings[0].fixed_delta_seconds == pytest.approx(0.05)


def test_observer_does_not_restore_settings_on_exit(monkeypatch):
    # Our "original" snapshot is stale the moment the real clock owner changes
    # anything, so pushing it back on shutdown would disrupt them.
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    conn = _conn(True)
    conn._original_settings = _FakeSettings()
    conn.disconnect()
    assert conn.world.applied_settings == []


# ── an observer reports the simulator's clock, not its own cycle count ───────

def test_world_clock_reads_the_simulator(monkeypatch):
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    conn = _conn(True)
    conn.world.frame, conn.world.elapsed = 1234, 61.7
    assert conn.world_clock() == (1234, 61.7)


def test_world_clock_is_none_without_carla(monkeypatch):
    monkeypatch.setattr("server.CARLA_AVAILABLE", False)
    assert _conn(True).world_clock() is None


def test_observed_clock_tracks_an_external_ticker(monkeypatch):
    # The external owner advances the world; our reported clock must follow it
    # rather than counting our own loop iterations.
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    conn = _conn(True)
    first = conn.world_clock()
    for _ in range(3):
        conn.world.tick()          # stands in for the external clock owner
    second = conn.world_clock()
    assert second[0] == first[0] + 3
    assert second[1] > first[1]


# ── role_name ────────────────────────────────────────────────────────────────

def test_stub_vehicle_carries_role_name():
    from server import _stub_world_state
    vehicle = _stub_world_state(1, 0.05)["vehicles"][0]
    assert "role_name" in vehicle, "shape must match live mode"
    assert vehicle["role_name"] == ""


def test_redis_bridge_passes_role_name_through():
    import os, sys
    sys.path.insert(0, os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bridges"))
    from ws_to_redis_bridge import build_traffic_payload

    state = {
        "tick": 1, "timestamp": 0.05,
        "vehicles": [
            {"id": 7, "type_id": "vehicle.lincoln.mkz_2020", "role_name": "ego_vehicle",
             "transform": {"location": {"x": 1, "y": 2, "z": 3},
                           "rotation": {"yaw": 90.0}}},
            {"id": 8, "type_id": "vehicle.audi.a2", "role_name": "",
             "transform": {"location": {"x": 4, "y": 5, "z": 6},
                           "rotation": {"yaw": 0.0}}},
        ],
    }
    roles = {v["id"]: v["role_name"] for v in build_traffic_payload(state)["vehicles"]}
    assert roles == {"7": "ego_vehicle", "8": ""}


def test_redis_bridge_defaults_role_name_when_absent():
    import os, sys
    sys.path.insert(0, os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bridges"))
    from ws_to_redis_bridge import build_traffic_payload

    state = {"tick": 1, "timestamp": 0.05, "vehicles": [
        {"id": 9, "type_id": "vehicle.x",
         "transform": {"location": {"x": 0, "y": 0, "z": 0}, "rotation": {"yaw": 0}}}]}
    assert build_traffic_payload(state)["vehicles"][0]["role_name"] == ""
