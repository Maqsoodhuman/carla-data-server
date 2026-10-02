"""Refusing to spawn a vehicle over empty space.

A map whose OpenDRIVE road network covers more ground than its built geometry
hands out spawn points above nothing. The spawn succeeds, the car falls
forever, the camera stays black, and no error is reported anywhere - so the
check exists to turn that silent failure into an ordinary failed ack.
"""

import types

import pytest

import server
from server import CarlaConnection, SensorBuffer


class _Hit:
    def __init__(self, label="Roads"):
        self.label = label


class _World:
    """Records probes; `ground` decides what the raycast finds."""

    def __init__(self, ground=True):
        self.ground = ground
        self.probes = []
        self.spawned = []

    def cast_ray(self, start, end):
        self.probes.append((start, end))
        return [_Hit()] if self.ground else []

    def get_blueprint_library(self):
        return types.SimpleNamespace(find=lambda bp_id: types.SimpleNamespace(
            id=bp_id, has_attribute=lambda a: False))

    def get_map(self):
        return types.SimpleNamespace(get_spawn_points=lambda: [_transform(0, 0, 0.5)])

    def try_spawn_actor(self, bp, t):
        self.spawned.append((bp.id, t))
        return types.SimpleNamespace(id=99, type_id=bp.id, attributes={},
                                     listen=lambda cb: None)


def _transform(x, y, z):
    return types.SimpleNamespace(location=types.SimpleNamespace(x=x, y=y, z=z))


def _fake_carla():
    mod = types.SimpleNamespace()
    mod.Location = lambda x, y, z: types.SimpleNamespace(x=x, y=y, z=z)
    mod.Transform = lambda loc, rot: types.SimpleNamespace(location=loc, rotation=rot)
    mod.Rotation = lambda pitch, yaw, roll: types.SimpleNamespace(
        pitch=pitch, yaw=yaw, roll=roll)
    return mod


def _conn(world, monkeypatch):
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    monkeypatch.setattr(server, "carla", _fake_carla(), raising=False)
    conn = CarlaConnection("h", 2000, 20.0, SensorBuffer())
    conn.world = world
    return conn


def test_vehicle_over_empty_space_is_refused(monkeypatch):
    world = _World(ground=False)
    conn = _conn(world, monkeypatch)
    assert conn.spawn_actor("vehicle.tesla.model3", None, False, spawn_point_index=0) is None
    assert world.spawned == [], "a car that would fall must never be spawned"


def test_vehicle_over_ground_is_spawned(monkeypatch):
    world = _World(ground=True)
    conn = _conn(world, monkeypatch)
    assert conn.spawn_actor("vehicle.tesla.model3", None, False, spawn_point_index=0) == 99
    assert len(world.spawned) == 1


def test_sensor_high_above_the_map_is_still_allowed(monkeypatch):
    # A bird's-eye camera is a legitimate placement; gravity is not its problem.
    world = _World(ground=False)
    conn = _conn(world, monkeypatch)
    placement = {"location": {"x": 0, "y": 0, "z": 120},
                 "rotation": {"pitch": -90, "yaw": 0, "roll": 0}}
    assert conn.spawn_actor("sensor.camera.rgb", placement, False) == 99
    assert world.probes == [], "a sensor placement must not be ground-probed"


def test_probe_looks_below_the_spawn_point(monkeypatch):
    world = _World(ground=True)
    conn = _conn(world, monkeypatch)
    conn.spawn_actor("vehicle.tesla.model3", None, False, spawn_point_index=0)
    start, end = world.probes[0]
    assert start.z > end.z, "the probe must point downwards"
    assert start.z == pytest.approx(0.5 + server.GROUND_PROBE_UP)
    assert end.z == pytest.approx(0.5 - server.GROUND_PROBE_DOWN)


def test_a_simulator_without_cast_ray_does_not_block_spawning(monkeypatch):
    # Never refuse a spawn over a question the simulator cannot answer.
    class _Old(_World):
        def cast_ray(self, start, end):
            raise AttributeError("cast_ray not available")

    world = _Old()
    conn = _conn(world, monkeypatch)
    assert conn.spawn_actor("vehicle.tesla.model3", None, False, spawn_point_index=0) == 99
