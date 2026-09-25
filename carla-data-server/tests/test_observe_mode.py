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

    def copy(self):
        clone = _FakeSettings()
        clone.synchronous_mode = self.synchronous_mode
        clone.fixed_delta_seconds = self.fixed_delta_seconds
        return clone


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
        # A copy, as real CARLA does - otherwise the caller mutating what it
        # got back would silently "apply" settings it never applied.
        return self.settings.copy()

    def apply_settings(self, settings):
        self.applied_settings.append(settings)
        self.settings = settings.copy()

    def tick(self):
        self.ticks += 1
        self.frame += 1
        self.elapsed += 0.05

    def get_snapshot(self):
        return _FakeSnapshot(self.frame, self.elapsed)

    def get_actors(self):
        return []


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


def test_observer_does_not_claim_to_have_restored_settings(monkeypatch, caplog):
    # The log is how an operator checks that --observe really kept its hands
    # off a shared simulator, so it must not claim a restore that never ran.
    import logging
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    conn = _conn(True)
    with caplog.at_level(logging.INFO, logger="carla-server"):
        conn.disconnect()
    assert "restored" not in caplog.text, caplog.text


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


def test_redis_bridge_blanks_role_name_because_it_is_a_relay():
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
    # Their renderer skips hero/external_ego on the assumption that those
    # cars' owners publish them on type 0/3. This bridge publishes neither, so
    # forwarding the tag would delete a manually driven car from their view.
    roles = {v["id"]: v["role_name"] for v in build_traffic_payload(state)["vehicles"]}
    assert roles == {"7": "", "8": ""}


def test_redis_bridge_defaults_role_name_when_absent():
    import os, sys
    sys.path.insert(0, os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bridges"))
    from ws_to_redis_bridge import build_traffic_payload

    state = {"tick": 1, "timestamp": 0.05, "vehicles": [
        {"id": 9, "type_id": "vehicle.x",
         "transform": {"location": {"x": 0, "y": 0, "z": 0}, "rotation": {"yaw": 0}}}]}
    assert build_traffic_payload(state)["vehicles"][0]["role_name"] == ""


# ── the tick loop itself, driven end to end in observe mode ──────────────────
#
# These are the tests that were missing: every defect worth catching here lives
# in TickLoopThread.run(), not in CarlaConnection. The fake below owns the
# schedule - it advances the simulated world every `frames_per_cycle` cycles and
# stops the loop after `cycles` of them - so the assertions do not depend on
# wall-clock timing.

import queue as _queue

from server import ServerState, TickLoopThread


class _FakeConn:
    """A CarlaConnection stand-in that drives the loop deterministically."""

    def __init__(self, observe_only, cycles, frames_per_cycle=1, start_frame=500):
        self.observe_only = observe_only
        self.cycles = cycles
        self.frames_per_cycle = frames_per_cycle
        self.frame = start_frame
        self.elapsed = 25.0
        self.state = None
        self.seen = 0
        self.snapshots = []          # (tick, timestamp, refresh_traffic)

    def apply_and_tick(self, controls):
        self.seen += 1
        if self.seen > 1:            # let the first cycle read the start frame
            self.frame += self.frames_per_cycle
            self.elapsed += 0.05 * self.frames_per_cycle
        if self.seen >= self.cycles:
            self.state.running.clear()

    def world_clock(self):
        return (self.frame, self.elapsed)

    def snapshot(self, tick, timestamp, refresh_traffic=True):
        self.snapshots.append((tick, timestamp, refresh_traffic))
        return {"tick": tick, "timestamp": timestamp,
                "pedestrians": [{"id": 1}] if refresh_traffic else [],
                "traffic_lights": [{"id": 2}] if refresh_traffic else []}


def _run_loop(conn, traffic_rate_divisor=1, tick_rate=1000.0):
    state = ServerState()
    state.broadcast_queue = _queue.Queue()   # unbounded: keep every publish
    conn.state = state
    loop = TickLoopThread(conn, state, tick_rate=tick_rate,
                          traffic_rate_divisor=traffic_rate_divisor)
    loop.run()
    published = []
    while not state.broadcast_queue.empty():
        published.append(state.broadcast_queue.get_nowait())
    return published


def test_slow_clock_owner_does_not_produce_duplicate_ticks():
    # The owner steps once for every three of our cycles. The two cycles that
    # find no new frame must publish nothing at all - not a repeat of the last
    # snapshot under a fresh tick.
    conn = _FakeConn(observe_only=True, cycles=9, frames_per_cycle=0)

    real_apply = conn.apply_and_tick
    def staggered(controls):
        real_apply(controls)
        if conn.seen % 3 == 0:
            conn.frame += 1
            conn.elapsed += 0.05
    conn.apply_and_tick = staggered

    published = _run_loop(conn)
    ticks = [m["tick"] for m in published]
    assert ticks == sorted(set(ticks)), f"tick must be strictly increasing, got {ticks}"
    stamps = [m["timestamp"] for m in published]
    assert len(set(stamps)) == len(stamps), f"no snapshot may repeat, got {stamps}"


def test_frozen_world_publishes_nothing():
    # A clock owner that died must not look like a healthy stream of identical
    # world states. After the first frame there is nothing new to say.
    conn = _FakeConn(observe_only=True, cycles=8, frames_per_cycle=0)
    published = _run_loop(conn)
    assert len(published) == 1, (
        f"a frozen world should fall silent, not publish {len(published)} times")


def test_observed_tick_is_local_and_contiguous_not_the_carla_frame():
    # tick is the ordering/gap-detection field, so it counts our publishes from
    # 1 - it is not CARLA's frame number, which starts high and can jump.
    conn = _FakeConn(observe_only=True, cycles=5, frames_per_cycle=7,
                     start_frame=91234)
    published = _run_loop(conn)
    assert [m["tick"] for m in published] == [1, 2, 3, 4, 5]


def test_observed_timestamp_follows_the_simulator_not_our_tick_rate():
    # timestamp is the interpolation basis, so it must track the world someone
    # else is stepping rather than accumulate our own tick interval.
    conn = _FakeConn(observe_only=True, cycles=4, frames_per_cycle=1)
    published = _run_loop(conn)
    assert [m["timestamp"] for m in published] == pytest.approx([25.0, 25.05, 25.1, 25.15])


def test_traffic_divisor_still_refreshes_when_the_owner_steps_many_frames():
    # Regression: the divisor used to be applied to CARLA's frame number, so an
    # owner stepping 2 frames per cycle from an odd frame never hit 0 mod 2 and
    # pedestrians/traffic_lights stayed empty for the whole run.
    conn = _FakeConn(observe_only=True, cycles=7, frames_per_cycle=2,
                     start_frame=1001)
    published = _run_loop(conn, traffic_rate_divisor=2)
    assert any(m["pedestrians"] for m in published), (
        "traffic never refreshed - the world would look permanently empty")


def test_owner_mode_clock_is_unchanged():
    conn = _FakeConn(observe_only=False, cycles=4)
    published = _run_loop(conn)
    assert [m["tick"] for m in published] == [1, 2, 3, 4]
    assert [round(m["timestamp"], 4) for m in published] == [0.001, 0.002, 0.003, 0.004]


def test_a_skipped_cycle_builds_no_snapshot_at_all():
    # Not just "does not publish": snapshot() drains the SensorBuffer, which is
    # destructive. Building one and discarding it would throw away a fresh
    # camera frame with nothing on the wire to show for it.
    conn = _FakeConn(observe_only=True, cycles=6, frames_per_cycle=0)
    published = _run_loop(conn)
    assert len(conn.snapshots) == 1 == len(published)


def test_a_stalled_clock_owner_is_reported(caplog):
    # This warning is the whole safety net of falling silent: without it a dead
    # clock owner and a healthy quiet world look identical from the outside.
    import logging
    conn = _FakeConn(observe_only=True, cycles=7, frames_per_cycle=0)
    with caplog.at_level(logging.WARNING, logger="carla-server"):
        _run_loop(conn, tick_rate=2.0)
    assert "clock owner" in caplog.text, caplog.text


def test_a_world_reload_is_reported_and_does_not_rewind_tick(caplog):
    # If the clock owner reloads the world, its frame counter and elapsed time
    # restart. tick must keep climbing - it is what consumers order on - while
    # timestamp follows the simulation back down, which is worth a warning.
    import logging
    conn = _FakeConn(observe_only=True, cycles=4, frames_per_cycle=1,
                     start_frame=90000)

    real_apply = conn.apply_and_tick
    def reloading(controls):
        real_apply(controls)
        if conn.seen == 3:          # the owner reloads the map here
            conn.frame, conn.elapsed = 12, 0.6
    conn.apply_and_tick = reloading

    with caplog.at_level(logging.WARNING, logger="carla-server"):
        published = _run_loop(conn, tick_rate=2.0)
    assert [m["tick"] for m in published] == [1, 2, 3, 4]
    assert "backwards" in caplog.text, caplog.text


def test_observed_snapshot_is_stamped_from_inside_the_lock(monkeypatch):
    # The timestamp on the wire must come from the same lock hold as the actor
    # positions. A caller passing a clock read from before the lock was taken
    # would label these positions with a frame the world has already left.
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    conn = _conn(True)
    conn.world.elapsed = 77.5
    state = conn.snapshot(tick=1, timestamp=0.05)
    assert state["timestamp"] == 77.5, "stale caller-supplied timestamp was published"


def test_owner_snapshot_keeps_the_timestamp_it_was_given(monkeypatch):
    monkeypatch.setattr("server.CARLA_AVAILABLE", True)
    conn = _conn(False)
    conn.world.elapsed = 77.5
    assert conn.snapshot(tick=1, timestamp=0.05)["timestamp"] == 0.05
