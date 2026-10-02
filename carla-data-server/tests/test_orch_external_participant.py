"""The external-participant scenario, and the wiring that carries it.

Autoware and CARLA's own manual_control.py attach straight to the simulator
rather than to this server, so no `ClientSession` owns their cars. Everything
here covers the parts of that path testable without a simulator: the wiring
that decides whether the scenario can run at all, and the pure helpers that
decide what it concludes.
"""

import sys
import threading
import types

import pytest

from orchestration import protocol as P
from orchestration import scenarios as S


# ── wiring: a scenario can be registered in one place and missing in another ──

def test_every_suite_entry_is_registered_and_has_a_timeout():
    # A scenario listed in the suite but absent from REGISTRY or TIMEOUTS fails
    # at the lab machine, in the middle of a run, rather than here.
    for name in S.DEFAULT_SUITE:
        assert name in S.REGISTRY, f"{name} is in DEFAULT_SUITE but not REGISTRY"
        assert name in S.TIMEOUTS, f"{name} is in DEFAULT_SUITE but has no timeout"


def test_external_participant_is_in_the_default_suite():
    assert "external_participant" in S.DEFAULT_SUITE
    assert S.REGISTRY["external_participant"] is S.scenario_external_participant


# ── preconditions: an absent simulator is a skip, never a failure ────────────

class _Ctx:
    def __init__(self, config):
        self.config = config
        self.run = {"config": {}}
        self.logs = []

    def log(self, message):
        self.logs.append(message)

    @property
    def params(self):
        return self.run["config"]


class _Cfg:
    carla_host = "127.0.0.1"
    carla_port = 2000
    data_server_url = "ws://127.0.0.1:8765"


def test_unreachable_simulator_is_skipped_not_failed(monkeypatch):
    # Hermetic on purpose: whether a real `carla` happens to be importable on
    # the machine running the tests must not change the verdict.
    fake = types.ModuleType("carla")

    def _client(host, port):
        raise RuntimeError("time-out while contacting the simulator")

    fake.Client = _client
    monkeypatch.setitem(sys.modules, "carla", fake)

    outcome = S.scenario_external_participant(_Ctx(_Cfg()))
    assert outcome.status == P.SKIPPED, outcome.status
    assert outcome.assertions == [], "a skip must not claim to have checked anything"


def test_missing_python_api_is_skipped_not_failed(monkeypatch):
    monkeypatch.setitem(sys.modules, "carla", None)  # import carla -> ImportError
    outcome = S.scenario_external_participant(_Ctx(_Cfg()))
    assert outcome.status == P.SKIPPED, outcome.status


# ── helpers that decide what the scenario concludes ──────────────────────────

def test_distance_is_euclidean():
    assert S._distance({"x": 0, "y": 0, "z": 0}, {"x": 3, "y": 4, "z": 0}) == 5.0


@pytest.mark.parametrize("a,b", [
    ({}, {"x": 1, "y": 1, "z": 1}),
    ({"x": 1, "y": 1, "z": 1}, {}),
    ({"x": 1, "y": 1}, {"x": 1, "y": 1, "z": 1}),
    ({"x": None, "y": 1, "z": 1}, {"x": 1, "y": 1, "z": 1}),
])
def test_missing_coordinates_read_as_infinite_not_zero(a, b):
    # Zero would read as "the position matches perfectly" and pass the
    # tolerance assertion on data that is simply absent.
    assert S._distance(a, b) == float("inf")


def test_reported_location_tolerates_a_missing_vehicle():
    assert S._reported_location(None) == {}
    assert S._reported_location({}) == {}
    assert S._reported_location({"transform": {}}) == {}


class _Frames:
    def __init__(self, frames):
        self.lock = threading.Lock()
        self.states = [(0.0, f) for f in frames]


def _frame(*ids):
    return {"vehicles": [{"id": i, "is_ego": False} for i in ids]}


def test_both_participants_must_appear_in_the_same_frame():
    # Seeing each car in a different frame would not show that one viewer
    # holds a coherent picture of a world they reached by different routes.
    split = _Frames([_frame(7), _frame(9)])
    assert S._frame_showing_both(split, 7, 9) == (None, None)

    together = _Frames([_frame(7), _frame(7, 9)])
    mine, theirs = S._frame_showing_both(together, 7, 9)
    assert mine["id"] == 7 and theirs["id"] == 9


def test_frame_search_is_bounded_to_recent_history():
    # Scanning everything retained would make this "has ever been seen", so a
    # car that has since left would still be found.
    old = _Frames([_frame(7, 9)] + [_frame(7) for _ in range(30)])
    assert S._frame_showing_both(old, 7, 9, window=25) == (None, None)
