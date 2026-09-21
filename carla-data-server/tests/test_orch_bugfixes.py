"""Regressions for bugs a review found after the suite was already green.

Each of these passed its own tests while being wrong, so the guard matters
more than usual.
"""

import copy

import pytest

from orchestration import protocol as P
from orchestration.coordinator import Coordinator
from orchestration.scenarios import _Collector, _monotonic_ticks


class _FakeCollector(_Collector):
    """A collector with no network: we only exercise its view of the feed."""

    def __init__(self):
        object.__setattr__(self, "_skip_init", True)
        import threading
        self.lock = threading.Lock()
        self.states = []
        self.connections = []
        self.acks = []
        self.ack_times = []
        self.peers_left = []
        self.approx_bytes = 0
        self.ego_id = None

    def feed(self, tick, vehicle_ids):
        self.states.append((float(tick), {
            "type": "world_state", "tick": tick, "timestamp": tick * 0.05,
            "wall_time": 0.0,
            "vehicles": [{"id": i, "is_ego": False} for i in vehicle_ids]}))


# ── a departed car must stop being "seen" ────────────────────────────────────

def test_sees_vehicle_reflects_the_current_frame_not_all_history():
    # Was: vehicle_by_id scanned the whole retained history, so once a car had
    # appeared it was found forever and peer_departure's
    # departed_car_removed_from_world could never pass on real CARLA.
    c = _FakeCollector()
    c.feed(1, [42, 7])
    assert c.sees_vehicle(42) is True
    for tick in range(2, 6):
        c.feed(tick, [7])
    assert c.sees_vehicle(42) is False, "a car gone from the world must read as gone"
    assert c.sees_vehicle(7) is True


def test_lookback_can_tolerate_a_single_dropped_frame():
    c = _FakeCollector()
    c.feed(1, [42])
    c.feed(2, [])
    assert c.sees_vehicle(42) is False
    assert c.sees_vehicle(42, lookback=2) is True


# ── assertions must not pass vacuously ───────────────────────────────────────

@pytest.mark.parametrize("ticks", [[], [5]])
def test_monotonic_ticks_rejects_too_few_samples(ticks):
    # "all(zip(x, x[1:]))" is vacuously true for 0 or 1 items, which let four
    # separate assertions pass having checked nothing.
    assert _monotonic_ticks(ticks) is False


def test_monotonic_ticks_still_accepts_a_real_increasing_run():
    assert _monotonic_ticks([1, 2, 5, 9]) is True
    assert _monotonic_ticks([1, 2, 2]) is False
    assert _monotonic_ticks([3, 2, 1]) is False


# ── the coordinator must not hand out its own state ──────────────────────────

@pytest.fixture
def coord(tmp_path):
    return Coordinator(str(tmp_path / "state"))


def test_callers_cannot_mutate_coordinator_state_through_a_returned_run(coord):
    run = coord.create_run("connectivity", config={"k": 1})
    rid = run["run_id"]

    fetched = coord.get_run(rid)
    fetched["history"].append({"state": "forged"})
    fetched["config"]["k"] = 999
    fetched["actions"].append("garbage")

    clean = coord.get_run(rid)
    assert len(clean["history"]) == 1
    assert clean["config"]["k"] == 1
    assert clean["actions"] == []


def test_submitted_results_are_snapshotted(coord):
    run = coord.create_run("world_state")
    rid = run["run_id"]
    coord.transition(rid, P.LAB_PREPARING)
    coord.transition(rid, P.LAB_READY)
    coord.claim_next(P.ROLE_CLIENT, "c1")
    coord.transition(rid, P.RUNNING)

    result = P.result(rid, "world_state", P.PASS, "c1", 1.0,
                      assertions=[P.assertion("a", True)])
    coord.submit_result(rid, result)
    result["assertions"].append(P.assertion("sneaked_in_later", False))

    assert len(coord.get_run(rid)["result"]["assertions"]) == 1


# ── the deadline must cover execution, not the wait for a client ─────────────

def test_waiting_for_a_client_does_not_consume_the_run_timeout(tmp_path):
    # Was: the deadline started at create_run, so a client that started after
    # the timeout found every run already swept - breaking the documented
    # "start order does not matter".
    coord = Coordinator(str(tmp_path / "state"), run_timeout=2.0)
    run = coord.create_run("connectivity")
    rid = run["run_id"]
    coord.transition(rid, P.LAB_PREPARING)
    coord.transition(rid, P.LAB_READY)

    # Simulate a client that shows up long after the run was advertised.
    coord._runs[rid]["deadline"] = P.now() - 100

    claimed = coord.claim_next(P.ROLE_CLIENT, "late-client")
    assert claimed is not None, "a late client must still be able to claim work"
    assert coord.get_run(rid)["deadline"] > P.now(), "claiming restarts the clock"
    assert coord.sweep_stale() == []


# ── list_runs(limit=0) ───────────────────────────────────────────────────────

def test_limit_zero_returns_nothing_not_everything(coord):
    for _ in range(4):
        coord.create_run("connectivity")
    assert coord.list_runs(limit=0) == []
    assert len(coord.list_runs(limit=2)) == 2
