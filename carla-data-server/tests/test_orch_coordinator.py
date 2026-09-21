"""Coordinator state machine, claiming, results, actions, workers, sweeping.

Pure logic - no HTTP, no CARLA, no network.
"""

import pytest

from orchestration import protocol as P
from orchestration.coordinator import Coordinator


@pytest.fixture
def coord(tmp_path):
    return Coordinator(str(tmp_path / "state"), run_timeout=60.0)


def _advance_to_waiting(coord, scenario="connectivity"):
    run = coord.create_run(scenario, suite_id="s1", created_by="lab-1")
    coord.transition(run["run_id"], P.LAB_PREPARING, actor="lab-1")
    coord.transition(run["run_id"], P.LAB_READY, actor="lab-1")
    return coord.get_run(run["run_id"])


# ── transitions ──────────────────────────────────────────────────────────────

def test_new_run_starts_in_created(coord):
    run = coord.create_run("connectivity")
    assert run["state"] == P.CREATED
    assert run["result"] is None
    assert run["history"][0]["state"] == P.CREATED


def test_lab_ready_auto_advertises_to_waiting_for_client(coord):
    run = _advance_to_waiting(coord)
    # LAB_READY is recorded, then the coordinator advertises the run itself.
    states = [h["state"] for h in run["history"]]
    assert states == [P.CREATED, P.LAB_PREPARING, P.LAB_READY, P.WAITING_FOR_CLIENT]
    assert run["state"] == P.WAITING_FOR_CLIENT


def test_illegal_transition_is_rejected(coord):
    run = coord.create_run("connectivity")
    with pytest.raises(P.TransitionError):
        coord.transition(run["run_id"], P.RUNNING, actor="client-1")
    assert coord.get_run(run["run_id"])["state"] == P.CREATED


def test_expected_from_guards_against_racing_workers(coord):
    run = _advance_to_waiting(coord)
    with pytest.raises(P.TransitionError):
        coord.transition(run["run_id"], P.CLIENT_READY, expected_from=P.LAB_PREPARING)


def test_terminal_runs_are_frozen(coord):
    run = _advance_to_waiting(coord)
    rid = run["run_id"]
    coord.claim_next(P.ROLE_CLIENT, "client-1")
    coord.transition(rid, P.RUNNING, actor="client-1")
    coord.submit_result(rid, P.result(rid, "connectivity", P.PASS, "client-1", 1.0,
                                      assertions=[P.assertion("x", True)]))
    with pytest.raises(P.TransitionError):
        coord.transition(rid, P.RUNNING, actor="client-1")


# ── claiming ─────────────────────────────────────────────────────────────────

def test_claim_moves_waiting_run_to_client_ready(coord):
    _advance_to_waiting(coord)
    claimed = coord.claim_next(P.ROLE_CLIENT, "client-1")
    assert claimed["state"] == P.CLIENT_READY
    assert claimed["claimed_by"] == "client-1"


def test_claim_returns_none_when_nothing_is_waiting(coord):
    coord.create_run("connectivity")  # still CREATED, not advertised
    assert coord.claim_next(P.ROLE_CLIENT, "client-1") is None


def test_a_run_is_only_claimed_once(coord):
    _advance_to_waiting(coord)
    first = coord.claim_next(P.ROLE_CLIENT, "client-1")
    second = coord.claim_next(P.ROLE_CLIENT, "client-2")
    assert first is not None and second is None


def test_claims_are_fifo(coord):
    a = _advance_to_waiting(coord, "connectivity")
    b = _advance_to_waiting(coord, "world_state")
    assert coord.claim_next(P.ROLE_CLIENT, "c1")["run_id"] == a["run_id"]
    assert coord.claim_next(P.ROLE_CLIENT, "c2")["run_id"] == b["run_id"]


def test_lab_role_cannot_claim(coord):
    _advance_to_waiting(coord)
    with pytest.raises(ValueError):
        coord.claim_next(P.ROLE_LAB, "lab-1")


# ── results ──────────────────────────────────────────────────────────────────

def _run_to_running(coord, scenario="world_state"):
    run = _advance_to_waiting(coord, scenario)
    coord.claim_next(P.ROLE_CLIENT, "client-1")
    coord.transition(run["run_id"], P.RUNNING, actor="client-1")
    return run["run_id"]


def test_submit_result_passes_through_collecting(coord):
    rid = _run_to_running(coord)
    final = coord.submit_result(rid, P.result(rid, "world_state", P.FAIL, "client-1", 2.5,
                                              assertions=[P.assertion("tick", False, 1, 0)]))
    assert final["state"] == P.FAIL
    assert [h["state"] for h in final["history"]][-2:] == [P.COLLECTING, P.FAIL]
    assert final["result"]["assertions"][0]["name"] == "tick"


def test_non_terminal_result_status_is_rejected(coord):
    rid = _run_to_running(coord)
    with pytest.raises(ValueError):
        coord.submit_result(rid, {"status": P.RUNNING})


def test_result_cannot_be_submitted_twice(coord):
    rid = _run_to_running(coord)
    coord.submit_result(rid, P.result(rid, "world_state", P.PASS, "c", 1.0,
                                      assertions=[P.assertion("a", True)]))
    with pytest.raises(P.TransitionError):
        coord.submit_result(rid, P.result(rid, "world_state", P.FAIL, "c", 1.0,
                                          assertions=[P.assertion("a", False)]))


def test_skipped_is_a_valid_outcome(coord):
    rid = _run_to_running(coord, "mirror")
    final = coord.submit_result(rid, P.result(rid, "mirror", P.SKIPPED, "client-1", 0.1,
                                              metrics={"reason": "no shadow CARLA"}))
    assert final["state"] == P.SKIPPED


# ── evidence and actions ─────────────────────────────────────────────────────

def test_evidence_is_written_and_indexed(coord):
    rid = _run_to_running(coord)
    entry = coord.attach_evidence(rid, "client.log", "line one\nline two", actor="client-1")
    assert entry["bytes"] > 0
    with open(entry["path"], encoding="utf-8") as f:
        assert "line two" in f.read()
    assert coord.get_run(rid)["evidence"][0]["name"] == "client.log"


def test_lab_action_round_trip(coord):
    rid = _run_to_running(coord, "reconnect")
    requested = coord.request_action(rid, "restart_data_server", actor="client-1")
    assert requested["state"] == "pending"

    claimed = coord.next_action(rid, actor="lab-1")
    assert claimed["action_id"] == requested["action_id"]
    assert coord.next_action(rid, actor="lab-1") is None, "already claimed"

    done = coord.complete_action(rid, claimed["action_id"], True, "restarted")
    assert done["state"] == "done" and done["ok"] is True


def test_completing_unknown_action_raises(coord):
    rid = _run_to_running(coord)
    with pytest.raises(KeyError):
        coord.complete_action(rid, "act-99", True)


# ── workers ──────────────────────────────────────────────────────────────────

def test_heartbeat_registers_and_ages(coord):
    coord.heartbeat("client-1", P.ROLE_CLIENT, {"scenarios": ["connectivity"]})
    workers = coord.list_workers()
    assert workers[0]["worker_id"] == "client-1"
    assert workers[0]["stale"] is False


def test_unknown_role_rejected(coord):
    with pytest.raises(ValueError):
        coord.heartbeat("w", "supervisor")


# ── timeouts and persistence ─────────────────────────────────────────────────

def test_stale_run_is_swept_to_error_with_evidence(tmp_path):
    coord = Coordinator(str(tmp_path / "state"), run_timeout=-1.0)  # already expired
    run = coord.create_run("connectivity")
    swept = coord.sweep_stale()
    assert [s["run_id"] for s in swept] == [run["run_id"]]
    final = coord.get_run(run["run_id"])
    assert final["state"] == P.ERROR
    assert final["result"]["errors"][0]["kind"] == "timeout"
    assert final["result"]["errors"][0]["state_when_timed_out"] == P.CREATED


def test_sweep_leaves_terminal_and_live_runs_alone(coord):
    rid = _run_to_running(coord)
    coord.submit_result(rid, P.result(rid, "world_state", P.PASS, "c", 1.0,
                                      assertions=[P.assertion("a", True)]))
    assert coord.sweep_stale() == []
    assert coord.get_run(rid)["state"] == P.PASS


def test_runs_survive_a_coordinator_restart(tmp_path):
    first = Coordinator(str(tmp_path / "state"))
    run = first.create_run("connectivity", suite_id="s1")
    first.transition(run["run_id"], P.LAB_PREPARING)

    second = Coordinator(str(tmp_path / "state"))
    reloaded = second.get_run(run["run_id"])
    assert reloaded is not None
    assert reloaded["state"] == P.LAB_PREPARING
    assert reloaded["suite_id"] == "s1"


def test_status_snapshot_separates_active_from_recent(coord):
    _advance_to_waiting(coord)
    status = coord.status()
    assert status["total_runs"] == 1
    assert status["counts_by_state"][P.WAITING_FOR_CLIENT] == 1
    assert status["active_runs"][0]["scenario"] == "connectivity"
