"""Run-state vocabulary and result schema."""

import pytest

from orchestration import protocol as P


def test_every_state_has_a_transition_entry():
    for state in P.ALLOWED_TRANSITIONS:
        for target in P.ALLOWED_TRANSITIONS[state]:
            assert target in P.ALLOWED_TRANSITIONS, f"{state} -> unknown {target}"


def test_terminal_states_are_dead_ends():
    for state in P.TERMINAL_STATES:
        assert P.ALLOWED_TRANSITIONS[state] == frozenset()
        assert P.is_terminal(state)


def test_happy_path_is_reachable_in_order():
    path = [P.CREATED, P.LAB_PREPARING, P.LAB_READY, P.WAITING_FOR_CLIENT,
            P.CLIENT_READY, P.RUNNING, P.COLLECTING, P.PASS]
    for src, dst in zip(path, path[1:]):
        P.check_transition(src, dst)  # must not raise


def test_error_is_reachable_from_every_live_state():
    for state, targets in P.ALLOWED_TRANSITIONS.items():
        if not P.is_terminal(state):
            assert P.ERROR in targets, f"{state} cannot fail"


def test_check_transition_rejects_unknown_states():
    with pytest.raises(P.TransitionError):
        P.check_transition("banana", P.PASS)
    with pytest.raises(P.TransitionError):
        P.check_transition(P.CREATED, "banana")


def test_skipping_is_not_possible_once_running():
    with pytest.raises(P.TransitionError):
        P.check_transition(P.RUNNING, P.SKIPPED)


# ── results ──────────────────────────────────────────────────────────────────

def test_result_requires_a_terminal_status():
    with pytest.raises(ValueError):
        P.result("r1", "connectivity", P.RUNNING, "w", 1.0)


def test_result_shape_matches_the_documented_schema():
    out = P.result("r1", "world_state", P.FAIL, "client-1", 12.345,
                   metrics={"hz": 20.1}, assertions=[P.assertion("a", False, 1, 2)],
                   errors=[{"kind": "x", "message": "y"}], artifacts=["client.log"])
    assert set(out) == {"run_id", "scenario", "status", "worker", "duration_seconds",
                        "metrics", "assertions", "errors", "artifacts"}
    assert out["duration_seconds"] == 12.345


def test_assertion_carries_expected_and_observed():
    a = P.assertion("rate", False, expected="20Hz", observed="3Hz", detail="slow link")
    assert a["passed"] is False
    assert a["expected"] == "20Hz" and a["observed"] == "3Hz"


def test_all_passing_assertions_give_pass():
    assert P.status_from_assertions([P.assertion("a", True),
                                     P.assertion("b", True)]) == P.PASS


def test_any_failing_assertion_gives_fail():
    assert P.status_from_assertions([P.assertion("a", True),
                                     P.assertion("b", False)]) == P.FAIL


def test_checking_nothing_is_an_error_not_a_pass():
    # "the process ran without crashing" must never be reported as a pass.
    assert P.status_from_assertions([]) == P.ERROR


def test_run_ids_are_unique():
    assert len({P.new_run_id() for _ in range(50)}) == 50
