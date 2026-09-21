"""Run state machine vocabulary and result schema.

This module is pure data + validation: no I/O, no HTTP, no CARLA. The
coordinator enforces these transitions, and both workers speak in these terms.
"""

import time
import uuid

# ── run states ───────────────────────────────────────────────────────────────

CREATED = "created"
LAB_PREPARING = "lab_preparing"
LAB_READY = "lab_ready"
WAITING_FOR_CLIENT = "waiting_for_client"
CLIENT_READY = "client_ready"
RUNNING = "running"
COLLECTING = "collecting"

PASS = "pass"
FAIL = "fail"
ERROR = "error"
SKIPPED = "skipped"

TERMINAL_STATES = frozenset({PASS, FAIL, ERROR, SKIPPED})

# A run can be abandoned (ERROR) from any live state; SKIPPED is only reachable
# where a precondition can still legitimately rule the scenario out.
ALLOWED_TRANSITIONS = {
    CREATED: frozenset({LAB_PREPARING, ERROR, SKIPPED}),
    LAB_PREPARING: frozenset({LAB_READY, ERROR, SKIPPED}),
    LAB_READY: frozenset({WAITING_FOR_CLIENT, ERROR}),
    WAITING_FOR_CLIENT: frozenset({CLIENT_READY, ERROR, SKIPPED}),
    CLIENT_READY: frozenset({RUNNING, ERROR, SKIPPED}),
    RUNNING: frozenset({COLLECTING, ERROR}),
    COLLECTING: frozenset({PASS, FAIL, ERROR, SKIPPED}),
    PASS: frozenset(),
    FAIL: frozenset(),
    ERROR: frozenset(),
    SKIPPED: frozenset(),
}

ROLE_LAB = "lab"
ROLE_CLIENT = "client"
ROLES = frozenset({ROLE_LAB, ROLE_CLIENT})


class TransitionError(Exception):
    """An illegal or conflicting state transition was requested."""


def is_terminal(state: str) -> bool:
    return state in TERMINAL_STATES


def check_transition(from_state: str, to_state: str):
    """Raise TransitionError unless from_state -> to_state is legal."""
    if from_state not in ALLOWED_TRANSITIONS:
        raise TransitionError(f"unknown current state {from_state!r}")
    if to_state not in ALLOWED_TRANSITIONS:
        raise TransitionError(f"unknown target state {to_state!r}")
    if to_state not in ALLOWED_TRANSITIONS[from_state]:
        raise TransitionError(f"illegal transition {from_state} -> {to_state}")


# ── ids and timestamps ───────────────────────────────────────────────────────

def new_run_id() -> str:
    return f"run-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"


def new_suite_id() -> str:
    return f"suite-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"


def now() -> float:
    return time.time()


def iso(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ts))


# ── results ──────────────────────────────────────────────────────────────────

def assertion(name: str, passed: bool, expected=None, observed=None, detail: str = "") -> dict:
    """One checked claim. `expected`/`observed` are what make a failure
    diagnosable by the agent on the other machine, so always fill them in."""
    return {
        "name": name,
        "passed": bool(passed),
        "expected": expected,
        "observed": observed,
        "detail": detail,
    }


def result(run_id: str, scenario: str, status: str, worker: str,
           duration_seconds: float, metrics: dict = None,
           assertions: list = None, errors: list = None,
           artifacts: list = None) -> dict:
    """Machine-readable scenario result. Metrics must only contain values that
    were actually measured - never synthesize a number that wasn't observed."""
    if status not in TERMINAL_STATES:
        raise ValueError(f"result status must be terminal, got {status!r}")
    return {
        "run_id": run_id,
        "scenario": scenario,
        "status": status,
        "worker": worker,
        "duration_seconds": round(float(duration_seconds), 3),
        "metrics": dict(metrics or {}),
        "assertions": list(assertions or []),
        "errors": list(errors or []),
        "artifacts": list(artifacts or []),
    }


def status_from_assertions(assertions: list) -> str:
    """PASS only if there is at least one assertion and all of them passed.
    An empty assertion list is an ERROR - a scenario that checked nothing has
    not demonstrated anything (process-running is not a pass)."""
    if not assertions:
        return ERROR
    return PASS if all(a.get("passed") for a in assertions) else FAIL
