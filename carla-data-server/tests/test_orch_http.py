"""HTTP transport + a full lab/client handshake over it, with a fake scenario.

Proves the coordination loop itself (advertise -> claim -> run -> result ->
next scenario) works without needing CARLA, a data server, or a second
machine.
"""

import threading
import time

import pytest

from orchestration import protocol as P
from orchestration import scenarios as S
from orchestration.coordinator import Coordinator
from orchestration.http_api import CoordinatorClient, CoordinatorError, CoordinatorServer


@pytest.fixture
def server(tmp_path):
    coordinator = Coordinator(str(tmp_path / "state"), run_timeout=30.0)
    srv = CoordinatorServer(coordinator, "127.0.0.1", 0).start()
    yield srv, coordinator
    srv.stop()


@pytest.fixture
def api(server):
    srv, _ = server
    return CoordinatorClient(f"http://127.0.0.1:{srv.port}")


def test_health(api):
    assert api.health()["ok"] is True


def test_create_and_fetch_run_over_http(api):
    run = api.create_run("connectivity", suite_id="s1", created_by="lab-1")
    assert api.get_run(run["run_id"])["scenario"] == "connectivity"


def test_transition_and_claim_over_http(api):
    run = api.create_run("connectivity")
    api.transition(run["run_id"], P.LAB_PREPARING, actor="lab-1")
    api.transition(run["run_id"], P.LAB_READY, actor="lab-1")
    claimed = api.claim("client-1")
    assert claimed["run_id"] == run["run_id"]
    assert claimed["state"] == P.CLIENT_READY


def test_claim_returns_none_when_idle(api):
    assert api.claim("client-1") is None


def test_illegal_transition_surfaces_as_an_error(api):
    run = api.create_run("connectivity")
    with pytest.raises(CoordinatorError) as exc:
        api.transition(run["run_id"], P.RUNNING, actor="client-1")
    assert "409" in str(exc.value)


def test_unknown_run_is_404(api):
    with pytest.raises(CoordinatorError) as exc:
        api.get_run("run-does-not-exist")
    assert "404" in str(exc.value)


def test_result_and_evidence_round_trip(api):
    run = api.create_run("world_state")
    rid = run["run_id"]
    api.transition(rid, P.LAB_PREPARING)
    api.transition(rid, P.LAB_READY)
    api.claim("client-1")
    api.transition(rid, P.RUNNING, actor="client-1")
    api.attach_evidence(rid, "client.log", "hello from the client", actor="client-1")
    final = api.submit_result(rid, P.result(rid, "world_state", P.PASS, "client-1", 3.0,
                                            metrics={"hz": 20.0},
                                            assertions=[P.assertion("rate", True)]))
    assert final["state"] == P.PASS
    assert final["result"]["metrics"]["hz"] == 20.0
    assert final["evidence"][0]["name"] == "client.log"


def test_lab_action_round_trip_over_http(api):
    run = api.create_run("reconnect")
    rid = run["run_id"]
    requested = api.request_action(rid, "restart_data_server", actor="client-1")
    claimed = api.next_action(rid, actor="lab-1")
    assert claimed["action_id"] == requested["action_id"]
    assert api.next_action(rid, actor="lab-1") is None
    done = api.complete_action(rid, claimed["action_id"], True, "restarted")
    assert done["ok"] is True


def test_heartbeat_and_worker_listing(api):
    api.heartbeat("lab-1", P.ROLE_LAB, {"suite": ["connectivity"]})
    api.heartbeat("client-1", P.ROLE_CLIENT)
    roles = sorted(w["role"] for w in api.workers())
    assert roles == [P.ROLE_CLIENT, P.ROLE_LAB]


def test_bad_role_is_rejected(api):
    with pytest.raises(CoordinatorError):
        api.heartbeat("w", "supervisor")


def test_unreachable_coordinator_gives_an_actionable_message():
    api = CoordinatorClient("http://127.0.0.1:1", timeout=2.0)
    with pytest.raises(CoordinatorError) as exc:
        api.health()
    assert "LAB_HOST" in str(exc.value)


# ── the actual two-worker loop, with scenario execution stubbed out ─────────

def test_lab_and_client_workers_complete_a_suite_without_a_human(server, monkeypatch):
    """Lab advertises three runs; a client worker picks each up and reports.
    Only the scenario bodies are faked - the coordination is the real thing."""
    srv, coordinator = server
    from orchestration.client_worker import ClientWorker
    from orchestration.config import Config

    executed = []

    def fake_run_scenario(name, ctx):
        executed.append(name)
        ctx.log(f"pretending to run {name}")
        if name == "world_state":
            return S.Outcome(P.FAIL, {"message_count": 3},
                             [P.assertion("received_expected_messages", False, 20, 3)])
        return S.Outcome(P.PASS, {"ok": 1}, [P.assertion("reachable", True)])

    monkeypatch.setattr(S, "run_scenario", fake_run_scenario)

    cfg = Config.from_env(state_dir=coordinator.state_dir)
    api = CoordinatorClient(f"http://127.0.0.1:{srv.port}")
    worker = ClientWorker(cfg, api, worker_id="client-test", idle_timeout=None)
    thread = threading.Thread(target=worker.serve_forever, daemon=True)
    thread.start()

    suite = ["connectivity", "world_state", "sustained_stream"]
    finished = []
    try:
        for scenario in suite:
            run = api.create_run(scenario, suite_id="suite-1", created_by="lab-test")
            api.transition(run["run_id"], P.LAB_PREPARING, actor="lab-test")
            api.transition(run["run_id"], P.LAB_READY, actor="lab-test")
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                current = api.get_run(run["run_id"])
                if P.is_terminal(current["state"]):
                    finished.append(current)
                    break
                time.sleep(0.05)
            else:
                pytest.fail(f"{scenario} never reached a terminal state")
    finally:
        worker.stop()
        thread.join(timeout=5)

    assert executed == suite, "client worker must pick up each advertised run in order"
    assert [r["state"] for r in finished] == [P.PASS, P.FAIL, P.PASS]

    failing = finished[1]
    assert failing["result"]["assertions"][0]["expected"] == 20
    assert failing["result"]["assertions"][0]["observed"] == 3
    assert any(e["name"] == "client.log" for e in failing["evidence"]), \
        "client-side evidence must reach the lab side for diagnosis"
