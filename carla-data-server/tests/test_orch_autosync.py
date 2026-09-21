"""Phase 5 retry budget and opt-in gating in the lab worker.

The git safety rules live in test_orch_gitsync.py; these cover the policy
wrapped around them: retries are bounded, passing scenarios are not retried,
and remotely-triggered code sync is refused unless explicitly enabled.
"""

import pytest

from orchestration import protocol as P
from orchestration.config import Config
from orchestration.lab_worker import LabWorker


class FakeApi:
    """Records what the worker did, without a coordinator or network."""

    def __init__(self, runs=None):
        self._runs = list(runs or [])
        self.created = []
        self.transitions = []
        self.messages = []
        self.completed_actions = []

    def list_runs(self, limit=50):
        return list(self._runs)

    def create_run(self, scenario, suite_id="", config=None, created_by="", timeout=None):
        run = {"run_id": f"run-{len(self.created)}", "scenario": scenario,
               "suite_id": suite_id, "config": config or {}, "state": P.CREATED}
        self.created.append(run)
        return run

    def transition(self, run_id, to_state, actor="", detail="", expected_from=None):
        self.transitions.append((run_id, to_state, detail))
        return {"run_id": run_id, "state": to_state}

    def post_message(self, sender, to, text, kind="note"):
        self.messages.append({"from": sender, "to": to, "text": text, "kind": kind})
        return self.messages[-1]

    def complete_action(self, run_id, action_id, ok, detail=""):
        self.completed_actions.append((action_id, ok, detail))
        return {"action_id": action_id, "ok": ok, "detail": detail}

    def next_action(self, run_id, actor=""):
        return None


def _worker(api, tmp_path, **kwargs):
    kwargs.setdefault("manage_server", False)
    worker = LabWorker(Config.from_env(state_dir=str(tmp_path)), api, **kwargs)
    worker._prepare = lambda scenario: (True, "stubbed ready")
    return worker


def test_failed_scenarios_are_requeued_after_a_sync(tmp_path):
    api = FakeApi([{"run_id": "r1", "scenario": "world_state", "state": P.FAIL,
                    "suite_id": "s1", "config": {}}])
    worker = _worker(api, tmp_path, auto_sync=True)
    worker._requeue_failures({"after": "abc123def", "commits": ["x"]})
    assert [r["scenario"] for r in api.created] == ["world_state"]
    assert any(state == P.LAB_READY for _, state, _ in api.transitions)


def test_passing_scenarios_are_not_requeued(tmp_path):
    # A later PASS means the earlier failure is already fixed.
    api = FakeApi([{"run_id": "r1", "scenario": "world_state", "state": P.FAIL},
                   {"run_id": "r2", "scenario": "world_state", "state": P.PASS}])
    worker = _worker(api, tmp_path, auto_sync=True)
    worker._requeue_failures({"after": "abc123def", "commits": []})
    assert api.created == []


def test_errored_scenarios_are_also_retried(tmp_path):
    api = FakeApi([{"run_id": "r1", "scenario": "mirror", "state": P.ERROR}])
    worker = _worker(api, tmp_path, auto_sync=True)
    worker._requeue_failures({"after": "abc123def", "commits": []})
    assert [r["scenario"] for r in api.created] == ["mirror"]


def test_retries_are_bounded_and_then_announced(tmp_path):
    api = FakeApi([{"run_id": "r1", "scenario": "world_state", "state": P.FAIL}])
    worker = _worker(api, tmp_path, auto_sync=True, max_retries=2)
    for _ in range(4):
        worker._requeue_failures({"after": "abc123def", "commits": []})
    assert len(api.created) == 2, "must stop at max_retries, not loop forever"
    assert any("Needs a human" in m["text"] for m in api.messages)


def test_each_scenario_gets_its_own_budget(tmp_path):
    api = FakeApi([{"run_id": "r1", "scenario": "world_state", "state": P.FAIL},
                   {"run_id": "r2", "scenario": "reconnect", "state": P.FAIL}])
    worker = _worker(api, tmp_path, auto_sync=True, max_retries=1)
    worker._requeue_failures({"after": "abc", "commits": []})
    assert sorted(r["scenario"] for r in api.created) == ["reconnect", "world_state"]


# ── opt-in gating ────────────────────────────────────────────────────────────

def test_sync_repo_action_is_refused_when_auto_sync_is_off(tmp_path):
    api = FakeApi()
    api.next_action = lambda run_id, actor="": {
        "action_id": "act-1", "action": "sync_repo", "params": {}}
    worker = _worker(api, tmp_path, auto_sync=False)
    worker._service_actions("r1")
    action_id, ok, detail = api.completed_actions[0]
    assert ok is False
    assert "disabled on this worker" in detail


def test_unknown_actions_are_refused(tmp_path):
    api = FakeApi()
    api.next_action = lambda run_id, actor="": {
        "action_id": "act-1", "action": "rm_rf_everything", "params": {}}
    worker = _worker(api, tmp_path, auto_sync=True)
    worker._service_actions("r1")
    action_id, ok, detail = api.completed_actions[0]
    assert ok is False
    assert "unknown lab action" in detail


def test_auto_sync_defaults_to_off(tmp_path):
    assert _worker(FakeApi(), tmp_path).auto_sync is False


def test_diverged_upstream_halts_instead_of_syncing(tmp_path, monkeypatch):
    api = FakeApi()
    worker = _worker(api, tmp_path, auto_sync=True)
    monkeypatch.setattr("orchestration.lab_worker.gitsync.remote_is_ahead",
                        lambda repo: {"ahead": False, "reason": "diverged - not a "
                                      "fast-forward", "head": "a" * 40,
                                      "remote_head": "b" * 40})
    called = []
    monkeypatch.setattr("orchestration.lab_worker.gitsync.sync_to",
                        lambda *a, **k: called.append(1))
    worker._maybe_sync()
    assert not called, "must never sync across diverged history"
    assert any("halted" in m["text"] for m in api.messages)
