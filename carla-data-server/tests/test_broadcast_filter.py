"""Unit tests for BroadcastThread._filter - a pure staticmethod, so this
exercises per-client topic/is_ego isolation without a live server or
network. Added after discovering STUB mode can't exercise a real is_ego=True
case live: _stub_world_state always returns a fixed vehicle id=1, which never
matches a STUB spawn's synthetic actor id, so is_ego never goes True over an
actual STUB-mode connection - see docs/wire-protocol.md / tests runbook."""

from server import BroadcastThread


def _sample_world_state():
    return {
        "type": "world_state", "tick": 1, "timestamp": 0.05, "wall_time": 123.0,
        "vehicles": [
            {"id": 1, "type_id": "vehicle.a", "transform": {}, "velocity": {}, "angular_vel": {}, "is_ego": False},
            {"id": 2, "type_id": "vehicle.b", "transform": {}, "velocity": {}, "angular_vel": {}, "is_ego": False},
        ],
        "pedestrians": [{"id": 100}],
        "traffic_lights": [{"id": 200}],
        "sensors": [],
    }


def test_topic_filtering_excludes_unsubscribed_topics():
    ws = _sample_world_state()
    out = BroadcastThread._filter(ws, {"vehicles", "pedestrians"}, ego_actor_id=None)
    assert "pedestrians" in out
    assert "traffic_lights" not in out


def test_is_ego_set_correctly_for_the_matching_actor_only():
    ws = _sample_world_state()
    out = BroadcastThread._filter(ws, {"vehicles"}, ego_actor_id=2)
    is_ego = {v["id"]: v["is_ego"] for v in out["vehicles"]}
    assert is_ego == {1: False, 2: True}


def test_is_ego_all_false_when_client_has_no_ego():
    ws = _sample_world_state()
    out = BroadcastThread._filter(ws, {"vehicles"}, ego_actor_id=None)
    assert all(v["is_ego"] is False for v in out["vehicles"])


def test_filtering_does_not_mutate_shared_world_state():
    # BroadcastThread.run() reuses one world_state dict across every
    # connected client's _filter() call - a client-specific is_ego rewrite
    # must never leak back into the shared snapshot other clients also read.
    ws = _sample_world_state()
    BroadcastThread._filter(ws, {"vehicles"}, ego_actor_id=2)
    assert ws["vehicles"][0]["is_ego"] is False
    assert ws["vehicles"][1]["is_ego"] is False


def test_two_clients_with_different_ego_do_not_see_each_others_ego():
    ws = _sample_world_state()
    out_a = BroadcastThread._filter(ws, {"vehicles"}, ego_actor_id=1)
    out_b = BroadcastThread._filter(ws, {"vehicles"}, ego_actor_id=2)
    a_is_ego = {v["id"]: v["is_ego"] for v in out_a["vehicles"]}
    b_is_ego = {v["id"]: v["is_ego"] for v in out_b["vehicles"]}
    assert a_is_ego == {1: True, 2: False}
    assert b_is_ego == {1: False, 2: True}
