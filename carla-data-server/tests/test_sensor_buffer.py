"""Unit tests for server.SensorBuffer's dirty-tracking dedup logic in
isolation - importable without CARLA installed, since server.py already
guards its `import carla` at module load."""

import threading

from server import SensorBuffer


def test_fresh_frames_both_drained():
    buf = SensorBuffer()
    buf.update(1, {"actor_id": 1})
    buf.update(2, {"actor_id": 2})
    drained_ids = {f["actor_id"] for f in buf.drain()}
    assert drained_ids == {1, 2}


def test_second_drain_with_nothing_new_is_empty():
    buf = SensorBuffer()
    buf.update(1, {"actor_id": 1})
    buf.drain()
    assert buf.drain() == []


def test_only_updated_sensor_reappears():
    buf = SensorBuffer()
    buf.update(1, {"actor_id": 1})
    buf.update(2, {"actor_id": 2})
    buf.drain()

    buf.update(1, {"actor_id": 1, "data": b"new frame"})
    drained = buf.drain()
    assert [f["actor_id"] for f in drained] == [1]


def test_remove_then_readd_behaves_like_fresh_sensor():
    buf = SensorBuffer()
    buf.update(1, {"actor_id": 1, "data": b"old"})
    buf.drain()

    buf.remove(1)
    # No leftover dirty/latest state after remove.
    assert buf.drain() == []

    buf.update(1, {"actor_id": 1, "data": b"fresh"})
    drained = buf.drain()
    assert len(drained) == 1
    assert drained[0]["data"] == b"fresh"


def test_concurrent_update_and_drain_do_not_corrupt_state():
    buf = SensorBuffer()
    stop = threading.Event()
    errors = []

    def updater(sensor_id):
        try:
            while not stop.is_set():
                buf.update(sensor_id, {"actor_id": sensor_id})
        except Exception as e:  # pragma: no cover - failure path
            errors.append(e)

    threads = [threading.Thread(target=updater, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()

    for _ in range(200):
        drained = buf.drain()
        ids = [f["actor_id"] for f in drained]
        assert len(ids) == len(set(ids)), "drain() returned duplicate sensor ids"

    stop.set()
    for t in threads:
        t.join(timeout=2.0)

    assert not errors
