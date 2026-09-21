"""Agent-to-agent mailbox: ordered, durable, filterable by recipient.

This is what lets the session on each machine talk to the other directly
instead of a human relaying text between them.
"""

import pytest

from orchestration.coordinator import Coordinator
from orchestration.http_api import CoordinatorClient, CoordinatorServer


@pytest.fixture
def coord(tmp_path):
    return Coordinator(str(tmp_path / "state"))


def test_messages_are_sequenced(coord):
    a = coord.post_message("laptop", "lab", "first")
    b = coord.post_message("lab", "laptop", "second")
    assert (a["seq"], b["seq"]) == (1, 2)


def test_since_returns_only_newer_messages(coord):
    coord.post_message("laptop", "lab", "old")
    marker = coord.post_message("laptop", "lab", "newer")["seq"]
    coord.post_message("laptop", "lab", "newest")
    later = coord.list_messages(since=marker)
    assert [m["text"] for m in later] == ["newest"]


def test_recipient_filter_includes_broadcasts(coord):
    coord.post_message("laptop", "lab", "for lab")
    coord.post_message("laptop", "client", "for client")
    coord.post_message("laptop", "all", "for everyone")
    lab = [m["text"] for m in coord.list_messages(to="lab")]
    assert lab == ["for lab", "for everyone"]


def test_messages_survive_a_coordinator_restart(tmp_path):
    first = Coordinator(str(tmp_path / "state"))
    first.post_message("lab", "laptop", "persisted note")

    second = Coordinator(str(tmp_path / "state"))
    assert [m["text"] for m in second.list_messages()] == ["persisted note"]
    # sequence must continue, not restart and collide
    assert second.post_message("laptop", "lab", "next")["seq"] == 2


def test_mailbox_round_trip_over_http(tmp_path):
    coordinator = Coordinator(str(tmp_path / "state"))
    srv = CoordinatorServer(coordinator, "127.0.0.1", 0).start()
    try:
        api = CoordinatorClient(f"http://127.0.0.1:{srv.port}")
        sent = api.post_message("laptop-agent", "lab", "pull 18f3e31 and rerun mirror")
        assert sent["seq"] == 1
        got = api.messages(to="lab")
        assert got[0]["text"] == "pull 18f3e31 and rerun mirror"
        assert got[0]["from"] == "laptop-agent"
        # a reader that has already seen #1 gets nothing new
        assert api.messages(since=1, to="lab") == []
    finally:
        srv.stop()


def test_empty_message_is_rejected(tmp_path):
    coordinator = Coordinator(str(tmp_path / "state"))
    srv = CoordinatorServer(coordinator, "127.0.0.1", 0).start()
    try:
        api = CoordinatorClient(f"http://127.0.0.1:{srv.port}")
        with pytest.raises(Exception):
            api.post_message("laptop", "lab", "")
    finally:
        srv.stop()
