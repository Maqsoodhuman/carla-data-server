"""Unit tests for client/wire.py in isolation - no server, no CARLA, no network."""

import wire


def test_validate_message_unknown_type_passes():
    # Forward-compat: a type we've never heard of must not be rejected.
    assert wire.validate_message({"type": "some_future_type"}) is True


def test_validate_message_known_type_missing_field_fails():
    for msg_type, required in wire.REQUIRED_FIELDS.items():
        incomplete = {"type": msg_type}
        assert wire.validate_message(incomplete) is False, msg_type
        complete = {"type": msg_type, **{f: "x" for f in required}}
        assert wire.validate_message(complete) is True, msg_type


def test_validate_message_non_dict_fails():
    assert wire.validate_message(["not", "a", "dict"]) is False
    assert wire.validate_message("also not a dict") is False
    assert wire.validate_message(None) is False


def test_parse_frame_valid_json():
    assert wire.parse_frame('{"type": "ping"}') == {"type": "ping"}


def test_parse_frame_invalid_json_returns_none():
    assert wire.parse_frame("not json") is None
    assert wire.parse_frame("") is None
    assert wire.parse_frame(None) is None


def test_make_subscribe_shape():
    msg = wire.make_subscribe(["vehicles", "sensors"])
    assert msg == {"type": wire.CMD_SUBSCRIBE, "payload": {"topics": ["vehicles", "sensors"]}}


def test_make_ping_shape():
    msg = wire.make_ping(123.456)
    assert msg == {"type": wire.CMD_PING, "payload": {"client_ts": 123.456}}


def test_valid_commands_and_topics_are_nonempty_strings():
    assert wire.VALID_COMMANDS
    assert wire.VALID_TOPICS
    assert all(isinstance(c, str) for c in wire.VALID_COMMANDS)
    assert all(isinstance(t, str) for t in wire.VALID_TOPICS)
