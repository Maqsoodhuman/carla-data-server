"""Static regression guard: no client/, bridges/, scripts/, or server/ file
(other than client/wire.py itself) should hardcode a message-type or command
string literal instead of importing the corresponding wire.MSG_*/wire.CMD_*
constant. Hardcoding a literal is exactly how a consumer silently drifts from
the protocol wire.py exists to keep in sync - see docs/wire-protocol.md.

This test is what should have caught scripts/interactive_driver.py's
send_spawn_sensor override, which hardcoded {"type": "spawn_sensor", ...}
instead of using wire.CMD_SPAWN_SENSOR.
"""

import os
import re

import wire

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SCAN_DIRS = ("client", "bridges", "scripts", "server")
_EXCLUDE_FILES = {os.path.join(_ROOT, "client", "wire.py")}

_KNOWN_TYPE_STRINGS = wire.SERVER_MSG_TYPES | wire.VALID_COMMANDS
# Matches "type": "<literal>" or 'type': '<literal>' - a dict-literal type
# tag, not a prose mention of the same word in a docstring/comment.
_TYPE_LITERAL_RE = re.compile(r"""["']type["']\s*:\s*["'](\w+)["']""")


def _iter_python_files():
    for sub in _SCAN_DIRS:
        base = os.path.join(_ROOT, sub)
        if not os.path.isdir(base):
            continue
        for dirpath, _dirnames, filenames in os.walk(base):
            for name in filenames:
                if name.endswith(".py"):
                    path = os.path.join(dirpath, name)
                    if path not in _EXCLUDE_FILES:
                        yield path


def test_no_hardcoded_protocol_type_literals():
    violations = []
    for path in _iter_python_files():
        with open(path, "r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                for match in _TYPE_LITERAL_RE.finditer(line):
                    literal = match.group(1)
                    if literal in _KNOWN_TYPE_STRINGS:
                        violations.append(
                            f"{os.path.relpath(path, _ROOT)}:{lineno}: "
                            f'hardcoded "type": "{literal}" - use a wire.MSG_*/wire.CMD_* constant'
                        )

    assert not violations, "Protocol literal drift found:\n" + "\n".join(violations)
