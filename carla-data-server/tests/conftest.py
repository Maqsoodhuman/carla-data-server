"""Shared pytest setup: put server/ and client/ on sys.path so tests can
import server.py and wire.py directly, the same way bridges/scripts do
(see bridges/carla_mirror_client.py's sys.path.insert convention)."""

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _sub in ("server", "client"):
    _path = os.path.join(_ROOT, _sub)
    if _path not in sys.path:
        sys.path.insert(0, _path)
