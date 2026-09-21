"""Two-machine autonomous test orchestration for carla-data-server.

A LAB machine runs CARLA + the data server + a coordinator; a CLIENT machine
connects and executes scenarios against it. The coordinator is the single
source of truth for run state, so neither machine needs a human relaying
information between them.

The LLM agents supervising each machine sit *above* this layer - the loop
below works on its own:

    Claude/Codex  ->  python -m orchestration {lab,client}
                  ->  coordinator (HTTP, run state machine)
                  ->  scenario execution
                  ->  structured result + evidence
"""

import os
import sys

# The repo's existing convention for reaching sibling source dirs (see
# bridges/carla_mirror_client.py). Lets orchestration modules `import wire`
# and `import client` without packaging changes.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _sub in ("client", "server"):
    _path = os.path.join(_ROOT, _sub)
    if _path not in sys.path:
        sys.path.insert(0, _path)

REPO_ROOT = _ROOT
VERSION = "1"
