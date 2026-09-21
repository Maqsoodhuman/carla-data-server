"""Prerequisite checks that explain their own failures.

`python -m orchestration doctor --role lab|client` is the first thing to run
on either machine, and the lab worker runs the lab checks itself before
advertising a run as ready.
"""

import importlib
import os
import socket
import sys

from . import REPO_ROOT
from . import protocol as P


def _check(name, ok, detail, fix=""):
    return {"name": name, "ok": bool(ok), "detail": detail, "fix": fix}


def _tcp(host, port, timeout=5.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, f"{host}:{port} reachable"
    except OSError as exc:
        return False, f"{host}:{port} unreachable ({type(exc).__name__}: {exc})"


def check_python():
    ok = sys.version_info[:2] >= (3, 8)
    return _check("python_version", ok, f"Python {sys.version.split()[0]}",
                  "" if ok else "Python 3.8+ required")


def check_websockets():
    try:
        import websockets  # noqa: F401
    except ImportError:
        return _check("websockets_importable", False, "websockets not installed",
                      "pip install -r requirements.txt")
    # server.py uses the legacy asyncio API, dropped in websockets 14+.
    try:
        importlib.import_module("websockets.server")
        from websockets.server import WebSocketServerProtocol  # noqa: F401
        legacy_ok = True
        detail = "websockets present with the legacy asyncio API server.py needs"
    except (ImportError, AttributeError) as exc:
        legacy_ok = False
        detail = f"websockets is installed but lacks the legacy API server.py uses ({exc})"
    return _check("websockets_legacy_api", legacy_ok, detail,
                  "" if legacy_ok else "pip install 'websockets<14' (server.py uses "
                                       "websockets.serve / WebSocketServerProtocol)")


def check_carla_module():
    try:
        import carla  # noqa: F401
        return _check("carla_python_api", True, "carla PythonAPI importable")
    except ImportError:
        return _check("carla_python_api", False,
                      "carla PythonAPI not importable (server.py would run in STUB mode)",
                      "activate venv/ (which has CARLA 0.9.16) rather than venv-stub/")


def check_repo_layout():
    needed = ["server/server.py", "client/client.py", "client/wire.py",
              "bridges/carla_mirror_client.py"]
    missing = [p for p in needed if not os.path.isfile(os.path.join(REPO_ROOT, p))]
    return _check("repo_layout", not missing,
                  f"repo root {REPO_ROOT}" if not missing else f"missing {missing}",
                  "" if not missing else "run from the inner carla-data-server/ directory")


def check_carla_server(cfg):
    ok, detail = _tcp(cfg.carla_host, cfg.carla_port)
    return _check("carla_simulator", ok, detail,
                  "" if ok else f"start CARLA listening on {cfg.carla_host}:{cfg.carla_port}")


def check_data_server(cfg, host=None):
    host = host or cfg.lab_host
    ok, detail = _tcp(host, cfg.data_server_port)
    return _check("data_server", ok, detail,
                  "" if ok else "start it with `python -m orchestration lab` (which manages "
                                "it) or `python server/server.py`")


def check_coordinator(cfg):
    ok, detail = _tcp(cfg.lab_host, cfg.coordinator_port)
    return _check("coordinator", ok, detail,
                  "" if ok else f"start the lab worker on {cfg.lab_host}, and check "
                                f"LAB_HOST/COORDINATOR_PORT plus any firewall between "
                                f"the machines")


def check_shadow_carla(cfg):
    ok, detail = _tcp(cfg.shadow_carla_host, cfg.shadow_carla_port)
    return _check("shadow_carla", ok, detail,
                  "" if ok else "optional: only the `mirror` scenario needs it; it is "
                                "SKIPPED (never failed) when absent")


def run_doctor(cfg, role: str) -> dict:
    checks = [check_python(), check_repo_layout(), check_websockets()]
    if role == P.ROLE_LAB:
        checks += [check_carla_module(), check_carla_server(cfg),
                   check_data_server(cfg, host="127.0.0.1")]
    else:
        checks += [check_coordinator(cfg), check_data_server(cfg), check_shadow_carla(cfg)]

    required = {c["name"] for c in checks} - {"shadow_carla", "carla_python_api"}
    blocking = [c for c in checks if not c["ok"] and c["name"] in required]
    return {
        "role": role,
        "config": cfg.as_dict(),
        "checks": checks,
        "ok": not blocking,
        "blocking": [c["name"] for c in blocking],
    }


def format_doctor(report: dict) -> str:
    lines = [f"doctor: role={report['role']}  ->  "
             f"{'OK' if report['ok'] else 'BLOCKED: ' + ', '.join(report['blocking'])}", ""]
    for check in report["checks"]:
        mark = "PASS" if check["ok"] else "FAIL"
        lines.append(f"  [{mark}] {check['name']}: {check['detail']}")
        if not check["ok"] and check["fix"]:
            lines.append(f"         fix: {check['fix']}")
    cfg = report["config"]
    lines += ["", "  coordinator (client connects to): " + cfg["coordinator_url"],
              "  data server (client connects to): " + cfg["data_server_url"],
              "  carla: %s:%s" % (cfg["carla_host"], cfg["carla_port"])]
    return "\n".join(lines)
