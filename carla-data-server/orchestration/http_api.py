"""HTTP transport for the coordinator: a stdlib server plus a tiny client.

Deliberately stdlib-only (no FastAPI/requests) so neither machine needs new
dependencies, and every endpoint is curl-able for a human or an agent
debugging by hand.
"""

import json
import logging
import re
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs

from . import protocol as P

log = logging.getLogger("orchestration.http")

SWEEP_INTERVAL = 5.0


class CoordinatorError(Exception):
    """Coordinator rejected a request or was unreachable."""


# ── server ───────────────────────────────────────────────────────────────────

def _make_handler(coordinator):
    routes = []

    def route(method, pattern):
        compiled = re.compile(f"^{pattern}$")

        def decorator(fn):
            routes.append((method, compiled, fn))
            return fn
        return decorator

    @route("GET", "/health")
    def _health(handler, match, query, body):
        return 200, {"ok": True, "service": "carla-data-server orchestration",
                     "version": P and "1", "time": P.now()}

    @route("GET", "/status")
    def _status(handler, match, query, body):
        return 200, coordinator.status()

    @route("GET", "/runs")
    def _list_runs(handler, match, query, body):
        try:
            limit = int(query.get("limit", ["50"])[0])
        except ValueError:
            return 400, {"error": "limit must be an integer"}
        status = query.get("status", [None])[0]
        suite = query.get("suite_id", [None])[0]
        return 200, {"runs": coordinator.list_runs(limit=limit, status=status,
                                                   suite_id=suite)}

    @route("POST", "/runs")
    def _create_run(handler, match, query, body):
        scenario = body.get("scenario")
        if not scenario:
            return 400, {"error": "scenario is required"}
        run = coordinator.create_run(
            scenario=scenario,
            suite_id=body.get("suite_id", ""),
            config=body.get("config"),
            created_by=body.get("created_by", ""),
            timeout=body.get("timeout"),
        )
        return 201, {"run": run}

    @route("GET", r"/runs/([\w.\-]+)")
    def _get_run(handler, match, query, body):
        run = coordinator.get_run(match.group(1))
        if run is None:
            return 404, {"error": "unknown run"}
        return 200, {"run": run}

    @route("POST", r"/runs/([\w.\-]+)/transition")
    def _transition(handler, match, query, body):
        try:
            run = coordinator.transition(
                match.group(1), body["to_state"],
                actor=body.get("actor", ""), detail=body.get("detail", ""),
                expected_from=body.get("expected_from"),
            )
        except KeyError:
            return 404, {"error": "unknown run"}
        except P.TransitionError as exc:
            return 409, {"error": str(exc)}
        return 200, {"run": run}

    @route("POST", "/claim")
    def _claim(handler, match, query, body):
        run = coordinator.claim_next(body.get("role", P.ROLE_CLIENT),
                                     body.get("worker_id", ""))
        return 200, {"run": run}

    @route("POST", r"/runs/([\w.\-]+)/result")
    def _result(handler, match, query, body):
        try:
            run = coordinator.submit_result(match.group(1), body["result"],
                                            actor=body.get("actor", ""))
        except KeyError:
            return 404, {"error": "unknown run"}
        except (P.TransitionError, ValueError) as exc:
            return 409, {"error": str(exc)}
        return 200, {"run": run}

    @route("POST", r"/runs/([\w.\-]+)/evidence")
    def _evidence(handler, match, query, body):
        try:
            entry = coordinator.attach_evidence(
                match.group(1), body.get("name", "evidence.log"),
                body.get("content", ""), kind=body.get("kind", "log"),
                actor=body.get("actor", ""))
        except KeyError:
            return 404, {"error": "unknown run"}
        return 200, {"evidence": entry}

    @route("POST", r"/runs/([\w.\-]+)/actions")
    def _request_action(handler, match, query, body):
        try:
            entry = coordinator.request_action(
                match.group(1), body["action"], body.get("params"),
                actor=body.get("actor", ""))
        except KeyError:
            return 404, {"error": "unknown run"}
        return 201, {"action": entry}

    @route("GET", r"/runs/([\w.\-]+)/actions/next")
    def _next_action(handler, match, query, body):
        try:
            entry = coordinator.next_action(match.group(1),
                                            actor=query.get("actor", [""])[0])
        except KeyError:
            return 404, {"error": "unknown run"}
        return 200, {"action": entry}

    @route("POST", r"/runs/([\w.\-]+)/actions/([\w.\-]+)/complete")
    def _complete_action(handler, match, query, body):
        try:
            entry = coordinator.complete_action(
                match.group(1), match.group(2), bool(body.get("ok")),
                detail=body.get("detail", ""))
        except KeyError:
            return 404, {"error": "unknown run or action"}
        return 200, {"action": entry}

    @route("POST", "/workers/heartbeat")
    def _heartbeat(handler, match, query, body):
        try:
            entry = coordinator.heartbeat(body["worker_id"], body["role"],
                                          body.get("info"))
        except (KeyError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 200, {"worker": entry}

    @route("GET", "/workers")
    def _workers(handler, match, query, body):
        return 200, {"workers": coordinator.list_workers()}

    @route("POST", "/messages")
    def _post_message(handler, match, query, body):
        if not body.get("text"):
            return 400, {"error": "text is required"}
        entry = coordinator.post_message(
            sender=body.get("from", "unknown"), to=body.get("to", "all"),
            text=body["text"], kind=body.get("kind", "note"))
        return 201, {"message": entry}

    @route("GET", "/messages")
    def _get_messages(handler, match, query, body):
        try:
            since = int(query.get("since", ["0"])[0])
        except ValueError:
            return 400, {"error": "since must be an integer"}
        to = query.get("to", [None])[0]
        return 200, {"messages": coordinator.list_messages(since=since, to=to)}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "carla-orchestration/1"

        def log_message(self, fmt, *args):  # keep stdout clean
            log.debug("%s - %s", self.address_string(), fmt % args)

        def _dispatch(self, method):
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query)
            body = {}
            if method == "POST":
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                if raw:
                    try:
                        body = json.loads(raw.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        return self._send(400, {"error": "body must be JSON"})
                if not isinstance(body, dict):
                    return self._send(400, {"error": "body must be a JSON object"})
            for route_method, pattern, fn in routes:
                if route_method != method:
                    continue
                match = pattern.match(parsed.path)
                if match:
                    try:
                        code, payload = fn(self, match, query, body)
                    except Exception as exc:  # never kill the coordinator
                        log.exception("handler error for %s %s", method, parsed.path)
                        code, payload = 500, {"error": f"{type(exc).__name__}: {exc}"}
                    return self._send(code, payload)
            self._send(404, {"error": f"no route for {method} {parsed.path}"})

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        def _send(self, code, payload):
            raw = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    return Handler


class CoordinatorServer:
    """Coordinator HTTP endpoint plus the stale-run sweeper."""

    def __init__(self, coordinator, bind_host: str, port: int):
        self.coordinator = coordinator
        self._httpd = ThreadingHTTPServer((bind_host, port), _make_handler(coordinator))
        self._httpd.daemon_threads = True
        self._thread = None
        self._sweeper = None
        self._stop = threading.Event()

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    def start(self):
        self._thread = threading.Thread(target=self._httpd.serve_forever,
                                        name="coordinator-http", daemon=True)
        self._thread.start()
        self._sweeper = threading.Thread(target=self._sweep_loop,
                                         name="coordinator-sweeper", daemon=True)
        self._sweeper.start()
        return self

    def _sweep_loop(self):
        while not self._stop.wait(SWEEP_INTERVAL):
            try:
                for run in self.coordinator.sweep_stale():
                    log.warning("swept stale run %s (%s)", run["run_id"], run["scenario"])
            except Exception:
                log.exception("sweeper error")

    def stop(self):
        self._stop.set()
        self._httpd.shutdown()
        self._httpd.server_close()
        if self._thread:
            self._thread.join(timeout=5)


# ── client ───────────────────────────────────────────────────────────────────

class CoordinatorClient:
    """Thin JSON-over-HTTP client. Used by both workers and the CLI."""

    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, payload: dict = None):
        url = f"{self.base_url}{path}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise CoordinatorError(f"{method} {path} -> HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise CoordinatorError(
                f"cannot reach coordinator at {self.base_url} ({exc.reason}). "
                f"Is the lab worker running, and is LAB_HOST/COORDINATOR_PORT correct?"
            ) from exc
        return json.loads(raw) if raw else {}

    # endpoints
    def health(self):
        return self._request("GET", "/health")

    def status(self):
        return self._request("GET", "/status")

    def list_runs(self, limit=50, status=None, suite_id=None):
        query = f"?limit={limit}"
        if status:
            query += f"&status={status}"
        if suite_id:
            query += f"&suite_id={suite_id}"
        return self._request("GET", f"/runs{query}")["runs"]

    def create_run(self, scenario, suite_id="", config=None, created_by="", timeout=None):
        return self._request("POST", "/runs", {
            "scenario": scenario, "suite_id": suite_id, "config": config or {},
            "created_by": created_by, "timeout": timeout})["run"]

    def get_run(self, run_id):
        return self._request("GET", f"/runs/{run_id}")["run"]

    def transition(self, run_id, to_state, actor="", detail="", expected_from=None):
        return self._request("POST", f"/runs/{run_id}/transition", {
            "to_state": to_state, "actor": actor, "detail": detail,
            "expected_from": expected_from})["run"]

    def claim(self, worker_id, role=P.ROLE_CLIENT):
        return self._request("POST", "/claim", {"worker_id": worker_id, "role": role})["run"]

    def submit_result(self, run_id, result, actor=""):
        return self._request("POST", f"/runs/{run_id}/result",
                             {"result": result, "actor": actor})["run"]

    def attach_evidence(self, run_id, name, content, kind="log", actor=""):
        return self._request("POST", f"/runs/{run_id}/evidence", {
            "name": name, "content": content, "kind": kind, "actor": actor})["evidence"]

    def request_action(self, run_id, action, params=None, actor=""):
        return self._request("POST", f"/runs/{run_id}/actions", {
            "action": action, "params": params or {}, "actor": actor})["action"]

    def next_action(self, run_id, actor=""):
        actor_q = urllib.parse.quote(str(actor), safe="")
        return self._request("GET", f"/runs/{run_id}/actions/next?actor={actor_q}")["action"]

    def complete_action(self, run_id, action_id, ok, detail=""):
        return self._request("POST", f"/runs/{run_id}/actions/{action_id}/complete",
                             {"ok": ok, "detail": detail})["action"]

    def heartbeat(self, worker_id, role, info=None):
        return self._request("POST", "/workers/heartbeat", {
            "worker_id": worker_id, "role": role, "info": info or {}})["worker"]

    def workers(self):
        return self._request("GET", "/workers")["workers"]

    def post_message(self, sender, to, text, kind="note"):
        return self._request("POST", "/messages", {
            "from": sender, "to": to, "text": text, "kind": kind})["message"]

    def messages(self, since=0, to=None):
        query = f"?since={since}" + (f"&to={to}" if to else "")
        return self._request("GET", f"/messages{query}")["messages"]
