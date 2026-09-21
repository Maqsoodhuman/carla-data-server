"""LAB worker: owns CARLA-side prerequisites, the data server process, and
drives the suite by creating one run per scenario and waiting for a client.

Loop shape:

    verify environment -> (re)start data server -> for each scenario:
        create run -> LAB_PREPARING -> LAB_READY (coordinator advertises it)
        -> service any lab actions the client requests while it runs
        -> collect server-side evidence -> record outcome -> next scenario
"""

import logging
import os
import signal
import subprocess
import sys
import threading
import time

from . import REPO_ROOT
from . import protocol as P
from . import scenarios as S
from .doctor import check_carla_map, run_doctor

log = logging.getLogger("orchestration.lab")

POLL_INTERVAL = 1.0
HEARTBEAT_INTERVAL = 5.0
SERVER_START_GRACE = 6.0


class DataServerProcess:
    """Supervises server/server.py so the lab can restart it on request."""

    def __init__(self, cfg, log_path: str):
        self.cfg = cfg
        self.log_path = log_path
        self.proc = None

    def start(self) -> bool:
        if self.proc and self.proc.poll() is None:
            return True
        cmd = [sys.executable, "-u", os.path.join(REPO_ROOT, "server", "server.py"),
               "--host", self.cfg.data_server_bind_host,
               "--port", str(self.cfg.data_server_port),
               "--carla-host", self.cfg.carla_host,
               "--carla-port", str(self.cfg.carla_port),
               "--tick-rate", str(self.cfg.tick_rate)]
        log.info("starting data server: %s", " ".join(cmd))
        self._handle = open(self.log_path, "a", encoding="utf-8")
        self._handle.write(f"\n=== start {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        self._handle.flush()
        self.proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=self._handle,
                                     stderr=subprocess.STDOUT)
        time.sleep(SERVER_START_GRACE)
        alive = self.proc.poll() is None
        if not alive:
            log.error("data server exited immediately (code %s); see %s",
                      self.proc.returncode, self.log_path)
        return alive

    def stop(self):
        if not self.proc or self.proc.poll() is not None:
            return
        self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)

    def restart(self) -> bool:
        self.stop()
        time.sleep(1.0)
        self.proc = None
        return self.start()

    def tail(self, lines: int = 200) -> str:
        try:
            with open(self.log_path, "r", encoding="utf-8", errors="replace") as f:
                return "".join(f.readlines()[-lines:])
        except OSError as exc:
            return f"<could not read {self.log_path}: {exc}>"


class LabWorker:
    def __init__(self, cfg, coordinator_client, suite=None, manage_server=True,
                 on_failure="stop", worker_id=None):
        self.cfg = cfg
        self.api = coordinator_client
        self.suite = list(suite or S.DEFAULT_SUITE)
        self.manage_server = manage_server
        self.on_failure = on_failure
        self.worker_id = worker_id or f"lab-{os.uname().nodename}-{os.getpid()}"
        self.server = DataServerProcess(
            cfg, os.path.join(cfg.state_dir, "data-server.log")) if manage_server else None
        self._stop = threading.Event()

    # ── lifecycle ────────────────────────────────────────────────────────────

    def stop(self):
        self._stop.set()

    def _heartbeat_loop(self):
        while not self._stop.wait(HEARTBEAT_INTERVAL):
            try:
                self.api.heartbeat(self.worker_id, P.ROLE_LAB,
                                   {"suite": self.suite, "manages_server": self.manage_server})
            except Exception as exc:
                log.debug("heartbeat failed: %s", exc)

    def run_suite(self) -> dict:
        suite_id = P.new_suite_id()
        log.info("suite %s: %s", suite_id, " -> ".join(self.suite))
        self.api.heartbeat(self.worker_id, P.ROLE_LAB, {"suite": self.suite})
        threading.Thread(target=self._heartbeat_loop, name="lab-heartbeat",
                         daemon=True).start()

        report = run_doctor(self.cfg, P.ROLE_LAB)
        if self.manage_server:
            # The data server check is expected to fail before we start it.
            if not self.server.start():
                log.error("could not start the data server - aborting suite")
                return {"suite_id": suite_id, "aborted": "data_server_failed",
                        "doctor": report, "runs": []}
            report = run_doctor(self.cfg, P.ROLE_LAB)
        for check in report["checks"]:
            log.info("doctor %-22s %s", check["name"], "ok" if check["ok"] else check["detail"])

        outcomes = []
        try:
            for scenario in self.suite:
                if self._stop.is_set():
                    break
                outcome = self._run_one(scenario, suite_id, report)
                outcomes.append(outcome)
                if outcome["state"] == P.FAIL and self.on_failure == "stop":
                    log.warning("scenario %s FAILED and on-failure=stop - halting suite",
                                scenario)
                    break
                if outcome["state"] == P.ERROR and self.on_failure == "stop":
                    log.warning("scenario %s ERRORED and on-failure=stop - halting suite",
                                scenario)
                    break
        finally:
            if self.manage_server and not self._keep_server:
                self.server.stop()

        summary = {
            "suite_id": suite_id,
            "policy": {"on_failure": self.on_failure},
            "scenarios": self.suite,
            "runs": outcomes,
            "passed": sum(1 for o in outcomes if o["state"] == P.PASS),
            "failed": sum(1 for o in outcomes if o["state"] == P.FAIL),
            "errored": sum(1 for o in outcomes if o["state"] == P.ERROR),
            "skipped": sum(1 for o in outcomes if o["state"] == P.SKIPPED),
        }
        log.info("suite %s finished: %s", suite_id,
                 " ".join(f"{k}={v}" for k, v in summary.items()
                          if k in ("passed", "failed", "errored", "skipped")))
        return summary

    _keep_server = False

    def service_forever(self):
        """Stay up after the suite: keep the data server running and keep
        servicing requeued runs (`rerun`) and the lab actions they request.
        Without this a rerun would be advertised to a client with no data
        server behind it, and fail for the wrong reason."""
        log.info("servicing requeued runs; Ctrl+C to stop")
        while not self._stop.is_set():
            try:
                for run in self.api.list_runs(limit=25):
                    if P.is_terminal(run["state"]):
                        continue
                    if run["state"] == P.CREATED:
                        # A rerun arrives already advertised; anything still in
                        # CREATED needs the lab to prepare it.
                        self.api.transition(run["run_id"], P.LAB_PREPARING,
                                            actor=self.worker_id, detail="servicing rerun")
                        ready, detail = self._prepare(run["scenario"])
                        self.api.transition(
                            run["run_id"], P.LAB_READY if ready else P.ERROR,
                            actor=self.worker_id, detail=detail)
                    else:
                        self._service_actions(run["run_id"])
            except Exception as exc:
                log.debug("service loop: %s", exc)
            time.sleep(POLL_INTERVAL)

    # ── one scenario ─────────────────────────────────────────────────────────

    def _run_one(self, scenario: str, suite_id: str, doctor_report: dict) -> dict:
        params = {"lab_manages_server": bool(self.manage_server)}
        run = self.api.create_run(scenario=scenario, suite_id=suite_id, config=params,
                                  created_by=self.worker_id,
                                  timeout=S.TIMEOUTS.get(scenario, 180.0))
        run_id = run["run_id"]
        log.info("[%s] %s: preparing", run_id, scenario)
        self.api.transition(run_id, P.LAB_PREPARING, actor=self.worker_id,
                            detail="verifying lab prerequisites")

        ready, detail = self._prepare(scenario)
        if not ready:
            log.warning("[%s] %s: lab not ready - %s", run_id, scenario, detail)
            self.api.submit_result(run_id, P.result(
                run_id=run_id, scenario=scenario, status=P.ERROR, worker=self.worker_id,
                duration_seconds=0.0,
                errors=[{"kind": "lab_not_ready", "message": detail}]),
                actor=self.worker_id)
            return self.api.get_run(run_id)

        self.api.transition(run_id, P.LAB_READY, actor=self.worker_id, detail=detail)
        log.info("[%s] %s: LAB_READY - waiting for a client worker", run_id, scenario)

        final = self._wait_for_completion(run_id, scenario)
        self._attach_server_evidence(run_id)
        state = final["state"]
        log.info("[%s] %s: %s", run_id, scenario, state.upper())
        result = final.get("result") or {}
        for a in result.get("assertions", []):
            if not a.get("passed"):
                log.warning("[%s]   failed assertion %s: expected %r, observed %r",
                            run_id, a["name"], a.get("expected"), a.get("observed"))
        return final

    def _prepare(self, scenario: str):
        # A wrong map fails quietly rather than loudly (spawn indexes land
        # elsewhere, mirrored traffic lights pair up against a different
        # layout), so it gates the run instead of being discovered later.
        map_check = check_carla_map(self.cfg)
        if not map_check["ok"] and not map_check["skipped"]:
            return False, f"carla_map: {map_check['detail']}"
        if self.manage_server:
            if not self.server.start():
                return False, "data server is not running and could not be started"
            return True, f"data server up; {map_check['detail']}"
        ok = run_doctor(self.cfg, P.ROLE_LAB)
        blocking = [c for c in ok["checks"]
                    if not c["ok"] and c["name"] in ("data_server", "repo_layout")]
        if blocking:
            return False, "; ".join(f"{c['name']}: {c['detail']}" for c in blocking)
        return True, "external data server verified"

    def _wait_for_completion(self, run_id: str, scenario: str) -> dict:
        """Poll until terminal, servicing lab actions the client requests."""
        while not self._stop.is_set():
            run = self.api.get_run(run_id)
            if P.is_terminal(run["state"]):
                return run
            self._service_actions(run_id)
            time.sleep(POLL_INTERVAL)
        return self.api.get_run(run_id)

    def _service_actions(self, run_id: str):
        action = self.api.next_action(run_id, actor=self.worker_id)
        if not action:
            return
        name = action["action"]
        log.info("[%s] servicing lab action: %s", run_id, name)
        if name == "restart_data_server":
            if not self.manage_server:
                self.api.complete_action(run_id, action["action_id"], False,
                                         "this lab worker does not manage the data server")
                return
            ok = self.server.restart()
            self.api.complete_action(run_id, action["action_id"], ok,
                                     "data server restarted" if ok else "restart failed")
        else:
            self.api.complete_action(run_id, action["action_id"], False,
                                     f"unknown lab action {name!r}")

    def _attach_server_evidence(self, run_id: str):
        if not self.manage_server:
            return
        try:
            self.api.attach_evidence(run_id, "data-server.log", self.server.tail(300),
                                     kind="server_log", actor=self.worker_id)
        except Exception as exc:
            log.debug("could not attach server log: %s", exc)
