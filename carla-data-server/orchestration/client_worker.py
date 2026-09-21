"""CLIENT worker: asks the coordinator for work, executes the scenario
against the lab's data server, and submits a structured result.

Loop shape:

    heartbeat -> claim a waiting run -> RUNNING -> execute scenario
    -> attach client-side logs -> submit result -> wait for the next one
"""

import logging
import os
import threading
import time

from . import protocol as P
from . import scenarios as S
from .http_api import CoordinatorError

log = logging.getLogger("orchestration.client")

POLL_INTERVAL = 1.0
HEARTBEAT_INTERVAL = 5.0


class ClientWorker:
    def __init__(self, cfg, coordinator_client, worker_id=None, idle_timeout=None):
        self.cfg = cfg
        self.api = coordinator_client
        self.worker_id = worker_id or f"client-{os.uname().nodename}-{os.getpid()}"
        self.idle_timeout = idle_timeout  # None = run until stopped
        self._stop = threading.Event()
        self.completed = []

    def stop(self):
        self._stop.set()

    def _heartbeat_loop(self):
        while not self._stop.wait(HEARTBEAT_INTERVAL):
            try:
                self.api.heartbeat(self.worker_id, P.ROLE_CLIENT,
                                   {"scenarios": sorted(S.REGISTRY)})
            except Exception as exc:
                log.debug("heartbeat failed: %s", exc)

    def serve_forever(self) -> list:
        self.api.heartbeat(self.worker_id, P.ROLE_CLIENT, {"scenarios": sorted(S.REGISTRY)})
        threading.Thread(target=self._heartbeat_loop, name="client-heartbeat",
                         daemon=True).start()
        log.info("client worker %s polling %s", self.worker_id, self.api.base_url)

        idle_since = time.monotonic()
        while not self._stop.is_set():
            try:
                run = self.api.claim(self.worker_id)
            except CoordinatorError as exc:
                log.warning("%s", exc)
                time.sleep(POLL_INTERVAL * 3)
                continue
            if run is None:
                if self.idle_timeout and time.monotonic() - idle_since > self.idle_timeout:
                    log.info("no work for %.0fs - exiting", self.idle_timeout)
                    break
                time.sleep(POLL_INTERVAL)
                continue
            self.execute(run)
            # Reset *after* the scenario: the timer measures time spent idle,
            # not time since the last claim, so a long scenario must not make
            # the worker quit on its very next empty poll.
            idle_since = time.monotonic()
        return self.completed

    def execute(self, run: dict) -> dict:
        run_id, scenario = run["run_id"], run["scenario"]
        log.info("[%s] claimed %s", run_id, scenario)
        ctx = S.ScenarioContext(run=run, config=self.cfg, coordinator=self.api,
                                worker_id=self.worker_id)
        started = time.monotonic()
        try:
            self.api.transition(run_id, P.RUNNING, actor=self.worker_id,
                                detail=f"executing {scenario}")
            outcome = S.run_scenario(scenario, ctx)
        except Exception as exc:
            import traceback
            outcome = S.Outcome(P.ERROR, errors=[{
                "kind": type(exc).__name__, "message": str(exc),
                "traceback": traceback.format_exc()}])
        duration = time.monotonic() - started

        artifacts = list(outcome.artifacts)
        if ctx.logs:
            try:
                entry = self.api.attach_evidence(
                    run_id, "client.log", "\n".join(ctx.logs), kind="client_log",
                    actor=self.worker_id)
                artifacts.append(entry["name"])
            except Exception as exc:
                log.debug("could not attach client log: %s", exc)

        result = P.result(run_id=run_id, scenario=scenario, status=outcome.status,
                          worker=self.worker_id, duration_seconds=duration,
                          metrics=outcome.metrics, assertions=outcome.assertions,
                          errors=outcome.errors, artifacts=artifacts)
        try:
            self.api.submit_result(run_id, result, actor=self.worker_id)
        except CoordinatorError as exc:
            log.error("[%s] could not submit result: %s", run_id, exc)

        failed = [a["name"] for a in result["assertions"] if not a["passed"]]
        log.info("[%s] %s -> %s (%.1fs)%s", run_id, scenario, result["status"].upper(),
                 duration, f" failed: {failed}" if failed else "")
        for a in result["assertions"]:
            if not a["passed"]:
                log.warning("[%s]   %s: expected %r, observed %r", run_id, a["name"],
                            a.get("expected"), a.get("observed"))
        self.completed.append(result)
        return result
