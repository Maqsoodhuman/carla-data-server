"""Scenario implementations - the client-side work each run performs.

Every scenario returns real assertions with expected/observed values. A
scenario that merely started a process without checking anything is treated
as an ERROR, not a PASS (see protocol.status_from_assertions).

Reuses the repo's own client stack rather than re-implementing the protocol:
`CARLAClient` (connection, subscribe handshake, ping/RTT, reconnect) and
`wire` (message-type registry and validation).
"""

import json
import os
import socket
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

import websockets  # noqa: F401  (import checked early; used via client.py)

import wire
from client import CARLAClient

from . import protocol as P
from . import REPO_ROOT


@dataclass
class ScenarioContext:
    run: dict
    config: object
    coordinator: object
    worker_id: str
    logs: list = field(default_factory=list)

    def log(self, message: str):
        line = f"{time.strftime('%H:%M:%S')} {message}"
        self.logs.append(line)
        return line

    @property
    def params(self) -> dict:
        return self.run.get("config") or {}


@dataclass
class Outcome:
    status: str
    metrics: dict = field(default_factory=dict)
    assertions: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    artifacts: list = field(default_factory=list)


class _Collector(CARLAClient):
    """CARLAClient that records what it received so assertions can inspect it."""

    def __init__(self, url, subscriptions, role="orchestration"):
        super().__init__(url, subscriptions, role=role)
        self.lock = threading.Lock()
        self.states = []            # (monotonic_recv_time, message)
        self.connections = []       # (monotonic_time, client_id)
        self.approx_bytes = 0

    def on_connected(self, client_id):
        with self.lock:
            self.connections.append((time.monotonic(), client_id))

    def on_world_state(self, state):
        # Deliberately not calling super(): its periodic logging is noise here.
        size = len(json.dumps(state))
        with self.lock:
            self.states.append((time.monotonic(), state))
            self.approx_bytes += size

    def snapshot(self):
        with self.lock:
            return list(self.states), list(self.connections), self.approx_bytes


def _wait_for(predicate, timeout, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _tick_stats(states):
    ticks = [msg.get("tick") for _, msg in states if isinstance(msg.get("tick"), int)]
    times = [t for t, _ in states]
    intervals = [(b - a) * 1000.0 for a, b in zip(times, times[1:])]
    gaps = [b - a for a, b in zip(ticks, ticks[1:])]
    return ticks, intervals, gaps


def _monotonic_ticks(ticks) -> bool:
    return all(b > a for a, b in zip(ticks, ticks[1:]))


# ── scenarios ────────────────────────────────────────────────────────────────

def scenario_connectivity(ctx: ScenarioContext) -> Outcome:
    """Can this machine reach the lab's data server and complete a handshake?"""
    cfg = ctx.config
    timeout = float(ctx.params.get("timeout", 10.0))
    assertions, metrics = [], {}

    started = time.monotonic()
    tcp_ok, tcp_error = False, ""
    try:
        with socket.create_connection((cfg.lab_host, cfg.data_server_port), timeout=timeout):
            tcp_ok = True
    except OSError as exc:
        tcp_error = f"{type(exc).__name__}: {exc}"
    metrics["tcp_connect_ms"] = round((time.monotonic() - started) * 1000, 1)
    assertions.append(P.assertion(
        "tcp_connect", tcp_ok,
        expected=f"TCP connect to {cfg.lab_host}:{cfg.data_server_port}",
        observed="connected" if tcp_ok else tcp_error,
        detail=("" if tcp_ok else
                "No TCP route to the data server. On campus wifi, inbound TCP to the "
                "lab PC is silently dropped - use Tailscale or the same LAN."),
    ))
    if not tcp_ok:
        return Outcome(P.FAIL, metrics, assertions)

    collector = _Collector(cfg.data_server_url, ["vehicles"], role="orchestration-connectivity")
    thread = collector.run_in_thread()
    try:
        handshake_start = time.monotonic()
        got_welcome = _wait_for(lambda: bool(collector.connections), timeout)
        metrics["welcome_ms"] = round((time.monotonic() - handshake_start) * 1000, 1)
        assertions.append(P.assertion(
            "welcome_received", got_welcome,
            expected=f"welcome within {timeout}s",
            observed=f"{metrics['welcome_ms']}ms" if got_welcome else "no welcome",
        ))
        client_id = collector.connections[0][1] if collector.connections else None
        assertions.append(P.assertion(
            "welcome_has_client_id", bool(client_id),
            expected="non-empty client_id", observed=client_id))

        got_state = _wait_for(lambda: bool(collector.states), timeout)
        assertions.append(P.assertion(
            "world_state_flowing", got_state,
            expected=f"at least one world_state within {timeout}s",
            observed=f"{len(collector.states)} messages"))
        if collector.states:
            first = collector.states[0][1]
            assertions.append(P.assertion(
                "world_state_schema_valid", wire.validate_message(first),
                expected=f"required fields {wire.REQUIRED_FIELDS[wire.MSG_WORLD_STATE]}",
                observed=sorted(first.keys())))
    finally:
        collector.disconnect()
        thread.join(timeout=5)

    return Outcome(P.status_from_assertions(assertions), metrics, assertions)


def scenario_world_state(ctx: ScenarioContext) -> Outcome:
    """Does world_state carry a well-formed, correctly filtered, monotonic feed?"""
    cfg = ctx.config
    want = int(ctx.params.get("message_count", 20))
    timeout = float(ctx.params.get("timeout", 30.0))
    subscriptions = list(ctx.params.get("subscriptions", ["vehicles", "pedestrians"]))
    assertions, metrics = [], {}

    collector = _Collector(cfg.data_server_url, subscriptions, role="orchestration-world-state")
    thread = collector.run_in_thread()
    try:
        _wait_for(lambda: len(collector.states) >= want, timeout)
    finally:
        collector.disconnect()
        thread.join(timeout=5)

    states, connections, approx_bytes = collector.snapshot()
    metrics["message_count"] = len(states)
    metrics["subscriptions"] = subscriptions

    assertions.append(P.assertion(
        "received_expected_messages", len(states) >= want,
        expected=f">= {want} world_state messages in {timeout}s", observed=len(states)))
    if not states:
        return Outcome(P.FAIL, metrics, assertions,
                       errors=[{"kind": "no_data", "message": "no world_state received"}])

    messages = [msg for _, msg in states]
    invalid = [i for i, msg in enumerate(messages) if not wire.validate_message(msg)]
    assertions.append(P.assertion(
        "all_messages_valid", not invalid,
        expected="every message passes wire.validate_message",
        observed=f"{len(invalid)} invalid (indices {invalid[:5]})"))

    ticks, intervals, gaps = _tick_stats(states)
    metrics["first_tick"], metrics["last_tick"] = (ticks[0], ticks[-1]) if ticks else (None, None)
    metrics["max_tick_gap"] = max(gaps) if gaps else None
    metrics["mean_interval_ms"] = round(statistics.mean(intervals), 2) if intervals else None
    metrics["payload_bytes_mean_approx"] = round(approx_bytes / len(states), 1)

    assertions.append(P.assertion(
        "tick_strictly_increasing", _monotonic_ticks(ticks),
        expected="tick increases every message (drops allowed, reordering is not)",
        observed=f"{ticks[:5]}...{ticks[-3:]}"))

    sim_times = [msg.get("timestamp") for msg in messages]
    sim_ok = all(isinstance(t, (int, float)) for t in sim_times) and sim_times[-1] > sim_times[0]
    assertions.append(P.assertion(
        "sim_time_advances", sim_ok,
        expected="world_state.timestamp (sim seconds) increases over the run",
        observed=f"{sim_times[0]} -> {sim_times[-1]}" if sim_times else None))

    missing = sorted({t for t in subscriptions for msg in messages if t not in msg})
    assertions.append(P.assertion(
        "subscribed_topics_present", not missing,
        expected=f"every message carries {subscriptions}", observed=f"missing {missing}"))

    unsubscribed = sorted(set(wire.VALID_TOPICS) - set(subscriptions))
    leaked = sorted({t for t in unsubscribed for msg in messages if t in msg})
    assertions.append(P.assertion(
        "unsubscribed_topics_absent", not leaked,
        expected=f"no {unsubscribed} keys (server-side per-client filtering)",
        observed=f"leaked {leaked}"))

    return Outcome(P.status_from_assertions(assertions), metrics, assertions)


def scenario_sustained_stream(ctx: ScenarioContext) -> Outcome:
    """Does the feed hold a stable rate, with sane RTT, over a sustained window?"""
    cfg = ctx.config
    duration = float(ctx.params.get("duration", 20.0))
    tolerance = float(ctx.params.get("rate_tolerance", 0.30))
    max_gap = int(ctx.params.get("max_tick_gap", 10))
    max_rtt_ms = float(ctx.params.get("max_rtt_ms", 1000.0))
    assertions, metrics = [], {}

    collector = _Collector(cfg.data_server_url, ["vehicles"], role="orchestration-sustained")
    thread = collector.run_in_thread()
    try:
        if not _wait_for(lambda: bool(collector.states), 15.0):
            collector.disconnect()
            thread.join(timeout=5)
            return Outcome(P.FAIL, metrics, [P.assertion(
                "stream_started", False, expected="world_state within 15s",
                observed="nothing received")])
        window_start = time.monotonic()
        baseline = len(collector.states)
        time.sleep(duration)
        observed_window = time.monotonic() - window_start
        measured = len(collector.states) - baseline
        latency = collector.get_latency_stats()
    finally:
        collector.disconnect()
        thread.join(timeout=5)

    states, connections, approx_bytes = collector.snapshot()
    ticks, intervals, gaps = _tick_stats(states)

    hz = measured / observed_window if observed_window else 0.0
    metrics.update({
        "observed_hz": round(hz, 2),
        "expected_hz": cfg.tick_rate,
        "window_seconds": round(observed_window, 2),
        "messages_in_window": measured,
        "max_tick_gap": max(gaps) if gaps else None,
        "jitter_ms_stdev": round(statistics.stdev(intervals), 2) if len(intervals) > 1 else None,
        "rtt_avg_ms": None if latency["samples"] == 0 else round(latency["avg_rtt_ms"], 2),
        "rtt_max_ms": None if latency["samples"] == 0 else round(latency["max_rtt_ms"], 2),
        "rtt_samples": latency["samples"],
        "approx_kbytes_total": round(approx_bytes / 1024.0, 1),
        "connection_count": len(connections),
    })

    low, high = cfg.tick_rate * (1 - tolerance), cfg.tick_rate * (1 + tolerance)
    assertions.append(P.assertion(
        "rate_within_tolerance", low <= hz <= high,
        expected=f"{low:.1f}-{high:.1f} Hz (tick rate {cfg.tick_rate} +/-{tolerance:.0%})",
        observed=f"{hz:.2f} Hz"))
    assertions.append(P.assertion(
        "no_large_tick_gaps", (max(gaps) if gaps else 0) <= max_gap,
        expected=f"max tick gap <= {max_gap} (drop-oldest may drop, not reorder)",
        observed=max(gaps) if gaps else 0))
    assertions.append(P.assertion(
        "tick_strictly_increasing", _monotonic_ticks(ticks),
        expected="no reordered or repeated ticks", observed=f"{len(ticks)} ticks checked"))
    assertions.append(P.assertion(
        "single_connection", len(connections) == 1,
        expected="1 connection (no mid-run drop/reconnect)", observed=len(connections)))
    # RTT is a round trip measured against our own clock, so unlike wall_time
    # deltas it is immune to clock skew between the two machines.
    if latency["samples"]:
        assertions.append(P.assertion(
            "rtt_under_threshold", latency["max_rtt_ms"] <= max_rtt_ms,
            expected=f"max RTT <= {max_rtt_ms}ms",
            observed=f"{latency['max_rtt_ms']:.1f}ms over {latency['samples']} samples"))
    else:
        assertions.append(P.assertion(
            "rtt_samples_collected", False, expected=">=1 ping/ack RTT sample",
            observed="0 - ping acks never arrived"))

    return Outcome(P.status_from_assertions(assertions), metrics, assertions)


def scenario_reconnect(ctx: ScenarioContext) -> Outcome:
    """Does the client recover when the connection drops mid-stream?

    Preferred mode asks the LAB worker to restart the data server (a real
    outage). If the lab does not manage the server process, falls back to a
    client-initiated drop and records which mode actually ran - the result
    never claims a server restart that did not happen.
    """
    cfg = ctx.config
    settle = float(ctx.params.get("settle_seconds", 3.0))
    recover_timeout = float(ctx.params.get("recover_timeout", 45.0))
    assertions, metrics, errors = [], {}, []

    collector = _Collector(cfg.data_server_url, ["vehicles"], role="orchestration-reconnect")
    thread = collector.run_in_thread()
    try:
        baseline_ok = _wait_for(lambda: len(collector.states) >= 5, 20.0)
        assertions.append(P.assertion(
            "baseline_stream_ok", baseline_ok,
            expected=">=5 world_state before the outage", observed=len(collector.states)))
        if not baseline_ok:
            return Outcome(P.FAIL, metrics, assertions)

        pre_count = len(collector.states)
        pre_client_id = collector.connections[-1][1]

        # Clock starts when the outage is *initiated*: the lab's restart takes
        # several seconds, during which the client may already have recovered,
        # so timing from "lab reported done" would report a fake ~0s.
        outage_at = time.monotonic()
        mode = "client_initiated"
        action = None
        if ctx.params.get("lab_manages_server"):
            action = ctx.coordinator.request_action(
                ctx.run["run_id"], "restart_data_server",
                {"reason": "reconnect scenario"}, actor=ctx.worker_id)
            ctx.log(f"requested lab action {action['action_id']}: restart_data_server")
            done = _wait_for(
                lambda: (ctx.coordinator.get_run(ctx.run["run_id"])["actions"][-1]["state"]
                         in ("done", "failed")), 60.0, interval=0.5)
            final = ctx.coordinator.get_run(ctx.run["run_id"])["actions"][-1]
            if done and final.get("ok"):
                mode = "server_restart"
            else:
                errors.append({"kind": "lab_action_failed",
                               "message": f"restart_data_server: {final.get('detail')}"})
                ctx.log("lab restart failed; falling back to a client-initiated drop")
        if mode == "client_initiated":
            # Force the socket shut from this side and let CARLAClient's
            # reconnect/backoff loop recover on its own.
            ws, loop = collector._ws, collector._loop
            if ws is None or loop is None:
                return Outcome(P.ERROR, metrics, assertions, errors=[
                    {"kind": "no_socket", "message": "client had no live socket to drop"}])
            import asyncio
            asyncio.run_coroutine_threadsafe(ws.close(code=4000, reason="reconnect test"), loop)

        metrics["mode"] = mode
        reconnected = _wait_for(lambda: len(collector.connections) >= 2, recover_timeout, 0.2)
        # Upper bound: includes the lab's own restart time, not just client backoff.
        metrics["recover_seconds_since_outage_requested"] = round(
            time.monotonic() - outage_at, 2)
        assertions.append(P.assertion(
            "reconnected", reconnected,
            expected=f"a second connection within {recover_timeout}s",
            observed=f"{len(collector.connections)} connections, "
                     f"{metrics['recover_seconds_since_outage_requested']}s"))
        if not reconnected:
            return Outcome(P.FAIL, metrics, assertions, errors=errors)

        post_client_id = collector.connections[-1][1]
        assertions.append(P.assertion(
            "new_session_issued", post_client_id != pre_client_id,
            expected="server issues a different client_id after reconnect",
            observed=f"{pre_client_id} -> {post_client_id}"))

        resumed_at = len(collector.states)
        resumed = _wait_for(lambda: len(collector.states) >= resumed_at + 5, 20.0)
        assertions.append(P.assertion(
            "stream_resumed", resumed,
            expected=">=5 world_state after reconnect",
            observed=len(collector.states) - resumed_at))
        time.sleep(settle)

        # Ticks restart from 1 after a real server restart, so only check
        # ordering *within* the post-reconnect segment.
        post_states = collector.states[resumed_at:]
        post_ticks = [m.get("tick") for _, m in post_states if isinstance(m.get("tick"), int)]
        assertions.append(P.assertion(
            "post_reconnect_ticks_ordered", _monotonic_ticks(post_ticks),
            expected="ticks increase within the post-reconnect segment "
                     "(a server restart legitimately resets tick to 1)",
            observed=f"{post_ticks[:5]}..."))
        metrics.update({"pre_messages": pre_count, "post_messages": len(post_states),
                        "pre_client_id": pre_client_id, "post_client_id": post_client_id})
    finally:
        collector.disconnect()
        thread.join(timeout=5)

    return Outcome(P.status_from_assertions(assertions), metrics, assertions, errors)


def scenario_mirror(ctx: ScenarioContext) -> Outcome:
    """Does carla_mirror_client replicate the lab's world into a local CARLA?

    Requires the CARLA PythonAPI and a reachable shadow simulator on THIS
    machine; when either is missing the run is SKIPPED, never passed.
    """
    cfg = ctx.config
    duration = float(ctx.params.get("duration", 20.0))
    assertions, metrics = [], {}

    try:
        import carla  # noqa: F401
    except ImportError:
        return Outcome(P.SKIPPED, {"reason": "carla PythonAPI not importable"}, [],
                       errors=[{"kind": "precondition",
                                "message": "mirror needs the CARLA PythonAPI on the client "
                                           "machine (use venv/, not venv-stub/)"}])
    try:
        with socket.create_connection((cfg.shadow_carla_host, cfg.shadow_carla_port), timeout=5):
            pass
    except OSError as exc:
        return Outcome(P.SKIPPED,
                       {"reason": f"shadow CARLA unreachable at "
                                  f"{cfg.shadow_carla_host}:{cfg.shadow_carla_port}"}, [],
                       errors=[{"kind": "precondition", "message": str(exc)}])

    import carla
    shadow = carla.Client(cfg.shadow_carla_host, cfg.shadow_carla_port)
    shadow.set_timeout(10.0)
    world = shadow.get_world()
    before = len(world.get_actors().filter("vehicle.*"))

    bridge = os.path.join(REPO_ROOT, "bridges", "carla_mirror_client.py")
    cmd = [sys.executable, bridge, "--server", cfg.data_server_url,
           "--shadow-host", cfg.shadow_carla_host,
           "--shadow-port", str(cfg.shadow_carla_port)]
    ctx.log("running: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    try:
        time.sleep(duration)
        still_running = proc.poll() is None
        after = len(world.get_actors().filter("vehicle.*"))
    finally:
        proc.terminate()
        try:
            output = proc.communicate(timeout=10)[0] or ""
        except subprocess.TimeoutExpired:
            proc.kill()
            output = proc.communicate()[0] or ""
    ctx.logs.extend(output.splitlines()[-100:])

    metrics.update({"shadow_vehicles_before": before, "shadow_vehicles_after": after,
                    "bridge_exit_code": proc.returncode, "duration_seconds": duration})
    assertions.append(P.assertion(
        "bridge_stayed_up", still_running,
        expected=f"bridge alive for {duration}s", observed=f"exit={proc.returncode}"))
    assertions.append(P.assertion(
        "shadow_actors_appeared", after > before,
        expected="mirrored vehicles spawned in the shadow CARLA",
        observed=f"{before} -> {after}"))
    return Outcome(P.status_from_assertions(assertions), metrics, assertions)


REGISTRY = {
    "connectivity": scenario_connectivity,
    "world_state": scenario_world_state,
    "sustained_stream": scenario_sustained_stream,
    "reconnect": scenario_reconnect,
    "mirror": scenario_mirror,
}

DEFAULT_SUITE = ["connectivity", "world_state", "sustained_stream", "reconnect", "mirror"]

# Generous per-scenario ceilings; the coordinator sweeps anything that exceeds them.
TIMEOUTS = {
    "connectivity": 90.0,
    "world_state": 120.0,
    "sustained_stream": 180.0,
    "reconnect": 240.0,
    "mirror": 180.0,
}


def run_scenario(name: str, ctx: ScenarioContext) -> Outcome:
    fn = REGISTRY.get(name)
    if fn is None:
        return Outcome(P.ERROR, errors=[{"kind": "unknown_scenario",
                                         "message": f"no scenario named {name!r}",
                                         "available": sorted(REGISTRY)}])
    started = time.monotonic()
    try:
        return fn(ctx)
    except Exception as exc:
        import traceback
        return Outcome(P.ERROR, {"elapsed_seconds": round(time.monotonic() - started, 2)}, [],
                       errors=[{"kind": type(exc).__name__, "message": str(exc),
                                "traceback": traceback.format_exc()}])
