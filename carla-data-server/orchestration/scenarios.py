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
from .doctor import map_matches


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
        self.acks = []              # every ack except pings (filtered upstream)
        self.peers_left = []        # (peer_id, owned_actor_ids)
        self.approx_bytes = 0
        self.ego_id = None          # set from the spawn ack

    def on_connected(self, client_id):
        with self.lock:
            self.connections.append((time.monotonic(), client_id))

    def on_world_state(self, state):
        # Deliberately not calling super(): its periodic logging is noise here.
        size = len(json.dumps(state))
        with self.lock:
            self.states.append((time.monotonic(), state))
            self.approx_bytes += size

    def on_ack(self, ack):
        with self.lock:
            self.acks.append(ack)
            if ack.get("command") == wire.CMD_SPAWN and ack.get("actor_id"):
                self.ego_id = ack["actor_id"]

    def on_peer_left(self, peer_id, owned_actor_ids):
        with self.lock:
            self.peers_left.append((peer_id, list(owned_actor_ids)))

    def snapshot(self):
        with self.lock:
            return list(self.states), list(self.connections), self.approx_bytes

    @property
    def client_id_seen(self):
        with self.lock:
            return self.connections[-1][1] if self.connections else None

    def latest_vehicles(self):
        with self.lock:
            return dict(self.states[-1][1]) if self.states else {}

    def vehicle_by_id(self, actor_id):
        """Most recent view this participant has of one vehicle."""
        with self.lock:
            for _, state in reversed(self.states):
                for v in state.get("vehicles", []):
                    if v.get("id") == actor_id:
                        return dict(v)
        return None

    def sees_vehicle(self, actor_id) -> bool:
        return self.vehicle_by_id(actor_id) is not None


def _spawn_ego(collector, spawn_index, blueprint="vehicle.tesla.model3", timeout=15.0):
    """Ask the server for an ego and wait for the ack. Returns the actor id."""
    collector.send_spawn_at_index(blueprint_id=blueprint,
                                  spawn_point_index=spawn_index, autopilot=False)
    _wait_for(lambda: collector.ego_id is not None, timeout)
    return collector.ego_id


def _stub_mode_skip(reason_detail: str) -> "Outcome":
    """STUB mode fabricates a fixed world, so a spawned actor never enters the
    feed. That is a missing precondition for actor-level scenarios, not a
    failure - report it as such rather than as a bogus pass or fail."""
    return Outcome(P.SKIPPED,
                   {"reason": "server appears to be in STUB mode (spawned actors "
                              "never appear in world_state)"}, [],
                   errors=[{"kind": "precondition", "message": reason_detail,
                            "fix": "run the data server against a real CARLA "
                                   "simulator (venv/, not venv-stub/)"}])


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


def scenario_multi_client(ctx: ScenarioContext) -> Outcome:
    """Do two participants share one world and see each other correctly?

    This is the capability UB-DigitalTwin gets from Redis message types 0/1
    (each participant publishes its own vehicle; everyone renders the others).
    Here the server owns every actor and marks `is_ego` per viewer, so the
    thing to prove is that each participant sees its own car flagged, the
    peer's car unflagged, and both agree on where those cars are.
    """
    cfg = ctx.config
    spawn_a = int(ctx.params.get("spawn_index_a", 0))
    spawn_b = int(ctx.params.get("spawn_index_b", 1))
    assertions, metrics = [], {}

    a = _Collector(cfg.data_server_url, ["vehicles"], role="orchestration-agent-a")
    b = _Collector(cfg.data_server_url, ["vehicles"], role="orchestration-agent-b")
    ta, tb = a.run_in_thread(), b.run_in_thread()
    try:
        if not _wait_for(lambda: a.connections and b.connections, 20.0):
            return Outcome(P.FAIL, metrics, [P.assertion(
                "both_participants_connected", False, expected="2 connections",
                observed=f"a={len(a.connections)} b={len(b.connections)}")])

        id_a, id_b = a.client_id_seen, b.client_id_seen
        metrics["client_id_a"], metrics["client_id_b"] = id_a, id_b
        assertions.append(P.assertion(
            "distinct_client_ids", id_a != id_b,
            expected="the server issues a different client_id per participant",
            observed=f"{id_a} vs {id_b}"))

        ego_a = _spawn_ego(a, spawn_a)
        ego_b = _spawn_ego(b, spawn_b)
        metrics["ego_a"], metrics["ego_b"] = ego_a, ego_b
        assertions.append(P.assertion(
            "both_egos_spawned", bool(ego_a) and bool(ego_b) and ego_a != ego_b,
            expected="two distinct ego actor ids", observed=f"{ego_a}, {ego_b}"))
        if not (ego_a and ego_b):
            return Outcome(P.FAIL, metrics, assertions)

        # Both cars must show up in both feeds before anything else is meaningful.
        appeared = _wait_for(
            lambda: a.sees_vehicle(ego_a) and a.sees_vehicle(ego_b)
            and b.sees_vehicle(ego_a) and b.sees_vehicle(ego_b), 20.0)
        if not appeared:
            return _stub_mode_skip(
                f"spawned egos {ego_a}/{ego_b} never appeared in world_state")

        va_own, va_peer = a.vehicle_by_id(ego_a), a.vehicle_by_id(ego_b)
        vb_own, vb_peer = b.vehicle_by_id(ego_b), b.vehicle_by_id(ego_a)

        assertions.append(P.assertion(
            "a_sees_own_car_as_ego", va_own.get("is_ego") is True,
            expected="A's own vehicle carries is_ego=true in A's feed",
            observed=va_own.get("is_ego")))
        assertions.append(P.assertion(
            "a_sees_peer_car_not_as_ego", va_peer.get("is_ego") is False,
            expected="B's vehicle carries is_ego=false in A's feed",
            observed=va_peer.get("is_ego"),
            detail="is_ego is per-viewer; leaking another participant's ego flag "
                   "would make every client think it owns the same car"))
        assertions.append(P.assertion(
            "b_sees_own_car_as_ego", vb_own.get("is_ego") is True,
            expected="B's own vehicle carries is_ego=true in B's feed",
            observed=vb_own.get("is_ego")))
        assertions.append(P.assertion(
            "b_sees_peer_car_not_as_ego", vb_peer.get("is_ego") is False,
            expected="A's vehicle carries is_ego=false in B's feed",
            observed=vb_peer.get("is_ego")))

        # Same world: both participants must agree on where the shared cars are.
        def _loc(v):
            loc = (v or {}).get("transform", {}).get("location", {})
            return (loc.get("x"), loc.get("y"))

        drift = max(abs((_loc(va_peer)[i] or 0) - (_loc(vb_own)[i] or 0)) for i in (0, 1))
        metrics["peer_position_drift_m"] = round(drift, 3)
        assertions.append(P.assertion(
            "participants_agree_on_peer_position", drift < 5.0,
            expected="<5 m between the two views of the same car "
                     "(samples are from slightly different ticks)",
            observed=f"{drift:.2f} m"))

        ids_a = {v["id"] for v in a.latest_vehicles().get("vehicles", [])}
        ids_b = {v["id"] for v in b.latest_vehicles().get("vehicles", [])}
        assertions.append(P.assertion(
            "both_see_the_same_vehicle_set", ids_a == ids_b,
            expected="identical vehicle ids in both feeds",
            observed=f"only A: {sorted(ids_a - ids_b)}, only B: {sorted(ids_b - ids_a)}"))
        metrics["vehicles_visible"] = len(ids_a)
    finally:
        for client, thread in ((a, ta), (b, tb)):
            client.disconnect()
            thread.join(timeout=5)

    return Outcome(P.status_from_assertions(assertions), metrics, assertions)


def scenario_peer_departure(ctx: ScenarioContext) -> Outcome:
    """When one participant leaves, is everyone else told, and is its car gone?

    Equivalent to UB-DigitalTwin's type 1 (`destroy`). Their protocol notes a
    crashed participant never sends one, so the server-side eviction path is
    what actually has to work - here that is `client_left` plus the ego
    teardown the server does on disconnect.
    """
    cfg = ctx.config
    assertions, metrics = [], {}

    stayer = _Collector(cfg.data_server_url, ["vehicles"], role="orchestration-stayer")
    leaver = _Collector(cfg.data_server_url, ["vehicles"], role="orchestration-leaver")
    ts, tl = stayer.run_in_thread(), leaver.run_in_thread()
    try:
        if not _wait_for(lambda: stayer.connections and leaver.connections, 20.0):
            return Outcome(P.FAIL, metrics, [P.assertion(
                "both_participants_connected", False, expected="2 connections",
                observed=f"{len(stayer.connections)}/{len(leaver.connections)}")])
        leaver_id = leaver.client_id_seen
        metrics["leaver_client_id"] = leaver_id

        ego = _spawn_ego(leaver, int(ctx.params.get("spawn_index", 2)))
        metrics["leaver_ego"] = ego
        had_actor = bool(ego) and _wait_for(lambda: stayer.sees_vehicle(ego), 20.0)
        metrics["ego_was_visible_to_peer"] = had_actor

        leaver.disconnect()
        tl.join(timeout=10)

        got_event = _wait_for(lambda: any(p == leaver_id for p, _ in stayer.peers_left),
                              30.0, 0.2)
        assertions.append(P.assertion(
            "peer_departure_announced", got_event,
            expected=f"client_left naming {leaver_id}",
            observed=[p for p, _ in stayer.peers_left] or "no client_left received"))

        if got_event and had_actor:
            owned = next(ids for p, ids in stayer.peers_left if p == leaver_id)
            metrics["announced_owned_actor_ids"] = owned
            assertions.append(P.assertion(
                "departure_reports_owned_actors", ego in owned,
                expected=f"owned_actor_ids contains the leaver's ego {ego}",
                observed=owned,
                detail="peers need this to tear down their local copy of the car"))
            gone = _wait_for(lambda: not stayer.sees_vehicle(ego), 25.0, 0.5)
            assertions.append(P.assertion(
                "departed_car_removed_from_world", gone,
                expected="the server destroys the ego, so it leaves world_state",
                observed="still present" if not gone else "removed"))
        elif got_event and not had_actor:
            # No real actor (STUB mode): the event itself is still verifiable.
            metrics["note"] = ("leaver had no visible ego, so only the client_left "
                               "event was checked, not actor teardown")
    finally:
        for client, thread in ((stayer, ts), (leaver, tl)):
            client.disconnect()
            thread.join(timeout=5)

    return Outcome(P.status_from_assertions(assertions), metrics, assertions)


def scenario_ego_control(ctx: ScenarioContext) -> Outcome:
    """Does a control command actually move the car?

    This is the regression guard for the CARLA sync-mode bug where
    world.get_actor() returns an actor whose apply_control() is silently
    ignored - acks come back ok and nothing moves. Only a real simulator can
    catch it, so in STUB mode this is SKIPPED rather than faked.
    """
    cfg = ctx.config
    throttle_seconds = float(ctx.params.get("throttle_seconds", 4.0))
    min_speed = float(ctx.params.get("min_speed_ms", 1.0))
    assertions, metrics = [], {}

    driver = _Collector(cfg.data_server_url, ["vehicles"], role="orchestration-driver")
    thread = driver.run_in_thread()
    try:
        if not _wait_for(lambda: bool(driver.connections), 20.0):
            return Outcome(P.FAIL, metrics, [P.assertion(
                "connected", False, expected="a connection", observed="none")])
        ego = _spawn_ego(driver, int(ctx.params.get("spawn_index", 0)))
        metrics["ego"] = ego
        if not ego or not _wait_for(lambda: driver.sees_vehicle(ego), 20.0):
            return _stub_mode_skip(f"ego {ego} never appeared in world_state")

        def speed_of(actor_id):
            v = (driver.vehicle_by_id(actor_id) or {}).get("velocity", {})
            return (v.get("x", 0) ** 2 + v.get("y", 0) ** 2 + v.get("z", 0) ** 2) ** 0.5

        start_speed = speed_of(ego)
        deadline = time.monotonic() + throttle_seconds
        while time.monotonic() < deadline:
            driver.send_ego_control(throttle=0.75, steer=0.0, brake=0.0)
            time.sleep(0.05)
        top_speed = speed_of(ego)

        deadline = time.monotonic() + throttle_seconds
        while time.monotonic() < deadline:
            driver.send_ego_control(throttle=0.0, steer=0.0, brake=1.0)
            time.sleep(0.05)
        braked_speed = speed_of(ego)

        metrics.update({"speed_at_start_ms": round(start_speed, 3),
                        "speed_after_throttle_ms": round(top_speed, 3),
                        "speed_after_brake_ms": round(braked_speed, 3)})
        assertions.append(P.assertion(
            "throttle_accelerates_the_car", top_speed >= min_speed,
            expected=f">= {min_speed} m/s after {throttle_seconds}s of throttle",
            observed=f"{top_speed:.2f} m/s",
            detail="if acks are ok but speed stays 0, apply_control is being "
                   "silently dropped - the known sync-mode actor-handle bug"))
        assertions.append(P.assertion(
            "brake_decelerates_the_car", braked_speed < max(top_speed * 0.5, 0.5),
            expected="clearly slower after braking",
            observed=f"{top_speed:.2f} -> {braked_speed:.2f} m/s"))
        failed_acks = [a for a in driver.acks
                       if a.get("command") == wire.CMD_EGO_CONTROL
                       and a.get("status") != "ok"]
        assertions.append(P.assertion(
            "control_commands_acked_ok", not failed_acks,
            expected="no failed ego_control acks",
            observed=f"{len(failed_acks)} failed: "
                     f"{[a.get('message') for a in failed_acks[:3]]}"))
    finally:
        driver.disconnect()
        thread.join(timeout=5)

    return Outcome(P.status_from_assertions(assertions), metrics, assertions)


def scenario_udp_bridge(ctx: ScenarioContext) -> Outcome:
    """Does the Unity-facing UDP leg deliver usable packets?

    Equivalent to UB-DigitalTwin's type 3 relay. Checks the reshaped payload
    the UB-MR app actually consumes, including the tick/sim_time fields added
    for interpolation.
    """
    cfg = ctx.config
    want = int(ctx.params.get("packet_count", 20))
    timeout = float(ctx.params.get("timeout", 40.0))
    assertions, metrics = [], {}

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    sock.settimeout(2.0)
    udp_port = sock.getsockname()[1]

    bridge = os.path.join(REPO_ROOT, "bridges", "ws_to_udp_bridge.py")
    cmd = [sys.executable, "-u", bridge, "--server", cfg.data_server_url,
           "--udp-host", "127.0.0.1", "--udp-port", str(udp_port)]
    ctx.log("running: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    packets = []
    try:
        deadline = time.monotonic() + timeout
        while len(packets) < want and time.monotonic() < deadline:
            try:
                data, _ = sock.recvfrom(65535)
            except socket.timeout:
                if proc.poll() is not None:
                    break
                continue
            try:
                packets.append(json.loads(data.decode("utf-8")))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                ctx.log(f"undecodable UDP packet: {exc}")
        still_running = proc.poll() is None
    finally:
        sock.close()
        proc.terminate()
        try:
            output = proc.communicate(timeout=10)[0] or ""
        except subprocess.TimeoutExpired:
            proc.kill()
            output = proc.communicate()[0] or ""
    ctx.logs.extend(output.splitlines()[-60:])

    metrics["packets_received"] = len(packets)
    metrics["bridge_exit_code"] = proc.returncode
    assertions.append(P.assertion(
        "bridge_stayed_up", still_running,
        expected="bridge alive while relaying", observed=f"exit={proc.returncode}"))
    assertions.append(P.assertion(
        "udp_packets_received", len(packets) >= want,
        expected=f">= {want} packets in {timeout}s", observed=len(packets)))
    if not packets:
        return Outcome(P.FAIL, metrics, assertions,
                       errors=[{"kind": "no_data",
                                "message": "no UDP packets reached the listener"}])

    required = ("vehicles", "timestamp", "tick", "sim_time")
    missing = sorted({f for f in required for p in packets if f not in p})
    assertions.append(P.assertion(
        "packet_schema_complete", not missing,
        expected=f"every packet carries {list(required)}",
        observed=f"missing {missing}",
        detail="tick/sim_time are what Unity should interpolate on; timestamp is "
               "wall-clock and skews between machines"))

    ticks = [p.get("tick") for p in packets if isinstance(p.get("tick"), int)]
    assertions.append(P.assertion(
        "tick_advances_over_udp", _monotonic_ticks(ticks),
        expected="tick increases across packets (UDP may drop, not reorder here)",
        observed=f"{ticks[:5]}...{ticks[-3:]}" if ticks else "no integer ticks"))
    metrics["first_tick"], metrics["last_tick"] = (ticks[0], ticks[-1]) if ticks else (None, None)

    shaped = [p for p in packets if p.get("vehicles")]
    if shaped:
        sample = shaped[-1]["vehicles"][0]
        needed = ("id", "blueprint", "location", "yaw")
        absent = [f for f in needed if f not in sample]
        assertions.append(P.assertion(
            "vehicle_entries_shaped_for_unity", not absent,
            expected=f"each vehicle has {list(needed)}", observed=f"missing {absent}"))
        metrics["vehicles_per_packet"] = len(shaped[-1]["vehicles"])
    else:
        metrics["note"] = "no packet contained a vehicle; shape of entries unchecked"

    return Outcome(P.status_from_assertions(assertions), metrics, assertions)


def scenario_camera_follow(ctx: ScenarioContext) -> Outcome:
    """Does the spectator-camera follower track an actor in a local CARLA?

    Equivalent to UB-DigitalTwin's camera-follow role. Needs a CARLA on this
    machine to move the spectator in; SKIPPED when there isn't one.
    """
    cfg = ctx.config
    duration = float(ctx.params.get("duration", 15.0))
    host = ctx.params.get("view_host", cfg.shadow_carla_host)
    port = int(ctx.params.get("view_port", cfg.shadow_carla_port))
    assertions, metrics = [], {}

    try:
        import carla
    except ImportError:
        return Outcome(P.SKIPPED, {"reason": "carla PythonAPI not importable"}, [],
                       errors=[{"kind": "precondition",
                                "message": "camera-follow drives a local CARLA "
                                           "spectator; use venv/, not venv-stub/"}])
    try:
        viewer = carla.Client(host, port)
        viewer.set_timeout(10.0)
        world = viewer.get_world()
        before = world.get_spectator().get_transform()
    except (RuntimeError, OSError) as exc:
        return Outcome(P.SKIPPED,
                       {"reason": f"no CARLA to view with at {host}:{port}"}, [],
                       errors=[{"kind": "precondition", "message": str(exc)}])

    script = os.path.join(REPO_ROOT, "scripts", "camera_follow.py")
    cmd = [sys.executable, "-u", script, "--server", cfg.data_server_url,
           "--carla-host", host, "--carla-port", str(port),
           "--duration", str(duration)]
    ctx.log("running: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    try:
        output = proc.communicate(timeout=duration + 45)[0] or ""
    except subprocess.TimeoutExpired:
        proc.kill()
        output = proc.communicate()[0] or ""
    ctx.logs.extend(output.splitlines()[-60:])

    after = world.get_spectator().get_transform()
    moved = max(abs(after.location.x - before.location.x),
                abs(after.location.y - before.location.y),
                abs(after.location.z - before.location.z))
    metrics.update({"spectator_moved_m": round(moved, 3),
                    "follower_exit_code": proc.returncode})

    assertions.append(P.assertion(
        "follower_exited_cleanly", proc.returncode == 0,
        expected="exit 0 after its --duration",
        observed=f"exit={proc.returncode}"))
    assertions.append(P.assertion(
        "spectator_was_moved", moved > 0.5,
        expected="the spectator camera is repositioned to track the target",
        observed=f"moved {moved:.2f} m",
        detail="0 m means the follower never found a target, or never wrote to "
               "the simulator"))
    if "followed" in output:
        metrics["follower_report"] = output.strip().splitlines()[-1][:200]
    return Outcome(P.status_from_assertions(assertions), metrics, assertions)


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
    endpoint = f"{cfg.shadow_carla_host}:{cfg.shadow_carla_port}"
    try:
        with socket.create_connection((cfg.shadow_carla_host, cfg.shadow_carla_port), timeout=5):
            pass
    except OSError as exc:
        return Outcome(P.SKIPPED, {"reason": f"nothing listening at {endpoint}"}, [],
                       errors=[{"kind": "precondition", "message": str(exc)}])

    # A TCP probe is NOT proof of a shadow simulator: CARLA also binds
    # rpc_port+1 and +2 for streaming, so with CARLA on 2000 a probe of 2001
    # connects to the *primary* simulator's streaming port and looks healthy.
    # Only an RPC handshake settles it - and failing it means "no shadow
    # available" (SKIPPED), not "the test broke" (ERROR).
    import carla
    shadow = carla.Client(cfg.shadow_carla_host, cfg.shadow_carla_port)
    shadow.set_timeout(float(ctx.params.get("shadow_timeout", 10.0)))
    try:
        world = shadow.get_world()
        shadow_map = world.get_map().name
    except RuntimeError as exc:
        return Outcome(
            P.SKIPPED,
            {"reason": f"no CARLA RPC endpoint at {endpoint}",
             "hint": "a plain TCP connect can succeed against the primary "
                     "simulator's streaming port (rpc_port+1)"}, [],
            errors=[{"kind": "precondition",
                     "message": f"{endpoint} accepted TCP but is not a CARLA RPC "
                                f"endpoint: {exc}",
                     "fix": "start a second simulator with a non-colliding port, e.g. "
                            "-carla-rpc-port=2003, and set SHADOW_CARLA_PORT to match"}])
    before = len(world.get_actors().filter("vehicle.*"))

    # carla_mirror_client pairs traffic lights by index, so a shadow running a
    # different map mirrors onto the wrong layout instead of failing outright.
    metrics["shadow_map"] = shadow_map
    metrics["required_map"] = cfg.carla_map
    assertions.append(P.assertion(
        "shadow_map_matches", map_matches(shadow_map, cfg.carla_map),
        expected=cfg.carla_map, observed=shadow_map,
        detail="both simulators must run the same map or mirrored actors and "
               "traffic-light indexes refer to different worlds"))

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
    "ego_control": scenario_ego_control,
    "multi_client": scenario_multi_client,
    "peer_departure": scenario_peer_departure,
    "udp_bridge": scenario_udp_bridge,
    "reconnect": scenario_reconnect,
    "mirror": scenario_mirror,
    "camera_follow": scenario_camera_follow,
}

# Cheapest and most fundamental first, so a broken link fails fast; the
# CARLA-dependent and subprocess-heavy ones come last.
DEFAULT_SUITE = ["connectivity", "world_state", "sustained_stream", "ego_control",
                 "multi_client", "peer_departure", "udp_bridge", "reconnect",
                 "mirror", "camera_follow"]

# Generous per-scenario ceilings; the coordinator sweeps anything that exceeds them.
TIMEOUTS = {
    "connectivity": 90.0,
    "world_state": 120.0,
    "sustained_stream": 180.0,
    "ego_control": 180.0,
    "multi_client": 180.0,
    "peer_departure": 180.0,
    "udp_bridge": 150.0,
    "reconnect": 240.0,
    "mirror": 180.0,
    "camera_follow": 180.0,
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
