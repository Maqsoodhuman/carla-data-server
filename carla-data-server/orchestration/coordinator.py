"""The coordinator: shared source of truth for run state.

Pure Python + the filesystem - no HTTP in here, so the whole state machine is
directly unit-testable. `http_api` wraps this for cross-machine access.

Every mutation is serialized under one lock and persisted to
<state_dir>/runs/<run_id>.json, so `status`/`result`/`logs` still work after a
coordinator restart and either machine's agent can read the same evidence.
"""

import copy
import json
import os
import threading

from . import protocol as P

# A run that sits in a live state past its deadline is swept to ERROR rather
# than hanging the suite forever.
DEFAULT_RUN_TIMEOUT = 180.0
WORKER_STALE_AFTER = 30.0


class Coordinator:
    def __init__(self, state_dir: str, run_timeout: float = DEFAULT_RUN_TIMEOUT):
        self.state_dir = os.path.abspath(state_dir)
        self.runs_dir = os.path.join(self.state_dir, "runs")
        self.logs_dir = os.path.join(self.state_dir, "logs")
        os.makedirs(self.runs_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)

        self.run_timeout = run_timeout
        self._lock = threading.Lock()
        self._runs = {}      # run_id -> run dict
        self._order = []     # run_ids, oldest first
        self._workers = {}   # worker_id -> worker dict
        self._messages = []  # agent-to-agent mailbox
        self._message_seq = 0
        self._load()
        self._load_messages()

    # ── persistence ──────────────────────────────────────────────────────────

    def _load(self):
        if not os.path.isdir(self.runs_dir):
            return
        loaded = []
        for name in os.listdir(self.runs_dir):
            if not name.endswith(".json"):
                continue
            path = os.path.join(self.runs_dir, name)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    run = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(run, dict) and run.get("run_id"):
                loaded.append(run)
        loaded.sort(key=lambda r: r.get("created_at", 0))
        for run in loaded:
            self._runs[run["run_id"]] = run
            self._order.append(run["run_id"])

    def _persist(self, run: dict):
        path = os.path.join(self.runs_dir, f"{run['run_id']}.json")
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(run, f, indent=2, sort_keys=True)
        os.replace(tmp, path)

    def run_log_dir(self, run_id: str) -> str:
        path = os.path.join(self.logs_dir, run_id)
        os.makedirs(path, exist_ok=True)
        return path

    # ── runs ─────────────────────────────────────────────────────────────────

    def create_run(self, scenario: str, suite_id: str = "", config: dict = None,
                   created_by: str = "", timeout: float = None) -> dict:
        ts = P.now()
        run_id = P.new_run_id()
        timeout = float(timeout or self.run_timeout)
        run = {
            "run_id": run_id,
            "scenario": scenario,
            "suite_id": suite_id,
            "state": P.CREATED,
            "created_at": ts,
            "created_at_iso": P.iso(ts),
            "updated_at": ts,
            "deadline": ts + timeout,
            "timeout_seconds": timeout,
            "created_by": created_by,
            "config": dict(config or {}),
            "claimed_by": None,
            "history": [{"state": P.CREATED, "at": ts, "at_iso": P.iso(ts),
                         "actor": created_by, "detail": "run created"}],
            "result": None,
            "evidence": [],
            "actions": [],
        }
        with self._lock:
            self._runs[run_id] = run
            self._order.append(run_id)
            self._persist(run)
            return copy.deepcopy(run)

    def get_run(self, run_id: str):
        # Deep copy: a shallow dict() would hand out the live history/actions/
        # evidence/config lists, letting any caller mutate coordinator state.
        with self._lock:
            run = self._runs.get(run_id)
            return copy.deepcopy(run) if run else None

    def list_runs(self, limit: int = 50, status: str = None, suite_id: str = None) -> list:
        with self._lock:
            runs = [self._runs[rid] for rid in self._order]
        if status:
            runs = [r for r in runs if r["state"] == status]
        if suite_id:
            runs = [r for r in runs if r.get("suite_id") == suite_id]
        # limit=0 must mean "none", not "everything" (runs[-0:] is the whole list).
        runs = runs[-limit:] if limit > 0 else []
        return [copy.deepcopy(r) for r in runs]

    def _record(self, run: dict, state: str, actor: str, detail: str):
        ts = P.now()
        run["state"] = state
        run["updated_at"] = ts
        run["history"].append({"state": state, "at": ts, "at_iso": P.iso(ts),
                               "actor": actor, "detail": detail})
        self._persist(run)

    def transition(self, run_id: str, to_state: str, actor: str = "",
                   detail: str = "", expected_from: str = None) -> dict:
        """Guarded state change. `expected_from` makes the change conditional,
        which is what keeps two workers from both driving the same run."""
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(run_id)
            current = run["state"]
            if expected_from is not None and current != expected_from:
                raise P.TransitionError(
                    f"run {run_id} is in {current!r}, expected {expected_from!r}")
            P.check_transition(current, to_state)
            self._record(run, to_state, actor, detail)
            if to_state == P.CLIENT_READY:
                # The deadline must cover execution, not the wait for a client:
                # otherwise a client that starts minutes after the lab finds the
                # run already swept, which breaks "start order does not matter".
                run["deadline"] = P.now() + run["timeout_seconds"]
            # LAB_READY means the lab is done preparing; the coordinator
            # immediately advertises the run so a client can pick it up.
            if to_state == P.LAB_READY:
                P.check_transition(P.LAB_READY, P.WAITING_FOR_CLIENT)
                self._record(run, P.WAITING_FOR_CLIENT, "coordinator",
                             "advertised for client workers")
            return copy.deepcopy(run)

    def claim_next(self, role: str, worker_id: str) -> dict:
        """Atomically hand the oldest waiting run to one client worker."""
        if role != P.ROLE_CLIENT:
            raise ValueError(f"only {P.ROLE_CLIENT} workers claim runs, got {role!r}")
        with self._lock:
            for run_id in self._order:
                run = self._runs[run_id]
                if run["state"] != P.WAITING_FOR_CLIENT:
                    continue
                run["claimed_by"] = worker_id
                run["deadline"] = P.now() + run["timeout_seconds"]
                self._record(run, P.CLIENT_READY, worker_id, "claimed by client worker")
                return copy.deepcopy(run)
            return None

    def submit_result(self, run_id: str, result: dict, actor: str = "") -> dict:
        """Attach a scenario result and move the run to its terminal state.
        The submitted status is authoritative but must be terminal."""
        status = result.get("status")
        if status not in P.TERMINAL_STATES:
            raise ValueError(f"result status must be terminal, got {status!r}")
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(run_id)
            if P.is_terminal(run["state"]):
                raise P.TransitionError(
                    f"run {run_id} already finished as {run['state']!r}")
            if run["state"] != P.COLLECTING:
                P.check_transition(run["state"], P.COLLECTING)
                self._record(run, P.COLLECTING, actor, "result submitted")
            run["result"] = copy.deepcopy(result)
            self._record(run, status, actor, f"finalized as {status}")
            return copy.deepcopy(run)

    def attach_evidence(self, run_id: str, name: str, content: str,
                        kind: str = "log", actor: str = "") -> dict:
        """Persist a log/artifact next to the run so the other machine's agent
        can read exactly what this side saw."""
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(run_id)
            safe = "".join(c for c in name if c.isalnum() or c in "._-") or "evidence"
            path = os.path.join(self.run_log_dir(run_id), safe)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            entry = {"name": safe, "kind": kind, "path": path,
                     "bytes": len(content.encode("utf-8")),
                     "actor": actor, "at": P.now()}
            run["evidence"].append(entry)
            run["updated_at"] = P.now()
            self._persist(run)
            return entry

    # ── lab actions (client asks the lab to do something mid-run) ────────────

    def request_action(self, run_id: str, action: str, params: dict = None,
                       actor: str = "") -> dict:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(run_id)
            entry = {
                "action_id": f"act-{len(run['actions']) + 1}",
                "action": action,
                "params": dict(params or {}),
                "requested_by": actor,
                "requested_at": P.now(),
                "state": "pending",
                "ok": None,
                "detail": "",
                "completed_at": None,
            }
            run["actions"].append(entry)
            run["updated_at"] = P.now()
            self._persist(run)
            return dict(entry)

    def next_action(self, run_id: str, actor: str = "") -> dict:
        """Lab worker polls for work it must perform for a running scenario."""
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(run_id)
            for entry in run["actions"]:
                if entry["state"] == "pending":
                    entry["state"] = "in_progress"
                    entry["claimed_by"] = actor
                    run["updated_at"] = P.now()
                    self._persist(run)
                    return dict(entry)
            return None

    def complete_action(self, run_id: str, action_id: str, ok: bool,
                        detail: str = "") -> dict:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise KeyError(run_id)
            for entry in run["actions"]:
                if entry["action_id"] == action_id:
                    entry["state"] = "done" if ok else "failed"
                    entry["ok"] = bool(ok)
                    entry["detail"] = detail
                    entry["completed_at"] = P.now()
                    run["updated_at"] = P.now()
                    self._persist(run)
                    return dict(entry)
            raise KeyError(action_id)

    # ── agent mailbox ────────────────────────────────────────────────────────
    # A durable, ordered channel so the agent on each machine can talk to the
    # other directly instead of a human copying text between sessions. Kept
    # separate from run evidence: messages outlive any single run.

    def post_message(self, sender: str, to: str, text: str, kind: str = "note") -> dict:
        with self._lock:
            entry = {
                "seq": self._message_seq + 1,
                "at": P.now(),
                "at_iso": P.iso(P.now()),
                "from": sender,
                "to": to,
                "kind": kind,
                "text": text,
            }
            self._message_seq += 1
            self._messages.append(entry)
            self._persist_messages()
            return dict(entry)

    def list_messages(self, since: int = 0, to: str = None, limit: int = 100) -> list:
        with self._lock:
            out = [m for m in self._messages if m["seq"] > since
                   and (to is None or m["to"] in (to, "all"))]
            return [dict(m) for m in out[-limit:]]

    def _persist_messages(self):
        path = os.path.join(self.state_dir, "messages.json")
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"messages": self._messages}, f, indent=2)
        os.replace(tmp, path)

    def _load_messages(self):
        path = os.path.join(self.state_dir, "messages.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                self._messages = json.load(f).get("messages", [])
        except (OSError, json.JSONDecodeError):
            self._messages = []
        self._message_seq = max((m.get("seq", 0) for m in self._messages), default=0)

    # ── workers ──────────────────────────────────────────────────────────────

    def heartbeat(self, worker_id: str, role: str, info: dict = None) -> dict:
        if role not in P.ROLES:
            raise ValueError(f"unknown role {role!r}")
        with self._lock:
            entry = self._workers.get(worker_id) or {
                "worker_id": worker_id, "role": role, "first_seen": P.now()}
            entry["role"] = role
            entry["last_seen"] = P.now()
            entry["info"] = dict(info or {})
            self._workers[worker_id] = entry
            return dict(entry)

    def list_workers(self) -> list:
        now = P.now()
        with self._lock:
            out = []
            for entry in self._workers.values():
                item = dict(entry)
                item["age_seconds"] = round(now - entry["last_seen"], 1)
                item["stale"] = item["age_seconds"] > WORKER_STALE_AFTER
                out.append(item)
            return out

    # ── housekeeping ─────────────────────────────────────────────────────────

    def sweep_stale(self) -> list:
        """Fail runs that blew past their deadline so nothing hangs forever."""
        swept = []
        now = P.now()
        with self._lock:
            for run_id in self._order:
                run = self._runs[run_id]
                if P.is_terminal(run["state"]):
                    continue
                if now <= run.get("deadline", 0):
                    continue
                run["result"] = P.result(
                    run_id=run_id, scenario=run["scenario"], status=P.ERROR,
                    worker="coordinator", duration_seconds=now - run["created_at"],
                    errors=[{
                        "kind": "timeout",
                        "message": (f"run exceeded {run['timeout_seconds']}s in state "
                                    f"{run['state']!r}"),
                        "state_when_timed_out": run["state"],
                    }],
                )
                self._record(run, P.ERROR, "coordinator", "stale run swept")
                swept.append(dict(run))
        return swept

    def status(self) -> dict:
        with self._lock:
            runs = [self._runs[rid] for rid in self._order]
            active = [dict(r) for r in runs if not P.is_terminal(r["state"])]
            recent = [dict(r) for r in runs[-10:]]
        counts = {}
        for run in runs:
            counts[run["state"]] = counts.get(run["state"], 0) + 1
        return {
            "state_dir": self.state_dir,
            "total_runs": len(runs),
            "counts_by_state": counts,
            "active_runs": [_summary(r) for r in active],
            "recent_runs": [_summary(r) for r in recent],
            "workers": self.list_workers(),
        }


def _summary(run: dict) -> dict:
    result = run.get("result") or {}
    return {
        "run_id": run["run_id"],
        "scenario": run["scenario"],
        "suite_id": run.get("suite_id", ""),
        "state": run["state"],
        "created_at_iso": run.get("created_at_iso", ""),
        "claimed_by": run.get("claimed_by"),
        "failed_assertions": [a["name"] for a in result.get("assertions", [])
                              if not a.get("passed")],
    }
