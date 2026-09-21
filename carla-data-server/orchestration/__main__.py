"""CLI for the two-machine test orchestration.

    LAB machine:     python -m orchestration lab
    CLIENT machine:  python -m orchestration client --lab-host <LAB_IP>

Inspection (either machine, --lab-host as needed):

    python -m orchestration status --json
    python -m orchestration result <run-id> --json
    python -m orchestration logs <run-id>
    python -m orchestration runs
    python -m orchestration doctor --role client
    python -m orchestration rerun <run-id>
"""

import argparse
import json
import logging
import os
import signal
import sys
import time

from . import protocol as P
from . import scenarios as S
from .client_worker import ClientWorker
from .config import Config
from .coordinator import Coordinator
from .doctor import format_doctor, run_doctor
from .http_api import CoordinatorClient, CoordinatorError, CoordinatorServer
from .lab_worker import LabWorker


def _setup_logging(verbose: bool):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )


def _emit(payload, as_json: bool, text: str = None):
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(text if text is not None else json.dumps(payload, indent=2, default=str))


def _api(cfg) -> CoordinatorClient:
    return CoordinatorClient(cfg.coordinator_url)


def _format_summary(summary: dict) -> str:
    if summary.get("aborted"):
        return f"suite {summary.get('suite_id', '?')} aborted: {summary['aborted']}"
    lines = [f"suite {summary.get('suite_id', '?')}  "
             f"passed={summary.get('passed', 0)} failed={summary.get('failed', 0)} "
             f"errored={summary.get('errored', 0)} skipped={summary.get('skipped', 0)}",
             ""]
    for run in summary.get("runs", []):
        result = run.get("result") or {}
        failed = [a["name"] for a in result.get("assertions", []) if not a.get("passed")]
        lines.append(f"  {run.get('scenario', '?'):<18} {run.get('state', '?').upper():<8} "
                     f"{run.get('run_id', '')}"
                     + (f"  failed: {failed}" if failed else ""))
    return "\n".join(lines)


# ── commands ─────────────────────────────────────────────────────────────────

def cmd_lab(args, cfg):
    os.makedirs(cfg.state_dir, exist_ok=True)
    suite = args.suite.split(",") if args.suite else (
        [args.scenario] if args.scenario else list(S.DEFAULT_SUITE))
    unknown = [s for s in suite if s not in S.REGISTRY]
    if unknown:
        raise SystemExit(f"unknown scenario(s) {unknown}; available: {sorted(S.REGISTRY)}")

    server = None
    if not args.no_coordinator:
        coordinator = Coordinator(cfg.state_dir)
        server = CoordinatorServer(coordinator, cfg.coordinator_bind_host,
                                   cfg.coordinator_port).start()
        logging.getLogger("orchestration.lab").info(
            "coordinator listening on http://%s:%d (clients connect to "
            "http://<this-machine>:%d)", cfg.coordinator_bind_host, server.port,
            server.port)

    api = CoordinatorClient(f"http://127.0.0.1:{cfg.coordinator_port}"
                            if server else cfg.coordinator_url)
    worker = LabWorker(cfg, api, suite=suite, manage_server=not args.no_manage_server,
                       on_failure=args.on_failure, auto_sync=args.auto_sync,
                       max_retries=args.max_retries, sync_interval=args.sync_interval)
    if args.auto_sync:
        logging.getLogger("orchestration.lab").warning(
            "--auto-sync is ON: upstream commits will be fast-forwarded and run on "
            "this machine automatically. The coordinator has no authentication, so "
            "only do this on a trusted network.")
    # Staying alive is pointless without the data server: reruns would be
    # advertised to a client with nothing behind them.
    worker._keep_server = args.keep_server or args.keep_alive

    def _sigint(signum, frame):
        logging.getLogger("orchestration.lab").info("shutting down...")
        worker.stop()
    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    try:
        summary = worker.run_suite()
    finally:
        if args.keep_alive and server:
            logging.getLogger("orchestration.lab").info(
                "suite done; coordinator + data server still up, servicing reruns")
            try:
                worker.service_forever()
            except KeyboardInterrupt:
                pass
            if not args.keep_server and worker.server:
                worker.server.stop()
        if server:
            server.stop()
    _emit(summary, args.json, _format_summary(summary))
    failed = summary.get("failed", 0) + summary.get("errored", 0)
    return 1 if failed else 0


def cmd_client(args, cfg):
    api = _api(cfg)
    # Start order must not matter: the client may come up before the lab, so
    # wait for the coordinator instead of exiting. Noisy on purpose, so a
    # genuinely wrong LAB_HOST is obvious rather than looking like patience.
    clog = logging.getLogger("orchestration.client")
    deadline = time.monotonic() + args.connect_timeout if args.connect_timeout else None
    last_error, announced = None, 0.0
    while True:
        try:
            api.health()
            break
        except CoordinatorError as exc:
            last_error = exc
            if deadline and time.monotonic() > deadline:
                raise SystemExit(
                    f"{exc}\n\nGave up after {args.connect_timeout:.0f}s. Run "
                    f"`python -m orchestration doctor --role client` for a full "
                    f"diagnosis, or pass --connect-timeout 0 to wait indefinitely.")
            if time.monotonic() - announced > 15:
                announced = time.monotonic()
                clog.warning("waiting for the coordinator at %s ...", cfg.coordinator_url)
            time.sleep(2.0)
    worker = ClientWorker(cfg, api, idle_timeout=args.idle_timeout)

    def _sigint(signum, frame):
        logging.getLogger("orchestration.client").info("shutting down...")
        worker.stop()
    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    results = worker.serve_forever()
    _emit({"completed": results}, args.json,
          "\n".join(f"{r['scenario']:<18} {r['status'].upper()}" for r in results)
          or "no runs executed")
    return 0


def cmd_status(args, cfg):
    status = _api(cfg).status()
    lines = [f"coordinator: {cfg.coordinator_url}",
             f"runs: {status['total_runs']} total  {status['counts_by_state']}", ""]
    if status["workers"]:
        lines.append("workers:")
        for w in status["workers"]:
            lines.append(f"  {w['role']:<7} {w['worker_id']:<32} "
                         f"last seen {w['age_seconds']}s ago"
                         f"{'  STALE' if w['stale'] else ''}")
        lines.append("")
    lines.append("active runs:" if status["active_runs"] else "active runs: none")
    for r in status["active_runs"]:
        lines.append(f"  {r['run_id']}  {r['scenario']:<18} {r['state']}")
    lines.append("")
    lines.append("recent runs:")
    for r in status["recent_runs"]:
        failed = f"  failed: {r['failed_assertions']}" if r["failed_assertions"] else ""
        lines.append(f"  {r['run_id']}  {r['scenario']:<18} {r['state'].upper()}{failed}")
    _emit(status, args.json, "\n".join(lines))
    return 0


def cmd_runs(args, cfg):
    runs = _api(cfg).list_runs(limit=args.limit, status=args.status)
    lines = [f"{r['run_id']}  {r['scenario']:<18} {r['state'].upper():<8} "
             f"{r.get('created_at_iso', '')}" for r in runs]
    _emit({"runs": runs}, args.json, "\n".join(lines) or "no runs")
    return 0


def cmd_result(args, cfg):
    try:
        run = _api(cfg).get_run(args.run_id)
    except CoordinatorError as exc:
        raise SystemExit(str(exc))
    result = run.get("result")
    if args.json:
        _emit({"run": run}, True)
        return 0
    lines = [f"run:      {run['run_id']}",
             f"scenario: {run['scenario']}",
             f"state:    {run['state'].upper()}",
             f"suite:    {run.get('suite_id', '')}",
             f"claimed:  {run.get('claimed_by')}"]
    if not result:
        lines.append("\n(no result submitted yet)")
        lines.append("history: " + " -> ".join(h["state"] for h in run["history"]))
        print("\n".join(lines))
        return 0
    lines += [f"worker:   {result['worker']}",
              f"duration: {result['duration_seconds']}s", "", "assertions:"]
    for a in result["assertions"]:
        mark = "PASS" if a["passed"] else "FAIL"
        lines.append(f"  [{mark}] {a['name']}")
        if not a["passed"]:
            lines.append(f"         expected: {a['expected']!r}")
            lines.append(f"         observed: {a['observed']!r}")
            if a.get("detail"):
                lines.append(f"         note:     {a['detail']}")
    if result["metrics"]:
        lines += ["", "metrics:"]
        lines += [f"  {k}: {v}" for k, v in sorted(result["metrics"].items())]
    if result["errors"]:
        lines += ["", "errors:"]
        for err in result["errors"]:
            lines.append(f"  {err.get('kind')}: {err.get('message')}")
            if err.get("traceback"):
                lines.append("    " + err["traceback"].replace("\n", "\n    ").rstrip())
    if run.get("evidence"):
        lines += ["", "evidence (see `logs " + run["run_id"] + "`):"]
        lines += [f"  {e['name']} ({e['kind']}, {e['bytes']}B)" for e in run["evidence"]]
    lines += ["", "history: " + " -> ".join(h["state"] for h in run["history"])]
    print("\n".join(lines))
    return 0


def cmd_logs(args, cfg):
    run = _api(cfg).get_run(args.run_id)
    evidence = run.get("evidence") or []
    if not evidence:
        print(f"no evidence attached to {args.run_id}")
        return 0
    for entry in evidence:
        if args.name and entry["name"] != args.name:
            continue
        print(f"===== {entry['name']} ({entry['kind']}, from {entry['actor']}) =====")
        try:
            with open(entry["path"], "r", encoding="utf-8", errors="replace") as f:
                print(f.read())
        except OSError as exc:
            print(f"<unreadable: {exc}>  (path is on the {entry['kind']} machine)")
    return 0


def cmd_rerun(args, cfg):
    api = _api(cfg)
    old = api.get_run(args.run_id)
    run = api.create_run(scenario=old["scenario"], suite_id=old.get("suite_id", ""),
                         config=old.get("config"), created_by="rerun",
                         timeout=S.TIMEOUTS.get(old["scenario"]))
    api.transition(run["run_id"], P.LAB_PREPARING, actor="rerun", detail="requeued")
    api.transition(run["run_id"], P.LAB_READY, actor="rerun",
                   detail=f"requeued from {args.run_id}")
    _emit({"run": run}, args.json,
          f"requeued {old['scenario']} as {run['run_id']} (a client worker will pick it up)")
    return 0


def cmd_mail(args, cfg):
    """Agent-to-agent channel: the two machines' sessions talk through the
    coordinator instead of a human copying text between them."""
    api = _api(cfg)
    if args.action == "send":
        text = args.text
        if args.file:
            text = sys.stdin.read() if args.file == "-" else open(args.file).read()
        if not text:
            raise SystemExit("nothing to send: pass --text or --file")
        entry = api.post_message(sender=args.sender, to=args.to, text=text, kind=args.kind)
        _emit({"message": entry}, args.json,
              f"sent #{entry['seq']} to {entry['to']} ({len(text)} chars)")
        return 0

    since = args.since
    printed_any = False
    while True:
        messages = api.messages(since=since, to=args.to)
        for m in messages:
            since = max(since, m["seq"])
            printed_any = True
            if args.json:
                print(json.dumps(m, indent=2))
            else:
                print(f"\n===== #{m['seq']} {m['at_iso']}  {m['from']} -> {m['to']}"
                      f"  [{m['kind']}] =====\n{m['text']}")
        if not args.watch:
            if not printed_any and not args.json:
                print(f"no messages for {args.to or 'anyone'} after #{args.since}")
            return 0
        time.sleep(args.interval)


def cmd_doctor(args, cfg):
    report = run_doctor(cfg, args.role)
    _emit(report, args.json, format_doctor(report))
    return 0 if report["ok"] else 1


def cmd_scenarios(args, cfg):
    payload = {"scenarios": [{"name": n, "timeout": S.TIMEOUTS.get(n),
                              "doc": (S.REGISTRY[n].__doc__ or "").strip().split("\n")[0]}
                             for n in S.DEFAULT_SUITE if n in S.REGISTRY]}
    _emit(payload, args.json,
          "\n".join(f"{s['name']:<18} {s['doc']}" for s in payload["scenarios"]))
    return 0


# ── entry point ──────────────────────────────────────────────────────────────

def build_parser():
    parser = argparse.ArgumentParser(prog="python -m orchestration",
                                     description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--lab-host", help="host the CLIENT connects to (env LAB_HOST)")
    parser.add_argument("--coordinator-port", type=int, help="env COORDINATOR_PORT")
    parser.add_argument("--data-server-port", type=int, help="env DATA_SERVER_PORT")
    parser.add_argument("--state-dir", help="env ORCH_STATE_DIR")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    lab = sub.add_parser("lab", help="run the LAB worker (also hosts the coordinator)")
    lab.add_argument("--suite", help="comma-separated scenarios (default: full suite)")
    lab.add_argument("--scenario", help="run exactly one scenario")
    lab.add_argument("--on-failure", choices=["stop", "continue"], default="stop")
    lab.add_argument("--no-manage-server", action="store_true",
                     help="do not start/restart server.py; expect it to be running already")
    lab.add_argument("--no-coordinator", action="store_true",
                     help="use an already-running coordinator instead of hosting one")
    lab.add_argument("--keep-server", action="store_true",
                     help="leave the data server running after the suite")
    lab.add_argument("--keep-alive", action="store_true",
                     help="keep the coordinator serving after the suite finishes")
    lab.add_argument("--auto-sync", action="store_true",
                     help="PHASE 5 (off by default): fast-forward to new upstream "
                          "commits, restart the data server, and retry failed "
                          "scenarios automatically. Only ever fast-forwards, never "
                          "merges/rebases/resets, and refuses on a dirty tree. "
                          "Implies remote code execution - only enable on a machine "
                          "whose coordinator port is on a network you trust.")
    lab.add_argument("--max-retries", type=int, default=2,
                     help="auto-sync retries per scenario before giving up (default 2)")
    lab.add_argument("--sync-interval", type=float, default=30.0,
                     help="seconds between upstream checks when --auto-sync is on")
    lab.set_defaults(func=cmd_lab)

    client = sub.add_parser("client", help="run the CLIENT worker")
    client.add_argument("--idle-timeout", type=float,
                        help="exit after this many seconds with no work")
    client.add_argument("--connect-timeout", type=float, default=300.0,
                        help="how long to wait for the coordinator at startup "
                             "(0 = wait indefinitely). Start order does not matter.")
    client.set_defaults(func=cmd_client)

    status = sub.add_parser("status", help="coordinator + worker + run snapshot")
    status.set_defaults(func=cmd_status)

    runs = sub.add_parser("runs", help="list runs")
    runs.add_argument("--limit", type=int, default=20)
    runs.add_argument("--status")
    runs.set_defaults(func=cmd_runs)

    result = sub.add_parser("result", help="full result for one run")
    result.add_argument("run_id")
    result.set_defaults(func=cmd_result)

    logs = sub.add_parser("logs", help="evidence attached to one run")
    logs.add_argument("run_id")
    logs.add_argument("--name", help="only this evidence file")
    logs.set_defaults(func=cmd_logs)

    rerun = sub.add_parser("rerun", help="requeue the scenario from a previous run")
    rerun.add_argument("run_id")
    rerun.set_defaults(func=cmd_rerun)

    mail = sub.add_parser("mail", help="agent-to-agent messages via the coordinator")
    mail.add_argument("action", choices=["send", "read"])
    mail.add_argument("--to", default=None,
                      help="recipient: lab, client, or all (read: filter)")
    mail.add_argument("--sender", default=os.environ.get("ORCH_AGENT", "agent"),
                      help="who you are (env ORCH_AGENT)")
    mail.add_argument("--text", help="message body")
    mail.add_argument("--file", help="read body from a file, or - for stdin")
    mail.add_argument("--kind", default="note")
    mail.add_argument("--since", type=int, default=0, help="only messages after this seq")
    mail.add_argument("--watch", action="store_true", help="keep polling for new messages")
    mail.add_argument("--interval", type=float, default=5.0)
    mail.set_defaults(func=cmd_mail)

    doctor = sub.add_parser("doctor", help="verify prerequisites and explain failures")
    doctor.add_argument("--role", choices=[P.ROLE_LAB, P.ROLE_CLIENT], default=P.ROLE_CLIENT)
    doctor.set_defaults(func=cmd_doctor)

    scenarios = sub.add_parser("scenarios", help="list available scenarios")
    scenarios.set_defaults(func=cmd_scenarios)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    cfg = Config.from_env(
        lab_host=args.lab_host, coordinator_port=args.coordinator_port,
        data_server_port=args.data_server_port, state_dir=args.state_dir)
    try:
        return args.func(args, cfg)
    except CoordinatorError as exc:
        raise SystemExit(str(exc))


if __name__ == "__main__":
    sys.exit(main())
