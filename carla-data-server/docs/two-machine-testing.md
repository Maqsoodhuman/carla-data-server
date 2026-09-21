# Two-machine autonomous testing

Runs the integration suite across a LAB machine (CARLA + data server) and a
CLIENT machine, with a coordinator as the source of truth so neither side
needs a human relaying "ready" / "done" / "here's the failure" between them.

```
  LAB PC                                    CLIENT PC
  python -m orchestration lab               python -m orchestration client --lab-host <LAB_IP>
     │                                             │
     ├─ verify CARLA + start data server           │
     ├─ create run, LAB_READY ────────────────────▶│  (polls, claims it)
     │                                             ├─ runs the scenario
     │◀──────────────── result + client logs ──────┤
     ├─ attach server log, record PASS/FAIL        │
     └─ next scenario ────────────────────────────▶│  ...
```

The loop is plain Python: it works with no LLM involved. Claude/Codex sessions
sit above it and read `status` / `result` / `logs`.

## Run states

`created → lab_preparing → lab_ready → waiting_for_client → client_ready →
running → collecting → pass | fail | error | skipped`

`skipped` exists so an unavailable precondition (e.g. no shadow CARLA for
`mirror`) is never reported as a pass. A scenario that asserts nothing is
recorded as `error`, not `pass`.

## Scenarios

| Scenario | Checks |
| --- | --- |
| `connectivity` | TCP reach, WebSocket handshake, `welcome` schema, first `world_state` |
| `world_state` | message validity, tick strictly increasing, sim-time advancing, subscribed topics present, **unsubscribed topics absent** |
| `sustained_stream` | rate within tolerance of tick rate, tick gaps bounded, no mid-run drop, ping/ack RTT bounded |
| `reconnect` | lab restarts the data server; client must observe the outage, reconnect, get a new `client_id`, resume streaming |
| `mirror` | `carla_mirror_client.py` replicates into a local shadow CARLA (SKIPPED without the CARLA PythonAPI + a reachable shadow) |

RTT comes from the ping/ack round trip, not `wall_time` deltas — across two
machines with unsynced clocks, wall-clock latency is meaningless, so it is
recorded as a metric but never asserted on.

## Setup

### LAB PC

1. Pull the branch: `git pull origin main`
2. Dependencies: `pip install -r requirements.txt` (plus `pytest` for the unit
   suite). No new runtime dependencies — the coordinator is stdlib-only.
3. Configure (all optional; these are the defaults):
   ```
   export COORDINATOR_PORT=8770      # coordinator binds 0.0.0.0:8770
   export DATA_SERVER_PORT=8765      # data server binds 0.0.0.0:8765
   export CARLA_HOST=127.0.0.1
   export CARLA_PORT=2000
   export CARLA_MAP=UBAutonomousProvingGrounds   # "any" disables the check
   export ORCH_TICK_RATE=20
   ```
4. Start CARLA (headless is fine) on `CARLA_PORT`, **with the map in
   `CARLA_MAP` loaded** — see "Map verification" below.
5. Run **one** command:
   ```
   cd carla-data-server/carla-data-server
   python -m orchestration lab --keep-alive
   ```
   It verifies prerequisites, starts `server/server.py` itself, then walks the
   suite, advertising one run at a time.

### CLIENT PC

1. Point it at the lab and run **one** command:
   ```
   cd carla-data-server/carla-data-server
   export LAB_HOST=<lab-ip-or-tailscale-ip>
   export COORDINATOR_PORT=8770
   export DATA_SERVER_PORT=8765
   python -m orchestration client
   ```

Start order does not matter — whichever comes first waits for the other.

## What happens automatically

The lab advertises a run; the client claims it, executes it, and submits a
structured result with assertions, metrics, errors and artifacts; the lab
attaches its server log and advances to the next scenario. Default policy is
fail-fast (`--on-failure stop`); `--on-failure continue` runs the whole suite.

## Map verification

Every machine in a session must run the same CARLA map (`CARLA_MAP`, default
`UBAutonomousProvingGrounds`). A wrong map does **not** fail loudly on its
own — spawn indexes point at different physical locations, and
`carla_mirror_client.py` pairs traffic lights *by index*, so a mismatched
shadow mirrors onto a different layout while appearing to work. So it is
checked explicitly:

- `doctor --role lab` reports the loaded map versus the required one.
- The lab worker re-checks before advertising each run, and refuses to reach
  `lab_ready` on a mismatch (the run ends as `error` naming both maps).
- The `mirror` scenario asserts the shadow simulator's map matches too.

Comparison ignores CARLA's path prefix and case, so
`/Game/Carla/Maps/UBAutonomousProvingGrounds` matches
`UBAutonomousProvingGrounds`. When the map cannot be read at all (no CARLA
PythonAPI), the check is reported as **SKIP**, never as a pass or a failure.
Set `CARLA_MAP=any` to deliberately test another map (e.g. Town10HD).

## Inspecting

```
python -m orchestration status --json              # workers, active + recent runs
python -m orchestration runs --limit 20            # run list
python -m orchestration result <run-id>            # assertions, metrics, errors
python -m orchestration result <run-id> --json     # same, machine-readable
python -m orchestration logs <run-id>              # client + server evidence
python -m orchestration doctor --role client       # prerequisites, with fixes
python -m orchestration scenarios                  # what can run
```

Every command takes `--lab-host` / `--json`. Failure output always carries the
assertion name plus its `expected` and `observed` values, so the agent on the
other machine can diagnose without re-running anything.

## Common operations

- **Stop a worker**: Ctrl+C (both shut down gracefully).
- **Rerun one failed test**: `python -m orchestration rerun <run-id>` — needs a
  lab worker still up (`--keep-alive`), which keeps the data server running and
  services the requeued run.
- **Run one scenario**: `python -m orchestration lab --scenario world_state`
- **Run a subset**: `python -m orchestration lab --suite connectivity,reconnect`
- **Use an already-running data server**: `lab --no-manage-server` (note:
  `reconnect` then degrades to a client-initiated drop and records
  `mode: client_initiated` instead of `server_restart` — it never claims a
  restart that did not happen).

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `cannot reach coordinator at http://…` | Lab worker not running, wrong `LAB_HOST`/`COORDINATOR_PORT`, or the port is firewalled. Campus wifi silently drops inbound TCP to the lab PC — use Tailscale or the same LAN. `nc -zv <lab-host> 8770` to confirm. |
| `connectivity` fails on `tcp_connect` | Same network issue, but for `DATA_SERVER_PORT`. |
| Everything `SKIPPED` | Expected for `mirror` without the CARLA PythonAPI or a shadow simulator. |
| Server runs in STUB mode | `carla` not importable — use `venv/`, not `venv-stub/`. `doctor --role lab` reports this. |
| Runs end as `error` with `carla_map: loaded 'X', required 'Y'` | The simulator has the wrong map loaded. Load `CARLA_MAP` in CARLA, or set `CARLA_MAP=any` if you meant to use another map. |
| `mirror` fails `shadow_map_matches` | The shadow CARLA is on a different map than the lab's — mirrored actors and traffic-light indexes would refer to different worlds. |
| `websockets_legacy_api` FAIL | `server.py` uses the legacy asyncio API removed in websockets 14+. `pip install 'websockets<14'`. |
| Run stuck, then `error` with `kind: timeout` | The coordinator swept it past its deadline; check the other machine's worker is alive via `status`. |

## Scope and limits

Verified on a single machine in STUB mode (coordinator, both workers, all five
scenarios). **Unverified**: real CARLA behavior, genuine cross-machine
networking, and the `mirror` scenario against a real shadow simulator — those
need the lab hardware. See `docs/lab-test-plan.md` for the manual checks that
still need real CARLA.

Autonomous source-code modification (agent diagnoses → commits → other machine
pulls → restarts → reruns) is deliberately **not** implemented. The result and
evidence structure is designed to support it later; distributed testing is
proven first.
