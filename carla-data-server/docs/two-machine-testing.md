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

| Scenario | Checks | Needs real CARLA? |
| --- | --- | --- |
| `connectivity` | TCP reach, WebSocket handshake, `welcome` schema, first `world_state` | no |
| `world_state` | message validity, tick strictly increasing, sim-time advancing, subscribed topics present, **unsubscribed topics absent** | no |
| `sustained_stream` | rate within tolerance of tick rate, tick gaps bounded, no mid-run drop, ping/ack RTT bounded | no |
| `ego_control` | throttle actually accelerates the car and braking slows it; no failed control acks | **yes** |
| `multi_client` | two participants, each with its own ego: own car flagged `is_ego`, peer's car not, both agree on the vehicle set and on peer position | **yes** |
| `peer_departure` | a participant leaves: peers get `client_left` naming its `owned_actor_ids`, and its car leaves the world | partly |
| `udp_bridge` | `ws_to_udp_bridge.py` delivers Unity-shaped UDP packets carrying `tick`/`sim_time`, ticks advancing | no |
| `reconnect` | lab restarts the data server; client must observe the outage, reconnect, get a new `client_id`, resume streaming | no |
| `mirror` | `carla_mirror_client.py` replicates into a shadow CARLA, maps match, actors appear | **yes** + shadow sim |
| `camera_follow` | `scripts/camera_follow.py` repositions a local CARLA spectator to track an actor | **yes** |

Scenarios that need a real simulator are **SKIPPED**, never passed or failed,
when the server is in STUB mode or no CARLA is reachable — STUB mode serves a
fixed synthetic world in which a spawned actor never appears, so actor-level
assertions cannot mean anything there.

### Role parity with UB-DigitalTwin

The suite is built to cover the same responsibilities UB-DigitalTwin splits
across Redis roles, so this server can stand in for that hub:

| UB-DigitalTwin role | Their message type | Covered here by |
| --- | --- | --- |
| `traffic-publisher` | 2 (traffic batch) | the data server itself (authoritative) |
| `traffic-renderer` | subscribes 2 | `mirror` |
| `ego-renderer` | 0 (participant pose) | `multi_client` (peer car visible and correctly placed) |
| `multi-agent-renderer` | 0 / 1 | `multi_client` + `peer_departure` |
| `manual-control` | 0 | `ego_control` |
| `udp-bridge` | 3 (MR ego relay) | `udp_bridge` |
| `camera-follow` | none (reads CARLA directly) | `camera_follow` |

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
| `mirror` SKIPPED with "accepted TCP but is not a CARLA RPC endpoint" | `SHADOW_CARLA_PORT` points at the **primary** simulator's streaming port. CARLA binds `rpc_port`, `+1` and `+2`, so with CARLA on 2000 the ports 2001/2002 are already taken. Start the shadow with `-carla-rpc-port=2003` and set `SHADOW_CARLA_PORT=2003` (the default). |
| Server runs in STUB mode | `carla` not importable — use `venv/`, not `venv-stub/`. `doctor --role lab` reports this. |
| Runs end as `error` with `carla_map: loaded 'X', required 'Y'` | The simulator has the wrong map loaded. Load `CARLA_MAP` in CARLA, or set `CARLA_MAP=any` if you meant to use another map. |
| `mirror` fails `shadow_map_matches` | The shadow CARLA is on a different map than the lab's — mirrored actors and traffic-light indexes would refer to different worlds. |
| `websockets_legacy_api` FAIL | `server.py` uses the legacy asyncio API removed in websockets 14+. `pip install 'websockets<14'`. |
| Run stuck, then `error` with `kind: timeout` | The coordinator swept it past its deadline; check the other machine's worker is alive via `status`. |

## Phase 5 — automated code sync (opt-in)

```
python -m orchestration lab --keep-alive --auto-sync
```

Every `--sync-interval` seconds (30 by default) the lab worker checks the
remote, fast-forwards, restarts the data server so it runs the new code, and
re-queues the scenarios that had failed — bounded by `--max-retries` (2) so a
persistent failure cannot spin forever. It announces what it did on the agent
mailbox.

Safety rules, all covered by `tests/test_orch_gitsync.py` against real repos:

- **fast-forward only** — never merges, rebases, resets, checks out or forces
- **refuses a dirty working tree** — stops and names the files rather than
  stashing or discarding
- **refuses diverged history** — messages both sides instead of guessing
- **never commits or pushes** — authoring stays with a human or an agent under
  human supervision

**It is off by default, and should stay off unless you understand this:** the
coordinator has no authentication, so anything that can reach its port could
trigger a sync, which is remote code execution on the lab machine. Only enable
it on a network you control, ideally while watching.

## Agent mailbox

The session on each machine can talk to the other through the coordinator
rather than a human copying text:

```
python -m orchestration mail send --to lab --sender client-agent --text "..."
python -m orchestration mail read --to client --watch
```

Messages are ordered, durable across coordinator restarts, and filterable by
recipient (`lab`, `client`, or `all`).

## Scope and limits

Status as of 2026-09-21, and what each claim rests on:

| Scenario | Single machine, STUB | Lab PC, real CARLA | Cross-machine |
| --- | --- | --- | --- |
| `connectivity` | PASS | PASS | PASS |
| `world_state` | PASS | PASS | PASS |
| `sustained_stream` | PASS | PASS | PASS (19.98 Hz, 34 ms RTT) |
| `reconnect` | PASS | PASS (real server restart) | not run |
| `mirror` | SKIPPED | PASS (0 → 6 shadow actors) | not run |
| `ego_control` | SKIPPED (no real actors) | **not yet run** | not run |
| `multi_client` | SKIPPED (no real actors) | **not yet run** | not run |
| `peer_departure` | PASS (event only) | **not yet run** | not run |
| `udp_bridge` | PASS (5 assertions) | **not yet run** | not run |
| `camera_follow` | SKIPPED (no local CARLA) | **not yet run** | not run |

So: the first five are proven on real hardware; the five added for role parity
are implemented and unit-covered, but four of them have never executed against
a real simulator. Treat those as unverified until they have.

Known environment trap: the interpreter running the lab worker decides whether
the data server is real or STUB, because it launches `server/server.py` with
`sys.executable`. With a CARLA-capable interpreter and no simulator listening,
`server.py` exits rather than falling back to STUB — so use a stub interpreter
when you deliberately want STUB mode.

Still not implemented, deliberately: an agent autonomously authoring and
pushing code. Phase 5 only *consumes* commits someone else published.
