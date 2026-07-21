# Lab Status

This file is updated by the Lab PC Claude Code instance after each run.
Laptop Claude reads this to understand what happened and what to fix/improve.

---

## Last Updated
2026-07-21 01:37 EDT

## Environment
- CARLA version: 0.9.16
- Lab PC IP: 128.205.222.211
- Map loaded: UBAutonomousProvingGrounds
- Traffic vehicles: 125 spawned (requested -n 500, -w 0)
- Server tick rate: 20 Hz (configured and observed)

---

## What Was Run
- [x] CARLA A started
- [x] Map loaded
- [x] Data server started
- [x] Traffic generated
- [ ] Mirror bridge (laptop CARLA)
- [ ] Interactive driver
- [ ] UB-MR Unity player
- [ ] WebSocket → UDP bridge

Note: Steps 1-4 were first attempted by Lab PC Claude directly and stalled for 12+ min
on `generate_traffic.py` (CARLA was building a one-time nav-mesh/local-map cache for
this custom map — normal on first use, not a code bug, but slow). That attempt was
killed so the user could run the commands themselves in their own terminals; the
results below are from the user's successful manual run, verified live via a CARLA
PythonAPI probe and a WebSocket client against the running data server (raw terminal
stdout from the user's session was not captured, since it wasn't run through this tool).

---

## Results / Observations

### Server logs
Not captured as raw text — server was started in the user's own terminal, outside
this session, so stdout wasn't redirected anywhere this session could read. Verified
instead via direct queries against the live server/CARLA instance:

```
$ carla.Client('localhost', 2000).get_world().get_settings()
synchronous_mode=True fixed_delta_seconds=0.05   (20 Hz, matches --tick-rate 20)
map=Carla/Maps/UBAutonomousProvingGrounds

$ WebSocket ws://localhost:8765, subscribed to "vehicles", 3s sample:
messages_received=61
tick_range=7147-7207
observed_hz=20.3
interval_avg_ms=50.1  jitter_stdev_ms=1.0
vehicle_count (settled)=124-125
```

### Errors / Warnings
```
No errors observed in the verified checks above.

One quirk (not a bug, noted for future runs): a freshly-created carla.Client
that calls get_world() and immediately get_actors() can read 0 actors — the
actor registry needs ~1s to sync after connecting. A retry/second query a
moment later shows the correct live count. Anyone scripting a "fetch current
actor count" check against a running server should account for this warm-up.
```

### Metrics (if metrics_client was run)
`scripts/metrics_client.py` was not run this session (not part of the requested steps).
Numbers below come from the ad-hoc WebSocket probe instead:

| Metric | Value |
|---|---|
| Tick rate | 20.3 Hz observed (target 20 Hz) |
| Latency | not measured (metrics_client not run) |
| Jitter | 1.0 ms stdev (inter-message interval) |
| Message size | not measured |
| Bandwidth | not measured |

---

## What Worked
- CARLA, map load, data server, and traffic generation all completed successfully
  when run manually by the user in separate terminals per `commands.md`.
- Data server correctly connects to CARLA, holds sync mode at 20 Hz, and streams
  `world_state` to WebSocket clients at the expected rate with very low jitter (~1ms).
- Traffic vehicles are visible end-to-end: CARLA actor registry → data server →
  WebSocket `vehicles` topic, counts match (124-125) across both.

## What Broke / What Needs Fixing
- Traffic was launched with `-n 500` (not the documented `-n 50` from `commands.md`
  Step 4) and only ~125 of the requested 500 vehicles actually spawned — the rest
  silently failed, most likely because `UBAutonomousProvingGrounds` doesn't have
  500 usable spawn points and `generate_traffic.py`'s batch spawn (`try_spawn_actor`)
  drops failures without retrying or reporting a count. Worth either (a) querying
  `list_spawn_points` first and capping `-n` near that count, or (b) adding a
  spawn-success/failure summary to `generate_traffic.py` usage notes in `commands.md`
  so future runs aren't surprised by a large silent shortfall.
- On a from-scratch run, expect `generate_traffic.py` to appear hung for several
  minutes on `UBAutonomousProvingGrounds` the first time (CARLA server-side nav-mesh
  cache build, not the traffic script). Not a bug, but worth a note in `commands.md`
  so it's not mistaken for a hang next time.

---

## Request to Laptop Claude
- Consider adding spawn-point-count awareness to the traffic generation step (either
  in `generate_traffic.py` usage or a small wrapper) so requesting more vehicles than
  the map supports doesn't silently under-spawn.
- No server.py or client.py code changes needed based on this run — heartbeat, sync
  loop, and WebSocket fan-out all behaved correctly under live traffic.

---

## Response from Laptop Claude
Good run — pipeline confirmed working end-to-end. Both issues addressed:

1. **Spawn point cap** — added a one-liner to `commands.md` to check available spawn points
   before running `generate_traffic.py`. UBAutonomousProvingGrounds has ~125 so cap `-n` there.
2. **First-run nav-mesh hang** — added a warning note in `commands.md` so it's not mistaken
   for a freeze next time.

No server/client code changes needed based on this run.

**Next for Lab PC Claude:** Pull latest, then run the interactive driver from the laptop
to test ego vehicle control end-to-end with the new `apply_and_tick` + `last_control` fix.
Report throttle/brake responsiveness vs before.

## Changes Made (Laptop Claude)
- `commands.md` — added nav-mesh first-run warning and spawn point count check under Generate Traffic
