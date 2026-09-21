# Lab PC test plan: real CARLA, real hardware, cross-machine

Everything runnable without real CARLA or a second machine is already covered by
`tests/` (`pytest tests/`) and the STUB-mode manual runbook that produced them —
see `CLAUDE.md`'s "Common commands" and the git history for that session. This
doc covers what's left: tests that genuinely need real CARLA and/or a second
machine.

All commands run from `~/Documents/carla-data-server/carla-data-server` with
`venv` activated (real CARLA installed), unless noted.

## Deployment topology

- **Lab PC** (128.205.222.211, RTX 5090): CARLA at `~/Documents/carla`
  (`UBAutonomousProvingGrounds` map), this repo, `venv` has CARLA 0.9.16.
  Normally runs CARLA A (headless, port 2000) + the data server (port 8765) +
  a traffic-generation script.
- **Laptop**: its own CARLA build, this same repo, the UB-MR Unity project and
  a built player. Normally runs the mirror bridge (→ its own local CARLA B),
  `interactive_driver.py`, and `ws_to_udp_bridge.py` (→ UB-MR).
- **Known blocker**: campus wifi silently drops inbound TCP from the laptop to
  the Lab PC's port 8765 (ICMP works, TCP times out). Tailscale is the planned
  fix, pending setup on both machines. **Tier 2 is blocked until that's
  resolved, or both machines are on the same LAN.**

## Tier 1 — Lab PC only (no cross-machine networking needed)

- [ ] **1. Bring-up sanity.** Start CARLA A headless on port 2000, then:
  ```
  python server/server.py --carla-host localhost --carla-port 2000 --tick-rate 20
  ```
  Confirm the log shows a real connection, not "STUB mode". Connect
  `python client/client.py --server ws://localhost:8765 --role spectator`
  and confirm real map actors appear and RTT is near-zero over loopback.

- [ ] **2. Real manual-control drive — actor-cache bug regression.**
  ```
  python client/client.py --server ws://localhost:8765 --role manual --spawn-index 0
  ```
  This exercises `CarlaConnection.apply_and_tick`'s actor-cache workaround for
  the known CARLA sync-mode bug (`world.get_actor()` silently ignores
  `apply_control()`) — confirm the vehicle **actually moves** through the full
  LISTING→SPAWNING→DRIVING→DONE sequence, not just that acks come back `ok`.
  STUB mode can't catch a regression here since it never touches real CARLA
  actors.

- [ ] **3. Real sensor camera + dedup.**
  ```
  python scripts/interactive_driver.py --server ws://localhost:8765 --spawn-index 0
  ```
  (needs a display on the Lab PC, or X11 forwarding.) In parallel:
  ```
  python scripts/metrics_client.py --server ws://localhost:8765
  ```
  Confirm the pygame window renders continuously with no flicker/freeze on
  skip-ticks, and that `metrics_client.py`'s message-size/bandwidth is
  measurably lower than a run where every tick resends the same JPEG. This is
  `SensorBuffer`'s dirty-tracking dedup (`server/server.py`), which STUB mode
  can't exercise at all since it never attaches a real sensor listener. See
  the "Publish-rate contract" section of `docs/wire-protocol.md`.

- [ ] **4. Two-CARLA mirroring, entirely local.** Start a second CARLA
  instance on the Lab PC with `-carla-rpc-port=2003` — **not 2001**: CARLA
  binds `rpc_port`, `+1` and `+2`, so 2001/2002 already belong to the
  primary simulator on 2000 (a TCP probe of 2001 succeeds and looks like a
  second simulator, but RPC calls time out). Then:
  ```
  python bridges/carla_mirror_client.py --server ws://localhost:8765 \
                                        --shadow-host localhost --shadow-port 2003
  ```
  Confirms vehicle/pedestrian spawn-teleport-despawn logic and traffic-light
  index-matching against two *real* CARLA worlds. Also restart the server with
  `--traffic-rate-divisor 4` and visually confirm the documented "stepping"
  consequence in the mirrored pedestrians/lights (expected — see
  `docs/wire-protocol.md`'s rate-tiering section — not a bug).

- [ ] **5. Load/scale evidence gathering.** Use CARLA's bundled
  `PythonAPI/examples/generate_traffic.py` (or the Traffic Manager directly)
  to spawn a meaningful number of vehicles/pedestrians on CARLA A. Run the
  data server with and without `--traffic-rate-divisor`, watching the log for
  `TickLoop overrun` warnings. This is the evidence-based check that was
  previously missing before ever reconsidering offloading sensor JPEG
  encoding to another process — closes that question with real data.

- [ ] **6. Full local integration smoke test.** Run server + spectator +
  manual + `interactive_driver.py` + local `carla_mirror_client.py` together
  at `--tick-rate 20`, watching for any interaction effect (GIL contention
  between JPEG encoding and the tick loop, tick overruns) not visible under
  STUB mode's near-zero CPU cost.

## Tier 2 — Cross-machine, laptop ↔ Lab PC (gated on network connectivity)

- [ ] **0. Prerequisite gate — check this first, every time.**
  ```
  nc -zv <lab-ip-or-tailscale-ip> 8765
  ```
  from the laptop. If it times out over the raw campus IP (128.205.222.211),
  none of the items below will work until Tailscale is set up (or both
  machines are on the same LAN) — don't spend time on them yet.

- [ ] **1. `interactive_driver.py` (laptop) → data server (Lab PC).**
  ```
  python scripts/interactive_driver.py --server ws://<lab-ip>:8765 --spawn-index 0
  ```
  The real end-to-end control-loop test loopback can't substitute for:
  steering commands laptop→LabPC→CARLA, camera frames CARLA→LabPC→laptop,
  both directions over an actual WAN-like link. Confirm it's usable (latency,
  frame rate), not just technically connected.

- [ ] **2. `carla_mirror_client.py` (laptop) → data server (Lab PC) → laptop's
  own local CARLA B.**
  ```
  python bridges/carla_mirror_client.py --server ws://<lab-ip>:8765 \
                                        --shadow-host localhost --shadow-port 2000
  ```
  The real version of Tier 1 item 4, over the actual deployment link instead
  of loopback — confirms mirroring holds up under real latency/jitter, and
  that `--traffic-rate-divisor`'s cosmetic "stepping" doesn't turn into an
  actual mis-teardown under jitter (e.g. a pedestrian's presence flickering
  due to reordered delivery).

- [ ] **3. `ws_to_udp_bridge.py` (laptop) → data server (Lab PC) → UB-MR Unity
  player (laptop).**
  ```
  python bridges/ws_to_udp_bridge.py --server ws://<lab-ip>:8765 \
                                     --udp-host localhost --udp-port 12345
  ```
  Confirms `TrafficReceiver.cs` actually consumes the `tick`/`sim_time`
  fields (see `docs/wire-protocol.md`'s "Simulation-time contract") correctly
  and still tolerates the legacy `timestamp` field — a real
  backward-compatibility check against the actual Unity code, not just the
  bridge's own Python.

- [ ] **4. Coordinate-conversion visual check.** Per the documented mapping
  (`Unity.x = CARLA.y, Unity.y = CARLA.z, Unity.z = CARLA.x`, yaw negated),
  visually confirm in the UB-MR Unity app that vehicles appear at the
  position/orientation matching CARLA's actual world — an easy-to-silently-
  invert mapping that only a real Unity render can catch.

- [ ] **5. Multi-machine silence-timeout eviction.** With the laptop client
  connected to the Lab PC server, kill the laptop's network (or the process
  abruptly) and confirm the Lab PC's janitor still evicts it within
  `--silence-timeout`, destroys its ego, and broadcasts `client_left` — under
  real network jitter this time, not the near-zero loopback timing already
  verified in the STUB-mode runbook.

## Known open items to keep in mind while testing

- `CARLAClient.disconnect()` (`client/client.py`) doesn't actually close the
  active connection — a client that calls it (e.g. `ManualControlClient`
  reaching `DONE`) hangs until externally killed rather than exiting. This
  will affect item 2 above and any script relying on a clean exit after
  `disconnect()`.
- STUB mode's `is_ego` limitation (item 3 in the earlier manual runbook)
  doesn't apply here — real CARLA spawns real actors, so `is_ego` should
  behave correctly live for the first time in items 1-2 above.
