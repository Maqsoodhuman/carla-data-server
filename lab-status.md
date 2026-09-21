# Lab Status

This file is updated by the Lab PC Claude Code instance after each run.
Laptop Claude reads this to understand what happened and what to fix/improve.

---

## Last Updated
2026-09-20 23:29 EDT

## Environment
- CARLA version: 0.9.16
- Lab PC IP (LAN/WAN, campus wifi blocks inbound TCP to this): 128.205.222.211
- Lab PC also has a live Tailscale interface — see note below (exact tailnet
  IPs deliberately omitted from this file since it's committed to a public repo)
- Map loaded: UBAutonomousProvingGrounds
- Server tick rate: 20 Hz (configured and observed)
- New this session: `orchestration/` package pulled (`d74a177`/`5e86ea4`) — coordinator
  on :8770, data server still on :8765, both bound 0.0.0.0

---

## What Was Run
- [x] `git pull origin main` → fast-forwarded `1a2350b` → `5e86ea4`
- [x] `venv/` set up for orchestration: `pip install pytest`
- [x] `python -m orchestration doctor --role lab`
- [x] CARLA started headless (`-RenderOffScreen`), `UBAutonomousProvingGrounds` loaded
- [x] `pytest tests/ -q`
- [x] `python -m orchestration lab --keep-alive` (this machine)
- [x] `python -m orchestration client` (also this machine — same-box proof run,
      *not* yet cross-machine; done deliberately first per the task instructions,
      since campus wifi drops inbound TCP to this box and a premature
      cross-machine attempt would just look like a hang)
- [ ] Cross-machine run (laptop as CLIENT) — not attempted yet, see "Request" below

---

## Results / Observations

### doctor --role lab
Before starting anything: `BLOCKED: carla_simulator, data_server` (expected —
nothing running yet). After CARLA + map load: fully green, including the new
map check:
```
[PASS] carla_simulator: 127.0.0.1:2000 reachable
[PASS] carla_map: loaded 'Carla/Maps/UBAutonomousProvingGrounds', required 'UBAutonomousProvingGrounds'
```
`websockets` in `venv/` is 16.0; the legacy `WebSocketServerProtocol` API `server.py`
needs is still importable there (deprecated, not removed) — no downgrade needed on
this machine despite the `websockets<14` note in the docs.

### pytest tests/ -q
**87 passed, 1 failed** — not the "69 passed" the task expected. One real
test-isolation bug, surfaced specifically by running in `venv/` against a live,
correctly-mapped CARLA (this is likely why it wasn't caught in STUB-mode testing):

```
FAILED tests/test_orch_doctor.py::test_check_is_skipped_not_failed_when_map_cannot_be_read
  assert check["ok"] is False   # got True
```
The test assumes CARLA is unreachable so `check_carla_map()` returns "cannot
verify" (skipped). It doesn't mock anything, though — it calls the real function
against `CARLA_HOST:CARLA_PORT` (default 127.0.0.1:2000). Since real CARLA is
actually running there with the matching map in this environment, the check
legitimately returns `ok: True`, flipping the assertion. Needs to mock/patch
CARLA's absence or unreachability explicitly rather than relying on the ambient
environment not having a real simulator.

### Scenario suite (real CARLA, both workers on this machine)
| Scenario | Result |
|---|---|
| connectivity | PASS |
| world_state | PASS |
| sustained_stream | PASS (20.1s, no drops) |
| reconnect | **PASS** — lab issued a real `restart_data_server`, client saw `ConnectionRefusedError`, retried, reconnected, resumed streaming |
| mirror | **ERROR** (not SKIP) |

`mirror` root cause (from `orchestration result <run-id> --json`):
```
RuntimeError: time-out of 10000ms while waiting for the simulator,
make sure the simulator is ready and connected to 127.0.0.1:2001
  at scenarios.py:441, scenario_mirror -> shadow.get_world()
```
No assertions ran — it errored before any. Cause: a single CARLA instance's own
**streaming socket** sits on RPC-port+1 (2001 here), which is also the default
`SHADOW_CARLA_PORT`. `doctor`'s `shadow_carla` check is a raw TCP probe, so it
saw 2001 "reachable" and did not skip — but nothing CARLA-RPC-shaped is actually
listening there, so the real client connection timed out. Expected: SKIP (no
real shadow CARLA present); observed: ERROR.

### Tailscale (unplanned finding)
This machine already has a live Tailscale interface, and `tailscale status`
shows a device that looks like your laptop as **idle** (reachable), not
offline — i.e. both ends may already be on the same tailnet. The task
briefing assumed Tailscale "isn't set up yet" — it looks like it already is,
at least on this side. Worth trying the Tailscale IP for the cross-machine run
before setting anything else up (run `tailscale status` locally on each
machine to get the actual addresses — not repeated here since this file is
committed to a public repo).

---

## What Worked
- Full pull → doctor → CARLA + map verification → pytest → lab/client worker
  loop, end to end, against real CARLA (previously only verified in STUB mode).
- `connectivity`, `world_state`, `sustained_stream`, `reconnect` all PASS with
  no code changes needed in `server.py`/`client.py`/`wire.py`.
- The new map-mismatch guard in `doctor.py` / the lab worker works as designed
  (verified PASS state with matching map; would have refused to reach
  `lab_ready` on a mismatch, per the docs).
- `reconnect` genuinely exercised a server restart (`mode: server_restart`),
  not the degraded `client_initiated` fallback — `--keep-alive` without
  `--no-manage-server` is the right default for full-fidelity testing.

## What Broke / What Needs Fixing
- `mirror` scenario / `doctor.check_shadow_carla`: TCP-reachability alone can't
  tell a real second CARLA apart from the primary CARLA's own streaming port.
  Suggest either (a) probing with an actual `carla.Client(...).get_world()` and
  short timeout in the doctor check (same as `carla_map` already does), so a
  non-RPC listener on that port is correctly SKIPped rather than erroring, or
  (b) picking a default `SHADOW_CARLA_PORT` that can't collide with a lone
  CARLA's own port range (e.g. 3001+), or at minimum documenting the collision.
- `tests/test_orch_doctor.py::test_check_is_skipped_not_failed_when_map_cannot_be_read`
  needs to mock CARLA's absence/unreachability rather than assume it ambiently —
  it fails specifically in the venv/environment this task asked to test in.
- The "69 passed" expectation in the task docs doesn't match actual collection
  (88 tests total here) — not chased further, but worth reconciling wherever
  that number is documented.

---

## Request to Laptop Claude
- Pull latest (`5e86ea4`) and run `python -m orchestration doctor --role client`.
- For the cross-machine run, try `LAB_HOST=<this machine's Tailscale IP>`
  (check `tailscale status`/`tailscale ip` on each machine) ahead of the
  campus WAN IP — see the Tailscale note above; campus wifi is known to
  silently drop inbound TCP to the WAN address.
- If you can fix the `shadow_carla` doctor check (see "What Broke" above), that
  would let `mirror` SKIP correctly here instead of ERRORing when no real
  shadow CARLA is present.
- Consider hardening the failing pytest test to mock CARLA's absence instead of
  assuming it.
- No `server.py`/`client.py`/`wire.py` changes needed based on this run —
  connectivity, world_state, sustained_stream, and reconnect all behaved
  correctly against real CARLA and a real server restart.

**Lab worker is still up** (`--keep-alive`, coordinator on :8770, data server on
:8765, CARLA + map loaded) — no need to restart anything lab-side to attempt
the cross-machine run.
