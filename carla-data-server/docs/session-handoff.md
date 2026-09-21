# Session handoff — 2026-09-21

Where things stand, what is proven, and what to pick up next.

## What exists now

Three layers, added over this session:

1. **Wire-protocol hygiene** — `client/wire.py` is the single source of truth
   for message types; `docs/wire-protocol.md` is the contract;
   `tests/test_protocol_conformance.py` fails if any file hardcodes a type
   string again.
2. **Test suite** — `tests/`, 118 tests, no CARLA or network needed:
   `pytest tests/` (needs `pip install pytest`).
3. **Two-machine orchestration** — `orchestration/`: a coordinator owning run
   state, LAB and CLIENT workers, ten scenarios, an agent mailbox, and opt-in
   automated code sync. See `docs/two-machine-testing.md`.

## What is actually proven, and where

| | Proven on |
| --- | --- |
| `connectivity`, `world_state`, `sustained_stream` | STUB, real CARLA, **and cross-machine** (19.98 Hz of 20, max tick gap 1, jitter 0.81 ms, RTT 34 ms over Tailscale) |
| `reconnect` | real CARLA, with a genuine data-server restart |
| `mirror` | real CARLA + a real shadow sim on port 2003 (0 → 6 mirrored actors, maps matched) |
| `peer_departure`, `udp_bridge` | STUB only |
| `ego_control`, `multi_client`, `camera_follow` | **never run against a real simulator** |

Phases 3 (real CARLA) and 4 (two machines) are closed for the original five
scenarios. The five added for UB-DigitalTwin role parity are implemented and
unit-covered but mostly unexercised — that is the honest gap.

## Next session: start here

1. **Run the five new scenarios on the Lab PC.** They are the whole point of
   the role-parity work and four of them have never touched a real simulator:
   ```
   python -m orchestration lab --keep-alive \
       --suite ego_control,multi_client,peer_departure,camera_follow,udp_bridge \
       --on-failure continue
   ```
   `multi_client` is the most valuable: two participants with their own egos
   seeing each other is the capability that makes this server a replacement
   for UB-DigitalTwin's Redis hub, and it has never actually happened.
2. **Cross-machine mirror** — the laptop's `venv/` has CARLA 0.9.16 and works,
   so the laptop can host a shadow sim and mirror the lab's world for real.
3. **Decide on `--auto-sync`.** The lab agent correctly declined to enable it
   unilaterally. Recommendation: add a shared-token check to the coordinator
   first, since it is currently unauthenticated on a shared tailnet.

## Environment notes (these have cost time twice)

- `venv/` on the laptop is **fine**: Python 3.10, CARLA 0.9.16, a compatible
  `websockets`. `venv-stub/` is **broken**: no interpreter (it was copied from
  another machine) and `websockets` 16 removed the legacy API `server.py` uses.
- The interpreter running the lab worker decides STUB vs real, because it
  launches `server/server.py` with `sys.executable`. A CARLA-capable
  interpreter with no simulator listening makes `server.py` exit rather than
  fall back to STUB.
- Reach the Lab PC over **Tailscale**, not the campus IP: campus wifi silently
  drops inbound TCP, Tailscale works for CARLA (2000), the data server (8765)
  and the coordinator (8770).
- A second CARLA must clear the primary's port range: CARLA binds
  `rpc_port`, `+1` and `+2`, so with the primary on 2000 use
  `-carla-rpc-port=2003`. A bare TCP probe of 2001 misleadingly succeeds.

## Open issues

- **Coordinator has no authentication.** Known, documented, and the reason
  `--auto-sync` is off by default.
- **`128.205.222.211` is committed throughout a public repo** —
  `ws_to_udp_bridge.py`, `interactive_driver.py`, `metrics_client.py`,
  `commands.md`, `lab-status.md`. All predates this work; worth one
  deliberate scrub commit rather than piecemeal edits.
- `CLAUDE.md` still shows `--shadow-port 2001` in the mirror example, which
  collides with the primary CARLA's port range (see above).

## What repeatedly caught real bugs

Not the 118 tests. Running the thing on real hardware did:

| Bug | Found by |
| --- | --- |
| `mirror` reporting a missing shadow as ERROR (CARLA port+1 collision) | lab hardware |
| `_format_summary` undefined — crashed every suite completion | lab agent |
| client exiting instead of waiting for the coordinator | follow-up to the above |
| a test that only passed on machines without CARLA | lab hardware |
| client idle timer measuring since last claim, not since idle | local e2e run |
| `reconnect` reporting a fake `0.0s` recovery | local e2e run |

The lesson worth keeping: this laptop is the unusual environment (no CARLA, a
broken venv), so "passes locally" has been a weaker signal than it looks.
