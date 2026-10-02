# Testing a shared simulator with mixed clients

The target: one central CARLA, one data server, and several independently
controlled clients of different kinds — Autoware, UB-MR, or a manually driven
client in CARLA. This document is the test matrix for that, and the reasoning
that decides which cells exist.

See `docs/two-machine-testing.md` for how to run scenarios, and
`docs/wire-protocol.md` for the clock-ownership rule the matrix turns on.

## Two ways a client attaches

This is the distinction the matrix is built on, because the two paths exercise
almost disjoint code.

| | Through the data server | Direct to CARLA |
| --- | --- | --- |
| Who | `interactive_driver.py`, `client.py --role manual`, any `CARLAClient` | **Autoware**, **CARLA's `manual_control.py`** |
| Gets its car by | `spawn` command over WebSocket | its own PythonAPI connection |
| Server knows it as | a `ClientSession` + an entry in `_actor_cache` | an actor in `world.get_actors()`, nothing more |
| `is_ego` | true for the owning client | false for everyone — nobody here owns it |
| Identified by | `is_ego`, per viewer | `role_name` only |
| On disconnect | janitor evicts, destroys the ego, broadcasts `client_left` | nothing; its owner destroys it, or it stays |

Every actor-level scenario except `external_participant` covers the left-hand
column only. Autoware and `manual_control.py` — the two clients named in the
requirement — are both the right-hand column.

## Clock ownership decides the server's mode

In synchronous mode exactly one process may call `world.tick()`. So the choice
is forced, not optional:

- **No Autoware** → the data server owns the clock. Run it normally.
- **Autoware present** → Autoware's bridge owns the clock. The data server
  **must** run `--observe`, or the world double-steps for everyone.

A second ticker is the single most destructive mistake available here, and it
does not announce itself: the simulation simply runs at twice speed. Scenarios
must never call `world.tick()` — `external_participant` spawns, moves and
destroys actors without ticking, deliberately.

## Where UB-MR sits

UB-MR is a **consumer**, not a driver. In `UB-MR/Assets/UB_MR/Scripts/` the only
network object is `new UdpClient(listenPort)` in `TrafficReceiver.cs`; there is
no `.Send(` anywhere under that tree, and `Redis_Networking/` contains only a
receiver and a renderer. Its outbound traffic is ROS2 — virtual object
detections and camera images from `AutonomousVehicle.cs` — which feeds
*Autoware's perception*, not vehicle control.

So UB-MR influences the shared world indirectly, through Autoware, and never
drives a CARLA actor itself:

    UB-MR --ROS2 virtual objects--> Autoware --control--> CARLA
    UB-MR <--UDP world_state-- ws_to_udp_bridge <--WebSocket-- data server

`ws_to_udp_bridge.py` is correspondingly send-only. That matches the
architecture; it is not a gap. If UB-MR is ever meant to drive a car directly,
it needs a UDP return path into `ego_control`, which does not exist today.

## The matrix

| # | Clock owner | Participants | Server mode | Covered by |
| --- | --- | --- | --- | --- |
| 1 | data server | 2 × WebSocket clients | default | `multi_client` |
| 2 | data server | WebSocket client + `manual_control.py` | default | `external_participant` |
| 3 | external ticker | 2 × WebSocket clients | `--observe` | manual (see below) |
| 4 | Autoware | Autoware ego + WebSocket client | `--observe` | `external_participant` |
| 5 | Autoware | Autoware + `manual_control.py` + UB-MR + WebSocket client | `--observe` | all of the above together |

Rows 2 and 4 are the same scenario under different clock owners, which is the
point: `external_participant` does not care who owns the clock, so running it
in both modes is what separates "the foreign-actor path works" from "the
foreign-actor path works while observing".

## Running it

Row 1 and row 2, data server owning the clock:

```
python -m orchestration lab --suite multi_client,external_participant --on-failure continue
```

Row 3, an external ticker instead of Autoware — prove `--observe` against
something trivial before adding an autonomy stack, so a failure is
unambiguous. `scripts/` has no ticker; write one that sets
`synchronous_mode = True`, `fixed_delta_seconds = 1/20` and loops on
`world.tick()`. Then start the server with `--observe` and run:

```
python -m orchestration lab --suite connectivity,world_state,sustained_stream
```

Checks that only matter in this row, and that no scenario asserts yet:

- `tick` increments by exactly 1, with no repeats
- `timestamp` does **not** start at zero, and advances with the ticker
- at **half** the server's `--tick-rate`, the feed downsamples rather than
  emitting duplicate snapshots
- killing the ticker makes the feed go **silent**, with
  `world still on frame N after M cycles` in the server log — a stream of
  identical states instead is a regression
- with `--traffic-rate-divisor 2`, `pedestrians` is not permanently empty

Rows 4 and 5, Autoware owning the clock:

```
python -m orchestration lab --suite external_participant,multi_client,peer_departure --on-failure continue
```

## What `external_participant` asserts

1. a car spawned outside the server reaches `world_state` at all
2. its `role_name` survives the server untouched
3. it is `is_ego: false` for every client — nobody here owns it
4. our own server-spawned ego is still `is_ego: true` in the **same frame**
5. its reported position matches the simulator within tolerance
6. the feed follows it when its own owner moves it
7. it leaves `world_state` when its owner destroys it

It also records `blueprint_role_name_default` as a metric — the value CARLA
gives a vehicle blueprint when nobody sets one. That is compiled into the
simulator and cannot be read off the Python package, and it decides whether
`role_name` can distinguish a participant from traffic at all.

## Known asymmetry

Autoware tags its car `ego_vehicle` and `manual_control.py` tags its own
`hero`, so foreign participants identify themselves. This server does **not**
set `role_name` on cars it spawns, so in row 5 a viewer can pick out Autoware's
car but not the one our own driver is in. Resolving that means choosing a tag
for server-spawned egos — and the value matters, because `hero` is on the skip
list of UB-DigitalTwin's traffic renderer (see `bridges/ws_to_redis_bridge.py`).
Decide it once `blueprint_role_name_default` is known.

## Expect more jitter under `--observe`

Two independent clocks at the same nominal rate drift in phase, so cycles
alternate between "no new frame" and "two new frames". The long-run rate holds,
but `sustained_stream`'s `jitter_ms_stdev` will read roughly double what it
does in owner mode. Judge the publish rate, not the jitter, before calling that
a fault.
