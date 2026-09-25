# CARLA data server wire protocol

The contract between `server/server.py` and every consumer: `client/client.py`,
`bridges/`, and `scripts/`. The registry constants below are defined once in
`client/wire.py` — import from there instead of hardcoding type strings.

## Overview

One WebSocket connection per client. Every frame, in both directions, is a
single JSON object with a `"type"` field. There is no separate framing layer;
`websockets` handles message boundaries.

## Message type registry

### Server → client

| Type | Constant | Required fields | Meaning |
| --- | --- | --- | --- |
| `welcome` | `wire.MSG_WELCOME` | `client_id` | Sent once, right after connect. Also carries `valid_topics`, `valid_commands`, `silence_timeout`. |
| `world_state` | `wire.MSG_WORLD_STATE` | `tick`, `timestamp`, `wall_time` | One combined snapshot per tick: `vehicles`, `pedestrians`, `traffic_lights`, `sensors`, filtered by the client's `subscribe`d topics. |
| `ack` | `wire.MSG_ACK` | `command`, `status` | Reply to any client→server command. `list_spawn_points` and `ping` piggyback their response as a JSON string inside `message`. |
| `client_left` | `wire.MSG_CLIENT_LEFT` | `client_id` | Broadcast to remaining clients when a peer disconnects or is evicted. Carries `owned_actor_ids`. |

### Client → server

| Command | Constant |
| --- | --- |
| `ego_control` | `wire.CMD_EGO_CONTROL` |
| `spawn` | `wire.CMD_SPAWN` |
| `destroy` | `wire.CMD_DESTROY` |
| `subscribe` | `wire.CMD_SUBSCRIBE` |
| `list_spawn_points` | `wire.CMD_LIST_SPAWN_POINTS` |
| `ping` | `wire.CMD_PING` |
| `spawn_sensor` | `wire.CMD_SPAWN_SENSOR` |

Valid subscription topics: `wire.VALID_TOPICS` = `vehicles`, `pedestrians`,
`traffic_lights`, `sensors`.

## Simulation-time contract

`world_state` carries three independent time fields. Using the wrong one for
the wrong purpose has already caused a real bug (see below) — pick correctly:

- **`tick`** — a monotonic integer, incremented once per server tick, never
  resets while the server runs. Use it to detect gaps, reordering, or measure
  "how many ticks since I last saw this."
- **`timestamp`** — accumulated simulation time in seconds (`tick *
  tick_interval`). This is the correct basis for time-based interpolation or
  extrapolation between snapshots, because it advances in lockstep with the
  simulation regardless of network jitter or when a message actually arrives.
### Vehicle identity

Each vehicle carries `role_name`, CARLA's own tag for who spawned it and why:
`ego_vehicle` for a car driven by an external autonomy stack, `hero` for a
manually driven one, empty for background traffic. Consumers that must
distinguish another participant's car from ordinary traffic key on this rather
than guessing from the blueprint. `is_ego` is a different thing: it is computed
per viewing client and marks the car *that client* owns.

- **`wall_time`** — `time.time()` on the server host at the moment the
  snapshot was built. Use this **only** for network latency measurement
  (`local_now - wall_time`), as `scripts/metrics_client.py` and
  `scripts/interactive_driver.py` already do. Never use it for interpolation:
  it's subject to clock skew between machines and has no relationship to
  simulation stepping.

**Known past bug, now fixed:** `bridges/ws_to_udp_bridge.py` used to forward
`wall_time` to Unity labeled `"timestamp"`, which the receiving code used to
interpolate/extrapolate between lossy, unordered UDP packets. That's wrong for
exactly the reason above. The bridge now also sends `tick` and `sim_time`
(`= world_state.timestamp`) alongside the original `timestamp` field (kept for
compatibility) — downstream code should migrate to `sim_time`/`tick`.

## Clock ownership

In synchronous mode exactly one process may advance the world. By default this
server does: it sets `synchronous_mode`, `fixed_delta_seconds`, and calls
`world.tick()` once per cycle.

Start it with `--observe` when something else owns the clock — an Autoware
bridge, or a dedicated time master. The server then leaves the world settings
untouched, never ticks, and reads `tick` and `timestamp` from the simulator's
own snapshot instead of counting its own cycles. Controls are still applied,
because setting an actor's control does not advance the world.

Two processes advancing one synchronous world double-step it, so the choice is
not optional when sharing a simulator.

## Publish-rate contract

- `world_state` is broadcast once per server tick, at `--tick-rate` (default
  20 Hz). Every subscribed topic is included in the same message — there is
  no per-topic broadcast rate for `vehicles`.
- `--traffic-rate-divisor N` (default `1`, i.e. unchanged behavior) refreshes
  `pedestrians`/`traffic_lights` only every Nth tick, reusing the last
  computed list on skipped ticks. `vehicles` always refreshes every tick
  regardless of this flag, because a viewing client's own ego vehicle always
  lives in the `vehicles` list (see "Why not vehicles?" below).
  - This reduces CARLA-side per-tick query cost when there are many
    pedestrians. It does **not** by itself reduce wire bytes/sec — the same
    (repeated) values are still serialized and sent every tick. A real
    bandwidth reduction would need WebSocket permessage-deflate or a
    "changed" side-channel; neither exists today.
  - Known consequence: `bridges/carla_mirror_client.py` mirrors pedestrian/
    light state 1:1, so with a divisor > 1 its mirrored actors visibly step
    instead of moving smoothly. This is expected, not a bug, when the flag
    is enabled.
- Sensor frames are only included in `world_state.sensors` for sensors that
  produced a new frame since the last tick (`SensorBuffer` tracks dirtiness
  and drains-and-clears). A tick with no fresh camera frame sends `"sensors":
  []` rather than re-shipping the last known JPEG.

### Why not vehicles?

`is_ego` is computed per **viewing client**, not globally — a vehicle that's
background traffic to client A may be client B's own ego
(`BroadcastThread._filter`). There is no way to identify "vehicles nobody
cares about" at the snapshot layer without risking some client's control
feedback going stale. Pedestrians and traffic lights have no such ambiguity,
so they're the only safe candidate for rate tiering.

## Rules for consumers

- **Check `type` before reading anything else.** Different message types have
  different payload shapes.
- **Ignore unknown types silently.** Never log an error or drop the
  connection over a type you don't handle — this is what lets the protocol
  grow without a synchronized upgrade of every consumer.
  `client/client.py`'s dispatch has no `else` branch for exactly this reason.
- **Validate required fields before use.** Call `wire.validate_message(msg)`
  (checks `wire.REQUIRED_FIELDS` for known types; unknown types always pass).
- **Never let one bad message kill your loop.** Every handler in
  `client/client.py` is wrapped in try/except for this reason — new consumers
  should do the same.
- **Don't republish unchanged data.** `SensorBuffer` already does this for
  sensor frames; a hand-rolled consumer relaying data onward (like
  `ws_to_udp_bridge.py`) should apply the same principle if it starts
  buffering/forwarding on its own schedule.

## Rules for publishers

- Publish at a fixed, declared rate — the server's tick loop does this by
  construction.
- Add fields freely; consumers must ignore fields they don't recognize.
  Never repurpose or remove an existing field's meaning — that's a silent
  breakage for anyone who hasn't upgraded.

## Versioning

There is no `type`-registry version field today. The additive-only field
policy above is the de facto versioning strategy: as long as changes only add
fields (never repurpose/remove them), no consumer needs to know a version
number to stay compatible. Add a `proto_version` field only if an unavoidable
breaking change is ever required — none of the changes in this document
needed one.
