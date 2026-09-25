# Consumer roles

Every process below connects to `server/server.py` as an ordinary WebSocket
client (via `client.CARLAClient` or raw `websockets`) — there is no separate
broker or pub/sub layer in this project; the server itself is the single
writer, and each of these is an independent reader/writer process. This is
the direct equivalent of a Redis-based architecture's per-role publisher/
subscriber processes, minus the extra broker hop.

See `docs/wire-protocol.md` for the message contract these roles share.

| Process | Base | Subscribes to | Sends commands | Rate | Purpose |
| --- | --- | --- | --- | --- | --- |
| `client/client.py --role spectator` (`SpectatorClient`) | `CARLAClient` | `vehicles`, `pedestrians`, `traffic_lights` | none (plus internal `ping`) | tick-rate | Read-only display/logging. |
| `client/client.py --role manual` (`ManualControlClient`) | `CARLAClient` | `vehicles`, `pedestrians`, `traffic_lights` | `list_spawn_points`, `spawn`, `ego_control`, `destroy` | tick-rate | Worked example: scripted LISTING→SPAWNING→DRIVING→DONE drive sequence. |
| `client/client.py --role mr_agent` (`MRAgentClient`) | `CARLAClient` | `vehicles`, `pedestrians`, `traffic_lights`, `sensors` | none (plus internal `ping`) | tick-rate | Forwards each `world_state` to an external callback (feeds a Unity/MR consumer not in this repo). |
| `scripts/interactive_driver.py` | `CARLAClient` | `vehicles`, `sensors` | `list_spawn_points`, `spawn`, `spawn_sensor`, `ego_control`, `destroy` | tick-rate | Full read+write pygame driver with live camera feed; the only role that both drives and renders. |
| `bridges/carla_mirror_client.py` | `CARLAClient` | `vehicles`, `pedestrians`, `traffic_lights` | none | tick-rate | Mirrors every actor into a second, passive CARLA instance ("CARLA B") via its own PythonAPI connection. |
| `bridges/ws_to_udp_bridge.py` | raw `websockets` | `vehicles` | `subscribe`, `ping` only | tick-rate in, fire-and-forget UDP out | Reshapes `world_state` into Unity's `TrafficReceiver` UDP JSON format for the UB-MR app. Lossy by design — drops pedestrians/traffic_lights/sensors. |
| `scripts/metrics_client.py` | raw `websockets` | `vehicles`, `pedestrians`, `traffic_lights`, `sensors` | `subscribe`, `ping` only | tick-rate | Read-only: measures latency, jitter, message size, bandwidth, actor counts. Reports once per second. |

## Interop: running UB-DigitalTwin's Redis clients against this server

`bridges/ws_to_redis_bridge.py` republishes `world_state` onto the Redis
channel UB-DigitalTwin's clients already subscribe to, in the envelope from
their `docs/telemetry-protocol.md`. That lets their roles
(`multi_traffic_renderer`, `multi_agent_renderer`, and the rest) run
**unmodified** with this server in place of their Redis hub, instead of
forking each one onto `CARLAClient`.

| | |
| --- | --- |
| Subscribes to | `vehicles` |
| Publishes | type 2 `traffic` at a fixed `--publish-hz`; type 1 `destroy` on shutdown |
| Does not publish | type 0 (`telemetry`) - this is a relay, not a participant, and every car is already in type 2; type 3 (`ego`) - `ws_to_udp_bridge.py` covers the Unity direction |
| Needs | `pip install redis` (only this bridge does) |

Translation gaps, because our wire protocol has no equivalent:

- **`role_name` is passed through** from `world_state`, so their renderer's
  `hero`/`external_ego` filtering sees whatever tag the spawning process set.
  Cars spawned through *this* server's `spawn` command carry the blueprint
  default, not `hero` — see "Vehicle identity" in `docs/wire-protocol.md`.
- **`color` is not in `world_state`**, so one `--color` applies to all.

`server_timestamp` carries our simulation clock, not wall clock, which is what
their interpolator expects and what survives clock skew between machines.

## Why two of these are raw `websockets` instead of `CARLAClient`

`ws_to_udp_bridge.py` and `metrics_client.py` only ever consume `world_state`
and send `subscribe`/`ping` — they never issue `spawn`/`ego_control`/etc., so
they don't need `CARLAClient`'s full command surface or reconnect/state-
machine scaffolding. Both still import `client/wire.py` for the shared
message-type constants and `make_subscribe`/`make_ping`/`parse_frame`/
`validate_message` helpers, so they can't drift from the protocol the way
hand-rolled duplicate constants would.

## Adding a new role

Prefer subclassing `CARLAClient` (see `client/client.py`) unless your
consumer genuinely never sends commands — in that case, raw `websockets` is
fine, but import `client/wire.py` for the type registry rather than
hardcoding string literals, and add a row to the table above.
