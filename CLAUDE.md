# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

This repo has a doubly nested `carla-data-server/carla-data-server/` structure — the inner directory is the actual project root. All commands below assume you `cd carla-data-server/carla-data-server` first.

Two virtualenvs live alongside the source:

- `venv/` — full environment with the CARLA 0.9.16 PythonAPI installed. Use this on machines that talk to a real CARLA simulator.
- `venv-stub/` — minimal environment (just `websockets`). Use this for pure server-development work; `server.py` auto-detects the missing `carla` module and drops into STUB mode (synthetic vehicles/pedestrians/traffic lights) so the whole pipeline is exercisable without CARLA.

There is no git repo, no test suite, no CI. Runtime testing is done by starting the server and connecting a client.

## Common commands

Server:
```
python server/server.py --host 0.0.0.0 --port 8765 \
                       --carla-host localhost --carla-port 2000 \
                       --tick-rate 20 --silence-timeout 5
```

Base client (spectator / manual / mr_agent roles):
```
python client/client.py --server ws://localhost:8765 --role spectator
python client/client.py --server ws://localhost:8765 --role manual --spawn-index 5
```

Interactive pygame driver with live camera:
```
python scripts/interactive_driver.py --server ws://localhost:8765 --spawn-index 5
```

Bridge to a second CARLA instance ("CARLA B") for mirrored rendering:
```
python bridges/carla_mirror_client.py --server ws://localhost:8765 \
                                     --shadow-host localhost --shadow-port 2001
```

Bridge to the UB-MR Unity app over UDP JSON:
```
python bridges/ws_to_udp_bridge.py --server ws://<host>:8765 \
                                   --udp-host localhost --udp-port 12345
```

Regenerate protobuf bindings if `proto/world_state.proto` changes:
```
protoc --python_out=server proto/world_state.proto
```

## Architecture

The server is a hybrid threaded + asyncio process. Understanding which thread owns which piece of state is the main thing to keep in mind when editing `server/server.py`:

- **TickLoopThread** (thread) — the only writer to CARLA. Each tick it drains `command_queue`, calls `world.tick()`, snapshots actor state, and pushes the snapshot onto `broadcast_queue`. All CARLA PythonAPI calls happen here or on sensor callbacks; `CarlaConnection._lock` serializes them.
- **BroadcastThread** (thread) — pops snapshots off `broadcast_queue`, filters per-client based on `session.subscriptions` and `ego_actor_id` (setting `is_ego`), caches the JSON encoding per unique (subs, ego) key, and dispatches into each session's `send_queue` via `asyncio.run_coroutine_threadsafe`.
- **asyncio loop** (main thread) — `websockets.serve` per-connection handler, plus `janitor` (evicts clients whose `last_seen` exceeds `silence_timeout`) and `peer_event_fanout` (broadcasts `client_left` events).
- **SensorBuffer** (shared) — CARLA sensor `listen()` callbacks fire on background threads; they write into `SensorBuffer._latest`, which the tick loop drains into the snapshot. Camera frames are JPEG-encoded (BGRA→BGR→RGB via PIL) at quality 60 before hitting the buffer; the broadcast filter base64-encodes them for JSON transport.

Cross-thread queues (`queue.Queue` for command/broadcast, `asyncio.Queue` for send/peer-event) are the ONLY communication path. Every `asyncio.Queue` is bounded and drops-oldest on overflow (`_enqueue_drop_oldest`) — do not remove this behavior; a slow client must not stall the tick loop.

## Wire protocol

All frames are JSON objects with a `"type"` field over WebSocket. `proto/world_state.proto` and `server/serializer.py` define an equivalent protobuf schema — `serializer.py` is a swap-in encode/decode module, but the running server currently uses JSON exclusively. Keep JSON and proto in sync when adding fields.

Server → client message types: `welcome`, `world_state`, `ack`, `client_left`.
Client → server command types (see `VALID_COMMANDS` in `server.py`): `ego_control`, `spawn`, `destroy`, `subscribe`, `list_spawn_points`, `ping`, `spawn_sensor`.

`list_spawn_points` and `ping` piggy-back their response payload as a JSON string inside the `ack.message` field — this is intentional; the ack channel is the reply channel for RPC-shaped commands.

Heartbeat is app-level, not WebSocket-level: the client pings every `PING_INTERVAL` (2s), any inbound frame bumps `session.last_seen`, and the janitor evicts sessions silent longer than `silence_timeout` (default 5s). Eviction destroys the client's ego actor and broadcasts `client_left` with `owned_actor_ids` so peers can react.

## Client extension model

`client/client.py` exposes `CARLAClient` as the base. Subclasses override `on_world_state`, `on_ack`, `on_connected`, `on_peer_left`. `ManualControlClient` shows the state-machine pattern (LISTING → SPAWNING → DRIVING → DONE) driven by ack callbacks. Bridges and scripts import `CARLAClient` via a `sys.path.insert(0, ".../client")` hack — preserve that when adding new bridges (they are not on any Python path otherwise).

Ping acks are consumed internally to update `_rtt_buffer` and are NOT forwarded to `on_ack`, so subclasses don't need to filter them. If you add another RPC-style command whose ack subclasses shouldn't see, follow the same pattern (see `ack_is_ping`).
