# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working conventions

These bias toward caution over speed. For trivial tasks, use judgment.

1. **Think before coding.** Don't assume — state assumptions explicitly, or ask if uncertain. If multiple interpretations exist, present them rather than picking silently. If a simpler approach exists, say so. If something is unclear, stop and ask rather than guessing.
2. **Simplicity first.** Minimum code that solves the problem — nothing speculative. No features beyond what was asked, no abstractions for single-use code, no unrequested flexibility/configurability, no error handling for impossible scenarios. If it could be a quarter the length, rewrite it.
3. **Surgical changes.** Touch only what the task requires. Don't "improve" adjacent code, comments, or formatting; don't refactor working code; match existing style even if you'd do it differently. If you notice unrelated dead code, mention it rather than deleting it. Do remove imports/variables/functions that your own change made unused, but leave pre-existing dead code alone. Every changed line should trace directly to the request.
4. **Goal-driven execution.** Turn vague tasks into verifiable goals ("fix the bug" → reproduce with a test, then make it pass) and state a brief plan with a verification step per item before multi-step work.

## Repository layout

This repo has a doubly nested `carla-data-server/carla-data-server/` structure — the inner directory is the actual project root. All commands below assume you `cd carla-data-server/carla-data-server` first.

Two virtualenvs live alongside the source:

- `venv/` — full environment with the CARLA 0.9.16 PythonAPI installed. Use this on machines that talk to a real CARLA simulator.
- `venv-stub/` — minimal environment (just `websockets`). Use this for pure server-development work; `server.py` auto-detects the missing `carla` module and drops into STUB mode (synthetic vehicles/pedestrians/traffic lights) so the whole pipeline is exercisable without CARLA.

There is no CI. `tests/` holds a small `pytest` suite (`pip install pytest`, then `pytest tests/` from `carla-data-server/carla-data-server/`) covering pure logic that needs no live server or CARLA: `client/wire.py`, `SensorBuffer`'s dirty-tracking, `BroadcastThread._filter`'s per-client isolation, `_enqueue_drop_oldest`'s backpressure semantics, and a static scan (`test_protocol_conformance.py`) that fails if any file hardcodes a message-type/command string literal instead of importing the matching `wire.MSG_*`/`wire.CMD_*` constant. Everything else (live WebSocket behavior, CARLA-dependent paths, multi-client/multi-machine scenarios) is still tested manually by starting the server and connecting a client.

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


## Architecture

The server is a hybrid threaded + asyncio process. Understanding which thread owns which piece of state is the main thing to keep in mind when editing `server/server.py`:

- **TickLoopThread** (thread) — the only writer to CARLA. Each tick it drains `command_queue`, calls `world.tick()`, snapshots actor state, and pushes the snapshot onto `broadcast_queue`. All CARLA PythonAPI calls happen here or on sensor callbacks; `CarlaConnection._lock` serializes them.
- **BroadcastThread** (thread) — pops snapshots off `broadcast_queue`, filters per-client based on `session.subscriptions` and `ego_actor_id` (setting `is_ego`), caches the JSON encoding per unique (subs, ego) key, and dispatches into each session's `send_queue` via `asyncio.run_coroutine_threadsafe`.
- **asyncio loop** (main thread) — `websockets.serve` per-connection handler, plus `janitor` (evicts clients whose `last_seen` exceeds `silence_timeout`) and `peer_event_fanout` (broadcasts `client_left` events).
- **SensorBuffer** (shared) — CARLA sensor `listen()` callbacks fire on background threads; they write into `SensorBuffer._latest`, which the tick loop drains into the snapshot. Camera frames are JPEG-encoded (BGRA→BGR→RGB via PIL) at quality 60 before hitting the buffer; the broadcast filter base64-encodes them for JSON transport. `drain()` returns only sensors with a frame newer than the last drain (dirty-tracked) — a tick with no fresh camera frame sends no stale JPEG at all.

`--traffic-rate-divisor N` (default 1, unchanged behavior) refreshes `pedestrians`/`traffic_lights` only every Nth tick in `TickLoopThread`, reusing the last computed lists on skipped ticks; `vehicles` always refreshes every tick, since a viewing client's own ego lives in that list too. See `docs/wire-protocol.md` for the tradeoffs.

Cross-thread queues (`queue.Queue` for command/broadcast, `asyncio.Queue` for send/peer-event) are the ONLY communication path. Every `asyncio.Queue` is bounded and drops-oldest on overflow (`_enqueue_drop_oldest`) — do not remove this behavior; a slow client must not stall the tick loop.

## Wire protocol

All frames are JSON objects with a `"type"` field over WebSocket. JSON is the only wire format. `proto/world_state.proto` is kept as a schema sketch for a possible binary format later; nothing reads it, and there is no encoder — the `serializer.py` swap-in module and its generated bindings were removed as dead code. If you revive protobuf, regenerate with `protoc --python_out=server proto/world_state.proto` and write the encoder then.

`client/wire.py` is the single source of truth for message-type strings, `VALID_TOPICS`/`VALID_COMMANDS`, and shared helpers (`parse_frame`, `validate_message`, `make_subscribe`, `make_ping`) — `server.py`, `client.py`, and every raw-`websockets` bridge/script import from it rather than hardcoding literals. Full contract (message shapes, the `tick`/`timestamp`/`wall_time` interpolation contract, rate-tiering, consumer/publisher rules): `docs/wire-protocol.md`. Per-process role table (who subscribes to what, who sends commands, at what rate): `docs/telemetry-roles.md`.

Server → client message types: `welcome`, `world_state`, `ack`, `client_left`.
Client → server command types: `ego_control`, `spawn`, `destroy`, `subscribe`, `list_spawn_points`, `ping`, `spawn_sensor`.

`list_spawn_points` and `ping` piggy-back their response payload as a JSON string inside the `ack.message` field — this is intentional; the ack channel is the reply channel for RPC-shaped commands.

Heartbeat is app-level, not WebSocket-level: the client pings every `PING_INTERVAL` (2s), any inbound frame bumps `session.last_seen`, and the janitor evicts sessions silent longer than `silence_timeout` (default 5s). Eviction destroys the client's ego actor and broadcasts `client_left` with `owned_actor_ids` so peers can react.

## Client extension model

`client/client.py` exposes `CARLAClient` as the base. Subclasses override `on_world_state`, `on_ack`, `on_connected`, `on_peer_left`. `ManualControlClient` shows the state-machine pattern (LISTING → SPAWNING → DRIVING → DONE) driven by ack callbacks. Bridges and scripts import `CARLAClient` via a `sys.path.insert(0, ".../client")` hack — preserve that when adding new bridges (they are not on any Python path otherwise).

Ping acks are consumed internally to update `_rtt_buffer` and are NOT forwarded to `on_ack`, so subclasses don't need to filter them. If you add another RPC-style command whose ack subclasses shouldn't see, follow the same pattern (see `ack_is_ping`).
