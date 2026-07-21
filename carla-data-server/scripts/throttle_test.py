"""
Throttle test v2: properly drains the WebSocket buffer so speed
readings reflect the CURRENT state, not stale buffered messages.
"""
import asyncio
import json
import time
import websockets

SERVER = "ws://128.205.222.211:8765"
SPAWN_INDEX = 5  # Try a different spawn point


async def test():
    ws = await websockets.connect(SERVER, max_size=10 * 1024 * 1024)
    welcome = json.loads(await ws.recv())
    print(f"Connected: {welcome['client_id']}")

    # Subscribe
    await ws.send(json.dumps({
        "type": "subscribe",
        "payload": {"topics": ["vehicles"]}
    }))

    # List spawn points
    await ws.send(json.dumps({
        "type": "list_spawn_points",
        "payload": {}
    }))

    # Drain until we get the list_spawn_points ack
    while True:
        msg = json.loads(await ws.recv())
        if msg.get("type") == "ack" and msg.get("command") == "list_spawn_points":
            break

    # Spawn
    await ws.send(json.dumps({
        "type": "spawn",
        "payload": {
            "blueprint_id": "vehicle.tesla.model3",
            "spawn_point_index": SPAWN_INDEX,
            "autopilot": False
        }
    }))

    # Drain until spawn ack
    actor_id = 0
    while True:
        msg = json.loads(await ws.recv())
        if msg.get("type") == "ack" and msg.get("command") == "spawn":
            actor_id = msg.get("actor_id", 0)
            print(f"Spawned: actor_id={actor_id}")
            break

    # Helper: send a control and get the LATEST speed
    # Drains all pending messages to avoid reading stale data
    async def send_and_get_speed(throttle, brake, hand_brake=False):
        await ws.send(json.dumps({
            "type": "ego_control",
            "payload": {
                "throttle": throttle,
                "steer": 0,
                "brake": brake,
                "hand_brake": hand_brake,
                "reverse": False,
            }
        }))
        # Wait for next tick
        await asyncio.sleep(0.06)
        # Drain all pending messages, keep the last world_state
        latest_speed = None
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=0.01)
                msg = json.loads(raw)
                if msg.get("type") == "world_state":
                    for v in msg.get("vehicles", []):
                        if v.get("is_ego"):
                            vel = v["velocity"]
                            latest_speed = (vel["x"]**2 + vel["y"]**2 + vel["z"]**2)**0.5 * 3.6
            except asyncio.TimeoutError:
                break
        return latest_speed if latest_speed is not None else -1

    # Phase 1: Throttle ON for 3 seconds
    print("\n=== THROTTLE ON (3s) ===")
    for i in range(30):
        speed = await send_and_get_speed(throttle=1.0, brake=0.0)
        if i % 5 == 0:
            print(f"  [{i*100}ms] speed = {speed:.1f} km/h  (throttle=1.0)")

    # Phase 2: Throttle OFF for 5 seconds
    print("\n=== THROTTLE OFF (5s) ===")
    for i in range(50):
        speed = await send_and_get_speed(throttle=0.0, brake=0.0)
        if i % 5 == 0:
            print(f"  [{i*100}ms] speed = {speed:.1f} km/h  (throttle=0.0)")

    # Phase 3: Full brake + handbrake for 3 seconds
    print("\n=== BRAKE + HANDBRAKE (3s) ===")
    for i in range(30):
        speed = await send_and_get_speed(throttle=0.0, brake=1.0, hand_brake=True)
        if i % 5 == 0:
            print(f"  [{i*100}ms] speed = {speed:.1f} km/h  (brake=1.0 + handbrake)")

    # Cleanup
    await ws.send(json.dumps({
        "type": "destroy",
        "payload": {"actor_id": actor_id}
    }))
    print("\nDone. Destroyed ego.")
    await ws.close()

asyncio.run(test())
