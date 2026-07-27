"""
WebSocket-to-UDP Bridge for UB-MR
=================================

What it is
----------
The last hop between the CARLA Data Server and the UB-MR Unity app, which
speaks UDP JSON and knows nothing of the server's WebSocket protocol. Unlike
the mirror bridge it does not build on CARLAClient - it uses raw websockets,
because it only consumes world_state and never sends commands.

What it does
------------
Subscribes, receives world_state at 20 Hz, reshapes each into the
TrafficReceiver format, sends it over UDP. TrafficReceiver.cs listens on port
12345 and spawns or moves vehicle GameObjects from what arrives.

How it works
------------
The conversion is lossy by design: Unity needs only id, blueprint, colour,
location and yaw, so pedestrians, traffic lights and sensor frames are dropped
rather than serialised and ignored downstream. UDP is fire-and-forget - at
20 Hz a lost frame is replaced ~50 ms later, so retransmission would cost more
latency than the loss. The WebSocket side reconnects on failure; the UDP socket
is connectionless and needs none.

Usage:
    python3 bridges/ws_to_udp_bridge.py --server ws://128.205.222.211:8765 --udp-host localhost --udp-port 12345
"""

import argparse
import asyncio
import json
import logging
import socket
import time

import websockets
import websockets.exceptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("ws-udp-bridge")


class WStoUDPBridge:
    def __init__(self, server_url, udp_host, udp_port):
        self.server_url = server_url
        self.udp_host = udp_host
        self.udp_port = udp_port
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.msg_count = 0
        self.last_log = 0.0

    def convert_world_state(self, state):
        """Reshape a server world_state into the Unity TrafficReceiver format."""
        vehicles = []
        for v in state.get("vehicles", []):
            transform = v.get("transform", {})
            location = transform.get("location", {})
            rotation = transform.get("rotation", {})
            vehicles.append({
                "id": str(v["id"]),
                "blueprint": v.get("type_id", "vehicle.unknown"),
                "color": "255,255,255",
                "location": {
                    "x": location.get("x", 0),
                    "y": location.get("y", 0),
                    "z": location.get("z", 0),
                },
                "yaw": rotation.get("yaw", 0),
            })
        return {
            "vehicles": vehicles,
            "timestamp": state.get("wall_time", time.time()),
        }

    def send_udp(self, payload):
        """Send JSON payload over UDP to the Unity app."""
        data = json.dumps(payload).encode("utf-8")
        self.udp_socket.sendto(data, (self.udp_host, self.udp_port))

    async def run(self):
        while True:
            try:
                async with websockets.connect(
                    self.server_url,
                    max_size=10 * 1024 * 1024,
                    ping_interval=10,
                    ping_timeout=20,
                ) as ws:
                    # Read welcome
                    raw = await ws.recv()
                    welcome = json.loads(raw)
                    client_id = welcome.get("client_id", "unknown")
                    log.info("Connected to data server as %s", client_id)

                    # Subscribe to vehicles
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "payload": {"topics": ["vehicles"]}
                    }))

                    # Keep-alive ping
                    async def keep_alive():
                        while True:
                            await asyncio.sleep(3)
                            try:
                                await ws.send(json.dumps({
                                    "type": "ping",
                                    "payload": {}
                                }))
                            except Exception:
                                return

                    ping_task = asyncio.create_task(keep_alive())

                    try:
                        async for raw in ws:
                            try:
                                msg = json.loads(raw)
                            except json.JSONDecodeError:
                                continue

                            if msg.get("type") != "world_state":
                                continue

                            # Convert and send over UDP
                            payload = self.convert_world_state(msg)
                            self.send_udp(payload)
                            self.msg_count += 1

                            # Log once per second
                            now = time.monotonic()
                            if now - self.last_log >= 1.0:
                                n_vehicles = len(payload["vehicles"])
                                log.info("tick=%d vehicles=%d udp_msgs_sent=%d",
                                         msg.get("tick", 0), n_vehicles, self.msg_count)
                                self.last_log = now
                    finally:
                        ping_task.cancel()
                        await asyncio.gather(ping_task, return_exceptions=True)

            except (websockets.exceptions.ConnectionClosed,
                    ConnectionRefusedError, OSError,
                    asyncio.TimeoutError) as e:
                log.warning("Connection lost: %s - reconnecting in 3s", e)
                await asyncio.sleep(3)
            except KeyboardInterrupt:
                break

        self.udp_socket.close()
        log.info("Bridge stopped. Total UDP messages sent: %d", self.msg_count)


def main():
    parser = argparse.ArgumentParser(description="WebSocket-to-UDP Bridge for UB-MR")
    parser.add_argument("--server", default="ws://128.205.222.211:8765",
                        help="WebSocket URL of the CARLA Data Server")
    parser.add_argument("--udp-host", default="localhost",
                        help="Host where UB-MR Unity app is listening")
    parser.add_argument("--udp-port", type=int, default=12345,
                        help="UDP port the Unity app listens on")
    args = parser.parse_args()

    log.info("Bridge: %s → UDP %s:%d", args.server, args.udp_host, args.udp_port)

    bridge = WStoUDPBridge(args.server, args.udp_host, args.udp_port)
    try:
        asyncio.run(bridge.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
