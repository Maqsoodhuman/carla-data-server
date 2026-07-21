"""
Interactive Manual Client with Camera View
===========================================
Connects to the CARLA Data Server, spawns an ego vehicle with a camera,
and displays the camera feed in a pygame window while you drive.

Controls:
    UP / W      = throttle
    DOWN / S    = brake
    LEFT / A    = steer left
    RIGHT / D   = steer right
    SPACE       = hand brake
    R           = toggle reverse
    Q / ESC     = quit and destroy ego

Usage:
    python3 scripts/interactive_driver.py --server ws://localhost:8765
    python3 scripts/interactive_driver.py --server ws://128.205.222.211:8765 --spawn-index 5

Requires: pip install pygame Pillow
"""

import argparse
import asyncio
import base64
import io
import json
import logging
import os
import sys
import threading
import time

import pygame

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "client"))
from client import CARLAClient

import websockets
import websockets.exceptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("driver")

WINDOW_W = 800
WINDOW_H = 600


class InteractiveDriver(CARLAClient):

    def __init__(self, server_url, spawn_index):
        super().__init__(
            server_url,
            subscriptions=["vehicles", "sensors"],
            role="interactive_driver",
        )
        self._spawn_index = spawn_index
        self._state = "INIT"
        self._ego_id = None
        self._camera_id = None
        self._reverse = False
        self._last_telemetry = 0.0
        self._ego_speed = 0.0
        self._ego_x = 0.0
        self._ego_y = 0.0
        self._ego_yaw = 0.0
        self._vehicle_count = 0
        self._frame_lock = threading.Lock()
        self._latest_frame = None
        self._frame_count = 0

        # Metrics (collected in background from on_world_state)
        self._metrics_lock = threading.Lock()
        self._last_recv_mono = None
        self._latencies = []
        self._intervals = []
        self._msg_sizes = []
        self._metrics_hz = 0.0
        self._metrics_lat_avg = 0.0
        self._metrics_lat_max = 0.0
        self._metrics_jitter = 0.0
        self._metrics_bw_kbps = 0.0
        self._metrics_msg_size = 0
        self._metrics_window_start = time.monotonic()
        self._metrics_window_ticks = 0

    def send_spawn_sensor(self, sensor_type="sensor.camera.rgb", parent_id=None,
                          transform=None, attributes=None):
        payload = {"sensor_type": sensor_type}
        if parent_id:
            payload["parent_actor_id"] = parent_id
        if transform:
            payload["transform"] = transform
        if attributes:
            payload["attributes"] = attributes
        self._enqueue({"type": "spawn_sensor", "payload": payload})

    def on_connected(self, client_id):
        super().on_connected(client_id)
        self._state = "LISTING"
        log.info("Asking for spawn points...")
        self.send_list_spawn_points()

    def on_ack(self, ack):
        cmd = ack.get("command", "")

        if cmd == "list_spawn_points" and self._state == "LISTING":
            try:
                payload = json.loads(ack.get("message", "") or "{}")
                points = payload.get("spawn_points", [])
            except json.JSONDecodeError:
                points = []
            if not points:
                log.error("No spawn points")
                self.disconnect()
                return
            log.info("Got %d spawn points, using index %d", len(points), self._spawn_index)
            self._state = "SPAWNING"
            self.send_spawn_at_index(
                blueprint_id="vehicle.tesla.model3",
                spawn_point_index=self._spawn_index,
                autopilot=False,
            )

        elif cmd == "spawn" and self._state == "SPAWNING":
            if ack.get("status") == "ok" and ack.get("actor_id"):
                self._ego_id = ack["actor_id"]
                self._state = "SPAWNING_CAMERA"
                log.info("Ego spawned: actor_id=%d. Spawning camera...", self._ego_id)
                self.send_spawn_sensor(
                    sensor_type="sensor.camera.rgb",
                    parent_id=self._ego_id,
                    transform={
                        "location": {"x": -5.5, "y": 0.0, "z": 2.8},
                        "rotation": {"pitch": -12.0, "yaw": 0.0, "roll": 0.0},
                    },
                    attributes={
                        "image_size_x": str(WINDOW_W),
                        "image_size_y": str(WINDOW_H),
                        "fov": "90",
                        "sensor_tick": "0.05",
                    },
                )
            else:
                log.error("Spawn failed: %s", ack.get("message"))
                self.disconnect()

        elif cmd == "spawn_sensor" and self._state == "SPAWNING_CAMERA":
            if ack.get("status") == "ok" and ack.get("actor_id"):
                self._camera_id = ack["actor_id"]
                self._state = "DRIVING"
                log.info("Camera spawned: actor_id=%d. Drive with arrow keys!", self._camera_id)
            else:
                log.warning("Camera spawn failed: %s. Driving without camera.", ack.get("message"))
                self._state = "DRIVING"

    def on_world_state(self, state):
        now = time.time()
        now_mono = time.monotonic()
        self._vehicle_count = len(state.get("vehicles", []))

        # ── Metrics collection ───────────────────────────────────────────
        with self._metrics_lock:
            # Latency
            wall_time = state.get("wall_time", now)
            self._latencies.append((now - wall_time) * 1000)

            # Inter-message interval
            if self._last_recv_mono is not None:
                self._intervals.append((now_mono - self._last_recv_mono) * 1000)
            self._last_recv_mono = now_mono

            self._metrics_window_ticks += 1

            # Update summary every second
            elapsed = now_mono - self._metrics_window_start
            if elapsed >= 1.0:
                import statistics
                self._metrics_hz = self._metrics_window_ticks / elapsed
                if self._latencies:
                    self._metrics_lat_avg = statistics.mean(self._latencies)
                    self._metrics_lat_max = max(abs(l) for l in self._latencies)
                if len(self._intervals) > 1:
                    self._metrics_jitter = statistics.stdev(self._intervals)
                # Reset window
                self._latencies = []
                self._intervals = []
                self._metrics_window_start = now_mono
                self._metrics_window_ticks = 0

        # ── Ego telemetry ────────────────────────────────────────────────
        ego = next((v for v in state.get("vehicles", []) if v.get("is_ego")), None)
        if ego:
            vel = ego["velocity"]
            self._ego_speed = (vel["x"] ** 2 + vel["y"] ** 2 + vel["z"] ** 2) ** 0.5
            self._ego_x = ego["transform"]["location"]["x"]
            self._ego_y = ego["transform"]["location"]["y"]
            self._ego_yaw = ego["transform"]["rotation"]["yaw"]
            now2 = time.monotonic()
            if now2 - self._last_telemetry >= 3.0:
                self._last_telemetry = now2
                log.info("pos=(%.0f, %.0f) yaw=%.0f speed=%.1f km/h | %.0f Hz lat=%.1f ms jitter=%.1f ms",
                         self._ego_x, self._ego_y, self._ego_yaw,
                         self._ego_speed * 3.6,
                         self._metrics_hz, self._metrics_lat_avg, self._metrics_jitter)

        for sensor in state.get("sensors", []):
            if sensor.get("encoding") == "jpeg" and sensor.get("data"):
                try:
                    jpeg_bytes = base64.b64decode(sensor["data"])
                    from PIL import Image
                    pil_img = Image.open(io.BytesIO(jpeg_bytes))
                    raw = pil_img.tobytes()
                    pg_surface = pygame.image.fromstring(raw, pil_img.size, pil_img.mode)
                    with self._frame_lock:
                        self._latest_frame = pg_surface
                        self._frame_count += 1
                except Exception:
                    pass


def run_pygame_loop(driver):
    pygame.init()
    screen = pygame.display.set_mode((WINDOW_W, WINDOW_H))
    pygame.display.set_caption("CARLA Remote Driver")
    clock = pygame.time.Clock()
    font_large = pygame.font.SysFont("monospace", 22, bold=True)
    font_small = pygame.font.SysFont("monospace", 16)
    font_hud = pygame.font.SysFont("monospace", 14)

    running = True
    throttle = 0.0
    brake = 0.0
    steer = 0.0
    steer_cache = 0.0
    while running:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.KEYDOWN:
                if event.key in (pygame.K_q, pygame.K_ESCAPE):
                    running = False
                elif event.key == pygame.K_r:
                    driver._reverse = not driver._reverse

        if driver._state not in ("DRIVING", "SPAWNING_CAMERA"):
            screen.fill((30, 30, 30))
            text = font_large.render(f"State: {driver._state} ...", True, (200, 200, 200))
            screen.blit(text, (20, WINDOW_H // 2 - 20))
            pygame.display.flip()
            clock.tick(20)
            continue

        keys = pygame.key.get_pressed()
        milliseconds = clock.get_time()

        # ── Throttle/Brake: binary, same as CARLA manual_control.py ──
        new_throttle = 1.0 if (keys[pygame.K_UP] or keys[pygame.K_w]) else 0.0
        new_brake = 1.0 if (keys[pygame.K_DOWN] or keys[pygame.K_s]) else 0.0

        # Log state changes
        if new_throttle != throttle:
            log.info("CLIENT: throttle %.1f -> %.1f", throttle, new_throttle)
        if new_brake != brake:
            log.info("CLIENT: brake %.1f -> %.1f", brake, new_brake)
        throttle = new_throttle
        brake = new_brake

        # ── Steer: smooth ramping via steer_cache (CARLA's exact logic) ──
        steer_increment = 1.5e-3 * milliseconds
        if keys[pygame.K_LEFT] or keys[pygame.K_a]:
            if steer_cache > 0:
                steer_cache = 0
            else:
                steer_cache -= steer_increment
        elif keys[pygame.K_RIGHT] or keys[pygame.K_d]:
            if steer_cache < 0:
                steer_cache = 0
            else:
                steer_cache += steer_increment
        else:
            steer_cache = 0.0
        steer_cache = min(0.7, max(-0.7, steer_cache))
        steer = round(steer_cache, 3)

        hand_brake = keys[pygame.K_SPACE]

        # Send controls EVERY frame to ensure throttle=0 reaches server immediately
        if driver._state == "DRIVING":
            driver.send_ego_control(throttle=throttle, steer=steer, brake=brake,
                                    hand_brake=hand_brake, reverse=driver._reverse)

        with driver._frame_lock:
            frame = driver._latest_frame

        if frame is not None:
            if frame.get_size() != (WINDOW_W, WINDOW_H):
                frame = pygame.transform.scale(frame, (WINDOW_W, WINDOW_H))
            screen.blit(frame, (0, 0))
        else:
            screen.fill((20, 25, 30))
            msg = "Waiting for camera feed..." if driver._camera_id else "No camera"
            screen.blit(font_small.render(msg, True, (180, 180, 180)), (WINDOW_W // 2 - 130, WINDOW_H // 2))

        hud = pygame.Surface((WINDOW_W, 70), pygame.SRCALPHA)
        hud.fill((0, 0, 0, 140))
        screen.blit(hud, (0, WINDOW_H - 70))

        speed_kmh = driver._ego_speed * 3.6
        color = (100, 255, 100) if speed_kmh < 40 else (255, 200, 50) if speed_kmh < 80 else (255, 80, 80)
        screen.blit(font_large.render(f"{speed_kmh:.0f} km/h", True, color), (15, WINDOW_H - 65))
        screen.blit(font_hud.render(f"({driver._ego_x:.0f},{driver._ego_y:.0f}) yaw={driver._ego_yaw:.0f}", True, (180, 180, 180)), (200, WINDOW_H - 65))

        ctrls = []
        if throttle > 0: ctrls.append("THR")
        if brake > 0: ctrls.append("BRK")
        if steer < 0: ctrls.append("LEFT")
        elif steer > 0: ctrls.append("RIGHT")
        if hand_brake: ctrls.append("HB")
        if driver._reverse: ctrls.append("REV")
        screen.blit(font_hud.render("  ".join(ctrls) or "---", True, (200, 200, 200)), (200, WINDOW_H - 45))
        screen.blit(font_hud.render(f"veh:{driver._vehicle_count} frames:{driver._frame_count}", True, (180, 180, 180)), (550, WINDOW_H - 65))

        # Metrics line
        screen.blit(font_hud.render(
            f"{driver._metrics_hz:.0f}Hz  lat:{driver._metrics_lat_avg:.1f}ms  jitter:{driver._metrics_jitter:.1f}ms",
            True, (120, 200, 255)), (15, WINDOW_H - 20))

        pygame.display.flip()
        clock.tick(60)

    log.info("Quitting...")
    if driver._camera_id:
        driver.send_destroy(driver._camera_id)
        time.sleep(0.3)
    if driver._ego_id:
        driver.send_destroy(driver._ego_id)
        time.sleep(0.3)
    driver.disconnect()
    pygame.quit()


def main():
    parser = argparse.ArgumentParser(description="CARLA Interactive Driver with Camera")
    parser.add_argument("--server", default="ws://localhost:8765")
    parser.add_argument("--spawn-index", type=int, default=0)
    args = parser.parse_args()

    driver = InteractiveDriver(args.server, args.spawn_index)
    driver.run_in_thread()

    log.info("Connecting to %s ...", args.server)
    for _ in range(100):
        if driver._state != "INIT":
            break
        time.sleep(0.1)

    try:
        run_pygame_loop(driver)
    except KeyboardInterrupt:
        if driver._camera_id:
            driver.send_destroy(driver._camera_id)
        if driver._ego_id:
            driver.send_destroy(driver._ego_id)
        driver.disconnect()

    log.info("Driver stopped")


if __name__ == "__main__":
    main()
