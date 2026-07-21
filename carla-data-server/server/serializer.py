"""
Serializer
==========
Thin abstraction over JSON and Protobuf encoding for the CARLA Data Server
protocol. Both the server and clients import this module and use the same
encode/decode functions.

Usage:
    from serializer import Serializer

    ser = Serializer(format="json")      # or "protobuf"
    raw = ser.encode_world_state(state_dict)
    state_dict = ser.decode_message(raw)

The dict format is the canonical internal representation. JSON and Protobuf
are just two ways to put it on the wire. Switching formats requires no
changes to any business logic.
"""

import json
from typing import Union

try:
    from proto_gen import world_state_pb2 as pb
    PROTOBUF_AVAILABLE = True
except ImportError:
    try:
        import world_state_pb2 as pb
        PROTOBUF_AVAILABLE = True
    except ImportError:
        PROTOBUF_AVAILABLE = False


class Serializer:
    def __init__(self, format: str = "json"):
        self.format = format
        if format == "protobuf" and not PROTOBUF_AVAILABLE:
            raise RuntimeError("protobuf format requested but world_state_pb2 not found. "
                               "Run: protoc --python_out=. world_state.proto")

    @property
    def is_binary(self) -> bool:
        return self.format == "protobuf"

    # ── encode (dict -> bytes or str) ────────────────────────────────────

    def encode_world_state(self, state: dict) -> Union[str, bytes]:
        if self.format == "protobuf":
            return self._dict_to_proto_world_state(state)
        return json.dumps(state)

    def encode_ack(self, ack: dict) -> Union[str, bytes]:
        if self.format == "protobuf":
            return self._dict_to_proto_ack(ack)
        return json.dumps(ack)

    def encode_welcome(self, welcome: dict) -> str:
        # Welcome is always JSON (even in protobuf mode) because the client
        # needs to know the format before it can decode anything
        return json.dumps(welcome)

    def encode_command(self, cmd: dict) -> Union[str, bytes]:
        if self.format == "protobuf":
            return self._dict_to_proto_command(cmd)
        return json.dumps(cmd)

    # ── decode (bytes or str -> dict) ────────────────────────────────────

    def decode_message(self, raw: Union[str, bytes]) -> dict:
        if isinstance(raw, bytes) and self.format == "protobuf":
            return self._proto_to_dict(raw)
        if isinstance(raw, str):
            return json.loads(raw)
        # Fallback: try JSON on bytes
        return json.loads(raw.decode("utf-8"))

    # ── protobuf -> dict ─────────────────────────────────────────────────

    def _proto_to_dict(self, raw: bytes) -> dict:
        envelope = pb.Envelope()
        envelope.ParseFromString(raw)

        which = envelope.WhichOneof("payload")
        if which == "world_state":
            return self._proto_world_state_to_dict(envelope.world_state)
        elif which == "ack":
            return self._proto_ack_to_dict(envelope.ack)
        elif which == "command":
            return self._proto_command_to_dict(envelope.command)
        return {}

    def _proto_world_state_to_dict(self, ws) -> dict:
        result = {
            "type": "world_state",
            "tick": ws.tick,
            "timestamp": ws.timestamp,
            "wall_time": ws.wall_time,
            "vehicles": [],
            "pedestrians": [],
            "traffic_lights": [],
            "sensors": [],
        }
        for v in ws.vehicles:
            result["vehicles"].append({
                "id": v.id,
                "type_id": v.type_id,
                "transform": self._transform_to_dict(v.transform),
                "velocity": self._vec3_to_dict(v.velocity),
                "angular_vel": self._vec3_to_dict(v.angular_vel),
                "is_ego": v.is_ego,
            })
        for p in ws.pedestrians:
            result["pedestrians"].append({
                "id": p.id,
                "type_id": p.type_id,
                "transform": self._transform_to_dict(p.transform),
                "velocity": self._vec3_to_dict(p.velocity),
            })
        tl_state_map = {0: "Red", 1: "Yellow", 2: "Green", 3: "Off"}
        for tl in ws.traffic_lights:
            result["traffic_lights"].append({
                "id": tl.id,
                "transform": self._transform_to_dict(tl.transform),
                "state": tl_state_map.get(tl.state, "Off"),
                "elapsed": tl.elapsed,
            })
        for s in ws.sensors:
            result["sensors"].append({
                "actor_id": s.actor_id,
                "sensor_type": s.sensor_type,
                "encoding": s.encoding,
                "data": bytes(s.data),
                "frame": s.frame,
            })
        return result

    def _proto_ack_to_dict(self, ack) -> dict:
        status_map = {0: "ok", 1: "failed", 2: "denied"}
        return {
            "type": "ack",
            "client_id": ack.client_id,
            "status": status_map.get(ack.status, "ok"),
            "message": ack.message,
            "actor_id": ack.actor_id,
        }

    def _proto_command_to_dict(self, cmd) -> dict:
        which = cmd.WhichOneof("payload")
        if which == "ego_control":
            return {
                "type": "ego_control",
                "payload": {
                    "throttle": cmd.ego_control.throttle,
                    "steer": cmd.ego_control.steer,
                    "brake": cmd.ego_control.brake,
                    "hand_brake": cmd.ego_control.hand_brake,
                    "reverse": cmd.ego_control.reverse,
                }
            }
        elif which == "spawn":
            return {
                "type": "spawn",
                "payload": {
                    "blueprint_id": cmd.spawn.blueprint_id,
                    "transform": self._transform_to_dict(cmd.spawn.transform),
                    "autopilot": cmd.spawn.autopilot,
                }
            }
        elif which == "destroy":
            return {
                "type": "destroy",
                "payload": {"actor_id": cmd.destroy.actor_id}
            }
        elif which == "subscribe":
            return {
                "type": "subscribe",
                "payload": {"topics": list(cmd.subscribe.topics)}
            }
        return {}

    # ── dict -> protobuf ─────────────────────────────────────────────────

    def _dict_to_proto_world_state(self, state: dict) -> bytes:
        ws = pb.WorldState()
        ws.tick = state.get("tick", 0)
        ws.timestamp = state.get("timestamp", 0.0)
        ws.wall_time = state.get("wall_time", 0.0)

        for v in state.get("vehicles", []):
            veh = ws.vehicles.add()
            veh.id = v["id"]
            veh.type_id = v.get("type_id", "")
            self._fill_transform(veh.transform, v.get("transform", {}))
            self._fill_vec3(veh.velocity, v.get("velocity", {}))
            self._fill_vec3(veh.angular_vel, v.get("angular_vel", {}))
            veh.is_ego = v.get("is_ego", False)

        for p in state.get("pedestrians", []):
            ped = ws.pedestrians.add()
            ped.id = p["id"]
            ped.type_id = p.get("type_id", "")
            self._fill_transform(ped.transform, p.get("transform", {}))
            self._fill_vec3(ped.velocity, p.get("velocity", {}))

        tl_state_map = {"Red": 0, "Yellow": 1, "Green": 2, "Off": 3}
        for tl in state.get("traffic_lights", []):
            light = ws.traffic_lights.add()
            light.id = tl["id"]
            self._fill_transform(light.transform, tl.get("transform", {}))
            light.state = tl_state_map.get(tl.get("state", "Off"), 3)
            light.elapsed = tl.get("elapsed", 0.0)

        for s in state.get("sensors", []):
            sensor = ws.sensors.add()
            sensor.actor_id = s.get("actor_id", 0)
            sensor.sensor_type = s.get("sensor_type", "")
            sensor.encoding = s.get("encoding", "")
            data = s.get("data", b"")
            if isinstance(data, bytes):
                sensor.data = data
            elif isinstance(data, str):
                sensor.data = data.encode("utf-8")
            sensor.frame = s.get("frame", 0)

        envelope = pb.Envelope()
        envelope.world_state.CopyFrom(ws)
        return envelope.SerializeToString()

    def _dict_to_proto_ack(self, ack: dict) -> bytes:
        a = pb.CommandAck()
        a.client_id = ack.get("client_id", "")
        status_map = {"ok": 0, "failed": 1, "denied": 2}
        a.status = status_map.get(ack.get("status", "ok"), 0)
        a.message = ack.get("message", "")
        a.actor_id = ack.get("actor_id", 0)

        envelope = pb.Envelope()
        envelope.ack.CopyFrom(a)
        return envelope.SerializeToString()

    def _dict_to_proto_command(self, cmd: dict) -> bytes:
        c = pb.ClientCommand()
        cmd_type = cmd.get("type", "")
        payload = cmd.get("payload", {})

        if cmd_type == "ego_control":
            c.ego_control.throttle = payload.get("throttle", 0)
            c.ego_control.steer = payload.get("steer", 0)
            c.ego_control.brake = payload.get("brake", 0)
            c.ego_control.hand_brake = payload.get("hand_brake", False)
            c.ego_control.reverse = payload.get("reverse", False)
        elif cmd_type == "spawn":
            c.spawn.blueprint_id = payload.get("blueprint_id", "")
            if "transform" in payload:
                self._fill_transform(c.spawn.transform, payload["transform"])
            c.spawn.autopilot = payload.get("autopilot", False)
        elif cmd_type == "destroy":
            c.destroy.actor_id = payload.get("actor_id", 0)
        elif cmd_type == "subscribe":
            c.subscribe.topics.extend(payload.get("topics", []))

        envelope = pb.Envelope()
        envelope.command.CopyFrom(c)
        return envelope.SerializeToString()

    # ── helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _transform_to_dict(t) -> dict:
        return {
            "location": {"x": t.location.x, "y": t.location.y, "z": t.location.z},
            "rotation": {"pitch": t.rotation.pitch, "yaw": t.rotation.yaw, "roll": t.rotation.roll},
        }

    @staticmethod
    def _vec3_to_dict(v) -> dict:
        return {"x": v.x, "y": v.y, "z": v.z}

    @staticmethod
    def _fill_transform(proto_t, d: dict):
        loc = d.get("location", {})
        rot = d.get("rotation", {})
        proto_t.location.x = loc.get("x", 0)
        proto_t.location.y = loc.get("y", 0)
        proto_t.location.z = loc.get("z", 0)
        proto_t.rotation.pitch = rot.get("pitch", 0)
        proto_t.rotation.yaw = rot.get("yaw", 0)
        proto_t.rotation.roll = rot.get("roll", 0)

    @staticmethod
    def _fill_vec3(proto_v, d: dict):
        proto_v.x = d.get("x", 0)
        proto_v.y = d.get("y", 0)
        proto_v.z = d.get("z", 0)
