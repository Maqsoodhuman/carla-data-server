"""Environment-driven configuration.

Binding vs connecting is explicit and deliberate:

  * the LAB machine BINDS the coordinator to COORDINATOR_BIND_HOST (0.0.0.0)
    and the data server to DATA_SERVER_BIND_HOST (0.0.0.0)
  * the CLIENT machine CONNECTS to LAB_HOST on those same ports

Nothing here hard-codes a machine address; LAB_HOST defaults to localhost so
the whole loop can run on one machine for development.
"""

import os
from dataclasses import dataclass, asdict

DEFAULTS = {
    "LAB_HOST": "127.0.0.1",
    "COORDINATOR_PORT": 8770,
    "COORDINATOR_BIND_HOST": "0.0.0.0",
    "DATA_SERVER_PORT": 8765,
    "DATA_SERVER_BIND_HOST": "0.0.0.0",
    "CARLA_HOST": "127.0.0.1",
    "CARLA_PORT": 2000,
    "SHADOW_CARLA_HOST": "127.0.0.1",
    "SHADOW_CARLA_PORT": 2001,
    "ORCH_TICK_RATE": 20.0,
    "ORCH_STATE_DIR": ".orchestration",
}


def _str(name):
    return os.environ.get(name) or str(DEFAULTS[name])


def _int(name):
    raw = os.environ.get(name)
    if not raw:
        return int(DEFAULTS[name])
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"{name} must be an integer, got {raw!r}")


def _float(name):
    raw = os.environ.get(name)
    if not raw:
        return float(DEFAULTS[name])
    try:
        return float(raw)
    except ValueError:
        raise SystemExit(f"{name} must be a number, got {raw!r}")


@dataclass
class Config:
    lab_host: str
    coordinator_port: int
    coordinator_bind_host: str
    data_server_port: int
    data_server_bind_host: str
    carla_host: str
    carla_port: int
    shadow_carla_host: str
    shadow_carla_port: int
    tick_rate: float
    state_dir: str

    @classmethod
    def from_env(cls, **overrides):
        cfg = cls(
            lab_host=_str("LAB_HOST"),
            coordinator_port=_int("COORDINATOR_PORT"),
            coordinator_bind_host=_str("COORDINATOR_BIND_HOST"),
            data_server_port=_int("DATA_SERVER_PORT"),
            data_server_bind_host=_str("DATA_SERVER_BIND_HOST"),
            carla_host=_str("CARLA_HOST"),
            carla_port=_int("CARLA_PORT"),
            shadow_carla_host=_str("SHADOW_CARLA_HOST"),
            shadow_carla_port=_int("SHADOW_CARLA_PORT"),
            tick_rate=_float("ORCH_TICK_RATE"),
            state_dir=_str("ORCH_STATE_DIR"),
        )
        for key, value in overrides.items():
            if value is not None:
                setattr(cfg, key, value)
        return cfg

    # ── what a CLIENT connects to ────────────────────────────────────────────

    @property
    def coordinator_url(self) -> str:
        return f"http://{self.lab_host}:{self.coordinator_port}"

    @property
    def data_server_url(self) -> str:
        return f"ws://{self.lab_host}:{self.data_server_port}"

    # ── what the LAB binds ───────────────────────────────────────────────────

    @property
    def coordinator_bind(self) -> tuple:
        return (self.coordinator_bind_host, self.coordinator_port)

    def as_dict(self) -> dict:
        data = asdict(self)
        data["coordinator_url"] = self.coordinator_url
        data["data_server_url"] = self.data_server_url
        return data
