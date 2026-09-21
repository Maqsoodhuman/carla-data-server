"""Map verification and doctor blocking semantics.

A wrong CARLA map is the quiet failure mode this guards: spawn indexes point
somewhere else and carla_mirror_client pairs traffic lights by index against
a different layout, so nothing errors - it just mirrors the wrong world.
"""

import pytest

from orchestration import protocol as P
from orchestration.config import Config
from orchestration.doctor import (check_carla_map, map_basename, map_matches,
                                  run_doctor)


@pytest.mark.parametrize("raw,expected", [
    ("UBAutonomousProvingGrounds", "ubautonomousprovinggrounds"),
    ("/Game/Carla/Maps/UBAutonomousProvingGrounds", "ubautonomousprovinggrounds"),
    ("Carla/Maps/Town10HD_Opt", "town10hd_opt"),
    ("/Game/Carla/Maps/Town10HD_Opt/", "town10hd_opt"),
    ("  Town01  ", "town01"),
])
def test_map_basename_strips_carla_path_prefixes(raw, expected):
    assert map_basename(raw) == expected


def test_map_matches_ignores_path_and_case():
    assert map_matches("/Game/Carla/Maps/UBAutonomousProvingGrounds",
                       "ubautonomousprovinggrounds")


def test_map_mismatch_is_detected():
    assert not map_matches("/Game/Carla/Maps/Town10HD_Opt",
                           "UBAutonomousProvingGrounds")


def test_town10_is_not_confused_with_town01():
    assert not map_matches("Carla/Maps/Town01", "Town10HD_Opt")


@pytest.mark.parametrize("expected", ["any", "ANY", "", "  any  "])
def test_any_disables_the_check(expected):
    assert map_matches("literally/whatever", expected)


def test_check_passes_without_carla_when_map_is_any(monkeypatch):
    monkeypatch.setenv("CARLA_MAP", "any")
    check = check_carla_map(Config.from_env())
    assert check["ok"] is True
    assert check["skipped"] is False


def test_check_is_skipped_not_failed_when_map_cannot_be_read(monkeypatch):
    # No CARLA PythonAPI here, so the map is unknowable - that must not be
    # reported as a mismatch, and must not block.
    monkeypatch.setenv("CARLA_MAP", "UBAutonomousProvingGrounds")
    check = check_carla_map(Config.from_env())
    assert check["ok"] is False
    assert check["skipped"] is True
    assert "cannot verify map" in check["detail"]


def test_wrong_map_reports_both_loaded_and_required(monkeypatch):
    monkeypatch.setenv("CARLA_MAP", "UBAutonomousProvingGrounds")
    monkeypatch.setattr("orchestration.doctor.loaded_carla_map",
                        lambda host, port, timeout=10.0: ("/Game/Carla/Maps/Town10HD_Opt", ""))
    check = check_carla_map(Config.from_env())
    assert check["ok"] is False
    assert check["skipped"] is False
    assert "Town10HD_Opt" in check["detail"] and "UBAutonomousProvingGrounds" in check["detail"]
    assert "load UBAutonomousProvingGrounds" in check["fix"]


def test_correct_map_passes(monkeypatch):
    monkeypatch.setenv("CARLA_MAP", "UBAutonomousProvingGrounds")
    monkeypatch.setattr(
        "orchestration.doctor.loaded_carla_map",
        lambda host, port, timeout=10.0: ("/Game/Carla/Maps/UBAutonomousProvingGrounds", ""))
    assert check_carla_map(Config.from_env())["ok"] is True


# ── blocking semantics ───────────────────────────────────────────────────────

def test_skipped_checks_never_block(monkeypatch):
    # On this machine there is no CARLA at all, so carla_map is skipped and
    # the lab report must not be blocked by it.
    monkeypatch.setenv("CARLA_MAP", "UBAutonomousProvingGrounds")
    report = run_doctor(Config.from_env(), P.ROLE_LAB)
    assert "carla_map" not in report["blocking"]


def test_wrong_map_does_block_the_lab(monkeypatch):
    monkeypatch.setenv("CARLA_MAP", "UBAutonomousProvingGrounds")
    monkeypatch.setattr("orchestration.doctor.loaded_carla_map",
                        lambda host, port, timeout=10.0: ("Town10HD_Opt", ""))
    report = run_doctor(Config.from_env(), P.ROLE_LAB)
    assert "carla_map" in report["blocking"]
    assert report["ok"] is False


def test_lab_worker_refuses_to_prepare_on_a_wrong_map(monkeypatch, tmp_path):
    from orchestration.lab_worker import LabWorker
    monkeypatch.setenv("CARLA_MAP", "UBAutonomousProvingGrounds")
    monkeypatch.setattr("orchestration.lab_worker.check_carla_map",
                        lambda cfg: {"ok": False, "skipped": False,
                                     "detail": "loaded 'Town01', required 'UB...'",
                                     "name": "carla_map", "fix": ""})
    worker = LabWorker(Config.from_env(state_dir=str(tmp_path)), coordinator_client=None,
                       manage_server=False)
    ready, detail = worker._prepare("connectivity")
    assert ready is False
    assert "carla_map" in detail
